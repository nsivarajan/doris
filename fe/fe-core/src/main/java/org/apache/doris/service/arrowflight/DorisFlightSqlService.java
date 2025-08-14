// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.service.arrowflight;

import org.apache.doris.common.Config;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.service.arrowflight.auth2.FlightBearerTokenAuthenticator;
import org.apache.doris.service.arrowflight.auth2.JksToPemConverter;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsWithTokenManager;
import org.apache.doris.service.arrowflight.tokens.FlightTokenManager;
import org.apache.doris.service.arrowflight.tokens.FlightTokenManagerImpl;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * flight sql protocol implementation based on nio.
 */
public class DorisFlightSqlService {
    private static final Logger LOG = LogManager.getLogger(DorisFlightSqlService.class);
    private final FlightServer flightServer;
    private final FlightTokenManager flightTokenManager;
    private final FlightSessionsManager flightSessionsManager;
    private volatile boolean running;

    public DorisFlightSqlService(int port) {
        BufferAllocator allocator = new RootAllocator();
        // arrow_flight_token_cache_size less than qe_max_connection to avoid `Reach limit of connections`.
        // arrow flight sql is a stateless protocol, connection is usually not actively disconnected.
        // bearer token is evict from the cache will unregister ConnectContext.
        this.flightTokenManager = new FlightTokenManagerImpl(
                Math.min(Config.arrow_flight_token_cache_size, Config.qe_max_connection / 2),
                Config.arrow_flight_token_alive_time);
        this.flightSessionsManager = new FlightSessionsWithTokenManager(flightTokenManager);

        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure(FrontendOptions.getLocalHostAddress(), port), flightSessionsManager);

        // Determine the effective authentication type based on inheritance rules
        String effectiveAuthType = determineEffectiveAuthType();

        // Determine if SSL should be enabled
        // If global authentication_type is mtls, SSL is automatically enabled
        boolean globalIsMtls = "mtls".equalsIgnoreCase(Config.authentication_type);
        boolean enableSsl = Config.arrow_flight_enable_ssl || globalIsMtls;

        // If the effective authentication type is ldap, ensure LDAP is properly configured
        if ("ldap".equalsIgnoreCase(effectiveAuthType)) {
            LOG.info("LDAP authentication is enabled for Arrow Flight");
        }

        FlightServer.Builder builder;

        if (enableSsl) {
            // Use TLS for Arrow Flight
            try {
                Location location = Location.forGrpcTls("0.0.0.0", port);
                builder = FlightServer.builder(allocator, location, producer);

                // Check if key store file exists
                File keyStoreFile = new File(Config.key_store_path);
                if (!keyStoreFile.exists()) {
                    LOG.error("Key store file not found: {}", Config.key_store_path);
                    throw new IOException("Key store file not found: " + Config.key_store_path);
                }

                try {
                    // Convert key store to PEM format
                    File[] keyFiles = JksToPemConverter.convertJksToPem(
                            Config.key_store_path,
                            Config.key_store_password.toCharArray(),
                            Config.key_store_alias);

                    // Configure TLS with PEM files
                    builder.useTls(keyFiles[0], keyFiles[1]);

                    // If global authentication_type is mtls, require client certificates
                    if (globalIsMtls) {
                        LOG.info("mTLS transport security is enabled for Arrow Flight");

                        // Check if trust store file exists
                        File trustStoreFile = new File(Config.mysql_ssl_default_ca_certificate);
                        if (!trustStoreFile.exists()) {
                            LOG.error("Trust store file not found: {}", Config.mysql_ssl_default_ca_certificate);
                            throw new IOException("Trust store file not found: "
                                    + Config.mysql_ssl_default_ca_certificate);
                        }

                        // Convert trust store to PEM format
                        File trustFile = JksToPemConverter.convertTruststoreToPem(
                                Config.mysql_ssl_default_ca_certificate,
                                Config.mysql_ssl_default_ca_certificate_password.toCharArray());

                        // Configure client authentication with PEM file
                        builder.useMTlsClientVerification(trustFile);
                    }

                    // Always use bearer token authenticator since Arrow Flight 17.0.0 cannot
                    // access client certificates during authentication
                    builder.headerAuthenticator(new FlightBearerTokenAuthenticator(flightTokenManager));
                } catch (Exception e) {
                    LOG.error("Failed to convert certificates to PEM format", e);
                    throw new IOException("Failed to convert certificates to PEM format", e);
                }
            } catch (IOException e) {
                LOG.error("Failed to configure TLS for Arrow Flight", e);
                // Fall back to insecure connection
                builder = FlightServer.builder(allocator, Location.forGrpcInsecure("0.0.0.0", port), producer)
                        .headerAuthenticator(new FlightBearerTokenAuthenticator(flightTokenManager));
            }
        } else {
            // Use insecure connection
            builder = FlightServer.builder(allocator, Location.forGrpcInsecure("0.0.0.0", port), producer)
                    .headerAuthenticator(new FlightBearerTokenAuthenticator(flightTokenManager));
        }

        flightServer = builder.build();

        LOG.info("Arrow Flight SQL service is created, port: {}, arrow_flight_max_connections: {}, "
                + "arrow_flight_token_alive_time_second: {}, authentication_type: {}, "
                + "global_authentication_type: {}, SSL enabled: {}",
                port, Config.arrow_flight_max_connections, Config.arrow_flight_token_alive_time_second,
                effectiveAuthType, Config.authentication_type, enableSsl);
    }

    // start Arrow Flight SQL service, return true if success, otherwise false
    public boolean start() {
        try {
            flightServer.start();
            running = true;
            LOG.info("Arrow Flight SQL service is started.");
        } catch (IOException e) {
            LOG.error("Start Arrow Flight SQL service failed.", e);
            return false;
        }
        return true;
    }

    public void stop() {
        if (running) {
            running = false;
            try {
                flightServer.close();
            } catch (InterruptedException e) {
                LOG.warn("close Arrow Flight SQL server failed.", e);
            }
        }
    }

    /**
     * Determines the effective authentication type for Arrow Flight based on inheritance rules:
     * 1. If arrow_flight_authentication_type is explicitly set, use that value
     * 2. If authentication_type is "ldap", arrow_flight_authentication_type inherits "ldap"
     * 3. If authentication_type is "default", arrow_flight_authentication_type inherits "default"
     * 4. If authentication_type is "mtls", use arrow_flight_authentication_type if set, otherwise use "default"
     *
     * @return The effective authentication type to use for Arrow Flight
     */
    private String determineEffectiveAuthType() {
        // If arrow_flight_authentication_type is explicitly set, use that value
        if (Config.arrow_flight_authentication_type != null && !Config.arrow_flight_authentication_type.isEmpty()) {
            return Config.arrow_flight_authentication_type;
        }

        // Otherwise, inherit from authentication_type based on rules
        String globalAuthType = Config.authentication_type;

        // Handle null authentication_type
        if (globalAuthType == null || globalAuthType.isEmpty()) {
            return "default";
        }

        globalAuthType = globalAuthType.toLowerCase();

        if ("ldap".equals(globalAuthType) || "default".equals(globalAuthType)) {
            return globalAuthType;
        } else if ("mtls".equals(globalAuthType)) {
            // For mtls global auth type, default to "default" for Arrow Flight
            return "default";
        }

        // Default to "default" if authentication_type is not recognized
        return "default";
    }
}