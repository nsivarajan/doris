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

package org.apache.doris.service.arrowflight.auth2;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.authenticate.mtls.MTLSUtils;

import org.apache.arrow.flight.CallStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.cert.X509Certificate;

/**
 * Utility class for mTLS authentication in Arrow Flight.
 */
public class MTLSFlightUtils {
    private static final Logger LOG = LogManager.getLogger(MTLSFlightUtils.class);

    private MTLSFlightUtils() {
        // Utility class should not be instantiated
    }

    /**
     * Validates a client certificate and returns the corresponding username.
     *
     * @param clientCert the client certificate
     * @return the username derived from the certificate
     * @throws CallStatus.FlightRuntimeException if validation fails
     */
    public static String validateCertificateAndGetUsername(X509Certificate clientCert) {
        if (clientCert == null) {
            LOG.warn("Client certificate is null");
            throw CallStatus.UNAUTHENTICATED.withDescription("Client certificate is null").toRuntimeException();
        }

        try {
            // Generate username from certificate serial number using MTLSUtils
            String username = MTLSUtils.getUsernameFromCertificate(clientCert);
            String serialNumber = MTLSUtils.getSerialNumberHex(clientCert);

            LOG.info("Generated username '{}' for certificate with serial number '{}'", username, serialNumber);

            // Look up Doris user by generated username
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(username, "%");
            if (!Env.getCurrentEnv().getAuth().doesUserExist(userIdentity)) {
                LOG.warn("No Doris user found for username: {} (certificate serial: {})", username, serialNumber);
                throw CallStatus.UNAUTHENTICATED.withDescription("User not found: " + username).toRuntimeException();
            }

            return username;
        } catch (AnalysisException e) {
            LOG.error("Failed to generate username from certificate", e);
            throw CallStatus.UNAUTHENTICATED.withCause(e).withDescription(e.getMessage()).toRuntimeException();
        } catch (Exception e) {
            LOG.error("Failed to validate certificate", e);
            throw CallStatus.UNAUTHENTICATED.withCause(e).withDescription(e.getMessage()).toRuntimeException();
        }
    }

    /**
     * Validates a client certificate and returns the corresponding FlightAuthResult.
     *
     * @param clientCert the client certificate
     * @param remoteIp the remote IP address
     * @return the FlightAuthResult
     * @throws CallStatus.FlightRuntimeException if validation fails
     */
    public static FlightAuthResult validateCertificateAndGetAuthResult(X509Certificate clientCert, String remoteIp) {
        String username = validateCertificateAndGetUsername(clientCert);

        try {
            // Create user identity
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(username, remoteIp);

            // Create FlightAuthResult
            return FlightAuthResult.of(username, userIdentity, remoteIp);
        } catch (Exception e) {
            LOG.error("Failed to create FlightAuthResult", e);
            throw CallStatus.UNAUTHENTICATED.withCause(e).withDescription(e.getMessage()).toRuntimeException();
        }
    }
}
