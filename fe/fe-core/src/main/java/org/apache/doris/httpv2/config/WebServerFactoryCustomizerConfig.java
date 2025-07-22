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

package org.apache.doris.httpv2.config;

import org.apache.doris.common.Config;

import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.springframework.boot.web.embedded.jetty.ConfigurableJettyWebServerFactory;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;

@Configuration
public class WebServerFactoryCustomizerConfig implements WebServerFactoryCustomizer<ConfigurableJettyWebServerFactory> {
    @Override
    public void customize(ConfigurableJettyWebServerFactory factory) {
        // Enable SSL if either HTTPS is enabled or mTLS authentication is configured
        boolean enableSsl = Config.enable_https || "mtls".equalsIgnoreCase(Config.authentication_type);

        if (enableSsl) {
            ((JettyServletWebServerFactory) factory).setConfigurations(
                    Collections.singleton(new HttpToHttpsJettyConfig())
            );

            factory.addServerCustomizers(
                    server -> {
                        // Configure HTTP
                        HttpConfiguration httpConfiguration = new HttpConfiguration();
                        httpConfiguration.setSecurePort(Config.https_port);
                        httpConfiguration.setSecureScheme("https");

                        // Create HTTP connector
                        ServerConnector httpConnector = new ServerConnector(server);
                        httpConnector.addConnectionFactory(new HttpConnectionFactory(httpConfiguration));
                        httpConnector.setPort(Config.http_port);
                        server.addConnector(httpConnector);

                        // Configure and add a single HTTPS connector
                        configureSslConnector(server, httpConfiguration);
                    }
            );
        }
    }

    /**
     * Configures a single SSL connector for either standard HTTPS or mTLS authentication
     * using the same port (Config.https_port) to avoid any conflicts.
     */
    private void configureSslConnector(Server server, HttpConfiguration httpConfiguration) {
        // Create SSL Context Factory
        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();

        // Determine if we're using mTLS authentication
        boolean isMtls = "mtls".equalsIgnoreCase(Config.authentication_type);

        // Configure server certificate (key store)
        sslContextFactory.setKeyStorePath(Config.key_store_path);
        sslContextFactory.setKeyStorePassword(Config.key_store_password);
        sslContextFactory.setKeyStoreType(Config.key_store_type);

        // Configure client certificate validation (trust store) for mTLS
        boolean needClientAuth = isMtls || Config.ssl_force_client_auth;
        if (needClientAuth) {
            sslContextFactory.setNeedClientAuth(true);

            // For trust store (client certificate validation), still use the MySQL SSL CA certificate
            sslContextFactory.setTrustStorePath(Config.mysql_ssl_default_ca_certificate);
            sslContextFactory.setTrustStorePassword(Config.mysql_ssl_default_ca_certificate_password);
            sslContextFactory.setTrustStoreType(Config.ssl_trust_store_type);
        }

        // Configure HTTPS
        HttpConfiguration httpsConfiguration = new HttpConfiguration(httpConfiguration);
        httpsConfiguration.addCustomizer(new SecureRequestCustomizer());

        // Create a single SSL connector on the configured HTTPS port
        ServerConnector sslConnector = new ServerConnector(
                server,
                new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
                new HttpConnectionFactory(httpsConfiguration));
        sslConnector.setPort(Config.https_port);

        server.addConnector(sslConnector);
    }
}
