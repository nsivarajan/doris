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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.springframework.boot.web.embedded.jetty.ConfigurableJettyWebServerFactory;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.util.Collections;

@Configuration
public class WebServerFactoryCustomizerConfig implements WebServerFactoryCustomizer<ConfigurableJettyWebServerFactory> {
    private static final Logger LOG = LogManager.getLogger(WebServerFactoryCustomizerConfig.class);

    @Override
    public void customize(ConfigurableJettyWebServerFactory factory) {
        // When authentication_type is "mtls", automatically enable HTTPS
        if ("mtls".equalsIgnoreCase(Config.authentication_type)) {
            Config.enable_https = true;
            Config.ssl_force_client_auth = true;
        }

        if (Config.enable_https) {
            ((JettyServletWebServerFactory) factory).setConfigurations(
                    Collections.singleton(new HttpToHttpsJettyConfig())
            );

            factory.addServerCustomizers(
                    server -> {
                        // Configure HTTP connector
                        HttpConfiguration httpConfiguration = new HttpConfiguration();
                        httpConfiguration.setSecurePort(Config.https_port);
                        httpConfiguration.setSecureScheme("https");

                        ServerConnector connector = new ServerConnector(server);
                        connector.addConnectionFactory(new HttpConnectionFactory(httpConfiguration));
                        connector.setPort(Config.http_port);

                        server.addConnector(connector);
                        
                        // Configure HTTPS connector with client certificate authentication if enabled
                        if ("mtls".equalsIgnoreCase(Config.authentication_type)) {
                            try {
                                // Create SSL Context Factory
                                SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
                                
                                // Configure server keystore - use the same keystore as MySQL
                                sslContextFactory.setKeyStorePath(Config.key_store_path);
                                if (Config.key_store_password != null && !Config.key_store_password.isEmpty()) {
                                    sslContextFactory.setKeyStorePassword(Config.key_store_password);
                                }
                                if (Config.key_store_alias != null && !Config.key_store_alias.isEmpty()) {
                                    sslContextFactory.setCertAlias(Config.key_store_alias);
                                }
                                
                                // Configure client certificate authentication
                                sslContextFactory.setNeedClientAuth(true);
                                
                                // Configure truststore for client certificates
                                if (Config.mysql_ssl_default_ca_certificate != null) {
                                    File trustStoreFile = new File(Config.mysql_ssl_default_ca_certificate);
                                    if (trustStoreFile.exists()) {
                                        sslContextFactory.setTrustStorePath(Config.mysql_ssl_default_ca_certificate);
                                        if (Config.mysql_ssl_default_ca_certificate_password != null) {
                                            sslContextFactory.setTrustStorePassword(Config.mysql_ssl_default_ca_certificate_password);
                                        }
                                    } else {
                                        LOG.warn("Truststore file not found: {}", Config.mysql_ssl_default_ca_certificate);
                                    }
                                }
                                
                                // Configure HTTPS
                                HttpConfiguration httpsConfig = new HttpConfiguration(httpConfiguration);
                                httpsConfig.addCustomizer(new SecureRequestCustomizer());
                                
                                // Create SSL connector
                                ServerConnector sslConnector = new ServerConnector(
                                        server,
                                        new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
                                        new HttpConnectionFactory(httpsConfig));
                                sslConnector.setPort(Config.https_port);
                                
                                server.addConnector(sslConnector);
                                LOG.info("HTTPS connector with client certificate authentication enabled on port {}",
                                         Config.https_port);
                            } catch (Exception e) {
                                LOG.error("Failed to configure HTTPS with client certificate authentication", e);
                            }
                        }
                    }
            );
        }
    }
}
