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

package org.apache.doris.tls.impl;

import org.apache.doris.common.Config;
import org.apache.doris.tls.server.HttpServerTlsProvider;
import org.apache.doris.tls.server.TlsProtocolSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.springframework.boot.web.embedded.jetty.ConfigurableJettyWebServerFactory;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;

import javax.net.ssl.SSLContext;

/**
 * Provides HTTPS on {@code Config.https_port} when {@code enable_tls=true} and
 * HTTP is not in {@code tls_excluded_protocols}.
 *
 * Builds SSLContext from PEM files (tls_certificate_path / tls_private_key_path /
 * tls_ca_certificate_path). Respects tls_verify_mode for optional client-cert auth.
 */
public class TlsHttpServerProvider implements HttpServerTlsProvider {

    private static final Logger LOG = LogManager.getLogger(TlsHttpServerProvider.class);

    @Override
    public void customize(ConfigurableJettyWebServerFactory factory) {
        if (!Config.enable_tls) {
            return;
        }
        if (!TlsProtocolSet.isProtocolIncluded(TlsProtocolSet.Protocol.HTTP)) {
            LOG.info("TLS: HTTP excluded from tls_excluded_protocols — skipping HTTPS connector");
            return;
        }

        try {
            SSLContext sslContext = TlsContextFactory.buildSslContext();

            SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
            sslContextFactory.setSslContext(sslContext);

            // Client certificate policy
            String verifyMode = Config.tls_verify_mode;
            if ("verify_peer".equals(verifyMode)) {
                sslContextFactory.setWantClientAuth(true);
            } else if ("verify_fail_if_no_peer_cert".equals(verifyMode)) {
                sslContextFactory.setNeedClientAuth(true);
            }

            ((JettyServletWebServerFactory) factory).addServerCustomizers(server -> {
                HttpConfiguration httpsConfig = new HttpConfiguration();
                httpsConfig.setSecurePort(Config.https_port);
                httpsConfig.setSecureScheme("https");
                httpsConfig.addCustomizer(new SecureRequestCustomizer());

                ServerConnector httpsConnector = new ServerConnector(server,
                        new SslConnectionFactory(sslContextFactory, "http/1.1"),
                        new HttpConnectionFactory(httpsConfig));
                httpsConnector.setPort(Config.https_port);
                server.addConnector(httpsConnector);

                // Also configure existing HTTP connector to redirect to HTTPS
                for (org.eclipse.jetty.server.Connector connector : server.getConnectors()) {
                    if (connector instanceof ServerConnector) {
                        HttpConnectionFactory hcf =
                                ((ServerConnector) connector).getConnectionFactory(HttpConnectionFactory.class);
                        if (hcf != null) {
                            hcf.getHttpConfiguration().setSecurePort(Config.https_port);
                            hcf.getHttpConfiguration().setSecureScheme("https");
                        }
                    }
                }

                LOG.info("TLS: HTTPS connector started on port {}, verify_mode={}",
                        Config.https_port, Config.tls_verify_mode);
            });

        } catch (Exception e) {
            throw new RuntimeException("Failed to configure HTTPS connector: " + e.getMessage(), e);
        }
    }
}
