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

import java.util.Collections;

import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.springframework.boot.web.embedded.jetty.ConfigurableJettyWebServerFactory;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Configuration;

import org.apache.doris.common.Config;

@Configuration
public class WebServerFactoryCustomizerConfig implements WebServerFactoryCustomizer<ConfigurableJettyWebServerFactory> {
    @Override
    public void customize(ConfigurableJettyWebServerFactory factory) {
        if (Config.enable_https) {
            ((JettyServletWebServerFactory) factory).setConfigurations(
                    Collections.singleton(new HttpToHttpsJettyConfig())
            );

            factory.addServerCustomizers(
                    server -> {
                        HttpConfiguration httpConfiguration = new HttpConfiguration();
                        httpConfiguration.setSecurePort(Config.https_port);
                        httpConfiguration.setSecureScheme("https");

                        server.addLifeCycleListener(new LifeCycle.Listener() {
                            @Override
                            public void lifeCycleStarting(LifeCycle event) {
                                for (org.eclipse.jetty.server.Connector connector : server.getConnectors()) {
                                    if (connector instanceof org.eclipse.jetty.server.ServerConnector) {
                                        // 'sc' is managed by Jetty; no need to close. This is not a resource leak.
                                        org.eclipse.jetty.server.ServerConnector sc =
                                                (org.eclipse.jetty.server.ServerConnector) connector;
                                        for (org.eclipse.jetty.server.ConnectionFactory cf : sc.getConnectionFactories()) {
                                            if (cf instanceof org.eclipse.jetty.server.SslConnectionFactory) {
                                                SslContextFactory.Server sslContextFactory =
                                                        (SslContextFactory.Server) ((org.eclipse.jetty.server.SslConnectionFactory) cf)
                                                                .getSslContextFactory();
                                                // Set truststore if configured
                                                if (Config.trust_store_path != null && !Config.trust_store_path.isEmpty()) {
                                                    sslContextFactory.setTrustStorePath(Config.trust_store_path);
                                                    sslContextFactory.setTrustStorePassword(Config.trust_store_password);
                                                    sslContextFactory.setTrustStoreType(Config.trust_store_type);
                                                }
                                                if ("mtls".equalsIgnoreCase(Config.authentication_type)) {
                                                    sslContextFactory.setNeedClientAuth(true);
                                                } else {
                                                    sslContextFactory.setNeedClientAuth(false);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            @Override
                            public void lifeCycleStarted(LifeCycle event) {}

                            @Override
                            public void lifeCycleFailure(LifeCycle event, Throwable cause) {}

                            @Override
                            public void lifeCycleStopping(LifeCycle event) {}

                            @Override
                            public void lifeCycleStopped(LifeCycle event) {}
                        });

                        ServerConnector connector = new ServerConnector(server);
                        connector.addConnectionFactory(new HttpConnectionFactory(httpConfiguration));
                        connector.setPort(Config.http_port);

                        server.addConnector(connector);
                    }
            );
        }
    }
}
