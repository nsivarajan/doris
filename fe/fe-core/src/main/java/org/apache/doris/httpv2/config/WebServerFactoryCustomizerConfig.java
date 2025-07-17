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

import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
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
        if (Config.enable_https) {
            JettyServletWebServerFactory jettyFactory = (JettyServletWebServerFactory) factory;
            
            jettyFactory.setConfigurations(Collections.singleton(new HttpToHttpsJettyConfig()));
            
            jettyFactory.addServerCustomizers(server -> {
                SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
                
                sslContextFactory.setKeyStorePath(Config.key_store_path);
                sslContextFactory.setKeyStorePassword(Config.key_store_password);
                sslContextFactory.setKeyStoreType("JKS"); // Use Config value if available
                
                if (Config.key_store_alias != null && !Config.key_store_alias.isEmpty()) {
                    sslContextFactory.setCertAlias(Config.key_store_alias);
                }
                
                sslContextFactory.setTrustStorePath(Config.mysql_ssl_default_ca_certificate);
                sslContextFactory.setTrustStorePassword(Config.mysql_ssl_default_ca_certificate_password);
                sslContextFactory.setTrustStoreType(Config.ssl_trust_store_type);
                
                if ("mtls".equalsIgnoreCase(Config.authentication_type)) {
                    sslContextFactory.setWantClientAuth(true);
                    sslContextFactory.setNeedClientAuth(true);
                } else {
                    sslContextFactory.setWantClientAuth(false);
                    sslContextFactory.setNeedClientAuth(false);
                }
                
                HttpConfiguration httpsConfig = new HttpConfiguration();
                httpsConfig.setSecureScheme("https");
                httpsConfig.setSecurePort(Config.https_port);
                
                ServerConnector httpsConnector = new ServerConnector(server,
                        new org.eclipse.jetty.server.SslConnectionFactory(
                                sslContextFactory, "http/1.1"),
                        new HttpConnectionFactory(httpsConfig));
                httpsConnector.setPort(Config.https_port);
                
                HttpConfiguration httpConfig = new HttpConfiguration();
                ServerConnector httpConnector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
                httpConnector.setPort(Config.http_port);
                
                server.setConnectors(new ServerConnector[] { httpConnector, httpsConnector });
            });
        }
    }
}
