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

import org.springframework.boot.web.embedded.jetty.ConfigurableJettyWebServerFactory;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.Ssl;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;

@Configuration
public class WebServerFactoryCustomizerConfig implements WebServerFactoryCustomizer<ConfigurableJettyWebServerFactory> {
    @Override
    public void customize(ConfigurableJettyWebServerFactory factory) {
        boolean isMtls = "mtls".equalsIgnoreCase(Config.authentication_type);
        
        // For mTLS authentication, we need to enable HTTPS regardless of enable_https setting
        // because client certificates are exchanged during the SSL/TLS handshake
        if (isMtls) {
            // Log a message to inform the user that HTTPS is being enabled for mTLS
            System.out.println("mTLS authentication requires HTTPS. Enabling HTTPS for the Web UI.");
            
            // Enable HTTPS redirection configuration
            ((JettyServletWebServerFactory) factory).setConfigurations(
                    Collections.singleton(new HttpToHttpsJettyConfig())
            );
            
            // Configure SSL for mTLS
            Ssl ssl = new Ssl();
            
            // Use the key store settings for the server certificate
            ssl.setKeyStore(Config.key_store_path);
            ssl.setKeyStorePassword(Config.key_store_password);
            ssl.setKeyStoreType(Config.key_store_type);
            
            // Enable client authentication for mTLS
            ssl.setClientAuth(Ssl.ClientAuth.NEED);
            
            // Use the MySQL SSL CA certificate for the trust store
            ssl.setTrustStore(Config.mysql_ssl_default_ca_certificate);
            ssl.setTrustStorePassword(Config.mysql_ssl_default_ca_certificate_password);
            ssl.setTrustStoreType(Config.ssl_trust_store_type);
            
            // Enable SSL
            ssl.setEnabled(true);
            
            // Apply SSL configuration to the factory
            factory.setSsl(ssl);
        }
        // For standard HTTPS with client authentication
        else if (Config.enable_https && Config.ssl_force_client_auth) {
            // Configure SSL with client authentication
            Ssl ssl = new Ssl();
            
            // Use the key store settings for the server certificate
            ssl.setKeyStore(Config.key_store_path);
            ssl.setKeyStorePassword(Config.key_store_password);
            ssl.setKeyStoreType(Config.key_store_type);
            
            // Enable client authentication
            ssl.setClientAuth(Ssl.ClientAuth.NEED);
            
            // Use the MySQL SSL CA certificate for the trust store
            ssl.setTrustStore(Config.mysql_ssl_default_ca_certificate);
            ssl.setTrustStorePassword(Config.mysql_ssl_default_ca_certificate_password);
            ssl.setTrustStoreType(Config.ssl_trust_store_type);
            
            // SSL is already enabled by HttpServer.java when enable_https is true
            
            // Apply SSL configuration to the factory
            factory.setSsl(ssl);
        }
        // For standard HTTPS without client authentication, HttpServer.java handles it
    }
}
