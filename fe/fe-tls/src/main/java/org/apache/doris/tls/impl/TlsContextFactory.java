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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Builds SSLContext from PEM files configured via enable_tls config block:
 *   tls_certificate_path     — server/client cert (PEM)
 *   tls_private_key_path     — private key (PEM, PKCS#8)
 *   tls_private_key_password — key password (may be empty)
 *   tls_ca_certificate_path  — CA cert for client verification (PEM, optional)
 *
 * Reused by all three SPI implementations (HTTP, MySQL, Thrift/Flight).
 */
public final class TlsContextFactory {

    private static final Logger LOG = LogManager.getLogger(TlsContextFactory.class);

    private TlsContextFactory() {
    }

    /**
     * Builds an SSLContext from the tls_* config properties.
     * Loads server cert + private key into a KeyStore; optionally loads the CA cert
     * for client certificate verification into a TrustStore.
     */
    public static SSLContext buildSslContext() throws Exception {
        String certPath    = Config.tls_certificate_path;
        String keyPath     = Config.tls_private_key_path;
        String keyPassword = Config.tls_private_key_password;
        String caPath      = Config.tls_ca_certificate_path;

        if (certPath == null || certPath.isBlank()) {
            throw new IllegalStateException("tls_certificate_path is required when enable_tls=true");
        }
        if (keyPath == null || keyPath.isBlank()) {
            throw new IllegalStateException("tls_private_key_path is required when enable_tls=true");
        }

        char[] keyPass = keyPassword == null ? new char[0] : keyPassword.toCharArray();

        // Load server certificate chain
        List<X509Certificate> certChain = loadCertChain(certPath);
        if (certChain.isEmpty()) {
            throw new IllegalStateException("No certificates found in " + certPath);
        }

        // Load private key
        PrivateKey privateKey = PemLoader.loadPrivateKey(keyPath, keyPass);

        // Build KeyStore with server cert + key
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        keyStore.setKeyEntry("server",
                privateKey, keyPass,
                certChain.toArray(new java.security.cert.Certificate[0]));

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPass);

        // Build TrustStore — if CA path provided, use it; otherwise trust JVM default CAs
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        if (caPath != null && !caPath.isBlank()) {
            KeyStore trustStore = KeyStore.getInstance("JKS");
            trustStore.load(null, null);
            List<X509Certificate> caCerts = loadCertChain(caPath);
            for (int i = 0; i < caCerts.size(); i++) {
                trustStore.setCertificateEntry("ca-" + i, caCerts.get(i));
            }
            tmf.init(trustStore);
            LOG.info("TLS: loaded CA certificate from {}", caPath);
        } else {
            tmf.init((KeyStore) null); // JVM default truststore
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        LOG.info("TLS: SSLContext built from cert={} key={}", certPath, keyPath);
        return sslContext;
    }

    private static List<X509Certificate> loadCertChain(String path)
            throws IOException, CertificateException {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        List<X509Certificate> certs = new ArrayList<>();
        try (InputStream is = Files.newInputStream(Paths.get(path))) {
            for (java.security.cert.Certificate c : cf.generateCertificates(is)) {
                certs.add((X509Certificate) c);
            }
        }
        return certs;
    }
}
