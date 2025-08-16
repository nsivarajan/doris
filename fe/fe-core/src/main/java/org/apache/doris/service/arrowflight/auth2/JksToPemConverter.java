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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Enumeration;

/**
 * Utility class for converting JKS keystore files to PEM format.
 * Arrow Flight requires PEM format certificates, but Doris uses JKS format.
 */
public class JksToPemConverter {
    private static final Logger LOG = LogManager.getLogger(JksToPemConverter.class);
    private static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----\n";
    private static final String END_CERT = "-----END CERTIFICATE-----\n";
    private static final String BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----\n";
    private static final String END_PRIVATE_KEY = "-----END PRIVATE KEY-----\n";

    private JksToPemConverter() {
        // Utility class should not be instantiated
    }

    /**
     * Converts a JKS keystore to PEM format files.
     *
     * @param jksPath the path to the JKS keystore file
     * @param jksPassword the password for the JKS keystore
     * @param alias the alias of the certificate to extract (if null, uses the first entry)
     * @return a File array containing [0] the certificate file and [1] the private key file
     * @throws IOException if an I/O error occurs
     * @throws Exception if any other error occurs
     */
    public static File[] convertJksToPem(String jksPath, char[] jksPassword, String alias) throws Exception {
        LOG.info("Converting JKS to PEM: {}", jksPath);

        // Create temporary files for certificate and private key
        File certFile = File.createTempFile("arrow_flight_cert_", ".pem");
        File keyFile = File.createTempFile("arrow_flight_key_", ".pem");

        // Ensure temporary files are deleted on JVM exit
        certFile.deleteOnExit();
        keyFile.deleteOnExit();

        // Load the keystore
        KeyStore keystore = KeyStore.getInstance("JKS");
        try (java.io.InputStream is = Files.newInputStream(new File(jksPath).toPath())) {
            keystore.load(is, jksPassword);
        }

        // If alias is not specified, use the first entry
        if (alias == null || alias.isEmpty()) {
            Enumeration<String> aliases = keystore.aliases();
            if (aliases.hasMoreElements()) {
                alias = aliases.nextElement();
                LOG.info("Using first alias found in keystore: {}", alias);
            } else {
                throw new IOException("No aliases found in keystore");
            }
        }

        // Check if alias exists
        if (!keystore.containsAlias(alias)) {
            throw new IOException("Alias not found in keystore: " + alias);
        }

        // Extract certificate
        Certificate cert = keystore.getCertificate(alias);
        if (cert == null) {
            throw new IOException("No certificate found for alias: " + alias);
        }

        // Extract private key
        Key privateKey = keystore.getKey(alias, jksPassword);
        if (privateKey == null) {
            throw new IOException("No private key found for alias: " + alias);
        }

        // Write certificate to PEM file
        try (OutputStream os = new FileOutputStream(certFile)) {
            os.write(BEGIN_CERT.getBytes());
            os.write(Base64.getMimeEncoder(64, "\n".getBytes()).encode(cert.getEncoded()));
            os.write("\n".getBytes());
            os.write(END_CERT.getBytes());
        }

        // Write private key to PEM file
        try (OutputStream os = new FileOutputStream(keyFile)) {
            os.write(BEGIN_PRIVATE_KEY.getBytes());
            os.write(Base64.getMimeEncoder(64, "\n".getBytes()).encode(privateKey.getEncoded()));
            os.write("\n".getBytes());
            os.write(END_PRIVATE_KEY.getBytes());
        }

        LOG.info("JKS to PEM conversion complete. Certificate: {}, Private key: {}",
                certFile.getAbsolutePath(), keyFile.getAbsolutePath());

        return new File[] { certFile, keyFile };
    }

    /**
     * Converts a JKS truststore to a PEM format file.
     *
     * @param jksPath the path to the JKS truststore file
     * @param jksPassword the password for the JKS truststore
     * @return the PEM format truststore file
     * @throws IOException if an I/O error occurs
     * @throws Exception if any other error occurs
     */
    public static File convertTruststoreToPem(String jksPath, char[] jksPassword) throws Exception {
        LOG.info("Converting truststore JKS to PEM: {}", jksPath);

        // Create temporary file for truststore
        File trustFile = File.createTempFile("arrow_flight_trust_", ".pem");

        // Ensure temporary file is deleted on JVM exit
        trustFile.deleteOnExit();

        // Load the truststore
        KeyStore truststore = KeyStore.getInstance("JKS");
        try (java.io.InputStream is = Files.newInputStream(new File(jksPath).toPath())) {
            truststore.load(is, jksPassword);
        }

        // Write all certificates to PEM file
        try (OutputStream os = new FileOutputStream(trustFile)) {
            Enumeration<String> aliases = truststore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                Certificate cert = truststore.getCertificate(alias);
                if (cert instanceof X509Certificate) {
                    os.write(BEGIN_CERT.getBytes());
                    os.write(Base64.getMimeEncoder(64, "\n".getBytes()).encode(cert.getEncoded()));
                    os.write("\n".getBytes());
                    os.write(END_CERT.getBytes());
                }
            }
        }

        LOG.info("Truststore JKS to PEM conversion complete. Truststore: {}", trustFile.getAbsolutePath());

        return trustFile;
    }
}
