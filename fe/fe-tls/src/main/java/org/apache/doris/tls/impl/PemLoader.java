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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/**
 * Loads PEM-encoded private keys (PKCS#8 format, with or without passphrase).
 * Supports -----BEGIN PRIVATE KEY----- (unencrypted PKCS#8) and
 * -----BEGIN ENCRYPTED PRIVATE KEY----- (encrypted PKCS#8).
 */
public final class PemLoader {

    private PemLoader() {
    }

    public static PrivateKey loadPrivateKey(String path, char[] password) throws Exception {
        String pem = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
        return parsePrivateKey(pem, password);
    }

    static PrivateKey parsePrivateKey(String pem, char[] password) throws Exception {
        pem = pem.trim();
        if (pem.contains("BEGIN ENCRYPTED PRIVATE KEY")) {
            return loadEncryptedPkcs8(pem, password);
        } else if (pem.contains("BEGIN PRIVATE KEY")) {
            return loadUnencryptedPkcs8(pem);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported private key format. Expected PKCS#8 PEM "
                    + "(BEGIN PRIVATE KEY or BEGIN ENCRYPTED PRIVATE KEY). "
                    + "Convert with: openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-pkcs8.pem");
        }
    }

    private static PrivateKey loadUnencryptedPkcs8(String pem) throws Exception {
        String b64 = pem
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s+", "");
        byte[] der = Base64.getDecoder().decode(b64);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        try {
            return kf.generatePrivate(new PKCS8EncodedKeySpec(der));
        } catch (Exception e) {
            // Try EC key
            kf = KeyFactory.getInstance("EC");
            return kf.generatePrivate(new PKCS8EncodedKeySpec(der));
        }
    }

    private static PrivateKey loadEncryptedPkcs8(String pem, char[] password) throws Exception {
        // Use BouncyCastle-compatible approach via JCE PKCS#8 EncryptedPrivateKeyInfo
        String b64 = pem
                .replace("-----BEGIN ENCRYPTED PRIVATE KEY-----", "")
                .replace("-----END ENCRYPTED PRIVATE KEY-----", "")
                .replaceAll("\\s+", "");
        byte[] encryptedDer = Base64.getDecoder().decode(b64);

        javax.crypto.EncryptedPrivateKeyInfo epki =
                new javax.crypto.EncryptedPrivateKeyInfo(encryptedDer);
        javax.crypto.SecretKeyFactory skf =
                javax.crypto.SecretKeyFactory.getInstance(epki.getAlgName());
        javax.crypto.spec.PBEKeySpec pbeKeySpec =
                new javax.crypto.spec.PBEKeySpec(password);
        javax.crypto.SecretKey pbeKey = skf.generateSecret(pbeKeySpec);

        javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance(epki.getAlgName());
        cipher.init(javax.crypto.Cipher.DECRYPT_MODE, pbeKey, epki.getAlgParameters());

        byte[] der = epki.getKeySpec(cipher).getEncoded();
        KeyFactory kf = KeyFactory.getInstance("RSA");
        try {
            return kf.generatePrivate(new PKCS8EncodedKeySpec(der));
        } catch (Exception e) {
            kf = KeyFactory.getInstance("EC");
            return kf.generatePrivate(new PKCS8EncodedKeySpec(der));
        }
    }
}
