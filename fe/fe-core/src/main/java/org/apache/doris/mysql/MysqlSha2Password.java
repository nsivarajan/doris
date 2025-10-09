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

package org.apache.doris.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import javax.crypto.Cipher;

/**
 * SHA-256 based password utilities for caching_sha2_password authentication plugin.
 *
 * This class implements the cryptographic functions required for MySQL's
 * caching_sha2_password authentication method, including:
 * - SHA-256 password hashing and scrambling
 * - RSA encryption/decryption for secure password transmission
 * - Password verification and validation
 *
 * The implementation follows MySQL's caching_sha2_password specification:
 * https://dev.mysql.com/doc/internals/en/caching-sha2-authentication-plugin.html
 */
public class MysqlSha2Password {
    private static final Logger LOG = LogManager.getLogger(MysqlSha2Password.class);

    // Cryptographic algorithm constants
    private static final String SHA256_ALGORITHM = "SHA-256";
    private static final String RSA_ALGORITHM = "RSA";
    private static final String RSA_CIPHER_ALGORITHM = "RSA/ECB/OAEPWithSHA-1AndMGF1Padding";

    // RSA key configuration
    private static final int DEFAULT_RSA_KEY_SIZE = 2048;
    private static final int MIN_RSA_KEY_SIZE = 1024;
    private static final int MAX_RSA_KEY_SIZE = 4096;

    // Password constants
    public static final byte[] EMPTY_PASSWORD = new byte[0];
    public static final int SHA256_HASH_LENGTH = 32; // SHA-256 produces 32-byte hash

    // PEM format constants
    private static final String PEM_PUBLIC_KEY_HEADER = "-----BEGIN PUBLIC KEY-----";
    private static final String PEM_PUBLIC_KEY_FOOTER = "-----END PUBLIC KEY-----";
    private static final String LINE_SEPARATOR = System.lineSeparator();

    /**
     * Scramble password using SHA-256 algorithm (MySQL caching_sha2_password method)
     *
     * The scrambling process follows MySQL's specification:
     * 1. SHA256(password) -> stage1_hash
     * 2. SHA256(stage1_hash) -> stage2_hash
     * 3. SHA256(nonce + stage2_hash) -> stage3_hash
     * 4. XOR(stage1_hash, stage3_hash) -> scrambled_password
     *
     * @param password Plain text password bytes
     * @param nonce Random nonce from server handshake
     * @return Scrambled password bytes
     * @throws NoSuchAlgorithmException if SHA-256 algorithm is not available
     */
    public static byte[] scrambleSha256(byte[] password, byte[] nonce) throws NoSuchAlgorithmException {
        if (password == null || password.length == 0) {
            return EMPTY_PASSWORD;
        }

        if (nonce == null || nonce.length == 0) {
            throw new IllegalArgumentException("Nonce cannot be null or empty");
        }

        MessageDigest sha256 = MessageDigest.getInstance(SHA256_ALGORITHM);

        // Stage 1: SHA256(password)
        byte[] stage1Hash = sha256.digest(password);

        // Stage 2: SHA256(stage1Hash)
        sha256.reset();
        byte[] stage2Hash = sha256.digest(stage1Hash);

        // Stage 3: SHA256(nonce + stage2Hash)
        sha256.reset();
        sha256.update(nonce);
        sha256.update(stage2Hash);
        byte[] stage3Hash = sha256.digest();

        // Final: XOR(stage1Hash, stage3Hash)
        return xorBytes(stage1Hash, stage3Hash);
    }

    /**
     * Verify scrambled password against stored password hash
     *
     * This reverses the scrambling process to verify the password:
     * 1. SHA256(nonce + stored_hash) -> stage3_hash
     * 2. XOR(scrambled_password, stage3_hash) -> stage1_hash
     * 3. SHA256(stage1_hash) -> computed_stage2_hash
     * 4. Compare computed_stage2_hash with stored_hash
     *
     * @param scrambledPassword Scrambled password from client
     * @param nonce Random nonce from server handshake
     * @param storedPasswordHash Stored SHA-256 password hash
     * @return true if password is valid, false otherwise
     * @throws NoSuchAlgorithmException if SHA-256 algorithm is not available
     */
    public static boolean checkScrambleSha256(
            byte[] scrambledPassword, byte[] nonce, byte[] storedPasswordHash) throws NoSuchAlgorithmException {

        if (scrambledPassword == null || nonce == null || storedPasswordHash == null) {
            return false;
        }

        if (scrambledPassword.length == 0 && storedPasswordHash.length == 0) {
            return true; // Both empty passwords
        }

        if (scrambledPassword.length != SHA256_HASH_LENGTH || storedPasswordHash.length != SHA256_HASH_LENGTH) {
            return false; // Invalid hash lengths
        }

        MessageDigest sha256 = MessageDigest.getInstance(SHA256_ALGORITHM);

        // Reverse the scrambling process
        sha256.update(nonce);
        sha256.update(storedPasswordHash);
        byte[] stage3Hash = sha256.digest();

        // XOR to get original stage1Hash
        byte[] stage1Hash = xorBytes(scrambledPassword, stage3Hash);

        // Verify by computing SHA256(stage1Hash) and comparing with stored hash
        sha256.reset();
        byte[] computedStage2Hash = sha256.digest(stage1Hash);

        return MessageDigest.isEqual(computedStage2Hash, storedPasswordHash);
    }

    /**
     * Create SHA-256 password hash for storage (equivalent to MySQL's PASSWORD() function)
     *
     * This creates the hash that gets stored in the user table:
     * SHA256(SHA256(password))
     *
     * @param plainPassword Plain text password
     * @return SHA-256 password hash for storage
     * @throws NoSuchAlgorithmException if SHA-256 algorithm is not available
     */
    public static byte[] makeScrambledPasswordSha256(String plainPassword) throws NoSuchAlgorithmException {
        if (plainPassword == null || plainPassword.isEmpty()) {
            return EMPTY_PASSWORD;
        }

        MessageDigest sha256 = MessageDigest.getInstance(SHA256_ALGORITHM);
        byte[] passwordBytes = plainPassword.getBytes(StandardCharsets.UTF_8);

        // Double SHA-256 hash (similar to mysql_native_password but with SHA-256)
        byte[] stage1Hash = sha256.digest(passwordBytes);
        sha256.reset();
        return sha256.digest(stage1Hash);
    }

    /**
     * Encrypt password using RSA public key for secure transmission
     *
     * @param password Plain text password to encrypt
     * @param publicKey RSA public key for encryption
     * @return Encrypted password bytes
     * @throws Exception if encryption fails
     */
    public static byte[] encryptPasswordWithRSA(String password, RSAPublicKey publicKey) throws Exception {
        if (password == null) {
            password = "";
        }

        if (publicKey == null) {
            throw new IllegalArgumentException("RSA public key cannot be null");
        }

        // Add null terminator as required by MySQL protocol
        byte[] passwordBytes = (password + "\0").getBytes(StandardCharsets.UTF_8);

        Cipher cipher = Cipher.getInstance(RSA_CIPHER_ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);

        return cipher.doFinal(passwordBytes);
    }

    /**
     * Decrypt password using RSA private key
     *
     * @param encryptedPassword Encrypted password bytes
     * @param privateKey RSA private key for decryption
     * @return Decrypted plain text password
     * @throws Exception if decryption fails
     */
    public static String decryptPasswordWithRSA(byte[] encryptedPassword, RSAPrivateKey privateKey) throws Exception {
        if (encryptedPassword == null || encryptedPassword.length == 0) {
            return "";
        }

        if (privateKey == null) {
            throw new IllegalArgumentException("RSA private key cannot be null");
        }
    
        Cipher cipher = Cipher.getInstance(RSA_CIPHER_ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, privateKey);

        byte[] decryptedBytes = cipher.doFinal(encryptedPassword);
        String decryptedPassword = new String(decryptedBytes, StandardCharsets.UTF_8);

        // Remove null terminator if present
        if (decryptedPassword.endsWith("\0")) {
            decryptedPassword = decryptedPassword.substring(0, decryptedPassword.length() - 1);
        }

        return decryptedPassword;
    }

    /**
     * Generate RSA key pair for password encryption
     *
     * @param keySize RSA key size in bits (must be between 1024 and 4096)
     * @return Generated RSA key pair
     * @throws Exception if key generation fails
     */
    public static KeyPair generateRSAKeyPair(int keySize) throws Exception {
        if (keySize < MIN_RSA_KEY_SIZE || keySize > MAX_RSA_KEY_SIZE) {
            throw new IllegalArgumentException(
                String.format("RSA key size must be between %d and %d bits", MIN_RSA_KEY_SIZE, MAX_RSA_KEY_SIZE));
        }

        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(RSA_ALGORITHM);
        keyPairGenerator.initialize(keySize, new SecureRandom());

        return keyPairGenerator.generateKeyPair();
    }

    /**
     * Generate RSA key pair with default key size
     *
     * @return Generated RSA key pair with default size
     * @throws Exception if key generation fails
     */
    public static KeyPair generateRSAKeyPair() throws Exception {
        return generateRSAKeyPair(DEFAULT_RSA_KEY_SIZE);
    }

    /**
     * Convert RSA public key to PEM format for transmission to client
     *
     * @param publicKey RSA public key to convert
     * @return PEM-formatted public key string
     */
    public static String convertPublicKeyToPEM(RSAPublicKey publicKey) {
        if (publicKey == null) {
            throw new IllegalArgumentException("Public key cannot be null");
        }

        byte[] encoded = publicKey.getEncoded();
        String base64Encoded = Base64.getEncoder().encodeToString(encoded);

        StringBuilder pemBuilder = new StringBuilder();
        pemBuilder.append(PEM_PUBLIC_KEY_HEADER).append(LINE_SEPARATOR);

        // Split base64 string into 64-character lines
        int index = 0;
        while (index < base64Encoded.length()) {
            int endIndex = Math.min(index + 64, base64Encoded.length());
            pemBuilder.append(base64Encoded, index, endIndex).append(LINE_SEPARATOR);
            index = endIndex;
        }

        pemBuilder.append(PEM_PUBLIC_KEY_FOOTER).append(LINE_SEPARATOR);

        return pemBuilder.toString();
    }

    /**
     * Validate password strength (basic validation)
     *
     * @param password Password to validate
     * @return true if password meets basic requirements
     */
    public static boolean isPasswordValid(String password) {
        if (password == null) {
            return false;
        }

        // Allow empty passwords for compatibility
        if (password.isEmpty()) {
            return true;
        }

        // Basic validation - can be enhanced based on requirements
        return password.length() <= 256; // MySQL password length limit
    }

    /**
     * XOR two byte arrays of equal length
     *
     * @param a First byte array
     * @param b Second byte array
     * @return XOR result
     * @throws IllegalArgumentException if arrays have different lengths
     */
    private static byte[] xorBytes(byte[] a, byte[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException(
                String.format("Byte arrays must have the same length: %d != %d", a.length, b.length));
        }

        byte[] result = new byte[a.length];
        for (int i = 0; i < a.length; i++) {
            result[i] = (byte) (a[i] ^ b[i]);
        }
        return result;
    }

    /**
     * Secure comparison of two byte arrays (constant time to prevent timing attacks)
     *
     * @param a First byte array
     * @param b Second byte array
     * @return true if arrays are equal
     */
    public static boolean secureEquals(byte[] a, byte[] b) {
        if (a == null || b == null) {
            return a == b;
        }

        if (a.length != b.length) {
            return false;
        }

        int result = 0;
        for (int i = 0; i < a.length; i++) {
            result |= a[i] ^ b[i];
        }

        return result == 0;
    }

    /**
     * Clear sensitive byte array by filling with zeros
     *
     * @param sensitiveData Byte array to clear
     */
    public static void clearSensitiveData(byte[] sensitiveData) {
        if (sensitiveData != null) {
            for (int i = 0; i < sensitiveData.length; i++) {
                sensitiveData[i] = 0;
            }
        }
    }

    /**
     * Generate cryptographically secure random bytes
     *
     * @param length Number of bytes to generate
     * @return Random byte array
     */
    public static byte[] generateSecureRandomBytes(int length) {
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative");
        }

        byte[] randomBytes = new byte[length];
        new SecureRandom().nextBytes(randomBytes);
        return randomBytes;
    }
}
