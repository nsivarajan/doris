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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.common.Config;
import org.apache.doris.mysql.MysqlSha2Password;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.KeyPair;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * RSA key manager for caching_sha2_password authentication.
 *
 * This class manages RSA key pairs used for secure password transmission
 * when SSL is not available. It provides:
 * - Automatic key generation and rotation
 * - Thread-safe key access
 * - Configurable key size and rotation interval
 * - Secure key storage and cleanup
 */
public class RSAKeyManager {
    private static final Logger LOG = LogManager.getLogger(RSAKeyManager.class);

    // Key rotation configuration
    private static final long DEFAULT_KEY_ROTATION_HOURS = 24; // Rotate keys daily
    private static final long MIN_KEY_ROTATION_HOURS = 1;
    private static final long MAX_KEY_ROTATION_HOURS = 168; // 1 week

    // Thread configuration
    private static final String KEY_ROTATION_THREAD_NAME = "rsa-key-rotation";

    // Current key pair
    private volatile RSAPublicKey currentPublicKey;
    private volatile RSAPrivateKey currentPrivateKey;
    private volatile long keyGenerationTime;

    // Key rotation
    private final ScheduledExecutorService keyRotationExecutor;
    private final long keyRotationIntervalHours;
    private final int keySize;

    // Thread safety
    private final ReentrantReadWriteLock keyLock = new ReentrantReadWriteLock();

    // Singleton instance
    private static volatile RSAKeyManager instance;
    private static final Object instanceLock = new Object();

    /**
     * Private constructor for singleton pattern
     */
    private RSAKeyManager() {
        this.keySize = Config.sha2_password_rsa_key_length;
        this.keyRotationIntervalHours = DEFAULT_KEY_ROTATION_HOURS;

        // Create key rotation executor with daemon thread
        this.keyRotationExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, KEY_ROTATION_THREAD_NAME);
            t.setDaemon(true);
            return t;
        });

        // Generate initial key pair
        generateNewKeyPair();

        // Schedule periodic key rotation
        scheduleKeyRotation();

        LOG.info("RSA key manager initialized: keySize={}, rotationInterval={}h",
                keySize, keyRotationIntervalHours);
    }

    /**
     * Get singleton instance of RSA key manager
     *
     * @return RSA key manager instance
     */
    public static RSAKeyManager getInstance() {
        if (instance == null) {
            synchronized (instanceLock) {
                if (instance == null) {
                    instance = new RSAKeyManager();
                }
            }
        }
        return instance;
    }

    /**
     * Get current RSA public key for encryption
     *
     * @return Current RSA public key
     */
    public RSAPublicKey getPublicKey() {
        keyLock.readLock().lock();
        try {
            return currentPublicKey;
        } finally {
            keyLock.readLock().unlock();
        }
    }

    /**
     * Get current RSA private key for decryption
     *
     * @return Current RSA private key
     */
    public RSAPrivateKey getPrivateKey() {
        keyLock.readLock().lock();
        try {
            return currentPrivateKey;
        } finally {
            keyLock.readLock().unlock();
        }
    }

    /**
     * Get public key in PEM format for transmission to client
     *
     * @return PEM-formatted public key
     */
    public String getPublicKeyPEM() {
        RSAPublicKey publicKey = getPublicKey();
        if (publicKey == null) {
            LOG.error("No RSA public key available");
            return "";
        }

        try {
            return MysqlSha2Password.convertPublicKeyToPEM(publicKey);
        } catch (Exception e) {
            LOG.error("Failed to convert public key to PEM format", e);
            return "";
        }
    }

    /**
     * Get key generation time
     *
     * @return Timestamp when current key was generated
     */
    public long getKeyGenerationTime() {
        keyLock.readLock().lock();
        try {
            return keyGenerationTime;
        } finally {
            keyLock.readLock().unlock();
        }
    }

    /**
     * Get key age in milliseconds
     *
     * @return Age of current key in milliseconds
     */
    public long getKeyAge() {
        return System.currentTimeMillis() - getKeyGenerationTime();
    }

    /**
     * Get key size in bits
     *
     * @return RSA key size in bits
     */
    public int getKeySize() {
        return keySize;
    }

    /**
     * Check if key rotation is due
     *
     * @return true if key should be rotated
     */
    public boolean isKeyRotationDue() {
        long keyAgeHours = getKeyAge() / (1000 * 60 * 60);
        return keyAgeHours >= keyRotationIntervalHours;
    }

    /**
     * Manually trigger key rotation
     *
     * @return true if key rotation was successful
     */
    public boolean rotateKeys() {
        LOG.info("Manually triggering RSA key rotation");
        return generateNewKeyPair();
    }

    /**
     * Encrypt password using current public key
     *
     * @param password Plain text password to encrypt
     * @return Encrypted password bytes
     * @throws Exception if encryption fails
     */
    public byte[] encryptPassword(String password) throws Exception {
        RSAPublicKey publicKey = getPublicKey();
        if (publicKey == null) {
            throw new IllegalStateException("No RSA public key available for encryption");
        }

        return MysqlSha2Password.encryptPasswordWithRSA(password, publicKey);
    }

    /**
     * Decrypt password using current private key
     *
     * @param encryptedPassword Encrypted password bytes
     * @return Decrypted plain text password
     * @throws Exception if decryption fails
     */
    public String decryptPassword(byte[] encryptedPassword) throws Exception {
        RSAPrivateKey privateKey = getPrivateKey();
        if (privateKey == null) {
            throw new IllegalStateException("No RSA private key available for decryption");
        }

        return MysqlSha2Password.decryptPasswordWithRSA(encryptedPassword, privateKey);
    }

    /**
     * Shutdown key manager and cleanup resources
     */
    public void shutdown() {
        LOG.info("Shutting down RSA key manager");

        // Shutdown key rotation executor
        keyRotationExecutor.shutdown();
        try {
            if (!keyRotationExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                keyRotationExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            keyRotationExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Clear keys
        keyLock.writeLock().lock();
        try {
            currentPublicKey = null;
            currentPrivateKey = null;
            keyGenerationTime = 0;
        } finally {
            keyLock.writeLock().unlock();
        }

        LOG.info("RSA key manager shutdown complete");
    }

    /**
     * Generate new RSA key pair
     *
     * @return true if key generation was successful
     */
    private boolean generateNewKeyPair() {
        try {
            LOG.info("Generating new RSA key pair (size: {} bits)", keySize);
            long startTime = System.currentTimeMillis();
            KeyPair keyPair = MysqlSha2Password.generateRSAKeyPair(keySize);


            keyLock.writeLock().lock();
            try {
                // Clear old keys (if any)
                currentPublicKey = null;
                currentPrivateKey = null;

                // Set new keys
                currentPublicKey = (RSAPublicKey) keyPair.getPublic();
                currentPrivateKey = (RSAPrivateKey) keyPair.getPrivate();
                keyGenerationTime = System.currentTimeMillis();
            } finally {
                keyLock.writeLock().unlock();
            }
            long duration = System.currentTimeMillis() - startTime;
            LOG.info("RSA key pair generated successfully in {}ms", duration);

            return true;


        } catch (Exception e) {
            LOG.error("Failed to generate RSA key pair", e);
            return false;
        }
    }

    /**
     * Schedule periodic key rotation
     */
    private void scheduleKeyRotation() {
        keyRotationExecutor.scheduleAtFixedRate(
                this::performScheduledKeyRotation,
                keyRotationIntervalHours,
                keyRotationIntervalHours,
                TimeUnit.HOURS
        );

        LOG.info("Scheduled RSA key rotation every {} hours", keyRotationIntervalHours);
    }

    /**
     * Perform scheduled key rotation
     */
    private void performScheduledKeyRotation() {
        try {
            LOG.info("Performing scheduled RSA key rotation");

            if (generateNewKeyPair()) {
                LOG.info("Scheduled RSA key rotation completed successfully");
            } else {
                LOG.error("Scheduled RSA key rotation failed");
            }

        } catch (Exception e) {
            LOG.error("Error during scheduled RSA key rotation", e);
        }
    }

    /**
     * Get key manager statistics
     *
     * @return Key manager statistics as formatted string
     */
    public String getStatistics() {
        long keyAgeHours = getKeyAge() / (1000 * 60 * 60);
        return String.format(
            "RSAKeyManager[keySize=%d, keyAge=%dh, rotationInterval=%dh, rotationDue=%s]",
            keySize, keyAgeHours, keyRotationIntervalHours, isKeyRotationDue()
        );
    }

    /**
     * Validate key manager configuration
     *
     * @return true if configuration is valid
     */
    public boolean isConfigurationValid() {
        return keySize >= 1024 && keySize <= 4096
            && keyRotationIntervalHours >= MIN_KEY_ROTATION_HOURS
            && keyRotationIntervalHours <= MAX_KEY_ROTATION_HOURS;
    }
}
