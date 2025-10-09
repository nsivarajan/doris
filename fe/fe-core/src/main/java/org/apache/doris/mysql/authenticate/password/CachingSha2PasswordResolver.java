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

package org.apache.doris.mysql.authenticate.password;

import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.MysqlSha2Password;
import org.apache.doris.mysql.authenticate.PasswordCache;
import org.apache.doris.mysql.authenticate.RSAKeyManager;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.interfaces.RSAPublicKey;
import java.util.Optional;

/**
 * Password resolver for caching_sha2_password authentication plugin.
 *
 * This resolver implements the MySQL caching_sha2_password authentication protocol:
 * 1. Initial authentication with SHA-256 scrambled password
 * 2. Fast authentication if password is cached
 * 3. Full authentication if password is not cached:
 *    - Use SSL for password transmission if available
 *    - Use RSA encryption if SSL is not available
 * 4. Cache password hash for future fast authentication
 *
 * This eliminates the security vulnerability of clear text password transmission
 * that exists in the current mysql_native_password implementation.
 */
public class CachingSha2PasswordResolver implements PasswordResolver {
    private static final Logger LOG = LogManager.getLogger(CachingSha2PasswordResolver.class);
    
    // Protocol constants for caching_sha2_password
    private static final byte FAST_AUTH_SUCCESS = 0x03;
    private static final byte FULL_AUTH_REQUIRED = 0x01;
    private static final byte RSA_KEY_REQUEST = 0x02;
    
    // Cache and key management
    private final PasswordCache passwordCache;
    private final RSAKeyManager rsaKeyManager;
    
    /**
     * Constructor with default cache and key manager
     */
    public CachingSha2PasswordResolver() {
        this.passwordCache = new PasswordCache(
            Config.sha2_password_cache_size,
            Config.sha2_password_cache_ttl_seconds
        );
        this.rsaKeyManager = RSAKeyManager.getInstance();
        
        LOG.info("CachingSha2PasswordResolver initialized with cache size: {}, TTL: {}s",
                Config.sha2_password_cache_size, Config.sha2_password_cache_ttl_seconds);
    }
    
    /**
     * Constructor with custom cache and key manager (for testing)
     */
    public CachingSha2PasswordResolver(PasswordCache passwordCache, RSAKeyManager rsaKeyManager) {
        this.passwordCache = passwordCache;
        this.rsaKeyManager = rsaKeyManager;
    }
    
    @Override
    public Optional<Password> resolvePassword(ConnectContext context,
                                            MysqlChannel channel,
                                            MysqlSerializer serializer,
                                            MysqlAuthPacket authPacket,
                                            MysqlHandshakePacket handshakePacket) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Starting caching_sha2_password authentication for user: {}", authPacket.getUser());
        }
        
        try {
            // Phase 1: Initial authentication attempt
            byte[] scrambledPassword = authPacket.getAuthResponse();
            byte[] nonce = handshakePacket.getAuthPluginData();
            String username = authPacket.getUser();
            
            if (scrambledPassword == null || nonce == null || username == null) {
                LOG.warn("Invalid authentication data: scrambled={}, nonce={}, user={}",
                        scrambledPassword != null, nonce != null, username != null);
                ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, username, "YES");
                return Optional.empty();
            }
            
            CachingSha2Password password = new CachingSha2Password(scrambledPassword, nonce);
            
            // Phase 2: Check password cache (Fast Auth)
            if (passwordCache.isPasswordCached(username, scrambledPassword)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Fast authentication successful for user: {}", username);
                }
                
                password.transitionToFastAuthSuccess();
                sendFastAuthSuccess(channel, serializer);
                return Optional.of(password);
            }
            
            // Phase 3: Full authentication required
            if (LOG.isDebugEnabled()) {
                LOG.debug("Full authentication required for user: {}", username);
            }
            
            password.transitionToFullAuth();
            sendFullAuthRequired(channel, serializer);
            
            // Phase 4: Handle full authentication
            return handleFullAuthentication(context, channel, serializer, password, username);
            
        } catch (Exception e) {
            LOG.error("Error during caching_sha2_password authentication", e);
            ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, authPacket.getUser(), "YES");
            return Optional.empty();
        }
    }
    
    /**
     * Handle full authentication process
     */
    private Optional<Password> handleFullAuthentication(ConnectContext context,
                                                      MysqlChannel channel,
                                                      MysqlSerializer serializer,
                                                      CachingSha2Password password,
                                                      String username) throws IOException {
        
        // Check if SSL is available for secure password transmission
        if (channel.isSslMode()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Using SSL for password transmission for user: {}", username);
            }
            return handleSSLPasswordTransmission(channel, password, username);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Using RSA encryption for password transmission for user: {}", username);
            }
            return handleRSAPasswordTransmission(channel, serializer, password, username);
        }
    }
    
    /**
     * Handle password transmission over SSL
     */
    private Optional<Password> handleSSLPasswordTransmission(MysqlChannel channel,
                                                           CachingSha2Password password,
                                                           String username) throws IOException {
        
        // Receive plain text password over SSL connection
        ByteBuffer passwordPacket = channel.fetchOnePacket();
        if (passwordPacket == null) {
            LOG.warn("Failed to receive password packet over SSL for user: {}", username);
            return Optional.empty();
        }
        
        // Read null-terminated password string
        byte[] passwordBytes = MysqlProto.readEofString(passwordPacket);
        String plainTextPassword = new String(passwordBytes, java.nio.charset.StandardCharsets.UTF_8);
        
        // Remove null terminator if present
        if (plainTextPassword.endsWith("\0")) {
            plainTextPassword = plainTextPassword.substring(0, plainTextPassword.length() - 1);
        }
        
        password.setPlainTextPassword(plainTextPassword);
        password.transitionToComplete();
        
        // Cache the password hash for future fast authentication
        try {
            byte[] passwordHash = MysqlSha2Password.makeScrambledPasswordSha256(plainTextPassword);
            passwordCache.cachePassword(username, passwordHash);
            
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cached password hash for user: {}", username);
            }
        } catch (Exception e) {
            LOG.warn("Failed to cache password hash for user: {}", username, e);
        }
        
        return Optional.of(password);
    }
    
    /**
     * Handle password transmission using RSA encryption
     */
    private Optional<Password> handleRSAPasswordTransmission(MysqlChannel channel,
                                                           MysqlSerializer serializer,
                                                           CachingSha2Password password,
                                                           String username) throws IOException {
        
        try {
            // Send RSA public key to client
            RSAPublicKey publicKey = rsaKeyManager.getPublicKey();
            if (publicKey == null) {
                LOG.error("No RSA public key available for user: {}", username);
                ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, username, "YES");
                return Optional.empty();
            }
            
            sendRSAPublicKey(serializer, channel, publicKey);
            password.transitionToRSAExchange(publicKey);
            
            // Receive encrypted password from client
            ByteBuffer encryptedPasswordPacket = channel.fetchOnePacket();
            if (encryptedPasswordPacket == null) {
                LOG.warn("Failed to receive encrypted password packet for user: {}", username);
                return Optional.empty();
            }
            
            byte[] encryptedPassword = MysqlProto.readEofString(encryptedPasswordPacket);
            password.transitionToPasswordEncrypted(encryptedPassword);
            
            // Decrypt password using RSA private key
            String plainTextPassword = rsaKeyManager.decryptPassword(encryptedPassword);
            password.setPlainTextPassword(plainTextPassword);
            password.transitionToComplete();
            
            // Cache the password hash for future fast authentication
            try {
                byte[] passwordHash = MysqlSha2Password.makeScrambledPasswordSha256(plainTextPassword);
                passwordCache.cachePassword(username, passwordHash);
                
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cached password hash for user: {} (RSA decryption)", username);
                }
            } catch (Exception e) {
                LOG.warn("Failed to cache password hash for user: {}", username, e);
            }
            
            return Optional.of(password);
            
        } catch (Exception e) {
            LOG.error("Error during RSA password transmission for user: {}", username, e);
            ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, username, "YES");
            return Optional.empty();
        }
    }
    
    /**
     * Send fast authentication success response
     */
    private void sendFastAuthSuccess(MysqlChannel channel, MysqlSerializer serializer) throws IOException {
        serializer.reset();
        serializer.writeInt1(FAST_AUTH_SUCCESS);
        channel.sendAndFlush(serializer.toByteBuffer());
    }
    
    /**
     * Send full authentication required response
     */
    private void sendFullAuthRequired(MysqlChannel channel, MysqlSerializer serializer) throws IOException {
        serializer.reset();
        serializer.writeInt1(FULL_AUTH_REQUIRED);
        channel.sendAndFlush(serializer.toByteBuffer());
    }
    
    /**
     * Send RSA public key to client
     */
    private void sendRSAPublicKey(MysqlSerializer serializer, MysqlChannel channel, RSAPublicKey publicKey)
            throws IOException {
        try {
            String pemKey = MysqlSha2Password.convertPublicKeyToPEM(publicKey);
            serializer.reset();
            serializer.writeEofString(pemKey);
            channel.sendAndFlush(serializer.toByteBuffer());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sent RSA public key to client (key size: {} bits)", publicKey.getModulus().bitLength());
            }
        } catch (Exception e) {
            LOG.error("Failed to send RSA public key to client", e);
            throw new IOException("Failed to send RSA public key", e);
        }
    }
    /**
     * Get password cache for monitoring
     */
    public PasswordCache getPasswordCache() {
        return passwordCache;
    }
    
    /**
     * Get RSA key manager for monitoring
     */
    public RSAKeyManager getRSAKeyManager() {
        return rsaKeyManager;
    }
    
    /**
     * Get resolver statistics
     */
    public String getStatistics() {
        return String.format("CachingSha2PasswordResolver[%s, %s]",
                passwordCache.getStatistics(),
                rsaKeyManager.getStatistics());
    }
    
    /**
     * Clear password cache (for administrative purposes)
     */
    public void clearCache() {
        passwordCache.clear();
        LOG.info("Password cache cleared by administrator");
    }
    
    /**
     * Remove specific user from cache
     */
    public boolean removeUserFromCache(String username) {
        boolean removed = passwordCache.removeUser(username);
        if (removed) {
            LOG.info("Removed user from password cache: {}", username);
        }
        return removed;
    }
    
    /**
     * Shutdown resolver and cleanup resources
     */
    public void shutdown() {
        LOG.info("Shutting down CachingSha2PasswordResolver");
        
        if (passwordCache != null) {
            passwordCache.shutdown();
        }
        
        // Note: RSAKeyManager is singleton and managed globally
        
        LOG.info("CachingSha2PasswordResolver shutdown complete");
    }
}
