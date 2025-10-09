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

import java.security.interfaces.RSAPublicKey;
import java.util.Arrays;

/**
 * Password implementation for caching_sha2_password authentication plugin.
 * 
 * This class manages the multi-phase authentication process for MySQL's
 * caching_sha2_password plugin, which uses SHA-256 hashing and supports
 * both fast authentication (cached passwords) and full authentication
 * (RSA encryption or SSL).
 */
public class CachingSha2Password implements Password {
    
    /**
     * Authentication phases for caching_sha2_password protocol
     */
    public enum AuthPhase {
        /** Initial authentication attempt with scrambled password */
        INITIAL_AUTH,
        
        /** Fast authentication successful (password found in cache) */
        FAST_AUTH_SUCCESS,
        
        /** Full authentication required (password not in cache) */
        FULL_AUTH_REQUIRED,
        
        /** RSA key exchange phase (no SSL available) */
        RSA_KEY_EXCHANGE,
        
        /** Password encrypted and transmitted */
        PASSWORD_ENCRYPTED,
        
        /** Authentication process complete */
        AUTH_COMPLETE
    }
    
    private final byte[] scrambledPassword;
    private final byte[] nonce;
    private AuthPhase currentPhase;
    private RSAPublicKey serverPublicKey;
    private byte[] encryptedPassword;
    private String plainTextPassword;
    
    /**
     * Constructor for initial authentication phase
     * 
     * @param scrambledPassword SHA-256 scrambled password from client
     * @param nonce Random nonce from server handshake
     */
    public CachingSha2Password(byte[] scrambledPassword, byte[] nonce) {
        this.scrambledPassword = scrambledPassword != null ? scrambledPassword.clone() : new byte[0];
        this.nonce = nonce != null ? nonce.clone() : new byte[0];
        this.currentPhase = AuthPhase.INITIAL_AUTH;
    }
    
    /**
     * Constructor for full authentication with plain text password
     * 
     * @param scrambledPassword SHA-256 scrambled password from client
     * @param nonce Random nonce from server handshake
     * @param plainTextPassword Plain text password for full authentication
     */
    public CachingSha2Password(byte[] scrambledPassword, byte[] nonce, String plainTextPassword) {
        this(scrambledPassword, nonce);
        this.plainTextPassword = plainTextPassword;
    }
    
    /**
     * Get the scrambled password received from client
     * 
     * @return Copy of scrambled password bytes
     */
    public byte[] getScrambledPassword() {
        return scrambledPassword.clone();
    }
    
    /**
     * Get the nonce (random string) from server handshake
     * 
     * @return Copy of nonce bytes
     */
    public byte[] getNonce() {
        return nonce.clone();
    }
    
    /**
     * Get current authentication phase
     * 
     * @return Current authentication phase
     */
    public AuthPhase getCurrentPhase() {
        return currentPhase;
    }
    
    /**
     * Transition to fast authentication success phase
     */
    public void transitionToFastAuthSuccess() {
        this.currentPhase = AuthPhase.FAST_AUTH_SUCCESS;
    }
    
    /**
     * Transition to full authentication required phase
     */
    public void transitionToFullAuth() {
        this.currentPhase = AuthPhase.FULL_AUTH_REQUIRED;
    }
    
    /**
     * Transition to RSA key exchange phase
     * 
     * @param publicKey RSA public key for encryption
     */
    public void transitionToRSAExchange(RSAPublicKey publicKey) {
        this.serverPublicKey = publicKey;
        this.currentPhase = AuthPhase.RSA_KEY_EXCHANGE;
    }
    
    /**
     * Transition to password encrypted phase
     * 
     * @param encryptedPassword Encrypted password bytes
     */
    public void transitionToPasswordEncrypted(byte[] encryptedPassword) {
        this.encryptedPassword = encryptedPassword != null ? encryptedPassword.clone() : new byte[0];
        this.currentPhase = AuthPhase.PASSWORD_ENCRYPTED;
    }
    
    /**
     * Transition to authentication complete phase
     */
    public void transitionToComplete() {
        this.currentPhase = AuthPhase.AUTH_COMPLETE;
    }
    
    /**
     * Check if authentication is in initial phase
     * 
     * @return true if in initial authentication phase
     */
    public boolean isInitialAuth() {
        return currentPhase == AuthPhase.INITIAL_AUTH;
    }
    
    /**
     * Check if fast authentication was successful
     * 
     * @return true if fast authentication succeeded
     */
    public boolean isFastAuthSuccess() {
        return currentPhase == AuthPhase.FAST_AUTH_SUCCESS;
    }
    
    /**
     * Check if full authentication is required
     * 
     * @return true if full authentication is required
     */
    public boolean isFullAuthRequired() {
        return currentPhase == AuthPhase.FULL_AUTH_REQUIRED;
    }
    
    /**
     * Check if RSA key exchange is required
     * 
     * @return true if RSA key exchange is needed
     */
    public boolean isRSAExchangeRequired() {
        return currentPhase == AuthPhase.RSA_KEY_EXCHANGE;
    }
    
    /**
     * Check if password has been encrypted
     * 
     * @return true if password is encrypted
     */
    public boolean isPasswordEncrypted() {
        return currentPhase == AuthPhase.PASSWORD_ENCRYPTED;
    }
    
    /**
     * Check if authentication is complete
     * 
     * @return true if authentication is complete
     */
    public boolean isAuthComplete() {
        return currentPhase == AuthPhase.AUTH_COMPLETE;
    }
    
    /**
     * Get RSA public key for encryption
     * 
     * @return RSA public key, or null if not set
     */
    public RSAPublicKey getServerPublicKey() {
        return serverPublicKey;
    }
    
    /**
     * Get encrypted password bytes
     * 
     * @return Copy of encrypted password, or empty array if not set
     */
    public byte[] getEncryptedPassword() {
        return encryptedPassword != null ? encryptedPassword.clone() : new byte[0];
    }
    
    /**
     * Get plain text password for full authentication
     * 
     * @return Plain text password, or null if not set
     */
    public String getPlainTextPassword() {
        return plainTextPassword;
    }
    
    /**
     * Set plain text password for full authentication
     * 
     * @param plainTextPassword Plain text password
     */
    public void setPlainTextPassword(String plainTextPassword) {
        this.plainTextPassword = plainTextPassword;
    }
    
    /**
     * Validate that the password is in a consistent state
     * 
     * @return true if password state is valid
     */
    public boolean isValid() {
        switch (currentPhase) {
            case INITIAL_AUTH:
                return scrambledPassword.length > 0 && nonce.length > 0;
            case FAST_AUTH_SUCCESS:
                return scrambledPassword.length > 0;
            case FULL_AUTH_REQUIRED:
                return true; // Always valid in this phase
            case RSA_KEY_EXCHANGE:
                return serverPublicKey != null;
            case PASSWORD_ENCRYPTED:
                return encryptedPassword != null && encryptedPassword.length > 0;
            case AUTH_COMPLETE:
                return true; // Always valid when complete
            default:
                return false;
        }
    }
    
    /**
     * Clear sensitive data from memory
     */
    public void clearSensitiveData() {
        if (scrambledPassword != null) {
            Arrays.fill(scrambledPassword, (byte) 0);
        }
        if (encryptedPassword != null) {
            Arrays.fill(encryptedPassword, (byte) 0);
        }
        if (plainTextPassword != null) {
            // Clear string by creating new char array and filling it
            char[] chars = plainTextPassword.toCharArray();
            Arrays.fill(chars, '\0');
            plainTextPassword = null;
        }
    }
    
    @Override
    public String getAuthPluginName() {
        return "caching_sha2_password";
    }
    
    @Override
    public boolean isAuthenticated() {
        return currentPhase == AuthPhase.FAST_AUTH_SUCCESS ||
               currentPhase == AuthPhase.AUTH_COMPLETE;
    }
    
    @Override
    public void clearPassword() {
        clearSensitiveData();
    }
    
    @Override
    public String toSafeString() {
        return String.format("CachingSha2Password{phase=%s, authenticated=%s}",
                currentPhase, isAuthenticated());
    }
    
    @Override
    public String toString() {
        return String.format("CachingSha2Password{phase=%s, hasScrambled=%s, hasNonce=%s, hasPublicKey=%s}",
                currentPhase,
                scrambledPassword.length > 0,
                nonce.length > 0,
                serverPublicKey != null);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        CachingSha2Password that = (CachingSha2Password) obj;
        return currentPhase == that.currentPhase
                && Arrays.equals(scrambledPassword, that.scrambledPassword)
                && Arrays.equals(nonce, that.nonce);
    }
    
    @Override
    public int hashCode() {
        int result = currentPhase != null ? currentPhase.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(scrambledPassword);
        result = 31 * result + Arrays.hashCode(nonce);
        return result;
    }
}
