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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for CachingSha2Password class.
 *
 * Tests all phases of the caching_sha2_password authentication process including
 * state transitions, data management, and security features.
 */
@DisplayName("CachingSha2Password Tests")
public class CachingSha2PasswordTest {
    
    private byte[] testScrambledPassword;
    private byte[] testNonce;
    private String testPlainTextPassword;
    private RSAPublicKey testPublicKey;
    
    @BeforeEach
    void setUp() throws Exception {
        // Initialize test data
        testScrambledPassword = "scrambled_password_hash".getBytes();
        testNonce = "random_nonce_12345".getBytes();
        testPlainTextPassword = "test_password_123";
        
        // Generate test RSA key pair
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        KeyPair keyPair = keyGen.generateKeyPair();
        testPublicKey = (RSAPublicKey) keyPair.getPublic();
    }
    
    @Test
    @DisplayName("Constructor with scrambled password and nonce")
    void testConstructorWithScrambledPasswordAndNonce() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        
        assertNotNull(password);
        assertEquals(CachingSha2Password.AuthPhase.INITIAL_AUTH, password.getCurrentPhase());
        assertArrayEquals(testScrambledPassword, password.getScrambledPassword());
        assertArrayEquals(testNonce, password.getNonce());
        assertNull(password.getPlainTextPassword());
        assertTrue(password.isInitialAuth());
        assertTrue(password.isValid());
    }
    
    @Test
    @DisplayName("Constructor with plain text password")
    void testConstructorWithPlainTextPassword() {
        CachingSha2Password password = new CachingSha2Password(
                testScrambledPassword, testNonce, testPlainTextPassword);
        
        assertNotNull(password);
        assertEquals(CachingSha2Password.AuthPhase.INITIAL_AUTH, password.getCurrentPhase());
        assertArrayEquals(testScrambledPassword, password.getScrambledPassword());
        assertArrayEquals(testNonce, password.getNonce());
        assertEquals(testPlainTextPassword, password.getPlainTextPassword());
    }
    
    @Test
    @DisplayName("Constructor with null parameters")
    void testConstructorWithNullParameters() {
        CachingSha2Password password = new CachingSha2Password(null, null);
        
        assertNotNull(password);
        assertEquals(CachingSha2Password.AuthPhase.INITIAL_AUTH, password.getCurrentPhase());
        assertEquals(0, password.getScrambledPassword().length);
        assertEquals(0, password.getNonce().length);
        assertFalse(password.isValid()); // Should be invalid with empty data
    }
    
    @Test
    @DisplayName("Phase transition to fast auth success")
    void testTransitionToFastAuthSuccess() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        
        password.transitionToFastAuthSuccess();
        
        assertEquals(CachingSha2Password.AuthPhase.FAST_AUTH_SUCCESS, password.getCurrentPhase());
        assertTrue(password.isFastAuthSuccess());
        assertFalse(password.isInitialAuth());
        assertTrue(password.isValid());
    }
    
    @Test
    @DisplayName("Phase transition to full auth required")
    void testTransitionToFullAuth() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        
        password.transitionToFullAuth();
        
        assertEquals(CachingSha2Password.AuthPhase.FULL_AUTH_REQUIRED, password.getCurrentPhase());
        assertTrue(password.isFullAuthRequired());
        assertFalse(password.isInitialAuth());
        assertTrue(password.isValid());
    }
    
    @Test
    @DisplayName("Phase transition to RSA exchange")
    void testTransitionToRSAExchange() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        
        password.transitionToRSAExchange(testPublicKey);
        
        assertEquals(CachingSha2Password.AuthPhase.RSA_KEY_EXCHANGE, password.getCurrentPhase());
        assertTrue(password.isRSAExchangeRequired());
        assertEquals(testPublicKey, password.getServerPublicKey());
        assertTrue(password.isValid());
    }
    
    @Test
    @DisplayName("Phase transition to password encrypted")
    void testTransitionToPasswordEncrypted() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        byte[] encryptedPassword = "encrypted_password_data".getBytes();
        
        password.transitionToPasswordEncrypted(encryptedPassword);
        
        assertEquals(CachingSha2Password.AuthPhase.PASSWORD_ENCRYPTED, password.getCurrentPhase());
        assertTrue(password.isPasswordEncrypted());
        assertArrayEquals(encryptedPassword, password.getEncryptedPassword());
        assertTrue(password.isValid());
    }
    
    @Test
    @DisplayName("Phase transition to complete")
    void testTransitionToComplete() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        
        password.transitionToComplete();
        
        assertEquals(CachingSha2Password.AuthPhase.AUTH_COMPLETE, password.getCurrentPhase());
        assertTrue(password.isAuthComplete());
        assertTrue(password.isValid());
    }
    
    @Test
    @DisplayName("Complete authentication flow")
    void testCompleteAuthenticationFlow() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        
        // Initial state
        assertTrue(password.isInitialAuth());
        assertTrue(password.isValid());
        
        // Transition to full auth required
        password.transitionToFullAuth();
        assertTrue(password.isFullAuthRequired());
        
        // Transition to RSA exchange
        password.transitionToRSAExchange(testPublicKey);
        assertTrue(password.isRSAExchangeRequired());
        assertEquals(testPublicKey, password.getServerPublicKey());
        
        // Transition to password encrypted
        byte[] encryptedPassword = "encrypted_data".getBytes();
        password.transitionToPasswordEncrypted(encryptedPassword);
        assertTrue(password.isPasswordEncrypted());
        
        // Set plain text password
        password.setPlainTextPassword(testPlainTextPassword);
        assertEquals(testPlainTextPassword, password.getPlainTextPassword());
        
        // Complete authentication
        password.transitionToComplete();
        assertTrue(password.isAuthComplete());
        assertTrue(password.isValid());
    }
    
    @Test
    @DisplayName("Fast authentication flow")
    void testFastAuthenticationFlow() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        
        // Initial state
        assertTrue(password.isInitialAuth());
        
        // Direct transition to fast auth success (cached password)
        password.transitionToFastAuthSuccess();
        assertTrue(password.isFastAuthSuccess());
        assertTrue(password.isValid());
        
        // Complete authentication
        password.transitionToComplete();
        assertTrue(password.isAuthComplete());
    }
    
    @Test
    @DisplayName("Data immutability and defensive copying")
    void testDataImmutability() {
        byte[] originalScrambled = testScrambledPassword.clone();
        byte[] originalNonce = testNonce.clone();
        
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        
        // Modify original arrays
        Arrays.fill(testScrambledPassword, (byte) 0);
        Arrays.fill(testNonce, (byte) 0);
        
        // Password should still have original data (defensive copying)
        assertArrayEquals(originalScrambled, password.getScrambledPassword());
        assertArrayEquals(originalNonce, password.getNonce());
        
        // Modify returned arrays
        byte[] returnedScrambled = password.getScrambledPassword();
        byte[] returnedNonce = password.getNonce();
        Arrays.fill(returnedScrambled, (byte) 0);
        Arrays.fill(returnedNonce, (byte) 0);
        
        // Password should still have original data (defensive copying on return)
        assertArrayEquals(originalScrambled, password.getScrambledPassword());
        assertArrayEquals(originalNonce, password.getNonce());
    }
    
    @Test
    @DisplayName("Sensitive data clearing")
    void testSensitiveDataClearing() {
        byte[] encryptedPassword = "encrypted_password_data".getBytes();
        CachingSha2Password password = new CachingSha2Password(
                testScrambledPassword, testNonce, testPlainTextPassword);
        password.transitionToPasswordEncrypted(encryptedPassword);
        
        // Verify data is present before clearing
        assertTrue(password.getScrambledPassword().length > 0);
        assertTrue(password.getEncryptedPassword().length > 0);
        assertNotNull(password.getPlainTextPassword());
        
        // Clear sensitive data
        password.clearSensitiveData();
        
        // Verify data is cleared
        assertNull(password.getPlainTextPassword());
        // Note: Arrays should be zeroed but length remains the same
        assertEquals(testScrambledPassword.length, password.getScrambledPassword().length);
        assertEquals(encryptedPassword.length, password.getEncryptedPassword().length);
    }
    
    @Test
    @DisplayName("Validation in different phases")
    void testValidationInDifferentPhases() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        
        // Initial auth - valid with scrambled password and nonce
        assertTrue(password.isValid());
        
        // Fast auth success - valid with scrambled password
        password.transitionToFastAuthSuccess();
        assertTrue(password.isValid());
        
        // Full auth required - always valid
        password.transitionToFullAuth();
        assertTrue(password.isValid());
        
        // RSA exchange - valid with public key
        password.transitionToRSAExchange(testPublicKey);
        assertTrue(password.isValid());
        
        // Password encrypted - valid with encrypted password
        password.transitionToPasswordEncrypted("encrypted".getBytes());
        assertTrue(password.isValid());
        
        // Auth complete - always valid
        password.transitionToComplete();
        assertTrue(password.isValid());
    }
    
    @Test
    @DisplayName("Invalid states")
    void testInvalidStates() {
        // Empty scrambled password and nonce
        CachingSha2Password emptyPassword = new CachingSha2Password(new byte[0], new byte[0]);
        assertFalse(emptyPassword.isValid());
        
        // RSA exchange without public key
        CachingSha2Password rsaPassword = new CachingSha2Password(testScrambledPassword, testNonce);
        rsaPassword.transitionToRSAExchange(null);
        assertFalse(rsaPassword.isValid());
        
        // Password encrypted without encrypted data
        CachingSha2Password encryptedPassword = new CachingSha2Password(testScrambledPassword, testNonce);
        encryptedPassword.transitionToPasswordEncrypted(null);
        assertFalse(encryptedPassword.isValid());
    }
    
    @Test
    @DisplayName("Equality and hash code")
    void testEqualityAndHashCode() {
        CachingSha2Password password1 = new CachingSha2Password(testScrambledPassword, testNonce);
        CachingSha2Password password2 = new CachingSha2Password(testScrambledPassword, testNonce);
        CachingSha2Password password3 = new CachingSha2Password("different".getBytes(), testNonce);
        
        // Test equality
        assertEquals(password1, password2);
        assertNotEquals(password1, password3);
        assertNotEquals(password1, null);
        assertNotEquals(password1, "not a password");
        
        // Test hash code consistency
        assertEquals(password1.hashCode(), password2.hashCode());
        assertNotEquals(password1.hashCode(), password3.hashCode());
        
        // Test equality after phase transitions
        password1.transitionToFastAuthSuccess();
        password2.transitionToFastAuthSuccess();
        assertEquals(password1, password2);
        
        password1.transitionToComplete();
        assertNotEquals(password1, password2);
    }
    
    @Test
    @DisplayName("String representation")
    void testToString() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        
        String str = password.toString();
        assertNotNull(str);
        assertTrue(str.contains("CachingSha2Password"));
        assertTrue(str.contains("INITIAL_AUTH"));
        assertTrue(str.contains("hasScrambled=true"));
        assertTrue(str.contains("hasNonce=true"));
        assertTrue(str.contains("hasPublicKey=false"));
        
        // Test after RSA exchange
        password.transitionToRSAExchange(testPublicKey);
        str = password.toString();
        assertTrue(str.contains("RSA_KEY_EXCHANGE"));
        assertTrue(str.contains("hasPublicKey=true"));
    }
    
    @Test
    @DisplayName("Encrypted password handling with null")
    void testEncryptedPasswordWithNull() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        
        password.transitionToPasswordEncrypted(null);
        
        assertEquals(0, password.getEncryptedPassword().length);
        assertFalse(password.isValid()); // Should be invalid with null encrypted password
    }
    
    @Test
    @DisplayName("Phase state consistency")
    void testPhaseStateConsistency() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        
        // Only one phase should be true at a time
        assertTrue(password.isInitialAuth());
        assertFalse(password.isFastAuthSuccess());
        assertFalse(password.isFullAuthRequired());
        assertFalse(password.isRSAExchangeRequired());
        assertFalse(password.isPasswordEncrypted());
        assertFalse(password.isAuthComplete());
        
        password.transitionToFastAuthSuccess();
        assertFalse(password.isInitialAuth());
        assertTrue(password.isFastAuthSuccess());
        assertFalse(password.isFullAuthRequired());
        assertFalse(password.isRSAExchangeRequired());
        assertFalse(password.isPasswordEncrypted());
        assertFalse(password.isAuthComplete());
    }
}
