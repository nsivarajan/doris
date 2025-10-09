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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.util.Arrays;

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

        Assertions.assertNotNull(password);
        Assertions.assertEquals(CachingSha2Password.AuthPhase.INITIAL_AUTH, password.getCurrentPhase());
        Assertions.assertArrayEquals(testScrambledPassword, password.getScrambledPassword());
        Assertions.assertArrayEquals(testNonce, password.getNonce());
        Assertions.assertNull(password.getPlainTextPassword());
        Assertions.assertTrue(password.isInitialAuth());
        Assertions.assertTrue(password.isValid());
    }

    @Test
    @DisplayName("Constructor with plain text password")
    void testConstructorWithPlainTextPassword() {
        CachingSha2Password password = new CachingSha2Password(
                testScrambledPassword, testNonce, testPlainTextPassword);

        Assertions.assertNotNull(password);
        Assertions.assertEquals(CachingSha2Password.AuthPhase.INITIAL_AUTH, password.getCurrentPhase());
        Assertions.assertArrayEquals(testScrambledPassword, password.getScrambledPassword());
        Assertions.assertArrayEquals(testNonce, password.getNonce());
        Assertions.assertEquals(testPlainTextPassword, password.getPlainTextPassword());
    }

    @Test
    @DisplayName("Constructor with null parameters")
    void testConstructorWithNullParameters() {
        CachingSha2Password password = new CachingSha2Password(null, null);

        Assertions.assertNotNull(password);
        Assertions.assertEquals(CachingSha2Password.AuthPhase.INITIAL_AUTH, password.getCurrentPhase());
        Assertions.assertEquals(0, password.getScrambledPassword().length);
        Assertions.assertEquals(0, password.getNonce().length);
        Assertions.assertFalse(password.isValid()); // Should be invalid with empty data
    }

    @Test
    @DisplayName("Phase transition to fast auth success")
    void testTransitionToFastAuthSuccess() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);

        password.transitionToFastAuthSuccess();

        Assertions.assertEquals(CachingSha2Password.AuthPhase.FAST_AUTH_SUCCESS, password.getCurrentPhase());
        Assertions.assertTrue(password.isFastAuthSuccess());
        Assertions.assertFalse(password.isInitialAuth());
        Assertions.assertTrue(password.isValid());
    }

    @Test
    @DisplayName("Phase transition to full auth required")
    void testTransitionToFullAuth() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);

        password.transitionToFullAuth();

        Assertions.assertEquals(CachingSha2Password.AuthPhase.FULL_AUTH_REQUIRED, password.getCurrentPhase());
        Assertions.assertTrue(password.isFullAuthRequired());
        Assertions.assertFalse(password.isInitialAuth());
        Assertions.assertTrue(password.isValid());
    }

    @Test
    @DisplayName("Phase transition to RSA exchange")
    void testTransitionToRSAExchange() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);

        password.transitionToRSAExchange(testPublicKey);

        Assertions.assertEquals(CachingSha2Password.AuthPhase.RSA_KEY_EXCHANGE, password.getCurrentPhase());
        Assertions.assertTrue(password.isRSAExchangeRequired());
        Assertions.assertEquals(testPublicKey, password.getServerPublicKey());
        Assertions.assertTrue(password.isValid());
    }

    @Test
    @DisplayName("Phase transition to password encrypted")
    void testTransitionToPasswordEncrypted() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
        byte[] encryptedPassword = "encrypted_password_data".getBytes();

        password.transitionToPasswordEncrypted(encryptedPassword);

        Assertions.assertEquals(CachingSha2Password.AuthPhase.PASSWORD_ENCRYPTED, password.getCurrentPhase());
        Assertions.assertTrue(password.isPasswordEncrypted());
        Assertions.assertArrayEquals(encryptedPassword, password.getEncryptedPassword());
        Assertions.assertTrue(password.isValid());
    }

    @Test
    @DisplayName("Phase transition to complete")
    void testTransitionToComplete() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);

        password.transitionToComplete();

        Assertions.assertEquals(CachingSha2Password.AuthPhase.AUTH_COMPLETE, password.getCurrentPhase());
        Assertions.assertTrue(password.isAuthComplete());
        Assertions.assertTrue(password.isValid());
    }

    @Test
    @DisplayName("Complete authentication flow")
    void testCompleteAuthenticationFlow() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);
    
        // Initial state
        Assertions.assertTrue(password.isInitialAuth());
        Assertions.assertTrue(password.isValid());

        // Transition to full auth required
        password.transitionToFullAuth();
        Assertions.assertTrue(password.isFullAuthRequired());

        // Transition to RSA exchange
        password.transitionToRSAExchange(testPublicKey);
        Assertions.assertTrue(password.isRSAExchangeRequired());
        Assertions.assertEquals(testPublicKey, password.getServerPublicKey());

        // Transition to password encrypted
        byte[] encryptedPassword = "encrypted_data".getBytes();
        password.transitionToPasswordEncrypted(encryptedPassword);
        Assertions.assertTrue(password.isPasswordEncrypted());

        // Set plain text password
        password.setPlainTextPassword(testPlainTextPassword);
        Assertions.assertEquals(testPlainTextPassword, password.getPlainTextPassword());

        // Complete authentication
        password.transitionToComplete();
        Assertions.assertTrue(password.isAuthComplete());
        Assertions.assertTrue(password.isValid());
    }

    @Test
    @DisplayName("Fast authentication flow")
    void testFastAuthenticationFlow() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);

        // Initial state
        Assertions.assertTrue(password.isInitialAuth());

        // Direct transition to fast auth success (cached password)
        password.transitionToFastAuthSuccess();
        Assertions.assertTrue(password.isFastAuthSuccess());
        Assertions.assertTrue(password.isValid());

        // Complete authentication
        password.transitionToComplete();
        Assertions.assertTrue(password.isAuthComplete());
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
        Assertions.assertArrayEquals(originalScrambled, password.getScrambledPassword());
        Assertions.assertArrayEquals(originalNonce, password.getNonce());

        // Modify returned arrays
        byte[] returnedScrambled = password.getScrambledPassword();
        byte[] returnedNonce = password.getNonce();
        Arrays.fill(returnedScrambled, (byte) 0);
        Arrays.fill(returnedNonce, (byte) 0);

        // Password should still have original data (defensive copying on return)
        Assertions.assertArrayEquals(originalScrambled, password.getScrambledPassword());
        Assertions.assertArrayEquals(originalNonce, password.getNonce());
    }

    @Test
    @DisplayName("Sensitive data clearing")
    void testSensitiveDataClearing() {
        byte[] encryptedPassword = "encrypted_password_data".getBytes();
        CachingSha2Password password = new CachingSha2Password(
                testScrambledPassword, testNonce, testPlainTextPassword);
        password.transitionToPasswordEncrypted(encryptedPassword);

        // Verify data is present before clearing
        Assertions.assertTrue(password.getScrambledPassword().length > 0);
        Assertions.assertTrue(password.getEncryptedPassword().length > 0);
        Assertions.assertNotNull(password.getPlainTextPassword());

        // Clear sensitive data
        password.clearSensitiveData();

        // Verify data is cleared
        Assertions.assertNull(password.getPlainTextPassword());
        // Note: Arrays should be zeroed but length remains the same
        Assertions.assertEquals(testScrambledPassword.length, password.getScrambledPassword().length);
        Assertions.assertEquals(encryptedPassword.length, password.getEncryptedPassword().length);
    }

    @Test
    @DisplayName("Validation in different phases")
    void testValidationInDifferentPhases() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);

        // Initial auth - valid with scrambled password and nonce
        Assertions.assertTrue(password.isValid());

        // Fast auth success - valid with scrambled password
        password.transitionToFastAuthSuccess();
        Assertions.assertTrue(password.isValid());

        // Full auth required - always valid
        password.transitionToFullAuth();
        Assertions.assertTrue(password.isValid());

        // RSA exchange - valid with public key
        password.transitionToRSAExchange(testPublicKey);
        Assertions.assertTrue(password.isValid());

        // Password encrypted - valid with encrypted password
        password.transitionToPasswordEncrypted("encrypted".getBytes());
        Assertions.assertTrue(password.isValid());

        // Auth complete - always valid
        password.transitionToComplete();
        Assertions.assertTrue(password.isValid());
    }

    @Test
    @DisplayName("Invalid states")
    void testInvalidStates() {
        // Empty scrambled password and nonce
        CachingSha2Password emptyPassword = new CachingSha2Password(new byte[0], new byte[0]);
        Assertions.assertFalse(emptyPassword.isValid());

        // RSA exchange without public key
        CachingSha2Password rsaPassword = new CachingSha2Password(testScrambledPassword, testNonce);
        rsaPassword.transitionToRSAExchange(null);
        Assertions.assertFalse(rsaPassword.isValid());

        // Password encrypted without encrypted data
        CachingSha2Password encryptedPassword = new CachingSha2Password(testScrambledPassword, testNonce);
        encryptedPassword.transitionToPasswordEncrypted(null);
        Assertions.assertFalse(encryptedPassword.isValid());
    }

    @Test
    @DisplayName("Equality and hash code")
    void testEqualityAndHashCode() {
        CachingSha2Password password1 = new CachingSha2Password(testScrambledPassword, testNonce);
        CachingSha2Password password2 = new CachingSha2Password(testScrambledPassword, testNonce);
        CachingSha2Password password3 = new CachingSha2Password("different".getBytes(), testNonce);

        // Test equality
        Assertions.assertEquals(password1, password2);
        Assertions.assertNotEquals(password1, password3);
        Assertions.assertNotEquals(password1, null);
        Assertions.assertNotEquals(password1, "not a password");

        // Test hash code consistency
        Assertions.assertEquals(password1.hashCode(), password2.hashCode());
        Assertions.assertNotEquals(password1.hashCode(), password3.hashCode());

        // Test equality after phase transitions
        password1.transitionToFastAuthSuccess();
        password2.transitionToFastAuthSuccess();
        Assertions.assertEquals(password1, password2);

        password1.transitionToComplete();
        Assertions.assertNotEquals(password1, password2);
    }

    @Test
    @DisplayName("String representation")
    void testToString() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);

        String str = password.toString();
        Assertions.assertNotNull(str);
        Assertions.assertTrue(str.contains("CachingSha2Password"));
        Assertions.assertTrue(str.contains("INITIAL_AUTH"));
        Assertions.assertTrue(str.contains("hasScrambled=true"));
        Assertions.assertTrue(str.contains("hasNonce=true"));
        Assertions.assertTrue(str.contains("hasPublicKey=false"));

        // Test after RSA exchange
        password.transitionToRSAExchange(testPublicKey);
        str = password.toString();
        Assertions.assertTrue(str.contains("RSA_KEY_EXCHANGE"));
        Assertions.assertTrue(str.contains("hasPublicKey=true"));
    }

    @Test
    @DisplayName("Encrypted password handling with null")
    void testEncryptedPasswordWithNull() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);

        password.transitionToPasswordEncrypted(null);

        Assertions.assertEquals(0, password.getEncryptedPassword().length);
        Assertions.assertFalse(password.isValid()); // Should be invalid with null encrypted password
    }

    @Test
    @DisplayName("Phase state consistency")
    void testPhaseStateConsistency() {
        CachingSha2Password password = new CachingSha2Password(testScrambledPassword, testNonce);

        // Only one phase should be true at a time
        Assertions.assertTrue(password.isInitialAuth());
        Assertions.assertFalse(password.isFastAuthSuccess());
        Assertions.assertFalse(password.isFullAuthRequired());
        Assertions.assertFalse(password.isRSAExchangeRequired());
        Assertions.assertFalse(password.isPasswordEncrypted());
        Assertions.assertFalse(password.isAuthComplete());

        password.transitionToFastAuthSuccess();
        Assertions.assertFalse(password.isInitialAuth());
        Assertions.assertTrue(password.isFastAuthSuccess());
        Assertions.assertFalse(password.isFullAuthRequired());
        Assertions.assertFalse(password.isRSAExchangeRequired());
        Assertions.assertFalse(password.isPasswordEncrypted());
        Assertions.assertFalse(password.isAuthComplete());
    }
}
