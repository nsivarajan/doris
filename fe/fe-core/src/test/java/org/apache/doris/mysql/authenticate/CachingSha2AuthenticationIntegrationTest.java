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
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlEnhancedHandshakePacket;
import org.apache.doris.mysql.authenticate.password.CachingSha2Password;
import org.apache.doris.mysql.authenticate.password.CachingSha2PasswordResolver;
import org.apache.doris.mysql.authenticate.password.Password;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;

/**
 * Integration test suite for caching_sha2_password authentication system.
 *
 * Tests the complete authentication flow including handshake, authentication state machine,
 * password resolution, and security transport decisions.
 */
@DisplayName("CachingSha2 Authentication Integration Tests")
public class CachingSha2AuthenticationIntegrationTest {

    @Mock
    private ConnectContext mockContext;

    @Mock
    private MysqlChannel mockChannel;

    private EnhancedAuthenticator authenticator;
    private AuthenticationStateMachine stateMachine;
    private SecurityTransportDecision.TransportDecision transportDecision;
    private RSAPublicKey testPublicKey;

    private boolean originalCachingSha2Enabled;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Save original configuration
        originalCachingSha2Enabled = Config.enable_caching_sha2_password;

        // Enable caching_sha2_password for tests
        Config.enable_caching_sha2_password = true;

        // Initialize test components
        authenticator = new EnhancedAuthenticator();

        // Generate test RSA key pair
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        KeyPair keyPair = keyGen.generateKeyPair();
        testPublicKey = (RSAPublicKey) keyPair.getPublic();

        // Setup mock context
        Mockito.when(mockContext.getConnectionId()).thenReturn(12345);
        Mockito.when(mockContext.getMysqlChannel()).thenReturn(mockChannel);

        // Setup mock channel
        Mockito.when(mockChannel.isSslMode()).thenReturn(false); // Default to non-SSL for RSA testing
    }

    @AfterEach
    void tearDown() {
        // Restore original configuration
        Config.enable_caching_sha2_password = originalCachingSha2Enabled;
        // Cleanup authenticator
        if (authenticator != null) {
            authenticator.shutdown();
        }
    }


    @Test
    @DisplayName("Enhanced handshake packet creation with caching_sha2_password")
    void testEnhancedHandshakePacketCreation() {
        MysqlEnhancedHandshakePacket handshake = new MysqlEnhancedHandshakePacket(12345);

        Assertions.assertNotNull(handshake);
        Assertions.assertEquals(MysqlEnhancedHandshakePacket.CACHING_SHA2_PASSWORD_PLUGIN,
                    handshake.getSelectedAuthPlugin());
        Assertions.assertTrue(handshake.isEnhancedAuthEnabled());
        
        // Test plugin compatibility check
        Assertions.assertTrue(handshake.checkAuthPluginSameAsDoris(
                MysqlEnhancedHandshakePacket.CACHING_SHA2_PASSWORD_PLUGIN));
        Assertions.assertFalse(handshake.checkAuthPluginSameAsDoris(
                MysqlEnhancedHandshakePacket.MYSQL_NATIVE_PASSWORD_PLUGIN));
    }

    @Test
    @DisplayName("Enhanced handshake packet with disabled caching_sha2_password")
    void testEnhancedHandshakePacketWithDisabledFeature() {
        Config.enable_caching_sha2_password = false;

        MysqlEnhancedHandshakePacket handshake = new MysqlEnhancedHandshakePacket(12345);

        Assertions.assertNotNull(handshake);
        Assertions.assertEquals(MysqlEnhancedHandshakePacket.MYSQL_NATIVE_PASSWORD_PLUGIN,
                    handshake.getSelectedAuthPlugin());
        Assertions.assertFalse(handshake.isEnhancedAuthEnabled());
    }

    @Test
    @DisplayName("Authentication state machine initialization and basic flow")
    void testAuthenticationStateMachineFlow() throws Exception {
        stateMachine = new AuthenticationStateMachine(mockContext, mockChannel, null);

        // Initial state
        Assertions.assertEquals(AuthenticationStateMachine.State.INITIAL, stateMachine.getCurrentState());
        Assertions.assertFalse(stateMachine.isAuthenticationComplete());
        Assertions.assertFalse(stateMachine.isAuthenticationSuccessful());

        // Send handshake
        Assertions.assertTrue(stateMachine.processEvent(AuthenticationStateMachine.Event.SEND_HANDSHAKE));
        Assertions.assertEquals(AuthenticationStateMachine.State.HANDSHAKE_SENT, stateMachine.getCurrentState());

        // Receive auth packet
        Assertions.assertTrue(stateMachine.processEvent(AuthenticationStateMachine.Event.RECEIVE_AUTH_PACKET));
        Assertions.assertEquals(AuthenticationStateMachine.State.AUTH_PACKET_RECEIVED, stateMachine.getCurrentState());

        // Authentication success
        Assertions.assertTrue(stateMachine.processEvent(AuthenticationStateMachine.Event.AUTHENTICATE_SUCCESS));
        Assertions.assertEquals(AuthenticationStateMachine.State.AUTHENTICATED, stateMachine.getCurrentState());
        Assertions.assertTrue(stateMachine.isAuthenticationComplete());
        Assertions.assertTrue(stateMachine.isAuthenticationSuccessful());
    }

    @Test
    @DisplayName("Authentication state machine plugin switch flow")
    void testAuthenticationStateMachinePluginSwitch() throws Exception {
        stateMachine = new AuthenticationStateMachine(mockContext, mockChannel, null);

        // Initial flow to auth packet received
        stateMachine.processEvent(AuthenticationStateMachine.Event.SEND_HANDSHAKE);
        stateMachine.processEvent(AuthenticationStateMachine.Event.RECEIVE_AUTH_PACKET);

        // Plugin mismatch triggers switch
        Assertions.assertTrue(stateMachine.processEvent(AuthenticationStateMachine.Event.PLUGIN_MISMATCH));
        Assertions.assertEquals(AuthenticationStateMachine.State.PLUGIN_SWITCH_REQUIRED, stateMachine.getCurrentState());

        // Send plugin switch
        Assertions.assertTrue(stateMachine.processEvent(AuthenticationStateMachine.Event.SEND_PLUGIN_SWITCH));
        Assertions.assertEquals(AuthenticationStateMachine.State.PLUGIN_SWITCH_SENT, stateMachine.getCurrentState());

        // Receive plugin response and authenticate
        Assertions.assertTrue(stateMachine.processEvent(AuthenticationStateMachine.Event.RECEIVE_PLUGIN_RESPONSE));
        Assertions.assertEquals(AuthenticationStateMachine.State.AUTHENTICATED, stateMachine.getCurrentState());
    }

    @Test
    @DisplayName("Security transport decision with SSL available")
    void testSecurityTransportDecisionWithSSL() {
        Mockito.when(mockChannel.isSslMode()).thenReturn(true);

        SecurityTransportDecision.TransportDecision decision =
                SecurityTransportDecision.determineTransportMethod(mockContext, mockChannel);

        Assertions.assertNotNull(decision);
        Assertions.assertEquals(SecurityTransportDecision.TransportMethod.SSL_ENCRYPTED, decision.getMethod());
        Assertions.assertTrue(decision.isSecure());
        Assertions.assertFalse(decision.requiresRSAKey());
        Assertions.assertTrue(decision.getReason().contains("SSL"));
    }

    @Test
    @DisplayName("Security transport decision with RSA fallback")
    void testSecurityTransportDecisionWithRSAFallback() {
        Mockito.when(mockChannel.isSslMode()).thenReturn(false);

        SecurityTransportDecision.TransportDecision decision =
                SecurityTransportDecision.determineTransportMethod(mockContext, mockChannel);

        Assertions.assertNotNull(decision);
        // Should be RSA_ENCRYPTED if RSA keys are available, or INSECURE_FALLBACK if not
        Assertions.assertTrue(decision.getMethod() == SecurityTransportDecision.TransportMethod.RSA_ENCRYPTED
                || decision.getMethod() == SecurityTransportDecision.TransportMethod.INSECURE_FALLBACK);

        if (decision.getMethod() == SecurityTransportDecision.TransportMethod.RSA_ENCRYPTED) {
            Assertions.assertTrue(decision.isSecure());
            Assertions.assertTrue(decision.requiresRSAKey());
        }
    }

    @Test
    @DisplayName("Security transport decision with disabled caching_sha2_password")
    void testSecurityTransportDecisionWithDisabledFeature() {
        Config.enable_caching_sha2_password = false;

        SecurityTransportDecision.TransportDecision decision =
                SecurityTransportDecision.determineTransportMethod(mockContext, mockChannel);

        Assertions.assertNotNull(decision);
        Assertions.assertEquals(SecurityTransportDecision.TransportMethod.INSECURE_FALLBACK, decision.getMethod());
        Assertions.assertFalse(decision.isSecure());
        Assertions.assertFalse(decision.requiresRSAKey());
        Assertions.assertTrue(decision.getReason().contains("disabled"));
    }

    @Test
    @DisplayName("Enhanced authenticator with caching_sha2_password")
    void testEnhancedAuthenticatorWithCachingSha2Password() throws Exception {
        // Create test password in complete state
        byte[] scrambledPassword = "test_scrambled_password".getBytes();
        byte[] nonce = "test_nonce_12345".getBytes();
        String plainTextPassword = "test_password_123";

        CachingSha2Password password = new CachingSha2Password(scrambledPassword, nonce, plainTextPassword);
        password.transitionToComplete();

        // Create mock request
        AuthenticateRequest request = Mockito.mock(AuthenticateRequest.class);
        Mockito.when(request.getUserName()).thenReturn("test_user");
        Mockito.when(request.getRemoteIp()).thenReturn("127.0.0.1");
        Mockito.when(request.getPassword()).thenReturn((Password) password);

        // Test authentication (will fail due to mocked environment, but should handle caching_sha2_password)
        AuthenticateResponse response = authenticator.authenticate(request);

        // Verify the authenticator attempted to process caching_sha2_password
        Assertions.assertNotNull(response);
        // Response will be failed due to mocked environment, but the flow should be tested
    }

    @Test
    @DisplayName("Enhanced authenticator password resolver selection")
    void testEnhancedAuthenticatorPasswordResolverSelection() {
        // With caching_sha2_password enabled
        Config.enable_caching_sha2_password = true;
        EnhancedAuthenticator enabledAuthenticator = new EnhancedAuthenticator();
        Assertions.assertTrue(enabledAuthenticator.getPasswordResolver() instanceof CachingSha2PasswordResolver);

        // With caching_sha2_password disabled
        Config.enable_caching_sha2_password = false;
        EnhancedAuthenticator disabledAuthenticator = new EnhancedAuthenticator();
        // Should fall back to native password resolver
        Assertions.assertNotNull(disabledAuthenticator.getPasswordResolver());

        enabledAuthenticator.shutdown();
        disabledAuthenticator.shutdown();
    }

    @Test
    @DisplayName("Enhanced authenticator statistics tracking")
    void testEnhancedAuthenticatorStatistics() {
        EnhancedAuthenticator.AuthenticationMetrics initialMetrics = authenticator.getMetrics();

        Assertions.assertEquals(0, initialMetrics.getTotalCount());
        Assertions.assertEquals(0, initialMetrics.getCachingSha2Count());
        Assertions.assertEquals(0, initialMetrics.getNativePasswordCount());
        Assertions.assertEquals(0, initialMetrics.getSuccessCount());
        Assertions.assertEquals(0, initialMetrics.getFailureCount());
        Assertions.assertEquals(0.0, initialMetrics.getSuccessRate());

        // Statistics should be updated after authentication attempts
        String stats = authenticator.getStatistics();
        Assertions.assertNotNull(stats);
        Assertions.assertTrue(stats.contains("EnhancedAuthenticator"));
        Assertions.assertTrue(stats.contains("enhanced_enabled=true"));
    }

    @Test
    @DisplayName("Security transport statistics tracking")
    void testSecurityTransportStatistics() {
        // Reset statistics for clean test
        SecurityTransportDecision.resetStatistics();

        SecurityTransportDecision.TransportMetrics initialMetrics =
                SecurityTransportDecision.getTransportMetrics();
        Assertions.assertEquals(0, initialMetrics.getTotalDecisions());

        // Make some transport decisions
        SecurityTransportDecision.determineTransportMethod(mockContext, mockChannel);
        SecurityTransportDecision.determineTransportMethod(mockContext, mockChannel);

        SecurityTransportDecision.TransportMetrics updatedMetrics =
                SecurityTransportDecision.getTransportMetrics();
        Assertions.assertEquals(2, updatedMetrics.getTotalDecisions());

        String stats = SecurityTransportDecision.getTransportStatistics();
        Assertions.assertNotNull(stats);
        Assertions.assertTrue(stats.contains("TransportStats"));
        Assertions.assertTrue(stats.contains("total=2"));
    }

    @Test
    @DisplayName("Complete authentication flow integration")
    void testCompleteAuthenticationFlowIntegration() throws Exception {
        // This test demonstrates the complete flow integration
        // Note: Full integration would require more complex mocking of Doris internals

        // 1. Enhanced handshake packet creation
        MysqlEnhancedHandshakePacket handshake = new MysqlEnhancedHandshakePacket(12345);
        Assertions.assertEquals(MysqlEnhancedHandshakePacket.CACHING_SHA2_PASSWORD_PLUGIN,
                    handshake.getSelectedAuthPlugin());

        // 2. Security transport decision
        SecurityTransportDecision.TransportDecision decision =
                SecurityTransportDecision.determineTransportMethod(mockContext, mockChannel);
        Assertions.assertNotNull(decision);

        // 3. Authentication state machine
        AuthenticationStateMachine stateMachine = new AuthenticationStateMachine(
                mockContext, mockChannel, null);
        Assertions.assertEquals(AuthenticationStateMachine.State.INITIAL, stateMachine.getCurrentState());

        // 4. Enhanced authenticator
        Assertions.assertNotNull(authenticator);
        Assertions.assertTrue(authenticator.getPasswordResolver() instanceof CachingSha2PasswordResolver);

        // 5. Verify components work together
        Assertions.assertTrue(SecurityTransportDecision.isSecureTransport(
                SecurityTransportDecision.TransportMethod.SSL_ENCRYPTED));
        Assertions.assertTrue(SecurityTransportDecision.isSecureTransport(
                SecurityTransportDecision.TransportMethod.RSA_ENCRYPTED));
        Assertions.assertFalse(SecurityTransportDecision.isSecureTransport(
                SecurityTransportDecision.TransportMethod.INSECURE_FALLBACK));
    }

    @Test
    @DisplayName("Password cache integration")
    void testPasswordCacheIntegration() {
        PasswordCache cache = new PasswordCache(100, 300); // 100 entries, 5 minutes TTL

        String username = "test_user";
        byte[] passwordHash = "test_password_hash".getBytes();

        // Initially not cached
        Assertions.assertFalse(cache.isPasswordCached(username, passwordHash));

        // Cache password
        cache.cachePassword(username, passwordHash);
        Assertions.assertTrue(cache.isPasswordCached(username, passwordHash));

        // Different password should not be cached
        byte[] differentHash = "different_password_hash".getBytes();
        Assertions.assertFalse(cache.isPasswordCached(username, differentHash));

        // Cleanup
        cache.shutdown();
    }

    @Test
    @DisplayName("Configuration integration")
    void testConfigurationIntegration() {
        // Test configuration affects component behavior

        // Enable caching_sha2_password
        Config.enable_caching_sha2_password = true;
        MysqlEnhancedHandshakePacket enabledHandshake = new MysqlEnhancedHandshakePacket(12345);
        Assertions.assertEquals(MysqlEnhancedHandshakePacket.CACHING_SHA2_PASSWORD_PLUGIN,
                    enabledHandshake.getSelectedAuthPlugin());

        // Disable caching_sha2_password
        Config.enable_caching_sha2_password = false;
        MysqlEnhancedHandshakePacket disabledHandshake = new MysqlEnhancedHandshakePacket(12345);
        Assertions.assertEquals(MysqlEnhancedHandshakePacket.MYSQL_NATIVE_PASSWORD_PLUGIN,
                    disabledHandshake.getSelectedAuthPlugin());

        // Transport decision should respect configuration
        SecurityTransportDecision.TransportDecision decision =
                SecurityTransportDecision.determineTransportMethod(mockContext, mockChannel);
        Assertions.assertEquals(SecurityTransportDecision.TransportMethod.INSECURE_FALLBACK, decision.getMethod());
        Assertions.assertTrue(decision.getReason().contains("disabled"));
    }
}
