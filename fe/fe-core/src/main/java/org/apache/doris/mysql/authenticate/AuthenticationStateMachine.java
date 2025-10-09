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
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlEnhancedHandshakePacket;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.password.CachingSha2PasswordResolver;
import org.apache.doris.mysql.authenticate.password.NativePasswordResolver;
import org.apache.doris.mysql.authenticate.password.Password;
import org.apache.doris.mysql.authenticate.password.PasswordResolver;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Authentication state machine for managing MySQL protocol authentication flows.
 *
 * This state machine coordinates the authentication process between clients and server,
 * handling different authentication plugins and their specific protocol requirements.
 * It manages state transitions, protocol packet exchanges, and authentication resolution.
 *
 * Supported authentication flows:
 * 1. caching_sha2_password - Multi-phase authentication with caching
 * 2. mysql_native_password - Legacy single-phase authentication
 * 3. Plugin switching - Dynamic plugin selection based on client capabilities
 */
public class AuthenticationStateMachine {
    private static final Logger LOG = LogManager.getLogger(AuthenticationStateMachine.class);
    
    /**
     * Authentication states in the state machine
     */
    public enum State {
        INITIAL,                    // Initial state before handshake
        HANDSHAKE_SENT,            // Handshake packet sent to client
        AUTH_PACKET_RECEIVED,      // Authentication packet received from client
        PLUGIN_SWITCH_REQUIRED,    // Plugin switch is required
        PLUGIN_SWITCH_SENT,        // Plugin switch request sent
        AUTHENTICATING,            // Authentication in progress
        AUTHENTICATED,             // Authentication successful
        AUTHENTICATION_FAILED,     // Authentication failed
        ERROR                      // Error state
    }
    
    /**
     * Authentication events that trigger state transitions
     */
    public enum Event {
        SEND_HANDSHAKE,           // Send handshake packet
        RECEIVE_AUTH_PACKET,      // Receive authentication packet
        PLUGIN_MISMATCH,          // Authentication plugin mismatch
        SEND_PLUGIN_SWITCH,       // Send plugin switch request
        RECEIVE_PLUGIN_RESPONSE,  // Receive plugin switch response
        AUTHENTICATE_SUCCESS,     // Authentication successful
        AUTHENTICATE_FAILURE,     // Authentication failed
        ERROR_OCCURRED           // Error occurred
    }
    
    // State machine instance variables
    private State currentState;
    private final ConnectContext context;
    private final MysqlChannel channel;
    private final MysqlSerializer serializer;
    
    // Authentication components
    private MysqlHandshakePacket handshakePacket;
    private MysqlAuthPacket authPacket;
    private String selectedAuthPlugin;
    private PasswordResolver passwordResolver;
    private Password resolvedPassword;
    
    // Statistics and monitoring
    private final AtomicLong stateTransitions = new AtomicLong(0);
    private final Map<String, AtomicLong> pluginUsageStats = new HashMap<>();
    private long authStartTime;
    private long authEndTime;
    
    /**
     * Constructor for authentication state machine
     */
    public AuthenticationStateMachine(ConnectContext context, MysqlChannel channel, MysqlSerializer serializer) {
        this.context = context;
        this.channel = channel;
        this.serializer = serializer;
        this.currentState = State.INITIAL;
        this.authStartTime = System.currentTimeMillis();
        
        // Initialize plugin usage statistics
        pluginUsageStats.put(MysqlEnhancedHandshakePacket.CACHING_SHA2_PASSWORD_PLUGIN, new AtomicLong(0));
        pluginUsageStats.put(MysqlEnhancedHandshakePacket.MYSQL_NATIVE_PASSWORD_PLUGIN, new AtomicLong(0));
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Authentication state machine initialized for connection: {}",
                    context.getConnectionId());
        }
    }
    
    /**
     * Process an authentication event and transition state
     */
    public synchronized boolean processEvent(Event event) throws IOException {
        State previousState = currentState;
        boolean success = false;
        
        try {
            switch (currentState) {
                case INITIAL:
                    success = handleInitialState(event);
                    break;
                case HANDSHAKE_SENT:
                    success = handleHandshakeSentState(event);
                    break;
                case AUTH_PACKET_RECEIVED:
                    success = handleAuthPacketReceivedState(event);
                    break;
                case PLUGIN_SWITCH_REQUIRED:
                    success = handlePluginSwitchRequiredState(event);
                    break;
                case PLUGIN_SWITCH_SENT:
                    success = handlePluginSwitchSentState(event);
                    break;
                case AUTHENTICATING:
                    success = handleAuthenticatingState(event);
                    break;
                case AUTHENTICATED:
                case AUTHENTICATION_FAILED:
                case ERROR:
                    // Terminal states - no further transitions
                    success = false;
                    break;
                default:
                    LOG.error("Unknown authentication state: {}", currentState);
                    transitionTo(State.ERROR);
                    success = false;
            }
            
            if (LOG.isDebugEnabled() && previousState != currentState) {
                LOG.debug("Authentication state transition: {} -> {} (event: {}, success: {})",
                        previousState, currentState, event, success);
            }
            
        } catch (Exception e) {
            LOG.error("Error processing authentication event {} in state {}", event, currentState, e);
            transitionTo(State.ERROR);
            success = false;
        }
        
        return success;
    }
    
    /**
     * Handle events in INITIAL state
     */
    private boolean handleInitialState(Event event) throws IOException {
        if (event == Event.SEND_HANDSHAKE) {
            // Create and send handshake packet
            int connectionId = context.getConnectionId();
            
            if (Config.enable_caching_sha2_password) {
                handshakePacket = new MysqlEnhancedHandshakePacket(connectionId);
                selectedAuthPlugin = MysqlEnhancedHandshakePacket.CACHING_SHA2_PASSWORD_PLUGIN;
            } else {
                handshakePacket = new MysqlHandshakePacket(connectionId);
                selectedAuthPlugin = MysqlHandshakePacket.AUTH_PLUGIN_NAME;
            }
            
            // Send handshake packet
            serializer.reset();
            handshakePacket.writeTo(serializer);
            channel.sendAndFlush(serializer.toByteBuffer());
            
            transitionTo(State.HANDSHAKE_SENT);
            return true;
        }
        
        return false;
    }
    
    /**
     * Handle events in HANDSHAKE_SENT state
     */
    private boolean handleHandshakeSentState(Event event) throws IOException {
        if (event == Event.RECEIVE_AUTH_PACKET) {
            // Authentication packet should be received and parsed externally
            // This event indicates the packet is ready for processing
            transitionTo(State.AUTH_PACKET_RECEIVED);
            return true;
        }
        
        return false;
    }
    
    /**
     * Handle events in AUTH_PACKET_RECEIVED state
     */
    private boolean handleAuthPacketReceivedState(Event event) throws IOException {
        if (event == Event.PLUGIN_MISMATCH) {
            transitionTo(State.PLUGIN_SWITCH_REQUIRED);
            return true;
        } else if (event == Event.AUTHENTICATE_SUCCESS || event == Event.AUTHENTICATE_FAILURE) {
            // Direct authentication without plugin switch
            return startAuthentication();
        }
        
        return false;
    }
    
    /**
     * Handle events in PLUGIN_SWITCH_REQUIRED state
     */
    private boolean handlePluginSwitchRequiredState(Event event) throws IOException {
        if (event == Event.SEND_PLUGIN_SWITCH) {
            // Send plugin switch request
            serializer.reset();
            handshakePacket.buildAuthSwitchRequest(serializer);
            channel.sendAndFlush(serializer.toByteBuffer());
            
            transitionTo(State.PLUGIN_SWITCH_SENT);
            return true;
        }
        
        return false;
    }
    
    /**
     * Handle events in PLUGIN_SWITCH_SENT state
     */
    private boolean handlePluginSwitchSentState(Event event) throws IOException {
        if (event == Event.RECEIVE_PLUGIN_RESPONSE) {
            // Plugin response received, start authentication
            return startAuthentication();
        }
        
        return false;
    }
    
    /**
     * Handle events in AUTHENTICATING state
     */
    private boolean handleAuthenticatingState(Event event) throws IOException {
        if (event == Event.AUTHENTICATE_SUCCESS) {
            transitionTo(State.AUTHENTICATED);
            authEndTime = System.currentTimeMillis();
            
            // Update statistics
            pluginUsageStats.get(selectedAuthPlugin).incrementAndGet();
            
            if (LOG.isInfoEnabled()) {
                LOG.info("Authentication successful for user: {} using plugin: {} (duration: {}ms)",
                        authPacket != null ? authPacket.getUser() : "unknown",
                        selectedAuthPlugin,
                        authEndTime - authStartTime);
            }
            
            return true;
        } else if (event == Event.AUTHENTICATE_FAILURE) {
            transitionTo(State.AUTHENTICATION_FAILED);
            authEndTime = System.currentTimeMillis();
            
            if (LOG.isWarnEnabled()) {
                LOG.warn("Authentication failed for user: {} using plugin: {} (duration: {}ms)",
                        authPacket != null ? authPacket.getUser() : "unknown",
                        selectedAuthPlugin,
                        authEndTime - authStartTime);
            }
            
            return false;
        }
        
        return false;
    }
    
    /**
     * Start the authentication process
     */
    private boolean startAuthentication() throws IOException {
        transitionTo(State.AUTHENTICATING);
        
        // Create appropriate password resolver based on selected plugin
        if (MysqlEnhancedHandshakePacket.CACHING_SHA2_PASSWORD_PLUGIN.equals(selectedAuthPlugin)) {
            passwordResolver = new CachingSha2PasswordResolver();
        } else {
            // For mysql_native_password, use NativePasswordResolver
            passwordResolver = new NativePasswordResolver();
        }
        
        // Resolve password using the appropriate resolver
        Optional<Password> passwordOpt = passwordResolver.resolvePassword(
                context, channel, serializer, authPacket, handshakePacket);
        
        if (passwordOpt.isPresent()) {
            resolvedPassword = passwordOpt.get();
            return processEvent(Event.AUTHENTICATE_SUCCESS);
        } else {
            return processEvent(Event.AUTHENTICATE_FAILURE);
        }
    }
    
    /**
     * Transition to a new state
     */
    private void transitionTo(State newState) {
        State oldState = currentState;
        currentState = newState;
        stateTransitions.incrementAndGet();
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("State transition: {} -> {}", oldState, newState);
        }
    }
    
    /**
     * Set the authentication packet received from client
     */
    public void setAuthPacket(MysqlAuthPacket authPacket) {
        this.authPacket = authPacket;
    }
    
    /**
     * Check if authentication plugin matches
     */
    public boolean checkAuthPluginMatch(String clientAuthPlugin) {
        if (handshakePacket == null) {
            return false;
        }
        
        return handshakePacket.checkAuthPluginSameAsDoris(clientAuthPlugin);
    }
    
    /**
     * Get current authentication state
     */
    public State getCurrentState() {
        return currentState;
    }
    
    /**
     * Check if authentication is complete (success or failure)
     */
    public boolean isAuthenticationComplete() {
        return currentState == State.AUTHENTICATED
                || currentState == State.AUTHENTICATION_FAILED
                || currentState == State.ERROR;
    }
    
    /**
     * Check if authentication was successful
     */
    public boolean isAuthenticationSuccessful() {
        return currentState == State.AUTHENTICATED;
    }
    
    /**
     * Get the resolved password (only available after successful authentication)
     */
    public Optional<Password> getResolvedPassword() {
        return Optional.ofNullable(resolvedPassword);
    }
    
    /**
     * Get the selected authentication plugin
     */
    public String getSelectedAuthPlugin() {
        return selectedAuthPlugin;
    }
    
    /**
     * Get authentication duration in milliseconds
     */
    public long getAuthenticationDuration() {
        if (authEndTime > 0) {
            return authEndTime - authStartTime;
        } else {
            return System.currentTimeMillis() - authStartTime;
        }
    }
    
    /**
     * Get statistics for monitoring
     */
    public String getStatistics() {
        return String.format("AuthStateMachine[state=%s, plugin=%s, transitions=%d, duration=%dms,"
                + " caching_sha2_usage=%d, native_password_usage=%d]",
                currentState, selectedAuthPlugin, stateTransitions.get(), getAuthenticationDuration(),
                pluginUsageStats.get(MysqlEnhancedHandshakePacket.CACHING_SHA2_PASSWORD_PLUGIN).get(),
                pluginUsageStats.get(MysqlEnhancedHandshakePacket.MYSQL_NATIVE_PASSWORD_PLUGIN).get());
    }
    
    /**
     * Reset the state machine for reuse (if needed)
     */
    public void reset() {
        currentState = State.INITIAL;
        handshakePacket = null;
        authPacket = null;
        selectedAuthPlugin = null;
        passwordResolver = null;
        resolvedPassword = null;
        authStartTime = System.currentTimeMillis();
        authEndTime = 0;
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Authentication state machine reset");
        }
    }
    
    /**
     * Cleanup resources
     */
    public void cleanup() {
        if (resolvedPassword != null) {
            resolvedPassword.clearPassword();
        }
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Authentication state machine cleanup completed");
        }
    }
}
