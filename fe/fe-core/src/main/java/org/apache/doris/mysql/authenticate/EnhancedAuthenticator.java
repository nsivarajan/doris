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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.authenticate.password.CachingSha2Password;
import org.apache.doris.mysql.authenticate.password.CachingSha2PasswordResolver;
import org.apache.doris.mysql.authenticate.password.NativePassword;
import org.apache.doris.mysql.authenticate.password.NativePasswordResolver;
import org.apache.doris.mysql.authenticate.password.Password;
import org.apache.doris.mysql.authenticate.password.PasswordResolver;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enhanced authenticator that supports both caching_sha2_password and mysql_native_password.
 * 
 * This authenticator provides a unified authentication interface that can handle multiple
 * authentication plugins based on configuration and client capabilities. It maintains
 * backward compatibility while enabling enhanced security features.
 * 
 * Features:
 * - Dynamic password resolver selection based on password type
 * - Support for both caching_sha2_password and mysql_native_password
 * - Fallback mechanisms for compatibility
 * - Performance monitoring and statistics
 * - Secure password handling and cleanup
 */
public class EnhancedAuthenticator implements Authenticator {
    private static final Logger LOG = LogManager.getLogger(EnhancedAuthenticator.class);
    
    // Password resolvers for different authentication methods
    private final PasswordResolver cachingSha2PasswordResolver;
    private final PasswordResolver nativePasswordResolver;
    
    // Statistics and monitoring
    private final AtomicLong cachingSha2AuthCount = new AtomicLong(0);
    private final AtomicLong nativePasswordAuthCount = new AtomicLong(0);
    private final AtomicLong successfulAuthCount = new AtomicLong(0);
    private final AtomicLong failedAuthCount = new AtomicLong(0);
    
    /**
     * Constructor initializes both password resolvers
     */
    public EnhancedAuthenticator() {
        this.cachingSha2PasswordResolver = new CachingSha2PasswordResolver();
        this.nativePasswordResolver = new NativePasswordResolver();
        
        LOG.info("Enhanced authenticator initialized with caching_sha2_password support: {}", 
                Config.enable_caching_sha2_password);
    }
    
    @Override
    public AuthenticateResponse authenticate(AuthenticateRequest request) throws IOException {
        String userName = request.getUserName();
        String remoteIp = request.getRemoteIp();
        Password password = request.getPassword();
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enhanced authentication started for user: {} from IP: {} using password type: {}", 
                    userName, remoteIp, password.getClass().getSimpleName());
        }
        
        try {
            // Determine authentication method based on password type
            if (password instanceof CachingSha2Password) {
                return authenticateWithCachingSha2Password(request, (CachingSha2Password) password);
            } else if (password instanceof NativePassword) {
                return authenticateWithNativePassword(request, (NativePassword) password);
            } else {
                LOG.warn("Unknown password type for user {}: {}", userName, password.getClass().getName());
                failedAuthCount.incrementAndGet();
                return AuthenticateResponse.failedResponse;
            }
            
        } catch (Exception e) {
            LOG.error("Authentication error for user {} from IP {}", userName, remoteIp, e);
            failedAuthCount.incrementAndGet();
            return AuthenticateResponse.failedResponse;
        } finally {
            // Always clear sensitive password data after authentication
            if (password != null) {
                password.clearPassword();
            }
        }
    }
    
    /**
     * Authenticate using caching_sha2_password method
     */
    private AuthenticateResponse authenticateWithCachingSha2Password(AuthenticateRequest request, 
                                                                   CachingSha2Password password) 
            throws IOException {
        
        String userName = request.getUserName();
        String remoteIp = request.getRemoteIp();
        
        cachingSha2AuthCount.incrementAndGet();
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Authenticating user {} with caching_sha2_password (phase: {})", 
                    userName, password.getCurrentPhase());
        }
        
        // Verify password authentication was successful
        if (password.getCurrentPhase() != CachingSha2Password.AuthPhase.AUTH_COMPLETE) {
            LOG.warn("caching_sha2_password authentication not complete for user: {} (phase: {})",
                    userName, password.getCurrentPhase());
            failedAuthCount.incrementAndGet();
            return AuthenticateResponse.failedResponse;
        }
        
        // Get plain text password for user verification
        String plainTextPassword = password.getPlainTextPassword();
        if (plainTextPassword == null) {
            LOG.error("Plain text password not available after caching_sha2_password authentication for user: {}", 
                    userName);
            failedAuthCount.incrementAndGet();
            return AuthenticateResponse.failedResponse;
        }
        
        // Verify user credentials against Doris user system
        List<UserIdentity> currentUserIdentity = Lists.newArrayList();
        try {
            // Use the plain text password for internal authentication
            // Note: This is secure because the password was obtained through encrypted transmission
            Env.getCurrentEnv().getAuth().checkPlainPassword(userName, remoteIp, plainTextPassword, 
                    currentUserIdentity);
            
            successfulAuthCount.incrementAndGet();
            
            if (LOG.isInfoEnabled()) {
                LOG.info("caching_sha2_password authentication successful for user: {} from IP: {}", 
                        userName, remoteIp);
            }
            
            return new AuthenticateResponse(true, currentUserIdentity.get(0));
            
        } catch (AuthenticationException e) {
            LOG.warn("User verification failed for caching_sha2_password authentication: user={}, ip={}", 
                    userName, remoteIp, e);
            ErrorReport.report(e.errorCode, e.msgs);
            failedAuthCount.incrementAndGet();
            return AuthenticateResponse.failedResponse;
        }
    }
    
    /**
     * Authenticate using mysql_native_password method (legacy compatibility)
     */
    private AuthenticateResponse authenticateWithNativePassword(AuthenticateRequest request, 
                                                              NativePassword password) 
            throws IOException {
        
        String userName = request.getUserName();
        String remoteIp = request.getRemoteIp();
        
        nativePasswordAuthCount.incrementAndGet();
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Authenticating user {} with mysql_native_password (legacy mode)", userName);
        }
        
        // Use existing native password authentication logic
        List<UserIdentity> currentUserIdentity = Lists.newArrayList();
        try {
            Env.getCurrentEnv().getAuth().checkPassword(userName, remoteIp,
                    password.getRemotePasswd(), password.getRandomString(), currentUserIdentity);
            
            successfulAuthCount.incrementAndGet();
            
            if (LOG.isInfoEnabled()) {
                LOG.info("mysql_native_password authentication successful for user: {} from IP: {}", 
                        userName, remoteIp);
            }
            
            return new AuthenticateResponse(true, currentUserIdentity.get(0));
            
        } catch (AuthenticationException e) {
            LOG.warn("mysql_native_password authentication failed: user={}, ip={}", userName, remoteIp, e);
            ErrorReport.report(e.errorCode, e.msgs);
            failedAuthCount.incrementAndGet();
            return AuthenticateResponse.failedResponse;
        }
    }
    
    @Override
    public boolean canDeal(String qualifiedUser) {
        // Enhanced authenticator can handle all users
        return true;
    }
    
    @Override
    public PasswordResolver getPasswordResolver() {
        // Return the appropriate password resolver based on configuration
        if (Config.enable_caching_sha2_password) {
            return cachingSha2PasswordResolver;
        } else {
            return nativePasswordResolver;
        }
    }
    
    /**
     * Get the caching_sha2_password resolver specifically
     */
    public PasswordResolver getCachingSha2PasswordResolver() {
        return cachingSha2PasswordResolver;
    }
    
    /**
     * Get the native password resolver specifically
     */
    public PasswordResolver getNativePasswordResolver() {
        return nativePasswordResolver;
    }
    
    /**
     * Get authentication statistics for monitoring
     */
    public String getStatistics() {
        long totalAuth = cachingSha2AuthCount.get() + nativePasswordAuthCount.get();
        long successRate = totalAuth > 0 ? (successfulAuthCount.get() * 100 / totalAuth) : 0;
        
        return String.format("EnhancedAuthenticator[total=%d, success=%d, failed=%d, successRate=%d%%, " +
                           "caching_sha2=%d, native=%d, enhanced_enabled=%s]",
                totalAuth, successfulAuthCount.get(), failedAuthCount.get(), successRate,
                cachingSha2AuthCount.get(), nativePasswordAuthCount.get(), 
                Config.enable_caching_sha2_password);
    }
    
    /**
     * Get detailed authentication metrics
     */
    public AuthenticationMetrics getMetrics() {
        return new AuthenticationMetrics(
                cachingSha2AuthCount.get(),
                nativePasswordAuthCount.get(),
                successfulAuthCount.get(),
                failedAuthCount.get(),
                Config.enable_caching_sha2_password
        );
    }
    
    /**
     * Reset authentication statistics (for testing or administrative purposes)
     */
    public void resetStatistics() {
        cachingSha2AuthCount.set(0);
        nativePasswordAuthCount.set(0);
        successfulAuthCount.set(0);
        failedAuthCount.set(0);
        
        LOG.info("Enhanced authenticator statistics reset");
    }
    
    /**
     * Shutdown the authenticator and cleanup resources
     */
    public void shutdown() {
        LOG.info("Enhanced authenticator shutting down");
        
        // Cleanup password resolvers if they support it
        if (cachingSha2PasswordResolver instanceof CachingSha2PasswordResolver) {
            ((CachingSha2PasswordResolver) cachingSha2PasswordResolver).shutdown();
        }
        
        LOG.info("Enhanced authenticator shutdown complete. Final statistics: {}", getStatistics());
    }
    
    /**
     * Authentication metrics data class
     */
    public static class AuthenticationMetrics {
        private final long cachingSha2Count;
        private final long nativePasswordCount;
        private final long successCount;
        private final long failureCount;
        private final boolean enhancedEnabled;
        
        public AuthenticationMetrics(long cachingSha2Count, long nativePasswordCount, 
                                   long successCount, long failureCount, boolean enhancedEnabled) {
            this.cachingSha2Count = cachingSha2Count;
            this.nativePasswordCount = nativePasswordCount;
            this.successCount = successCount;
            this.failureCount = failureCount;
            this.enhancedEnabled = enhancedEnabled;
        }
        
        public long getCachingSha2Count() { return cachingSha2Count; }
        public long getNativePasswordCount() { return nativePasswordCount; }
        public long getSuccessCount() { return successCount; }
        public long getFailureCount() { return failureCount; }
        public boolean isEnhancedEnabled() { return enhancedEnabled; }
        public long getTotalCount() { return cachingSha2Count + nativePasswordCount; }
        public double getSuccessRate() { 
            long total = getTotalCount();
            return total > 0 ? (double) successCount / total * 100.0 : 0.0; 
        }
    }
}
