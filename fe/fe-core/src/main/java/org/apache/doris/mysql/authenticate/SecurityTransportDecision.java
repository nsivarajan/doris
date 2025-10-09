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
import org.apache.doris.mysql.authenticate.RSAKeyManager;
import org.apache.doris.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Security transport decision logic for caching_sha2_password authentication.
 * 
 * This class determines the optimal security transport method for password transmission
 * during caching_sha2_password authentication. It evaluates SSL availability, RSA key
 * availability, and configuration settings to make intelligent security decisions.
 * 
 * Decision hierarchy:
 * 1. SSL/TLS (preferred) - Direct encrypted transmission
 * 2. RSA encryption - Public key encryption for non-SSL connections
 * 3. Fallback to mysql_native_password - For compatibility
 */
public class SecurityTransportDecision {
    private static final Logger LOG = LogManager.getLogger(SecurityTransportDecision.class);
    
    /**
     * Security transport methods available for password transmission
     */
    public enum TransportMethod {
        /** SSL/TLS encrypted connection (most secure) */
        SSL_ENCRYPTED,
        
        /** RSA public key encryption (secure for non-SSL) */
        RSA_ENCRYPTED,
        
        /** No secure transport available (fallback to legacy) */
        INSECURE_FALLBACK,
        
        /** Transport method cannot be determined */
        UNKNOWN
    }
    
    /**
     * Decision result containing transport method and reasoning
     */
    public static class TransportDecision {
        private final TransportMethod method;
        private final String reason;
        private final boolean isSecure;
        private final boolean requiresRSAKey;
        
        public TransportDecision(TransportMethod method, String reason, boolean isSecure, boolean requiresRSAKey) {
            this.method = method;
            this.reason = reason;
            this.isSecure = isSecure;
            this.requiresRSAKey = requiresRSAKey;
        }
        
        public TransportMethod getMethod() { return method; }
        public String getReason() { return reason; }
        public boolean isSecure() { return isSecure; }
        public boolean requiresRSAKey() { return requiresRSAKey; }
        
        @Override
        public String toString() {
            return String.format("TransportDecision[method=%s, secure=%s, reason=%s]", 
                    method, isSecure, reason);
        }
    }
    
    // Statistics for monitoring transport method usage
    private static final AtomicLong sslTransportCount = new AtomicLong(0);
    private static final AtomicLong rsaTransportCount = new AtomicLong(0);
    private static final AtomicLong insecureFallbackCount = new AtomicLong(0);
    private static final AtomicLong decisionCount = new AtomicLong(0);
    
    /**
     * Determine the optimal security transport method for password transmission
     * 
     * @param context Connection context
     * @param channel MySQL channel for communication
     * @return Transport decision with method and reasoning
     */
    public static TransportDecision determineTransportMethod(ConnectContext context, MysqlChannel channel) {
        decisionCount.incrementAndGet();
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Determining security transport method for connection: {}", context.getConnectionId());
        }
        
        // Check if caching_sha2_password is enabled
        if (!Config.enable_caching_sha2_password) {
            return new TransportDecision(
                TransportMethod.INSECURE_FALLBACK,
                "caching_sha2_password disabled in configuration",
                false,
                false
            );
        }
        
        // Priority 1: Check SSL/TLS availability
        TransportDecision sslDecision = evaluateSSLTransport(channel);
        if (sslDecision.getMethod() == TransportMethod.SSL_ENCRYPTED) {
            sslTransportCount.incrementAndGet();
            return sslDecision;
        }
        
        // Priority 2: Check RSA encryption availability
        TransportDecision rsaDecision = evaluateRSATransport();
        if (rsaDecision.getMethod() == TransportMethod.RSA_ENCRYPTED) {
            rsaTransportCount.incrementAndGet();
            return rsaDecision;
        }
        
        // Priority 3: Fallback to insecure transport
        insecureFallbackCount.incrementAndGet();
        LOG.warn("No secure transport available for connection: {}, falling back to insecure method", 
                context.getConnectionId());
        
        return new TransportDecision(
            TransportMethod.INSECURE_FALLBACK,
            "No SSL or RSA encryption available",
            false,
            false
        );
    }
    
    /**
     * Evaluate SSL/TLS transport availability
     */
    private static TransportDecision evaluateSSLTransport(MysqlChannel channel) {
        try {
            // Check if SSL is enabled in the channel
            if (channel.isSslMode()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("SSL transport available and active");
                }
                
                return new TransportDecision(
                    TransportMethod.SSL_ENCRYPTED,
                    "SSL/TLS connection established",
                    true,
                    false
                );
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("SSL transport not available - channel not in SSL mode");
                }
                
                return new TransportDecision(
                    TransportMethod.UNKNOWN,
                    "SSL not enabled on channel",
                    false,
                    false
                );
            }
        } catch (Exception e) {
            LOG.warn("Error evaluating SSL transport availability", e);
            return new TransportDecision(
                TransportMethod.UNKNOWN,
                "Error checking SSL availability: " + e.getMessage(),
                false,
                false
            );
        }
    }
    
    /**
     * Evaluate RSA encryption transport availability
     */
    private static TransportDecision evaluateRSATransport() {
        try {
            RSAKeyManager keyManager = RSAKeyManager.getInstance();
            
            // Check if RSA key manager is available and has keys
            if (keyManager.getPublicKey() != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("RSA transport available with key size: {} bits", 
                            keyManager.getPublicKey().getModulus().bitLength());
                }
                
                return new TransportDecision(
                    TransportMethod.RSA_ENCRYPTED,
                    "RSA public key encryption available",
                    true,
                    true
                );
            } else {
                LOG.warn("RSA key manager available but no public key found");
                return new TransportDecision(
                    TransportMethod.UNKNOWN,
                    "RSA key manager has no public key",
                    false,
                    false
                );
            }
        } catch (Exception e) {
            LOG.error("Error evaluating RSA transport availability", e);
            return new TransportDecision(
                TransportMethod.UNKNOWN,
                "Error checking RSA availability: " + e.getMessage(),
                false,
                false
            );
        }
    }
    
    /**
     * Check if the transport method is secure
     */
    public static boolean isSecureTransport(TransportMethod method) {
        return method == TransportMethod.SSL_ENCRYPTED || method == TransportMethod.RSA_ENCRYPTED;
    }
    
    /**
     * Check if the transport method requires RSA keys
     */
    public static boolean requiresRSAKeys(TransportMethod method) {
        return method == TransportMethod.RSA_ENCRYPTED;
    }
    
    /**
     * Get recommended transport method based on configuration and capabilities
     */
    public static TransportMethod getRecommendedTransport(boolean sslAvailable, boolean rsaAvailable) {
        if (sslAvailable) {
            return TransportMethod.SSL_ENCRYPTED;
        } else if (rsaAvailable) {
            return TransportMethod.RSA_ENCRYPTED;
        } else {
            return TransportMethod.INSECURE_FALLBACK;
        }
    }
    
    /**
     * Validate transport decision against security requirements
     */
    public static boolean validateTransportSecurity(TransportDecision decision, boolean requireSecure) {
        if (requireSecure && !decision.isSecure()) {
            LOG.warn("Transport decision {} does not meet security requirements", decision);
            return false;
        }
        return true;
    }
    
    /**
     * Get transport method statistics for monitoring
     */
    public static String getTransportStatistics() {
        long total = decisionCount.get();
        long sslCount = sslTransportCount.get();
        long rsaCount = rsaTransportCount.get();
        long insecureCount = insecureFallbackCount.get();
        
        double sslPercent = total > 0 ? (double) sslCount / total * 100.0 : 0.0;
        double rsaPercent = total > 0 ? (double) rsaCount / total * 100.0 : 0.0;
        double insecurePercent = total > 0 ? (double) insecureCount / total * 100.0 : 0.0;
        
        return String.format("TransportStats[total=%d, SSL=%d(%.1f%%), RSA=%d(%.1f%%), insecure=%d(%.1f%%)]",
                total, sslCount, sslPercent, rsaCount, rsaPercent, insecureCount, insecurePercent);
    }
    
    /**
     * Get detailed transport metrics
     */
    public static TransportMetrics getTransportMetrics() {
        return new TransportMetrics(
                decisionCount.get(),
                sslTransportCount.get(),
                rsaTransportCount.get(),
                insecureFallbackCount.get()
        );
    }
    
    /**
     * Reset transport statistics (for testing or administrative purposes)
     */
    public static void resetStatistics() {
        decisionCount.set(0);
        sslTransportCount.set(0);
        rsaTransportCount.set(0);
        insecureFallbackCount.set(0);
        
        LOG.info("Security transport statistics reset");
    }
    
    /**
     * Transport metrics data class
     */
    public static class TransportMetrics {
        private final long totalDecisions;
        private final long sslCount;
        private final long rsaCount;
        private final long insecureCount;
        
        public TransportMetrics(long totalDecisions, long sslCount, long rsaCount, long insecureCount) {
            this.totalDecisions = totalDecisions;
            this.sslCount = sslCount;
            this.rsaCount = rsaCount;
            this.insecureCount = insecureCount;
        }
        
        public long getTotalDecisions() { return totalDecisions; }
        public long getSslCount() { return sslCount; }
        public long getRsaCount() { return rsaCount; }
        public long getInsecureCount() { return insecureCount; }
        public long getSecureCount() { return sslCount + rsaCount; }
        
        public double getSslPercentage() {
            return totalDecisions > 0 ? (double) sslCount / totalDecisions * 100.0 : 0.0;
        }
        
        public double getRsaPercentage() {
            return totalDecisions > 0 ? (double) rsaCount / totalDecisions * 100.0 : 0.0;
        }
        
        public double getSecurePercentage() {
            return totalDecisions > 0 ? (double) getSecureCount() / totalDecisions * 100.0 : 0.0;
        }
        
        public double getInsecurePercentage() {
            return totalDecisions > 0 ? (double) insecureCount / totalDecisions * 100.0 : 0.0;
        }
    }
}
