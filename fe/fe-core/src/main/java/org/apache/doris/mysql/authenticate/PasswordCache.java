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

import org.apache.doris.mysql.MysqlSha2Password;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.MessageDigest;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe password cache for caching_sha2_password authentication.
 *
 * This cache stores SHA-256 password hashes to enable fast authentication
 * for repeat connections. It implements:
 * - LRU eviction policy when cache is full
 * - TTL-based expiration for security
 * - Thread-safe concurrent access
 * - Performance metrics and monitoring
 * - Automatic cleanup of expired entries
 */
public class PasswordCache {
    private static final Logger LOG = LogManager.getLogger(PasswordCache.class);

    // Cache configuration
    private final int maxSize;
    private final long ttlMillis;
    private final ConcurrentHashMap<String, CacheEntry> cache;
    private final ScheduledExecutorService cleanupExecutor;

    // Performance metrics
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);
    private final AtomicLong evictionCount = new AtomicLong(0);
    private final AtomicLong expiredCount = new AtomicLong(0);

    // Cleanup configuration
    private static final long CLEANUP_INTERVAL_SECONDS = 60; // Clean up every minute
    private static final String CLEANUP_THREAD_NAME = "password-cache-cleanup";

    /**
     * Cache entry containing password hash and metadata
     */
    private static class CacheEntry {
        final byte[] passwordHash;
        final long timestamp;
        final AtomicLong accessCount;
        volatile long lastAccessTime;

        CacheEntry(byte[] passwordHash) {
            this.passwordHash = passwordHash != null ? passwordHash.clone() : new byte[0];
            this.timestamp = System.currentTimeMillis();
            this.lastAccessTime = this.timestamp;
            this.accessCount = new AtomicLong(0);
        }

        /**
         * Check if entry has expired based on TTL
         */
        boolean isExpired(long ttlMillis) {
            return System.currentTimeMillis() - timestamp > ttlMillis;
        }

        /**
         * Update access time and increment access count
         */
        void recordAccess() {
            this.lastAccessTime = System.currentTimeMillis();
            this.accessCount.incrementAndGet();
        }

        /**
         * Get age of entry in milliseconds
         */
        long getAge() {
            return System.currentTimeMillis() - timestamp;
        }

        /**
         * Clear sensitive data from memory
         */
        void clearSensitiveData() {
            MysqlSha2Password.clearSensitiveData(passwordHash);
        }
    }

    /**
     * Create password cache with specified configuration
     *
     * @param maxSize Maximum number of entries in cache
     * @param ttlSeconds Time-to-live for cache entries in seconds
     */
    public PasswordCache(int maxSize, long ttlSeconds) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("Cache max size must be positive");
        }
        if (ttlSeconds <= 0) {
            throw new IllegalArgumentException("Cache TTL must be positive");
        }

        this.maxSize = maxSize;
        this.ttlMillis = ttlSeconds * 1000;
        this.cache = new ConcurrentHashMap<>(Math.min(maxSize, 1024)); // Initial capacity

        // Create cleanup executor with daemon thread
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, CLEANUP_THREAD_NAME);
            t.setDaemon(true);
            return t;
        });

        // Schedule periodic cleanup
        cleanupExecutor.scheduleAtFixedRate(
                this::cleanup,
                CLEANUP_INTERVAL_SECONDS,
                CLEANUP_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );

        LOG.info("Password cache initialized: maxSize={}, ttlSeconds={}", maxSize, ttlSeconds);
    }

    /**
     * Check if password is cached and valid
     *
     * @param username Username to check
     * @param scrambledPassword Scrambled password to verify
     * @return true if password is cached and matches
     */
    public boolean isPasswordCached(String username, byte[] scrambledPassword) {
        if (username == null || scrambledPassword == null) {
            return false;
        }

        CacheEntry entry = cache.get(username);
        if (entry == null) {
            missCount.incrementAndGet();
            return false;
        }

        // Check if entry has expired
        if (entry.isExpired(ttlMillis)) {
            cache.remove(username, entry); // Remove expired entry
            entry.clearSensitiveData();
            expiredCount.incrementAndGet();
            missCount.incrementAndGet();
            return false;
        }

        // Verify password hash matches
        boolean matches = MessageDigest.isEqual(entry.passwordHash, scrambledPassword);
        if (matches) {
            entry.recordAccess();
            hitCount.incrementAndGet();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Password cache hit for user: {}", username);
            }
        } else {
            missCount.incrementAndGet();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Password cache miss for user: {} (hash mismatch)", username);
            }
        }

        return matches;
    }

    /**
     * Cache password hash for user
     *
     * @param username Username to cache
     * @param passwordHash SHA-256 password hash to cache
     */
    public void cachePassword(String username, byte[] passwordHash) {
        if (username == null || passwordHash == null) {
            LOG.warn("Cannot cache null username or password hash");
            return;
        }

        // Check if cache is full and needs eviction
        if (cache.size() >= maxSize) {
            evictOldestEntry();
        }

        CacheEntry newEntry = new CacheEntry(passwordHash);
        CacheEntry oldEntry = cache.put(username, newEntry);

        // Clear old entry if it existed
        if (oldEntry != null) {
            oldEntry.clearSensitiveData();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Cached password for user: {} (cache size: {})", username, cache.size());
        }
    }

    /**
     * Remove user from cache
     *
     * @param username Username to remove
     * @return true if user was cached and removed
     */
    public boolean removeUser(String username) {
        if (username == null) {
            return false;
        }

        CacheEntry removed = cache.remove(username);
        if (removed != null) {
            removed.clearSensitiveData();
            LOG.debug("Removed user from cache: {}", username);
            return true;
        }

        return false;
    }

    /**
     * Clear all entries from cache
     */
    public void clear() {
        // Clear sensitive data before removing entries
        cache.values().forEach(CacheEntry::clearSensitiveData);
        cache.clear();

        // Reset metrics
        hitCount.set(0);
        missCount.set(0);
        evictionCount.set(0);
        expiredCount.set(0);

        LOG.info("Password cache cleared");
    }

    /**
     * Get current cache size
     *
     * @return Number of entries in cache
     */
    public int size() {
        return cache.size();
    }

    /**
     * Get maximum cache size
     *
     * @return Maximum number of entries
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * Get cache TTL in seconds
     *
     * @return TTL in seconds
     */
    public long getTtlSeconds() {
        return ttlMillis / 1000;
    }

    /**
     * Get cache hit count
     *
     * @return Number of cache hits
     */
    public long getHitCount() {
        return hitCount.get();
    }

    /**
     * Get cache miss count
     *
     * @return Number of cache misses
     */
    public long getMissCount() {
        return missCount.get();
    }

    /**
     * Get cache hit rate
     *
     * @return Hit rate as percentage (0.0 to 1.0)
     */
    public double getHitRate() {
        long hits = hitCount.get();
        long total = hits + missCount.get();
        return total > 0 ? (double) hits / total : 0.0;
    }

    /**
     * Get eviction count
     *
     * @return Number of entries evicted due to size limit
     */
    public long getEvictionCount() {
        return evictionCount.get();
    }

    /**
     * Get expired entry count
     *
     * @return Number of entries that expired
     */
    public long getExpiredCount() {
        return expiredCount.get();
    }

    /**
     * Get cache statistics as formatted string
     *
     * @return Cache statistics
     */
    public String getStatistics() {
        return String.format(
            "PasswordCache[size=%d/%d, hits=%d, misses=%d, hitRate=%.2f%%, evictions=%d, expired=%d]",
            size(), maxSize, getHitCount(), getMissCount(), getHitRate() * 100,
            getEvictionCount(), getExpiredCount()
        );
    }

    /**
     * Shutdown cache and cleanup resources
     */
    public void shutdown() {
        LOG.info("Shutting down password cache");

        // Shutdown cleanup executor
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Clear cache
        clear();

        LOG.info("Password cache shutdown complete");
    }

    /**
     * Periodic cleanup of expired entries
     */
    private void cleanup() {
        try {
            long startTime = System.currentTimeMillis();
            int initialSize = cache.size();
            int removedCount = 0;

            // Remove expired entries
            cache.entrySet().removeIf(entry -> {
                if (entry.getValue().isExpired(ttlMillis)) {
                    entry.getValue().clearSensitiveData();
                    return true;
                }
                return false;
            });

            removedCount = initialSize - cache.size();
            if (removedCount > 0) {
                expiredCount.addAndGet(removedCount);
                long duration = System.currentTimeMillis() - startTime;
                LOG.debug("Cache cleanup removed {} expired entries in {}ms", removedCount, duration);
            }

        } catch (Exception e) {
            LOG.warn("Error during cache cleanup", e);
        }
    }

    /**
     * Evict oldest entry when cache is full (LRU eviction)
     */
    private void evictOldestEntry() {
        if (cache.isEmpty()) {
            return;
        }

        // Find entry with oldest timestamp (LRU based on creation time)
        cache.entrySet().stream()
                .min(Comparator.comparing(entry -> entry.getValue().timestamp))
                .ifPresent(entry -> {
                    String username = entry.getKey();
                    CacheEntry cacheEntry = entry.getValue();

                    if (cache.remove(username, cacheEntry)) {
                        cacheEntry.clearSensitiveData();
                        evictionCount.incrementAndGet();

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Evicted oldest cache entry for user: {} (age: {}ms)",
                                    username, cacheEntry.getAge());
                        }
                    }
                });
    }
}
