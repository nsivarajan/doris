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

package org.apache.doris.dr;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.dr.storage.DRStorageBackend;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/**
 * Runs on the STANDBY (DR) FE. Reads segment files from the relay bucket
 * and applies them to the local BDBJE journal, keeping the DR FE in sync
 * with the primary's metadata.
 *
 * Reads segment files in chronological order (lexicographic key order).
 * Buffers one segment at a time to bound heap usage.
 * Deduplicates entries (journal_id < nextJournalId) to handle safe re-reads
 * after a restart or segment boundary overlap.
 *
 * applyUntil(targetJournalId) is used during planned switchover to apply
 * all segments up to the agreed checkpoint before promotion.
 */
public class DRConsumer implements Runnable {

    private static final Logger LOG = LogManager.getLogger(DRConsumer.class);

    // how long to sleep when no new segments are available
    private static final long POLL_INTERVAL_MS = 1000;

    private final Env env;
    private final DRStorageBackend storage;
    private final DRConfig config;

    private volatile boolean running = true;

    // metrics exposed via DRManager.getStatus()
    private volatile long lastAppliedJournalId = 0;
    private volatile long lagMs = 0;
    private volatile long lagEntries = 0;

    // internal cursor state
    private long nextJournalId = 1;
    private String lastLoadedSegment = null;
    private final Queue<DRJournalEntry> buffer = new ArrayDeque<>();

    public DRConsumer(Env env, DRStorageBackend storage, DRConfig config) {
        this.env = env;
        this.storage = storage;
        this.config = config;
    }

    @Override
    public void run() {
        LOG.info("[DR:Consumer] started group={} site={}", config.groupId, config.siteName);
        recoverCursor();

        while (running) {
            try {
                applyNextBatch();
            } catch (Exception e) {
                LOG.warn("[DR:Consumer] apply failed, will retry: {}", e.getMessage(), e);
            }
            sleepSafely(config.consumePollIntervalMs);
        }
        LOG.info("[DR:Consumer] stopped group={} lastApplied={}",
                config.groupId, lastAppliedJournalId);
    }

    public void stop() {
        running = false;
    }

    public long getLagMs() { return lagMs; }
    public long getLagEntries() { return lagEntries; }
    public long getLastAppliedJournalId() { return lastAppliedJournalId; }

    /**
     * Blocks until all segments up to and including targetJournalId are applied.
     * Called during planned switchover to drain the relay before promotion.
     */
    public void applyUntil(long targetJournalId) throws Exception {
        LOG.info("[DR:Consumer] applying until journal_id={}", targetJournalId);
        while (lastAppliedJournalId < targetJournalId && running) {
            applyNextBatch();
            if (lastAppliedJournalId < targetJournalId) {
                sleepSafely(POLL_INTERVAL_MS);
            }
        }
        LOG.info("[DR:Consumer] reached target journal_id={}", targetJournalId);
    }

    // ── apply ─────────────────────────────────────────────────────────────

    private void applyNextBatch() throws Exception {
        // fill buffer from next available segment
        if (buffer.isEmpty()) {
            loadNextSegment();
        }
        if (buffer.isEmpty()) {
            return; // no new data yet
        }

        // apply all buffered entries via FE EditLog replay
        JournalCursor cursor = buildRelayedCursor();
        env.getEditLog().playbackJournalFromCursor(cursor);
    }

    /**
     * Builds a JournalCursor backed by our in-memory buffer,
     * so Doris's existing EditLog replay machinery can apply entries
     * without knowing they came from the relay bucket.
     */
    private JournalCursor buildRelayedCursor() {
        return new JournalCursor() {
            @Override
            public Pair<Long, JournalEntity> next() {
                if (buffer.isEmpty()) {
                    if (!loadNextSegmentSilent()) {
                        return null;
                    }
                }
                if (buffer.isEmpty()) {
                    return null;
                }
                DRJournalEntry entry = buffer.poll();
                nextJournalId = entry.journalId + 1;
                lastAppliedJournalId = entry.journalId;
                JournalEntity entity = deserializeEntity(entry);
                if (entity == null) {
                    return null;
                }
                LOG.debug("[DR:Consumer] applied journal_id={} opCode={}",
                        entry.journalId, entity.getOpCode());
                return Pair.of(entry.journalId, entity);
            }

            @Override
            public void close() {}
        };
    }

    // ── segment loading ───────────────────────────────────────────────────

    private void loadNextSegment() throws Exception {
        List<String> segments = storage.list(config.groupId + "/fe-editlog/segment_");
        for (String segKey : segments) {
            if (segKey.equals(lastLoadedSegment)) {
                continue;
            }
            long firstId = parseFirstJournalId(segKey);
            if (firstId < 0) {
                continue;
            }
            // allow fresh-start jump: if we have no prior state, start from first segment
            if (firstId > nextJournalId) {
                if (nextJournalId <= 1 && lastLoadedSegment == null) {
                    LOG.info("[DR:Consumer] fresh start, jumping to first segment at journal_id={}",
                            firstId);
                    nextJournalId = firstId;
                } else {
                    break; // gap — wait for missing segment
                }
            }
            byte[] bytes = storage.get(segKey);
            if (bytes == null) {
                continue;
            }
            List<DRJournalEntry> entries = DRExporter.deserializeSegment(bytes);
            // deduplicate: skip entries already applied
            for (DRJournalEntry e : entries) {
                if (e.journalId >= nextJournalId) {
                    buffer.add(e);
                }
            }
            lastLoadedSegment = segKey;

            // update lag metrics from checkpoint
            updateLagMetrics();

            if (!buffer.isEmpty()) {
                return;
            }
        }
    }

    /** Silent version for use inside the JournalCursor implementation. */
    private boolean loadNextSegmentSilent() {
        try {
            loadNextSegment();
            return !buffer.isEmpty();
        } catch (Exception e) {
            LOG.warn("[DR:Consumer] segment load failed: {}", e.getMessage());
            return false;
        }
    }

    // ── lag metrics ───────────────────────────────────────────────────────

    private void updateLagMetrics() {
        try {
            DRCheckpoint cp = DRCheckpoint.readLatest(storage, config.groupId);
            if (cp != null) {
                lagEntries = cp.feJournalId - lastAppliedJournalId;
                lagMs = System.currentTimeMillis() - cp.sampledAtMs;
                if (lagMs > config.lagAlertMs) {
                    LOG.warn("[DR:Consumer] BDBJE lag is high: {}ms ({} entries)",
                            lagMs, lagEntries);
                }
            }
        } catch (Exception e) {
            LOG.debug("[DR:Consumer] could not read checkpoint for lag update: {}",
                    e.getMessage());
        }
    }

    // ── cursor recovery ───────────────────────────────────────────────────

    /** On startup, restore nextJournalId from the local BDBJE journal position. */
    private void recoverCursor() {
        try {
            long localMax = env.getEditLog().getMaxJournalId();
            if (localMax > 0) {
                nextJournalId = localMax + 1;
                lastAppliedJournalId = localMax;
                LOG.info("[DR:Consumer] cursor recovered at journal_id={}", lastAppliedJournalId);
            }
        } catch (Exception e) {
            LOG.warn("[DR:Consumer] could not recover cursor, starting from 1: {}",
                    e.getMessage());
        }
    }

    // ── deserialisation ───────────────────────────────────────────────────

    private JournalEntity deserializeEntity(DRJournalEntry entry) {
        try {
            JournalEntity entity = new JournalEntity();
            entity.readFields(new DataInputStream(
                    new ByteArrayInputStream(entry.entityBytes)));
            return entity;
        } catch (Exception e) {
            LOG.error("[DR:Consumer] failed to deserialise journal_id={}: {}",
                    entry.journalId, e.getMessage(), e);
            return null;
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────

    /**
     * Extracts the first journal_id from a segment key.
     * Format: groupId/fe-editlog/segment_0000010500.log → 10500
     */
    private static long parseFirstJournalId(String segKey) {
        try {
            int slash = segKey.lastIndexOf('/');
            String filename = segKey.substring(slash + 1);
            int underscore = filename.lastIndexOf('_');
            int dot = filename.lastIndexOf('.');
            return Long.parseLong(filename.substring(underscore + 1, dot));
        } catch (Exception e) {
            return -1;
        }
    }

    private void sleepSafely(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
