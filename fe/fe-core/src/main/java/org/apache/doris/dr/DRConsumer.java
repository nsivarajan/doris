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
import org.apache.doris.journal.EditLog;
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
 * and applies them to the local Env via EditLog.loadJournal(), keeping the
 * DR FE's catalog in sync with the primary's metadata.
 *
 * H4 fix: use EditLog.loadJournal(env, logId, entity) — the actual Doris API
 * for applying journal entries. The previous playbackJournalFromCursor() did
 * not exist. DRConsumer now iterates entries manually and calls loadJournal
 * for each one, matching exactly how Env.replayJournal() works.
 *
 * M7 fix: deserialisation failure throws rather than silently advancing cursor.
 */
public class DRConsumer implements Runnable {

    private static final Logger LOG = LogManager.getLogger(DRConsumer.class);

    private static final long POLL_INTERVAL_MS = 1000;

    private final Env env;
    private final DRStorageBackend storage;
    private final DRConfig config;

    private volatile boolean running = true;

    // metrics
    private volatile long lastAppliedJournalId = 0;
    private volatile long lagMs = 0;
    private volatile long lagEntries = 0;

    // cursor state
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
     * Blocks until all segments up to targetJournalId are applied.
     * Called during planned switchover before promotion.
     */
    public void applyUntil(long targetJournalId) throws Exception {
        LOG.info("[DR:Consumer] applying until journal_id={}", targetJournalId);
        while (lastAppliedJournalId < targetJournalId && running) {
            applyNextBatch();
            if (lastAppliedJournalId < targetJournalId) {
                sleepSafely(POLL_INTERVAL_MS);
            }
        }
        LOG.info("[DR:Consumer] reached journal_id={}", targetJournalId);
    }

    // ── apply ─────────────────────────────────────────────────────────────

    private void applyNextBatch() throws Exception {
        if (buffer.isEmpty()) {
            loadNextSegment();
        }
        if (buffer.isEmpty()) {
            return;
        }

        // Apply buffered entries via the same API Doris uses for follower replay:
        // EditLog.loadJournal(env, logId, entity)  — from Env.replayJournal()
        while (!buffer.isEmpty()) {
            DRJournalEntry entry = buffer.peek();
            JournalEntity entity = deserializeEntity(entry); // throws on corrupt data (M7)
            EditLog.loadJournal(env, entry.journalId, entity);
            buffer.poll();
            lastAppliedJournalId = entry.journalId;
            nextJournalId = entry.journalId + 1;
            LOG.debug("[DR:Consumer] applied journal_id={} opCode={}",
                    entry.journalId, entity.getOpCode());
        }
        updateLagMetrics();
    }

    // ── segment loading (H6/pagination fix via prefix sort) ───────────────

    private void loadNextSegment() throws Exception {
        // H6 fix: list returns up to 1000 keys per call (OSS limit).
        // Segment keys are zero-padded so lexicographic = chronological order.
        // We process the first unread key and return; next call picks the next.
        List<String> segments = storage.list(config.groupId + "/fe-editlog/segment_");
        for (String segKey : segments) {
            if (segKey.equals(lastLoadedSegment)) {
                continue;
            }
            long firstId = parseFirstJournalId(segKey);
            if (firstId < 0) {
                continue;
            }
            // fresh-start: jump to first available segment
            if (firstId > nextJournalId) {
                if (nextJournalId <= 1 && lastLoadedSegment == null) {
                    LOG.info("[DR:Consumer] fresh start, jumping to journal_id={}", firstId);
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
            for (DRJournalEntry e : entries) {
                if (e.journalId >= nextJournalId) {
                    buffer.add(e);
                }
            }
            lastLoadedSegment = segKey;
            if (!buffer.isEmpty()) {
                return;
            }
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
                    LOG.warn("[DR:Consumer] BDBJE lag high: {}ms ({} entries)", lagMs, lagEntries);
                }
            }
        } catch (Exception e) {
            LOG.debug("[DR:Consumer] lag update skipped: {}", e.getMessage());
        }
    }

    // ── cursor recovery ───────────────────────────────────────────────────

    private void recoverCursor() {
        try {
            long localMax = env.getMaxJournalId();
            if (localMax > 0) {
                nextJournalId = localMax + 1;
                lastAppliedJournalId = localMax;
                LOG.info("[DR:Consumer] cursor recovered at journal_id={}", lastAppliedJournalId);
            }
        } catch (Exception e) {
            LOG.warn("[DR:Consumer] could not recover cursor, starting from 1: {}", e.getMessage());
        }
    }

    // ── deserialisation (M7: throws on corrupt entry) ─────────────────────

    private JournalEntity deserializeEntity(DRJournalEntry entry) throws Exception {
        JournalEntity entity = new JournalEntity();
        entity.readFields(new DataInputStream(
                new ByteArrayInputStream(entry.entityBytes)));
        return entity;
    }

    // ── helpers ───────────────────────────────────────────────────────────

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
