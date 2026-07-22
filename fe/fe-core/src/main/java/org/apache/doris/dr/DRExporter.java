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

import org.apache.doris.cloud.persist.CloudMetaSyncPoint;
import org.apache.doris.common.Pair;
import org.apache.doris.dr.storage.DRStorageBackend;
import org.apache.doris.journal.Journal;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.OperationType;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs on the ACTIVE (primary) FE. Reads BDBJE EditLog entries and uploads
 * them as gzip-compressed segment files to the relay OSS/S3 bucket.
 *
 * Segment file naming uses the first journal_id zero-padded to 10 digits
 * so that lexicographic sort equals chronological sort — the consumer can
 * simply list and process files in order.
 *
 * A checkpoint (DRCheckpoint.json) is written every checkpointIntervalMs
 * when a CloudMetaSyncPoint is observed. This pairs the BDBJE journal position
 * with the FDB versionstamp, giving the DR side a consistent restore point.
 *
 * A primary.lease file is renewed every 30 seconds. The DR side checks this
 * before promoting itself to prevent split-brain.
 */
public class DRExporter implements Runnable {

    private static final Logger LOG = LogManager.getLogger(DRExporter.class);
    private static final Gson GSON = new Gson();

    // binary segment format version — bump if format changes incompatibly
    static final byte FORMAT_VERSION = 1;

    // lease renewal interval — must be shorter than DRConfig.leaseTtlMs
    private static final long LEASE_RENEWAL_INTERVAL_MS = 30_000;

    private final Journal journal;
    private final DRStorageBackend storage;
    private final DRConfig config;

    private volatile long lastExportedJournalId = 0;
    private volatile boolean running = true;
    private volatile long lastCheckpointMs = 0;
    private volatile long lastLeaseMs = 0;
    private volatile long leaseFreshMs = 0; // ms since lease was last renewed

    // latest CloudMetaSyncPoint seen — links BDBJE position to FDB versionstamp
    private volatile CloudMetaSyncPoint latestSyncPoint = null;

    public DRExporter(Journal journal, DRStorageBackend storage, DRConfig config) {
        this.journal = journal;
        this.storage = storage;
        this.config = config;
    }

    @Override
    public void run() {
        LOG.info("[DR:Exporter] started group={} site={}", config.groupId, config.siteName);
        recoverCursor();

        while (running) {
            try {
                exportBatch();
                renewLeaseIfNeeded();
            } catch (Exception e) {
                LOG.warn("[DR:Exporter] export cycle failed, will retry: {}", e.getMessage(), e);
            }
            sleepSafely(config.exportIntervalMs);
        }
        LOG.info("[DR:Exporter] stopped group={}", config.groupId);
    }

    public void stop() {
        running = false;
    }

    public long getLastExportedJournalId() {
        return lastExportedJournalId;
    }

    /** Returns how many ms ago the lease was last renewed (for status reporting). */
    public long getLeaseFreshMs() {
        return leaseFreshMs;
    }

    // ── export ────────────────────────────────────────────────────────────

    private void exportBatch() throws Exception {
        List<DRJournalEntry> batch = readBatch();
        if (batch.isEmpty()) {
            return;
        }
        long firstId = batch.get(0).journalId;
        long lastId  = batch.get(batch.size() - 1).journalId;

        // 1. write segment — idempotent key, safe to retry on network error
        byte[] segmentBytes = serializeSegment(batch);
        String segmentKey = DRCheckpoint.segmentKey(config.groupId, firstId);
        storage.put(segmentKey, segmentBytes);

        // 2. advance cursor only AFTER segment is confirmed written
        storage.put(DRCheckpoint.cursorKey(config.groupId),
                buildCursorJson(segmentKey, lastId));

        lastExportedJournalId = lastId;
        LOG.debug("[DR:Exporter] exported journal_ids={}-{} bytes={}",
                firstId, lastId, segmentBytes.length);

        // 3. write checkpoint if enough time has passed and we have a sync point
        maybeWriteCheckpoint();
    }

    private List<DRJournalEntry> readBatch() throws Exception {
        List<DRJournalEntry> batch = new ArrayList<>();
        JournalCursor cursor = journal.read(lastExportedJournalId + 1, Long.MAX_VALUE);
        while (batch.size() < config.exportBatchSize) {
            Pair<Long, JournalEntity> entry = cursor.next();
            if (entry == null) {
                break;
            }
            long journalId = entry.first;
            JournalEntity entity = entry.second;

            // capture CloudMetaSyncPoint — links BDBJE position to FDB versionstamp
            if (entity.getOpCode() == OperationType.OP_META_SYNC_POINT) {
                latestSyncPoint = (CloudMetaSyncPoint) entity.getData();
            }

            batch.add(new DRJournalEntry(journalId, serializeEntity(entity)));
        }
        cursor.close();
        return batch;
    }

    // ── segment serialisation ─────────────────────────────────────────────

    /**
     * Binary segment format:
     *   [1 byte]  FORMAT_VERSION
     *   [4 bytes] entry count (big-endian int)
     *   per entry:
     *     [8 bytes] journalId (big-endian long)
     *     [4 bytes] entityBytes length (big-endian int)
     *     [N bytes] entityBytes (JournalEntity.write() output)
     */
    static byte[] serializeSegment(List<DRJournalEntry> entries) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeByte(FORMAT_VERSION);
        out.writeInt(entries.size());
        for (DRJournalEntry e : entries) {
            out.writeLong(e.journalId);
            out.writeInt(e.entityBytes.length);
            out.write(e.entityBytes);
        }
        out.flush();
        return baos.toByteArray();
    }

    /** Deserialises a segment back into entries — called by DRConsumer. */
    static List<DRJournalEntry> deserializeSegment(byte[] bytes) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        byte version = buf.get();
        if (version != FORMAT_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported segment format version: " + version
                    + ", expected: " + FORMAT_VERSION);
        }
        int count = buf.getInt();
        List<DRJournalEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long journalId = buf.getLong();
            int len = buf.getInt();
            byte[] entityBytes = new byte[len];
            buf.get(entityBytes);
            entries.add(new DRJournalEntry(journalId, entityBytes));
        }
        return entries;
    }

    private static byte[] serializeEntity(JournalEntity entity) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        entity.write(new DataOutputStream(baos));
        return baos.toByteArray();
    }

    // ── checkpoint ────────────────────────────────────────────────────────

    private void maybeWriteCheckpoint() throws Exception {
        long now = System.currentTimeMillis();
        if (now - lastCheckpointMs < config.checkpointIntervalMs) {
            return;
        }
        if (latestSyncPoint == null) {
            LOG.debug("[DR:Exporter] no CloudMetaSyncPoint yet, skipping checkpoint");
            return;
        }
        DRCheckpoint cp = new DRCheckpoint(
                config.groupId,
                lastExportedJournalId,
                latestSyncPoint.getVersionStamp(),
                now,
                now - config.crrMaxLagMs,
                config.siteName);
        cp.write(storage);
        lastCheckpointMs = now;
    }

    // ── primary.lease ─────────────────────────────────────────────────────

    private void renewLeaseIfNeeded() {
        long now = System.currentTimeMillis();
        if (now - lastLeaseMs < LEASE_RENEWAL_INTERVAL_MS) {
            return;
        }
        try {
            LeaseData lease = new LeaseData();
            lease.site = config.siteName;
            lease.timestamp = now;
            lease.expiresAt = now + config.leaseTtlMs;
            byte[] leaseBytes = GSON.toJson(lease).getBytes(StandardCharsets.UTF_8);
            storage.put(DRCheckpoint.leaseKey(config.groupId), leaseBytes);
            lastLeaseMs = now;
            leaseFreshMs = 0;
            LOG.debug("[DR:Exporter] primary.lease renewed site={}", config.siteName);
        } catch (Exception e) {
            leaseFreshMs = now - lastLeaseMs;
            LOG.warn("[DR:Exporter] failed to renew primary.lease: {}", e.getMessage());
        }
    }

    // ── cursor recovery ───────────────────────────────────────────────────

    /** H8 fix: throw on storage exception so cursor is not silently reset to 0. */
    private void recoverCursor() {
        try {
            byte[] cursorBytes = storage.get(DRCheckpoint.cursorKey(config.groupId));
            if (cursorBytes != null) {
                CursorData cursor = GSON.fromJson(
                        new String(cursorBytes, StandardCharsets.UTF_8), CursorData.class);
                lastExportedJournalId = cursor.lastJournalId;
                LOG.info("[DR:Exporter] cursor recovered at journal_id={}",
                        lastExportedJournalId);
            }
        } catch (Exception e) {
            // Storage error at startup is fatal — do not silently re-export from 0.
            throw new RuntimeException(
                    "[DR:Exporter] failed to recover cursor from relay storage. "
                    + "Cannot start exporter safely: " + e.getMessage(), e);
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private byte[] buildCursorJson(String segmentKey, long lastJournalId) {
        CursorData cursor = new CursorData();
        cursor.lastSegment = segmentKey;
        cursor.lastJournalId = lastJournalId;
        cursor.writtenBy = config.siteName;
        return GSON.toJson(cursor).getBytes(StandardCharsets.UTF_8);
    }

    private void sleepSafely(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** JSON model for the CURSOR file. */
    static class CursorData {
        public String lastSegment;
        public long lastJournalId;
        public String writtenBy;
    }

    /** JSON model for the primary.lease file. */
    static class LeaseData {
        public String site;
        public long timestamp;
        public long expiresAt;
    }
}
