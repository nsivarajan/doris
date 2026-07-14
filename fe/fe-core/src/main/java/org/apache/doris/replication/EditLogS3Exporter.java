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

package org.apache.doris.replication;

import org.apache.doris.cloud.persist.CloudMetaSyncPoint;
import org.apache.doris.common.Pair;
import org.apache.doris.journal.Journal;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.OperationType;
import org.apache.doris.replication.storage.ReplicationStorageBackend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Background thread that runs inside the FE Master process.
 * Reads new EditLog entries from BDB and writes them as segment files
 * to the replication bucket every exportIntervalMs milliseconds.
 * Also writes a checkpoint pairing fe_journal_id with fdb_versionstamp.
 *
 * Only started when Config.enable_replication_group = true and this FE is master.
 * Stopped when this FE resigns from master.
 */
public class EditLogS3Exporter implements Runnable {

    private static final Logger LOG = LogManager.getLogger(EditLogS3Exporter.class);
    private static final DateTimeFormatter ISO_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);
    private static final Gson GSON = new Gson();

    // segment file binary format version tag — checked on read to detect incompatible changes
    private static final byte FORMAT_VERSION = 1;

    private final Journal journal;
    private final ReplicationStorageBackend storage;
    private final ReplicationConfig config;

    private volatile long lastExportedJournalId = 0;
    private volatile boolean running = true;
    // latest CloudMetaSyncPoint seen in the journal — used for checkpointing
    private volatile CloudMetaSyncPoint latestSyncPoint = null;

    public EditLogS3Exporter(Journal journal, ReplicationStorageBackend storage,
            ReplicationConfig config) {
        this.journal = journal;
        this.storage = storage;
        this.config = config;
    }

    @Override
    public void run() {
        LOG.info("[Replication] EditLogS3Exporter started group={} site={}",
                config.groupId, config.siteName);
        recoverCursor();

        while (running) {
            try {
                exportBatch();
            } catch (Exception e) {
                LOG.warn("[Replication] EditLogS3Exporter export failed, will retry: {}",
                        e.getMessage(), e);
            }
            sleepSafely(config.exportIntervalMs);
        }
        LOG.info("[Replication] EditLogS3Exporter stopped group={}", config.groupId);
    }

    public void stop() {
        running = false;
    }

    /** Returns the last successfully exported journal ID — used for monitoring. */
    public long getLastExportedJournalId() {
        return lastExportedJournalId;
    }

    // ── export ────────────────────────────────────────────────────────────

    private void exportBatch() throws Exception {
        List<ReplicationJournalEntry> batch = readBatch();
        if (batch.isEmpty()) {
            return;
        }
        long firstId = batch.get(0).journalId;
        long lastId  = batch.get(batch.size() - 1).journalId;

        // 1. write segment file — idempotent key, safe to retry
        byte[] segmentBytes = serializeSegment(batch);
        String segmentKey = segmentKey(config.groupId, firstId);
        storage.put(segmentKey, segmentBytes);

        // 2. update CURSOR only after segment is confirmed written
        storage.put(cursorKey(config.groupId), buildCursorJson(segmentKey, lastId));

        lastExportedJournalId = lastId;
        LOG.debug("[Replication] exported journal_ids={}-{} segment={} bytes={}",
                firstId, lastId, segmentKey, segmentBytes.length);

        // 3. write checkpoint periodically (every checkpointIntervalMs)
        writeCheckpoint();
    }

    private List<ReplicationJournalEntry> readBatch() throws Exception {
        List<ReplicationJournalEntry> batch = new ArrayList<>();
        JournalCursor cursor = journal.read(lastExportedJournalId + 1);
        while (batch.size() < config.exportBatchSize) {
            Pair<Long, JournalEntity> entry = cursor.next();
            if (entry == null) {
                break;
            }
            long journalId = entry.first;
            JournalEntity entity = entry.second;

            // track the latest CloudMetaSyncPoint for checkpoint writing
            if (entity.getOpCode() == OperationType.OP_META_SYNC_POINT) {
                latestSyncPoint = (CloudMetaSyncPoint) entity.getData();
            }

            batch.add(new ReplicationJournalEntry(journalId, serializeEntity(entity)));
        }
        cursor.close();
        return batch;
    }

    // ── segment serialisation ─────────────────────────────────────────────

    /**
     * Segment binary format:
     *   [1 byte]  FORMAT_VERSION
     *   [4 bytes] entry count (big-endian int)
     *   per entry:
     *     [8 bytes] journalId (big-endian long)
     *     [4 bytes] entityBytes length (big-endian int)
     *     [N bytes] entityBytes (opCode + data from JournalEntity.write())
     */
    static byte[] serializeSegment(List<ReplicationJournalEntry> entries) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeByte(FORMAT_VERSION);
        out.writeInt(entries.size());
        for (ReplicationJournalEntry e : entries) {
            out.writeLong(e.journalId);
            out.writeInt(e.entityBytes.length);
            out.write(e.entityBytes);
        }
        out.flush();
        return baos.toByteArray();
    }

    /** Deserialise a segment back into entries — used by S3JournalCursor. */
    static List<ReplicationJournalEntry> deserializeSegment(byte[] bytes) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        byte version = buf.get();
        if (version != FORMAT_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported segment format version: " + version);
        }
        int count = buf.getInt();
        List<ReplicationJournalEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long journalId = buf.getLong();
            int len = buf.getInt();
            byte[] entityBytes = new byte[len];
            buf.get(entityBytes);
            entries.add(new ReplicationJournalEntry(journalId, entityBytes));
        }
        return entries;
    }

    /** Serialise a JournalEntity to raw bytes using its own write() method. */
    private static byte[] serializeEntity(JournalEntity entity) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        entity.write(new DataOutputStream(baos));
        return baos.toByteArray();
    }

    // ── checkpoint ────────────────────────────────────────────────────────

    private long lastCheckpointMs = 0;

    private void writeCheckpoint() throws Exception {
        long now = System.currentTimeMillis();
        if (now - lastCheckpointMs < config.checkpointIntervalMs) {
            return;
        }
        if (latestSyncPoint == null) {
            LOG.debug("[Replication] no CloudMetaSyncPoint yet, skipping checkpoint");
            return;
        }
        ReplicationCheckpoint cp = new ReplicationCheckpoint(
                config.groupId,
                latestSyncPoint.getCommittedVersion(),
                latestSyncPoint.getVersionStamp(),
                now,
                now - config.crrMaxLagMs,
                config.siteName,
                ISO_FMT.format(Instant.ofEpochMilli(now)));

        byte[] cpBytes = GSON.toJson(cp).getBytes(StandardCharsets.UTF_8);
        storage.put(checkpointLatestKey(config.groupId), cpBytes);

        // keep a timestamped copy for audit
        String histKey = checkpointHistoryKey(config.groupId,
                ISO_FMT.format(Instant.ofEpochMilli(now)).replace(":", "").replace("-", ""));
        storage.put(histKey, cpBytes);

        lastCheckpointMs = now;
        LOG.debug("[Replication] checkpoint written fe_journal_id={} fdb_vs={}",
                cp.feJournalId, cp.fdbVersionstamp);
    }

    // ── cursor recovery ───────────────────────────────────────────────────

    /** On startup, read the CURSOR file to resume from the last confirmed journal_id. */
    private void recoverCursor() {
        try {
            byte[] cursorBytes = storage.get(cursorKey(config.groupId));
            if (cursorBytes != null) {
                String json = new String(cursorBytes, StandardCharsets.UTF_8);
                CursorData cursor = GSON.fromJson(json, CursorData.class);
                lastExportedJournalId = cursor.lastJournalId;
                LOG.info("[Replication] cursor recovered at journal_id={}", lastExportedJournalId);
            }
        } catch (Exception e) {
            LOG.warn("[Replication] could not recover cursor, starting from 0: {}", e.getMessage());
        }
    }

    // ── key helpers ───────────────────────────────────────────────────────

    /** Segment key: zero-padded first journal_id ensures lexicographic = chronological sort. */
    public static String segmentKey(String groupId, long firstJournalId) {
        return groupId + "/fe-editlog/segment_" + String.format("%010d", firstJournalId) + ".log";
    }

    public static String cursorKey(String groupId) {
        return groupId + "/fe-editlog/CURSOR";
    }

    public static String checkpointLatestKey(String groupId) {
        return groupId + "/checkpoint/latest.json";
    }

    public static String checkpointHistoryKey(String groupId, String timestamp) {
        return groupId + "/checkpoint/history/checkpoint_" + timestamp + ".json";
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
}
