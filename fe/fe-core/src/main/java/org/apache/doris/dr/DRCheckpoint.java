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

import org.apache.doris.dr.storage.DRStorageBackend;
import org.apache.doris.dr.storage.DRStorageException;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Consistency anchor that links BDBJE journal position to FDB versionstamp.
 *
 * Written every checkpointIntervalMs by DRExporter when a CloudMetaSyncPoint
 * is observed in the BDBJE stream. At restore time, DRConsumer applies BDBJE
 * up to feJournalId, and fdbbackup restores FDB to fdbVersionstamp.
 * ossSafeBeforeMs is the OSS CRR safety boundary: all OSS data written before
 * this timestamp is guaranteed to be present in the DR bucket.
 */
public class DRCheckpoint {

    private static final Logger LOG = LogManager.getLogger(DRCheckpoint.class);
    private static final Gson GSON = new Gson();
    private static final DateTimeFormatter ISO_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    public String groupId;
    public long feJournalId;        // BDBJE journal position
    public String fdbVersionstamp;  // FDB versionstamp hex from CloudMetaSyncPoint
    public long sampledAtMs;        // wall clock when checkpoint was written
    public long ossSafeBeforeMs;    // = sampledAtMs - crrMaxLagMs
    public String primarySite;      // which site was primary when written
    public String createdAt;        // ISO-8601 for human readability

    // default constructor for JSON deserialisation
    public DRCheckpoint() {}

    public DRCheckpoint(String groupId, long feJournalId, String fdbVersionstamp,
            long sampledAtMs, long ossSafeBeforeMs, String primarySite) {
        this.groupId = groupId;
        this.feJournalId = feJournalId;
        this.fdbVersionstamp = fdbVersionstamp;
        this.sampledAtMs = sampledAtMs;
        this.ossSafeBeforeMs = ossSafeBeforeMs;
        this.primarySite = primarySite;
        this.createdAt = ISO_FMT.format(Instant.ofEpochMilli(sampledAtMs));
    }

    // ── storage keys ──────────────────────────────────────────────────────

    /** Key of the latest checkpoint — overwritten on every write. */
    public static String latestKey(String groupId) {
        return groupId + "/checkpoint/latest.json";
    }

    /** Key for a timestamped history copy — never overwritten. */
    public static String historyKey(String groupId, long sampledAtMs) {
        String ts = ISO_FMT.format(Instant.ofEpochMilli(sampledAtMs))
                .replace(":", "").replace("-", "");
        return groupId + "/checkpoint/history/chk-" + ts + ".json";
    }

    /** Key of the segment CURSOR file (last successfully exported journal_id). */
    public static String cursorKey(String groupId) {
        return groupId + "/fe-editlog/CURSOR";
    }

    /** Key of the primary.lease file — renewed every 30s by DRExporter. */
    public static String leaseKey(String groupId) {
        return groupId + "/primary.lease";
    }

    /** Segment key — zero-padded so lexicographic order = chronological order. */
    public static String segmentKey(String groupId, long firstJournalId) {
        return groupId + "/fe-editlog/segment_"
                + String.format("%010d", firstJournalId) + ".log";
    }

    // ── persistence ───────────────────────────────────────────────────────

    /** Write this checkpoint to relay storage (latest + timestamped history copy). */
    public void write(DRStorageBackend storage) throws DRStorageException {
        byte[] bytes = GSON.toJson(this).getBytes(StandardCharsets.UTF_8);
        storage.put(latestKey(groupId), bytes);
        storage.put(historyKey(groupId, sampledAtMs), bytes);
        LOG.debug("[DR] checkpoint written fe_journal_id={} fdb_vs={}",
                feJournalId, fdbVersionstamp);
    }

    /** Read the latest checkpoint from relay storage. Returns null if none exists yet. */
    public static DRCheckpoint readLatest(DRStorageBackend storage, String groupId)
            throws DRStorageException {
        byte[] bytes = storage.get(latestKey(groupId));
        if (bytes == null) {
            return null;
        }
        return GSON.fromJson(new String(bytes, StandardCharsets.UTF_8), DRCheckpoint.class);
    }

    /** List all available checkpoint keys in history (sorted lexicographically = chronologically). */
    public static List<String> listHistory(DRStorageBackend storage, String groupId)
            throws DRStorageException {
        return storage.list(groupId + "/checkpoint/history/");
    }
}
