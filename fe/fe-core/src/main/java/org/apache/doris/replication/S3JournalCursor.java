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

import org.apache.doris.common.Pair;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.replication.storage.ReplicationStorageBackend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/**
 * JournalCursor implementation that reads from the replication bucket instead of BDB.
 * Used by the DR FE in --dr-reader-mode to replay the primary's EditLog.
 *
 * Reads segment files sequentially. Buffers one segment at a time to bound memory.
 * Direction-agnostic: handles segments written by either Beijing or Shanghai
 * (the stream is continuous regardless of which site is currently primary).
 * Deduplicates entries with journal_id < nextJournalId to handle idempotent re-sends.
 */
public class S3JournalCursor implements JournalCursor {

    private static final Logger LOG = LogManager.getLogger(S3JournalCursor.class);

    // poll interval when no new segments are available — avoids tight loop
    private static final long POLL_INTERVAL_MS = 5000;

    private final ReplicationStorageBackend storage;
    private final String groupId;

    // in-memory buffer of entries from the currently loaded segment
    private final Queue<ReplicationJournalEntry> buffer = new ArrayDeque<>();
    // journal_id of the next entry we expect to return
    private long nextJournalId;
    // last segment key we successfully loaded — avoids re-loading
    private String lastLoadedSegment = null;

    public S3JournalCursor(ReplicationStorageBackend storage, String groupId,
            long startJournalId) {
        this.storage = storage;
        this.groupId = groupId;
        this.nextJournalId = startJournalId;
        LOG.info("[Replication:S3Cursor] created group={} startJournalId={}",
                groupId, startJournalId);
    }

    @Override
    public Pair<Long, JournalEntity> next() {
        // drain in-memory buffer before fetching the next segment
        while (buffer.isEmpty()) {
            if (!loadNextSegment()) {
                // no new segment available yet — sleep and try again
                sleepSafely(POLL_INTERVAL_MS);
            }
        }
        ReplicationJournalEntry entry = buffer.poll();
        nextJournalId = entry.journalId + 1;
        JournalEntity entity = deserializeEntity(entry);
        if (entity == null) {
            return null;
        }
        LOG.debug("[Replication:S3Cursor] next journal_id={} opCode={}",
                entry.journalId, entity.getOpCode());
        return Pair.of(entry.journalId, entity);
    }

    @Override
    public void close() {
        buffer.clear();
        LOG.info("[Replication:S3Cursor] closed group={} lastJournalId={}",
                groupId, nextJournalId - 1);
    }

    // ── segment loading ───────────────────────────────────────────────────

    /**
     * Finds and loads the next unread segment into the buffer.
     * Returns true if new entries were loaded, false if no new segment exists yet.
     */
    private boolean loadNextSegment() {
        try {
            // list all segment files, sorted lexicographically = chronologically
            List<String> segments = storage.list(
                    groupId + "/fe-editlog/segment_");

            for (String segKey : segments) {
                // skip segments we've already loaded
                if (segKey.equals(lastLoadedSegment)) {
                    continue;
                }
                // skip segments whose first journal_id is entirely before our cursor
                long firstId = parseFirstJournalId(segKey);
                if (firstId < 0) {
                    continue;
                }
                // if this segment starts after our next expected id there are two cases:
                // 1. genuine gap mid-stream (should not happen) — stop and wait
                // 2. fresh start: nextJournalId=1 but exporter started at a higher id
                //    in this case skip ahead to the first available segment
                if (firstId > nextJournalId) {
                    if (nextJournalId <= 1 && lastLoadedSegment == null) {
                        // fresh start — jump to first available segment
                        LOG.info("[Replication:S3Cursor] fresh start: jumping to first "
                                + "available segment at journal_id={}", firstId);
                        nextJournalId = firstId;
                    } else {
                        break; // genuine gap — wait for missing segments
                    }
                }

                byte[] bytes = storage.get(segKey);
                if (bytes == null) {
                    continue;
                }

                List<ReplicationJournalEntry> entries =
                        EditLogS3Exporter.deserializeSegment(bytes);

                // filter: skip entries already applied (deduplication)
                int added = 0;
                for (ReplicationJournalEntry e : entries) {
                    if (e.journalId >= nextJournalId) {
                        buffer.add(e);
                        added++;
                    }
                }

                lastLoadedSegment = segKey;
                LOG.debug("[Replication:S3Cursor] loaded segment={} entries={} added={}",
                        segKey, entries.size(), added);

                if (!buffer.isEmpty()) {
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.warn("[Replication:S3Cursor] failed to load segment: {}", e.getMessage(), e);
        }
        return false;
    }

    // ── deserialisation ───────────────────────────────────────────────────

    /** Reconstruct a JournalEntity from the raw bytes stored in the segment. */
    private JournalEntity deserializeEntity(ReplicationJournalEntry entry) {
        try {
            JournalEntity entity = new JournalEntity();
            entity.readFields(new DataInputStream(
                    new ByteArrayInputStream(entry.entityBytes)));
            return entity;
        } catch (Exception e) {
            LOG.error("[Replication:S3Cursor] failed to deserialise journal_id={}: {}",
                    entry.journalId, e.getMessage(), e);
            return null;
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────

    /**
     * Extracts the first journal_id from a segment key.
     * Key format: groupId/fe-editlog/segment_0000010500.log → 10500
     */
    private static long parseFirstJournalId(String segKey) {
        try {
            // find the last '/' then parse digits before '.log'
            int slash = segKey.lastIndexOf('/');
            String filename = segKey.substring(slash + 1); // segment_0000010500.log
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
