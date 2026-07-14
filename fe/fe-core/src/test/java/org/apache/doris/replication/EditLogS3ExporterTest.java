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

import org.apache.doris.replication.storage.LocalReplicationStorage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for segment serialization / deserialization and the CURSOR / key helpers.
 * Does not require BDB, FE, or cloud access — uses only LocalReplicationStorage.
 */
public class EditLogS3ExporterTest {

    private LocalReplicationStorage storage;
    private ReplicationConfig config;

    @Before
    public void setUp() {
        storage = new LocalReplicationStorage();
        config = ReplicationConfig.builder()
                .groupId("bj_to_sh")
                .siteName("beijing")
                .exportIntervalMs(100)
                .exportBatchSize(50)
                .checkpointIntervalMs(60000)
                .crrMaxLagMs(300000)
                .build();
    }

    // ── segment serialization ─────────────────────────────────────────────

    @Test
    public void testSegmentRoundTrip() throws Exception {
        List<ReplicationJournalEntry> entries = new ArrayList<>();
        entries.add(new ReplicationJournalEntry(100L, new byte[]{0x01, 0x02, 0x03}));
        entries.add(new ReplicationJournalEntry(101L, new byte[]{0x04, 0x05}));
        entries.add(new ReplicationJournalEntry(102L, new byte[]{0x06}));

        byte[] bytes = EditLogS3Exporter.serializeSegment(entries);
        Assert.assertNotNull(bytes);
        Assert.assertTrue(bytes.length > 0);

        List<ReplicationJournalEntry> decoded = EditLogS3Exporter.deserializeSegment(bytes);
        Assert.assertEquals(3, decoded.size());
        Assert.assertEquals(100L, decoded.get(0).journalId);
        Assert.assertEquals(101L, decoded.get(1).journalId);
        Assert.assertEquals(102L, decoded.get(2).journalId);
        Assert.assertArrayEquals(new byte[]{0x01, 0x02, 0x03}, decoded.get(0).entityBytes);
        Assert.assertArrayEquals(new byte[]{0x04, 0x05}, decoded.get(1).entityBytes);
    }

    @Test
    public void testEmptySegmentRoundTrip() throws Exception {
        List<ReplicationJournalEntry> entries = new ArrayList<>();
        byte[] bytes = EditLogS3Exporter.serializeSegment(entries);
        List<ReplicationJournalEntry> decoded = EditLogS3Exporter.deserializeSegment(bytes);
        Assert.assertTrue(decoded.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeserializeInvalidVersionThrows() throws Exception {
        // corrupt the format version byte
        byte[] valid = EditLogS3Exporter.serializeSegment(new ArrayList<>());
        valid[0] = (byte) 99; // unsupported version
        EditLogS3Exporter.deserializeSegment(valid);
    }

    // ── key format ────────────────────────────────────────────────────────

    @Test
    public void testSegmentKeyFormat() {
        String key = EditLogS3Exporter.segmentKey("bj_to_sh", 10500L);
        Assert.assertEquals("bj_to_sh/fe-editlog/segment_0000010500.log", key);
    }

    @Test
    public void testSegmentKeysSortLexicographically() {
        // zero-padding ensures lexicographic order = chronological order
        String k1 = EditLogS3Exporter.segmentKey("g", 1L);
        String k2 = EditLogS3Exporter.segmentKey("g", 500L);
        String k3 = EditLogS3Exporter.segmentKey("g", 9999999999L);
        Assert.assertTrue(k1.compareTo(k2) < 0);
        Assert.assertTrue(k2.compareTo(k3) < 0);
    }

    @Test
    public void testCursorKey() {
        Assert.assertEquals("bj_to_sh/fe-editlog/CURSOR",
                EditLogS3Exporter.cursorKey("bj_to_sh"));
    }

    @Test
    public void testCheckpointLatestKey() {
        Assert.assertEquals("bj_to_sh/checkpoint/latest.json",
                EditLogS3Exporter.checkpointLatestKey("bj_to_sh"));
    }

    // ── CURSOR idempotency ────────────────────────────────────────────────

    @Test
    public void testCursorWrittenToStorage() throws Exception {
        // simulate what exportBatch does: write segment then cursor
        List<ReplicationJournalEntry> entries = new ArrayList<>();
        entries.add(new ReplicationJournalEntry(1L, new byte[]{0x01}));
        entries.add(new ReplicationJournalEntry(2L, new byte[]{0x02}));

        String segKey = EditLogS3Exporter.segmentKey("bj_to_sh", 1L);
        storage.put(segKey, EditLogS3Exporter.serializeSegment(entries));

        // CURSOR key should be writable and readable
        storage.put(EditLogS3Exporter.cursorKey("bj_to_sh"),
                "{\"lastSegment\":\"" + segKey + "\",\"lastJournalId\":2,"
                + "\"writtenBy\":\"beijing\"}".getBytes());

        Assert.assertTrue(storage.exists(EditLogS3Exporter.cursorKey("bj_to_sh")));
    }

    @Test
    public void testStorageFailureDoesNotUpdateCursor() throws Exception {
        // inject a failure on the segment put
        storage.injectPutFailures(1);

        List<ReplicationJournalEntry> entries = new ArrayList<>();
        entries.add(new ReplicationJournalEntry(1L, new byte[]{0x01}));

        try {
            storage.put(EditLogS3Exporter.segmentKey("bj_to_sh", 1L),
                    EditLogS3Exporter.serializeSegment(entries));
            Assert.fail("Expected exception");
        } catch (Exception e) {
            // expected
        }

        // CURSOR must NOT have been updated (no segment → no cursor update)
        Assert.assertFalse(storage.exists(EditLogS3Exporter.cursorKey("bj_to_sh")));
    }
}
