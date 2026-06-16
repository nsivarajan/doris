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

package org.apache.doris.cloud.snapshot;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Unit tests for CloudSnapshotHandler.verifySnapshot() — Level 1 structural
 * verification that runs automatically after every backup.
 *
 * verifySnapshot() is package-private so it can be called directly here.
 * It is non-fatal (never throws) — all failure paths emit LOG.warn only.
 */
public class CloudSnapshotHandlerVerifyTest {

    private MockedStatic<MetaServiceProxy> mockedProxy;
    private MetaServiceProxy mockProxy;
    private CloudSnapshotHandler handler;
    private String originalCloudUniqueId;

    @BeforeEach
    public void setUp() {
        originalCloudUniqueId = Config.cloud_unique_id;
        mockProxy = Mockito.mock(MetaServiceProxy.class);
        mockedProxy = Mockito.mockStatic(MetaServiceProxy.class);
        mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
        handler = new CloudSnapshotHandler();
        Config.cloud_unique_id = "test_cloud_unique_id";
    }

    @AfterEach
    public void tearDown() {
        Config.cloud_unique_id = originalCloudUniqueId;
        if (mockedProxy != null) {
            mockedProxy.close();
        }
    }

    // Build a ListSnapshotResponse with a single NORMAL snapshot
    private Cloud.ListSnapshotResponse normalResponse(String snapshotId, String imageUrl,
                                                        long journalId) {
        Cloud.SnapshotInfoPB snap = Cloud.SnapshotInfoPB.newBuilder()
                .setSnapshotId(snapshotId)
                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_NORMAL)
                .setImageUrl(imageUrl)
                .setJournalId(journalId)
                .build();
        return Cloud.ListSnapshotResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                .addSnapshots(snap)
                .build();
    }

    // Build an OK response with no snapshots
    private Cloud.ListSnapshotResponse emptyResponse() {
        return Cloud.ListSnapshotResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                .build();
    }

    // Build an error response
    private Cloud.ListSnapshotResponse errorResponse() {
        return Cloud.ListSnapshotResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.UNDEFINED_ERR).setMsg("internal error"))
                .build();
    }

    /**
     * Happy path: NORMAL snapshot with valid imageUrl and journalId, no objInfo (HDFS).
     * Check 3 (S3 accessibility) is skipped — PASSED logged.
     */
    @Test
    public void testVerifyPassedNoObjInfo() throws Exception {
        String snapshotId = "abc123def456789012";
        Mockito.when(mockProxy.listSnapshot(Mockito.any()))
                .thenReturn(normalResponse(snapshotId, "prefix/snapshot/" + snapshotId, 100));

        // Should not throw — verifySnapshot is non-fatal
        handler.verifySnapshot(snapshotId, null);
    }

    /**
     * Snapshot not found in FDB (empty response) — FAILED logged, no exception.
     */
    @Test
    public void testVerifyFailedSnapshotNotFound() throws Exception {
        Mockito.when(mockProxy.listSnapshot(Mockito.any()))
                .thenReturn(emptyResponse());

        // Non-fatal: no exception, WARN logged internally
        handler.verifySnapshot("missing_snapshot_id", null);
    }

    /**
     * list_snapshot RPC returns an error — caught by outer catch, WARN logged.
     */
    @Test
    public void testVerifyFailedRpcError() throws Exception {
        Mockito.when(mockProxy.listSnapshot(Mockito.any()))
                .thenReturn(errorResponse());

        handler.verifySnapshot("snap_id", null);
    }

    /**
     * Snapshot status is ABORTED (not NORMAL) — FAILED logged.
     */
    @Test
    public void testVerifyFailedWrongStatus() throws Exception {
        String snapshotId = "aborted_snap";
        Cloud.SnapshotInfoPB snap = Cloud.SnapshotInfoPB.newBuilder()
                .setSnapshotId(snapshotId)
                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_ABORTED)
                .setImageUrl("prefix/snapshot/" + snapshotId)
                .setJournalId(100)
                .build();
        Cloud.ListSnapshotResponse resp = Cloud.ListSnapshotResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                .addSnapshots(snap)
                .build();
        Mockito.when(mockProxy.listSnapshot(Mockito.any())).thenReturn(resp);

        handler.verifySnapshot(snapshotId, null);
    }

    /**
     * image_url is empty — FAILED logged.
     */
    @Test
    public void testVerifyFailedEmptyImageUrl() throws Exception {
        String snapshotId = "snap_no_url";
        Mockito.when(mockProxy.listSnapshot(Mockito.any()))
                .thenReturn(normalResponse(snapshotId, "", 100));

        handler.verifySnapshot(snapshotId, null);
    }

    /**
     * journal_id is 0 — FAILED logged.
     */
    @Test
    public void testVerifyFailedZeroJournalId() throws Exception {
        String snapshotId = "snap_no_journal";
        Mockito.when(mockProxy.listSnapshot(Mockito.any()))
                .thenReturn(normalResponse(snapshotId, "prefix/snap", 0));

        handler.verifySnapshot(snapshotId, null);
    }

    /**
     * listSnapshot throws RpcException — caught by outer catch, WARN logged, no exception.
     */
    @Test
    public void testVerifyRpcException() throws Exception {
        Mockito.when(mockProxy.listSnapshot(Mockito.any()))
                .thenThrow(new org.apache.doris.rpc.RpcException("test-host", "simulated RPC failure"));

        // Must not throw — outer catch handles it
        handler.verifySnapshot("snap_id", null);
    }
}
