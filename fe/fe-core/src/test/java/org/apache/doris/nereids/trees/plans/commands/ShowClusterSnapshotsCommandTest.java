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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.snapshot.CloudSnapshotHandler;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class ShowClusterSnapshotsCommandTest {

    private CloudEnv cloudEnv;
    private CloudSnapshotHandler snapshotHandler;
    private ConnectContext connectContext;
    private MockedStatic<Env> envMockedStatic;
    private String originalDeployMode;

    @BeforeEach
    public void setUp() throws Exception {
        originalDeployMode = Config.deploy_mode;
        Config.deploy_mode = "cloud";

        snapshotHandler = Mockito.mock(CloudSnapshotHandler.class);
        cloudEnv = Mockito.mock(CloudEnv.class);
        connectContext = Mockito.mock(ConnectContext.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);

        Mockito.when(cloudEnv.getCloudSnapshotHandler()).thenReturn(snapshotHandler);
        Mockito.when(internalCatalog.getDbs()).thenReturn(Collections.emptyList());
        Mockito.when(internalCatalog.getDbNullable(Mockito.anyLong())).thenReturn(null);

        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(cloudEnv);
        envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(internalCatalog);
    }

    @AfterEach
    public void tearDown() {
        Config.deploy_mode = originalDeployMode;
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
    }

    // Builds a minimal SNAPSHOT_NORMAL snap with the given flags.
    private Cloud.SnapshotInfoPB normalSnap(boolean seeded, boolean exported, boolean needsExport) {
        Cloud.SnapshotInfoPB.Builder b = Cloud.SnapshotInfoPB.newBuilder()
                .setSnapshotId("000000001")
                .setSnapshotLabel("test-snap")
                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_NORMAL)
                .setCreateAt(System.currentTimeMillis() / 1000)
                .setTtlSeconds(86400)
                .setRowsetRefsSeeded(seeded)
                .setTableMetaExported(exported);
        if (needsExport) {
            b.addProtectedTableIds(1001L);
        }
        return b.build();
    }

    private Cloud.ListSnapshotResponse responseWith(Cloud.SnapshotInfoPB... snaps) {
        Cloud.ListSnapshotResponse.Builder b = Cloud.ListSnapshotResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK));
        for (Cloud.SnapshotInfoPB s : snaps) {
            b.addSnapshots(s);
        }
        return b.build();
    }

    @Test
    public void testStatePending() throws Exception {
        Cloud.SnapshotInfoPB snap = Cloud.SnapshotInfoPB.newBuilder()
                .setSnapshotId("1").setSnapshotLabel("s")
                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_PREPARE)
                .setCreateAt(System.currentTimeMillis() / 1000).setTtlSeconds(86400).build();
        Mockito.when(snapshotHandler.listSnapshot(false)).thenReturn(responseWith(snap));

        ShowResultSet rs = new ShowClusterSnapshotsCommand(false)
                .doRun(connectContext, Mockito.mock(StmtExecutor.class));
        Assertions.assertEquals("PENDING", rs.getResultRows().get(0).get(2));
    }

    @Test
    public void testStateSeeding() throws Exception {
        Mockito.when(snapshotHandler.listSnapshot(false))
                .thenReturn(responseWith(normalSnap(false, false, false)));
        ShowResultSet rs = new ShowClusterSnapshotsCommand(false)
                .doRun(connectContext, Mockito.mock(StmtExecutor.class));
        Assertions.assertEquals("SEEDING", rs.getResultRows().get(0).get(2));
    }

    @Test
    public void testStateExporting() throws Exception {
        // seeded=true, needsExport=true, exported=false → EXPORTING
        Mockito.when(snapshotHandler.listSnapshot(false))
                .thenReturn(responseWith(normalSnap(true, false, true)));
        ShowResultSet rs = new ShowClusterSnapshotsCommand(false)
                .doRun(connectContext, Mockito.mock(StmtExecutor.class));
        Assertions.assertEquals("EXPORTING", rs.getResultRows().get(0).get(2));
    }

    @Test
    public void testStateReady() throws Exception {
        // seeded=true, needsExport=true, exported=true → READY
        Mockito.when(snapshotHandler.listSnapshot(false))
                .thenReturn(responseWith(normalSnap(true, true, true)));
        ShowResultSet rs = new ShowClusterSnapshotsCommand(false)
                .doRun(connectContext, Mockito.mock(StmtExecutor.class));
        Assertions.assertEquals("READY", rs.getResultRows().get(0).get(2));
    }

    @Test
    public void testStateReadyFullCluster() throws Exception {
        // Full-cluster snapshot (no protected_table_ids): needsExport=false → READY once seeded.
        Mockito.when(snapshotHandler.listSnapshot(false))
                .thenReturn(responseWith(normalSnap(true, false, false)));
        ShowResultSet rs = new ShowClusterSnapshotsCommand(false)
                .doRun(connectContext, Mockito.mock(StmtExecutor.class));
        Assertions.assertEquals("READY", rs.getResultRows().get(0).get(2));
    }

    @Test
    public void testStateFailed() throws Exception {
        Cloud.SnapshotInfoPB snap = Cloud.SnapshotInfoPB.newBuilder()
                .setSnapshotId("1").setSnapshotLabel("s")
                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_ABORTED)
                .setCreateAt(System.currentTimeMillis() / 1000).setTtlSeconds(86400).build();
        Mockito.when(snapshotHandler.listSnapshot(true)).thenReturn(responseWith(snap));

        ShowResultSet rs = new ShowClusterSnapshotsCommand(true)
                .doRun(connectContext, Mockito.mock(StmtExecutor.class));
        Assertions.assertEquals("FAILED", rs.getResultRows().get(0).get(2));
    }

    @Test
    public void testStateDropped() throws Exception {
        // RECYCLED before TTL has passed → DROPPED
        long nowSec = System.currentTimeMillis() / 1000;
        Cloud.SnapshotInfoPB snap = Cloud.SnapshotInfoPB.newBuilder()
                .setSnapshotId("1").setSnapshotLabel("s")
                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_RECYCLED)
                .setCreateAt(nowSec - 3600)   // created 1h ago
                .setTtlSeconds(86400).build(); // TTL = 1 day (still valid)
        Mockito.when(snapshotHandler.listSnapshot(true)).thenReturn(responseWith(snap));

        ShowResultSet rs = new ShowClusterSnapshotsCommand(true)
                .doRun(connectContext, Mockito.mock(StmtExecutor.class));
        Assertions.assertEquals("DROPPED", rs.getResultRows().get(0).get(2));
    }

    @Test
    public void testStateExpired() throws Exception {
        // RECYCLED after TTL has passed → EXPIRED
        long nowSec = System.currentTimeMillis() / 1000;
        Cloud.SnapshotInfoPB snap = Cloud.SnapshotInfoPB.newBuilder()
                .setSnapshotId("1").setSnapshotLabel("s")
                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_RECYCLED)
                .setCreateAt(nowSec - 7200)   // created 2h ago
                .setTtlSeconds(3600).build(); // TTL = 1h (expired)
        Mockito.when(snapshotHandler.listSnapshot(true)).thenReturn(responseWith(snap));

        ShowResultSet rs = new ShowClusterSnapshotsCommand(true)
                .doRun(connectContext, Mockito.mock(StmtExecutor.class));
        Assertions.assertEquals("EXPIRED", rs.getResultRows().get(0).get(2));
    }

    @Test
    public void testDefaultViewFiltersTerminalStates() throws Exception {
        // SHOW CLUSTER SNAPSHOTS (includeAll=false) must hide EXPIRED, DROPPED, FAILED.
        long nowSec = System.currentTimeMillis() / 1000;
        Cloud.SnapshotInfoPB ready = normalSnap(true, true, true);
        Cloud.SnapshotInfoPB expired = Cloud.SnapshotInfoPB.newBuilder()
                .setSnapshotId("2").setSnapshotLabel("old")
                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_RECYCLED)
                .setCreateAt(nowSec - 7200).setTtlSeconds(3600).build();
        Mockito.when(snapshotHandler.listSnapshot(false)).thenReturn(responseWith(ready, expired));

        ShowResultSet rs = new ShowClusterSnapshotsCommand(false)
                .doRun(connectContext, Mockito.mock(StmtExecutor.class));
        // Only the READY row is visible; EXPIRED is filtered out.
        Assertions.assertEquals(1, rs.getResultRows().size());
        Assertions.assertEquals("READY", rs.getResultRows().get(0).get(2));
    }

    @Test
    public void testHistoryViewShowsAllStates() throws Exception {
        // SHOW CLUSTER SNAPSHOT HISTORY (includeAll=true) shows everything.
        long nowSec = System.currentTimeMillis() / 1000;
        Cloud.SnapshotInfoPB ready = normalSnap(true, true, true);
        Cloud.SnapshotInfoPB expired = Cloud.SnapshotInfoPB.newBuilder()
                .setSnapshotId("2").setSnapshotLabel("old")
                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_RECYCLED)
                .setCreateAt(nowSec - 7200).setTtlSeconds(3600).build();
        Cloud.SnapshotInfoPB failed = Cloud.SnapshotInfoPB.newBuilder()
                .setSnapshotId("3").setSnapshotLabel("fail")
                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_ABORTED)
                .setCreateAt(nowSec - 100).setTtlSeconds(86400).build();
        Mockito.when(snapshotHandler.listSnapshot(true)).thenReturn(responseWith(ready, expired, failed));

        ShowResultSet rs = new ShowClusterSnapshotsCommand(true)
                .doRun(connectContext, Mockito.mock(StmtExecutor.class));
        Assertions.assertEquals(3, rs.getResultRows().size());
    }

    @Test
    public void testExpiresInNever() throws Exception {
        // No TTL set → "never"
        Cloud.SnapshotInfoPB snap = Cloud.SnapshotInfoPB.newBuilder()
                .setSnapshotId("1").setSnapshotLabel("s")
                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_NORMAL)
                .setRowsetRefsSeeded(true).setTableMetaExported(false)
                .setCreateAt(System.currentTimeMillis() / 1000).build(); // no ttl_seconds
        Mockito.when(snapshotHandler.listSnapshot(false)).thenReturn(responseWith(snap));

        ShowResultSet rs = new ShowClusterSnapshotsCommand(false)
                .doRun(connectContext, Mockito.mock(StmtExecutor.class));
        Assertions.assertEquals("never", rs.getResultRows().get(0).get(5)); // ExpiresIn column
    }

    @Test
    public void testExpiresInDashForTerminalStates() throws Exception {
        // DROPPED → ExpiresIn = "-"
        long nowSec = System.currentTimeMillis() / 1000;
        Cloud.SnapshotInfoPB dropped = Cloud.SnapshotInfoPB.newBuilder()
                .setSnapshotId("1").setSnapshotLabel("s")
                .setStatus(Cloud.SnapshotStatus.SNAPSHOT_RECYCLED)
                .setCreateAt(nowSec - 100).setTtlSeconds(86400).build();
        Mockito.when(snapshotHandler.listSnapshot(true)).thenReturn(responseWith(dropped));

        ShowResultSet rs = new ShowClusterSnapshotsCommand(true)
                .doRun(connectContext, Mockito.mock(StmtExecutor.class));
        Assertions.assertEquals("-", rs.getResultRows().get(0).get(5));
    }

    @Test
    public void testResultSetHasNineColumns() throws Exception {
        Mockito.when(snapshotHandler.listSnapshot(false))
                .thenReturn(responseWith(normalSnap(true, true, true)));
        ShowResultSet rs = new ShowClusterSnapshotsCommand(false)
                .doRun(connectContext, Mockito.mock(StmtExecutor.class));
        // Label, SnapshotId, State, CreatedAt, ExpiresAt, ExpiresIn, Scope, Properties, SizeGB
        Assertions.assertEquals(9, rs.getMetaData().getColumnCount());
    }
}
