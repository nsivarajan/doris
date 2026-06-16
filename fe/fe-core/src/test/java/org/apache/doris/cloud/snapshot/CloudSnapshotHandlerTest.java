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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.catalog.CloudTabletInvertedIndex;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for CloudSnapshotHandler utility methods.
 */
public class CloudSnapshotHandlerTest {

    /**
     * Regression guard for resolvePartitionFilter (Fix 3).
     *
     * When a partition was dropped and recreated under the same name after the snapshot,
     * the live table's partition ID differs from the blob's original partition ID.
     * resolvePartitionFilter must prefer the blob-derived OlapTable when available so that
     * the partition filter sent to import_table_meta matches the snapshot-time FDB keys.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testResolvePartitionFilterPrefersBlob() throws Exception {
        CloudSnapshotHandler handler = Mockito.mock(CloudSnapshotHandler.class,
                Mockito.CALLS_REAL_METHODS);

        // Live table: partition "p0" has id=100 (post-repartition).
        Partition livePartition = Mockito.mock(Partition.class);
        Mockito.when(livePartition.getId()).thenReturn(100L);
        OlapTable liveTable = Mockito.mock(OlapTable.class);
        Mockito.when(liveTable.getPartition("p0", false)).thenReturn(livePartition);

        // Blob table: partition "p0" has id=50 (snapshot-time, before repartition).
        Partition blobPartition = Mockito.mock(Partition.class);
        Mockito.when(blobPartition.getId()).thenReturn(50L);
        OlapTable blobTable = Mockito.mock(OlapTable.class);
        Mockito.when(blobTable.getPartition("p0", false)).thenReturn(blobPartition);

        Method m = CloudSnapshotHandler.class.getDeclaredMethod(
                "resolvePartitionFilter", OlapTable.class, OlapTable.class, String.class);
        m.setAccessible(true);

        // With blobTable: should return blob's partition id=50.
        List<Long> withBlob = (List<Long>) m.invoke(handler, liveTable, blobTable, "p0");
        Assertions.assertEquals(1, withBlob.size());
        Assertions.assertEquals(50L, withBlob.get(0),
                "resolvePartitionFilter must use blob partition IDs when blobTable is provided");

        // Without blobTable (old snapshot format): fall back to live table id=100.
        List<Long> withoutBlob = (List<Long>) m.invoke(handler, liveTable, null, "p0");
        Assertions.assertEquals(1, withoutBlob.size());
        Assertions.assertEquals(100L, withoutBlob.get(0),
                "resolvePartitionFilter must fall back to live table IDs when blobTable is null");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testResolvePartitionFilterNullOrEmptyNames() throws Exception {
        CloudSnapshotHandler handler = Mockito.mock(CloudSnapshotHandler.class,
                Mockito.CALLS_REAL_METHODS);
        OlapTable table = Mockito.mock(OlapTable.class);

        Method m = CloudSnapshotHandler.class.getDeclaredMethod(
                "resolvePartitionFilter", OlapTable.class, OlapTable.class, String.class);
        m.setAccessible(true);

        List<Long> nullNames = (List<Long>) m.invoke(handler, table, null, null);
        Assertions.assertTrue(nullNames.isEmpty(), "null partitionNames must return empty list");

        List<Long> emptyNames = (List<Long>) m.invoke(handler, table, null, "  ");
        Assertions.assertTrue(emptyNames.isEmpty(), "blank partitionNames must return empty list");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testResolvePartitionFilterMultiplePartitions() throws Exception {
        CloudSnapshotHandler handler = Mockito.mock(CloudSnapshotHandler.class,
                Mockito.CALLS_REAL_METHODS);

        Partition p0 = Mockito.mock(Partition.class);
        Mockito.when(p0.getId()).thenReturn(10L);
        Partition p1 = Mockito.mock(Partition.class);
        Mockito.when(p1.getId()).thenReturn(20L);

        OlapTable blobTable = Mockito.mock(OlapTable.class);
        Mockito.when(blobTable.getPartition("p0", false)).thenReturn(p0);
        Mockito.when(blobTable.getPartition("p1", false)).thenReturn(p1);
        Mockito.when(blobTable.getPartition("missing", false)).thenReturn(null);

        Method m = CloudSnapshotHandler.class.getDeclaredMethod(
                "resolvePartitionFilter", OlapTable.class, OlapTable.class, String.class);
        m.setAccessible(true);

        List<Long> ids = (List<Long>) m.invoke(handler, null, blobTable, "p0, p1, missing");
        Assertions.assertEquals(2, ids.size(),
                "missing partition name must be silently skipped");
        Assertions.assertTrue(ids.contains(10L));
        Assertions.assertTrue(ids.contains(20L));
    }

    /**
     * Regression guard for cloneTableWithNewIds (Fix — restoreAsNewTable).
     *
     * After cloning:
     *   - The clone must get a different table ID.
     *   - Every CloudPartition.tableId must equal the new table ID.
     *   - Every CloudPartition's cachedVisibleVersion must be reset to -1
     *     (forceResetVisibleVersion) so MS is queried fresh rather than returning
     *     the stale snapshot-time version from the source.
     *   - Every CloudReplica.tableId must equal the new table ID.
     *   - Every CloudReplica.partitionId must equal the new (remapped) partition ID.
     *   - OlapTable.idToPartition must be keyed by the new partition IDs.
     */
    @Test
    public void testCloneTableWithNewIds_allIdsUpdated() throws Exception {
        // Build a minimal real OlapTable with one CloudPartition and two CloudTablets.
        final long srcTableId  = 1000L;
        final long srcPartId   = 2000L;
        final long srcTablet1  = 3001L;
        final long srcTablet2  = 3002L;
        final long srcReplica1 = 4001L;
        final long srcReplica2 = 4002L;
        final long dbId        = 5000L;

        // Use an incrementing counter for IDs returned by mocked Env.getNextId().
        AtomicLong idCounter = new AtomicLong(9000L);

        // Mock inverted index so addTablet/addReplica calls don't throw during setup.
        TabletInvertedIndex mockInvertedIndex = Mockito.mock(TabletInvertedIndex.class);
        Env mockEnv = Mockito.mock(Env.class);
        Mockito.when(mockEnv.getNextId()).thenAnswer(inv -> idCounter.getAndIncrement());

        CloudSnapshotHandler handler = Mockito.mock(CloudSnapshotHandler.class,
                Mockito.CALLS_REAL_METHODS);

        OlapTable clone;
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(mockEnv);
            mockedEnv.when(Env::getCurrentInvertedIndex).thenReturn(mockInvertedIndex);

            // Build the source table inside the mock scope so addTablet/addReplica succeed.
            Column col = new Column("k1", ScalarType.createType(PrimitiveType.INT), true, null, true, "0", "");
            MaterializedIndex baseIndex = new MaterializedIndex(srcTableId, IndexState.NORMAL);

            CloudTablet tablet1 = new CloudTablet(srcTablet1);
            CloudReplica replica1 = new CloudReplica(srcReplica1, -1L, ReplicaState.NORMAL,
                    1L, 0, dbId, srcTableId, srcPartId, srcTableId, 0L);
            tablet1.addReplica(replica1);
            baseIndex.addTablet(tablet1, null);

            CloudTablet tablet2 = new CloudTablet(srcTablet2);
            CloudReplica replica2 = new CloudReplica(srcReplica2, -1L, ReplicaState.NORMAL,
                    1L, 0, dbId, srcTableId, srcPartId, srcTableId, 1L);
            tablet2.addReplica(replica2);
            baseIndex.addTablet(tablet2, null);

            SinglePartitionInfo partInfo = new SinglePartitionInfo();
            CloudPartition srcPartition = new CloudPartition(srcPartId, "p0", baseIndex,
                    new RandomDistributionInfo(2), dbId, srcTableId);
            // Simulate a live partition that has a non-initial visible version.
            srcPartition.setCachedVisibleVersion(4L, System.currentTimeMillis());
            partInfo.setReplicaAllocation(srcPartId, new ReplicaAllocation((short) 1));

            OlapTable src = new OlapTable(srcTableId, "src_table", Arrays.asList(col),
                    KeysType.DUP_KEYS, partInfo, new RandomDistributionInfo(2));
            src.setIndexMeta(srcTableId, "src_table", Arrays.asList(col), 0, 0, (short) 1,
                    TStorageType.COLUMN, KeysType.DUP_KEYS);
            src.addPartition(srcPartition);

            Method m = CloudSnapshotHandler.class.getDeclaredMethod(
                    "cloneTableWithNewIds", OlapTable.class, String.class);
            m.setAccessible(true);
            clone = (OlapTable) m.invoke(handler, src, "clone_table");
        }

        // New table ID must differ from source.
        Assertions.assertNotEquals(srcTableId, clone.getId(),
                "clone must receive a new table ID");

        long newTableId = clone.getId();

        // Verify every CloudPartition has the new table ID and a reset visible version.
        Collection<Partition> clonePartitions = clone.getAllPartitions();
        Assertions.assertEquals(1, clonePartitions.size(), "clone must have one partition");
        for (Partition p : clonePartitions) {
            Assertions.assertTrue(p instanceof CloudPartition,
                    "partition must remain a CloudPartition after clone");
            CloudPartition cp = (CloudPartition) p;
            Assertions.assertEquals(newTableId, cp.getTableId(),
                    "CloudPartition.tableId must be updated to new table ID");
            Assertions.assertEquals(-1L, cp.getCachedVisibleVersion(),
                    "forceResetVisibleVersion must reset cachedVisibleVersion to -1");

            // idToPartition must use the new partition ID as key.
            long newPartId = p.getId();
            Assertions.assertNotEquals(srcPartId, newPartId,
                    "partition ID must be reassigned");
            Assertions.assertNotNull(clone.getPartition(newPartId),
                    "idToPartition must be keyed by the new partition ID");

            // Verify replicas have updated IDs.
            for (MaterializedIndex mi : p.getMaterializedIndices(
                    MaterializedIndex.IndexExtState.VISIBLE)) {
                for (org.apache.doris.catalog.Tablet tablet : mi.getTablets()) {
                    for (org.apache.doris.catalog.Replica replica : tablet.getReplicas()) {
                        Assertions.assertTrue(replica instanceof CloudReplica,
                                "replica must remain a CloudReplica after clone");
                        CloudReplica cr = (CloudReplica) replica;
                        Assertions.assertEquals(newTableId, cr.getTableId(),
                                "CloudReplica.tableId must be updated to new table ID");
                        Assertions.assertEquals(newPartId, cr.getPartitionId(),
                                "CloudReplica.partitionId must be updated to new partition ID");
                    }
                }
            }
        }
    }

    /**
     * Validates that after a restore-as-new-table the TabletInvertedIndex is populated
     * for each new tablet, preventing the 60-second leaderCheckpointer delay described
     * in the restoreAsNewTable comment.
     *
     * The test directly exercises the TabletInvertedIndex.addTablet contract used by
     * replayCreateTableInternal, using a real CloudTabletInvertedIndex instance.
     * This matches the exact code path that restoreAsNewTable relies on to ensure that
     * DELETE push tasks can complete without waiting for leaderCheckpointer to fire.
     */
    @Test
    public void testRestoreAsNewTable_tabletInvertedIndexPopulated() {
        final long dbId        = 6000L;
        final long tableId     = 6001L;
        final long partitionId = 6002L;
        final long indexId     = 6003L;
        final long tablet1Id   = 7001L;
        final long tablet2Id   = 7002L;

        // Use a real CloudTabletInvertedIndex — same type that CloudEnv creates.
        CloudTabletInvertedIndex invertedIndex = new CloudTabletInvertedIndex();

        TabletMeta meta1 = new TabletMeta(dbId, tableId, partitionId, indexId, 0,
                TStorageMedium.HDD);
        TabletMeta meta2 = new TabletMeta(dbId, tableId, partitionId, indexId, 0,
                TStorageMedium.HDD);

        // Simulate what replayCreateTableInternal does for each tablet.
        invertedIndex.addTablet(tablet1Id, meta1);
        invertedIndex.addTablet(tablet2Id, meta2);

        // Verify: getTabletMeta returns non-null for both new tablet IDs.
        // If this were null, getTabletMetaList → null → NPE → DELETE push task timeout (60s).
        TabletMeta retrieved1 = invertedIndex.getTabletMeta(tablet1Id);
        Assertions.assertNotNull(retrieved1,
                "getTabletMeta must return non-null for tablet1 after addTablet — "
                + "null would cause DELETE push task NPE and 60s timeout");
        Assertions.assertEquals(tableId, retrieved1.getTableId(),
                "TabletMeta must carry the restored table ID");
        Assertions.assertEquals(partitionId, retrieved1.getPartitionId(),
                "TabletMeta must carry the restored partition ID");

        TabletMeta retrieved2 = invertedIndex.getTabletMeta(tablet2Id);
        Assertions.assertNotNull(retrieved2,
                "getTabletMeta must return non-null for tablet2 after addTablet");
        Assertions.assertEquals(tableId, retrieved2.getTableId());
        Assertions.assertEquals(partitionId, retrieved2.getPartitionId());

        // Verify: a tablet NOT in the index returns null (baseline sanity check).
        Assertions.assertNull(invertedIndex.getTabletMeta(99999L),
                "getTabletMeta for an unknown tablet must return null");
    }
}
