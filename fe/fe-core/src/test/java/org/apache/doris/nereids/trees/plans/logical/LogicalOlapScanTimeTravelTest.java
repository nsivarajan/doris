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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that timeTravelTimestampMs is preserved through all with* optimizer rewrites.
 *
 * The time travel timestamp is set during FE analysis (BindRelation) and must survive
 * the full optimizer pipeline — partition pruning, tablet pruning, pre-agg status
 * changes, MV rewrite — before reaching PhysicalPlanTranslator.
 *
 * Uses the same OlapTable construction pattern as PlanConstructor to ensure
 * the scan is created with a valid, properly initialised table object.
 */
public class LogicalOlapScanTimeTravelTest {

    private static OlapTable testTable;
    private static final IdGenerator<RelationId> ID_GEN = RelationId.createGenerator();

    @BeforeClass
    public static void setup() {
        testTable = new OlapTable(100L, "orders",
                ImmutableList.of(
                        new Column("id", Type.BIGINT, true, AggregateType.NONE, "0", ""),
                        new Column("amount", Type.DOUBLE, false, AggregateType.NONE, "0", "")),
                KeysType.DUP_KEYS, new PartitionInfo(), null);
        testTable.setIndexMeta(-1, "orders", testTable.getFullSchema(),
                0, 0, (short) 1,
                org.apache.doris.thrift.TStorageType.COLUMN, KeysType.DUP_KEYS);
    }

    private static LogicalOlapScan newScan() {
        return new LogicalOlapScan(ID_GEN.getNextId(), testTable, ImmutableList.of("db"));
    }

    // -------------------------------------------------------------------------
    // Basic field behaviour
    // -------------------------------------------------------------------------

    @Test
    public void testNoTimeTravelByDefault() {
        LogicalOlapScan scan = newScan();
        Assert.assertFalse("new scan must not have time travel set", scan.hasTimeTravelTimestampMs());
        Assert.assertEquals(-1L, scan.getTimeTravelTimestampMs());
    }

    @Test
    public void testWithTimeTravelTimestampMs_setsAndReads() {
        long ts = 1_700_000_000_000L;
        LogicalOlapScan scan = newScan().withTimeTravelTimestampMs(ts);
        Assert.assertTrue(scan.hasTimeTravelTimestampMs());
        Assert.assertEquals(ts, scan.getTimeTravelTimestampMs());
    }

    @Test
    public void testWithTimeTravelTimestampMs_zeroIsValid() {
        // timestamp 0 = epoch; hasTimeTravelTimestampMs() checks >= 0
        LogicalOlapScan scan = newScan().withTimeTravelTimestampMs(0L);
        Assert.assertTrue(scan.hasTimeTravelTimestampMs());
        Assert.assertEquals(0L, scan.getTimeTravelTimestampMs());
    }

    // -------------------------------------------------------------------------
    // Preservation through with* rewrites — the critical regression suite.
    // Each with* must carry timeTravelTimestampMs forward; if it doesn't, the
    // optimizer would silently drop time travel and query current data instead.
    // -------------------------------------------------------------------------

    @Test
    public void testWithPreAggStatus_preservesTimeTravel() {
        long ts = 1_700_000_000_000L;
        LogicalOlapScan after = newScan()
                .withTimeTravelTimestampMs(ts)
                .withPreAggStatus(PreAggStatus.off("test"));
        Assert.assertTrue("withPreAggStatus must not reset timeTravelTimestampMs",
                after.hasTimeTravelTimestampMs());
        Assert.assertEquals(ts, after.getTimeTravelTimestampMs());
    }

    @Test
    public void testWithSelectedPartitionIds_preservesTimeTravel() {
        long ts = 1_700_000_000_000L;
        LogicalOlapScan after = newScan()
                .withTimeTravelTimestampMs(ts)
                .withSelectedPartitionIds(ImmutableList.of(1L, 2L));
        Assert.assertTrue("withSelectedPartitionIds must not reset timeTravelTimestampMs",
                after.hasTimeTravelTimestampMs());
        Assert.assertEquals(ts, after.getTimeTravelTimestampMs());
    }

    @Test
    public void testWithSelectedTabletIds_preservesTimeTravel() {
        long ts = 1_700_000_000_000L;
        LogicalOlapScan after = newScan()
                .withTimeTravelTimestampMs(ts)
                .withSelectedTabletIds(ImmutableList.of(10L, 20L));
        Assert.assertTrue("withSelectedTabletIds must not reset timeTravelTimestampMs",
                after.hasTimeTravelTimestampMs());
        Assert.assertEquals(ts, after.getTimeTravelTimestampMs());
    }

    @Test
    public void testWithTableScanParams_preservesTimeTravel() {
        long ts = 1_700_000_000_000L;
        // Use a real (non-null) TableScanParams instance to avoid Optional.of(null) NPE
        TableScanParams params = new TableScanParams("test", ImmutableMap.of(), ImmutableList.of());
        LogicalOlapScan after = newScan()
                .withTimeTravelTimestampMs(ts)
                .withTableScanParams(params);
        Assert.assertTrue("withTableScanParams must not reset timeTravelTimestampMs",
                after.hasTimeTravelTimestampMs());
        Assert.assertEquals(ts, after.getTimeTravelTimestampMs());
    }

    @Test
    public void testWithManuallySpecifiedTabletIds_preservesTimeTravel() {
        long ts = 1_700_000_000_000L;
        LogicalOlapScan after = newScan()
                .withTimeTravelTimestampMs(ts)
                .withManuallySpecifiedTabletIds(ImmutableList.of(5L));
        Assert.assertTrue("withManuallySpecifiedTabletIds must not reset timeTravelTimestampMs",
                after.hasTimeTravelTimestampMs());
        Assert.assertEquals(ts, after.getTimeTravelTimestampMs());
    }

    // -------------------------------------------------------------------------
    // Non-regression: normal scans must not be affected
    // -------------------------------------------------------------------------

    @Test
    public void testNormalScan_unaffectedByWithPreAggStatus() {
        LogicalOlapScan scan = newScan();
        Assert.assertFalse(scan.hasTimeTravelTimestampMs());

        LogicalOlapScan after = scan.withPreAggStatus(PreAggStatus.off("no-agg"));
        Assert.assertFalse("non-time-travel scan must stay non-time-travel after withPreAggStatus",
                after.hasTimeTravelTimestampMs());
        Assert.assertEquals(-1L, after.getTimeTravelTimestampMs());
    }

    @Test
    public void testNormalScan_unaffectedByWithSelectedPartitionIds() {
        LogicalOlapScan scan = newScan();
        LogicalOlapScan after = scan.withSelectedPartitionIds(ImmutableList.of(1L));
        Assert.assertFalse("non-time-travel scan must stay non-time-travel after partition pruning",
                after.hasTimeTravelTimestampMs());
    }

    @Test
    public void testChainedRewrites_preserveTimeTravel() {
        // Simulate a realistic optimizer pipeline:
        //   setTimestampMs → withPreAggStatus → withSelectedPartitionIds → withSelectedTabletIds
        long ts = 1_700_000_000_000L;
        LogicalOlapScan after = newScan()
                .withTimeTravelTimestampMs(ts)
                .withPreAggStatus(PreAggStatus.off("test"))
                .withSelectedPartitionIds(ImmutableList.of(1L))
                .withSelectedTabletIds(ImmutableList.of(10L));
        Assert.assertTrue("chained rewrites must preserve timeTravelTimestampMs",
                after.hasTimeTravelTimestampMs());
        Assert.assertEquals(ts, after.getTimeTravelTimestampMs());
    }
}
