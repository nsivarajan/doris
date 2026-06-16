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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * SHOW CLUSTER SNAPSHOTS FOR DR
 *
 * Lists full-cluster READY snapshots that can be used for DR via fdbrestore.
 * Filters: full-cluster scope (no for_dbs/for_tables/for_partitions), READY state,
 * rowset_refs_seeded=true, table_meta_exported=true.
 * Output includes FdbVersion (pass directly to fdbrestore --version=) and BdbjeImageUrl.
 */
public class ShowClusterSnapshotsForDrCommand extends ShowCommand {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("SnapshotId", ScalarType.createVarchar(64)));
        builder.addColumn(new Column("Label", ScalarType.createVarchar(256)));
        builder.addColumn(new Column("FdbVersion", ScalarType.createVarchar(24)));
        builder.addColumn(new Column("BdbjeImageUrl", ScalarType.createVarchar(512)));
        builder.addColumn(new Column("DrManifest", ScalarType.createVarchar(512)));
        builder.addColumn(new Column("CreatedAt", ScalarType.createVarchar(32)));
        builder.addColumn(new Column("ExpiresIn", ScalarType.createVarchar(16)));
        builder.addColumn(new Column("Tables", ScalarType.createVarchar(8)));
        META_DATA = builder.build();
    }

    public ShowClusterSnapshotsForDrCommand() {
        super(PlanType.SHOW_CLUSTER_SNAPSHOTS_FOR_DR_COMMAND);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Config.isCloudMode()) {
            throw new AnalysisException("The sql is illegal in disk mode");
        }
        if (!(Env.getCurrentEnv() instanceof CloudEnv)) {
            throw new AnalysisException("SHOW CLUSTER SNAPSHOTS FOR DR requires a CloudEnv instance");
        }

        CloudEnv cloudEnv = (CloudEnv) Env.getCurrentEnv();
        Cloud.ListSnapshotResponse resp = cloudEnv.getCloudSnapshotHandler().listSnapshot(false);

        long nowSec = System.currentTimeMillis() / 1000;
        List<List<String>> rows = Lists.newArrayList();

        for (Cloud.SnapshotInfoPB snap : resp.getSnapshotsList()) {
            if (!isDrEligible(snap, nowSec)) {
                continue;
            }
            String snapshotId = snap.getSnapshotId();
            String fdbVersion = String.valueOf(parseFdbVersion(snapshotId));
            // DR manifest path: derive snapshot dir from image_url, append dr/latest/runbook.txt
            String drManifest = deriveSnapshotDir(snap.getImageUrl()) + "/dr_runbook.txt";
            String createdAt = snap.hasCreateAt() ? formatEpoch(snap.getCreateAt()) : "-";
            String expiresIn = computeExpiresIn(snap, nowSec);
            String tables = String.valueOf(snap.getCapturedTablesCount());

            rows.add(Lists.newArrayList(
                    snapshotId,
                    snap.getSnapshotLabel(),
                    fdbVersion,
                    snap.getImageUrl(),
                    drManifest,
                    createdAt,
                    expiresIn,
                    tables));
        }
        return new ShowResultSet(META_DATA, rows);
    }

    /** Returns true only for full-cluster READY snapshots usable for DR via fdbrestore. */
    private static boolean isDrEligible(Cloud.SnapshotInfoPB snap, long nowSec) {
        if (snap.getStatus() != Cloud.SnapshotStatus.SNAPSHOT_NORMAL) {
            return false;
        }
        // Must be fully seeded and exported.
        if (!snap.hasRowsetRefsSeeded() || !snap.getRowsetRefsSeeded()) {
            return false;
        }
        if (!snap.hasTableMetaExported() || !snap.getTableMetaExported()) {
            return false;
        }
        // Full-cluster only (no scoped protection).
        if (snap.getProtectedDbIdsCount() > 0 || snap.getProtectedTableIdsCount() > 0
                || snap.getProtectedPartitionIdsCount() > 0) {
            return false;
        }
        // Not expired.
        if (snap.hasTtlSeconds() && snap.getTtlSeconds() > 0 && snap.hasCreateAt()) {
            if (nowSec > snap.getCreateAt() + snap.getTtlSeconds()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Parses the first 8 bytes (16 hex chars) of snapshot_id as a big-endian int64.
     * This is the FDB transaction version — pass directly to fdbrestore --version=.
     */
    static long parseFdbVersion(String snapshotId) {
        if (snapshotId == null || snapshotId.length() < 16) {
            return -1L;
        }
        long ver = 0;
        for (int i = 0; i < 16; i++) {
            int digit = Character.digit(snapshotId.charAt(i), 16);
            if (digit < 0) {
                return -1L;
            }
            ver = (ver << 4) | digit;
        }
        return ver;
    }

    /** Strips the filename from image_url to get the snapshot S3 directory. */
    private static String deriveSnapshotDir(String imageUrl) {
        if (imageUrl == null || imageUrl.isEmpty()) {
            return "unknown";
        }
        int slash = imageUrl.lastIndexOf('/');
        return slash > 0 ? imageUrl.substring(0, slash) : imageUrl;
    }

    private static String computeExpiresIn(Cloud.SnapshotInfoPB snap, long nowSec) {
        if (!snap.hasTtlSeconds() || snap.getTtlSeconds() <= 0 || !snap.hasCreateAt()) {
            return "never";
        }
        long remaining = snap.getCreateAt() + snap.getTtlSeconds() - nowSec;
        if (remaining <= 0) {
            return "< 1m";
        }
        long days = remaining / 86400;
        long hours = (remaining % 86400) / 3600;
        long minutes = (remaining % 3600) / 60;
        if (days > 0) {
            return hours > 0 ? days + "d " + hours + "h" : days + "d";
        }
        if (hours > 0) {
            return minutes > 0 ? hours + "h " + minutes + "m" : hours + "h";
        }
        return minutes > 0 ? minutes + "m" : "< 1m";
    }

    private static String formatEpoch(long epochSeconds) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds), ZoneOffset.UTC)
                .format(FMT);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowClusterSnapshotsForDrCommand(this, context);
    }
}
