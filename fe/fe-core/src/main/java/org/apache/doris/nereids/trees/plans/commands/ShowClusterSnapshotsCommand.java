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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
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
 * SHOW CLUSTER SNAPSHOTS;         — usable snapshots only (PENDING/SEEDING/EXPORTING/READY)
 * SHOW CLUSTER SNAPSHOT HISTORY; — all snapshots including DROPPED/EXPIRED/FAILED
 *
 * State values:
 *   PENDING   — commit in progress; unusable
 *   SEEDING   — recycler placing rowset ref-count pins; wait before restoring
 *   EXPORTING — rowsets pinned, recycler writing fdb_meta_*.pb to S3
 *   READY     — fully usable for all restore paths
 *   DROPPED   — explicitly dropped before TTL expired; data deleted
 *   EXPIRED   — TTL passed naturally; data deleted
 *   FAILED    — snapshot was aborted; unusable
 */
public class ShowClusterSnapshotsCommand extends ShowCommand {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Label", ScalarType.createVarchar(256)));
        builder.addColumn(new Column("SnapshotId", ScalarType.createVarchar(64)));
        builder.addColumn(new Column("State", ScalarType.createVarchar(16)));
        builder.addColumn(new Column("RequestedAt", ScalarType.createVarchar(32)));
        builder.addColumn(new Column("ReadyAt", ScalarType.createVarchar(32)));
        builder.addColumn(new Column("ExpiresAt", ScalarType.createVarchar(32)));
        builder.addColumn(new Column("ExpiresIn", ScalarType.createVarchar(16)));
        builder.addColumn(new Column("DataAsOf", ScalarType.createVarchar(32)));
        builder.addColumn(new Column("Scope", ScalarType.createVarchar(512)));
        builder.addColumn(new Column("Properties", ScalarType.createVarchar(1024)));
        builder.addColumn(new Column("SizeGB", ScalarType.createVarchar(16)));
        META_DATA = builder.build();
    }

    // includeAll=true adds FAILED (aborted) snapshots; used by SHOW CLUSTER SNAPSHOT HISTORY.
    private final boolean includeAll;

    public ShowClusterSnapshotsCommand() {
        this(false);
    }

    public ShowClusterSnapshotsCommand(boolean includeAll) {
        super(PlanType.SHOW_CLUSTER_SNAPSHOTS_COMMAND);
        this.includeAll = includeAll;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // Cloud-only command — SHOW CLUSTER SNAPSHOTS has no meaning in shared-nothing
        // disk mode where SHOW SNAPSHOT ON repo is the backup observability command.
        if (!Config.isCloudMode()) {
            throw new AnalysisException("The sql is illegal in disk mode");
        }
        if (!(Env.getCurrentEnv() instanceof CloudEnv)) {
            throw new AnalysisException("SHOW CLUSTER SNAPSHOTS requires a CloudEnv instance");
        }

        CloudEnv cloudEnv = (CloudEnv) Env.getCurrentEnv();
        Cloud.ListSnapshotResponse resp = cloudEnv.getCloudSnapshotHandler().listSnapshot(includeAll);

        List<List<String>> rows = Lists.newArrayList();
        for (Cloud.SnapshotInfoPB snap : resp.getSnapshotsList()) {
            String state = computeState(snap);
            // Default view: only usable snapshots. History view: everything.
            if (!includeAll && (state.equals("EXPIRED") || state.equals("DROPPED")
                    || state.equals("FAILED"))) {
                continue;
            }
            String requestedAt = snap.hasCreateAt() ? formatEpoch(snap.getCreateAt()) : "-";
            // V2: ReadyAt = RequestedAt (data frozen at FDB versionstamp). V1: ReadyAt = exported_at.
            boolean isPointInTime = isPointInTime(snap);
            String readyAt = isPointInTime ? requestedAt
                    : (snap.hasExportedAt() ? formatEpoch(snap.getExportedAt())
                    : (state.equals("READY") ? requestedAt : "-"));
            // TTL anchor: V2 → create_at, V1 → exported_at, fallback → create_at.
            long ttlAnchor = isPointInTime ? (snap.hasCreateAt() ? snap.getCreateAt() : 0L)
                    : (snap.hasExportedAt() ? snap.getExportedAt()
                    : (snap.hasCreateAt() ? snap.getCreateAt() : 0L));
            String expiresAt = (ttlAnchor > 0 && snap.hasTtlSeconds() && snap.getTtlSeconds() > 0)
                    ? formatEpoch(ttlAnchor + snap.getTtlSeconds()) : "never";
            String expiresIn = computeExpiresIn(snap, state, ttlAnchor);
            // DataAsOf: V2 → RequestedAt (FDB versionstamp anchor), V1 → ReadyAt (export anchor).
            String dataAsOf = isPointInTime(snap) ? requestedAt : readyAt;
            String sizeGb = snap.hasSnapshotRetainedDataSize()
                    ? String.format("%.2f", snap.getSnapshotRetainedDataSize() / 1073741824.0) : "-";

            rows.add(Lists.newArrayList(
                    snap.getSnapshotLabel(),
                    snap.getSnapshotId(),
                    state,
                    requestedAt,
                    readyAt,
                    expiresAt,
                    expiresIn,
                    dataAsOf,
                    resolveScope(snap),
                    buildProperties(snap),
                    sizeGb));
        }
        return new ShowResultSet(META_DATA, rows);
    }

    // Maps proto status + flags to the State values documented in the class Javadoc.
    private static String computeState(Cloud.SnapshotInfoPB snap) {
        switch (snap.getStatus()) {
            case SNAPSHOT_PREPARE:
                return "PENDING";
            case SNAPSHOT_ABORTED:
                return "FAILED";
            case SNAPSHOT_RECYCLED:
                // Use same TTL anchor as ExpiresAt/ExpiresIn: exported_at for V1, create_at fallback.
                if (snap.hasTtlSeconds() && snap.getTtlSeconds() > 0) {
                    long anchor = snap.hasExportedAt() ? snap.getExportedAt()
                            : (snap.hasCreateAt() ? snap.getCreateAt() : 0L);
                    if (anchor > 0 && System.currentTimeMillis() / 1000 < anchor + snap.getTtlSeconds()) {
                        return "DROPPED";
                    }
                }
                return "EXPIRED";
            case SNAPSHOT_NORMAL:
                if (!snap.hasRowsetRefsSeeded() || !snap.getRowsetRefsSeeded()) {
                    return "SEEDING";
                }
                if (needsExport(snap) && (!snap.hasTableMetaExported() || !snap.getTableMetaExported())) {
                    return "EXPORTING";
                }
                return "READY";
            default:
                return "UNKNOWN";
        }
    }

    private static boolean needsExport(Cloud.SnapshotInfoPB snap) {
        return !snap.getProtectedTableIdsList().isEmpty()
                || !snap.getProtectedPartitionIdsList().isEmpty()
                || !snap.getProtectedDbIdsList().isEmpty();
    }

    // ── ExpiresIn: "2d 3h", "45m", "< 1m", "never", "-" ─────────────────────

    private static String computeExpiresIn(Cloud.SnapshotInfoPB snap, String state, long ttlAnchor) {
        if ("EXPIRED".equals(state) || "DROPPED".equals(state) || "FAILED".equals(state)) {
            return "-";
        }
        if (ttlAnchor <= 0 || !snap.hasTtlSeconds() || snap.getTtlSeconds() <= 0) {
            return "never";
        }
        long expiresEpochSec = ttlAnchor + snap.getTtlSeconds();
        long remainingSec = expiresEpochSec - (System.currentTimeMillis() / 1000);
        if (remainingSec <= 0) {
            return "< 1m";
        }
        return formatDuration(remainingSec);
    }

    // ── DataAsOf helpers ─────────────────────────────────────────────────────

    private static boolean isPointInTime(Cloud.SnapshotInfoPB snap) {
        if (!snap.hasSnapshotClusterVersion()) {
            return false;
        }
        Cloud.MultiVersionStatus v = snap.getSnapshotClusterVersion();
        return v == Cloud.MultiVersionStatus.MULTI_VERSION_ENABLED
                || v == Cloud.MultiVersionStatus.MULTI_VERSION_READ_WRITE;
    }

    private static String formatDuration(long seconds) {
        long days = seconds / 86400;
        long hours = (seconds % 86400) / 3600;
        long minutes = (seconds % 3600) / 60;

        if (days > 0) {
            return hours > 0 ? days + "d " + hours + "h" : days + "d";
        }
        if (hours > 0) {
            return minutes > 0 ? hours + "h " + minutes + "m" : hours + "h";
        }
        return minutes > 0 ? minutes + "m" : "< 1m";
    }

    private static String resolveScope(Cloud.SnapshotInfoPB snap) {
        if (!snap.getProtectedPartitionIdsList().isEmpty()) {
            return "Partitions (" + snap.getProtectedPartitionIdsCount() + ")";
        }
        if (!snap.getProtectedTableIdsList().isEmpty()) {
            return "Tables (" + snap.getProtectedTableIdsCount() + ")";
        }
        if (!snap.getProtectedDbIdsList().isEmpty()) {
            return "DBs (" + snap.getProtectedDbIdsCount() + ")";
        }
        return "Full cluster";
    }

    // Reconstructs creation args as a short string, e.g. "ttl=7d  for_dbs=orders"
    private static String buildProperties(Cloud.SnapshotInfoPB snap) {
        StringBuilder sb = new StringBuilder();

        // TTL
        if (snap.hasTtlSeconds() && snap.getTtlSeconds() > 0) {
            sb.append("ttl=").append(formatTtl(snap.getTtlSeconds()));
        }

        // auto
        if (snap.hasAutoSnapshot() && snap.getAutoSnapshot()) {
            append(sb, "auto=true");
        }

        // for_dbs: resolve IDs → names where possible
        if (!snap.getProtectedDbIdsList().isEmpty()) {
            String names = resolveDbNames(snap.getProtectedDbIdsList());
            append(sb, "for_dbs=" + names);
        }

        // for_tables: resolve IDs → names where possible
        if (!snap.getProtectedTableIdsList().isEmpty()) {
            String names = resolveTableNames(snap.getProtectedTableIdsList());
            append(sb, "for_tables=" + names);
        }

        // for_partitions: resolving partition names requires nested table scan — show count
        if (!snap.getProtectedPartitionIdsList().isEmpty()) {
            append(sb, "for_partitions=(" + snap.getProtectedPartitionIdsCount() + " partitions)");
        }

        // NOTE: DR standby info (dr_instance_id) is not included here because
        // derived_instance_ids is not populated by fill_snapshot_info in list_snapshot.
        // Future: populate via find_derived_instance_ids() in fill_snapshot_info.

        return sb.length() == 0 ? "-" : sb.toString();
    }

    private static void append(StringBuilder sb, String part) {
        if (sb.length() > 0) {
            sb.append("  ");
        }
        sb.append(part);
    }

    private static String formatTtl(long seconds) {
        if (seconds % 86400 == 0) {
            return (seconds / 86400) + "d";
        }
        if (seconds % 3600 == 0) {
            return (seconds / 3600) + "h";
        }
        return seconds + "s";
    }

    private static String resolveDbNames(List<Long> dbIds) {
        List<String> names = Lists.newArrayList();
        for (long id : dbIds) {
            try {
                Database db = Env.getCurrentInternalCatalog().getDbNullable(id);
                names.add(db != null ? db.getName() : "id=" + id);
            } catch (Exception ignored) {
                // intentionally ignored — catalog may be temporarily unavailable; fall back to id
                names.add("id=" + id);
            }
        }
        return String.join(", ", names);
    }

    private static String resolveTableNames(List<Long> tableIds) {
        List<String> names = Lists.newArrayList();
        for (long tableId : tableIds) {
            String resolved = null;
            try {
                for (Database db : Env.getCurrentInternalCatalog().getDbs()) {
                    Table t = db.getTableNullable(tableId);
                    if (t != null) {
                        resolved = db.getName() + "." + t.getName();
                        break;
                    }
                }
            } catch (Exception ignored) {
                // intentionally ignored — fall back to id
            }
            names.add(resolved != null ? resolved : "id=" + tableId);
        }
        return String.join(", ", names);
    }

    private static String formatEpoch(long epochSeconds) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds), ZoneOffset.UTC)
                .format(FMT);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowClusterSnapshotsCommand(this, context);
    }
}
