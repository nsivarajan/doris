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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud.ListSnapshotResponse;
import org.apache.doris.cloud.proto.Cloud.SnapshotInfoPB;
import org.apache.doris.cloud.snapshot.CloudSnapshotHandler;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * SHOW CLUSTER SNAPSHOTS
 *
 * Displays all READY (and optionally ABORTED) cluster snapshots for this instance.
 * Columns: snapshot_id, label, status, created_at, ttl_days, journal_id, image_size_mb
 */
public class ShowClusterSnapshotsCommand extends Command implements NoForward {

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    public ShowClusterSnapshotsCommand() {
        super(PlanType.SHOW_CLUSTER_SNAPSHOTS_COMMAND);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ShowResultSet result = doRun(ctx);
        executor.sendResultSet(result);
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        if (!Config.isCloudMode()) {
            throw new AnalysisException("SHOW CLUSTER SNAPSHOTS is only supported in cloud mode");
        }
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }
    }

    private ShowResultSet doRun(ConnectContext ctx) throws Exception {
        CloudSnapshotHandler handler = ((CloudEnv) ctx.getEnv()).getCloudSnapshotHandler();
        ListSnapshotResponse response = handler.listSnapshot(false);

        List<List<String>> rows = new ArrayList<>();
        for (SnapshotInfoPB info : response.getSnapshotsList()) {
            String createdAt = info.getCreateAt() > 0
                    ? TS_FMT.format(Instant.ofEpochSecond(info.getCreateAt()))
                    : "N/A";
            long ttlDays = info.getTtlSeconds() > 0 ? info.getTtlSeconds() / 86400 : 0;
            String imageSizeMb = info.getSnapshotMetaImageSize() > 0
                    ? String.format("%.1f", info.getSnapshotMetaImageSize() / 1024.0 / 1024.0)
                    : "N/A";

            rows.add(ImmutableList.of(
                    info.getSnapshotId(),
                    info.getSnapshotLabel(),
                    info.getStatus().name(),
                    createdAt,
                    String.valueOf(ttlDays),
                    String.valueOf(info.getJournalId()),
                    imageSizeMb,
                    info.getFdbReadVersion() > 0 ? String.valueOf(info.getFdbReadVersion()) : "N/A"
            ));
        }
        return new ShowResultSet(getMetaData(), rows);
    }

    private ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String col : Lists.newArrayList(
                "SnapshotId", "Label", "Status", "CreatedAt", "TtlDays", "JournalId",
                "ImageSizeMb", "FdbReadVersion")) {
            builder.addColumn(new Column(col, ScalarType.createVarchar(128)));
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowClusterSnapshotsCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.SHOW;
    }
}
