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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;

/**
 * ADMIN RESTORE CLUSTER SNAPSHOT WHERE snapshot_id = '00000000d8dcc1870000' FOR TABLE db.tbl [AS db.new_tbl];
 * If the table exists: creates targetTable with new tablet IDs, remaps FDB keys, shares S3 files (zero copy).
 * If the table was dropped: recreates schema from snapshot blob, replays BDB-JE, imports FDB data.
 */
public class AdminRestoreTableSnapshotCommand extends Command implements ForwardWithSync, SchemaChangingCommand {

    private final String snapshotId;
    private final String dbName;
    private final String tableName;
    private final List<String> partitionNames;
    private final String targetDbName;
    private final String targetTableName;

    /**
     * Constructor with explicit AS target.
     */
    public AdminRestoreTableSnapshotCommand(String snapshotId, String dbName, String tableName,
                                             List<String> partitionNames,
                                             String targetDbName, String targetTableName) {
        super(PlanType.ADMIN_RESTORE_TABLE_SNAPSHOT_COMMAND);
        this.snapshotId = snapshotId;
        this.dbName = dbName;
        this.tableName = tableName;
        this.partitionNames = partitionNames;
        this.targetDbName = targetDbName;
        this.targetTableName = targetTableName;
    }

    /**
     * Backward-compatible constructor (no AS clause).
     */
    public AdminRestoreTableSnapshotCommand(String snapshotId, String dbName, String tableName,
                                             List<String> partitionNames) {
        this(snapshotId, dbName, tableName, partitionNames, null, null);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        CloudEnv cloudEnv = (CloudEnv) Env.getCurrentEnv();
        String partitions = (partitionNames == null || partitionNames.isEmpty())
                ? null : String.join(",", partitionNames);
        cloudEnv.getCloudSnapshotHandler()
                .restoreTableFromSnapshot(snapshotId, dbName, tableName,
                        targetDbName, targetTableName, partitions);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (!Config.isCloudMode()) {
            throw new AnalysisException("ADMIN RESTORE TABLE SNAPSHOT is only supported in cloud mode");
        }
        if ("admin".equalsIgnoreCase(Config.cluster_snapshot_min_privilege)) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        PrivPredicate.ADMIN.getPrivs().toString());
            }
        } else {
            UserIdentity currentUser = ctx.getCurrentUserIdentity();
            if (currentUser == null || !currentUser.isRootUser()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "root privilege");
            }
        }
        if (snapshotId == null || snapshotId.isEmpty()) {
            throw new AnalysisException("snapshot_id cannot be empty");
        }
        if (dbName == null || dbName.isEmpty()) {
            throw new AnalysisException("database name cannot be empty");
        }
        if (tableName == null || tableName.isEmpty()) {
            throw new AnalysisException("table name cannot be empty");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminRestoreTableSnapshotCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }
}
