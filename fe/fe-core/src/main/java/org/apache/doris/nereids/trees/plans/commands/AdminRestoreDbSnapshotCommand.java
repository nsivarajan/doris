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

/**
 * ADMIN RESTORE CLUSTER SNAPSHOT WHERE snapshot_id = '...' FOR DATABASE source_db [AS target_db];
 * Restores all tables from a DB-level snapshot. Source DB need not exist — table list is
 * read from the snapshot's captured_tables metadata captured at creation time.
 */
public class AdminRestoreDbSnapshotCommand extends Command implements ForwardWithSync, SchemaChangingCommand {

    private final String snapshotId;
    private final String sourceDb;
    private final String targetDb;

    /**
     * AdminRestoreDbSnapshotCommand
     */
    public AdminRestoreDbSnapshotCommand(String snapshotId, String sourceDb, String targetDb) {
        super(PlanType.ADMIN_RESTORE_DB_SNAPSHOT_COMMAND);
        this.snapshotId = snapshotId;
        this.sourceDb = sourceDb;
        this.targetDb = (targetDb == null || targetDb.isEmpty()) ? sourceDb + "_restored" : targetDb;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        CloudEnv cloudEnv = (CloudEnv) Env.getCurrentEnv();
        cloudEnv.getCloudSnapshotHandler()
                .restoreDbFromSnapshot(snapshotId, sourceDb, targetDb);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (!Config.isCloudMode()) {
            throw new AnalysisException("ADMIN RESTORE DB SNAPSHOT is only supported in cloud mode");
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
        if (sourceDb == null || sourceDb.isEmpty()) {
            throw new AnalysisException("source database name cannot be empty");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminRestoreDbSnapshotCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }
}
