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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * Full-cluster rollback from a snapshot. Requires operator to restart FE after completion.
 *   ADMIN RESTORE CLUSTER SNAPSHOT WHERE snapshot_id = '00000000d8dcc1870000';
 */
public class AdminRestoreClusterSnapshotCommand extends Command implements ForwardWithSync {

    private static final String SNAPSHOT_ID_KEY = "snapshot_id";

    private final String key;
    private final String snapshotId;

    /**
     * AdminRestoreClusterSnapshotCommand
     */
    public AdminRestoreClusterSnapshotCommand(String key, String value) {
        super(PlanType.ADMIN_RESTORE_CLUSTER_SNAPSHOT_COMMAND);
        this.key = key;
        this.snapshotId = value;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        throw new DdlException("Full cluster restore via SQL is not yet implemented. "
                + "For DR, use fdbrestore with fdb_version from SHOW CLUSTER SNAPSHOTS FOR DR.");
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (!Config.isCloudMode()) {
            throw new AnalysisException("The sql is illegal in disk mode");
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
        if (key == null || !key.equalsIgnoreCase(SNAPSHOT_ID_KEY)) {
            throw new AnalysisException("Where clause must be: snapshot_id = \"<id from SHOW CLUSTER SNAPSHOTS>\"");
        }
        if (snapshotId == null || snapshotId.isEmpty()) {
            throw new AnalysisException("snapshot_id cannot be empty");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminRestoreClusterSnapshotCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }
}
