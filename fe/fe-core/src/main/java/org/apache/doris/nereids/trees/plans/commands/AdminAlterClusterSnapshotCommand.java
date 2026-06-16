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
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
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
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * ADMIN ALTER CLUSTER SNAPSHOT WHERE snapshot_id = '...' SET ('ttl' = 'N');
 *
 * Extends (or shortens) the TTL of a READY snapshot. The new ttl_seconds is measured
 * from the snapshot's original create_at time, not from the current time.
 *
 * WARNING: If the new TTL is less than (now - create_at), the snapshot will be
 * immediately eligible for recycling on the next recycler cycle. To safely extend,
 * compute: new_ttl = (now - create_at) + desired_additional_seconds.
 *
 * Example: snapshot created 1 day ago with 1.5-day TTL, extend by 2 more days:
 *   elapsed = 86400s, desired_total = 86400 + 172800 = 259200s
 *   ADMIN ALTER CLUSTER SNAPSHOT WHERE snapshot_id = '00000000abc1'
 *   SET ('ttl' = '259200');
 */
public class AdminAlterClusterSnapshotCommand extends Command implements ForwardWithSync {

    private static final String PROP_TTL = "ttl";
    private static final Logger LOG = LogManager.getLogger(AdminAlterClusterSnapshotCommand.class);

    private final String key;
    private final String snapshotId;
    private final long newTtlSeconds;

    /**
     * AdminAlterClusterSnapshotCommand
     */
    public AdminAlterClusterSnapshotCommand(String key, String snapshotId,
                                             Map<String, String> properties)
            throws AnalysisException {
        super(PlanType.ADMIN_ALTER_CLUSTER_SNAPSHOT_COMMAND);
        this.key = key;
        this.snapshotId = snapshotId;

        if (properties == null || !properties.containsKey(PROP_TTL)) {
            throw new AnalysisException("SET clause must specify 'ttl' property");
        }
        String ttlStr = properties.get(PROP_TTL);
        try {
            this.newTtlSeconds = Long.parseLong(ttlStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("'ttl' must be a positive integer (seconds): " + ttlStr);
        }
        if (this.newTtlSeconds <= 0) {
            throw new AnalysisException("'ttl' must be a positive integer (seconds): " + ttlStr);
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        Cloud.UpdateSnapshotRequest req = Cloud.UpdateSnapshotRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id)
                .setSnapshotId(snapshotId)
                .setTtlSeconds(newTtlSeconds)
                .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                .build();
        Cloud.UpdateSnapshotResponse resp;
        try {
            resp = MetaServiceProxy.getInstance().updateSnapshot(req);
        } catch (RpcException e) {
            throw new DdlException("update_snapshot RPC failed: " + e.getMessage());
        }
        if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new DdlException("update_snapshot failed: " + resp.getStatus().getMsg());
        }
        LOG.info("snapshot TTL updated: snapshot_id={} new_ttl_seconds={}", snapshotId, newTtlSeconds);
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
        if (key == null || !key.equalsIgnoreCase("snapshot_id")) {
            throw new AnalysisException(
                    "Where clause must be: snapshot_id = \"<id from SHOW CLUSTER SNAPSHOTS>\"");
        }
        if (snapshotId == null || snapshotId.isEmpty()) {
            throw new AnalysisException("snapshot_id cannot be empty");
        }
        long maxTtl = Config.cloud_snapshot_max_ttl_seconds;
        if (maxTtl > 0 && newTtlSeconds > maxTtl) {
            throw new AnalysisException("'ttl' " + newTtlSeconds + "s exceeds maximum of " + maxTtl
                    + "s — raise cloud_snapshot_max_ttl_seconds in fe.conf if needed");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminAlterClusterSnapshotCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }
}
