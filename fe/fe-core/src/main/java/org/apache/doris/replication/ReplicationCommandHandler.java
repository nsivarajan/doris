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

package org.apache.doris.replication;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigException;
import org.apache.doris.common.UserException;
import org.apache.doris.httpv2.rest.ReplicationAction;
import org.apache.doris.nereids.trees.plans.commands.info.AlterSystemOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplicationAddVaultMappingOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplicationCreateGroupOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplicationDrillModeOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplicationEnterDrModeOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplicationFailbackOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplicationFailoverOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplicationPauseExportOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplicationPromoteMasterOp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Business-logic handler for all ALTER SYSTEM REPLICATION commands.
 *
 * Dispatch flow:
 *   ALTER SYSTEM REPLICATION CREATE GROUP ...        → doCreateGroup
 *   ALTER SYSTEM REPLICATION PAUSE EXPORT         → doPauseExport
 *   ALTER SYSTEM REPLICATION PROMOTE MASTER       → doPromoteMaster
 *   ALTER SYSTEM REPLICATION ENTER DR MODE        → doEnterDrMode
 *   ALTER SYSTEM REPLICATION ENTER/EXIT DRILL MODE → doDrillMode
 *   ALTER SYSTEM REPLICATION FAILOVER TO SITE ... → doFailover
 *   ALTER SYSTEM REPLICATION FAILBACK TO SITE ... → doFailback
 *   ALTER SYSTEM REPLICATION ADD VAULT MAPPING .. → doAddVaultMapping
 */
public class ReplicationCommandHandler {
    private static final Logger LOG = LogManager.getLogger(ReplicationCommandHandler.class);

    private ReplicationCommandHandler() {
        // utility class — no instances
    }

    /**
     * Dispatches an ALTER SYSTEM REPLICATION op to the appropriate handler.
     *
     * @param op the validated AlterSystemOp produced by the parser
     * @throws UserException on business-logic or RPC errors
     */
    public static void handle(AlterSystemOp op) throws UserException {
        if (op instanceof ReplicationCreateGroupOp) {
            ReplicationCreateGroupOp cg = (ReplicationCreateGroupOp) op;
            doCreateGroup(cg.getGroupId(), cg.getPrimarySite(), cg.getSecondarySite(),
                    cg.getProperties());

        } else if (op instanceof ReplicationPauseExportOp) {
            ReplicationAction.doPauseExport();

        } else if (op instanceof ReplicationPromoteMasterOp) {
            ReplicationAction.doPromoteMaster();

        } else if (op instanceof ReplicationEnterDrModeOp) {
            ReplicationAction.doEnterDrMode();

        } else if (op instanceof ReplicationDrillModeOp) {
            ReplicationDrillModeOp drill = (ReplicationDrillModeOp) op;
            ReplicationAction.doDrillMode(drill.isEnter());

        } else if (op instanceof ReplicationFailoverOp) {
            ReplicationFailoverOp fo = (ReplicationFailoverOp) op;
            doFailover(fo.getTargetSite());

        } else if (op instanceof ReplicationFailbackOp) {
            ReplicationFailbackOp fb = (ReplicationFailbackOp) op;
            doFailback(fb.getTargetSite());

        } else if (op instanceof ReplicationAddVaultMappingOp) {
            ReplicationAddVaultMappingOp av = (ReplicationAddVaultMappingOp) op;
            doAddVaultMapping(av.getVaultName(), av.getSecondaryEndpoint(), av.getSecondaryBucket());

        } else {
            throw new UserException("Unknown replication op: " + op.getClass().getSimpleName());
        }
    }

    // ── CREATE GROUP ─────────────────────────────────────────────────────────

    /**
     * Configures a new replication group and persists it to BDB.
     * Sets mutable Config fields (enable_replication_group, group_id, bucket, endpoint,
     * credential_type, storage_type, site_name already in fe.conf).
     * Starts the EditLogS3Exporter if this site is the primary site.
     */
    private static void doCreateGroup(String groupId, String primarySite,
            String secondarySite, java.util.Map<String, String> props) throws UserException {
        LOG.info("[Replication] CREATE GROUP groupId={} primarySite={} secondarySite={}",
                groupId, primarySite, secondarySite);

        // apply Config fields from properties — wrap ConfigException as UserException
        try {
            org.apache.doris.common.ConfigBase.setMutableConfig("replication_group_id", groupId);
            if (props.containsKey("storage_type")) {
                org.apache.doris.common.ConfigBase.setMutableConfig(
                        "replication_storage_type", props.get("storage_type"));
            }
            if (props.containsKey("replication_bucket")) {
                org.apache.doris.common.ConfigBase.setMutableConfig(
                        "replication_bucket", props.get("replication_bucket"));
            }
            if (props.containsKey("replication_endpoint")) {
                org.apache.doris.common.ConfigBase.setMutableConfig(
                        "replication_endpoint", props.get("replication_endpoint"));
            }
            if (props.containsKey("credential_type")) {
                org.apache.doris.common.ConfigBase.setMutableConfig(
                        "replication_credential_type", props.get("credential_type"));
            }
            org.apache.doris.common.ConfigBase.setMutableConfig("enable_replication_group", "true");
        } catch (ConfigException e) {
            throw new UserException("Failed to apply replication group config: " + e.getMessage(), e);
        }

        // determine if this FE is the primary site
        String thisSite = Config.replication_site_name;
        boolean isPrimary = primarySite.equals(thisSite);

        // build and persist group state to BDB
        ReplicationGroupInfo info = new ReplicationGroupInfo();
        info.groupId = groupId;
        info.primarySite = primarySite;
        info.drReadOnly = !isPrimary;
        info.lastUpdatedMs = System.currentTimeMillis();
        Env.getCurrentEnv().getEditLog().logReplicationGroupInfo(info);
        Env.getCurrentEnv().replayReplicationGroupInfo(info);

        // start exporter only on the primary site
        if (isPrimary) {
            ReplicationAction.doPromoteMaster();
            LOG.info("[Replication] CREATE GROUP complete — this site ({}) is PRIMARY, exporter started",
                    thisSite);
        } else {
            Config.dr_read_only_mode = true;
            LOG.info("[Replication] CREATE GROUP complete — this site ({}) is SECONDARY (dr_read_only=true)",
                    thisSite);
        }
    }

    // ── FAILOVER ──────────────────────────────────────────────────────────────

    /**
     * Executes a replication FAILOVER to the target site.
     *
     * Steps:
     *   1. Pause export on this FE to prevent split-brain writes.
     *   2. Promote this FE to master (lifts dr_read_only_mode, starts exporter).
     *   3. Persist the updated ReplicationGroupInfo (primarySite, drReadOnly=false) to BDB.
     *
     * NOTE: FDB must be restored to a consistent point before promoting if using fdbbackup mode.
     *       Vault re-mapping is a separate step performed via ADD VAULT MAPPING before failover.
     */
    private static void doFailover(String targetSite) throws UserException {
        LOG.info("[Replication] FAILOVER initiated to site={}", targetSite);

        // Step 1 — stop the exporter on this node before we take writes
        ReplicationAction.doPauseExport();

        // Step 2 — promote: lift write guard and start exporter
        ReplicationAction.doPromoteMaster();

        // Step 3 — persist state to BDB so it survives FE restart
        ReplicationGroupInfo info = new ReplicationGroupInfo();
        info.groupId = Config.replication_group_id.isEmpty()
                ? "default" : Config.replication_group_id;
        info.primarySite = targetSite;
        info.drReadOnly = false;
        info.lastUpdatedMs = System.currentTimeMillis();
        Env.getCurrentEnv().getEditLog().logReplicationGroupInfo(info);

        LOG.info("[Replication] FAILOVER complete to site={}. "
                + "NOTE: FDB restore to consistent point must be performed before promoting "
                + "if using fdbbackup mode.", targetSite);
    }

    // ── FAILBACK ─────────────────────────────────────────────────────────────

    /**
     * Executes a replication FAILBACK to the target (original primary) site.
     *
     * Steps:
     *   1. Stop the exporter on this DR-turned-primary node.
     *   2. Enter DR mode (sets dr_read_only_mode = true).
     *   3. Persist the updated ReplicationGroupInfo (primarySite=targetSite, drReadOnly=true) to BDB.
     */
    private static void doFailback(String targetSite) throws UserException {
        LOG.info("[Replication] FAILBACK initiated — returning primary to site={}", targetSite);

        // Step 1+2 — stop exporter and re-enter DR mode
        ReplicationAction.doEnterDrMode();

        // Step 3 — persist state to BDB
        ReplicationGroupInfo info = new ReplicationGroupInfo();
        info.groupId = Config.replication_group_id.isEmpty()
                ? "default" : Config.replication_group_id;
        info.primarySite = targetSite;
        info.drReadOnly = true;
        info.lastUpdatedMs = System.currentTimeMillis();
        Env.getCurrentEnv().getEditLog().logReplicationGroupInfo(info);

        LOG.info("[Replication] FAILBACK complete. This FE is now in DR standby mode. "
                + "Primary site is now={}", targetSite);
    }

    // ── ADD VAULT MAPPING ─────────────────────────────────────────────────────

    /**
     * Pushes a vault endpoint/bucket override to the Meta Service and
     * persists the mapping in BDB ReplicationGroupInfo.
     *
     * @param vaultName vault identifier in the Meta Service
     * @param endpoint  secondary OSS/S3 endpoint URL
     * @param bucket    secondary bucket name
     */
    private static void doAddVaultMapping(String vaultName, String endpoint,
            String bucket) throws UserException {
        LOG.info("[Replication] pushing vault override: vault={} endpoint={} bucket={}",
                vaultName, endpoint, bucket);

        // Build and send the RPC request to Meta Service
        Cloud.ApplyVaultOverrideRequest req = Cloud.ApplyVaultOverrideRequest.newBuilder()
                .setVaultName(vaultName)
                .setEndpoint(endpoint)
                .setBucket(bucket)
                .build();

        Cloud.ApplyVaultOverrideResponse resp;
        try {
            resp = MetaServiceProxy.getInstance().applyVaultOverride(req);
        } catch (Exception e) {
            throw new UserException(
                    "[Replication] RPC applyVaultOverride failed: " + e.getMessage(), e);
        }

        if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new UserException(
                    "[Replication] applyVaultOverride rejected by Meta Service: "
                            + resp.getStatus().getMsg());
        }

        LOG.info("[Replication] vault override accepted by Meta Service: vault={}", vaultName);

        // Persist the vault override in BDB so it is replayed on FE restart
        ReplicationGroupInfo info = new ReplicationGroupInfo();
        info.groupId = Config.replication_group_id.isEmpty()
                ? "default" : Config.replication_group_id;
        info.primarySite = Config.replication_site_name.isEmpty()
                ? "unknown" : Config.replication_site_name;
        info.drReadOnly = Config.dr_read_only_mode;
        info.lastUpdatedMs = System.currentTimeMillis();
        info.vaultOverrides.put(vaultName,
                new ReplicationGroupInfo.VaultOverride(endpoint, bucket));
        Env.getCurrentEnv().getEditLog().logReplicationGroupInfo(info);

        LOG.info("[Replication] vault override persisted to BDB: vault={}", vaultName);
    }
}
