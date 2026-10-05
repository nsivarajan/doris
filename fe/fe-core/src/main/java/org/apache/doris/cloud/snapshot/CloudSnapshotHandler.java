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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.master.Checkpoint;
import org.apache.doris.persist.Storage;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

public class CloudSnapshotHandler extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(CloudSnapshotHandler.class);
    private static final String DEFAULT_HANDLER_CLASS = CloudSnapshotHandler.class.getName();
    private static volatile Env snapshotEnv;

    public CloudSnapshotHandler() {
        super("cloud snapshot handler", Config.cloud_snapshot_handler_interval_second * 1000);
    }

    public static CloudSnapshotHandler getInstance() {
        if (!DEFAULT_HANDLER_CLASS.equals(Config.cloud_snapshot_handler_class)) {
            return createByClassName(Config.cloud_snapshot_handler_class);
        }
        try {
            for (CloudSnapshotHandler handler : ServiceLoader.load(CloudSnapshotHandler.class)) {
                return handler;
            }
        } catch (ServiceConfigurationError e) {
            LOG.error("failed to create cloud snapshot handler from service loader", e);
            System.exit(-1);
            return null;
        }
        return new CloudSnapshotHandler();
    }

    public static Env getSnapshotEnv() {
        return snapshotEnv;
    }

    public static void setSnapshotEnv(Env env) {
        snapshotEnv = env;
    }

    @SuppressWarnings("unchecked")
    private static CloudSnapshotHandler createByClassName(String className) {
        try {
            Class<CloudSnapshotHandler> theClass = (Class<CloudSnapshotHandler>) Class.forName(className);
            Constructor<CloudSnapshotHandler> constructor = theClass.getDeclaredConstructor();
            return constructor.newInstance();
        } catch (Exception e) {
            LOG.error("failed to create cloud snapshot handler, class name: {}", className, e);
            System.exit(-1);
            return null;
        }
    }

    public void initialize() {
        // do nothing
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.cloud_auto_snapshot_enabled) {
            return;
        }
        try {
            refreshAutoSnapshotJob();
        } catch (Exception e) {
            LOG.warn("runAfterCatalogReady: auto-snapshot job failed", e);
        }
    }

    public void submitJob(long ttl, String label, String vaultName) throws Exception {
        Env env = Env.getCurrentEnv();

        // Step 1: Force a fresh BDBJE checkpoint that includes ALL pending journal entries.
        //
        // Root cause of silent no-op: Checkpoint.doCheckpoint() only replays up to
        // getFinalizedJournalId(), which returns the last BDBJE database name - 1.
        // A BDBJE "database" is created on each rollEditLog() call. Until a roll happens
        // (every edit_log_roll_num=50,000 edits, or on master change), dbNames.size() < 2
        // and getFinalizedJournalId() returns 0 → checkpoint writes nothing new.
        //
        // Fix: force a rollEditLog() immediately before doCheckpoint(). This creates a new
        // BDBJE database boundary, making all currently-written journal entries "finalized"
        // so the checkpoint captures the complete current FE catalog state including:
        //   - new tables created since last roll
        //   - schema changes (ALTER TABLE)
        //   - user/privilege changes
        //   - partition additions
        // Data inserted into existing tables is safe regardless (row visibility is FDB-driven).
        Checkpoint checkpointer = env.getCheckpointer();
        if (checkpointer == null) {
            throw new DdlException("Checkpointer not available — FE may not be master");
        }
        LOG.info("submitJob: rolling edit log to finalize all pending journal entries, label={}", label);
        env.getEditLog().rollEditLog();

        String imageDir = env.getImageDir();
        Storage storageBefore = new Storage(imageDir);
        long journalIdBefore = storageBefore.getLatestImageSeq();

        LOG.info("submitJob: forcing checkpoint for snapshot label={}, current_journal_id={}",
                label, journalIdBefore);
        checkpointer.doCheckpoint();

        Storage storage = new Storage(imageDir);
        long journalId = storage.getLatestImageSeq();
        if (journalId <= 0) {
            throw new DdlException("No image file found after checkpoint in " + imageDir);
        }
        if (journalId == journalIdBefore) {
            LOG.warn("submitJob: checkpoint did not advance journal_id (still {}). "
                    + "Snapshot will use existing image — check enable_checkpoint config.",
                    journalId);
        }
        File imageFile = storage.getCurrentImageFile();
        if (!imageFile.exists()) {
            throw new DdlException("Image file does not exist: " + imageFile.getAbsolutePath());
        }

        // Step 2: Get FDB committed_version at this moment.
        // This is the exact FDB timestamp to pass to `fdbrestore --version` when restoring
        // the FDB backup. It ensures FDB state matches the BDBJE image above.
        // Uses create_meta_sync_point which commits a lightweight FDB transaction and
        // returns the resulting committed_version — no data is written.
        long fdbReadVersion = 0;
        try {
            Cloud.CreateMetaSyncPointRequest syncReq = Cloud.CreateMetaSyncPointRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                    .build();
            Cloud.CreateMetaSyncPointResponse syncResp =
                    MetaServiceProxy.getInstance().createMetaSyncPoint(syncReq);
            if (syncResp.getStatus().getCode() == Cloud.MetaServiceCode.OK) {
                fdbReadVersion = syncResp.getCommittedVersion();
                LOG.info("submitJob: fdb_read_version={} for snapshot label={}", fdbReadVersion, label);
            } else {
                LOG.warn("submitJob: createMetaSyncPoint failed ({}), proceeding without fdb_read_version",
                        syncResp.getStatus().getMsg());
            }
        } catch (Exception e) {
            LOG.warn("submitJob: createMetaSyncPoint RPC failed, proceeding without fdb_read_version", e);
        }

        LOG.info("submitJob: image file ready: {}, journal_id={}, size={}",
                imageFile.getAbsolutePath(), journalId, imageFile.length());

        // Step 3: beginSnapshot → get snapshot_id, image_url, obj_info
        Cloud.BeginSnapshotRequest.Builder beginReq = Cloud.BeginSnapshotRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id)
                .setSnapshotLabel(label)
                .setTtlSeconds(ttl)
                .setAutoSnapshot(label != null && label.startsWith("auto_"))
                .setTimeoutSeconds(3600)
                .setRequestIp(FrontendOptions.getLocalHostAddressCached());
        if (vaultName != null && !vaultName.isEmpty()) {
            beginReq.setVaultName(vaultName);
        }

        Cloud.BeginSnapshotResponse beginResp;
        try {
            beginResp = MetaServiceProxy.getInstance().beginSnapshot(beginReq.build());
        } catch (RpcException e) {
            throw new DdlException("beginSnapshot RPC failed: " + e.getMessage());
        }
        if (beginResp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new DdlException("beginSnapshot failed: " + beginResp.getStatus().getMsg());
        }

        String snapshotId = beginResp.getSnapshotId();
        String imageUrl = beginResp.getImageUrl();
        Cloud.ObjectStoreInfoPB objInfo = beginResp.getObjInfo();

        LOG.info("submitJob: snapshot begun: snapshot_id={}, image_url={}", snapshotId, imageUrl);

        // Step 4: upload image — abort on failure
        try {
            uploadImageFile(imageFile, imageUrl, objInfo, snapshotId);
        } catch (Exception e) {
            LOG.warn("submitJob: upload failed, aborting snapshot_id={}", snapshotId, e);
            abortSnapshot(snapshotId, "image upload failed: " + e.getMessage());
            throw new DdlException("snapshot upload failed: " + e.getMessage());
        }

        // Step 5: commitSnapshot with journal_id and fdb_read_version
        Cloud.CommitSnapshotRequest.Builder commitReq = Cloud.CommitSnapshotRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id)
                .setSnapshotId(snapshotId)
                .setImageUrl(imageUrl)
                .setLastJournalId(journalId)
                .setSnapshotMetaImageSize(imageFile.length())
                .setRequestIp(FrontendOptions.getLocalHostAddressCached());
        if (fdbReadVersion > 0) {
            commitReq.setFdbReadVersion(fdbReadVersion);
        }
        Cloud.CommitSnapshotResponse commitResp;
        try {
            commitResp = MetaServiceProxy.getInstance().commitSnapshot(commitReq.build());
        } catch (RpcException e) {
            abortSnapshot(snapshotId, "commitSnapshot RPC failed: " + e.getMessage());
            throw new DdlException("commitSnapshot RPC failed: " + e.getMessage());
        }
        if (commitResp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            abortSnapshot(snapshotId, "commitSnapshot failed: " + commitResp.getStatus().getMsg());
            throw new DdlException("commitSnapshot failed: " + commitResp.getStatus().getMsg());
        }

        LOG.info("submitJob: snapshot committed: snapshot_id={}, label={}, journal_id={}, fdb_read_version={}",
                snapshotId, label, journalId, fdbReadVersion);
    }

    // Upload the image file to image_url using multipart upload.
    // The multipart callback records upload_id via updateSnapshot so a crashed upload
    // can be aborted by the recycler's PREPARING timeout handler.
    private void uploadImageFile(File imageFile, String imageUrl,
            Cloud.ObjectStoreInfoPB objInfo, String snapshotId) throws Exception {
        RemoteBase remote = RemoteBase.newInstance(new ObjectInfo(objInfo));

        // Extract the object key from image_url by stripping the bucket prefix.
        // image_url format: <vault_prefix>/snapshots/<snapshot_id>/image
        // The key passed to putObject/multipartUploadObject is relative to the vault prefix,
        // so we pass the full image_url path as-is since RemoteBase.normalizePrefix() handles it.
        String objectKey = imageUrl;

        remote.multipartUploadObject(imageFile, objectKey, uploadId -> {
            // Record upload_id in meta-service so recycler can abort it on crash
            try {
                Cloud.UpdateSnapshotRequest updateReq = Cloud.UpdateSnapshotRequest.newBuilder()
                        .setCloudUniqueId(Config.cloud_unique_id)
                        .setSnapshotId(snapshotId)
                        .setUploadFile(objectKey)
                        .setUploadId(uploadId)
                        .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                        .build();
                Cloud.UpdateSnapshotResponse updateResp =
                        MetaServiceProxy.getInstance().updateSnapshot(updateReq);
                if (updateResp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                    LOG.warn("updateSnapshot failed for snapshot_id={}: {}",
                            snapshotId, updateResp.getStatus().getMsg());
                    return Pair.of(false, "updateSnapshot failed: " + updateResp.getStatus().getMsg());
                }
                return Pair.of(true, null);
            } catch (Exception e) {
                LOG.warn("updateSnapshot RPC failed for snapshot_id={}", snapshotId, e);
                return Pair.of(false, "updateSnapshot RPC failed: " + e.getMessage());
            }
        });
    }

    // Best-effort abort — called on failure paths. Logs but does not throw.
    private void abortSnapshot(String snapshotId, String reason) {
        try {
            Cloud.AbortSnapshotRequest req = Cloud.AbortSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .setSnapshotId(snapshotId)
                    .setReason(reason)
                    .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                    .build();
            MetaServiceProxy.getInstance().abortSnapshot(req);
        } catch (Exception e) {
            LOG.warn("abortSnapshot failed for snapshot_id={}, reason={}", snapshotId, reason, e);
        }
    }

    public synchronized void refreshAutoSnapshotJob() throws Exception {
        // Auto-snapshot policy:
        //   TTL    = 7 days (604800s) — configurable via cloud_auto_snapshot_ttl_seconds
        //   Max    = 168 snapshots (7 days × 24 hourly) — pruned by dropping oldest READY
        //   Label  = auto_<yyyy-MM-dd_HH:mm>
        //
        // This method is called by the MasterDaemon tick (every cloud_snapshot_handler_interval_second).
        // It takes a new snapshot and then prunes any snapshots beyond the configured max.

        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }

        long ttlSeconds = Config.cloud_auto_snapshot_ttl_seconds;
        long maxSnapshots = Config.cloud_auto_snapshot_max_reversed_num;
        String label = "auto_" + java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm"));

        LOG.info("refreshAutoSnapshotJob: taking auto snapshot label={}, ttl={}s, max={}",
                label, ttlSeconds, maxSnapshots);
        try {
            submitJob(ttlSeconds, label, null);
        } catch (Exception e) {
            LOG.warn("refreshAutoSnapshotJob: snapshot failed for label={}", label, e);
            // Don't propagate — auto-snapshot failure should not affect the cluster
            return;
        }

        // Prune: drop oldest READY snapshots beyond maxSnapshots
        try {
            Cloud.ListSnapshotResponse listResp = listSnapshot(false);
            java.util.List<Cloud.SnapshotInfoPB> snapshots = new java.util.ArrayList<>(
                    listResp.getSnapshotsList());
            // Only prune auto-snapshots — leave manually created ones alone
            java.util.List<Cloud.SnapshotInfoPB> autoSnapshots = snapshots.stream()
                    .filter(s -> s.getAutoSnapshot()
                            && s.getStatus() == Cloud.SnapshotStatus.SNAPSHOT_READY)
                    .sorted(java.util.Comparator.comparingLong(Cloud.SnapshotInfoPB::getCreateAt))
                    .collect(java.util.stream.Collectors.toList());

            int excess = autoSnapshots.size() - (int) maxSnapshots;
            for (int i = 0; i < excess; i++) {
                String oldId = autoSnapshots.get(i).getSnapshotId();
                LOG.info("refreshAutoSnapshotJob: pruning old auto-snapshot snapshot_id={}", oldId);
                try {
                    MetaServiceProxy.getInstance().dropSnapshot(
                            Cloud.DropSnapshotRequest.newBuilder()
                                    .setCloudUniqueId(Config.cloud_unique_id)
                                    .setSnapshotId(oldId)
                                    .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                                    .build());
                } catch (Exception ex) {
                    LOG.warn("refreshAutoSnapshotJob: failed to drop old snapshot_id={}", oldId, ex);
                }
            }
        } catch (Exception e) {
            LOG.warn("refreshAutoSnapshotJob: pruning failed", e);
        }
    }

    /**
     * Download the BDBJE image for a snapshot (called at FE startup on recovery cluster).
     *
     * Design:
     *   - Recovery cluster has a NEW FDB (restored from backup at fdb_read_version)
     *   - New MS points at that restored FDB — it already has the SnapshotPB record
     *   - New FE boots with the same cloud_unique_id / instance_id as production
     *     (the restored FDB has that instance's records)
     *
     * This method simply reads the snapshot record from MS to get image_url + obj_info,
     * then downloads the BDBJE image file to imageDir. No instance creation, no clone.
     *
     * Before calling this, the operator must:
     *   1. Drop old production FE node registrations from restored FDB via:
     *      POST /MetaService/http/drop_node (using existing endpoint)
     *   2. Add the recovery FE's IP via:
     *      POST /MetaService/http/add_node
     * After which getLocalTypeFromMetaService() will find this FE and boot proceeds.
     */
    public void cloneSnapshot(String snapshotId) throws Exception {
        if (snapshotId == null || snapshotId.isEmpty()) {
            throw new DdlException("cloneSnapshot: snapshot_id must not be empty");
        }
        LOG.info("cloneSnapshot: downloading image for snapshot_id={}", snapshotId);

        // Get snapshot record from MS to obtain image_url and obj_info (vault credentials).
        Cloud.ListSnapshotRequest req = Cloud.ListSnapshotRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id)
                .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                .setRequiredSnapshotId(snapshotId)
                .setIncludeAborted(false)
                .build();
        Cloud.ListSnapshotResponse listResp;
        try {
            listResp = MetaServiceProxy.getInstance().listSnapshot(req);
        } catch (RpcException e) {
            throw new DdlException("listSnapshot RPC failed: " + e.getMessage());
        }
        if (listResp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new DdlException("listSnapshot failed: " + listResp.getStatus().getMsg());
        }
        if (listResp.getSnapshotsCount() == 0) {
            throw new DdlException("snapshot not found in MS: " + snapshotId
                    + ". Ensure the new MS is backed by the restored FDB that contains this snapshot.");
        }
        Cloud.SnapshotInfoPB info = listResp.getSnapshots(0);
        String imageUrl = info.getImageUrl();
        if (imageUrl == null || imageUrl.isEmpty()) {
            throw new DdlException("snapshot has no image_url (status="
                    + info.getStatus() + ") — was it fully committed?");
        }

        // Download the image file from OSS using vault credentials from the snapshot record.
        // The restored FDB's InstanceInfoPB has the same vault configs as production.
        // We use the instance's default vault credentials; the MS resolves them via RAM role.
        Env env = Env.getCurrentEnv();
        String imageDir = env.getImageDir();

        // Try to extract image filename from URL (format: .../snapshots/<id>/image.<journalId>)
        String localImageName = Storage.IMAGE + ".snapshot";
        if (imageUrl.contains("/")) {
            String urlFilename = imageUrl.substring(imageUrl.lastIndexOf('/') + 1);
            if (urlFilename.startsWith(Storage.IMAGE + ".")) {
                localImageName = urlFilename;
            }
        }
        String localImagePath = imageDir + File.separator + localImageName;

        // Get obj_info from the instance's default vault via MS.
        // We cannot rely on CloneInstanceResponse.obj_info since we no longer call clone_instance.
        // Instead use beginSnapshot/abortSnapshot in read-only fashion — or simply use the
        // instance's vault credentials directly.
        // For now: use list_snapshot which already returns image_url; obj_info must be fetched
        // from the instance record via getInstanceInfo. Use the same approach as uploadImageFile:
        // the snapshot's resource_id identifies the vault; MS has its credentials.
        // Simplified: call get_obj_store_info to get the vault ObjectStoreInfoPB.
        Cloud.ObjectStoreInfoPB objInfo = getVaultObjInfo(info.getResourceId());

        RemoteBase remote = RemoteBase.newInstance(new ObjectInfo(objInfo));
        LOG.info("cloneSnapshot: downloading image from {} to {}", imageUrl, localImagePath);
        remote.getObject(imageUrl, localImagePath);

        LOG.info("cloneSnapshot: image downloaded to {}. "
                + "FE boot sequence will call loadImage(imageDir) to load it. "
                + "fdb_read_version={} journal_id={}",
                localImagePath, info.getFdbReadVersion(), info.getJournalId());
    }

    // Fetch ObjectStoreInfoPB for the vault that holds the snapshot image.
    // Uses get_obj_store_info which returns vault credentials including RAM role ARN.
    private Cloud.ObjectStoreInfoPB getVaultObjInfo(String resourceId) throws DdlException {
        Cloud.GetObjStoreInfoRequest req = Cloud.GetObjStoreInfoRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id)
                .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                .build();
        try {
            Cloud.GetObjStoreInfoResponse resp = MetaServiceProxy.getInstance().getObjStoreInfo(req);
            if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                throw new DdlException("getObjStoreInfo failed: " + resp.getStatus().getMsg());
            }
            // Check modern storage_vault entries first (enable_storage_vault=true instances).
            // StorageVaultPB.obj_info() carries the ObjectStoreInfoPB with bucket/endpoint/credentials.
            for (Cloud.StorageVaultPB vault : resp.getStorageVaultList()) {
                if (vault.hasObjInfo()) {
                    if (resourceId == null || resourceId.isEmpty()
                            || resourceId.equals(vault.getId())) {
                        return vault.getObjInfo();
                    }
                }
            }
            // Fall back to legacy obj_info list (pre-storage-vault instances)
            for (Cloud.ObjectStoreInfoPB obj : resp.getObjInfoList()) {
                if (resourceId == null || resourceId.isEmpty() || resourceId.equals(obj.getId())) {
                    return obj;
                }
            }
            // Final fallback: return first available from either list
            if (!resp.getStorageVaultList().isEmpty()
                    && resp.getStorageVaultList().get(0).hasObjInfo()) {
                return resp.getStorageVaultList().get(0).getObjInfo();
            }
            if (!resp.getObjInfoList().isEmpty()) {
                return resp.getObjInfoList().get(0);
            }
            throw new DdlException("no object store found for resource_id=" + resourceId);
        } catch (RpcException e) {
            throw new DdlException("getObjStoreInfo RPC failed: " + e.getMessage());
        }
    }

    public Cloud.ListSnapshotResponse listSnapshot(boolean includeAborted) throws DdlException {
        try {
            Cloud.ListSnapshotRequest request = Cloud.ListSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                    .setIncludeAborted(includeAborted)
                    .build();
            Cloud.ListSnapshotResponse response = MetaServiceProxy.getInstance().listSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("listSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
            return response;
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }

    public void alterInstance(Cloud.AlterInstanceRequest request) throws DdlException {
        try {
            Cloud.AlterInstanceResponse response = MetaServiceProxy.getInstance().alterInstance(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("alterInstance response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }
}

