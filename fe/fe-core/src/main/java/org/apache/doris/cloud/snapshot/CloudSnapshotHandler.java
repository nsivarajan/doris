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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.storage.ObjectInfo;
import org.apache.doris.cloud.storage.ObjectInfoAdapter;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CloudSnapshotHandler extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(CloudSnapshotHandler.class);

    public CloudSnapshotHandler() {
        super("cloud snapshot handler", Config.cloud_snapshot_handler_interval_second * 1000);
    }

    @SuppressWarnings("unchecked")
    public static CloudSnapshotHandler getInstance() {
        try {
            Class<CloudSnapshotHandler> theClass = (Class<CloudSnapshotHandler>) Class.forName(
                    Config.cloud_snapshot_handler_class);
            Constructor<CloudSnapshotHandler> constructor = theClass.getDeclaredConstructor();
            return constructor.newInstance();
        } catch (Exception e) {
            LOG.error("failed to create cloud snapshot handler, class name: {}", Config.cloud_snapshot_handler_class,
                    e);
            System.exit(-1);
            return null;
        }
    }

    public void initialize() {
        // do nothing
    }

    @Override
    protected void runAfterCatalogReady() {
        // do nothing
    }

    /**
     * Create a cluster snapshot with optional DB/table/partition-level granularity.
     * Quiesces DDL, flushes BDB-JE image, uploads to S3, and commits.
     *
     * @param forDbs        comma-separated DB names to protect (null = full cluster)
     * @param forTables     comma-separated "db.table" names to protect (null = full cluster)
     * @param forPartitions comma-separated "db.table.partition" names to protect (null = full cluster)
     */
    public void submitJob(long ttlSeconds, String label, String vaultName,
                          String forDbs, String forTables, String forPartitions) throws Exception {
        Env env = Env.getCurrentEnv();

        int granularityCount = (forDbs != null ? 1 : 0)
                + (forTables != null ? 1 : 0)
                + (forPartitions != null ? 1 : 0);
        if (granularityCount > 1) {
            throw new DdlException("FOR_DBS, FOR_TABLES, and FOR_PARTITIONS are mutually exclusive;"
                    + " specify at most one");
        }

        // Resolve granular filter names → internal IDs to pass to the meta-service.
        List<Long> includedDbIds        = resolveDbIds(forDbs);
        List<Long> includedTableIds     = resolveTableIds(forTables);
        List<Long> includedPartitionIds = resolvePartitionIds(forPartitions);

        final String[] snapshotId  = new String[1];
        final String[] imageUrl    = new String[1];
        final String[] localImagePath = new String[1];
        final long[]   journalId   = new long[1];
        final Cloud.ObjectStoreInfoPB[] objInfo = new Cloud.ObjectStoreInfoPB[1];
        @SuppressWarnings("unchecked")
        final Map<Long, ByteString>[] schemaJsons = new Map[1];
        @SuppressWarnings("unchecked")
        final List<Cloud.CapturedTableInfo>[] capturedTables = new List[1];

        // Quiesce DDL, call begin_snapshot, flush BDB-JE image.
        // If anything in the lambda throws, the snapshot is stuck in PREPARE — abort it.
        try {
            env.quiesceForSnapshot(() -> {
                Cloud.BeginSnapshotRequest.Builder beginBuilder =
                        Cloud.BeginSnapshotRequest.newBuilder()
                                .setCloudUniqueId(Config.cloud_unique_id)
                                .setSnapshotLabel(label != null ? label : "")
                                .setTtlSeconds(ttlSeconds)
                                .setTimeoutSeconds(Config.cloud_snapshot_timeout_seconds)
                                .setVaultName(vaultName != null ? vaultName : "")
                                .setAutoSnapshot(false)
                                .setRequestIp(FrontendOptions.getLocalHostAddressCached());

                includedDbIds.forEach(beginBuilder::addIncludedDbIds);
                includedTableIds.forEach(beginBuilder::addIncludedTableIds);
                includedPartitionIds.forEach(beginBuilder::addIncludedPartitionIds);

                Cloud.BeginSnapshotResponse beginResp =
                        MetaServiceProxy.getInstance().beginSnapshot(beginBuilder.build());

                // Retry on SNAPSHOT_PREPARE_MAYBE_COMMITTED: the begin commit may have succeeded;
                // abort_snapshot would destroy it. Retry lets the PREPARE dedup guard find it.
                if (beginResp.getStatus().getCode()
                        == Cloud.MetaServiceCode.SNAPSHOT_PREPARE_MAYBE_COMMITTED) {
                    LOG.warn("begin_snapshot SNAPSHOT_PREPARE_MAYBE_COMMITTED, retrying once");
                    beginResp = MetaServiceProxy.getInstance().beginSnapshot(beginBuilder.build());
                }
                checkResponse(beginResp.getStatus(), "begin_snapshot");

                snapshotId[0] = beginResp.getSnapshotId();
                imageUrl[0]   = beginResp.getImageUrl();  // keep relative — C++ owns FDB paths
                if (beginResp.hasObjInfo()) {
                    objInfo[0] = beginResp.getObjInfo();
                }

                localImagePath[0] = env.saveImage();
                journalId[0] = env.getReplayedJournalId();
                // Capture schemas inside quiesce so DDL is blocked — atomic with FDB snapshot.
                schemaJsons[0] = captureSchemaJsonsForCommit(forTables, forDbs);
                // Capture table name list for dropped-DB restore (no catalog needed on restore).
                capturedTables[0] = buildCapturedTablesList(forTables, forDbs);

                LOG.info("snapshot quiesce complete: snapshot_id={}, journal_id={}, "
                         + "included_dbs={}, included_tables={}, included_partitions={}",
                        snapshotId[0], journalId[0], includedDbIds.size(),
                        includedTableIds.size(), includedPartitionIds.size());
            });
        } catch (Exception e) {
            if (snapshotId[0] != null) {
                abortSnapshot(snapshotId[0], "quiesce phase failed: " + e.getMessage());
            }
            throw e instanceof DdlException ? (DdlException) e
                    : new DdlException("snapshot quiesce failed, snapshot aborted: "
                            + e.getMessage());
        }

        // Upload BDB-JE image to object storage.
        try {
            uploadImageFile(localImagePath[0], imageUrl[0], objInfo[0]);
        } catch (Exception e) {
            abortSnapshot(snapshotId[0], "image upload failed: " + e.getMessage());
            throw new DdlException("snapshot upload failed, snapshot aborted: " + e.getMessage());
        }

        // Export table schema JSON (non-fatal: failure does not abort the snapshot).
        // Skipped for DB-level/full-cluster snapshots — those embed schema in the commit blob.
        // The C++ recycler exports FDB data; this exports BDB-JE schema (DDL) to S3.
        if (objInfo[0] != null && (forTables != null || forPartitions != null)) {
            try {
                exportSchemaToS3(forTables, forPartitions, imageUrl[0], objInfo[0]);
            } catch (Exception e) {
                LOG.warn("snapshot schema export failed (non-fatal, same-cluster restore "
                         + "may have incomplete DDL): snapshot_id={}, error={}",
                        snapshotId[0], e.getMessage());
            }
        }

        // Commit snapshot — enters NORMAL state.
        Cloud.CommitSnapshotRequest.Builder commitBuilder =
                Cloud.CommitSnapshotRequest.newBuilder()
                        .setCloudUniqueId(Config.cloud_unique_id)
                        .setSnapshotId(snapshotId[0])
                        .setLastJournalId(journalId[0])
                        .setImageUrl(imageUrl[0])
                        .setSnapshotMetaImageSize(new File(localImagePath[0]).length())
                        .setRequestIp(FrontendOptions.getLocalHostAddressCached());
        if (schemaJsons[0] != null) {
            schemaJsons[0].forEach(commitBuilder::putTableSchemaJsons);
        }
        if (capturedTables[0] != null) {
            capturedTables[0].forEach(commitBuilder::addCapturedTables);
        }
        Cloud.CommitSnapshotRequest commitReq = commitBuilder.build();
        Cloud.CommitSnapshotResponse commitResp;
        try {
            commitResp = MetaServiceProxy.getInstance().commitSnapshot(commitReq);
        } catch (RpcException e) {
            abortSnapshot(snapshotId[0], "commit RPC failed: " + e.getMessage());
            throw new DdlException("commit_snapshot RPC failed, snapshot aborted: " + e.getMessage());
        }
        if (commitResp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            abortSnapshot(snapshotId[0], "commit failed: " + commitResp.getStatus().getMsg());
            throw new DdlException("commit_snapshot failed, snapshot aborted: "
                    + commitResp.getStatus().getMsg());
        }

        LOG.info("snapshot committed: snapshot_id={}, label={}, for_dbs={}, for_tables={}",
                snapshotId[0], label, forDbs, forTables);

        // Level 1 + 2 verification.
        verifySnapshot(snapshotId[0], objInfo[0]);
    }

    public void submitJob(long ttlSeconds, String label, String vaultName) throws Exception {
        submitJob(ttlSeconds, label, vaultName, null, null, null);
    }

    /**
     * Resolve comma-separated DB names → internal db_ids.
     * Returns empty list when forDbs is null (full-cluster snapshot).
     * Throws DdlException if any DB name cannot be resolved.
     */
    private List<Long> resolveDbIds(String forDbs) throws DdlException {
        if (forDbs == null || forDbs.trim().isEmpty()) {
            return new ArrayList<>();
        }
        List<Long> ids = new ArrayList<>();
        for (String dbName : forDbs.split(",")) {
            dbName = dbName.trim();
            if (dbName.isEmpty()) {
                continue;
            }
            try {
                Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbName);
                ids.add(db.getId());
            } catch (Exception e) {
                throw new DdlException("Cannot find database '" + dbName
                        + "' for snapshot granularity. Ensure the DB exists before creating "
                        + "a granular snapshot. Cause: " + e.getMessage());
            }
        }
        return ids;
    }

    /**
     * Resolve comma-separated "db.table" names → internal table_ids.
     * Returns empty list when forTables is null (full-cluster snapshot).
     * Throws DdlException if any table cannot be resolved or is not an OLAP table.
     */
    private List<Long> resolveTableIds(String forTables) throws DdlException {
        if (forTables == null || forTables.trim().isEmpty()) {
            return new ArrayList<>();
        }
        List<Long> ids = new ArrayList<>();
        for (String entry : forTables.split(",")) {
            entry = entry.trim();
            if (entry.isEmpty()) {
                continue;
            }
            String[] parts = entry.split("\\.", 2);
            if (parts.length != 2) {
                throw new DdlException("Invalid table reference '" + entry
                        + "': expected format 'database.table'");
            }
            try {
                Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(parts[0].trim());
                Table tableObj = db.getTableOrMetaException(parts[1].trim());
                if (!(tableObj instanceof OlapTable)) {
                    throw new DdlException("Table '" + entry + "' is type '"
                            + tableObj.getClass().getSimpleName()
                            + "' — only OLAP tables support granular snapshot rowset protection");
                }
                ids.add(tableObj.getId());
            } catch (DdlException de) {
                throw de; // re-throw as-is (already has a good message)
            } catch (Exception e) {
                throw new DdlException("Cannot resolve table '" + entry
                        + "' for snapshot granularity: " + e.getMessage());
            }
        }
        return ids;
    }

    private List<Long> resolvePartitionIds(String forPartitions) throws DdlException {
        if (forPartitions == null || forPartitions.trim().isEmpty()) {
            return new ArrayList<>();
        }
        List<Long> ids = new ArrayList<>();
        for (String entry : forPartitions.split(",")) {
            entry = entry.trim();
            if (entry.isEmpty()) {
                continue;
            }
            String[] parts = entry.split("\\.", 3);
            if (parts.length != 3) {
                throw new DdlException("Invalid partition reference '" + entry
                        + "': expected format 'database.table.partition'");
            }
            try {
                Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(parts[0].trim());
                Table tableObj = db.getTableOrMetaException(parts[1].trim());
                if (!(tableObj instanceof OlapTable)) {
                    throw new DdlException("Table '" + parts[0].trim() + "." + parts[1].trim()
                            + "' is type '" + tableObj.getClass().getSimpleName()
                            + "' — only OLAP tables support partition-level snapshot protection");
                }
                OlapTable olapTable = (OlapTable) tableObj;
                // Use getPartition(name, false) to exclude temp partitions — temp partitions
                // are transient (used during REPLACE PARTITION DDL) and must not be snapshotted.
                // Hold the table read lock while reading nameToPartition (non-concurrent TreeMap).
                Partition partition;
                olapTable.readLock();
                try {
                    partition = olapTable.getPartition(parts[2].trim(), false);
                } finally {
                    olapTable.readUnlock();
                }
                if (partition == null) {
                    throw new DdlException("Partition '" + parts[2].trim()
                            + "' not found in table '" + parts[0].trim() + "." + parts[1].trim()
                            + "'");
                }
                ids.add(partition.getId());
            } catch (DdlException de) {
                throw de;
            } catch (Exception e) {
                throw new DdlException("Cannot resolve partition '" + entry
                        + "' for snapshot granularity: " + e.getMessage());
            }
        }
        return ids;
    }

    /**
     * Level 1 structural verification — runs automatically after every backup.
     *
     * <p>Checks:
     * <ol>
     *   <li>Snapshot exists in FDB with status NORMAL (not stuck in PREPARE or ABORTED).</li>
     *   <li>image_url is non-empty and the image file is accessible in object storage.</li>
     *   <li>last_journal_id is positive (BDB-JE image was actually captured).</li>
     * </ol>
     *
     * <p>Non-fatal: a verification failure does not abort the snapshot — it was already
     * committed successfully. The operator is warned to investigate.
     */
    // package-private for unit testing
    void verifySnapshot(String snapshotId, Cloud.ObjectStoreInfoPB objInfoPB) {
        try {
            // Check 1: snapshot exists in FDB with status NORMAL
            Cloud.ListSnapshotRequest listReq = Cloud.ListSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .setRequiredSnapshotId(snapshotId)
                    .build();
            Cloud.ListSnapshotResponse listResp =
                    MetaServiceProxy.getInstance().listSnapshot(listReq);
            if (listResp.getStatus().getCode() != Cloud.MetaServiceCode.OK
                    || listResp.getSnapshotsCount() == 0) {
                LOG.warn("snapshot Level 1 verification FAILED: snapshot not found in FDB, "
                        + "snapshot_id={}", snapshotId);
                return;
            }
            Cloud.SnapshotInfoPB snap = listResp.getSnapshots(0);
            if (snap.getStatus() != Cloud.SnapshotStatus.SNAPSHOT_NORMAL) {
                LOG.warn("snapshot Level 1 verification FAILED: unexpected status={}, "
                        + "snapshot_id={}", snap.getStatus(), snapshotId);
                return;
            }

            // Check 2: image_url is non-empty and last_journal_id is positive
            if (snap.getImageUrl().isEmpty()) {
                LOG.warn("snapshot Level 1 verification FAILED: image_url is empty, "
                        + "snapshot_id={}", snapshotId);
                return;
            }
            if (snap.getJournalId() <= 0) {
                LOG.warn("snapshot Level 1 verification FAILED: journal_id={} not positive, "
                        + "snapshot_id={}", snap.getJournalId(), snapshotId);
                return;
            }

            // Check 3: image file is accessible in object storage and non-empty
            if (objInfoPB != null) {
                try {
                    ObjectInfo objInfo = new ObjectInfo(objInfoPB);
                    try (FileSystem fs = FileSystemFactory.getFileSystem(
                            ObjectInfoAdapter.toStorageProperties(objInfo))) {
                        org.apache.doris.filesystem.DorisInputFile inputFile =
                                fs.newInputFile(Location.of(toStorageUri(snap.getImageUrl(), objInfoPB)));
                        long imageSize = inputFile.length();
                        if (imageSize <= 0) {
                            LOG.warn("snapshot Level 1 verification FAILED: image file is empty, "
                                    + "snapshot_id={}, image_url={}",
                                    snapshotId, snap.getImageUrl());
                            return;
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("snapshot Level 1 verification FAILED: cannot access image file, "
                            + "snapshot_id={}, image_url={}, err={}",
                            snapshotId, snap.getImageUrl(), e.getMessage());
                    return;
                }
            }

            LOG.info("snapshot Level 1 verification PASSED: snapshot_id={}, "
                    + "image_url={}, journal_id={}, status=NORMAL",
                    snapshotId, snap.getImageUrl(), snap.getJournalId());

        } catch (Exception e) {
            LOG.warn("snapshot Level 1 verification encountered an error, snapshot_id={}: {}",
                    snapshotId, e.getMessage());
        }
    }

    /**
     * Export table/partition DDL schema to S3 as a JSON file alongside the FE image.
     * Uploads to {snapshot_dir}/schema_table_{table_id}.json for each protected table.
     * Enables same-cluster restore to recreate dropped tables or add back dropped partitions.
     *
     * JSON format:
     *   { "table_id": N, "db_name": "...", "table_name": "...",
     *     "create_table_ddl": "CREATE TABLE ...",
     *     "add_partition_ddls": ["ALTER TABLE ... ADD PARTITION ..."],
     *     "protected_partition_ids": [...] }
     */
    private void exportSchemaToS3(String forTables, String forPartitions,
                                   String imageUrl, Cloud.ObjectStoreInfoPB objInfoPB) throws Exception {
        // Derive snapshot directory from image_url (strip the fe_image filename).
        int slashPos = imageUrl.lastIndexOf('/');
        if (slashPos < 0) {
            throw new DdlException("malformed imageUrl (no slash): " + imageUrl);
        }
        String snapshotDir = imageUrl.substring(0, slashPos);

        // Build map: "db.table" → list of specific partition names (null = all partitions).
        Map<String, List<String>> tableToPartitions = new LinkedHashMap<>();
        if (forTables != null) {
            for (String entry : forTables.split(",")) {
                entry = entry.trim();
                if (!entry.isEmpty()) {
                    tableToPartitions.put(entry, null);
                }
            }
        }
        if (forPartitions != null) {
            for (String entry : forPartitions.split(",")) {
                entry = entry.trim();
                if (entry.isEmpty()) {
                    continue;
                }
                String[] parts = entry.split("\\.", 3);
                if (parts.length == 3) {
                    String tableKey = parts[0].trim() + "." + parts[1].trim();
                    tableToPartitions.computeIfAbsent(tableKey, k -> new ArrayList<>())
                            .add(parts[2].trim());
                }
            }
        }

        ObjectInfo objInfo = new ObjectInfo(objInfoPB);
        try (FileSystem fs = FileSystemFactory.getFileSystem(
                ObjectInfoAdapter.toStorageProperties(objInfo))) {
            for (Map.Entry<String, List<String>> tableEntry : tableToPartitions.entrySet()) {
                String[] nameParts = tableEntry.getKey().split("\\.", 2);
                String dbName = nameParts[0];
                String tableName = nameParts[1];
                List<String> protectedPartitionNames = tableEntry.getValue(); // null = all

                Database db;
                Table tableObj;
                try {
                    db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbName);
                    tableObj = db.getTableOrMetaException(tableName);
                } catch (Exception e) {
                    LOG.warn("exportSchemaToS3: cannot resolve {}.{}, skipping: {}",
                            dbName, tableName, e.getMessage());
                    continue;
                }
                if (!(tableObj instanceof OlapTable)) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) tableObj;
                long tableId = olapTable.getId();

                JsonObject schema = new JsonObject();
                schema.addProperty("table_id", tableId);
                schema.addProperty("db_name", dbName);
                schema.addProperty("table_name", tableName);

                List<String> createStmts = new ArrayList<>();
                List<String> addPartitionStmts = new ArrayList<>();

                olapTable.readLock();
                try {
                    // separatePartition=true: CREATE TABLE has no partition defs;
                    // addPartitionStmts has one ALTER TABLE ADD PARTITION per partition.
                    Env.getDdlStmt(tableObj, createStmts, addPartitionStmts, null,
                            true, false, -1L);
                } finally {
                    olapTable.readUnlock();
                }

                if (!createStmts.isEmpty()) {
                    schema.addProperty("create_table_ddl", createStmts.get(0));
                }

                // Filter add-partition stmts to only the protected partitions (if partition-level).
                // getDdlStmt formats each stmt as "... ADD PARTITION pName VALUES ..." (no backticks).
                JsonArray addPartArr = new JsonArray();
                JsonArray protectedIds = new JsonArray();
                for (String addStmt : addPartitionStmts) {
                    if (protectedPartitionNames == null) {
                        addPartArr.add(addStmt);
                    } else {
                        for (String pName : protectedPartitionNames) {
                            if (addStmt.contains(" " + pName + " ")) {
                                addPartArr.add(addStmt);
                                break;
                            }
                        }
                    }
                }

                // Collect protected partition IDs.
                olapTable.readLock();
                try {
                    List<String> pNames = protectedPartitionNames != null
                            ? protectedPartitionNames
                            : olapTable.getPartitions().stream()
                                    .map(Partition::getName)
                                    .collect(Collectors.toList());
                    for (String pName : pNames) {
                        Partition p = olapTable.getPartition(pName, false);
                        if (p != null) {
                            protectedIds.add(p.getId());
                        }
                    }
                } finally {
                    olapTable.readUnlock();
                }

                schema.add("add_partition_ddls", addPartArr);
                schema.add("protected_partition_ids", protectedIds);

                String jsonContent = schema.toString();
                String path = snapshotDir + "/schema_table_" + tableId + ".json";
                DorisOutputFile outputFile = fs.newOutputFile(Location.of(toStorageUri(path, objInfoPB)));
                try (OutputStream out = outputFile.create()) {
                    out.write(jsonContent.getBytes(StandardCharsets.UTF_8));
                }
                LOG.info("exportSchemaToS3: uploaded schema for table={}.{} id={} path={}",
                        dbName, tableName, tableId, path);
            }
        }
    }

    // Captures OlapTable.write() bytes for each table in scope (Doris binary serialization via DataOutputStream).
    // Sent in CommitSnapshotRequest so export_table_meta can embed them in blobs for dropped-table restore.
    private Map<Long, ByteString> captureSchemaJsonsForCommit(String forTables, String forDbs) {
        Map<Long, ByteString> result = new LinkedHashMap<>();
        // Tables specified directly ("db.table" comma-separated).
        if (forTables != null) {
            for (String entry : forTables.split(",")) {
                String[] parts = entry.trim().split("\\.", 2);
                if (parts.length == 2) {
                    appendSchemaJson(parts[0].trim(), parts[1].trim(), result);
                }
            }
        }
        // All tables in the specified DBs.
        if (forDbs != null) {
            for (String dbName : forDbs.split(",")) {
                dbName = dbName.trim();
                Database db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
                if (db == null) {
                    continue;
                }
                db.readLock();
                try {
                    for (Table t : db.getTables()) {
                        if (t instanceof OlapTable) {
                            appendSchemaJsonById((OlapTable) t, result);
                        }
                    }
                } finally {
                    db.readUnlock();
                }
            }
        }
        return result;
    }

    // Builds the CapturedTableInfo list — same scope as captureSchemaJsonsForCommit.
    // Must be called inside the quiesce window so table names are consistent with schemas.
    private List<Cloud.CapturedTableInfo> buildCapturedTablesList(String forTables, String forDbs) {
        List<Cloud.CapturedTableInfo> list = new ArrayList<>();
        if (forTables != null) {
            for (String entry : forTables.split(",")) {
                String[] parts = entry.trim().split("\\.", 2);
                if (parts.length == 2) {
                    addCapturedTable(parts[0].trim(), parts[1].trim(), list);
                }
            }
        }
        if (forDbs != null) {
            for (String dbName : forDbs.split(",")) {
                dbName = dbName.trim();
                Database db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
                if (db == null) {
                    continue;
                }
                db.readLock();
                try {
                    for (Table t : db.getTables()) {
                        if (t instanceof OlapTable) {
                            list.add(Cloud.CapturedTableInfo.newBuilder()
                                    .setTableId(t.getId())
                                    .setTableName(t.getName())
                                    .setDbName(dbName)
                                    .build());
                        }
                    }
                } finally {
                    db.readUnlock();
                }
            }
        }
        return list;
    }

    private void addCapturedTable(String dbName, String tableName, List<Cloud.CapturedTableInfo> out) {
        try {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
            if (db == null) {
                return;
            }
            Table t = db.getTableNullable(tableName);
            if (t instanceof OlapTable) {
                out.add(Cloud.CapturedTableInfo.newBuilder()
                        .setTableId(t.getId()).setTableName(tableName).setDbName(dbName).build());
            }
        } catch (Exception e) {
            LOG.warn("addCapturedTable: failed for {}.{}: {}", dbName, tableName, e.getMessage());
        }
    }

    /**
     * Restore all tables from a DB-level snapshot into a target DB.
     * Uses captured_tables from the snapshot so the source DB need not exist in the catalog.
     * Each table is restored via restoreTableFromSnapshot (Path A or Path B auto-selected).
     */
    public void restoreDbFromSnapshot(String snapshotId,
                                       String sourceDb, String targetDb) throws Exception {
        Cloud.SnapshotInfoPB snapshot = findSnapshotById(snapshotId);
        if (!snapshot.getTableMetaExported()) {
            throw new DdlException("snapshot '" + snapshotId + "' metadata export not complete.");
        }

        // Use captured_tables list (populated at snapshot creation inside the quiesce window).
        // This works even when the source DB has been dropped — no catalog access needed.
        List<Cloud.CapturedTableInfo> tables = snapshot.getCapturedTablesList().stream()
                .filter(ct -> sourceDb.equals(ct.getDbName()))
                .collect(Collectors.toList());

        if (tables.isEmpty()) {
            throw new DdlException("no tables found for DB '" + sourceDb + "' in snapshot '"
                    + snapshotId + "'. Ensure the snapshot was created with 'for_dbs=" + sourceDb + "'.");
        }

        // Ensure target DB exists.
        Database db = Env.getCurrentInternalCatalog().getDbNullable(targetDb);
        if (db == null) {
            Env.getCurrentInternalCatalog().createDb(targetDb, true, Collections.emptyMap());
        }

        List<String> failed = new ArrayList<>();
        for (Cloud.CapturedTableInfo ct : tables) {
            try {
                restoreTableFromSnapshot(snapshotId,
                        sourceDb, ct.getTableName(),
                        targetDb, ct.getTableName(),
                        null);
            } catch (Exception e) {
                LOG.warn("restoreDbFromSnapshot: failed for table={}: {}", ct.getTableName(),
                        e.getMessage());
                failed.add(ct.getTableName());
            }
        }

        if (!failed.isEmpty()) {
            throw new DdlException("restoreDbFromSnapshot: " + failed.size()
                    + " table(s) failed: " + failed + " — check logs for details.");
        }
        LOG.info("restoreDbFromSnapshot complete: snapshot={} {}->{} tables={}",
                snapshotId, sourceDb, targetDb, tables.size());
    }

    private void appendSchemaJson(String dbName, String tableName, Map<Long, ByteString> out) {
        try {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
            if (db == null) {
                return;
            }
            Table t = db.getTableNullable(tableName);
            if (t instanceof OlapTable) {
                appendSchemaJsonById((OlapTable) t, out);
            }
        } catch (Exception e) {
            LOG.warn("captureSchemaJson: failed for {}.{}: {}", dbName, tableName, e.getMessage());
        }
    }

    private void appendSchemaJsonById(OlapTable table, Map<Long, ByteString> out) {
        table.readLock();
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            try (DataOutputStream dos = new DataOutputStream(buf)) {
                table.write(dos);
            }
            out.put(table.getId(), ByteString.copyFrom(buf.toByteArray()));
        } catch (Exception e) {
            LOG.warn("captureSchemaJson: failed for table_id={}: {}", table.getId(), e.getMessage());
        } finally {
            table.readUnlock();
        }
    }

    /**
     * Restore a table from a cluster snapshot as a new table.
     *
     * If the source table still exists: creates targetTable with new tablet IDs, calls
     * import_table_meta with a tablet_id_remap. Both tables share S3 segment files —
     * zero data copy (rowset_id_v2 unchanged).
     *
     * If the source table was dropped: deserialises the OlapTable schema from the snapshot
     * blob, injects it into BDB-JE via replayCreateTable + logCreateTable (propagates to
     * all follower FEs), then calls import_table_meta with original tablet IDs.
     *
     * @param snapshotId       unique snapshot_id from SHOW CLUSTER SNAPSHOTS (hex versionstamp)
     * @param sourceDb         DB containing the table at snapshot time
     * @param sourceTable      table name at snapshot time
     * @param targetDb         DB to restore into; null → same as sourceDb
     * @param targetTableName  new table name; null → sourceTable + "_restored"
     * @param partitionNames   comma-separated partition names to restore; null = all
     */
    public void restoreTableFromSnapshot(String snapshotId,
                                          String sourceDb, String sourceTable,
                                          String targetDb, String targetTableName,
                                          String partitionNames) throws Exception {
        if (targetDb == null || targetDb.isEmpty()) {
            targetDb = sourceDb;
        }
        if (targetTableName == null || targetTableName.isEmpty()) {
            targetTableName = sourceTable + "_restored";
        }

        // Validate snapshot is ready.
        Cloud.SnapshotInfoPB snapshot = findSnapshotById(snapshotId);
        if (!snapshot.getTableMetaExported()) {
            throw new DdlException("snapshot '" + snapshotId + "' FDB metadata export not "
                    + "complete (table_meta_exported=false). Wait for the recycler cycle and retry.");
        }
        Cloud.ObjectStoreInfoPB objInfo = findVaultObjInfo(snapshot.getResourceId());

        // Download and parse the FDB metadata blob.
        // sourceDbObj may be null when the source DB has been dropped (dropped-DB restore path).
        Database sourceDbObj = Env.getCurrentInternalCatalog().getDbNullable(sourceDb);
        Table sourceTableObj = (sourceDbObj != null) ? sourceDbObj.getTableNullable(sourceTable) : null;
        if (sourceTableObj != null && !(sourceTableObj instanceof OlapTable)) {
            throw new DdlException("table '" + sourceDb + "." + sourceTable
                    + "' is not an OLAP table");
        }

        long tableId = (sourceTableObj != null)
                ? sourceTableObj.getId()
                : resolveTableIdFromSnapshot(snapshot, sourceDb, sourceTable, objInfo);

        String snapshotDir = snapshotDir(snapshot);
        String blobPath = snapshotDir + "/fdb_meta_table_" + tableId + ".pb";
        byte[] blobBytes = downloadBlobFromS3(blobPath, objInfo);
        Cloud.TableFdbMetaPB fdbMeta = Cloud.TableFdbMetaPB.parseFrom(blobBytes);

        LOG.info("restoreTableFromSnapshot: blob bytes={} snapshot_id={} {}.{} → {}.{}",
                blobBytes.length, snapshotId, sourceDb, sourceTable, targetDb, targetTableName);

        // Scope validation: skip DB-level check when source DB is dropped (can't get its ID).
        List<Long> protectedDbIds = snapshot.getProtectedDbIdsList();
        if (!protectedDbIds.isEmpty() && sourceDbObj != null
                && !protectedDbIds.contains(sourceDbObj.getId())) {
            throw new DdlException("DB '" + sourceDb + "' not in snapshot protected_db_ids");
        }
        List<Long> protectedTableIds = snapshot.getProtectedTableIdsList();
        if (!protectedTableIds.isEmpty() && !protectedTableIds.contains(tableId)) {
            throw new DdlException("Table '" + sourceDb + "." + sourceTable + "' (id=" + tableId
                    + ") not in snapshot protected_table_ids");
        }

        // Dispatch: source table exists → new table with remap; dropped → recreate from schema.
        Database targetDbObj = Env.getCurrentInternalCatalog().getDbOrMetaException(targetDb);
        if (targetDbObj.getTableNullable(targetTableName) != null) {
            throw new DdlException("target table '" + targetDb + "." + targetTableName
                    + "' already exists. Choose a different name via AS clause.");
        }

        if (sourceTableObj != null) {
            restoreAsNewTable(targetDbObj, targetTableName, (OlapTable) sourceTableObj,
                    fdbMeta, blobBytes, tableId, partitionNames);
        } else {
            restoreDroppedTable(targetDbObj, targetTableName, fdbMeta, blobBytes, tableId,
                    partitionNames);
        }
    }

    // ─── Restore: source table exists (new table with new IDs, shared S3 files) ──

    private void restoreAsNewTable(Database targetDb, String targetTableName,
                                    OlapTable sourceTable,
                                    Cloud.TableFdbMetaPB fdbMeta, byte[] blobBytes,
                                    long originalTableId,
                                    String partitionNames) throws Exception {
        // Clone with fresh IDs so FDB keys for the restored table are independent from the
        // source — otherwise import_table_meta would overwrite the source partition version.
        OlapTable newTable = cloneTableWithNewIds(sourceTable, targetTableName);

        // rollback=false until the table is in BDB-JE; set true after registration so any
        // subsequent failure (including a rare createTableWithLock throw after registerTable)
        // triggers cleanup via dropTableSilently.
        boolean rollback = false;
        try {
            // Register in BDB-JE and write EditLog (isReplay=false triggers the log write).
            Pair<Boolean, Boolean> reg = targetDb.createTableWithLock(newTable, false, false);
            if (!reg.first) {
                throw new DdlException("failed to register '" + targetTableName
                        + "' in catalog — table may have been created concurrently");
            }
            rollback = true;  // table is now registered — clean up on any subsequent failure

            // createTableWithLock skips TabletInvertedIndex; leader updates it only when
            // leaderCheckpointer fires (up to 60s). Populate immediately so DELETE/INSERT
            // on the restored table don't NPE while waiting for the checkpointer.
            Env.getCurrentInternalCatalog().populateTabletInvertedIndex(targetDb, newTable);

            // Build tablet ID and partition ID remaps: old → new.
            Map<Long, Long> tabletRemap = buildTabletIdRemap(sourceTable, newTable);
            Map<Long, Long> partitionRemap = buildPartitionIdRemap(sourceTable, newTable);

            // Use blob schema IDs to match snapshot-time FDB keys (live IDs may differ after repartition).
            OlapTable blobTable = null;
            if (partitionNames != null && !partitionNames.trim().isEmpty()
                    && fdbMeta.hasFeTableSchemaJson() && !fdbMeta.getFeTableSchemaJson().isEmpty()) {
                blobTable = deserializeOlapTable(fdbMeta.getFeTableSchemaJson().toByteArray());
            }
            List<Long> partIds = resolvePartitionFilter(sourceTable, blobTable, partitionNames);

            // Import snapshot FDB data. rowset_id_v2 is NOT remapped so both tables
            // share the same S3 segment files with zero data copy.
            callImportTableMeta(originalTableId, blobBytes, tabletRemap, partitionRemap, partIds,
                    newTable.getId());
            rollback = false;

            LOG.info("restoreAsNewTable complete: {}.{}", targetDb.getFullName(), targetTableName);
        } finally {
            if (rollback) {
                dropTableSilently(targetDb, newTable);
            }
        }
    }

    private Map<Long, Long> buildTabletIdRemap(OlapTable srcTable, OlapTable dstTable) {
        Map<Long, Long> remap = new LinkedHashMap<>();
        // Use dual strategy: name-based for RANGE/LIST (names stable across rename),
        // positional fallback for UNPARTITIONED (partition name tracks table name).
        List<Partition> srcParts = new ArrayList<>(srcTable.getPartitions());
        List<Partition> dstParts = new ArrayList<>(dstTable.getPartitions());
        Map<String, Partition> dstByName = dstParts.stream()
                .collect(Collectors.toMap(Partition::getName, p -> p, (a, b) -> a, LinkedHashMap::new));
        if (srcParts.size() != dstParts.size()) {
            LOG.warn("buildTabletIdRemap: partition count mismatch src={} dst={} — "
                    + "only {} partitions will be remapped; clone may be incomplete",
                    srcParts.size(), dstParts.size(), Math.min(srcParts.size(), dstParts.size()));
        }
        for (int pi = 0; pi < srcParts.size() && pi < dstParts.size(); pi++) {
            Partition srcPart = srcParts.get(pi);
            // Prefer name-based lookup; fall back to positional if names diverged (UNPARTITIONED).
            Partition dstPart = dstByName.getOrDefault(srcPart.getName(), dstParts.get(pi));
            Map<Long, MaterializedIndex> dstByIndexId = new LinkedHashMap<>();
            for (MaterializedIndex dstIdx : dstPart.getMaterializedIndices(IndexExtState.VISIBLE)) {
                dstByIndexId.put(dstIdx.getId(), dstIdx);
            }
            for (MaterializedIndex srcIdx : srcPart.getMaterializedIndices(IndexExtState.VISIBLE)) {
                MaterializedIndex dstIdx = dstByIndexId.get(srcIdx.getId());
                if (dstIdx == null) {
                    LOG.warn("buildTabletIdRemap: no matching dst index partition={} indexId={}",
                            srcPart.getName(), srcIdx.getId());
                    continue;
                }
                List<Long> srcTablets = srcIdx.getTabletIdsInOrder();
                List<Long> dstTablets = dstIdx.getTabletIdsInOrder();
                if (srcTablets.size() != dstTablets.size()) {
                    LOG.warn("buildTabletIdRemap: tablet count mismatch partition={} indexId={} src={} dst={}",
                            srcPart.getName(), srcIdx.getId(), srcTablets.size(), dstTablets.size());
                    continue;
                }
                for (int i = 0; i < srcTablets.size(); i++) {
                    remap.put(srcTablets.get(i), dstTablets.get(i));
                }
            }
        }
        return remap;
    }

    private Map<Long, Long> buildPartitionIdRemap(OlapTable srcTable, OlapTable dstTable) {
        Map<String, Long> dstByName = new HashMap<>();
        for (Partition p : dstTable.getPartitions()) {
            dstByName.put(p.getName(), p.getId());
        }
        Map<Long, Long> remap = new LinkedHashMap<>();
        for (Partition p : srcTable.getPartitions()) {
            Long dstId = dstByName.get(p.getName());
            if (dstId != null) {
                remap.put(p.getId(), dstId);
            }
        }
        return remap;
    }

    // ─── Restore: source table was dropped (recreate from snapshot schema) ────

    private void restoreDroppedTable(Database targetDb, String targetTableName,
                                      Cloud.TableFdbMetaPB fdbMeta, byte[] blobBytes,
                                      long originalTableId, String partitionNames) throws Exception {
        if (!fdbMeta.hasFeTableSchemaJson() || fdbMeta.getFeTableSchemaJson().isEmpty()) {
            throw new DdlException("blob for table_id=" + originalTableId
                    + " has no fe_table_schema_json — cannot restore dropped table. "
                    + "Re-create the snapshot after upgrading to a version that captures schema.");
        }

        // Deserialise original OlapTable (with original IDs) and rename.
        OlapTable restored = deserializeOlapTable(fdbMeta.getFeTableSchemaJson().toByteArray());
        restored.setName(targetTableName);

        // Check no live object shares these IDs (freed when table was dropped).
        if (targetDb.getTable(restored.getId()) != null) {
            throw new DdlException("table_id=" + restored.getId()
                    + " is still in use — cannot restore with original IDs. "
                    + "The table was re-created after the snapshot; use AS clause to restore under a new name.");
        }

        // BDB-JE first, FDB second. On FDB failure, catalog is rolled back so user can retry.

        List<Long> partIds = resolvePartitionFilter(null, restored, partitionNames);

        boolean catalogWritten = false;
        try {
            Env.getCurrentInternalCatalog().replayCreateTable(targetDb.getFullName(),
                    targetDb.getId(), restored);
            // replayCreateTable(isReplay=true) only updates in-memory state — no journal write.
            // Set catalogWritten before logCreateTable to trigger rollback on any exception.
            catalogWritten = true;

            Env.getCurrentEnv().getEditLog().logCreateTable(
                    new CreateTableInfo(targetDb.getFullName(), targetDb.getId(), restored));

            callImportTableMeta(originalTableId, blobBytes,
                    Collections.emptyMap(), Collections.emptyMap(), partIds, 0);
            LOG.info("restoreDroppedTable complete: {}.{}", targetDb.getFullName(), targetTableName);
        } catch (Exception e) {
            if (catalogWritten) {
                // FDB failed after catalog was written — roll back BDB-JE so the user can retry.
                try {
                    DropInfo di = new DropInfo(targetDb.getId(), restored.getId(),
                            targetTableName, false, true, 0L);
                    Env.getCurrentInternalCatalog().replayDropTable(targetDb, restored.getId(), true, null);
                    Env.getCurrentEnv().getEditLog().logDropTable(di);
                } catch (Exception rollbackEx) {
                    LOG.error("restoreDroppedTable: catalog rollback failed for {}.{}: {}",
                            targetDb.getFullName(), targetTableName, rollbackEx.getMessage());
                }
            }
            throw e;
        }
    }

    // ─── Restore helpers ─────────────────────────────────────────────────────

    private void callImportTableMeta(long tableId, byte[] blobBytes,
                                      Map<Long, Long> tabletIdRemap,
                                      Map<Long, Long> partitionIdRemap,
                                      List<Long> partitionIds,
                                      long targetTableId) throws DdlException {
        Cloud.ImportTableMetaRequest.Builder reqBuilder =
                Cloud.ImportTableMetaRequest.newBuilder()
                        .setCloudUniqueId(Config.cloud_unique_id)
                        .setTableId(tableId)
                        .setFdbMetaPb(ByteString.copyFrom(blobBytes))
                        .setRequestIp(FrontendOptions.getLocalHostAddressCached());
        tabletIdRemap.forEach(reqBuilder::putTabletIdRemap);
        partitionIdRemap.forEach(reqBuilder::putPartitionIdRemap);
        partitionIds.forEach(reqBuilder::addPartitionIds);
        if (targetTableId > 0) {
            reqBuilder.setTargetTableId(targetTableId);
        }

        Cloud.ImportTableMetaResponse resp;
        try {
            resp = MetaServiceProxy.getInstance().importTableMeta(reqBuilder.build());
        } catch (RpcException e) {
            throw new DdlException("import_table_meta RPC failed: " + e.getMessage());
        }
        if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new DdlException("import_table_meta failed: " + resp.getStatus().getMsg());
        }
        if (resp.getTabletsRestored() == 0 && resp.getPartitionsRestored() == 0) {
            // Treat empty restore as error to prevent silent success on wrong snapshot/partitions.
            throw new DdlException("import_table_meta restored 0 tablets/partitions for table_id="
                    + tableId + " — verify the snapshot covers this table.");
        }
        LOG.info("import_table_meta: tablets={} rowsets={} partitions={}",
                resp.getTabletsRestored(), resp.getRowsetsRestored(), resp.getPartitionsRestored());
        // TODO: add Prometheus counter/histogram metrics for restore operations (count, latency).
    }

    /** Finds the original table_id by (1) scanning captured_tables (O(1)), then (2) downloading
     *  each protected_table_ids blob until one matches the source table name (O(N)).
     *  captured_tables is populated for DB-level and table-level snapshots and avoids blob downloads. */
    private long resolveTableIdFromSnapshot(Cloud.SnapshotInfoPB snapshot,
                                             String sourceDb, String sourceTable,
                                             Cloud.ObjectStoreInfoPB objInfo) throws Exception {
        // Fast path: captured_tables is populated for DB- and table-level snapshots.
        for (Cloud.CapturedTableInfo ct : snapshot.getCapturedTablesList()) {
            if (sourceDb.equals(ct.getDbName()) && sourceTable.equals(ct.getTableName())) {
                return ct.getTableId();
            }
        }
        // Slow path: scan protected_table_ids blobs by downloading and checking the embedded schema.
        if (snapshot.getProtectedTableIdsList().isEmpty()) {
            throw new DdlException("table '" + sourceDb + "." + sourceTable
                    + "' not found in catalog, not in captured_tables, "
                    + "and snapshot has no protected_table_ids.");
        }
        String snapshotDir = snapshotDir(snapshot);
        for (long tid : snapshot.getProtectedTableIdsList()) {
            try {
                String path = snapshotDir + "/fdb_meta_table_" + tid + ".pb";
                byte[] blob = downloadBlobFromS3(path, objInfo);
                Cloud.TableFdbMetaPB meta = Cloud.TableFdbMetaPB.parseFrom(blob);
                if (!meta.hasFeTableSchemaJson() || meta.getFeTableSchemaJson().isEmpty()) {
                    continue;
                }
                OlapTable t = deserializeOlapTable(meta.getFeTableSchemaJson().toByteArray());
                // Note: t.getName() matches on table name only — DB name is not embedded in the
                // blob schema. This is safe for single-DB snapshots. For multi-DB snapshots on
                // old snapshots (no captured_tables), prefer using captured_tables (fast path above).
                if (sourceTable.equals(t.getName())) {
                    return tid;
                }
            } catch (Exception ignored) {
                // Not this table; continue scanning.
            }
        }
        throw new DdlException("table '" + sourceDb + "." + sourceTable
                + "' not found in snapshot '" + snapshot.getSnapshotLabel() + "'");
    }

    // Derives the snapshot directory path from image_url (strips the fe_image filename).
    private static String snapshotDir(Cloud.SnapshotInfoPB snapshot) throws DdlException {
        String imageUrl = snapshot.getImageUrl();
        int slashPos = imageUrl.lastIndexOf('/');
        if (slashPos < 0) {
            throw new DdlException("malformed image_url: " + imageUrl);
        }
        return imageUrl.substring(0, slashPos);
    }

    /** Clone source table with new name and fresh IDs allocated by Env.getNextId(). */
    private OlapTable cloneTableWithNewIds(OlapTable src, String newName) throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(buf)) {
            src.write(dos);
        }
        OlapTable clone;
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buf.toByteArray()))) {
            clone = OlapTable.read(dis);
        }
        clone.setName(newName);
        long newTableId = Env.getCurrentEnv().getNextId();
        clone.setId(newTableId);
        for (Partition partition : clone.getAllPartitions()) {
            // CloudPartition.tableId must match the restored table for getVisibleVersion()
            // to query the correct partition_version_key; wrong ID → VERSION_NOT_FOUND.
            if (partition instanceof CloudPartition) {
                CloudPartition cp = (CloudPartition) partition;
                cp.setTableId(newTableId);
                // src.write(dos) serialises the source partition's visibleVersion into the clone.
                // forceResetVisibleVersion() bypasses the monotonic guard so MS is queried fresh.
                cp.forceResetVisibleVersion();
                LOG.info("cloneTableWithNewIds: updated CloudPartition tableId={} partitionId={}",
                        newTableId, partition.getId());
            } else {
                LOG.warn("cloneTableWithNewIds: partition is NOT a CloudPartition, type={}",
                        partition.getClass().getSimpleName());
            }
            long oldPartitionId = partition.getId();
            partition.setIdForRestore(Env.getCurrentEnv().getNextId());
            clone.getPartitionInfo().renamePartitionId(oldPartitionId, partition.getId());
            clone.renamePartitionId(oldPartitionId, partition.getId());
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    tablet.setTabletId(Env.getCurrentEnv().getNextId());
                    // tableId/partitionId must match the restored table so BE::get_version uses
                    // the correct FDB key — wrong IDs return the live version, causing
                    // spec_version mismatch and empty scan results.
                    for (Replica replica : tablet.getReplicas()) {
                        if (replica instanceof CloudReplica) {
                            ((CloudReplica) replica).setTableId(newTableId);
                            ((CloudReplica) replica).setPartitionId(partition.getId());
                        }
                    }
                }
            }
        }
        return clone;
    }

    private OlapTable deserializeOlapTable(byte[] jsonBytes) throws Exception {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(jsonBytes))) {
            return OlapTable.read(dis);
        }
    }

    /**
     * Resolve partition names to IDs using blob schema (preferred) over live table,
     * since repartition after snapshot makes live IDs differ from FDB blob keys.
     */
    private List<Long> resolvePartitionFilter(OlapTable liveTable, OlapTable blobTable,
            String partitionNames) {
        if (partitionNames == null || partitionNames.trim().isEmpty()) {
            return Collections.emptyList();
        }
        OlapTable lookup = (blobTable != null) ? blobTable : liveTable;
        if (blobTable == null) {
            LOG.warn("resolvePartitionFilter: snapshot blob has no fe_table_schema_json — "
                    + "falling back to live table partition IDs. "
                    + "If any partition was dropped and recreated under the same name since the "
                    + "snapshot was taken, the restored partition IDs will not match the snapshot "
                    + "FDB keys and import_table_meta will return 0 tablets/partitions. "
                    + "Re-take the snapshot with a version that captures schema to avoid this.");
        }
        if (lookup == null) {
            LOG.warn("resolvePartitionFilter: both liveTable and blobTable are null — returning empty");
            return Collections.emptyList();
        }
        List<Long> ids = new ArrayList<>();
        for (String name : partitionNames.split(",")) {
            name = name.trim();
            if (name.isEmpty()) {
                continue;
            }
            Partition p = lookup.getPartition(name, false);
            if (p != null) {
                ids.add(p.getId());
            }
        }
        return ids;
    }

    private void dropTableSilently(Database db, OlapTable table) {
        try {
            DropInfo dropInfo = new DropInfo(db.getId(), table.getId(), table.getName(),
                    false, true, 0L);
            Env.getCurrentInternalCatalog().replayDropTable(db, table.getId(), true, null);
            Env.getCurrentEnv().getEditLog().logDropTable(dropInfo);
        } catch (Exception ex) {
            LOG.error("dropTableSilently: failed to roll back {}.{}: {}",
                    db.getFullName(), table.getName(), ex.getMessage());
        }
    }

    /**
     * Download a blob from S3 into memory. Suitable for fdb_meta.pb files (typically 1–200 MB).
     * Rejects blobs larger than 512 MB to avoid OOM on the FE JVM heap.
     * Uses the fe-filesystem abstraction — handles S3/OSS/COS/AZURE uniformly.
     */
    private static final int MAX_FDB_META_BLOB_BYTES = 512 * 1024 * 1024; // 512 MB

    private byte[] downloadBlobFromS3(String remotePath,
                                       Cloud.ObjectStoreInfoPB objInfoPB) throws Exception {
        if (objInfoPB == null) {
            throw new DdlException("HDFS vault not supported for FDB metadata restore");
        }
        ObjectInfo objInfo = new ObjectInfo(objInfoPB);
        try (FileSystem fs = FileSystemFactory.getFileSystem(
                ObjectInfoAdapter.toStorageProperties(objInfo))) {
            Location src = Location.of(toStorageUri(remotePath, objInfoPB));
            try (InputStream in = fs.newInputFile(src).newStream();
                    ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                byte[] buf = new byte[8 * 1024 * 1024];
                int n;
                int total = 0;
                while ((n = in.read(buf)) != -1) {
                    total += n;
                    if (total > MAX_FDB_META_BLOB_BYTES) {
                        throw new DdlException("fdb_meta blob at '" + remotePath + "' exceeds "
                                + (MAX_FDB_META_BLOB_BYTES / 1024 / 1024)
                                + " MB — table is too large for same-cluster restore via this path."
                                + " Contact the administrator to increase FE heap or use an "
                                + "alternative restore method.");
                    }
                    out.write(buf, 0, n);
                }
                return out.toByteArray();
            }
        }
    }

    // Looks up a snapshot by its unique snapshot_id (hex versionstamp from SHOW CLUSTER SNAPSHOTS).
    // Uses required_snapshot_id filter — O(1) meta-service lookup, not a linear scan.
    private Cloud.SnapshotInfoPB findSnapshotById(String snapshotId) throws Exception {
        Cloud.ListSnapshotRequest req = Cloud.ListSnapshotRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id)
                .setRequiredSnapshotId(snapshotId)
                .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                .build();
        Cloud.ListSnapshotResponse listResp;
        try {
            listResp = MetaServiceProxy.getInstance().listSnapshot(req);
        } catch (RpcException e) {
            throw new DdlException("list_snapshot RPC failed: " + e.getMessage());
        }
        if (listResp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new DdlException("list_snapshot failed: " + listResp.getStatus().getMsg());
        }
        return listResp.getSnapshotsList().stream()
                .filter(s -> snapshotId.equals(s.getSnapshotId())
                        && s.getStatus() == Cloud.SnapshotStatus.SNAPSHOT_NORMAL)
                .findFirst()
                .orElseThrow(() -> new DdlException(
                        "No NORMAL snapshot found with snapshot_id='" + snapshotId + "'"));
    }

    /**
     * Resolve a vault resource_id to its ObjectStoreInfoPB by searching the instance vault list.
     */
    private Cloud.ObjectStoreInfoPB findVaultObjInfo(String resourceId) throws Exception {
        Cloud.GetObjStoreInfoRequest vaultReq = Cloud.GetObjStoreInfoRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id)
                .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                .build();
        Cloud.GetObjStoreInfoResponse vaultResp;
        try {
            vaultResp = MetaServiceProxy.getInstance().getObjStoreInfo(vaultReq);
        } catch (RpcException e) {
            throw new DdlException("getObjStoreInfo RPC failed: " + e.getMessage());
        }
        if (vaultResp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new DdlException("getObjStoreInfo failed: " + vaultResp.getStatus().getMsg());
        }
        for (Cloud.StorageVaultPB vault : vaultResp.getStorageVaultList()) {
            if (resourceId.equals(vault.getId()) && vault.hasObjInfo()) {
                return vault.getObjInfo();
            }
        }
        throw new DdlException("Vault with resource_id='" + resourceId
                + "' not found or has no object store config");
    }

    /**
     * Upload the local BDB-JE image file to the object storage path returned by begin_snapshot.
     * Uses the fe-filesystem abstraction so S3/OSS/COS/AZURE/HDFS are all handled uniformly.
     */
    private void uploadImageFile(String localPath, String imageUrl,
                                  Cloud.ObjectStoreInfoPB objInfoPB) throws Exception {
        File localFile = new File(localPath);
        if (!localFile.exists()) {
            throw new DdlException("local image file not found: " + localPath);
        }

        if (objInfoPB == null) {
            // HDFS-backed vault: imageUrl is an HDFS path.
            // FE uses the HDFS vault config (already configured in the cluster).
            // StorageProperties resolved via vault name — use generic FileSystemFactory path.
            throw new DdlException("HDFS vault upload not yet implemented; obj_info was null");
        }

        ObjectInfo objInfo = new ObjectInfo(objInfoPB);
        try (FileSystem fs = FileSystemFactory.getFileSystem(ObjectInfoAdapter.toStorageProperties(objInfo))) {
            Location dest = Location.of(toStorageUri(imageUrl, objInfoPB));
            DorisOutputFile outputFile = fs.newOutputFile(dest);
            try (OutputStream out = outputFile.create();
                    FileInputStream in = new FileInputStream(localFile)) {
                byte[] buf = new byte[8 * 1024 * 1024]; // 8 MB buffer
                int n;
                while ((n = in.read(buf)) != -1) {
                    out.write(buf, 0, n);
                }
            }
        }
        LOG.info("uploaded image file: local={}, remote={}, size={}",
                localPath, imageUrl, localFile.length());
    }

    /**
     * Best-effort abort: marks the snapshot ABORTED so the recycler can clean up.
     * Logs on failure but does not throw — the caller already has an error to surface.
     */
    private void abortSnapshot(String snapshotId, String reason) {
        if (snapshotId == null) {
            return;
        }
        try {
            Cloud.AbortSnapshotRequest req = Cloud.AbortSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .setSnapshotId(snapshotId)
                    .setReason(reason)
                    .build();
            MetaServiceProxy.getInstance().abortSnapshot(req);
            LOG.info("aborted snapshot: snapshot_id={}, reason={}", snapshotId, reason);
        } catch (Exception e) {
            LOG.warn("failed to abort snapshot snapshot_id={}, reason={}: {}",
                    snapshotId, reason, e.getMessage());
        }
    }

    /**
     * Converts a bucket-relative path to a full storage URI using the correct scheme
     * for the vault's provider (oss://, cos://, obs://, s3://, etc.).
     * The meta-service returns image_url as a relative path with no scheme.
     * Passes through unchanged if path already contains "://".
     */
    private static String toStorageUri(String path, Cloud.ObjectStoreInfoPB objInfo) {
        if (path == null || path.isEmpty() || path.contains("://")) {
            return path;
        }
        if (objInfo == null || objInfo.getBucket().isEmpty()) {
            return path;
        }
        String scheme;
        switch (objInfo.getProvider()) {
            case OSS:
                scheme = "oss";
                break;
            case COS:
                scheme = "cos";
                break;
            case OBS:
                scheme = "obs";
                break;
            case AZURE:
                // Azure URIs use abfs://container@account/path — not supported here
                return path;
            default:
                // S3, GCP, BOS, TOS all use S3-compatible s3:// scheme
                scheme = "s3";
                break;
        }
        return scheme + "://" + objInfo.getBucket() + "/" + path;
    }

    private static void checkResponse(Cloud.MetaServiceResponseStatus status, String rpc)
            throws DdlException {
        if (status.getCode() != Cloud.MetaServiceCode.OK) {
            throw new DdlException(rpc + " failed: " + status.getMsg());
        }
    }

    public synchronized void refreshAutoSnapshotJob() throws Exception {
        throw new NotImplementedException("refreshAutoSnapshotJob is not implemented");
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
