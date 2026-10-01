# Cluster Snapshot & Recovery — Implementation Plan

Branch: `bckrecover-build-4.1.4` (based on `build-4.1.4`)

## Background

Cloud mode Doris stores all tablet data in shared object storage (storage vault).
The FE holds catalog metadata (databases, tables, partitions, tablets) as an image file.
FDB (FoundationDB via TxnKV) holds live rowset metadata per tablet.

A reliable cluster snapshot requires three things to be consistent:
1. **FE image** — serialized catalog state at journal_id N
2. **Rowset data files** — protected from recycler deletion during the TTL window
3. **FDB rowset records** — copied to new cluster on restore so BEs can read tablets

The approach is deliberately non-MVCC. No versioned key migration. No FDB versionstamp.
Each phase is independently shippable and cherry-pickable to upstream.

---

## User Experience Summary

```sql
-- Take a snapshot (15-day retention)
ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES(
    'label'      = '2026-09-27_daily',
    'ttl'        = '1296000',
    'vault_name' = 'default_vault'
);

-- List snapshots
SHOW CLUSTER SNAPSHOTS;

-- Drop a snapshot
ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = 'snap_001a2b3c4d';

-- Restore: start a new FE with --cluster_snapshot_file snap_001a2b3c4d
-- New cluster boots READ-ONLY, full catalog as of snapshot time
-- Copy data back: INSERT INTO prod.t SELECT * FROM recovery.t
```

### What is recoverable

| Scenario | Recoverable | Gap |
|----------|-------------|-----|
| DROP TABLE (after last snapshot) | YES | Data since last snapshot |
| DROP PARTITION (after last snapshot) | YES | Data since last snapshot |
| TRUNCATE TABLE (after last snapshot) | YES | Data since last snapshot |
| Schema change (ADD/DROP/MODIFY COLUMN) | YES | Schema as of snapshot time |
| DROP TABLE before snapshot | NO | Not in that snapshot |
| Data written after last snapshot | NO | This is the gap |
| Restore specific table without new cluster | NO | Need recovery cluster |

---

## Architecture

```
ADMIN CREATE CLUSTER SNAPSHOT
        │
        ▼
FE: CloudSnapshotHandler.submitJob()
    1. Force FE checkpoint  → image.<journal_id>  (local disk)
    2. beginSnapshot(label, ttl, vault_name)       → snapshot_id, image_url, obj_info
    3. multipartUpload(image file → image_url)
    4. commitSnapshot(snapshot_id, image_url, journal_id, image_size)
    5. on failure: abortSnapshot(snapshot_id, reason)

Meta-service: SnapshotManager.begin_snapshot()
    Write SnapshotPB into FDB:
      key:   snapshot_key({instance_id, snapshot_id})
      value: {status=PREPARING, created_at, ttl_seconds, label, resource_id}

Meta-service: SnapshotManager.commit_snapshot()
    Update SnapshotPB: {status=READY, image_url, last_journal_id, image_size}

Recycler: recycle_rowsets() — modified
    At start of cycle: read live snapshots → oldest_live_snapshot_created_at
    Per rowset: if recycle_pb.creation_time >= oldest_live_snapshot_created_at → SKIP
    (rowset was alive at snapshot time, protect until snapshot expires)

Restore: FE starts with --cluster_snapshot_file <snapshot_id>
    CloudSnapshotHandler.cloneSnapshot()
    1. clone_instance(from_snapshot_id, new_instance_id) → image_url
    2. getObject(image_url, local image path)
    3. FE loads image normally, boots READ-ONLY
    4. Background: bulk-copy FDB rowset records for all tablets
       get_rowset(tablet_id, 0, visible_version) from source MS
       prepare_rowset + commit_rowset into new instance MS
```

---

## Phase Plan

### Phase 1 — FE Image Snapshot (Java only)
**Goal:** `ADMIN CREATE CLUSTER SNAPSHOT` works end to end.  
**Files:** `CloudSnapshotHandler.java`  
**Depends on:** Phase 2 (meta-service stubs must be real)  
**Risk:** Low — pure addition, no existing code changed  

### Phase 2 — Meta-service Snapshot Records (C++)
**Goal:** `begin_snapshot`, `commit_snapshot`, `abort_snapshot`, `drop_snapshot`, `list_snapshot` write/read real FDB records.  
**Files:** `snapshot_manager.cpp`  
**Risk:** Medium — FDB writes, needs transaction discipline  

### Phase 3 — Recycler Protection (C++)
**Goal:** Rowsets dropped/compacted after a snapshot are not deleted until snapshot expires.  
**Files:** `recycler.cpp`, `recycler_snapshot.cpp`  
**Risk:** Medium — modifies hot path in recycle_rowsets(); needs careful testing  

### Phase 4 — New Cluster Restore (Java + C++)
**Goal:** New cluster can boot from a snapshot and read all data.  
**Files:** `CloudSnapshotHandler.java`, `snapshot_manager.cpp`  
**Risk:** Medium-High — FDB bulk copy + new instance bootstrap  

### Phase 5 — Operator UX (Java)
**Goal:** `SHOW CLUSTER SNAPSHOTS` command.  
**Files:** Grammar, command class, handler  
**Risk:** Low  

---

## Detailed Checklist

### Phase 1 — FE Image Snapshot

#### `fe/fe-core/.../cloud/snapshot/CloudSnapshotHandler.java`

- [ ] **P1-1** Implement `submitJob(long ttl, String label, String vaultName)`
  - [ ] Trigger FE checkpoint: call `Env.getCurrentEnv().getCheckpointer().doCheckpoint()`
  - [ ] Read resulting image file path and `replayedJournalId` from `Storage`
  - [ ] Build `BeginSnapshotRequest` with label, ttl, vault_name, cloud_unique_id, request_ip
  - [ ] Call `MetaServiceProxy.getInstance().beginSnapshot(request)` → get snapshot_id, image_url, obj_info
  - [ ] Build `RemoteBase` from `obj_info` (use `RemoteBase.newInstance(ObjectStoreInfoPB)`)
  - [ ] Call `remote.multipartUploadObject(imageFile, imageKey, updateFn)` where updateFn calls `updateSnapshot` RPC with upload_file + upload_id (for multipart abort recovery)
  - [ ] Build `CommitSnapshotRequest` with snapshot_id, image_url, last_journal_id, image_size
  - [ ] Call `MetaServiceProxy.getInstance().commitSnapshot(request)`
  - [ ] On any exception: call `MetaServiceProxy.getInstance().abortSnapshot(snapshot_id, reason)`
  - [ ] Log snapshot_id, image_url, journal_id on success
  - [ ] Write snapshot_id to EditLog via `env.getEditLog().logBeginSnapshot(snapshotState)`

- [ ] **P1-2** Implement `refreshAutoSnapshotJob()` — reads `InstanceInfoPB.snapshot_interval_seconds` and schedules `submitJob()` accordingly (can be simple poll loop in `runAfterCatalogReady`)

- [ ] **P1-3** Add unit test for `submitJob()` with mock MetaServiceProxy

---

### Phase 2 — Meta-service Snapshot Records

#### `cloud/src/snapshot/snapshot_manager.cpp`

- [ ] **P2-1** Implement `begin_snapshot(instance_id, request, response)`
  - [ ] Validate: instance exists, snapshot_switch_status == ENABLED (or allow if not set)
  - [ ] Validate: ttl_seconds > 0, label not empty
  - [ ] Look up vault by vault_name from instance's storage vaults; get resource_id
  - [ ] Create FDB transaction
  - [ ] Write `SnapshotPB {status=PREPARING, label, ttl_seconds, created_at=now, resource_id, timeout_seconds}` to `snapshot_key({instance_id, snapshot_id})`
  - [ ] Snapshot_id = FDB versionstamp of the transaction (use `txn->get_versionstamp()`)
  - [ ] Populate response: snapshot_id (hex-encoded versionstamp), image_url (vault_prefix/snapshots/snapshot_id/image), obj_info (from vault)
  - [ ] Commit transaction
  - [ ] Handle timeout: background job marks PREPARING snapshots as ABORTED after timeout_seconds

- [ ] **P2-2** Implement `commit_snapshot(instance_id, request, response)`
  - [ ] Read existing SnapshotPB, verify status == PREPARING
  - [ ] Update: status=READY, image_url, last_journal_id, snapshot_meta_image_size, finish_at=now
  - [ ] Write back and commit

- [ ] **P2-3** Implement `abort_snapshot(instance_id, request, response)`
  - [ ] Read existing SnapshotPB, verify status == PREPARING
  - [ ] Update: status=ABORTED, reason
  - [ ] If upload_file set: abort the multipart upload via object store API
  - [ ] Write back and commit

- [ ] **P2-4** Implement `drop_snapshot(instance_id, request, response)`
  - [ ] Read SnapshotPB, verify status == READY
  - [ ] Update: status=RECYCLED (recycler picks it up)
  - [ ] Write back and commit

- [ ] **P2-5** Implement `list_snapshot(instance_id, request, response)`
  - [ ] Scan `snapshot_key` range for instance_id
  - [ ] Filter by include_aborted flag
  - [ ] Populate response.snapshots with SnapshotInfoPB list

- [ ] **P2-6** Implement `recycle_snapshots(InstanceRecycler*)` — called by recycler
  - [ ] Scan all snapshot keys for instance
  - [ ] For each READY snapshot: if `now > created_at + ttl_seconds` → mark RECYCLED
  - [ ] For each RECYCLED snapshot: delete image file from object store, delete snapshot key from FDB
  - [ ] For each PREPARING snapshot: if `now > created_at + timeout_seconds` → mark ABORTED

- [ ] **P2-7** Add tests for all snapshot state transitions

---

### Phase 3 — Recycler Protection

#### `cloud/src/recycler/recycler.cpp`

- [ ] **P3-1** Add `get_oldest_live_snapshot_time(instance_id)` helper
  - [ ] Scan all READY snapshot keys for instance
  - [ ] Return `min(s.created_at)` across all READY snapshots; return INT64_MAX if none

- [ ] **P3-2** Modify `recycle_rowsets()` at top of function
  - [ ] Call `get_oldest_live_snapshot_time` → `oldest_snapshot_time`
  - [ ] Pass it into the per-rowset loop

- [ ] **P3-3** Modify per-rowset deletion check in `recycle_rowsets()`
  - [ ] Before deleting rowset data files: check `rowset_recycle_pb.creation_time() >= oldest_snapshot_time`
  - [ ] If true: skip (rowset was alive at snapshot time, protect it)
  - [ ] Add counter: `num_protected_by_snapshot` for observability

- [ ] **P3-4** Same protection for `recycle_partitions()`, `recycle_indexes()`, `recycle_tablets()`
  - [ ] Apply same `creation_time >= oldest_snapshot_time` check
  - [ ] This protects against DROP PARTITION, DROP TABLE, schema change index drop

- [ ] **P3-5** Add bvar metrics: `snapshot_protected_rowsets`, `snapshot_protected_partitions`

- [ ] **P3-6** Add integration test: take snapshot → drop table → verify recycle skips → expire snapshot → verify recycle runs

---

### Phase 4 — New Cluster Restore

#### `fe/fe-core/.../cloud/snapshot/CloudSnapshotHandler.java`

- [ ] **P4-1** Implement `cloneSnapshot(String snapshotId)`
  - [ ] Call `MetaServiceProxy.getInstance().cloneInstance(CloneInstanceRequest)` → get image_url, obj_info
  - [ ] Build `RemoteBase` from obj_info
  - [ ] Download image file: `remote.getObject(imageKey, localImagePath)`
  - [ ] Place image file in `Env.getCurrentEnv().getImageDir()`
  - [ ] FE boots normally from image (existing `Env.loadImage()` handles it)

- [ ] **P4-2** Implement FDB rowset bulk copy (background after FE boot)
  - [ ] After catalog loaded: iterate all databases → tables → partitions → tablets
  - [ ] For each tablet: call source MS `get_rowset(tablet_id, 0, visible_version)` → rowset list
  - [ ] Call new instance MS `prepare_rowset` + `commit_rowset` for each rowset
  - [ ] Run as background thread, log progress; FE is usable (READ-ONLY) immediately
  - [ ] Track completion: set a flag `fdb_rowset_copy_done` in instance info when finished

- [ ] **P4-3** Enforce READ-ONLY on restored cluster
  - [ ] In `CloudEnv.init()`: if restored from snapshot, set `InstanceInfoPB.ready_only = true`
  - [ ] Block all write operations (INSERT/UPDATE/DELETE/DDL) with clear error: `"This cluster was restored from snapshot snap_XXX and is read-only. Use it to SELECT data and INSERT into production."`

#### `cloud/src/snapshot/snapshot_manager.cpp`

- [ ] **P4-4** Implement `clone_instance(request, response)`
  - [ ] Validate: from_snapshot_id exists and is READY
  - [ ] Validate: new_instance_id does not already exist
  - [ ] Create new InstanceInfoPB:
    - `instance_id = new_instance_id`
    - `source_instance_id = from_instance_id`
    - `source_snapshot_id = from_snapshot_id`
    - `ready_only = true`
    - Copy storage vault references from source instance (same vault, no data copy)
  - [ ] Write new instance to FDB
  - [ ] Return `image_url` from the snapshot record, `obj_info` from the vault

---

### Phase 5 — Operator UX

#### Grammar + Command

- [ ] **P5-1** Add `SHOW CLUSTER SNAPSHOTS` to `DorisParser.g4`
- [ ] **P5-2** Add `ShowClusterSnapshotsCommand` class
  - [ ] Call `CloudSnapshotHandler.listSnapshot(includeAborted=false)`
  - [ ] Format as result set: snapshot_id, label, status, created_at, ttl_days, image_size_mb, journal_id
- [ ] **P5-3** Wire into `CommandVisitor` and `StmtExecutor`

---

## Key Files Reference

| File | Language | Phase | Purpose |
|------|----------|-------|---------|
| `fe/fe-core/.../cloud/snapshot/CloudSnapshotHandler.java` | Java | P1, P4 | FE snapshot orchestrator |
| `cloud/src/snapshot/snapshot_manager.cpp` | C++ | P2, P4 | Meta-service snapshot state machine |
| `cloud/src/snapshot/snapshot_manager.h` | C++ | P2, P4 | Abstract interface (already exists) |
| `cloud/src/recycler/recycler.cpp` | C++ | P3 | Rowset deletion gating |
| `cloud/src/recycler/recycler_snapshot.cpp` | C++ | P3 | Recycler snapshot hooks |
| `fe/fe-core/.../cloud/rpc/MetaServiceProxy.java` | Java | P1 | RPC client (already wired) |
| `fe/fe-core/.../cloud/rpc/MetaServiceClient.java` | Java | P1 | RPC client (already wired) |
| `gensrc/proto/cloud.proto` | Proto | — | All snapshot messages (already defined) |
| `fe/fe-core/src/main/antlr4/.../DorisParser.g4` | ANTLR4 | P5 | SQL grammar |

## RPCs Already Wired in FE (no changes needed)

| RPC | Java method |
|-----|-------------|
| `begin_snapshot` | `MetaServiceProxy.beginSnapshot()` |
| `update_snapshot` | `MetaServiceProxy.updateSnapshot()` |
| `commit_snapshot` | `MetaServiceProxy.commitSnapshot()` |
| `abort_snapshot` | `MetaServiceProxy.abortSnapshot()` |
| `drop_snapshot` | `AdminDropClusterSnapshotCommand.dropSnapshot()` (already works) |
| `list_snapshot` | `CloudSnapshotHandler.listSnapshot()` (already works) |
| `clone_instance` | `MetaServiceProxy.cloneInstance()` |

## What We Explicitly Do NOT Build

- MVCC versioned key migration — not needed for this approach
- FDB versionstamp-based snapshot isolation — requires versioned key migration
- `AS OF TIMESTAMP` for OlapTable — separate feature, separate scope
- `compact_snapshot` / delta chain compression — optimization, not needed for correctness
- Per-rowset snapshot pin scanning — replaced by TTL-based creation_time check

## Restore Flow Summary

```
1. Identify snapshot:  SHOW CLUSTER SNAPSHOTS;
2. Provision new cluster with:
     fe.conf: cloud_unique_id = 1:<new_instance_id>:fe
3. Start FE:  ./fe.sh start --cluster_snapshot_file <snapshot_id>
4. FE downloads image, boots READ-ONLY
5. Background: FDB rowset records bulk-copied from source instance
6. Recover data: INSERT INTO prod.t SELECT * FROM recovery.t
7. Shut down recovery cluster
```

## Open Questions / Decisions Needed

- [ ] How frequently should auto-snapshots run? (proposed: configurable, default daily)
- [ ] Max number of snapshots to retain? (proposed: `max_reserved_snapshot` in InstanceInfoPB, default 15)
- [ ] Should restored cluster auto-shutdown after N hours to prevent accidental use?
- [ ] Should we support cross-vault restore (snapshot vault ≠ production vault)?
- [ ] Bulk FDB copy concurrency limit — how many tablets in parallel?
