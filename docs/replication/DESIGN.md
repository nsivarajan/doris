# Doris Cloud Replication Group — Architecture & Design

**Status:** Proposed  
**Scope:** Cloud/decoupled mode only  
**Goal:** Single-command DR failover and failback with near-zero data loss, storage-provider agnostic

---

## 1. Goals

1. **DR failover** — one command brings Shanghai cluster live in under 60 seconds
2. **DR failback** — one command returns Beijing as primary after recovery, no bulk data transfer
3. **RPO** — bounded by object storage CRR lag (typically 1–5 min); approaches seconds with real-time CRR
4. **Zero performance impact** on primary cluster under any DML load
5. **Storage agnostic** — works identically on AWS S3, Alibaba Cloud OSS, Google Cloud Storage
6. **Read workload on secondary** — Shanghai cluster serves SELECT queries during normal operation; writes are blocked at FE level
7. **Feature flag gated** — entire feature is dormant by default (`enable_replication_group = false`); zero impact on clusters that don't use it
8. **Minimal core change** — ~845 lines across 7 files in Doris core; safe to roll back by flipping one flag
9. **Stay local** — patch on top of any Doris release; upgrade cost ≤ 2 hours per release

---

## 2. Non-Goals

- Coupled (local storage) mode — cloud mode only
- Active-active — this is active-passive DR only
- Automatic failover or failback — human decision required to trigger both
- Sub-second RPO without real-time CRR — object storage is the bottleneck
- MS-level or BE-level write guards — FE is the single enforcement point for read-only

---

## 3. Secondary Cluster Design

The secondary (Shanghai) cluster is **not cold standby** — it serves real read
traffic during normal operations. Three machines, each running FE + FDB + MS.

```
3 machines in Shanghai, each running:
  FE  — in --dr-reader-mode, serves SELECT queries, rejects writes
  FDB — running cluster (empty until DR activation)
  MS  — running, reads tablet metadata from Beijing FDB (cross-region)
        applies vault mappings from replication-group.json for all N vaults
        routes Shanghai BE to Shanghai OSS buckets (local, fast)
  BE  — optional, started for read compute workload

During normal ops:
  Reads:  Shanghai FE → Shanghai BE
          Shanghai MS intercepts vault requests → applies secondary vault mapping
          → returns Shanghai endpoint/bucket for ALL vaults
          → Shanghai BE reads from Shanghai OSS (local, fast) for all vaults
          Shanghai MS reads other tablet metadata from Beijing FDB (~20-50ms overhead)
  Writes: rejected at FE level, user directed to Beijing

On DR activation:
  fdbbackup restore → Shanghai FDB populated
  MS config updated → points to local Shanghai FDB
  failover command remaps ALL vaults in FDB to Shanghai endpoints
  1 FE promoted to master, other 2 become normal followers
  Shanghai fully independent of Beijing
```

**Pre-requisites — cloud/infra team (one-time setup per vault):**

For EACH storage vault that exists in the primary cluster:
```
□ Create corresponding bucket in Shanghai region
□ Configure bidirectional CRR: beijing-bucket ↔ shanghai-bucket
□ Beijing ECS role: add policy for this Beijing bucket
□ Shanghai ECS role: add policy for this Shanghai bucket

When a NEW vault is added to Beijing in future:
□ Same steps above for the new bucket pair
□ Run: replication-manager add-vault-mapping --vault-name <name> ...
□ Run: replication-manager reload-config
```

**Why MS reads Beijing FDB for tablet metadata (not vault data):**
fdbbackup is point-in-time, not continuous replication. Shanghai FDB is empty
until a restore is run. MS reads non-vault tablet metadata from Beijing FDB
cross-region (~20-50ms overhead, negligible for analytics). Vault config is
handled separately via the in-memory mapping override.

---

## 3. Lifecycle Overview

```
State: NORMAL (Beijing active)
  Beijing: primary — EditLogS3Exporter running, writing to bucket
  Shanghai: passive — DR FE in --dr-reader-mode, reading from bucket

       HUMAN DECISION: disaster confirmed
                ↓
  ./replication-manager failover --to-site shanghai

State: DR ACTIVE (Shanghai active)
  Shanghai: primary — EditLogS3Exporter now running, writing to bucket
  Beijing:  recovering hardware...
            on recovery: auto-starts in --dr-reader-mode, reads bucket
            catches up on 2 days of Shanghai's changes automatically

       HUMAN DECISION: Beijing healthy, ready to restore
                ↓
  ./replication-manager failback --to-site beijing

State: NORMAL (Beijing active again)
  Beijing: primary — EditLogS3Exporter running
  Shanghai: passive — back to --dr-reader-mode

No automatic triggers. Human decides when to fail over and when to fail back.
Once triggered, each command executes all steps automatically.
```

---

## 4. Architecture Overview

```
Primary Region (Beijing)                  DR Region (Shanghai)
────────────────────────                  ─────────────────────────────

FE Master
  EditLogS3Exporter thread ─────────────► Replication Bucket
  (background thread inside FE)             /<group_id>/fe-editlog/segment_*.log
  CloudMetaSyncPoint export ───────────►    /<group_id>/checkpoint/latest.json

Meta Service
  FDB (managed DR or fdbbackup) ────────►  /<group_id>/fdb-backup/

Object Storage ──── Bidirectional CRR ──► DR Object Storage
  All rowset data files            ◄────    (both directions always active)

                                          DR FE (--dr-reader-mode)
                                            reads segment files from bucket
                                            applies to local BDB continuously

                                          DR Meta Service
                                            restored from FDB backup on activation

                                          Replication Manager (external CLI tool)
                                            monitors lag
                                            orchestrates failover and failback
```

**Three independent streams unified by one consistency checkpoint.**

**Bidirectional CRR is configured from Day 0** — both buckets stay in sync
regardless of which site is primary. This makes failback zero-copy: when Beijing
recovers, its OSS already has all data written during the DR period.

---

## 5. Storage Abstraction Layer

All replication I/O goes through a single interface. No provider-specific code
outside the implementation classes.

### 5.1 Interface

```java
// doris-replication/core/ReplicationStorageBackend.java

public interface ReplicationStorageBackend {

    /** Write bytes at key. Idempotent — overwrite if exists. */
    void put(String key, byte[] data) throws ReplicationStorageException;

    /** Read bytes at key. Returns null if key does not exist. */
    byte[] get(String key) throws ReplicationStorageException;

    /** List all keys with given prefix, lexicographically sorted. */
    List<String> list(String prefix) throws ReplicationStorageException;

    /** True if key exists. */
    boolean exists(String key) throws ReplicationStorageException;

    /** Delete key. No-op if key does not exist. */
    void delete(String key) throws ReplicationStorageException;
}
```

### 5.2 Credential Provider Abstraction

Hardcoding AK/SK in config files is a security anti-pattern. In cloud
deployments, credentials come from the instance role, STS AssumeRole, or
Workload Identity. The design uses a `ReplicationCredentialProvider` interface
so the storage backends never handle raw credentials directly.

```java
// doris-replication/credentials/ReplicationCredentialProvider.java

public interface ReplicationCredentialProvider {
    /** Return current credentials (may be cached; refreshes when near expiry). */
    ReplicationCredentials getCredentials();
}

public class ReplicationCredentials {
    public final String accessKey;
    public final String secretKey;
    public final String securityToken;  // null for long-term credentials
    public final Instant expiresAt;     // null for long-term credentials
}
```

Four implementations cover all real-world scenarios:

```
doris-replication/credentials/
  StaticCredentialProvider.java        AK/SK from config (dev/testing only)
  InstanceProfileCredentialProvider.java  EC2/ECS/ECS RAM role via metadata endpoint
  AssumeRoleCredentialProvider.java    STS AssumeRole via ARN (primary approach)
  WorkloadIdentityCredentialProvider.java  GCP / K8s OIDC token exchange
```

**`AssumeRoleCredentialProvider` (the standard production approach):**

```java
// Calls STS/RAM STS with role ARN, returns short-lived credentials
// Auto-refreshes 5 minutes before expiry
// Works for: AWS IAM AssumeRole, Alibaba Cloud RAM STS AssumeRole

public class AssumeRoleCredentialProvider implements ReplicationCredentialProvider {

    private final String roleArn;
    private final String roleSessionName;   // identifies the session in audit logs
    private final String externalId;        // optional, for cross-account roles
    private final StsClient stsClient;      // provider-specific STS client
    private volatile ReplicationCredentials cached;

    @Override
    public ReplicationCredentials getCredentials() {
        // refresh if expired or within 5-minute window
        if (cached == null || isNearExpiry(cached)) {
            cached = assumeRole();
        }
        return cached;
    }

    private ReplicationCredentials assumeRole() {
        // calls STS AssumeRole API → returns AccessKeyId, SecretAccessKey, SessionToken
        // identical pattern to the OSS STS AssumeRole work done in this project
    }
}
```

**`InstanceProfileCredentialProvider` (zero-config for managed environments):**

```java
// Reads credentials from cloud metadata endpoint — no config needed
// AWS:    http://169.254.169.254/latest/meta-data/iam/security-credentials/<role>
// Alibaba: http://100.100.100.200/latest/meta-data/ram/security-credentials/<role>
// GCP:    http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/

public class InstanceProfileCredentialProvider implements ReplicationCredentialProvider {
    // auto-refreshes before expiry, no config required beyond credential_type = instance_profile
}
```

**Config (no AK/SK needed in most deployments):**

```properties
# Production — instance RAM role (zero credential config)
replication_credential_type = instance_profile

# Production — STS AssumeRole with ARN
replication_credential_type = assume_role
replication_role_arn         = acs:ram::123456789:role/doris-replication-role
replication_role_session_name = doris-replication-beijing
replication_external_id      = optional-external-id

# Development / testing only
replication_credential_type = ak_sk
replication_access_key       = <AK>
replication_secret_key       = <SK>
```

**`ReplicationStorageFactory` selects the credential provider from config:**

```java
public static ReplicationStorageBackend create(ReplicationConfig config) {
    // credential provider is resolved first, injected into storage backend
    ReplicationCredentialProvider creds = buildCredentialProvider(config);
    switch (config.storageType) {
        case S3:  return new S3ReplicationStorage(config, creds);
        case OSS: return new OSSReplicationStorage(config, creds);
        case GCS: return new GCSReplicationStorage(config, creds);
    }
}

private static ReplicationCredentialProvider buildCredentialProvider(ReplicationConfig config) {
    switch (config.credentialType) {
        case INSTANCE_PROFILE: return new InstanceProfileCredentialProvider(config.storageType);
        case ASSUME_ROLE:      return new AssumeRoleCredentialProvider(config);
        case WORKLOAD_IDENTITY: return new WorkloadIdentityCredentialProvider(config);
        case AK_SK:            return new StaticCredentialProvider(config.accessKey, config.secretKey);
    }
}
```

### 5.3 Implementations

All replication code lives inside the existing Doris repo under
`fe/fe-core/src/main/java/org/apache/doris/replication/`. No new Maven module,
no new repo. The cloud SDKs (AWS S3, Alibaba OSS) are already in
`fe-core/pom.xml` for the existing backup/restore feature — replication reuses
them. Only GCS SDK needs adding if GCS is required.

```
fe/fe-core/src/main/java/org/apache/doris/
  replication/                              ← all new replication code
    credentials/
      ReplicationCredentialProvider.java    (interface)
      ReplicationCredentials.java           (value class)
      StaticCredentialProvider.java         (AK/SK — dev only)
      InstanceProfileCredentialProvider.java (EC2/ECS/RAM role)
      AssumeRoleCredentialProvider.java     (STS AssumeRole — primary)
      WorkloadIdentityCredentialProvider.java (GCP/K8s)
      CredentialRefreshScheduler.java       (background refresh)
    storage/
      ReplicationStorageBackend.java        (interface)
      ReplicationStorageException.java
      S3ReplicationStorage.java
      OSSReplicationStorage.java
      GCSReplicationStorage.java
      LocalReplicationStorage.java          (testing only)
      ReplicationStorageFactory.java
    EditLogS3Exporter.java                  (background thread in FE)
    ReplicationConfig.java
    CheckpointData.java
    JournalEntry.java
  journal/
    S3JournalCursor.java                    (DR FE reads from bucket)

tools/replication-manager/                 ← Python CLI tool
  replication_manager.py
  commands/  clients/

docs/replication/                          ← already exists
```

`./build.sh --fe` builds everything — no changes to Maven module structure.

### 5.3 Bucket Layout (identical on all providers)

```
<replication-bucket>/
  <group_id>/
    fe-editlog/
      segment_0000000001.log    (journal_id 1 → 500,  written by whoever is primary)
      segment_0000000501.log    (journal_id 501 → 1000)
      ...
      CURSOR                    ({"last_segment":"...","last_journal_id":N,"written_by":"beijing"})

    checkpoint/
      latest.json               (consistent point, overwritten every 30s)
      history/
        checkpoint_20260714_094530.json

    fdb-backup/                 (fdbbackup or managed provider DR)

    replication-group.json      (group config, vault mappings, current primary)
```

The `CURSOR` file records which site wrote the last segment. On failback Beijing
reads segments written by Shanghai and continues from where Shanghai left off.

---

## 6. Component 1 — EditLogS3Exporter (Core Change)

Background thread inside the FE Master process. Reads new journal entries from
local BDB and writes them to the replication bucket as segment files.
Lifecycle tied to FE Master. Enabled by config. Zero performance impact — DDL
only, never DML data.

**When primary changes** (failover or failback), the new primary's FE Master
starts `EditLogS3Exporter`. It reads the `CURSOR` file, resumes from
`last_journal_id`, and continues appending segments. The segment stream is
continuous — the DR FE on the other side just keeps reading forward.

```java
public class EditLogS3Exporter implements Runnable {

    private final Journal journal;
    private final ReplicationStorageBackend storage;
    private final String groupId;
    private final String siteName;            // "beijing" or "shanghai"
    private volatile long lastExportedJournalId = 0;

    @Override
    public void run() {
        recoverCursor();
        while (running) {
            try {
                exportBatch();
                writeCheckpoint();
            } catch (Exception e) {
                LOG.warn("EditLogS3Exporter export failed, retrying", e);
            }
            sleepSafely(Config.replication_export_interval_ms);
        }
    }

    private void exportBatch() throws Exception {
        JournalCursor cursor = journal.read(lastExportedJournalId + 1);
        List<JournalEntry> batch = readBatch(cursor);
        if (batch.isEmpty()) return;

        long firstId = batch.get(0).journalId;
        long lastId  = batch.get(batch.size() - 1).journalId;

        storage.put(segmentKey(groupId, firstId), serializeSegment(batch));
        storage.put(cursorKey(groupId), buildCursorJson(firstId, lastId, siteName));
        lastExportedJournalId = lastId;
    }

    private void writeCheckpoint() throws Exception {
        CloudMetaSyncPoint syncPoint = readLatestSyncPoint();
        if (syncPoint == null) return;

        CheckpointData cp = new CheckpointData();
        cp.groupId         = groupId;
        cp.feJournalId     = syncPoint.committedVersion;
        cp.fdbVersionstamp = syncPoint.versionStamp;
        cp.sampledAtMs     = System.currentTimeMillis();
        cp.ossSafeBeforeMs = cp.sampledAtMs - Config.replication_crr_max_lag_ms;
        cp.primarySite     = siteName;

        storage.put(groupId + "/checkpoint/latest.json", JSON.toBytes(cp));
        storage.put(groupId + "/checkpoint/history/checkpoint_"
                    + CHECKPOINT_FMT.format(Instant.now()) + ".json", JSON.toBytes(cp));
    }

    private void recoverCursor() {
        byte[] cursorBytes = storage.get(cursorKey(groupId));
        if (cursorBytes != null) {
            lastExportedJournalId = parseCursor(cursorBytes).lastJournalId;
            LOG.info("EditLogS3Exporter recovered cursor at journal_id={}", lastExportedJournalId);
        }
    }
}
```

**Startup in Env.java (+30 lines):**
```java
if (Config.replication_group_enabled && Env.getCurrentEnv().isMaster()) {
    ReplicationStorageBackend storage = ReplicationStorageFactory.create(ReplicationConfig.fromDorisConfig());
    editLogS3Exporter = new EditLogS3Exporter(journal, storage,
            Config.replication_group_id, Config.replication_site_name);
    new Thread(editLogS3Exporter, "edit-log-s3-exporter").start();
}
```

---

## 7. Component 2 — S3JournalCursor (Core Change)

Implements the existing `JournalCursor` interface. Used by DR FE to read
journal entries from the replication bucket instead of the local BDB network.

```java
public class S3JournalCursor implements JournalCursor {

    private final ReplicationStorageBackend storage;
    private final String groupId;
    private long nextJournalId;
    private final Queue<JournalEntry> buffer = new ArrayDeque<>();

    @Override
    public Pair<Long, JournalEntity> next() {
        if (buffer.isEmpty()) loadNextSegment();
        if (buffer.isEmpty()) return null;    // no new segments yet
        JournalEntry e = buffer.poll();
        nextJournalId = e.journalId + 1;
        return Pair.of(e.journalId, e.entity);
    }

    private void loadNextSegment() {
        // list segment files, find ones we haven't applied yet
        // deserialize and buffer entries with journalId >= nextJournalId
        // handles segments from both Beijing and Shanghai seamlessly
        // (the stream is continuous regardless of which site wrote it)
    }
}
```

**DR FE startup — DorisFE.java (+50 lines):**
```java
// --dr-reader-mode flag:
//   FE starts as non-electable (no BDB quorum participation)
//   uses S3JournalCursor instead of BDB JournalCursor
//   applies journal entries from bucket to local BDB continuously
//   does not accept DDL writes until promoted
//   checks replication-group.json on startup:
//     if this site is not current primary → enter DR reader mode automatically
//     (handles Beijing auto-recovery after disaster without manual intervention)
//   ALL behaviour gated on Config.enable_replication_group
```

---

## 8. Component 3 — Feature Flag Gate

The entire replication feature is controlled by a single master switch. When
`false` (default), no replication code runs and Doris behaves identically to an
unpatched release. This makes the feature safe to deploy incrementally and safe
to roll back by flipping one flag.

```java
// Config.java — master switch, default off
@ConfField
public static boolean enable_replication_group = false;

// Config.java — secondary cluster write guard, checked only when master switch is on
@ConfField
public static boolean dr_read_only_mode = false;
```

Every touch point in the core checks the master switch first:

```java
// Env.java — exporter thread
if (Config.enable_replication_group && isMaster()) {
    startEditLogS3Exporter();
}

// DorisFE.java — DR reader mode + auto-standby
if (Config.enable_replication_group) {
    processDrReaderModeFlag();
    checkAutoStandby();
}

// StmtExecutor.java — write guard (~line 797, before Command dispatch)
// rejects writes when this FE is in DR read-only mode
if (Config.enable_replication_group && Config.dr_read_only_mode
        && logicalPlan instanceof Command && isDrWriteCommand(logicalPlan)) {
    throw new UserException(
        "This cluster is in DR read-only mode. " +
        "Connect to the primary cluster for write operations.");
}

private boolean isDrWriteCommand(LogicalPlan plan) {
    return plan instanceof InsertIntoTableCommand
        || plan instanceof InsertOverwriteTableCommand
        || plan instanceof UpdateCommand
        || plan instanceof DeleteFromCommand
        || plan instanceof DeleteFromUsingCommand
        || plan instanceof CreateTableCommand
        || plan instanceof AlterTableCommand
        || plan instanceof DropTableCommand
        || plan instanceof TruncateTableCommand
        || plan instanceof MergeIntoCommand;
}
```

**Write guard behaviour:**

| Operation | Primary FE (Beijing) | DR FE (Shanghai) |
|---|---|---|
| `SELECT` | ✅ allowed | ✅ allowed |
| `SHOW / DESC / EXPLAIN` | ✅ allowed | ✅ allowed |
| `INSERT / UPDATE / DELETE` | ✅ allowed | ❌ rejected with message |
| `CREATE / ALTER / DROP` | ✅ allowed | ❌ rejected with message |
| `LOAD DATA / stream load` | ✅ allowed | ❌ rejected with message |

Error message points user to primary:
```
ERROR: This cluster is in DR read-only mode.
       Connect to the primary cluster for write operations.
```

**On failover** — `promote-master` endpoint lifts the guard:
```java
Config.dr_read_only_mode = false;       // immediately allows writes
Config.enable_replication_group = true; // stays true (still a replication group)
// then promotes to BDB master, starts exporter
```

**Deployment safety:**
```
Step 1: Deploy new build (enable_replication_group = false)
        → nothing changes, validate build is stable

Step 2: Turn on replication (enable_replication_group = true)
        → exporter starts on primary, DR FE starts reading

Step 3: Enable write guard on secondary (dr_read_only_mode = true)
        → Shanghai rejects writes, serves reads

Rollback at any step:
        → set enable_replication_group = false
        → Doris back to original behaviour instantly
```

---

## 9. Component 4 — Consistency Checkpoint

Written every 30 seconds by `EditLogS3Exporter`. Pairs `fe_journal_id` with
`fdb_versionstamp` using the existing `CloudMetaSyncPoint` that Doris already
records. The `oss_safe_before_ms` field accounts for CRR lag and defines the
actual safe restore point.

```json
{
  "group_id":           "bj_to_sh",
  "fe_journal_id":      10500,
  "fdb_versionstamp":   "0x0000018A4B3F2100",
  "sampled_at_ms":      1752463530000,
  "oss_safe_before_ms": 1752463230000,
  "primary_site":       "beijing",
  "created_at":         "2026-07-14T09:45:30Z"
}
```

`oss_safe_before_ms = sampled_at_ms - crr_max_lag_ms`

All operations before `oss_safe_before_ms` are guaranteed present in all three
stores on both sites. This is the RPO boundary.

---

## 9. Component 4 — FDB / Meta Service Replication

External to Doris. Two options depending on deployment:

**Option A — Cloud-managed FDB (preferred):** Provider handles cross-region DR
natively. Bidirectional. RTO and RPO both seconds.

**Option B — Self-managed FDB (fdbbackup continuous):**
```bash
# Runs as system service on each site
fdbbackup start \
  --dest-url   's3://<replication-bucket>/<group_id>/fdb-backup/' \
  --cluster-file /etc/foundationdb/fdb.cluster

# On failover/failback: restore to oss_safe_before_ms
fdbbackup restore \
  --dest-url   's3://<replication-bucket>/<group_id>/fdb-backup/' \
  --cluster-file /etc/foundationdb/<target-site>.cluster \
  --timestamp  <oss_safe_before_ms>
```

Meta Service is stateless. Start a new instance pointing at the restored FDB.

---

## 10. Component 5 — Object Storage (Bidirectional CRR, N Vaults)

A Doris cluster may have **any number of storage vaults**, each pointing to a
different bucket. Every vault must have a corresponding secondary mapping and
bidirectional CRR configured. There is no default — each vault is an explicit
entry in `replication-group.json`.

**Bidirectional CRR — configured per vault bucket at cluster creation:**

```
For each vault:
  Beijing bucket ──── CRR ────► Shanghai bucket
  Shanghai bucket ─── CRR ────► Beijing bucket

Both sites always hold all data for all vaults.
Failback requires no bulk copy regardless of how many vaults exist.
```

| Provider | Feature | Lag SLA |
|---|---|---|
| Alibaba Cloud OSS | Cross-Region Replication (CRR) | 1–5 min; seconds with RTC |
| AWS S3 | S3 Replication | 1–15 min; seconds with RTC |
| Google Cloud Storage | Object Replication | minutes |

---

## 11. Component 6 — Vault Mapping (N Vaults)

Each storage vault registered in the primary cluster needs a secondary mapping
that tells the MS which endpoint, bucket, and credentials to use when serving
that vault on the secondary site.

### 11.1 replication-group.json — vault_mappings array

```json
{
  "group_id": "bj_to_sh",
  "primary_site": "beijing",
  "vault_mappings": [
    {
      "vault_name": "primary_vault",
      "primary": {
        "endpoint":        "oss-cn-beijing-internal.aliyuncs.com",
        "bucket":          "doris-beijing-data",
        "credential_type": "instance_profile"
      },
      "secondary": {
        "endpoint":        "oss-cn-shanghai-internal.aliyuncs.com",
        "bucket":          "doris-shanghai-data",
        "credential_type": "instance_profile"
      }
    },
    {
      "vault_name": "analytics_vault",
      "primary": {
        "endpoint":        "oss-cn-beijing-internal.aliyuncs.com",
        "bucket":          "analytics-beijing",
        "credential_type": "instance_profile"
      },
      "secondary": {
        "endpoint":        "oss-cn-shanghai-internal.aliyuncs.com",
        "bucket":          "analytics-shanghai",
        "credential_type": "instance_profile"
      }
    }
  ]
}
```

Credential types per vault entry:

| `credential_type` | How credentials are resolved |
|---|---|
| `instance_profile` | BE uses ECS instance RAM role — no credential config needed |
| `assume_role` | BE calls STS AssumeRole with `role_arn` field |

### 11.2 MS Vault Override (secondary site reads)

When secondary MS handles a storage resource request, it looks up the vault
name in the mapping and overrides endpoint, bucket, and credentials before
returning to BE:

```cpp
// meta_service.cpp — get_obj_store_info handler

if (config::enable_replication_group && !config::is_primary_site) {

    auto* mapping = replication_config.find_vault_mapping(vault_pb.name());

    if (mapping != nullptr) {
        // known vault — apply secondary config
        vault_pb.set_endpoint(mapping->secondary.endpoint);
        vault_pb.set_bucket(mapping->secondary.bucket);
        applyCredentials(vault_pb, mapping->secondary);
        LOG(DEBUG) << "[Replication] vault=" << vault_pb.name()
                   << " remapped to secondary bucket=" << mapping->secondary.bucket;
    } else {
        // unknown vault — warn operator, fall back to primary (cross-region, slow)
        LOG(WARNING) << "[Replication] vault=" << vault_pb.name()
                     << " has no secondary mapping. "
                     << "Add it with: replication-manager add-vault-mapping. "
                     << "Falling back to primary endpoint (cross-region read).";
    }
}
```

MS loads `replication-group.json` once at startup into memory. Reloads on
`SIGHUP` or via `replication-manager reload-config`.

### 11.3 ECS Role — One Role Per Site Covers All Vaults

Use a single ECS instance role per site. The role policy covers all vault
buckets for that site. This avoids per-vault credential management.

```
Beijing ECS role: doris-beijing-role
  oss:GetObject, PutObject, DeleteObject, ListBucket
    on oss://doris-beijing-data/*       ← vault 1
    on oss://analytics-beijing/*        ← vault 2
    on oss://cold-beijing/*             ← vault 3
    on oss://replication-data/*         ← replication bucket

Shanghai ECS role: doris-shanghai-role
  oss:GetObject, PutObject, DeleteObject, ListBucket
    on oss://doris-shanghai-data/*      ← vault 1
    on oss://analytics-shanghai/*       ← vault 2
    on oss://cold-shanghai/*            ← vault 3
    on oss://replication-data/*         ← replication bucket
```

All vault_mappings use `credential_type = instance_profile`. BE running on
Shanghai ECS automatically uses `doris-shanghai-role` — all Shanghai buckets
are authorized. No per-vault credential setup.

**When a new vault is added:** update the ECS role policy to include the new
bucket, configure CRR, then run `replication-manager add-vault-mapping`.

### 11.4 Adding a Vault After Setup

```bash
# New vault created in Beijing after replication group is running
./replication-manager add-vault-mapping \
  --group-id    bj_to_sh \
  --vault-name  new_vault \
  --secondary-bucket   new-vault-shanghai \
  --secondary-endpoint oss-cn-shanghai-internal.aliyuncs.com \
  --secondary-credential instance_profile

# Updates replication-group.json in bucket
# MS reloads config via SIGHUP or reload-config command
# CRR for new bucket must be configured separately (pre-requisite)
```

Until the mapping is added, the MS logs a WARNING and falls back to the primary
endpoint. The warning identifies exactly which vault needs attention.

## 12. Failover (Beijing → Shanghai)

```bash
./replication-manager failover --group-id bj_to_sh --to-site shanghai
```

```python
def failover(config):
    # 1. verify DR FE is caught up to last checkpoint
    checkpoint = read_checkpoint(config.bucket)
    dr_cursor  = read_cursor(config.bucket)
    assert checkpoint.fe_journal_id - dr_cursor.last_journal_id < config.max_lag

    # 2. pause Beijing export (prevent split-brain; skip if Beijing unreachable)
    try: primary_fe_client.post("/api/replication/pause-export")
    except: pass

    # 3. wait for Shanghai FE to reach checkpoint (max 30s)
    wait_for_journal(config.secondary_fe, checkpoint.fe_journal_id, timeout=30)

    # 4. restore FDB to consistent point (fdbbackup mode only)
    if config.fdb_mode == "fdbbackup":
        restore_fdb(config.secondary_fdb, checkpoint.oss_safe_before_ms)
        # MS now points to local Shanghai FDB (config update + restart)
        reconfigure_ms(config.secondary_ms, fdb=config.secondary_fdb_cluster)

    # 5. remap ALL vaults: for each vault_mapping, apply secondary config to FDB
    #    This updates the vault endpoint+bucket in FDB so MS reads correct config
    #    after step 4 (MS now owns its local FDB, no longer cross-region)
    for mapping in config.vault_mappings:
        ms_client(config.secondary_ms).alter_obj_store_info(
            vault_name = mapping.vault_name,
            endpoint   = mapping.secondary.endpoint,
            bucket     = mapping.secondary.bucket
        )

    # 6. promote Shanghai FE to master
    secondary_fe_client.post("/api/replication/promote-master")

    # 7. update replication-group.json: primary_site = "shanghai"
    update_group_config(config.bucket, primary_site="shanghai")
```

**Step 5 updates every vault in FDB atomically** (one MS call per vault).
After this, Shanghai MS serves all vault configs from local FDB — no more
cross-region reads for metadata. BE reads from Shanghai OSS for all vaults.

## 13. Failback (Shanghai → Beijing)

```bash
./replication-manager failback --group-id bj_to_sh --to-site beijing
```

**How Beijing gets Shanghai's data during the DR period:**

```
During DR period (Shanghai primary):
  Shanghai FE exports EditLog → replication bucket (all DDL changes)
  For EACH Shanghai vault bucket → CRR → corresponding Beijing bucket (automatic)

Beijing recovers:
  Reads replication-group.json → sees Shanghai is primary
  Enters --dr-reader-mode automatically (no human action)
  Reads Shanghai's EditLog segments from bucket → catches up in minutes
  ALL Beijing vault buckets already have all data (bidirectional CRR running)

Human runs failback when Beijing is confirmed healthy.
```

```python
def failback(config):
    # 1. verify Beijing FE caught up to Shanghai's checkpoint
    checkpoint = read_checkpoint(config.bucket)
    bj_cursor  = read_cursor(config.bucket, site="beijing")
    assert checkpoint.fe_journal_id - bj_cursor.last_journal_id < config.max_lag

    # 2. verify ALL vault CRR lags are within threshold
    #    each vault bucket in Beijing must have received all Shanghai writes
    for mapping in config.vault_mappings:
        crr_lag = get_crr_lag(mapping.secondary.bucket, mapping.primary.bucket)
        assert crr_lag < config.crr_max_lag_ms, \
            f"vault {mapping.vault_name} CRR lag {crr_lag}ms exceeds threshold"

    # 3. pause Shanghai export (prevent split-brain)
    secondary_fe_client.post("/api/replication/pause-export")

    # 4. wait for Beijing FE to reach Shanghai's final checkpoint (max 30s)
    wait_for_journal(config.primary_fe, checkpoint.fe_journal_id, timeout=30)

    # 5. restore Beijing FDB (fdbbackup mode only)
    if config.fdb_mode == "fdbbackup":
        restore_fdb(config.primary_fdb, checkpoint.oss_safe_before_ms)
        reconfigure_ms(config.primary_ms, fdb=config.primary_fdb_cluster)

    # 6. remap ALL vaults back: apply primary config to Beijing FDB
    for mapping in config.vault_mappings:
        ms_client(config.primary_ms).alter_obj_store_info(
            vault_name = mapping.vault_name,
            endpoint   = mapping.primary.endpoint,
            bucket     = mapping.primary.bucket
        )

    # 7. promote Beijing FE to master
    primary_fe_client.post("/api/replication/promote-master")

    # 8. Shanghai FE enters --dr-reader-mode
    secondary_fe_client.post("/api/replication/enter-dr-mode")

    # 9. update replication-group.json: primary_site = "beijing"
    update_group_config(config.bucket, primary_site="beijing")
```

**Step 2 checks CRR lag per vault** — every vault bucket must be in sync
before failback proceeds. If any vault is lagging, failback waits or fails
fast with a message telling the operator which vault to wait for.

**No bulk data transfer.** Beijing OSS already has everything via bidirectional
CRR. Failback time is dominated by FDB restore (if fdbbackup mode) or just
seconds if cloud-managed.

---

## 13. Beijing Auto-Recovery After Disaster

When Beijing hardware recovers, no manual action is needed to enter standby:

```java
// In DorisFE.java startup (new logic, ~20 lines):

ReplicationGroupConfig groupConfig = readGroupConfig(storage);
if (groupConfig != null && !groupConfig.primarySite.equals(Config.replication_site_name)) {
    // This site is not the current primary — enter DR reader mode automatically
    System.setProperty(FeConstants.DR_READER_MODE_KEY, "true");
    LOG.info("Site {} is not primary (primary is {}). Entering DR reader mode.",
             Config.replication_site_name, groupConfig.primarySite);
}
```

Beijing simply starts reading Shanghai's segments from the bucket and catches
up. When the human decides Beijing is healthy and wants to failback, they run
the failback command.

---

## 14. Replication Manager — Full Command Reference

```
Commands:
  create-group       one-time setup of replication group (all vaults)
  add-vault-mapping  add a new vault mapping after initial setup
  reload-config      reload replication-group.json on all MS nodes (SIGHUP)
  show-group         monitor lag, RPO, consistent point, per-vault CRR status
  failover           primary → secondary (disaster)
  failback           secondary → primary (recovery)
  status             detailed state of all components
  verify             pre-flight check before failover/failback
```

```bash
# Setup — auto-discovers all vaults from primary MS, prompts for secondary config
./replication-manager create-group \
  --group-id       bj_to_sh \
  --primary-site   beijing  --primary-fe bj-fe:8030  --primary-ms bj-ms:5000 \
  --secondary-site shanghai --secondary-fe sh-fe:8030 --secondary-ms sh-ms:5000 \
  --storage-type   OSS \
  --replication-bucket  replication-data \
  --primary-endpoint    oss-cn-beijing-internal.aliyuncs.com \
  --secondary-endpoint  oss-cn-shanghai-internal.aliyuncs.com \
  --credential-type     instance_profile \
  --vault-map "primary_vault=doris-shanghai-data" \
  --vault-map "analytics_vault=analytics-shanghai" \
  --vault-map "cold_vault=cold-shanghai" \
  --crr-max-lag-seconds 300 \
  --fdb-mode fdbbackup

# Or use interactive mode — tool discovers vaults and prompts for each:
./replication-manager create-group ... --interactive

  Discovering vaults from primary cluster...
  Found 3 vaults:
    [1] primary_vault   → oss-cn-beijing/doris-beijing-data
    [2] analytics_vault → oss-cn-beijing/analytics-beijing
    [3] cold_vault      → oss-cn-beijing/cold-beijing

  Secondary endpoint for all vaults [oss-cn-shanghai-internal.aliyuncs.com]: (enter)
  Secondary bucket for primary_vault   [doris-beijing-data]:   doris-shanghai-data
  Secondary bucket for analytics_vault [analytics-beijing]:    analytics-shanghai
  Secondary bucket for cold_vault      [cold-beijing]:         cold-shanghai

  Writing replication-group.json... done.
  3 vault mappings configured.

# Add a new vault after initial setup
./replication-manager add-vault-mapping \
  --group-id         bj_to_sh \
  --vault-name       new_vault \
  --secondary-bucket new-vault-shanghai \
  --secondary-endpoint oss-cn-shanghai-internal.aliyuncs.com \
  --credential-type  instance_profile

# Reload config on all MS nodes after adding vault mapping
./replication-manager reload-config --group-id bj_to_sh

# Monitor — shows per-vault CRR lag
./replication-manager show-group --group-id bj_to_sh

┌────────────────────────────┬──────────────────────────┬────────┐
│ Component                  │ Lag                       │ Status │
├────────────────────────────┼──────────────────────────┼────────┤
│ FE EditLog                 │ 3.2 seconds               │ OK     │
│ FDB                        │ 0.6 seconds               │ OK     │
│ CRR primary_vault (BJ→SH)  │ 2.1 minutes               │ OK     │
│ CRR primary_vault (SH→BJ)  │ 2.3 minutes               │ OK     │
│ CRR analytics_vault (BJ→SH)│ 1.8 minutes               │ OK     │
│ CRR analytics_vault (SH→BJ)│ 2.0 minutes               │ OK     │
│ CRR cold_vault (BJ→SH)     │ 4.1 minutes               │ WARN   │
│ CRR cold_vault (SH→BJ)     │ 4.3 minutes               │ WARN   │
│ Consistent point           │ 2026-07-14 09:40:30 UTC   │ OK     │
│ RPO (max CRR lag)          │ 4.3 minutes               │ WARN   │
│ Current primary            │ beijing                   │ OK     │
│ Vault mappings             │ 3 of 3 mapped             │ OK     │
└────────────────────────────┴──────────────────────────┴────────┘

# Disaster
./replication-manager failover --group-id bj_to_sh --to-site shanghai

# Recovery (after Beijing is healthy)
./replication-manager failback --group-id bj_to_sh --to-site beijing
```

**`show-group` RPO = max CRR lag across ALL vault buckets**, not just one.
A slow vault (cold storage, large objects) drives the overall RPO. Each vault
is monitored independently so the operator knows exactly which vault is lagging.

## 15. Configuration Reference

```properties
# fe.conf — same file on both sites, only site-specific fields differ

# ── Master switch ────────────────────────────────────────────────────────
# Default false — entire feature dormant, zero impact on unrelated clusters
# Set true only when replication group is set up and tested
enable_replication_group     = false

# ── DR read-only guard (secondary FEs only) ──────────────────────────────
# Set true on Shanghai FEs to block writes and serve reads only
# Only checked when enable_replication_group = true
dr_read_only_mode            = false

# ── Site identity ────────────────────────────────────────────────────────
replication_group_id         = bj_to_sh
replication_site_name        = beijing          # beijing | shanghai

# ── Replication bucket (same bucket, different regional endpoints) ────────
replication_storage_type     = OSS              # S3 | OSS | GCS
replication_bucket           = replication-data
replication_endpoint         = oss-cn-beijing-internal.aliyuncs.com

# ── Credentials — choose ONE ─────────────────────────────────────────────
# Recommended: instance RAM role (zero config)
replication_credential_type  = instance_profile

# Alternative: STS AssumeRole via ARN
# replication_credential_type    = assume_role
# replication_role_arn           = acs:ram::123456789:role/doris-replication-role
# replication_role_session_name  = doris-replication-beijing
# replication_external_id        = optional-external-id

# GCP / Kubernetes workload identity
# replication_credential_type    = workload_identity

# Dev/test only — never in production (logs a WARNING when used)
# replication_credential_type    = ak_sk
# replication_access_key         = <AK>
# replication_secret_key         = <SK>

# ── Tuning ────────────────────────────────────────────────────────────────
replication_export_interval_ms    = 5000    # export EditLog every 5 seconds
replication_export_batch_size     = 500     # journal entries per segment file
replication_checkpoint_interval_ms = 30000  # checkpoint every 30 seconds
replication_crr_max_lag_ms        = 300000  # assumed max CRR lag (5 min)
replication_recovery_mode         = auto-standby  # auto-enter DR mode if not primary
```

---

## 16. Core Doris Changes Summary

All changes isolated to new files or small additions. The `enable_replication_group`
flag ensures zero impact on clusters that don't use this feature.

```
New files (no upstream conflict — new package):
  fe/fe-core/.../replication/EditLogS3Exporter.java    ~380 lines
  fe/fe-core/.../replication/credentials/*.java        ~300 lines
  fe/fe-core/.../replication/storage/*.java            ~400 lines
  fe/fe-core/.../journal/S3JournalCursor.java          ~300 lines

Modified files — FE (rebase on each Doris upgrade):
  fe/fe-core/.../catalog/Env.java          +50 lines  (exporter start, gated on flag)
  fe/fe-core/.../DorisFE.java              +70 lines  (--dr-reader-mode, auto-standby, gated)
  fe/fe-core/.../qe/StmtExecutor.java      +20 lines  (write guard, gated on flag)
  fe/fe-common/.../common/Config.java      +25 lines  (2 new flags + replication fields)
  fe/fe-core/.../common/FeConstants.java   +5 lines   (constants)

Modified files — MS (cloud/src/meta-service/):
  meta_service.cpp                         +20 lines  (vault mapping override in get_obj_store_info)
  meta_service.h or config.h               +5 lines   (enable_replication_group, is_primary_site flags)
```

**Flag summary:**

| Flag | Default | Effect when true |
|---|---|---|
| `enable_replication_group` | `false` | Master switch — activates entire feature (FE + MS) |
| `dr_read_only_mode` | `false` | FE rejects all write statements |
| `is_primary_site` | `true` | MS applies vault mapping override when `false` (secondary) |

**MS vault mapping behaviour:**

When `enable_replication_group = true` and `is_primary_site = false`:
- MS reads `replication-group.json` from the replication bucket at startup (cached in memory)
- For each storage resource request: looks up vault name in mapping, applies secondary config
- Unmapped vaults: fall back to primary config + log WARNING
- Config reload: `SIGHUP` or `replication-manager reload-config`



## 17. RPO / RTO Analysis

### Normal operation RPO

```
RPO = max(fe_lag, fdb_lag, oss_crr_lag)

fe_lag   = export_interval (5s) + S3 propagation (~1s) ≈ 6 seconds
fdb_lag  = fdbbackup streaming ≈ 2–5 seconds
oss_lag  = CRR standard 1–5 min; RTC ~10 seconds

Standard CRR:   RPO ≈ 5 minutes
RTC enabled:    RPO ≈ 15 seconds
```

### Failover RTO

```
Managed FDB:    5–35 seconds
fdbbackup FDB:  5–15 minutes
```

### Failback RTO (after recovery)

```
FE EditLog catchup:   minutes (replaying DDL only)
OSS data:             0 seconds (bidirectional CRR already synced)
FDB restore:          0s managed / 5–15 min fdbbackup
Total failback:       minutes (no bulk data transfer)
```

### Summary

| Setup | Failover RPO | Failover RTO | Failback RTO |
|---|---|---|---|
| Standard CRR + managed FDB | 5 min | 35 sec | ~2 min |
| RTC + managed FDB | 15 sec | 35 sec | ~1 min |
| Standard CRR + fdbbackup | 5 min | 15 min | ~15 min |

---

## 18. Upgrade Strategy

When Doris releases a new version, rebase your fork:

```bash
# 1. Fetch and rebase onto new Doris release
git fetch upstream
git rebase upstream/master

# 2. Resolve conflicts (expected in these files only):
#    fe/fe-core/.../catalog/Env.java          (+50 lines, startup section)
#    fe/fe-core/.../DorisFE.java              (+70 lines, flag handling)
#    fe/fe-common/.../common/Config.java      (+20 lines, config fields)
#
#    All replication/* and journal/S3JournalCursor.java:
#    → zero conflicts (new files in a new package, upstream never touches them)

# 3. Rebuild FE
./build.sh --fe

# 4. Replication manager (tools/replication-manager/):
#    zero changes needed across Doris upgrades
#    uses stable FE HTTP API + Meta Service gRPC
```

Expected effort per Doris release: **30–60 minutes** (only 4 modified files rebase).

---

## Port Reference

All port numbers are Doris defaults from source code. Your deployment may differ.

| Component | Port | Protocol | Source config | Notes |
|---|---|---|---|---|
| FE HTTP | `8030` | HTTP | `fe.conf: http_port` | When `enable_https=false` |
| FE HTTPS | `8050` | HTTPS | `fe.conf: https_port` | When `enable_https=true` (http_port disabled) |
| FE MySQL query | `9030` | MySQL | `fe.conf: query_port` | Always plaintext |
| FE BDB edit log | `9010` | TCP | `fe.conf: edit_log_port` | Inter-FE replication |
| BE HTTP | `8040` | HTTP | `be.conf: webserver_port` | NOT used by replication |
| BE brpc | `8060` | brpc | `be.conf: brpc_port` | |
| BE heartbeat | `9050` | TCP | `be.conf: heartbeat_service_port` | |
| Meta Service brpc | `5000` | brpc | `meta_service.conf: brpc_listen_port` | |

**Key rules:**
- Replication endpoints (`/api/replication/*`) live on the **FE** — use `8030` (HTTP) or `8050` (HTTPS)
- `--primary-fe` / `--secondary-fe`: `host:8030` for HTTP, `host:8050` for HTTPS
- Add `--use-https` flag when `enable_https=true`; add `--ca-cert` for internal/self-signed certs
- `--primary-ms` / `--secondary-ms`: always `host:5000` (MS brpc, no TLS)
- MySQL queries always use `9030` regardless of HTTPS

```bash
# Plain HTTP cluster (enable_https=false)
./replication_manager.py failover --group-id bj_to_sh --to-site shanghai

# HTTPS cluster (enable_https=true, https_port=8050) — CA in system trust store
./replication_manager.py --use-https \
  failover --group-id bj_to_sh --to-site shanghai

# HTTPS with internal/self-signed CA — export from FE keystore first:
#   keytool -export -keystore $DORIS_HOME/conf/ssl/doris_keystore.jks \
#           -alias doris_ssl_certificate -file /tmp/doris-ca.crt -rfc
./replication_manager.py --use-https --ca-cert /tmp/doris-ca.crt \
  show-group --group-id bj_to_sh
```

**How Doris HTTPS works (from `InternalHttpsUtils.java`):**
- FE uses a **Java KeyStore (JKS)** (`key_store_path`, `key_store_password`, `key_store_type`)
- Every cert in every chain of the keystore is trusted — CA cert validates all nodes it signed
- **Hostname verification disabled** (`NoopHostnameVerifier`) — internal certs work regardless of CN/SAN
- Python client mirrors this: `ctx.check_hostname = False`, loads CA from exported PEM

---

## References

- TiDB TiCDC — resolved timestamp for multi-store consistency
- MySQL binlog shipping — EditLog S3 export pattern
- Snowflake Replication Groups — user-facing API design
- FoundationDB fdbbackup — continuous backup to S3
- Apache Doris CloudMetaSyncPoint — existing FE+FDB consistency pairing
