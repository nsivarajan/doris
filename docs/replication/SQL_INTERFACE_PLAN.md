# Replication Group — SQL Command Interface

**Status:** Planned  
**Replaces:** `tools/replication-manager/` Python CLI  
**Goal:** All replication operations available as SQL statements from any `mysql` client.  
**MS config changes:** Pushed via a new brpc RPC — no `meta_service.conf` editing, no MS restart.

---

## Why SQL instead of Python CLI

| Concern | Python CLI | SQL Commands |
|---|---|---|
| Install dependency | Python 3 + packages on ops machine | None — any `mysql` client works |
| Access control | Caller must have OS access to run script | Doris ADMIN privilege enforced by FE |
| Audit trail | Script execution log (external) | FE audit log (automatic, per-query) |
| Observability | Separate tool output | `SHOW REPLICATION GROUP STATUS` returns rows |
| MS config changes | Manual `meta_service.conf` edit + restart | FE calls MS brpc `apply_vault_override` |
| Snowflake parity | No | Yes — same pattern (`ALTER DATABASE ... PROMOTE`) |

---

## Target SQL Interface

```sql
-- ── Setup (one-time) ─────────────────────────────────────────────────────────
ALTER SYSTEM CREATE REPLICATION GROUP 'bj_to_sh'
  PRIMARY SITE 'beijing'
  SECONDARY SITE 'shanghai'
  PROPERTIES (
    'storage_type'        = 'OSS',
    'replication_bucket'  = 'doris-replication-bj',
    'replication_endpoint'= 'oss-cn-beijing-internal.aliyuncs.com',
    'credential_type'     = 'instance_profile'
  );

ALTER SYSTEM REPLICATION ADD VAULT MAPPING 'primary_vault'
  PROPERTIES (
    'secondary_endpoint' = 'oss-cn-shanghai-internal.aliyuncs.com',
    'secondary_bucket'   = 'doris-shanghai-data'
  );

-- ── Operations ───────────────────────────────────────────────────────────────
ALTER SYSTEM REPLICATION FAILOVER  TO SITE 'shanghai';   -- primary down → shanghai live
ALTER SYSTEM REPLICATION FAILBACK  TO SITE 'beijing';    -- beijing recovered → restore primary
ALTER SYSTEM REPLICATION PAUSE EXPORT;                   -- pause segment writing (pre-failover)
ALTER SYSTEM REPLICATION PROMOTE MASTER;                 -- this FE becomes BDB master
ALTER SYSTEM REPLICATION ENTER DR MODE;                  -- this FE becomes read-only DR reader
ALTER SYSTEM REPLICATION ENTER DRILL MODE;               -- isolated DR write test (primary untouched)
ALTER SYSTEM REPLICATION EXIT  DRILL MODE;               -- end drill, restore read-only guard

-- ── Monitoring ───────────────────────────────────────────────────────────────
SHOW REPLICATION GROUP STATUS;   -- one row per site, all component states
SHOW REPLICATION GROUP LAG;      -- FE lag, FDB lag, CRR lag per vault
```

All commands require **ADMIN** privilege. SELECT, SHOW, DESC continue to work on DR FE regardless.

---

## Part 1 — MS brpc: vault override RPC

### Why this is needed

`replication_vault_overrides` is currently a `meta_service.conf` string.  
Changing it requires editing the file on every MS node and restarting.  
The new RPC lets FE push vault overrides to MS in-memory AND persist them to FDB — no restart.

### Proto changes (`gensrc/proto/cloud.proto`)

```protobuf
message ApplyVaultOverrideRequest {
    optional string cloud_unique_id = 1;
    optional string vault_name      = 2;   // which vault to remap
    optional string endpoint        = 3;   // new OSS/S3 endpoint for secondary
    optional string bucket          = 4;   // new bucket name for secondary
    optional bool   clear           = 5;   // true = remove override (failback)
}

message ApplyVaultOverrideResponse {
    optional MetaServiceResponseStatus status = 1;
}

// Add to service MetaService { ... }
rpc apply_vault_override(ApplyVaultOverrideRequest)
    returns (ApplyVaultOverrideResponse);
```

### C++ handler — `cloud/src/meta-service/meta_service_replication.cpp` (new file)

```
apply_vault_override():
  1. Validate request (vault_name, endpoint, bucket non-empty unless clear=true)
  2. Write to FDB key: replication/vault_overrides/{vault_name}
     → persists across MS restart; MS reads all these keys at startup
  3. Update resource_mgr_'s in-memory override map
     → get_obj_store_info() immediately returns new endpoint/bucket
  4. If clear=true: delete FDB key + remove from in-memory map
  5. Return OK
```

FDB key layout:
```
replication/vault_overrides/primary_vault  → {"endpoint":"oss-cn-sh...","bucket":"doris-sh-data"}
replication/vault_overrides/archive_vault  → {"endpoint":"oss-cn-sh...","bucket":"doris-sh-arch"}
```

MS startup change: `meta_service_resource.cpp` reads all `replication/vault_overrides/*` keys from FDB
and populates in-memory map. Replaces `config::replication_vault_overrides` string entirely.

### Files changed (MS side)

| File | Change |
|---|---|
| `gensrc/proto/cloud.proto` | Add `ApplyVaultOverrideRequest/Response` + service entry |
| `cloud/src/meta-service/meta_service.h` | Add `apply_vault_override()` override to `MetaServiceImpl` |
| `cloud/src/meta-service/meta_service.cpp` | Register handler (call_impl wrapper) |
| `cloud/src/meta-service/meta_service_replication.cpp` | **New** — full handler implementation |
| `cloud/src/meta-service/meta_service_resource.cpp` | Read FDB override keys at startup; remove config string parsing |
| `cloud/src/common/config.h` | Remove `replication_vault_overrides` config field |

---

## Part 2 — FE persistent state + mutable config

### Problem

FE state changes (which site is primary, DR read-only mode) must survive FE restart.  
Currently these are `fe.conf` values that require restart to change.

### Solution A — Mutable config (no restart needed)

Mark replication configs `@ConfField(mutable = true)` in `Config.java`:

```java
@ConfField(mutable = true) public static boolean dr_read_only_mode = false;
@ConfField(mutable = true) public static boolean enable_replication_group = false;
@ConfField(mutable = true) public static String  replication_primary_endpoint = "";
```

SQL command handler calls `ConfigBase.setMutableConfig()` internally — no `ADMIN SET FRONTEND CONFIG`
needed from the operator.

### Solution B — BDB-persisted group state

New class `ReplicationGroupInfo` written to BDB journal via `EditLog` so it survives FE restart:

```java
// fe/fe-core/.../replication/ReplicationGroupInfo.java
public class ReplicationGroupInfo implements Writable {
    public String  groupId;
    public String  primarySite;       // "beijing" or "shanghai" — updated on failover/failback
    public boolean drReadOnly;        // written with primarySite so they stay in sync
    public Map<String, VaultOverride> vaultOverrides; // mirrors what was pushed to MS
    public long    lastUpdatedMs;
}
```

`EditLog` gets new opcode `OperationType.OP_REPLICATION_GROUP_INFO`.  
`Env.loadJournal()` handles this opcode → updates `Env.replicationGroupInfo`.  
On FE startup, `applyAutoStandbyIfNeeded()` reads this from BDB instead of from bucket JSON.

### Files changed (FE config + state)

| File | Change |
|---|---|
| `fe/fe-common/.../common/Config.java` | Mark 3 fields `mutable = true` |
| `fe/fe-core/.../replication/ReplicationGroupInfo.java` | **New** — Writable BDB state object |
| `fe/fe-core/.../persist/OperationType.java` | Add `OP_REPLICATION_GROUP_INFO` opcode |
| `fe/fe-core/.../catalog/Env.java` | `replicationGroupInfo` field; `loadJournal()` handler; `applyAutoStandby()` reads BDB |
| `fe/fe-core/.../journal/EditLog.java` | `logReplicationGroupInfo(ReplicationGroupInfo)` method |

---

## Part 3 — Grammar (DorisParser.g4 + DorisLexer.g4)

### New tokens (`DorisLexer.g4`)

```antlr
REPLICATION : 'REPLICATION' ;
FAILOVER    : 'FAILOVER'    ;
FAILBACK    : 'FAILBACK'    ;
PROMOTE     : 'PROMOTE'     ;
DRILL       : 'DRILL'       ;
```

All five are new — confirmed absent from current lexer.

### Parser rules (`DorisParser.g4`)

Add to `alterSystemClause`:

```antlr
// ── Replication Group Setup ──────────────────────────────────────────────────
| REPLICATION CREATE GROUP name=stringLiteral
      PRIMARY SITE primarySite=stringLiteral
      SECONDARY SITE secondarySite=stringLiteral
      properties=propertyClause?                               #replicationCreateGroup

| REPLICATION ADD VAULT MAPPING vaultName=identifier
      properties=propertyClause                               #replicationAddVaultMapping

// ── Operational Commands ─────────────────────────────────────────────────────
| REPLICATION FAILOVER  TO SITE site=stringLiteral            #replicationFailover
| REPLICATION FAILBACK  TO SITE site=stringLiteral            #replicationFailback
| REPLICATION PAUSE EXPORT                                     #replicationPauseExport
| REPLICATION PROMOTE MASTER                                   #replicationPromoteMaster
| REPLICATION ENTER DR MODE                                    #replicationEnterDrMode
| REPLICATION ENTER DRILL MODE                                 #replicationEnterDrillMode
| REPLICATION EXIT  DRILL MODE                                 #replicationExitDrillMode
```

Add to `showStatement`:

```antlr
| SHOW REPLICATION GROUP STATUS                               #showReplicationGroupStatus
| SHOW REPLICATION GROUP LAG                                  #showReplicationGroupLag
```

---

## Part 4 — FE Java: Command classes

### File layout

```
fe/fe-core/src/main/java/org/apache/doris/
  nereids/
    trees/plans/commands/
      info/
        ReplicationCreateGroupOp.java        ← new Op (one per ALTER SYSTEM variant)
        ReplicationAddVaultMappingOp.java
        ReplicationFailoverOp.java
        ReplicationFailbackOp.java
        ReplicationPauseExportOp.java
        ReplicationPromoteMasterOp.java
        ReplicationEnterDrModeOp.java
        ReplicationDrillModeOp.java          ← covers enter+exit
      ShowReplicationGroupCommand.java       ← new (handles both STATUS and LAG)

  replication/
    ReplicationCommandHandler.java           ← new (all business logic, one class)
```

`AlterSystemCommand.java` — existing file, no structural change; `doRun()` dispatches to
`ReplicationCommandHandler` based on Op type.

### Op pattern (same as `AddBackendOp`)

Each Op:
- Holds only the parsed parameters (immutable after construction)
- Implements `validate(ConnectContext)` — checks ADMIN privilege, validates parameter values
- Implements `toSql()` — for audit log reconstruction

Example:
```java
// ReplicationFailoverOp.java
public class ReplicationFailoverOp extends AlterSystemOp {
    private final String targetSite;

    public ReplicationFailoverOp(String targetSite) {
        this.targetSite = targetSite;
    }

    @Override
    public void validate(ConnectContext ctx) throws AnalysisException {
        // require ADMIN privilege
        if (!ctx.getCurrentUserIdentity().isRootUser()
                && !Env.getCurrentEnv().getAccessManager()
                       .checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            throw new AnalysisException("Access denied; requires ADMIN privilege");
        }
        if (targetSite == null || targetSite.isEmpty()) {
            throw new AnalysisException("FAILOVER requires a non-empty site name");
        }
    }

    @Override
    public String toSql() {
        return "ALTER SYSTEM REPLICATION FAILOVER TO SITE '" + targetSite + "'";
    }
}
```

### `ReplicationCommandHandler` — business logic

Single class; methods map 1:1 to SQL commands:

| Method | Steps |
|---|---|
| `createGroup(groupId, primarySite, secondarySite, props)` | Validate props → write `ReplicationGroupInfo` to BDB → start exporter on this FE if primary |
| `addVaultMapping(vaultName, endpoint, bucket)` | Call MS `apply_vault_override` on all MS nodes → update `ReplicationGroupInfo` in BDB |
| `failover(targetSite)` | 1. Check lag threshold, 2. Pause export, 3. Wait DR FE catchup, 4. Call `apply_vault_override` for all vaults (secondary endpoint), 5. Promote master, 6. Update `ReplicationGroupInfo` (primarySite=targetSite) |
| `failback(targetSite)` | Mirror of failover — CRR lag check first, then reverse vault remap |
| `pauseExport()` | `EditLogS3Exporter.pause()` |
| `promoteMaster()` | Stop S3JournalCursor → start BDB master → start exporter |
| `enterDrMode()` | Stop exporter → start S3JournalCursor → set `dr_read_only_mode=true` → write BDB |
| `enterDrillMode()` | Lift write guard WITHOUT starting exporter (primary bucket stays clean) |
| `exitDrillMode()` | Restore write guard |

MS calls within `ReplicationCommandHandler`:
```java
// calls apply_vault_override on every MS node in the cluster
private void pushVaultOverride(String vaultName, String endpoint, String bucket) {
    List<Backend> msNodes = Env.getCurrentSystemInfo().getMetaServiceNodes();
    for (Backend ms : msNodes) {
        ApplyVaultOverrideRequest req = ApplyVaultOverrideRequest.newBuilder()
            .setCloudUniqueId(Config.cloud_unique_id)
            .setVaultName(vaultName)
            .setEndpoint(endpoint)
            .setBucket(bucket)
            .build();
        ApplyVaultOverrideResponse resp = MetaServiceProxy.getInstance()
            .applyVaultOverride(req);
        if (resp.getStatus().getCode() != MetaServiceCode.OK) {
            throw new UserException("MS vault override failed on " + ms.getHost()
                + ": " + resp.getStatus().getMsg());
        }
    }
}
```

### `ShowReplicationGroupCommand` — result sets

```
SHOW REPLICATION GROUP STATUS columns:
  group_id | primary_site | this_site | exporter_running | dr_read_only
  | last_export_journal_id | lag_entries | consistent_point_ts | uptime_sec

SHOW REPLICATION GROUP LAG columns:
  group_id | vault_name | primary_to_dr_lag_sec | dr_to_primary_lag_sec
  | fdb_lag_sec | rpo_sec | status
```

`status` = `OK` / `WARN` / `CRITICAL` based on configured thresholds.

### `LogicalPlanBuilder.java` — visitor methods

One `visit*` per grammar rule; same pattern as `visitAddBackendClause`:

```java
@Override
public LogicalPlan visitReplicationFailover(ReplicationFailoverContext ctx) {
    String site = stripQuotes(ctx.site.getText());
    return new AlterSystemCommand(
        new ReplicationFailoverOp(site),
        PlanType.ALTER_SYSTEM_REPLICATION_FAILOVER);
}
// ... same pattern × 9 commands
```

Add `ALTER_SYSTEM_REPLICATION_*` entries to `PlanType` enum.

---

## Part 5 — Cleanup

### Python CLI retirement

`tools/replication-manager/` — kept as historical reference, marked `[DEPRECATED]` in README.  
No active maintenance after SQL interface ships.

### HTTP endpoints

`ReplicationAction.java` — mutation endpoints (`promote-master`, `enter-dr-mode`, `pause-export`,
`enter-drill-mode`, `exit-drill-mode`) become internal-only:
- Remove from documented API
- Add `localhost-only` guard (reject requests from non-loopback IPs)
- `ReplicationCommandHandler` still calls them internally via direct method call (not HTTP)

Read endpoints (`/status`, `/cursor`, `/metrics`) stay public for Prometheus scraping.

---

## File Inventory

### New files (11)

| File | Lines (est.) |
|---|---|
| `gensrc/proto/cloud.proto` additions | +25 |
| `cloud/src/meta-service/meta_service_replication.cpp` | ~120 |
| `fe/fe-core/.../replication/ReplicationGroupInfo.java` | ~80 |
| `fe/fe-core/.../commands/info/ReplicationCreateGroupOp.java` | ~60 |
| `fe/fe-core/.../commands/info/ReplicationAddVaultMappingOp.java` | ~50 |
| `fe/fe-core/.../commands/info/ReplicationFailoverOp.java` | ~50 |
| `fe/fe-core/.../commands/info/ReplicationFailbackOp.java` | ~50 |
| `fe/fe-core/.../commands/info/ReplicationPauseExportOp.java` | ~35 |
| `fe/fe-core/.../commands/info/ReplicationPromoteMasterOp.java` | ~35 |
| `fe/fe-core/.../commands/info/ReplicationEnterDrModeOp.java` | ~35 |
| `fe/fe-core/.../commands/info/ReplicationDrillModeOp.java` | ~40 |
| `fe/fe-core/.../commands/ShowReplicationGroupCommand.java` | ~120 |
| `fe/fe-core/.../replication/ReplicationCommandHandler.java` | ~300 |

### Modified files (9)

| File | Change |
|---|---|
| `gensrc/proto/cloud.proto` | +25 lines (messages + service entry) |
| `cloud/src/meta-service/meta_service.h` | +3 lines (override declaration) |
| `cloud/src/meta-service/meta_service.cpp` | +5 lines (call_impl registration) |
| `cloud/src/meta-service/meta_service_resource.cpp` | ~30 lines (FDB key read at startup; remove config parse) |
| `cloud/src/common/config.h` | -1 line (remove `replication_vault_overrides`) |
| `fe/fe-common/.../common/Config.java` | +3 lines (mutable = true on 3 fields) |
| `fe/fe-core/.../persist/OperationType.java` | +1 line (new opcode) |
| `fe/fe-core/.../catalog/Env.java` | ~40 lines (BDB state read; handler for new opcode) |
| `fe/fe-core/.../journal/EditLog.java` | ~15 lines (`logReplicationGroupInfo()` method) |
| `fe/fe-sql-parser/.../DorisLexer.g4` | +5 lines (5 new tokens) |
| `fe/fe-sql-parser/.../DorisParser.g4` | +15 lines (9 alter + 2 show rules) |
| `fe/fe-core/.../parser/LogicalPlanBuilder.java` | +90 lines (9 visitor methods) |
| `fe/fe-core/.../nereids/trees/plans/PlanType.java` | +9 lines (new enum values) |

---

## Implementation Sequence

Build in this order — each step compiles and tests independently:

```
Step 1 (MS)      cloud.proto + meta_service_replication.cpp + startup FDB read
                 → test: curl MS apply_vault_override, verify in-memory map updated

Step 2 (FE BDB)  ReplicationGroupInfo.java + OperationType + EditLog + Env handler
                 → test: write + restart FE, verify state restored from BDB

Step 3 (FE cmd)  ReplicationCommandHandler.java (stub methods, no MS calls yet)
                 → compilable, methods log and return OK

Step 4 (grammar) DorisLexer.g4 + DorisParser.g4 + LogicalPlanBuilder.java + Op classes
                 → test: parse each SQL statement, verify no ParseException

Step 5 (wire-up) ReplicationCommandHandler calls MS RPC + Env methods
                 → integration test: FAILOVER command → vault override pushed to MS

Step 6 (SHOW)    ShowReplicationGroupCommand.java + result set columns
                 → test: SHOW REPLICATION GROUP STATUS returns correct row

Step 7 (cleanup) Mutation HTTP endpoints → localhost-only; Python CLI → deprecated notice
```

---

## Test Plan

### Unit tests (new)

| Test class | What it covers |
|---|---|
| `ReplicationGroupInfoTest` | Writable serialise/deserialise round-trip; BDB write + restart |
| `ReplicationFailoverOpTest` | Validate: ADMIN required; empty site rejected |
| `ReplicationCommandHandlerTest` | `failover()` pauses exporter, pushes vault, promotes master, updates BDB |
| `ShowReplicationGroupCommandTest` | STATUS and LAG result sets have correct columns and row data |
| `ApplyVaultOverrideRpcTest` (C++) | Handler writes to FDB, updates in-memory map, clear removes both |

### SQL smoke tests

```sql
-- run on a dev cluster after implementation:
ALTER SYSTEM REPLICATION PAUSE EXPORT;
SHOW REPLICATION GROUP STATUS;   -- exporter_running = false
ALTER SYSTEM REPLICATION PROMOTE MASTER;
SHOW REPLICATION GROUP STATUS;   -- this_site = primary_site
ALTER SYSTEM REPLICATION ENTER DR MODE;
INSERT INTO t VALUES (1);        -- must return: "DR read-only mode" error
SELECT count(*) FROM t;          -- must succeed
ALTER SYSTEM REPLICATION EXIT DRILL MODE;
```

---

## What Does NOT Change

- `EditLogS3Exporter.java` — unchanged (started/stopped by `ReplicationCommandHandler`)
- `S3JournalCursor.java` — unchanged (started/stopped by `ReplicationCommandHandler`)
- `StmtExecutor.java` DR write guard — unchanged (reads `Config.dr_read_only_mode`)
- `ReplicationAction.java` read endpoints — unchanged (`/metrics`, `/status`, `/cursor`)
- `DESIGN.md` — overall DR architecture unchanged; this is purely a control-plane evolution
- Runbooks — updated to show SQL commands instead of Python CLI invocations
