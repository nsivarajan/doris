# Doris Cloud Replication Group — Implementation Plan

**Total duration:** 20 weeks (~5 months)  
**Team size:** 2–3 engineers  
**Repo:** `doris-replication/` (separate from core Doris)  
**Core Doris patch:** ~825 lines, 6 files

---

## Quality Standards (apply to every phase)

Every task that produces code must satisfy all three before it is marked done:

### ✅ Unit Tests
- Every public method has at least one unit test
- Happy path + at least two failure/edge cases per method
- Use `LocalReplicationStorage` for fast in-process tests (no real S3/OSS)
- Test file mirrors the source file: `EditLogS3Exporter.java` → `EditLogS3ExporterTest.java`
- Coverage gate: **≥ 90%** line coverage on all new files

### ✅ Logging
Every class must log at these levels:

| Level | When to use |
|---|---|
| `INFO` | Startup, shutdown, state transitions (failover, failback, promote) |
| `DEBUG` | Per-operation detail (segment exported, cursor updated, checkpoint written) |
| `WARN` | Recoverable failure that will be retried (S3 write fail, segment read fail) |
| `ERROR` | Unrecoverable failure that stops the component |

Log format must include: `[ReplicationGroup:<group_id>] [Site:<site_name>] <message> key=<context>`

### ✅ Code Comments
- One-line comment above every method explaining **why**, not what
- One-line comment on any non-obvious variable or constant
- No multi-line blocks — if a comment needs more than one line, the method should be extracted

---

## Milestones at a Glance

```
Week 1–2:   Phase 0 — Storage abstraction + tests
Week 3–6:   Phase 1 — EditLog streaming (primary → bucket → DR FE)
Week 7–8:   Phase 2 — Replication Manager CLI (create-group, show-group)
Week 9–12:  Phase 3 — Failover (one-command, fully automated)
Week 13–16: Phase 4 — Failback + auto-recovery (one-command, no data transfer)
Week 17–20: Phase 5 — Hardening, load testing, runbooks
```

---

## Phase 0 — Foundation (Week 1–2)

**Goal:** Storage abstraction + credential providers working on all three providers.

### Tasks

**Credential providers (Week 1, first priority — everything else depends on this):**

- [ ] `ReplicationCredentials` value class — accessKey, secretKey, securityToken, expiresAt
- [ ] `ReplicationCredentialProvider` interface — single method `getCredentials()`
- [ ] `StaticCredentialProvider` — reads AK/SK from config (dev/testing only, logs a WARNING when used)
- [ ] `InstanceProfileCredentialProvider` — reads from cloud metadata endpoint, auto-refreshes
  - [ ] AWS path: `http://169.254.169.254/latest/meta-data/iam/security-credentials/<role>`
  - [ ] Alibaba Cloud path: `http://100.100.100.200/latest/meta-data/ram/security-credentials/<role>`
  - [ ] GCP path: `http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/`
- [ ] `AssumeRoleCredentialProvider` — STS AssumeRole using role ARN, auto-refreshes 5 min before expiry
  - [ ] AWS STS: `AssumeRole` API
  - [ ] Alibaba Cloud RAM STS: `AssumeRole` API (reuse the STS work already done in this project)
- [ ] `WorkloadIdentityCredentialProvider` — GCP/K8s OIDC token exchange
- [ ] `CredentialRefreshScheduler` — background thread that refreshes before expiry, shared by all providers
- [ ] `ReplicationConfig` — credential_type field + role_arn, role_session_name, external_id fields
- [ ] `ReplicationStorageFactory.buildCredentialProvider()` — selects provider from config

**Storage backends (Week 2):**

- [ ] `ReplicationStorageBackend` interface (5 methods)
- [ ] `ReplicationStorageException` with error codes
- [ ] `LocalReplicationStorage` — filesystem-backed, no credentials needed
- [ ] `S3ReplicationStorage` — injects `ReplicationCredentialProvider`, builds AWS client per call
- [ ] `OSSReplicationStorage` — injects `ReplicationCredentialProvider`, builds OSS client per call
- [ ] `GCSReplicationStorage` — injects `ReplicationCredentialProvider`, builds GCS client per call
- [ ] `ReplicationStorageFactory` — creates backend + provider from config
- [ ] `CheckpointData` data model

### Unit Tests checklist

**Credential provider tests:**

- [ ] `StaticCredentialProviderTest`
  - [ ] `testReturnsConfiguredCredentials` — verify AK/SK returned correctly
  - [ ] `testLogsWarningWhenUsed` — verify WARNING logged (prod misuse detection)
- [ ] `AssumeRoleCredentialProviderTest`
  - [ ] `testReturnsCredentialsFromSTS` — mock STS response → verify temp credentials returned
  - [ ] `testRefreshesBeforeExpiry` — credentials expire in 4 min → verify refresh called
  - [ ] `testDoesNotRefreshIfFresh` — credentials expire in 30 min → verify no refresh
  - [ ] `testHandlesSTSError` — STS returns error → logs ERROR, throws ReplicationStorageException
  - [ ] `testIncludesExternalIdWhenConfigured` — verify ExternalId sent to STS when set
  - [ ] `testOmitsExternalIdWhenNotConfigured` — verify ExternalId not sent when not set
- [ ] `InstanceProfileCredentialProviderTest`
  - [ ] `testReadsFromAWSMetadataEndpoint` — mock HTTP → verify correct IAM role path queried
  - [ ] `testReadsFromAlibabaMetadataEndpoint` — mock HTTP → verify RAM role path queried
  - [ ] `testAutoRefreshesBeforeExpiry` — verify refresh before expiry window
  - [ ] `testHandlesMetadataEndpointUnavailable` — HTTP 404 → clear error, not silent failure
- [ ] `ReplicationStorageFactoryTest`
  - [ ] `testCreatesInstanceProfileProvider` — config `credential_type=instance_profile` → correct provider
  - [ ] `testCreatesAssumeRoleProvider` — config `credential_type=assume_role` + ARN → correct provider
  - [ ] `testCreatesStaticProvider` — config `credential_type=ak_sk` → correct provider + WARNING logged
  - [ ] `testMissingRoleArnThrows` — `credential_type=assume_role` without `role_arn` → IllegalArgumentException

**Storage backend tests:**

- [ ] `ReplicationStorageBackendTest` (run against `LocalReplicationStorage`)
  - [ ] `testPutAndGet` — write, read back, byte-for-byte equal
  - [ ] `testPutIdempotent` — write same key twice, last value wins
  - [ ] `testGetMissingKey` — returns null, no exception
  - [ ] `testList` — 5 keys same prefix → all 5 returned sorted
  - [ ] `testListEmpty` — empty prefix → empty list
  - [ ] `testExists` — true after put, false before
  - [ ] `testDelete` — key missing after delete
  - [ ] `testDeleteMissing` — no-op, no exception
  - [ ] `testLargePayload` — 10MB round-trip
  - [ ] `testCredentialRefreshOnExpiry` — inject expiring credentials → verify refresh called before next put
- [ ] `ReplicationStorageFactoryTest`
  - [ ] `testCreateS3WithAssumeRole` → S3 impl with AssumeRole provider
  - [ ] `testCreateOSSWithInstanceProfile` → OSS impl with InstanceProfile provider
  - [ ] `testCreateGCSWithWorkloadIdentity` → GCS impl with WorkloadIdentity provider
  - [ ] `testUnknownTypeThrows` → IllegalArgumentException

### Logging checklist

- [ ] `StaticCredentialProvider` — WARN on construction: "AK/SK credentials configured — use instance_profile or assume_role in production"
- [ ] `AssumeRoleCredentialProvider` — INFO: "Assuming role {role_arn} with session {session_name}"
- [ ] `AssumeRoleCredentialProvider` — INFO: "Credentials refreshed, expire at {expiry}"
- [ ] `AssumeRoleCredentialProvider` — ERROR: "STS AssumeRole failed: {error}" with role_arn context
- [ ] `InstanceProfileCredentialProvider` — INFO: "Reading credentials from instance metadata ({provider} path)"
- [ ] `InstanceProfileCredentialProvider` — WARN: "Credential refresh attempted, metadata endpoint unavailable"
- [ ] `CredentialRefreshScheduler` — DEBUG: "Scheduled refresh for {provider} at {time}"
- [ ] `ReplicationStorageFactory` — INFO: "Created {storage_type} backend with {credential_type} credential provider"
- [ ] All storage impls — WARN on retry attempt (attempt count, error)

### Comments checklist

- [ ] `ReplicationCredentialProvider` interface — comment: why credentials are not stored in config files
- [ ] `AssumeRoleCredentialProvider.getCredentials()` — comment: why 5-minute early refresh window
- [ ] `InstanceProfileCredentialProvider` — comment: why three different metadata URLs (one per provider)
- [ ] `StaticCredentialProvider` constructor — comment: why this exists (testing, never production)
- [ ] `CredentialRefreshScheduler` — comment: why background refresh is needed (avoids request latency spike at expiry)
- [ ] `ReplicationStorageFactory.buildCredentialProvider()` — comment: why factory owns provider lifecycle (single refresh scheduler shared)

### Acceptance criteria

- [ ] All storage tests pass using `LocalReplicationStorage`
- [ ] `AssumeRoleCredentialProvider` tested with mock STS (not real AWS)
- [ ] `InstanceProfileCredentialProvider` tested with mock HTTP server
- [ ] `StaticCredentialProvider` always logs WARNING when constructed
- [ ] Integration test on real OSS bucket using instance RAM role (no AK/SK in test config)
- [ ] 90%+ line coverage on all credential + storage files

---

## Phase 1 — EditLog Streaming (Week 3–6)

**Goal:** DR FE stays in sync with primary FE via the replication bucket.

### Week 3–4: EditLogS3Exporter (primary side)

- [ ] `Config.java` — add master flag `enable_replication_group = false` and `dr_read_only_mode = false`
- [ ] `EditLogS3Exporter` class skeleton — `Runnable`, lifecycle fields, `stop()` method
- [ ] BDB journal read loop — reads from `journal.read(lastExportedJournalId + 1)`, batches entries
- [ ] Segment serialization — `JournalEntry` list → byte[] using same encoding as BDB
- [ ] Segment deserialization — byte[] → `JournalEntry` list, verify round-trip in test
- [ ] Segment key format — `<group_id>/fe-editlog/segment_<010d>.log`
- [ ] CURSOR file write — written only after segment confirmed in storage (idempotent)
- [ ] `recoverCursor()` — reads CURSOR on startup, resumes from last confirmed journal_id
- [ ] Records `written_by` site name in CURSOR so DR side knows who wrote it
- [ ] Checkpoint writer — reads `CloudMetaSyncPoint` from journal, writes `checkpoint/latest.json`
- [ ] Checkpoint history — writes timestamped copy to `checkpoint/history/`, keeps last 100
- [ ] Hook into `Env.java` — starts thread only when `enable_replication_group = true` AND FE is master
- [ ] `Config.java` additions — all replication config fields

#### Unit Tests checklist

- [ ] `EditLogS3ExporterTest`
  - [ ] `testExportBatch` — feed 10 journal entries, verify segment written to storage, CURSOR updated
  - [ ] `testExportBatchEmpty` — no new entries → no segment written, no CURSOR update
  - [ ] `testIdempotentSegmentWrite` — same segment written twice → storage has one copy, no error
  - [ ] `testCursorUpdatedOnlyAfterSegmentConfirmed` — simulate storage failure on segment write → CURSOR not updated
  - [ ] `testRecoverCursorOnStartup` — pre-populate CURSOR, create exporter, verify starts from cursor journal_id
  - [ ] `testRecoverCursorMissing` — no CURSOR in storage → starts from journal_id 0, no error
  - [ ] `testStorageFailureRetries` — inject failure on first put, verify retry on next interval
  - [ ] `testCheckpointWritten` — after export, verify checkpoint/latest.json exists with correct fields
  - [ ] `testCheckpointHistoryRetention` — write 105 checkpoints, verify only 100 remain in history
  - [ ] `testStopGracefully` — call stop(), verify thread exits within 10 seconds
  - [ ] `testWrittenBySiteNameInCursor` — CURSOR contains `"written_by": "beijing"`

#### Logging checklist

- [ ] INFO: exporter thread started, including group_id and site_name
- [ ] INFO: exporter thread stopped
- [ ] INFO: cursor recovered on startup (journal_id value)
- [ ] DEBUG: each segment exported (first_journal_id, last_journal_id, segment key, byte size)
- [ ] DEBUG: CURSOR updated (journal_id)
- [ ] DEBUG: checkpoint written (fe_journal_id, fdb_versionstamp, oss_safe_before_ms)
- [ ] WARN: storage write failed, will retry (attempt count, error message)
- [ ] WARN: no CloudMetaSyncPoint found in journal, checkpoint skipped

#### Comments checklist

- [ ] `exportBatch()` — comment: why CURSOR is updated only after segment is confirmed written
- [ ] `recoverCursor()` — comment: why recovery is needed (handles FE master re-election)
- [ ] `writeCheckpoint()` — comment: why oss_safe_before_ms subtracts CRR lag
- [ ] `segmentKey()` — comment: why zero-padded to 10 digits (lexicographic sort = chronological order)
- [ ] `stop()` — comment: why daemon thread is used (JVM exit doesn't wait for it)

---

### Week 5–6: S3JournalCursor + DR FE (DR side)

- [ ] `S3JournalCursor` implements `JournalCursor` — reads segments from bucket
- [ ] Segment buffer — loads one segment at a time into memory, drains before loading next
- [ ] Direction-agnostic — handles segments written by either Beijing or Shanghai (reads `written_by`)
- [ ] Segment deduplication — skips entries with journal_id < nextJournalId (handles re-sends)
- [ ] Polling loop — polls storage every 5 seconds when buffer is empty
- [ ] `--dr-reader-mode` startup flag in `DorisFE.java` — only processed when `enable_replication_group = true`
- [ ] DR FE non-electable — sets `isElectable = false` in BDB-JE, cannot join quorum
- [ ] Auto-standby on startup — reads `replication-group.json`, enters DR mode if not primary site
- [ ] **Write guard in `StmtExecutor.java`** — gated on `enable_replication_group && dr_read_only_mode`
  - [ ] Rejects INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, TRUNCATE, LOAD with clear error message
  - [ ] Error message includes primary cluster endpoint for user to redirect to
  - [ ] SELECT, SHOW, DESC, EXPLAIN pass through unchanged
  - [ ] `isDrWriteCommand()` helper covers all write Command types
- [ ] Integration test: start primary FE, create 5 tables, start DR FE, verify all 5 appear in DR FE catalog within 15s
- [ ] Integration test: DR FE with `dr_read_only_mode=true` — INSERT rejected, SELECT succeeds

#### Unit Tests checklist

- [ ] `S3JournalCursorTest`
  - [ ] `testNextReturnsEntriesInOrder` — pre-populate 3 segments, verify entries returned in journal_id order
  - [ ] `testNextReturnsNullWhenNoData` — empty storage → next() returns null, no exception
  - [ ] `testNextPollsForNewSegments` — add segment after cursor created, verify next() returns it on next poll
  - [ ] `testDeduplicatesResentSegments` — same segment written twice, entries appear only once
  - [ ] `testSkipsEntriesBelowNextJournalId` — cursor starts at id 500, segment has ids 400-600, only 500-600 returned
  - [ ] `testHandlesBothDirections` — segments with `written_by: beijing` and `written_by: shanghai` both read correctly
  - [ ] `testStorageReadFailureReturnsNull` — storage throws on list → returns null, logs WARN, no crash
  - [ ] `testBufferDrainedBeforeNextLoad` — verifies in-memory buffer fully consumed before fetching next segment
- [ ] `DRFEStartupTest`
  - [ ] `testAutoStandbyWhenNotPrimary` — group config says primary=beijing, site=shanghai → DR mode entered
  - [ ] `testNoAutoStandbyWhenPrimary` — group config says primary=beijing, site=beijing → normal mode
  - [ ] `testNoAutoStandbyWhenNoGroupConfig` — no group config in storage → normal mode
  - [ ] `testFeatureDisabledWhenFlagOff` — `enable_replication_group=false` → no DR mode, no exporter
- [ ] `DrReadOnlyGuardTest`
  - [ ] `testInsertRejectedWhenDrReadOnly` — `enable_replication_group=true`, `dr_read_only_mode=true` → INSERT throws UserException
  - [ ] `testUpdateRejectedWhenDrReadOnly` — UPDATE rejected
  - [ ] `testDeleteRejectedWhenDrReadOnly` — DELETE rejected
  - [ ] `testCreateTableRejectedWhenDrReadOnly` — CREATE TABLE rejected
  - [ ] `testDropTableRejectedWhenDrReadOnly` — DROP TABLE rejected
  - [ ] `testTruncateRejectedWhenDrReadOnly` — TRUNCATE rejected
  - [ ] `testSelectAllowedWhenDrReadOnly` — SELECT passes through, no exception
  - [ ] `testShowAllowedWhenDrReadOnly` — SHOW passes through
  - [ ] `testWriteAllowedWhenFlagOff` — `enable_replication_group=false` → INSERT not rejected even if `dr_read_only_mode=true`
  - [ ] `testWriteAllowedWhenReadOnlyOff` — `enable_replication_group=true`, `dr_read_only_mode=false` → writes allowed
  - [ ] `testErrorMessageContainsPrimaryEndpoint` — rejected error message includes Beijing endpoint

#### Logging checklist

- [ ] INFO: DR reader mode activated, including group_id and site_name
- [ ] INFO: auto-standby triggered on startup (primary_site value)
- [ ] DEBUG: segment loaded from storage (segment key, entry count)
- [ ] DEBUG: entry returned (journal_id, op_code)
- [ ] DEBUG: polling for new segments (next_journal_id)
- [ ] WARN: storage read failed, will retry next poll interval
- [ ] WARN: DDL rejected because FE is in DR reader mode (operation type, client IP)

#### Comments checklist

- [ ] `loadNextSegment()` — comment: why we buffer one segment at a time (memory bound)
- [ ] `testSkipsEntries()` — comment: why deduplication is needed (segment may be re-written on exporter retry)
- [ ] DR FE read-only check — comment: why DDL is rejected (applying from S3 ensures consistency)
- [ ] Auto-standby check — comment: why startup check is needed (handles Beijing restart after disaster)

---

## Phase 2 — Replication Manager (Week 7–8)

**Goal:** Operator can set up and monitor a replication group with one command.

### Tasks

- [ ] `replication_manager.py` CLI skeleton — argparse, subcommands, `--dry-run` flag
- [ ] `ReplicationGroupConfig` data class — all fields, JSON serialise/deserialise
- [ ] `FEHttpClient` — HTTP wrapper for FE replication endpoints with timeout + retry
- [ ] `MetaServiceGrpcClient` — gRPC wrapper for `alter_obj_store_info` with timeout
- [ ] `create-group` command — validates config, writes `replication-group.json`, enables exporter on primary FE
- [ ] `show-group` command — reads checkpoint + both-site cursors, renders lag table
- [ ] `status` command — detailed JSON output of all component states
- [ ] `verify` command — pre-flight checks before failover/failback, lists any blockers
- [ ] FE HTTP endpoint `GET /api/replication/status` — returns cursor, lag, exporter state
- [ ] FE HTTP endpoint `GET /api/replication/cursor` — returns current journal_id of this FE

#### Unit Tests checklist

- [ ] `CreateGroupCommandTest`
  - [ ] `testCreatesGroupConfigInStorage` — verify `replication-group.json` written with correct fields
  - [ ] `testEnablesExporterOnPrimaryFE` — verify HTTP call to primary FE made
  - [ ] `testFailsIfBucketUnreachable` — storage error → clear error message, no partial state
  - [ ] `testFailsIfPrimaryFEUnreachable` — FE HTTP error → clear error message
  - [ ] `testDryRunDoesNotWrite` — `--dry-run` → nothing written, prints what would be done
- [ ] `ShowGroupCommandTest`
  - [ ] `testShowsLagFromCheckpoint` — mock checkpoint + cursor → verify lag values correct
  - [ ] `testShowsOKStatus` — lag within threshold → Status column shows OK
  - [ ] `testShowsWARNStatus` — lag above threshold → Status column shows WARN
  - [ ] `testHandlesMissingCheckpoint` — no checkpoint yet → shows "not started" gracefully
- [ ] `VerifyCommandTest`
  - [ ] `testPassesWhenAllHealthy` — all components reachable and in sync → exit code 0
  - [ ] `testFailsWhenDRFENotRunning` — DR FE HTTP unreachable → lists as blocker
  - [ ] `testFailsWhenLagTooHigh` — lag > max_failover_lag → lists as blocker
  - [ ] `testFailsWhenStorageUnreachable` — bucket unreachable → lists as blocker
- [ ] `FEHttpClientTest`
  - [ ] `testTimeoutRetry` — first call times out, second succeeds
  - [ ] `testNon200ResponseThrows` — HTTP 500 → raises FEClientException
- [ ] `MetaServiceGrpcClientTest`
  - [ ] `testAlterVaultSuccess` — mock gRPC returns OK → no exception
  - [ ] `testAlterVaultFailure` — mock gRPC returns error → raises MetaServiceException

#### Logging checklist

- [ ] INFO: each command starts (command name, group_id, dry_run flag)
- [ ] INFO: each command completes successfully
- [ ] INFO: group config written to storage (bucket, key)
- [ ] DEBUG: HTTP request/response (method, URL, status code, duration)
- [ ] DEBUG: gRPC request sent (rpc name, vault name)
- [ ] WARN: lag above warning threshold (component, lag value, threshold)
- [ ] ERROR: command failed (step that failed, error message)

#### Comments checklist

- [ ] `create-group` — comment: why group config is written before enabling exporter (ordering matters)
- [ ] `show-group` lag calculation — comment: why RPO = max of all three lags, not sum
- [ ] `verify` — comment: why verify must be run before every failover (prevents partial state)
- [ ] `FEHttpClient` retry logic — comment: why exponential backoff with max 3 retries

### Acceptance criteria

- [ ] `show-group` reflects actual FE lag within 5 seconds
- [ ] `create-group` with `--dry-run` prints plan and exits 0, writes nothing
- [ ] `verify` exits non-zero when DR FE is not running
- [ ] `verify` exits non-zero when lag > `--max-lag-seconds`

---

## Phase 3 — Failover (Week 9–12)

**Goal:** One command brings Shanghai live in under 60 seconds.

### Week 9–10: FE replication HTTP endpoints

- [ ] `POST /api/replication/pause-export` — pauses `EditLogS3Exporter` thread, returns 200
- [ ] `POST /api/replication/resume-export` — resumes paused exporter, returns 200
- [ ] `POST /api/replication/promote-master` — DR FE → master: stop S3 reader, start BDB master, start exporter
- [ ] `POST /api/replication/enter-dr-mode` — master → DR reader: stop exporter, start S3 cursor
- [ ] `GET /api/replication/cursor` — returns `{"journal_id": N, "site": "shanghai"}`
- [ ] Split-brain guard on promote — rejects if this FE is already a BDB master
- [ ] Split-brain guard on enter-dr-mode — rejects if this FE is already in DR mode

#### Unit Tests checklist

- [ ] `ReplicationEndpointsTest`
  - [ ] `testPauseExport` — exporter running → pause → exporter stops producing segments
  - [ ] `testResumeExport` — exporter paused → resume → exporter resumes from last cursor
  - [ ] `testPromoteMaster` — DR FE → promote → S3 cursor stops, BDB master starts, exporter starts
  - [ ] `testPromoteMasterIdempotent` — already master → returns 200, no side effects
  - [ ] `testPromoteMasterSplitBrainGuard` — promote when already BDB master → returns 409 with message
  - [ ] `testEnterDrMode` — master → enter-dr-mode → exporter stops, S3 cursor starts
  - [ ] `testEnterDrModeAlreadyDR` — already in DR mode → returns 200, no side effects
  - [ ] `testGetCursor` — returns current journal_id and site name

#### Logging checklist

- [ ] INFO: each endpoint called (method, endpoint path, caller IP)
- [ ] INFO: exporter paused (last journal_id at pause time)
- [ ] INFO: promote-master started and completed (time taken)
- [ ] INFO: enter-dr-mode started and completed
- [ ] WARN: split-brain guard triggered (current state, requested state)
- [ ] ERROR: promote-master failed partway (which step failed)

#### Comments checklist

- [ ] `pause-export` handler — comment: why pause is needed before promote (prevents split-brain)
- [ ] `promote-master` handler — comment: why exporter must start before returning 200 (DR side needs segments immediately)
- [ ] Split-brain guard — comment: why idempotency is safe but double-promote is not

---

### Week 11–12: Failover command + end-to-end

- [ ] `failover` command — 7-step sequence with timeout and rollback on failure
- [ ] Step 1: read checkpoint and verify DR FE lag < `max_failover_lag`
- [ ] Step 2: pause primary export (skip if primary unreachable)
- [ ] Step 3: wait for DR FE to reach checkpoint journal_id (max 30s)
- [ ] Step 4: FDB restore to consistent point (fdbbackup mode only; skip if managed)
- [ ] Step 5: remap storage vault via MS gRPC (endpoint + bucket)
- [ ] Step 6: promote DR FE to master via HTTP
- [ ] Step 7: update `replication-group.json` primary_site field
- [ ] Timeout handling per step — each step has configurable timeout, fails fast on exceeded
- [ ] Partial failure handling — if step 5+ fails, prints rollback instructions (manual)

#### Unit Tests checklist

- [ ] `FailoverCommandTest`
  - [ ] `testSuccessfulFailover` — all steps succeed → exit 0, group config updated
  - [ ] `testFailsWhenLagTooHigh` — DR FE too far behind → exits before any changes
  - [ ] `testSkipsPauseWhenPrimaryDown` — primary HTTP unreachable → continues to step 3
  - [ ] `testWaitsForDRFEToCatchUp` — DR FE 2 entries behind → waits, succeeds when caught up
  - [ ] `testTimeoutIfDRFENeverCatchesUp` — DR FE stuck → times out at step 3, no changes past step 2
  - [ ] `testVaultRemappingCalled` — verify MS gRPC called with correct secondary endpoint+bucket
  - [ ] `testGroupConfigUpdated` — verify `primary_site` = secondary site after success
  - [ ] `testDryRun` — `--dry-run` → prints all steps, executes none

#### Logging checklist

- [ ] INFO: failover started (group_id, from_site, to_site, timestamp)
- [ ] INFO: each step started and completed (step number, step name, duration)
- [ ] INFO: failover completed (total duration, new primary site)
- [ ] WARN: primary FE unreachable, skipping pause step
- [ ] WARN: DR FE catching up, waiting (current journal_id, target journal_id)
- [ ] ERROR: failover step failed (step number, error, partial state description)

#### Comments checklist

- [ ] Failover step 2 skip logic — comment: why skipping pause is safe if primary is down (it's already stopped writing)
- [ ] Failover step 3 wait logic — comment: why we wait up to 30s (segment export interval is 5s, network adds latency)
- [ ] Step 5 vault remap — comment: why vault must be remapped before promote-master (FE reads vault on startup)

### Phase 3 Acceptance criteria

- [ ] Failover completes in ≤ 60 seconds with managed FDB
- [ ] All data written before `oss_safe_before_ms` queryable on Shanghai after failover
- [ ] Failover with DR FE > max_lag behind → clear error, nothing changed on either site
- [ ] Failover with primary down → skips pause, completes correctly

---

## Phase 4 — Failback + Auto-Recovery (Week 13–16)

**Goal:** After Beijing recovers, one command returns it as primary. No bulk data copy.

### Week 13–14: Bidirectional CRR + auto-recovery

- [ ] Ops guide: configure bidirectional CRR on OSS, S3, GCS (one section per provider)
- [ ] Auto-standby on Beijing restart — reads `replication-group.json`, enters DR mode if not primary
- [ ] `S3JournalCursor` direction-agnostic — reads segments written by either site without code change
- [ ] `show-group` monitors both CRR directions (Beijing→Shanghai lag AND Shanghai→Beijing lag)
- [ ] `show-group` shows overall RPO = max of all five lag values (FE, FDB, both CRR directions)

#### Unit Tests checklist

- [ ] `AutoStandbyTest`
  - [ ] `testBeijingEntersDRModeAfterFailover` — group config says primary=shanghai, Beijing starts → DR mode
  - [ ] `testBeijingSkipsDRModeWhenPrimary` — group config says primary=beijing → normal mode
  - [ ] `testCursorResumesFromShanghaiSegments` — Beijing DR cursor reads Shanghai segments correctly
  - [ ] `testShowGroupReportsBidirectionalLag` — mock both CRR lag values, verify both shown in output
  - [ ] `testRPOIsBothCRRMax` — Beijing→Shanghai = 2 min, Shanghai→Beijing = 4 min → RPO = 4 min

#### Logging checklist

- [ ] INFO: auto-standby triggered on startup (who is primary, this site)
- [ ] INFO: cursor resumed reading from other site's segments (site name, starting journal_id)
- [ ] DEBUG: both-direction CRR lag values in show-group polling loop
- [ ] WARN: Shanghai→Beijing CRR lag exceeds threshold (value, threshold)

#### Comments checklist

- [ ] Auto-standby check — comment: why both sites have identical fe.conf (only `replication_site_name` differs)
- [ ] Both-direction CRR lag — comment: why we track Shanghai→Beijing lag (failback readiness)

---

### Week 15–16: Failback command

- [ ] `failback` command — 9-step sequence (mirrors failover with direction reversed)
- [ ] Step 1: verify Beijing FE caught up to Shanghai's checkpoint
- [ ] Step 2: verify Shanghai→Beijing CRR lag < threshold (data already in Beijing OSS)
- [ ] Step 3: pause Shanghai export
- [ ] Step 4: wait for Beijing FE to reach final checkpoint (max 30s)
- [ ] Step 5: FDB restore for Beijing (fdbbackup mode only; from Shanghai's backup)
- [ ] Step 6: remap storage vault back to Beijing endpoint + bucket
- [ ] Step 7: promote Beijing FE to master
- [ ] Step 8: Shanghai FE enters DR mode
- [ ] Step 9: update `replication-group.json` primary_site = "beijing"

#### Unit Tests checklist

- [ ] `FailbackCommandTest`
  - [ ] `testSuccessfulFailback` — all steps succeed → exit 0, group config updated to beijing
  - [ ] `testFailsWhenBeijingNotCaughtUp` — Beijing FE too far behind → exits before any changes
  - [ ] `testFailsWhenCRRLagTooHigh` — Shanghai→Beijing CRR lag > threshold → exits before any changes, message explains why
  - [ ] `testVaultRemappedToPrimary` — verify MS gRPC called with Beijing endpoint+bucket
  - [ ] `testShanghaiEntersDRModeAfterFailback` — verify `enter-dr-mode` called on Shanghai FE
  - [ ] `testGroupConfigUpdatedToPrimary` — verify `primary_site` = "beijing" after success
  - [ ] `testDryRun` — `--dry-run` → prints all steps, executes none
  - [ ] `testEndToEndNoBulkCopy` — integration: failover → insert rows → failback → row count unchanged, no sync step

#### Logging checklist

- [ ] INFO: failback started (group_id, from_site, to_site, timestamp)
- [ ] INFO: CRR lag verified (Shanghai→Beijing lag value)
- [ ] INFO: each step started and completed (step number, name, duration)
- [ ] INFO: failback completed (total duration, new primary site)
- [ ] WARN: Beijing FE still catching up (current vs target journal_id)
- [ ] WARN: CRR lag above threshold, failback blocked until resolved

#### Comments checklist

- [ ] Step 2 CRR verification — comment: why we check CRR lag even though bidirectional (instantaneous check, not historical)
- [ ] Step 5 FDB restore — comment: why we restore to oss_safe_before_ms not now (ensures FDB+OSS consistency)
- [ ] Step 8 Shanghai enters DR — comment: why Shanghai must enter DR before Beijing starts exporting (prevents segment conflicts)

### Phase 4 Acceptance criteria

- [ ] Beijing restarts after failover → auto-enters DR mode without manual action
- [ ] Failback blocked when CRR lag > threshold → clear message explaining what to wait for
- [ ] End-to-end: failover → INSERT 1M rows in Shanghai → failback → all 1M rows present in Beijing
- [ ] Failback RTO ≤ 15 minutes (dominated by FDB restore if fdbbackup; near-zero with managed FDB)

---

## Phase 5 — Hardening (Week 17–20)

**Goal:** Production-ready. Load tested. Runbooks complete.

### Load and stress tests

- [ ] DML load test: 10M rows/sec sustained for 1 hour, verify exporter lag ≤ 10 seconds throughout
- [ ] DDL load test: 1000 CREATE/ALTER/DROP per minute, verify DR FE lag ≤ 30 seconds
- [ ] Combined load: DML + DDL simultaneously, no lag regression
- [ ] Failover under load: run DML while executing failover, verify no data loss up to `oss_safe_before_ms`

### Failure injection tests

- [ ] Replication bucket unreachable for 5 minutes — exporter retries, resumes, no data loss
- [ ] Primary FE master re-election during export — new master recovers CURSOR, continues seamlessly
- [ ] DR FE restart mid-catchup — resumes from CURSOR, no duplicate entries applied
- [ ] Storage returns partial write error — CURSOR not updated, segment retried, no corruption
- [ ] fdbbackup restore fails — failover command fails with clear error before vault is remapped

### Monitoring

- [ ] Prometheus metrics exported from `show-group` polling
  - [ ] `doris_replication_fe_lag_seconds` (gauge)
  - [ ] `doris_replication_fdb_lag_seconds` (gauge)
  - [ ] `doris_replication_crr_lag_seconds{direction="primary_to_dr|dr_to_primary"}` (gauge)
  - [ ] `doris_replication_rpo_seconds` (gauge)
  - [ ] `doris_replication_consistent_point_timestamp` (gauge)
  - [ ] `doris_replication_primary_site` (label)
- [ ] Alert rules for Prometheus/Alertmanager
  - [ ] RPO > 10 minutes → WARNING
  - [ ] RPO > 30 minutes → CRITICAL
  - [ ] DR FE unreachable > 5 minutes → CRITICAL
  - [ ] Exporter thread stopped on primary → CRITICAL

### Documentation

- [ ] `docs/setup-guide.md` — step-by-step for OSS, S3, GCS + bidirectional CRR + fdbbackup config
- [ ] `docs/runbook-failover.md` — operator runbook with decision checklist before failover
- [ ] `docs/runbook-failback.md` — operator runbook with verification steps before failback
- [ ] `docs/upgrade-guide.md` — how to re-apply core patch on Doris upgrade
- [ ] `docs/monitoring.md` — Prometheus dashboard setup and alert thresholds
- [ ] `docs/troubleshooting.md` — common issues and resolutions

### GCS provider

- [ ] `GCSReplicationStorage` passes full provider test suite
- [ ] Integration test on real GCS bucket in CI
- [ ] GCS bidirectional replication setup guide added to `setup-guide.md`

### Final sign-off

- [ ] Full DR drill: failover → 24h simulated workload → failback → zero data loss confirmed
- [ ] Ops team executes failover independently using runbook (no engineer present)
- [ ] Ops team executes failback independently using runbook (no engineer present)
- [ ] All three providers tested end-to-end (S3, OSS, GCS)
- [ ] Core patch applies cleanly to latest Doris release
- [ ] 90%+ line coverage across all new code

---

## Repo Structure

Everything lives in your existing Doris fork. No new repo. No new Maven module.

```
doris/                                    ← existing Doris fork
  fe/
    fe-core/
      src/main/java/org/apache/doris/
        replication/                      ← NEW package (all replication code)
          credentials/
            ReplicationCredentialProvider.java
            ReplicationCredentials.java
            StaticCredentialProvider.java
            InstanceProfileCredentialProvider.java
            AssumeRoleCredentialProvider.java
            WorkloadIdentityCredentialProvider.java
            CredentialRefreshScheduler.java
          storage/
            ReplicationStorageBackend.java
            ReplicationStorageException.java
            S3ReplicationStorage.java
            OSSReplicationStorage.java
            GCSReplicationStorage.java
            LocalReplicationStorage.java
            ReplicationStorageFactory.java
          EditLogS3Exporter.java
          ReplicationConfig.java
          CheckpointData.java
          JournalEntry.java
        journal/
          S3JournalCursor.java            ← NEW (existing package)
        catalog/
          Env.java                        ← MODIFIED (+50 lines)
        DorisFE.java                      ← MODIFIED (+70 lines)
        common/
          Config.java                     ← MODIFIED (+20 lines)
          FeConstants.java                ← MODIFIED (+5 lines)

      src/test/java/org/apache/doris/
        replication/                      ← NEW tests mirror source package
          credentials/
            StaticCredentialProviderTest.java
            AssumeRoleCredentialProviderTest.java
            InstanceProfileCredentialProviderTest.java
            ReplicationStorageFactoryTest.java
          storage/
            ReplicationStorageBackendTest.java
          EditLogS3ExporterTest.java
          S3JournalCursorTest.java
          DRFEStartupTest.java
          ReplicationEndpointsTest.java

  tools/
    replication-manager/                  ← NEW Python CLI tool
      replication_manager.py
      commands/
        create_group.py
        show_group.py
        failover.py
        failback.py
        status.py
        verify.py
      clients/
        fe_http_client.py
        meta_service_grpc_client.py
      tests/
        test_create_group.py
        test_show_group.py
        test_failover.py
        test_failback.py
        test_verify.py
        test_fe_http_client.py
        test_ms_grpc_client.py
      integration/
        test_failover_end_to_end.py
        test_failback_end_to_end.py

  docs/
    replication/                          ← already exists
      DESIGN.md
      IMPLEMENTATION_PLAN.md
```

**`./build.sh --fe` builds all Java replication code automatically.**  
No `pom.xml` changes needed — cloud SDKs already present in `fe-core` for backup/restore.  
Only addition: GCS SDK dependency if GCS provider is needed (one line in `fe-core/pom.xml`).

  manager/
    replication_manager.py
    commands/
      create_group.py
      show_group.py
      failover.py
      failback.py
      status.py
      verify.py
    clients/
      fe_http_client.py
      meta_service_grpc_client.py

  patches/
    s3-journal-cursor.patch

  tests/
    unit/
      ReplicationStorageBackendTest.java
      EditLogS3ExporterTest.java
      S3JournalCursorTest.java
      DRFEStartupTest.java
      ReplicationEndpointsTest.java
      test_create_group.py
      test_show_group.py
      test_failover.py
      test_failback.py
      test_verify.py
      test_fe_http_client.py
      test_ms_grpc_client.py
    integration/
      test_oss_storage.py
      test_s3_storage.py
      test_gcs_storage.py
      test_failover_end_to_end.py
      test_failback_end_to_end.py

  docs/
    setup-guide.md
    runbook-failover.md
    runbook-failback.md
    upgrade-guide.md
    monitoring.md
    troubleshooting.md
```

---

## Core Doris Changes (6 files, ~825 lines)

These are the only upstream files we modify. Everything else is in new files/packages.

```
New files (zero upstream conflict):
  fe/fe-core/.../replication/EditLogS3Exporter.java    ~380 lines
  fe/fe-core/.../replication/credentials/*.java        ~300 lines
  fe/fe-core/.../replication/storage/*.java            ~400 lines
  fe/fe-core/.../journal/S3JournalCursor.java          ~300 lines

Modified files (rebase these on each Doris upgrade):
  fe/fe-core/.../catalog/Env.java                      +50 lines
  fe/fe-core/.../DorisFE.java                          +70 lines
  fe/fe-common/.../common/Config.java                  +20 lines
  fe/fe-core/.../common/FeConstants.java               +5 lines
```

On Doris upgrade: `git rebase upstream/master`, resolve conflicts in 4 files (~30–60 min). All `replication/*` files have zero conflicts — upstream never touches that package.

---

## Risk Register

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| JournalCursor interface changes in new Doris release | Low | Medium | Stable interface; update S3JournalCursor.java in ~30 min |
| OSS CRR lag spikes under high DML | Medium | Medium | RTC reduces to seconds; RPO tracks actual lag |
| FDB restore time exceeds RTO target | Low | High | Use managed FDB (zero restore time) |
| Split-brain during failover | Low | High | pause-export + group config update prevents; split-brain guard on endpoints |
| Bidirectional CRR creates infinite replication loop | Low | High | All providers prevent re-replication natively |
| Engineer unfamiliar with BDB-JE journal internals | Medium | Medium | JournalCursor is the only interface needed; existing follower code is the model |

---

## Definition of Done

### Per task
- [ ] Code written and compiles
- [ ] One-line comment above each method
- [ ] Logging at correct levels (INFO/DEBUG/WARN/ERROR per standard above)
- [ ] Unit tests written and passing
- [ ] 90%+ line coverage on new code

### Per phase
- [ ] All tasks complete per task checklist above
- [ ] Integration test passing on real OSS bucket
- [ ] Code reviewed by at least one other engineer
- [ ] Phase deliverable verified on test cluster

### Full project
- [ ] End-to-end DR drill: failover + 24h workload + failback, zero data loss
- [ ] Ops team executes both failover and failback independently
- [ ] Load test: exporter lag ≤ 10s at peak DML throughput
- [ ] All three providers (S3, OSS, GCS) tested end-to-end
- [ ] Core patch applies cleanly to latest Doris release
- [ ] All runbooks reviewed and signed off by ops team
