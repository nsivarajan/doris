# Failback Runbook — Shanghai → Beijing

**Use when:** Beijing has recovered and should resume as primary.

---

## Pre-conditions (ALL must be true before starting)

```bash
# 1. Beijing hardware confirmed healthy
# 2. Beijing FE is running in DR reader mode and catching up
./replication_manager.py show-group --group-id bj_to_sh

# 3. Run verify — all checks must pass
./replication_manager.py verify --group-id bj_to_sh --max-lag-seconds 120
```

Wait until:
- `FE EditLog lag` is small (< 100 entries)
- `CRR` lags for ALL vault buckets are within threshold
- No active incidents in Beijing

---

## Failback steps

### Step 1 — Confirm Beijing is healthy

```bash
# Beijing FE should be in DR reader mode, catching up
curl -s http://bj-fe-host:8040/api/replication/status | python3 -m json.tool
# exporter_running: false  ← correct, Beijing is DR reader not primary
# dr_read_only_mode: true  ← correct
```

### Step 2 — Check that Beijing OSS has all data

Bidirectional CRR should have already copied everything. Verify via `show-group`:

```bash
./replication_manager.py show-group --group-id bj_to_sh
# All vault CRR lags should be within crr_max_lag_seconds
```

If any vault shows high CRR lag, wait and recheck before proceeding.

### Step 3 — Execute failback

```bash
./replication_manager.py failback --group-id bj_to_sh --to-site beijing
```

Expected output:
```
Step 1/9: Verify beijing FE is caught up
  lag=12 ✓
Step 2/9: Verify CRR lag for all 3 vault(s)
  vault=primary_vault — check doris-shanghai-data→doris-beijing-data CRR ✓
  ...
Step 3/9: Pause secondary (shanghai) export
  Paused ✓
Step 4/9: Wait for beijing FE to reach journal_id=...
  Caught up ✓
Step 5/9: FDB restore for beijing
  ...
Step 6/9: Remap 3 vault(s) back to beijing
  vault=primary_vault → doris-beijing-data ✓
  ...
Step 7/9: Promote beijing FE to master
  promoted ✓
Step 8/9: Secondary (shanghai) enters DR mode
  DR mode entered ✓
Step 9/9: Update group config primary_site=beijing
  Updated ✓

Failback complete! Primary is now beijing.
```

### Step 4 — Verify Beijing is serving correctly

```bash
# Connect to Beijing and verify
mysql -h bj-fe-host -P 9030 -u root -p -e "SELECT count(*) FROM orders;"

# Verify Shanghai is back in read-only DR mode
curl -s http://sh-fe-host:8040/api/replication/status
# dr_read_only_mode: true  ← correct
```

### Step 5 — Update client connection strings

Point application connections back to Beijing FE endpoint.

### Step 6 — Monitor

```bash
watch -n 10 './replication_manager.py show-group --group-id bj_to_sh'
```

Within 5-10 minutes:
- `Current primary` = beijing
- `FE EditLog lag` < 10 seconds
- All CRR lags nominal

---

## Troubleshooting

**Failback blocked: CRR lag too high**
```
Wait for CRR to complete. Check OSS console for replication status.
Large datasets may take longer. Consider scheduling during low-traffic hours.
```

**Failback blocked: Beijing FE not caught up**
```
Check Beijing FE logs:
  tail -f bj-fe-host:/path/to/fe.log | grep "Replication"
If DR replayer is not running, restart Beijing FE:
  ./bin/start_fe.sh  # auto-standby will detect Shanghai is primary
```

**FDB restore fails**
```
Verify fdbbackup is streaming to the replication bucket.
Check: fdbbackup status --cluster-file /etc/foundationdb/fdb.cluster
Restore manually if needed, then re-run failback.
```
