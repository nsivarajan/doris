# Failover Runbook — Beijing → Shanghai

**Use when:** Beijing cluster is confirmed down and Shanghai must serve all traffic.

> **Port note:** FE API calls use port `8030` (HTTP) or `8050` (HTTPS).
> Add `--use-https` to all `replication_manager.py` commands if your cluster
> has `enable_https=true`. Add `--ca-cert /path/to/ca.crt` for internal CAs.

---

## Pre-flight checklist (run before declaring disaster)

```bash
./replication_manager.py verify --group-id bj_to_sh --max-lag-seconds 120
```

All items must pass. If any fail, investigate before proceeding.

---

## Decision criteria

Do NOT failover for:
- Network blip < 5 minutes (wait and recheck)
- Single FE node failure (internal HA handles it)
- Planned maintenance (use planned failover procedure instead)

DO failover for:
- All Beijing FE nodes unreachable > 10 minutes
- Beijing datacenter network partition
- Beijing hardware failure affecting majority of FE nodes

---

## Failover steps

### Step 1 — Confirm Beijing is down

```bash
# Try to reach Beijing FE — should fail
# HTTP cluster (enable_https=false):
curl -s http://bj-fe-host:8030/api/replication/status || echo "Beijing unreachable"

# HTTPS cluster (enable_https=true):
curl -s https://bj-fe-host:8050/api/replication/status || echo "Beijing unreachable"
# With internal CA cert:
curl -s --cacert /path/to/ca.crt https://bj-fe-host:8050/api/replication/status
```

### Step 2 — Check DR state

```bash
./replication_manager.py show-group --group-id bj_to_sh
```

Verify:
- `FE EditLog lag` — acceptable (< 1000 entries)
- `Consistent point` — recent (< 10 minutes ago)
- `RPO` — note this value — it is your data loss window

### Step 3 — Execute failover command

```bash
./replication_manager.py failover --group-id bj_to_sh --to-site shanghai
```

Expected output:
```
Step 1/7: Verify DR FE lag is within threshold
  DR FE lag=23 entries ✓
Step 2/7: Pause primary (beijing) export
  Warning: could not pause primary (may be down): ...   ← expected in disaster
  Continuing
Step 3/7: Wait for DR FE to reach journal_id=...
  Caught up ✓
Step 4/7: FDB restore
  Run: fdbbackup restore --timestamp <oss_safe_before_ms>
  Then reconfigure MS to point to secondary FDB cluster
  [Manual step — complete FDB restore before continuing]
Step 5/7: Remap 3 storage vault(s) to shanghai
  vault=primary_vault → doris-shanghai-data ✓
  ...
Step 6/7: Promote shanghai FE to master
  promoted ✓
Step 7/7: Update replication-group.json primary_site=shanghai
  Updated ✓
```

### Step 4 — ⚠️ MANUAL: Switch Shanghai MS to Shanghai FDB

**This step is required when `fdb_mode=fdbbackup`. Do not skip.**

After fdbbackup restore completes, Shanghai FDB now has Beijing's data.
Switch Shanghai Meta Service to use local Shanghai FDB instead of Beijing FDB:

```bash
# On ALL Shanghai MS nodes — edit meta_service.conf
vim /path/to/conf/meta_service.conf

# Change:
#   fdb_cluster_file_path = /etc/foundationdb/beijing-fdb.cluster
# To:
#   fdb_cluster_file_path = /etc/foundationdb/fdb.cluster   ← Shanghai FDB

# Restart MS on all Shanghai nodes
./bin/stop_ms.sh && ./bin/start_ms.sh --daemon

# Verify MS is healthy
curl -s http://sh-ms-host:5000/MetaService/version
```

Until this step is done, Shanghai BE nodes cannot read rowset metadata
and queries will fail.

### Step 5 — Resume failover command if it was paused at Step 4

```bash
# If the failover command was waiting, continue it now
# OR re-run — it is idempotent from step 5 onward
./replication_manager.py failover --group-id bj_to_sh --to-site shanghai
```

### Step 6 — Verify Shanghai is serving correctly

```bash
# Connect to Shanghai and run a test query
mysql -h sh-fe-host -P 9030 -u root -p -e "SHOW DATABASES;"

# Verify write works
mysql -h sh-fe-host -P 9030 -u root -p -e "INSERT INTO test_table VALUES (1, 'test');"
```

### Step 7 — Update client connection strings

Point all application connection strings to Shanghai FE endpoint.

---

## Monitoring after failover

```bash
# Watch lag until stable
watch -n 10 './replication_manager.py show-group --group-id bj_to_sh'
```

Prometheus alert to watch: `doris_replication_exporter_running{site="shanghai"}` should be 1.

---

## Rollback (if failover was a mistake)

If Shanghai was successfully promoted but you want to undo:
1. Beijing must be reachable
2. Switch Shanghai MS back to Beijing FDB (reverse of Step 4 above)
3. Run the failback procedure immediately (before significant data is written to Shanghai)

```bash
./replication_manager.py failback --group-id bj_to_sh --to-site beijing
```
