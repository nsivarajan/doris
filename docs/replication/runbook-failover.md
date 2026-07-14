# Failover Runbook — Beijing → Shanghai

**Use when:** Beijing cluster is confirmed down and Shanghai must serve all traffic.

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
curl -s http://bj-fe-host:8040/api/replication/status || echo "Beijing unreachable"
```

### Step 2 — Check DR state

```bash
./replication_manager.py show-group --group-id bj_to_sh
```

Verify:
- `FE EditLog lag` — acceptable (< 1000 entries)
- `Consistent point` — recent (< 10 minutes ago)
- `RPO` — note this value — it is your data loss window

### Step 3 — Execute failover

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
  ...
Step 5/7: Remap 3 storage vault(s) to shanghai
  vault=primary_vault → doris-shanghai-data ✓
  ...
Step 6/7: Promote shanghai FE to master
  promoted ✓
Step 7/7: Update replication-group.json primary_site=shanghai
  Updated ✓

Failover complete!
  New primary: shanghai (sh-fe-host:9030)
  Consistent point: 2026-07-14T09:40:30Z
  Data safe up to:  2026-07-14T09:35:30Z
```

### Step 4 — Verify Shanghai is serving correctly

```bash
# Connect to Shanghai and run a test query
mysql -h sh-fe-host -P 9030 -u root -p -e "SHOW DATABASES;"

# Verify write works
mysql -h sh-fe-host -P 9030 -u root -p -e "INSERT INTO test_table VALUES (1, 'test');"
```

### Step 5 — Update client connection strings

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
2. Run the failback procedure immediately (before significant data is written to Shanghai)

```bash
./replication_manager.py failback --group-id bj_to_sh --to-site beijing
```
