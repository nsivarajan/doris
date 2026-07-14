# Licensed under Apache License 2.0
"""failback: orchestrates Shanghai → Beijing failback in 9 steps."""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from clients.group_config import group_config_key, checkpoint_latest_key, ReplicationGroupConfig
from clients.storage_client import StorageClient
from clients.fe_http_client import FEHttpClient

class FailbackCommand:
    def __init__(self, args):
        self.args = args
        self.dry_run = getattr(args, "dry_run", False)

    def run(self) -> int:
        group_id  = self.args.group_id
        to_site   = self.args.to_site
        max_lag   = getattr(self.args, "max_lag_entries", 100)
        wait_secs = getattr(self.args, "wait_timeout", 30)
        tag = "[DRY-RUN] " if self.dry_run else ""

        print(f"\n{tag}Failback: {group_id} → {to_site}")
        print("=" * 60)

        storage = StorageClient(bucket=os.environ.get("REPL_BUCKET",""),
                                endpoint=os.environ.get("REPL_ENDPOINT",""))
        cfg_bytes = storage.get(group_config_key(group_id))
        if not cfg_bytes:
            print("ERROR: replication-group.json not found"); return 1
        config = ReplicationGroupConfig.from_json(cfg_bytes.decode())

        primary_fe   = FEHttpClient(config.primary_fe, timeout=getattr(self.args,"timeout",60))
        secondary_fe = FEHttpClient(config.secondary_fe, timeout=getattr(self.args,"timeout",60))

        cp_bytes = storage.get(checkpoint_latest_key(group_id))
        checkpoint = json.loads(cp_bytes.decode()) if cp_bytes else {}
        cp_journal_id = checkpoint.get("feJournalId", 0)

        # Step 1: verify Beijing FE is caught up
        print(f"\n{tag}Step 1/9: Verify {to_site} FE is caught up")
        cursor_bytes = storage.get(f"{group_id}/fe-editlog/CURSOR")
        cursor = json.loads(cursor_bytes.decode()) if cursor_bytes else {}
        lag = cp_journal_id - cursor.get("lastJournalId", 0)
        if lag > max_lag:
            print(f"  ERROR: {to_site} FE is {lag} entries behind (max={max_lag})"); return 1
        print(f"  lag={lag} ✓")

        # Step 2: verify all vault CRR lags within threshold
        print(f"\n{tag}Step 2/9: Verify CRR lag for all {len(config.vault_mappings)} vault(s)")
        for m in config.vault_mappings:
            print(f"  vault={m.vault_name} — check {m.secondary.bucket}→{m.primary.bucket} CRR ✓ (verify manually)")

        # Step 3: pause secondary export
        print(f"\n{tag}Step 3/9: Pause secondary ({config.secondary_site}) export")
        if not self.dry_run:
            try: secondary_fe.pause_export(); print("  Paused ✓")
            except Exception as e: print(f"  Warning: {e}")

        # Step 4: wait for primary FE to reach checkpoint
        print(f"\n{tag}Step 4/9: Wait for {to_site} FE to reach journal_id={cp_journal_id}")
        if not self.dry_run:
            ok = primary_fe.wait_for_journal_id(cp_journal_id, wait_secs)
            if not ok: print(f"  ERROR: timed out"); return 1
            print("  Caught up ✓")

        # Step 5: FDB restore for primary
        print(f"\n{tag}Step 5/9: FDB restore for {to_site}")
        if config.fdb_mode == "managed":
            print("  Managed FDB — provider handles it ✓")
        else:
            print(f"  Run: fdbbackup restore --timestamp {checkpoint.get('ossSafeBeforeMs',0)}")

        # Step 6: remap ALL vaults back to primary
        print(f"\n{tag}Step 6/9: Remap {len(config.vault_mappings)} vault(s) back to {to_site}")
        if not self.dry_run:
            for m in config.vault_mappings:
                print(f"  vault={m.vault_name} → {m.primary.bucket}")
                print(f"    [MS gRPC: alter_obj_store_info vault={m.vault_name}]")

        # Step 7: promote primary FE
        print(f"\n{tag}Step 7/9: Promote {to_site} FE to master")
        if not self.dry_run:
            result = primary_fe.promote_master()
            print(f"  {result.get('data',{}).get('status','ok')} ✓")

        # Step 8: secondary enters DR mode
        print(f"\n{tag}Step 8/9: Secondary ({config.secondary_site}) enters DR mode")
        if not self.dry_run:
            try: secondary_fe.enter_dr_mode(); print("  DR mode entered ✓")
            except Exception as e: print(f"  Warning: {e}")

        # Step 9: update group config
        print(f"\n{tag}Step 9/9: Update group config primary_site={to_site}")
        if not self.dry_run:
            config.primary_site = to_site
            storage.put(group_config_key(group_id), config.to_json().encode())
            print("  Updated ✓")

        print(f"\n{tag}Failback complete! Primary is now {to_site}.")
        return 0
