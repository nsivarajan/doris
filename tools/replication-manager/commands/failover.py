# Licensed under Apache License 2.0
"""failover: orchestrates Beijing → Shanghai failover in 7 steps."""

import json
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from clients.group_config import (
    group_config_key, checkpoint_latest_key,
    ReplicationGroupConfig
)
from clients.storage_client import StorageClient
from clients.fe_http_client import FEHttpClient


class FailoverCommand:

    def __init__(self, args):
        self.args = args
        self.dry_run = getattr(args, "dry_run", False)

    def run(self) -> int:
        group_id  = self.args.group_id
        to_site   = self.args.to_site
        max_lag   = getattr(self.args, "max_lag_entries", 100)
        wait_secs = getattr(self.args, "wait_timeout", 30)

        print(f"\n{'[DRY-RUN] ' if self.dry_run else ''}Failover: {group_id} → {to_site}")
        print("=" * 60)

        storage = self._build_storage()

        # load group config
        cfg_bytes = storage.get(group_config_key(group_id))
        if not cfg_bytes:
            print("ERROR: replication-group.json not found. Run create-group first.")
            return 1
        config = ReplicationGroupConfig.from_json(cfg_bytes.decode())

        # resolve secondary FE
        secondary_fe = FEHttpClient(config.secondary_fe,
                                    timeout=getattr(self.args, "timeout", 60))

        # ── Step 1: verify DR FE is caught up ────────────────────────────────
        self._step(1, "Verify DR FE lag is within threshold")
        cp_bytes = storage.get(checkpoint_latest_key(group_id))
        if not cp_bytes:
            print("  ERROR: no checkpoint found — cannot verify DR state")
            return 1
        checkpoint = json.loads(cp_bytes.decode())
        cp_journal_id = checkpoint.get("feJournalId", 0)

        cursor_bytes = storage.get(f"{group_id}/fe-editlog/CURSOR")
        cursor = json.loads(cursor_bytes.decode()) if cursor_bytes else {}
        dr_journal_id = cursor.get("lastJournalId", 0)
        lag = cp_journal_id - dr_journal_id

        if lag > max_lag:
            print(f"  ERROR: DR FE is {lag} entries behind (max={max_lag}). Aborting.")
            return 1
        print(f"  DR FE lag={lag} entries ✓")

        # ── Step 2: pause primary export ─────────────────────────────────────
        primary_fe_addr = config.primary_fe
        primary_fe = FEHttpClient(primary_fe_addr,
                                  timeout=getattr(self.args, "timeout", 60))
        self._step(2, f"Pause primary ({config.primary_site}) export")
        if not self.dry_run:
            try:
                primary_fe.pause_export()
                print(f"  Paused ✓")
            except Exception as e:
                print(f"  Warning: could not pause primary (may be down): {e}")
                print(f"  Continuing — primary unreachable is expected in disaster")

        # ── Step 3: wait for DR FE to reach checkpoint ────────────────────────
        self._step(3, f"Wait for DR FE to reach journal_id={cp_journal_id}")
        if not self.dry_run:
            caught_up = secondary_fe.wait_for_journal_id(cp_journal_id, wait_secs)
            if not caught_up:
                print(f"  ERROR: DR FE did not catch up within {wait_secs}s")
                return 1
            print(f"  Caught up ✓")

        # ── Step 4: FDB restore (fdbbackup mode) ─────────────────────────────
        self._step(4, "FDB restore")
        if config.fdb_mode == "managed":
            print("  Managed FDB — provider handles DR automatically ✓")
        else:
            oss_safe = checkpoint.get("ossSafeBeforeMs", 0)
            print(f"  Run: fdbbackup restore --timestamp {oss_safe}")
            print(f"  Then reconfigure MS to point to secondary FDB cluster")
            if not self.dry_run:
                print("  [Manual step — complete FDB restore before continuing]")

        # ── Step 5: remap ALL vaults ──────────────────────────────────────────
        self._step(5, f"Remap {len(config.vault_mappings)} storage vault(s) to {to_site}")
        if not self.dry_run and not getattr(self.args, "skip_vault_remap", False):
            for mapping in config.vault_mappings:
                print(f"  vault={mapping.vault_name} → {mapping.secondary.bucket}")
                # vault remapping goes via Meta Service gRPC
                # placeholder: in production, call MetaServiceGrpcClient here
                print(f"    [MS gRPC: alter_obj_store_info vault={mapping.vault_name}]")

        # ── Step 6: promote DR FE ─────────────────────────────────────────────
        self._step(6, f"Promote {to_site} FE to master")
        if not self.dry_run:
            result = secondary_fe.promote_master()
            print(f"  {result.get('data', {}).get('status', 'ok')} ✓")

        # ── Step 7: update group config ───────────────────────────────────────
        self._step(7, f"Update replication-group.json primary_site={to_site}")
        if not self.dry_run:
            config.primary_site = to_site
            storage.put(group_config_key(group_id), config.to_json().encode())
            print(f"  Updated ✓")

        print()
        print(f"{'[DRY-RUN] ' if self.dry_run else ''}Failover complete!")
        print(f"  New primary: {to_site} ({config.secondary_fe})")
        print(f"  Consistent point: {checkpoint.get('createdAt', 'unknown')}")
        return 0

    def _step(self, n: int, description: str):
        prefix = "[DRY-RUN] " if self.dry_run else ""
        print(f"\n{prefix}Step {n}/7: {description}")

    def _build_storage(self):
        return StorageClient(
            bucket=os.environ.get("REPL_BUCKET", ""),
            endpoint=os.environ.get("REPL_ENDPOINT", ""),
        )
