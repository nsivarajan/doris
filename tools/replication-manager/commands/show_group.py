# Licensed under Apache License 2.0
"""show-group: reads checkpoint and cursor from bucket, displays lag table."""

import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from clients.group_config import (
    checkpoint_latest_key, cursor_key, group_config_key,
    ReplicationGroupConfig
)
from clients.storage_client import StorageClient


class ShowGroupCommand:

    def __init__(self, args):
        self.args = args

    def run(self) -> int:
        group_id = self.args.group_id

        # load group config from bucket to get endpoints
        storage = self._build_storage()
        cfg_bytes = storage.get(group_config_key(group_id))
        if cfg_bytes is None:
            print(f"ERROR: replication-group.json not found in bucket for group={group_id}")
            print("Run 'create-group' first.")
            return 1

        config = ReplicationGroupConfig.from_json(cfg_bytes.decode())

        # read checkpoint
        cp_bytes = storage.get(checkpoint_latest_key(group_id))
        checkpoint = json.loads(cp_bytes.decode()) if cp_bytes else {}

        # read cursor (the primary exporter's progress)
        cursor_bytes = storage.get(cursor_key(group_id))
        cursor = json.loads(cursor_bytes.decode()) if cursor_bytes else {}

        # compute FE lag (entries behind checkpoint)
        cp_journal_id = checkpoint.get("feJournalId", -1)
        cursor_journal_id = cursor.get("lastJournalId", -1)
        fe_lag_entries = cp_journal_id - cursor_journal_id if cp_journal_id >= 0 else -1

        # RPO = time since checkpoint oss_safe_before_ms
        import time
        oss_safe_before_ms = checkpoint.get("ossSafeBeforeMs", 0)
        rpo_seconds = (time.time() * 1000 - oss_safe_before_ms) / 1000 if oss_safe_before_ms else -1

        # display
        primary_site = checkpoint.get("primarySite", config.primary_site)
        consistent_point = checkpoint.get("createdAt", "unknown")

        print(f"\nReplication Group: {group_id}")
        print(f"Current primary:   {primary_site}")
        print()

        # per-vault CRR lag (placeholder — actual CRR lag requires cloud API)
        rows = []
        rows.append(("FE EditLog lag",
                     f"{fe_lag_entries} entries" if fe_lag_entries >= 0 else "unknown",
                     "OK" if fe_lag_entries >= 0 and fe_lag_entries < 1000 else "WARN"))
        rows.append(("Consistent point", consistent_point,
                     "OK" if consistent_point != "unknown" else "WARN"))
        rows.append(("RPO",
                     f"{rpo_seconds:.0f} seconds" if rpo_seconds >= 0 else "unknown",
                     "OK" if 0 <= rpo_seconds < config.crr_max_lag_seconds * 1.5 else "WARN"))
        rows.append(("Vault mappings",
                     f"{len(config.vault_mappings)} configured",
                     "OK" if config.vault_mappings else "WARN"))

        col_w = [max(len(r[0]) for r in rows) + 2,
                 max(len(r[1]) for r in rows) + 2, 6]
        sep = "┌" + "─" * (col_w[0]+2) + "┬" + "─" * (col_w[1]+2) + "┬" + "─" * 8 + "┐"
        hdr = "├" + "─" * (col_w[0]+2) + "┼" + "─" * (col_w[1]+2) + "┼" + "─" * 8 + "┤"
        bot = "└" + "─" * (col_w[0]+2) + "┴" + "─" * (col_w[1]+2) + "┴" + "─" * 8 + "┘"

        print(sep)
        print(f"│ {'Component':<{col_w[0]}} │ {'Value':<{col_w[1]}} │ {'Status':<6} │")
        print(hdr)
        for name, val, status in rows:
            print(f"│ {name:<{col_w[0]}} │ {val:<{col_w[1]}} │ {status:<6} │")
        print(bot)
        print()

        has_warn = any(r[2] == "WARN" for r in rows)
        return 1 if has_warn else 0

    def _build_storage(self):
        """Build a storage client from CLI args or env vars."""
        return StorageClient(
            bucket=getattr(self.args, "bucket", None) or os.environ.get("REPL_BUCKET", ""),
            endpoint=getattr(self.args, "endpoint", None) or os.environ.get("REPL_ENDPOINT", ""),
        )
