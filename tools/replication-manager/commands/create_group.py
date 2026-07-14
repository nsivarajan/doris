# Licensed under Apache License 2.0
"""create-group: one-time replication group setup."""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from clients.group_config import (
    group_config_key, VaultMapping, VaultEndpointConfig,
    ReplicationGroupConfig
)
from clients.storage_client import StorageClient
from clients.fe_http_client import FEHttpClient


class CreateGroupCommand:

    def __init__(self, args):
        self.args = args
        self.dry_run = getattr(args, "dry_run", False)

    def run(self) -> int:
        a = self.args
        print(f"\n{'[DRY-RUN] ' if self.dry_run else ''}Creating replication group: {a.group_id}")

        # parse vault mappings from --vault-map "vault_name=secondary_bucket" flags
        vault_mappings = []
        for mapping_str in (a.vault_map or []):
            if "=" not in mapping_str:
                print(f"ERROR: --vault-map must be in 'vault_name=secondary_bucket' format")
                return 1
            vault_name, secondary_bucket = mapping_str.split("=", 1)
            vault_mappings.append(VaultMapping(
                vault_name=vault_name.strip(),
                primary=VaultEndpointConfig(
                    endpoint=a.primary_endpoint,
                    bucket="",  # discovered from MS in interactive mode
                    credential_type=a.credential_type,
                ),
                secondary=VaultEndpointConfig(
                    endpoint=a.secondary_endpoint,
                    bucket=secondary_bucket.strip(),
                    credential_type=a.credential_type,
                ),
            ))

        config = ReplicationGroupConfig(
            group_id=a.group_id,
            primary_site=a.primary_site,
            primary_fe=a.primary_fe,
            primary_ms=a.primary_ms,
            secondary_site=a.secondary_site,
            secondary_fe=a.secondary_fe,
            secondary_ms=a.secondary_ms,
            storage_type=a.storage_type,
            replication_bucket=a.replication_bucket,
            primary_endpoint=a.primary_endpoint,
            secondary_endpoint=a.secondary_endpoint,
            credential_type=a.credential_type,
            fdb_mode=a.fdb_mode,
            crr_max_lag_seconds=a.crr_max_lag_seconds,
            vault_mappings=vault_mappings,
        )

        print(f"  group_id:      {config.group_id}")
        print(f"  primary:       {config.primary_site} ({config.primary_fe})")
        print(f"  secondary:     {config.secondary_site} ({config.secondary_fe})")
        print(f"  storage:       {config.storage_type} / {config.replication_bucket}")
        print(f"  vault mappings: {len(vault_mappings)}")
        for m in vault_mappings:
            print(f"    {m.vault_name} → {m.secondary.bucket}")

        if self.dry_run:
            print("\n[DRY-RUN] Would write replication-group.json to bucket")
            print(config.to_json())
            return 0

        # write group config to replication bucket
        storage = StorageClient(
            bucket=a.replication_bucket,
            endpoint=a.primary_endpoint,
        )
        key = group_config_key(a.group_id)
        storage.put(key, config.to_json().encode())
        print(f"\n  Written: {key}")

        # enable exporter on primary FE via HTTP
        primary_fe = FEHttpClient(a.primary_fe, timeout=getattr(self.args, "timeout", 60))
        if primary_fe.is_reachable():
            status = primary_fe.get_status()
            data = status.get("data", {})
            if not data.get("enable_replication_group"):
                print(f"\n  Note: set enable_replication_group=true in {a.primary_fe} fe.conf")
            else:
                print(f"\n  Primary FE exporter running: {data.get('exporter_running')}")
        else:
            print(f"\n  Warning: could not reach primary FE at {a.primary_fe}")

        print(f"\nReplication group '{a.group_id}' created successfully.")
        print(f"\nNext steps:")
        print(f"  1. Ensure bidirectional CRR is configured for all vault buckets")
        print(f"  2. Set enable_replication_group=true on primary FE")
        print(f"  3. Add the following to secondary meta_service.conf:")
        print()
        self._print_ms_config(config)
        print()
        print(f"  4. Start secondary FE with --dr-reader-mode")
        print(f"  5. Run: ./replication_manager.py show-group --group-id {a.group_id}")
        return 0

    def _print_ms_config(self, config):
        """Generate the meta_service.conf snippet for the secondary MS."""
        # build vault override string: vault1:endpoint:bucket,vault2:...
        overrides = []
        for m in config.vault_mappings:
            overrides.append(
                f"{m.vault_name}:{m.secondary.endpoint}:{m.secondary.bucket}"
            )
        overrides_str = ",".join(overrides)

        print("  ┌─── secondary meta_service.conf ───────────────────────────────────")
        print(f"  │  enable_replication_group = true")
        print(f"  │  replication_site_name = {config.secondary_site}")
        if overrides_str:
            print(f"  │  replication_vault_overrides = {overrides_str}")
        else:
            print(f"  │  replication_vault_overrides = ")
            print(f"  │    # (no vault mappings yet — run add-vault-mapping)")
        print("  └────────────────────────────────────────────────────────────────────")
