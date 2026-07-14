# Licensed under Apache License 2.0
"""add-vault-mapping: add a new vault after initial setup."""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from clients.group_config import group_config_key, VaultMapping, VaultEndpointConfig, ReplicationGroupConfig
from clients.storage_client import StorageClient

class AddVaultMappingCommand:
    def __init__(self, args): self.args = args
    def run(self) -> int:
        a = self.args
        storage = StorageClient(bucket=os.environ.get("REPL_BUCKET",""),
                                endpoint=os.environ.get("REPL_ENDPOINT",""))
        cfg_bytes = storage.get(group_config_key(a.group_id))
        if not cfg_bytes:
            print("ERROR: replication-group.json not found"); return 1
        config = ReplicationGroupConfig.from_json(cfg_bytes.decode())
        new_mapping = VaultMapping(
            vault_name=a.vault_name,
            primary=VaultEndpointConfig(endpoint=config.primary_endpoint, bucket=""),
            secondary=VaultEndpointConfig(
                endpoint=a.secondary_endpoint,
                bucket=a.secondary_bucket,
                credential_type=getattr(a, "secondary_credential", "instance_profile"),
            ),
        )
        config.vault_mappings.append(new_mapping)
        storage.put(group_config_key(a.group_id), config.to_json().encode())
        print(f"Added vault mapping: {a.vault_name} → {a.secondary_bucket}")
        print("Run 'reload-config' to apply on all MS nodes")
        return 0

class ReloadConfigCommand:
    def __init__(self, args): self.args = args
    def run(self) -> int:
        print(f"Reload config for group={self.args.group_id}")
        print("Send SIGHUP to all Meta Service processes on secondary site")
        print("  kill -HUP $(pidof meta_service)")
        return 0
