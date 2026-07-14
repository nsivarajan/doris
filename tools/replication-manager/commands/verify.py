# Licensed under Apache License 2.0
"""verify: pre-flight checks before failover or failback."""
import json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from clients.group_config import group_config_key, checkpoint_latest_key, ReplicationGroupConfig
from clients.storage_client import StorageClient
from clients.fe_http_client import FEHttpClient

class VerifyCommand:
    def __init__(self, args):
        self.args = args

    def run(self) -> int:
        storage = StorageClient(bucket=os.environ.get("REPL_BUCKET",""),
                                endpoint=os.environ.get("REPL_ENDPOINT",""))
        cfg_bytes = storage.get(group_config_key(self.args.group_id))
        if not cfg_bytes:
            print("FAIL: replication-group.json not found"); return 1
        config = ReplicationGroupConfig.from_json(cfg_bytes.decode())

        blockers = []
        max_lag = getattr(self.args, "max_lag_seconds", 60)

        # check primary FE reachable
        pfe = FEHttpClient(config.primary_fe)
        if not pfe.is_reachable():
            blockers.append(f"Primary FE unreachable ({config.primary_fe})")

        # check secondary FE reachable
        sfe = FEHttpClient(config.secondary_fe)
        if not sfe.is_reachable():
            blockers.append(f"Secondary FE unreachable ({config.secondary_fe})")
        else:
            status = sfe.get_status().get("data", {})
            if not status.get("enable_replication_group"):
                blockers.append("Secondary FE: enable_replication_group=false")

        # check checkpoint age
        cp_bytes = storage.get(checkpoint_latest_key(self.args.group_id))
        if not cp_bytes:
            blockers.append("No checkpoint found — exporter may not be running")
        else:
            cp = json.loads(cp_bytes.decode())
            age = (time.time()*1000 - cp.get("sampledAtMs",0)) / 1000
            if age > max_lag * 2:
                blockers.append(f"Checkpoint is {age:.0f}s old (expected < {max_lag*2}s)")

        # check vault mappings
        if not config.vault_mappings:
            blockers.append("No vault mappings configured — add with add-vault-mapping")

        if blockers:
            print("VERIFY FAILED — blockers:")
            for b in blockers: print(f"  ✗ {b}")
            return 1
        print("VERIFY PASSED — all checks OK")
        return 0
