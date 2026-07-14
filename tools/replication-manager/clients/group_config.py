# Licensed under Apache License 2.0
"""
Shared group config model: reads/writes replication-group.json from the bucket.
"""

import json
from dataclasses import dataclass, field, asdict
from typing import List, Optional


@dataclass
class VaultEndpointConfig:
    endpoint: str
    bucket: str
    credential_type: str = "instance_profile"
    role_arn: str = ""


@dataclass
class VaultMapping:
    vault_name: str
    primary: VaultEndpointConfig
    secondary: VaultEndpointConfig


@dataclass
class ReplicationGroupConfig:
    group_id: str
    primary_site: str
    primary_fe: str        # host:port
    primary_ms: str        # host:grpc_port
    secondary_site: str
    secondary_fe: str
    secondary_ms: str
    storage_type: str
    replication_bucket: str
    primary_endpoint: str
    secondary_endpoint: str
    credential_type: str
    fdb_mode: str
    crr_max_lag_seconds: int
    vault_mappings: List[VaultMapping] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, text: str) -> "ReplicationGroupConfig":
        d = json.loads(text)
        mappings = []
        for m in d.get("vault_mappings", []):
            mappings.append(VaultMapping(
                vault_name=m["vault_name"],
                primary=VaultEndpointConfig(**m["primary"]),
                secondary=VaultEndpointConfig(**m["secondary"]),
            ))
        d["vault_mappings"] = mappings
        return cls(**d)


def group_config_key(group_id: str) -> str:
    return f"{group_id}/replication-group.json"


def checkpoint_latest_key(group_id: str) -> str:
    return f"{group_id}/checkpoint/latest.json"


def cursor_key(group_id: str) -> str:
    return f"{group_id}/fe-editlog/CURSOR"
