#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Doris Replication Group Manager CLI.

Usage:
  ./replication_manager.py create-group  --group-id ... (one-time setup)
  ./replication_manager.py show-group    --group-id ... (monitor lag)
  ./replication_manager.py verify        --group-id ... (pre-flight check)
  ./replication_manager.py failover      --group-id ... --to-site shanghai
  ./replication_manager.py failback      --group-id ... --to-site beijing
  ./replication_manager.py add-vault-mapping --group-id ... --vault-name ...
  ./replication_manager.py reload-config --group-id ...
"""

import argparse
import sys
import os

# make sub-packages importable when run as a script
sys.path.insert(0, os.path.dirname(__file__))

from commands.create_group import CreateGroupCommand
from commands.show_group   import ShowGroupCommand
from commands.verify       import VerifyCommand
from commands.failover     import FailoverCommand
from commands.failback     import FailbackCommand
from commands.add_vault    import AddVaultMappingCommand
from commands.reload_config import ReloadConfigCommand


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="replication_manager",
        description="Doris Replication Group Manager",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be done without making any changes")
    parser.add_argument("--timeout", type=int, default=60,
                        help="HTTP request timeout in seconds (default: 60)")

    sub = parser.add_subparsers(dest="command", required=True)

    # ── create-group ─────────────────────────────────────────────────────────
    cg = sub.add_parser("create-group", help="One-time replication group setup")
    cg.add_argument("--group-id",               required=True)
    cg.add_argument("--primary-site",           required=True)
    cg.add_argument("--primary-fe",             required=True, help="host:http_port")
    cg.add_argument("--primary-ms",             required=True, help="host:grpc_port")
    cg.add_argument("--secondary-site",         required=True)
    cg.add_argument("--secondary-fe",           required=True, help="host:http_port")
    cg.add_argument("--secondary-ms",           required=True, help="host:grpc_port")
    cg.add_argument("--storage-type",           default="OSS", choices=["OSS", "S3", "GCS"])
    cg.add_argument("--replication-bucket",     required=True)
    cg.add_argument("--primary-endpoint",       required=True)
    cg.add_argument("--secondary-endpoint",     required=True)
    cg.add_argument("--credential-type",        default="instance_profile",
                    choices=["instance_profile", "assume_role", "ak_sk"])
    cg.add_argument("--vault-map",              action="append", metavar="VAULT=BUCKET",
                    help="vault_name=secondary_bucket_name (repeat for each vault)")
    cg.add_argument("--crr-max-lag-seconds",    type=int, default=300)
    cg.add_argument("--fdb-mode",               default="fdbbackup",
                    choices=["managed", "fdbbackup"])
    cg.add_argument("--interactive",            action="store_true",
                    help="Discover vaults from primary MS and prompt for secondary config")

    # ── show-group ────────────────────────────────────────────────────────────
    sg = sub.add_parser("show-group", help="Show replication lag and RPO")
    sg.add_argument("--group-id", required=True)
    sg.add_argument("--bucket",   required=False, help="Override bucket from group config")
    sg.add_argument("--endpoint", required=False, help="Override endpoint")

    # ── verify ────────────────────────────────────────────────────────────────
    v = sub.add_parser("verify", help="Pre-flight checks before failover or failback")
    v.add_argument("--group-id",        required=True)
    v.add_argument("--max-lag-seconds", type=int, default=60,
                   help="Maximum acceptable FE lag before verify passes (default: 60)")

    # ── failover ──────────────────────────────────────────────────────────────
    fo = sub.add_parser("failover", help="Failover primary → secondary")
    fo.add_argument("--group-id",         required=True)
    fo.add_argument("--to-site",          required=True)
    fo.add_argument("--max-lag-entries",  type=int, default=100,
                    help="Abort if DR FE is more than N entries behind (default: 100)")
    fo.add_argument("--wait-timeout",     type=int, default=30,
                    help="Seconds to wait for DR FE to catch up (default: 30)")
    fo.add_argument("--skip-vault-remap", action="store_true",
                    help="Skip vault remapping (useful if vaults already updated)")

    # ── failback ──────────────────────────────────────────────────────────────
    fb = sub.add_parser("failback", help="Failback secondary → primary")
    fb.add_argument("--group-id",         required=True)
    fb.add_argument("--to-site",          required=True)
    fb.add_argument("--max-lag-entries",  type=int, default=100)
    fb.add_argument("--wait-timeout",     type=int, default=30)

    # ── add-vault-mapping ─────────────────────────────────────────────────────
    av = sub.add_parser("add-vault-mapping", help="Add a new vault mapping after initial setup")
    av.add_argument("--group-id",               required=True)
    av.add_argument("--vault-name",             required=True)
    av.add_argument("--secondary-bucket",       required=True)
    av.add_argument("--secondary-endpoint",     required=True)
    av.add_argument("--secondary-credential",   default="instance_profile")

    # ── reload-config ─────────────────────────────────────────────────────────
    rc = sub.add_parser("reload-config",
                        help="Reload replication-group.json on all MS nodes")
    rc.add_argument("--group-id", required=True)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    commands = {
        "create-group":     CreateGroupCommand,
        "show-group":       ShowGroupCommand,
        "verify":           VerifyCommand,
        "failover":         FailoverCommand,
        "failback":         FailbackCommand,
        "add-vault-mapping": AddVaultMappingCommand,
        "reload-config":    ReloadConfigCommand,
    }

    cmd_class = commands.get(args.command)
    if not cmd_class:
        parser.print_help()
        sys.exit(1)

    cmd = cmd_class(args)
    try:
        exit_code = cmd.run()
        sys.exit(exit_code if exit_code is not None else 0)
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
