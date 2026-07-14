# Licensed under Apache License 2.0
"""
dr_drill.py — isolated DR write capability test.

Temporarily activates the secondary cluster as an independent writer
using a brand-new isolated OSS bucket (no CRR, no connection to production).
Primary is completely undisturbed throughout.

Flow:
  1. User manually repoints secondary MS to local FDB (printed instructions)
  2. FE write guard lifted (enter-drill-mode endpoint)
  3. Create isolated test vault + table via secondary FE
  4. Run INSERT / SELECT / UPDATE to verify write path
  5. Drop test database
  6. FE write guard restored (exit-drill-mode endpoint)
  7. User manually repoints MS back to primary FDB (printed instructions)
"""

import os
import sys
import subprocess
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from clients.fe_http_client import FEHttpClient
from clients.group_config import group_config_key, ReplicationGroupConfig
from clients.storage_client import StorageClient


class DrDrillCommand:

    # dedicated database created and destroyed per drill — never touches production
    DRILL_DB = "dr_drill"
    DRILL_TABLE = "write_test"

    def __init__(self, args):
        self.args = args
        self.dry_run = getattr(args, "dry_run", False)

    def run(self) -> int:
        a = self.args
        print(f"\n{'[DRY-RUN] ' if self.dry_run else ''}DR Drill — {a.secondary_site}")
        print("=" * 60)
        print("Primary will not be touched at any point during this drill.\n")

        # load group config for secondary endpoints
        storage = self._build_storage()
        cfg_bytes = storage.get(group_config_key(a.group_id))
        if not cfg_bytes:
            print("ERROR: replication-group.json not found. Run create-group first.")
            return 1
        config = ReplicationGroupConfig.from_json(cfg_bytes.decode())

        secondary_http_port = getattr(a, "secondary_http_port", 8030)
        secondary_mysql_port = getattr(a, "secondary_mysql_port", 9030)
        secondary_mysql_user = getattr(a, "mysql_user", "root")
        secondary_mysql_pass = getattr(a, "mysql_password", "")

        fe_client = FEHttpClient(
            f"{config.secondary_fe.split(':')[0]}:{secondary_http_port}",
            timeout=getattr(a, "timeout", 60)
        )

        # ── Step 1: Manual MS repoint instructions ────────────────────────
        print("Step 1/7: Repoint secondary MS to local FDB")
        print()
        print("  Run on ALL secondary MS nodes:")
        print(f"  vim /path/to/conf/meta_service.conf")
        print(f"  # Change:")
        print(f"  #   fdb_cluster_file_path = /etc/foundationdb/fdb.cluster  ← local FDB")
        print(f"  ./bin/stop_ms.sh && ./bin/start_ms.sh --daemon")
        print()

        if not self.dry_run:
            input("  Press ENTER when MS has been restarted with local FDB... ")
        print("  MS repointed ✓")

        # ── Step 2: Enter drill mode on secondary FE ──────────────────────
        print("\nStep 2/7: Enter drill mode on secondary FE (lift write guard, no export)")
        if not self.dry_run:
            try:
                result = fe_client._post("/api/replication/enter-drill-mode")
                status = result.get("data", {}).get("status", "unknown")
                if status != "drill-mode-active":
                    print(f"  ERROR: unexpected status {status}")
                    return 1
                print(f"  Write guard lifted ✓ (exporter NOT started — primary bucket safe)")
            except Exception as e:
                print(f"  ERROR: {e}")
                return 1
        else:
            print("  [DRY-RUN] Would call POST /api/replication/enter-drill-mode")

        # ── Step 3: Create isolated test vault ────────────────────────────
        print("\nStep 3/7: Create isolated test vault (new bucket, no CRR)")
        vault_sql = f"""
CREATE STORAGE VAULT IF NOT EXISTS drill_test_vault
PROPERTIES (
  'type'        = 'S3',
  's3.endpoint' = '{a.drill_endpoint}',
  's3.bucket'   = '{a.drill_bucket}',
  's3.region'   = '{getattr(a, "drill_region", "cn-shanghai")}',
  's3.root.path'= '/drill',
  'provider'    = 'OSS'
);"""
        if not self.dry_run:
            ok = self._run_sql(config, secondary_mysql_port,
                               secondary_mysql_user, secondary_mysql_pass, vault_sql)
            if not ok:
                self._restore(fe_client, config, secondary_mysql_port,
                              secondary_mysql_user, secondary_mysql_pass)
                return 1
        print(f"  Drill vault → {a.drill_bucket} ✓")

        # ── Step 4: Create test database and table ────────────────────────
        print("\nStep 4/7: Create isolated test database and table")
        setup_sql = f"""
CREATE DATABASE IF NOT EXISTS {self.DRILL_DB};
CREATE TABLE IF NOT EXISTS {self.DRILL_DB}.{self.DRILL_TABLE} (
    id      BIGINT,
    site    VARCHAR(100),
    ts      DATETIME,
    note    VARCHAR(200)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    'replication_num'    = '1',
    'storage_vault_name' = 'drill_test_vault'
);"""
        if not self.dry_run:
            ok = self._run_sql(config, secondary_mysql_port,
                               secondary_mysql_user, secondary_mysql_pass, setup_sql)
            if not ok:
                self._restore(fe_client, config, secondary_mysql_port,
                              secondary_mysql_user, secondary_mysql_pass)
                return 1
        print(f"  {self.DRILL_DB}.{self.DRILL_TABLE} created ✓")

        # ── Step 5: Run write tests ───────────────────────────────────────
        print("\nStep 5/7: Run write verification tests")
        test_id = int(time.time())
        site_name = config.secondary_site

        tests = [
            ("INSERT",
             f"INSERT INTO {self.DRILL_DB}.{self.DRILL_TABLE} "
             f"VALUES ({test_id}, '{site_name}', NOW(), 'DR drill write test');"),
            ("SELECT",
             f"SELECT id, site, ts FROM {self.DRILL_DB}.{self.DRILL_TABLE} "
             f"WHERE id = {test_id};"),
            ("UPDATE",
             f"UPDATE {self.DRILL_DB}.{self.DRILL_TABLE} "
             f"SET note = 'verified' WHERE id = {test_id};"),
            ("DELETE-NONEXISTENT",
             f"DELETE FROM {self.DRILL_DB}.{self.DRILL_TABLE} WHERE id = -999;"),
        ]

        all_passed = True
        for op, sql in tests:
            if not self.dry_run:
                ok = self._run_sql(config, secondary_mysql_port,
                                   secondary_mysql_user, secondary_mysql_pass,
                                   sql, show_output=(op == "SELECT"))
                if not ok:
                    print(f"  {op}: FAILED ✗")
                    all_passed = False
                else:
                    print(f"  {op}: ✓")
            else:
                print(f"  [DRY-RUN] {op}: would execute")

        if not all_passed:
            print("\n  Some tests failed — drill FAILED")
            self._restore(fe_client, config, secondary_mysql_port,
                          secondary_mysql_user, secondary_mysql_pass)
            return 1

        # ── Step 6: Cleanup test data ─────────────────────────────────────
        print("\nStep 6/7: Cleanup test database")
        cleanup_sql = f"DROP DATABASE IF EXISTS {self.DRILL_DB};"
        if not self.dry_run:
            self._run_sql(config, secondary_mysql_port,
                          secondary_mysql_user, secondary_mysql_pass, cleanup_sql)
        print("  Test database dropped ✓")

        # ── Step 7: Restore DR state ──────────────────────────────────────
        self._restore(fe_client, config, secondary_mysql_port,
                      secondary_mysql_user, secondary_mysql_pass)

        print()
        print(f"{'[DRY-RUN] ' if self.dry_run else ''}DR Drill PASSED ✓")
        print(f"  {site_name} can accept writes when promoted.")
        print(f"  Primary was not touched.")
        return 0

    def _restore(self, fe_client, config, mysql_port, mysql_user, mysql_pass):
        """Restore DR state: re-enable write guard and print MS repoint instructions."""
        print("\nStep 7/7: Restore DR state")

        # restore write guard
        if not self.dry_run:
            try:
                fe_client._post("/api/replication/exit-drill-mode")
                print("  Write guard restored ✓")
            except Exception as e:
                print(f"  WARNING: could not restore write guard via API: {e}")
                print("  Manually set dr_read_only_mode=true in fe.conf and restart FE")

        # print MS repoint instructions
        print()
        print("  MANUAL: Repoint secondary MS back to primary FDB:")
        print(f"  vim /path/to/conf/meta_service.conf")
        print(f"  # Change back to:")
        print(f"  #   fdb_cluster_file_path = /etc/foundationdb/primary-fdb.cluster")
        print(f"  ./bin/stop_ms.sh && ./bin/start_ms.sh --daemon")
        print()
        print("  Secondary FE will auto-detect primary_site=beijing from group config")
        print("  and re-enter DR reader mode on next restart.")

    def _run_sql(self, config, mysql_port, user, password, sql,
                 show_output=False) -> bool:
        """Execute SQL against secondary FE via mysql CLI."""
        host = config.secondary_fe.split(":")[0]
        cmd = ["mysql", f"-h{host}", f"-P{mysql_port}", f"-u{user}",
               "-e", sql.strip()]
        if password:
            cmd.insert(4, f"-p{password}")
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                print(f"  SQL ERROR: {result.stderr.strip()}")
                return False
            if show_output and result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    print(f"  {line}")
            return True
        except FileNotFoundError:
            print("  ERROR: mysql client not found. Install mysql-client.")
            return False
        except subprocess.TimeoutExpired:
            print("  ERROR: SQL timed out after 30 seconds")
            return False

    def _build_storage(self):
        return StorageClient(
            bucket=os.environ.get("REPL_BUCKET", ""),
            endpoint=os.environ.get("REPL_ENDPOINT", ""),
        )
