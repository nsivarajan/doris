# Licensed under Apache License 2.0
"""
FE HTTP API client for replication manager.
Wraps calls to /api/replication/* endpoints with timeout and retry.
"""

import json
import time
import urllib.request
import urllib.error
from typing import Optional, Dict, Any


class FEHttpClient:
    """Simple HTTP client for Doris FE replication endpoints."""

    def __init__(self, host_port: str, timeout: int = 30,
                 username: str = "admin", password: str = ""):
        """
        host_port: "hostname:8030" (FE HTTP port)
        """
        self.base_url = f"http://{host_port}"
        self.timeout = timeout
        self.username = username
        self.password = password

    def get_status(self) -> Dict[str, Any]:
        """GET /api/replication/status — returns exporter state."""
        return self._get("/api/replication/status")

    def get_cursor(self) -> Dict[str, Any]:
        """GET /api/replication/cursor — returns last exported journal_id."""
        return self._get("/api/replication/cursor")

    def pause_export(self) -> Dict[str, Any]:
        """POST /api/replication/pause-export — pause before failover."""
        return self._post("/api/replication/pause-export")

    def promote_master(self) -> Dict[str, Any]:
        """POST /api/replication/promote-master — promote DR FE to master."""
        return self._post("/api/replication/promote-master")

    def enter_dr_mode(self) -> Dict[str, Any]:
        """POST /api/replication/enter-dr-mode — transition back to DR reader."""
        return self._post("/api/replication/enter-dr-mode")

    def is_reachable(self) -> bool:
        """Returns True if the FE HTTP endpoint responds to a status call."""
        try:
            self.get_status()
            return True
        except Exception:
            return False

    def wait_for_journal_id(self, target_journal_id: int,
                            timeout_sec: int = 30) -> bool:
        """
        Polls get_cursor() until last_exported_journal_id >= target_journal_id.
        Returns True if caught up within timeout, False otherwise.
        """
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            try:
                cursor = self.get_cursor()
                current = cursor.get("data", {}).get("last_exported_journal_id", -1)
                if current >= target_journal_id:
                    return True
                remaining = int(deadline - time.time())
                print(f"  Waiting for FE to reach journal_id={target_journal_id} "
                      f"(current={current}, {remaining}s remaining)...")
            except Exception as e:
                print(f"  Warning: cursor poll failed: {e}")
            time.sleep(2)
        return False

    # ── internals ─────────────────────────────────────────────────────────────

    def _get(self, path: str) -> Dict[str, Any]:
        url = self.base_url + path
        req = urllib.request.Request(url)
        self._add_auth(req)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"HTTP {e.code} from {url}: {e.read().decode()}") from e
        except Exception as e:
            raise RuntimeError(f"Request failed {url}: {e}") from e

    def _post(self, path: str, data: Optional[bytes] = None) -> Dict[str, Any]:
        url = self.base_url + path
        req = urllib.request.Request(url, data=data or b"", method="POST")
        req.add_header("Content-Type", "application/json")
        self._add_auth(req)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"HTTP {e.code} from {url}: {e.read().decode()}") from e
        except Exception as e:
            raise RuntimeError(f"Request failed {url}: {e}") from e

    def _add_auth(self, req: urllib.request.Request):
        """Add HTTP Basic auth if credentials are configured."""
        if self.username:
            import base64
            token = base64.b64encode(
                f"{self.username}:{self.password}".encode()).decode()
            req.add_header("Authorization", f"Basic {token}")
