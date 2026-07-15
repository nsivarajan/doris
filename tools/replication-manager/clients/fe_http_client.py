# Licensed under Apache License 2.0
"""
FE HTTP/HTTPS API client for replication manager.

Doris FE HTTPS setup (from InternalHttpsUtils.java on Auditlog-on-https branch):
  - FE uses a Java KeyStore (JKS/PKCS12) at Config.key_store_path
  - Trust model: all certs in keystore chain are trusted (leaf + intermediate + CA)
  - Hostname verification: DISABLED (NoopHostnameVerifier) — internal certs may
    not match hostnames exactly

For this Python client, the equivalent is:
  - Export the CA cert from the FE keystore to a PEM file:
      keytool -export -keystore $DORIS_HOME/conf/ssl/doris_keystore.jks \\
              -alias doris_ssl_certificate -file /tmp/doris-ca.crt -rfc
  - Pass --ca-cert /tmp/doris-ca.crt to the CLI
  - Hostname verification is disabled by default (matching Doris behaviour)

Port reference:
  HTTP  cluster (enable_https=false): port 8030 (Config.http_port)
  HTTPS cluster (enable_https=true):  port 8050 (Config.https_port)
"""

import json
import ssl
import time
import urllib.request
import urllib.error
from typing import Optional, Dict, Any


class FEHttpClient:
    """
    HTTP/HTTPS client for Doris FE replication endpoints.
    Mirrors the trust model of Doris's own InternalHttpsUtils:
      - Loads CA from keystore-exported PEM (--ca-cert)
      - Hostname verification disabled (internal certs don't always match)
    """

    def __init__(self, host_port: str, timeout: int = 30,
                 username: str = "admin", password: str = "",
                 use_https: bool = False,
                 ca_cert: Optional[str] = None,
                 verify_ssl: bool = True):
        """
        host_port:   "hostname:8030" or "hostname:8050"
        use_https:   True when FE has enable_https=true (port 8050)
        ca_cert:     Path to PEM CA cert exported from FE keystore.
                     Export with:
                       keytool -export -keystore $DORIS_HOME/conf/ssl/doris_keystore.jks
                               -alias doris_ssl_certificate -file ca.crt -rfc
        verify_ssl:  True = validate cert against ca_cert (recommended).
                     False = skip all validation (dev/test only).
                     Note: hostname verification is always disabled to match
                     Doris's own NoopHostnameVerifier behaviour.
        """
        scheme = "https" if use_https else "http"
        self.base_url = f"{scheme}://{host_port}"
        self.timeout = timeout
        self.username = username
        self.password = password
        self.use_https = use_https
        self.ca_cert = ca_cert
        self.verify_ssl = verify_ssl
        self._ssl_ctx = self._build_ssl_context() if use_https else None

    def _build_ssl_context(self) -> ssl.SSLContext:
        """
        Build SSL context matching Doris InternalHttpsUtils:
          - Load CA cert from keystore-exported PEM
          - Disable hostname check (NoopHostnameVerifier equivalent)
        """
        if not self.verify_ssl:
            # dev/test only — skip all validation
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            return ctx

        ctx = ssl.create_default_context()
        if self.ca_cert:
            # load CA cert exported from FE keystore
            ctx.load_verify_locations(cafile=self.ca_cert)
        # disable hostname verification — matches Doris NoopHostnameVerifier
        # FE internal certs may use IP SANs or non-matching CN
        ctx.check_hostname = False
        return ctx

    # ── API methods ────────────────────────────────────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        return self._get("/api/replication/status")

    def get_cursor(self) -> Dict[str, Any]:
        return self._get("/api/replication/cursor")

    def pause_export(self) -> Dict[str, Any]:
        return self._post("/api/replication/pause-export")

    def promote_master(self) -> Dict[str, Any]:
        return self._post("/api/replication/promote-master")

    def enter_dr_mode(self) -> Dict[str, Any]:
        return self._post("/api/replication/enter-dr-mode")

    def enter_drill_mode(self) -> Dict[str, Any]:
        return self._post("/api/replication/enter-drill-mode")

    def exit_drill_mode(self) -> Dict[str, Any]:
        return self._post("/api/replication/exit-drill-mode")

    def is_reachable(self) -> bool:
        try:
            self.get_status()
            return True
        except Exception:
            return False

    def wait_for_journal_id(self, target_journal_id: int,
                            timeout_sec: int = 30) -> bool:
        """Polls cursor until caught up or timeout."""
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
            kwargs = {"timeout": self.timeout}
            if self._ssl_ctx:
                kwargs["context"] = self._ssl_ctx
            with urllib.request.urlopen(req, **kwargs) as resp:
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
            kwargs = {"timeout": self.timeout}
            if self._ssl_ctx:
                kwargs["context"] = self._ssl_ctx
            with urllib.request.urlopen(req, **kwargs) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"HTTP {e.code} from {url}: {e.read().decode()}") from e
        except Exception as e:
            raise RuntimeError(f"Request failed {url}: {e}") from e

    def _add_auth(self, req: urllib.request.Request):
        if self.username:
            import base64
            token = base64.b64encode(
                f"{self.username}:{self.password}".encode()).decode()
            req.add_header("Authorization", f"Basic {token}")
