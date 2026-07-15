# Licensed under Apache License 2.0
"""
Shared helper: build FEHttpClient from parsed CLI args.
All commands import this instead of constructing FEHttpClient directly
so that --use-https / --ca-cert / --no-verify-ssl are applied consistently.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from clients.fe_http_client import FEHttpClient


def fe_client_from_args(host_port: str, args) -> FEHttpClient:
    """
    Build FEHttpClient with TLS settings from global CLI args.

    Examples:
      Plain HTTP  (default):    host:8030, no TLS flags
      HTTPS system CA:          host:8050, --use-https
      HTTPS internal CA:        host:8050, --use-https --ca-cert /path/to/ca.crt
      HTTPS no verify (dev):    host:8050, --use-https --no-verify-ssl
    """
    return FEHttpClient(
        host_port=host_port,
        timeout=getattr(args, "timeout", 60),
        username=getattr(args, "fe_http_user", "admin"),
        password=getattr(args, "fe_http_password", ""),
        use_https=getattr(args, "use_https", False),
        ca_cert=getattr(args, "ca_cert", None),
        verify_ssl=not getattr(args, "no_verify_ssl", False),
    )
