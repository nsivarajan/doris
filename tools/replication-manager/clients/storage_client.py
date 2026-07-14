# Licensed under Apache License 2.0
"""
storage_client.py — thin OSS/S3 wrapper used by CLI commands.
Uses oss2 (Alibaba Cloud) or boto3 (AWS) depending on storage type.
Falls back to environment variables for credentials when no explicit config.
"""

import os


class StorageClient:
    """Wraps OSS or S3 for replication bucket access in the CLI tool."""

    def __init__(self, bucket: str, endpoint: str,
                 access_key: str = "", secret_key: str = "",
                 storage_type: str = "OSS"):
        self.bucket = bucket
        self.endpoint = endpoint
        self.access_key = access_key or os.environ.get("REPL_ACCESS_KEY", "")
        self.secret_key = secret_key or os.environ.get("REPL_SECRET_KEY", "")
        self.storage_type = storage_type.upper()
        self._client = None

    def get(self, key: str) -> bytes | None:
        """Read object at key. Returns None if not found."""
        try:
            if self.storage_type == "OSS":
                return self._oss_get(key)
            return self._s3_get(key)
        except Exception:
            return None

    def put(self, key: str, data: bytes) -> None:
        """Write bytes to key."""
        if self.storage_type == "OSS":
            self._oss_put(key, data)
        else:
            self._s3_put(key, data)

    def list(self, prefix: str) -> list[str]:
        """List keys with prefix, sorted."""
        if self.storage_type == "OSS":
            return self._oss_list(prefix)
        return self._s3_list(prefix)

    # ── OSS ───────────────────────────────────────────────────────────────────

    def _oss_client(self):
        if self._client is None:
            import oss2
            auth = oss2.Auth(self.access_key, self.secret_key)
            self._client = oss2.Bucket(auth, self.endpoint, self.bucket)
        return self._client

    def _oss_get(self, key: str) -> bytes | None:
        import oss2
        try:
            result = self._oss_client().get_object(key)
            return result.read()
        except oss2.exceptions.NoSuchKey:
            return None

    def _oss_put(self, key: str, data: bytes) -> None:
        self._oss_client().put_object(key, data)

    def _oss_list(self, prefix: str) -> list[str]:
        import oss2
        keys = []
        for obj in oss2.ObjectIterator(self._oss_client(), prefix=prefix):
            keys.append(obj.key)
        return sorted(keys)

    # ── S3 ────────────────────────────────────────────────────────────────────

    def _s3_client(self):
        if self._client is None:
            import boto3
            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint if self.endpoint else None,
                aws_access_key_id=self.access_key or None,
                aws_secret_access_key=self.secret_key or None,
            )
        return self._client

    def _s3_get(self, key: str) -> bytes | None:
        try:
            resp = self._s3_client().get_object(Bucket=self.bucket, Key=key)
            return resp["Body"].read()
        except Exception:
            return None

    def _s3_put(self, key: str, data: bytes) -> None:
        self._s3_client().put_object(Bucket=self.bucket, Key=key, Body=data)

    def _s3_list(self, prefix: str) -> list[str]:
        resp = self._s3_client().list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return sorted(obj["Key"] for obj in resp.get("Contents", []))
