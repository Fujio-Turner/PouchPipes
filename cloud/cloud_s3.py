"""
AWS S3 output forwarder for changes_worker.

Uploads each processed document as a JSON object to an S3 bucket.
Supports AWS S3, MinIO, LocalStack, and any S3-compatible store
via the endpoint_url config.

Requires: pip install boto3

AWS best practices implemented:
  - Disable boto3 internal retries (our retry loop is authoritative)
  - ContentLength on every PutObject to avoid chunked transfer
  - Dedicated thread pool executor for boto3 calls (avoid blocking event loop)
  - head_bucket to validate credentials + bucket access at startup
  - Specific error classification for S3 error codes (SlowDown, RequestTimeout, etc.)
  - Server-side encryption (SSE-S3, SSE-KMS) support
  - Storage class selection (STANDARD, IA, GLACIER, etc.)
  - Custom x-amz-meta-* metadata headers
  - S3-compatible endpoint support (MinIO, LocalStack, Ceph, GCS interop)
"""

import asyncio
import concurrent.futures
import functools
import logging

from pipeline.pipeline_logging import log_event

try:
    from icecream import ic
except ImportError:
    ic = lambda *a, **kw: None  # noqa: E731

try:
    import boto3
    from botocore.config import Config as BotoConfig
    from botocore.exceptions import (
        ClientError,
        BotoCoreError,
        NoCredentialsError,
        EndpointConnectionError,
        ConnectTimeoutError,
        ReadTimeoutError,
    )
except ImportError:
    boto3 = None
    ClientError = None
    BotoCoreError = None
    NoCredentialsError = None
    EndpointConnectionError = None
    ConnectTimeoutError = None
    ReadTimeoutError = None
    BotoConfig = None

from .cloud_base import BaseCloudForwarder

logger = logging.getLogger("changes_worker")


class S3OutputForwarder(BaseCloudForwarder):
    """Async S3 output forwarder using boto3."""

    def __init__(self, out_cfg: dict, dry_run: bool = False, metrics=None):
        if boto3 is None:
            raise RuntimeError(
                "boto3 is not installed – pip install boto3  "
                "(required for S3 output mode)"
            )
        super().__init__(out_cfg, dry_run, metrics=metrics)

        cfg = self._get_provider_cfg(out_cfg)
        self._bucket = cfg.get("bucket", "")
        self._region = cfg.get("region", "us-east-1")
        self._endpoint_url = cfg.get("endpoint_url", "") or None
        self._access_key_id = cfg.get("access_key_id", "") or None
        self._secret_access_key = cfg.get("secret_access_key", "") or None
        self._session_token = cfg.get("session_token", "") or None
        self._storage_class = cfg.get("storage_class", "") or None
        self._sse = cfg.get("server_side_encryption", "") or None
        self._kms_key_id = cfg.get("kms_key_id", "") or None
        self._custom_metadata = cfg.get("metadata", {})

        self._client = None
        # Dedicated thread pool for boto3 sync calls – avoids starving the
        # default executor that aiohttp and other async code may share.
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="s3-io"
        )
        self._init_metrics()

        ic(
            "S3OutputForwarder init",
            self._bucket,
            self._region,
            self._endpoint_url,
            self._storage_class,
            self._sse,
        )

    @property
    def _provider(self) -> str:
        return "s3"

    def _get_provider_cfg(self, out_cfg: dict) -> dict:
        return out_cfg.get("s3", {})

    # ── Client lifecycle ────────────────────────────────────────────────

    async def _create_client(self) -> None:
        """Create the boto3 S3 client in a thread (boto3 is sync)."""
        loop = asyncio.get_event_loop()
        self._client = await loop.run_in_executor(self._executor, self._build_client)
        ic("_create_client: OK", self._bucket, self._region)

    def _build_client(self):
        """Build the boto3 S3 client (called in executor thread)."""
        kwargs = {
            "service_name": "s3",
            "region_name": self._region,
            "config": BotoConfig(
                # Disable boto3 internal retries – our retry loop in
                # BaseCloudForwarder._send_with_retry() is authoritative.
                retries={"max_attempts": 1, "mode": "standard"},
                # Reasonable connect/read timeouts for S3
                connect_timeout=10,
                read_timeout=30,
            ),
        }
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url
        if self._access_key_id and self._secret_access_key:
            kwargs["aws_access_key_id"] = self._access_key_id
            kwargs["aws_secret_access_key"] = self._secret_access_key
        if self._session_token:
            kwargs["aws_session_token"] = self._session_token

        return boto3.client(**kwargs)

    async def _close_client(self) -> None:
        """Close the boto3 client and thread pool."""
        if self._client:
            loop = asyncio.get_event_loop()
            try:
                await loop.run_in_executor(self._executor, self._client.close)
            except Exception as exc:
                ic("_close_client: error closing boto3 client", type(exc).__name__)
            self._client = None
        self._executor.shutdown(wait=False)
        ic("_close_client: done")

    # ── Object operations ───────────────────────────────────────────────

    async def _upload_object(
        self, key: str, body: bytes, content_type: str, metadata: dict
    ) -> dict:
        """Upload a single object to S3."""
        loop = asyncio.get_event_loop()
        put_kwargs = {
            "Bucket": self._bucket,
            "Key": key,
            "Body": body,
            "ContentType": content_type,
            # Always send ContentLength — avoids chunked transfer encoding
            # and lets S3 validate the upload integrity.
            "ContentLength": len(body),
        }
        if self._storage_class:
            put_kwargs["StorageClass"] = self._storage_class
        if self._sse:
            put_kwargs["ServerSideEncryption"] = self._sse
        if self._kms_key_id and self._sse == "aws:kms":
            put_kwargs["SSEKMSKeyId"] = self._kms_key_id

        # Merge custom metadata from config with per-doc metadata
        merged = {**self._custom_metadata, **(metadata or {})}
        if merged:
            put_kwargs["Metadata"] = {k: str(v) for k, v in merged.items()}

        ic("_upload_object", key, len(body), content_type)
        resp = await loop.run_in_executor(
            self._executor,
            functools.partial(self._client.put_object, **put_kwargs),
        )
        return {
            "status": resp.get("ResponseMetadata", {}).get("HTTPStatusCode", 200),
            "etag": resp.get("ETag", ""),
        }

    async def _delete_object(self, key: str) -> dict:
        """Delete a single object from S3."""
        loop = asyncio.get_event_loop()
        ic("_delete_object", key)
        resp = await loop.run_in_executor(
            self._executor,
            functools.partial(self._client.delete_object, Bucket=self._bucket, Key=key),
        )
        return {
            "status": resp.get("ResponseMetadata", {}).get("HTTPStatusCode", 204),
        }

    # ── Health check ────────────────────────────────────────────────────

    async def _test_bucket(self) -> bool:
        """Check if the bucket is accessible via HeadBucket."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self._executor,
            functools.partial(self._client.head_bucket, Bucket=self._bucket),
        )
        ic("_test_bucket: OK", self._bucket)
        log_event(logger, "info", "OUTPUT", "S3 bucket accessible", mode="s3")
        return True

    # ── Error classification ────────────────────────────────────────────

    def _is_transient(self, exc: Exception) -> bool:
        """Classify S3/boto3 errors as transient (retryable) or permanent."""
        # Connection-level errors are always transient
        if isinstance(exc, (ConnectionError, OSError, TimeoutError)):
            return True
        # botocore-specific connection/timeout errors
        if EndpointConnectionError and isinstance(exc, EndpointConnectionError):
            return True
        if ConnectTimeoutError and isinstance(exc, ConnectTimeoutError):
            return True
        if ReadTimeoutError and isinstance(exc, ReadTimeoutError):
            return True
        # Generic botocore errors (excluding ClientError which has its own logic)
        if BotoCoreError and isinstance(exc, BotoCoreError):
            # BotoCoreError subtypes that aren't ClientError are generally transient
            if ClientError and isinstance(exc, ClientError):
                pass  # fall through to ClientError handling below
            else:
                return True
        if ClientError and isinstance(exc, ClientError):
            code = exc.response.get("Error", {}).get("Code", "")
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            # S3-specific transient error codes
            if code in (
                "RequestTimeout",
                "SlowDown",  # S3 rate limiting (503)
                "InternalError",
                "ServiceUnavailable",
                "RequestTimeTooSkewed",
            ):
                return True
            if status >= 500:
                return True
            if status == 429:
                return True
            # Everything else (4xx) is permanent
            return False
        # NoCredentialsError is permanent
        if NoCredentialsError and isinstance(exc, NoCredentialsError):
            return False
        return False

    def _error_class(self, exc: Exception) -> str:
        """Return a short classification string for metrics and DLQ."""
        if isinstance(exc, (ConnectionError, OSError)):
            return "connection"
        if isinstance(exc, TimeoutError):
            return "timeout"
        if EndpointConnectionError and isinstance(exc, EndpointConnectionError):
            return "connection"
        if ConnectTimeoutError and isinstance(exc, ConnectTimeoutError):
            return "timeout"
        if ReadTimeoutError and isinstance(exc, ReadTimeoutError):
            return "timeout"
        if NoCredentialsError and isinstance(exc, NoCredentialsError):
            return "auth"
        if ClientError and isinstance(exc, ClientError):
            code = exc.response.get("Error", {}).get("Code", "")
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            if code in ("AccessDenied", "AllAccessDisabled"):
                return "access_denied"
            if code in ("NoSuchBucket", "NoSuchKey", "NotFound"):
                return "not_found"
            if code in (
                "InvalidAccessKeyId",
                "SignatureDoesNotMatch",
                "ExpiredToken",
            ):
                return "auth"
            if code in ("SlowDown", "RequestTimeout") or status == 429:
                return "rate_limit"
            if code == "EntityTooLarge":
                return "payload_too_large"
            if status >= 500:
                return "server_error"
            return "client_error"
        return "unknown"
