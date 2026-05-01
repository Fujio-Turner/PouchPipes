"""
Attachment upload to external destinations (Phase 2).

Supports S3, HTTP, and filesystem destinations.  Each attachment is
uploaded individually with retry + exponential backoff.  Results are
returned as a dict of :class:`AttachmentUploadResult` for Phase 3
(post-processing) to consume.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import logging
import os
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import aiohttp

try:
    from icecream import ic
except ImportError:  # pragma: no cover
    ic = lambda *a, **kw: None  # noqa: E731

from cloud.cloud_base import render_key
from pipeline.pipeline_logging import log_event
from rest.attachment_config import AttachmentConfig

if TYPE_CHECKING:
    from main import MetricsCollector

logger = logging.getLogger("changes_worker")


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class AttachmentUploadResult:
    attachment_name: str
    destination_type: str
    key: str
    location: str
    content_type: str
    length: int
    digest: str
    uploaded_at: str
    etag: str = ""
    access_url: str = ""


# ---------------------------------------------------------------------------
# Uploader
# ---------------------------------------------------------------------------


class AttachmentUploader:
    """Upload fetched attachment binaries to a configured destination."""

    def __init__(
        self,
        config: AttachmentConfig,
        gateway_cfg: dict | None = None,
        metrics: MetricsCollector | None = None,
    ):
        self._config = config
        self._dest = config.destination
        self._retry = config.retry
        self._gateway_cfg = gateway_cfg or {}
        self._metrics = metrics

        # Lazy-initialised providers
        self._s3_client = None
        self._s3_executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._http_session: aiohttp.ClientSession | None = None

    # -- public entry point -------------------------------------------------

    async def upload_many(
        self,
        doc: dict,
        stubs: dict,
        fetched: dict[str, bytes],
    ) -> dict[str, AttachmentUploadResult]:
        """Upload all fetched attachments to the destination.

        Returns a mapping of ``attachment_name → AttachmentUploadResult``
        for successfully uploaded attachments.
        """
        if not fetched:
            return {}

        dest_type = self._dest.type
        sem = asyncio.Semaphore(self._config.fetch.max_concurrent_downloads)
        results: dict[str, AttachmentUploadResult] = {}
        errors: list[str] = []

        async def _upload_one(name: str, data: bytes) -> None:
            async with sem:
                stub = stubs.get(name, {})
                try:
                    result = await self._upload_single(doc, name, stub, data, dest_type)
                    results[name] = result
                except Exception as exc:
                    self._inc("attachments_upload_errors_total")
                    log_event(
                        logger,
                        "warn",
                        "ATTACHMENT",
                        "attachment upload failed: %s – %s" % (name, exc),
                        doc_id=doc.get("_id", doc.get("id", "<unknown>")),
                    )
                    errors.append(name)

        tasks = [_upload_one(name, data) for name, data in fetched.items()]
        await asyncio.gather(*tasks)

        if results:
            total_bytes = sum(r.length for r in results.values())
            self._inc("attachments_uploaded_total", len(results))
            self._inc("attachments_bytes_uploaded_total", total_bytes)

        if errors and self._config.halt_on_failure:
            from rest.attachments import AttachmentError

            doc_id = doc.get("_id", doc.get("id", "<unknown>"))
            raise AttachmentError(
                "failed to upload attachment(s) for doc %s: %s"
                % (doc_id, ", ".join(errors))
            )

        return results

    # -- single upload with retry -------------------------------------------

    async def _upload_single(
        self,
        doc: dict,
        name: str,
        stub: dict,
        data: bytes,
        dest_type: str,
    ) -> AttachmentUploadResult:
        """Upload one attachment with retry + backoff."""
        content_type = stub.get("content_type", "application/octet-stream")
        key = self._render_attachment_key(doc, name, stub, data)

        t_start = time.monotonic()
        last_exc: Exception | None = None

        for attempt in range(1, self._retry.max_retries + 1):
            try:
                if dest_type == "s3":
                    etag = await self._upload_s3(
                        key, data, content_type, doc, name, stub
                    )
                    location = "s3://%s/%s" % (self._dest.s3.bucket, key)
                elif dest_type == "http":
                    etag = await self._upload_http(
                        key, data, content_type, doc, name, stub
                    )
                    location = (
                        self._dest.http.url_template.format(
                            doc_id=doc.get("_id", doc.get("id", "")),
                            attachment_name=name,
                            key=key,
                        )
                        if self._dest.http.url_template
                        else key
                    )
                elif dest_type == "filesystem":
                    etag = await self._upload_filesystem(key, data)
                    location = str(
                        (
                            Path(self._dest.filesystem.base_path) / key.lstrip("/")
                        ).resolve()
                    )
                else:
                    raise ValueError("unsupported destination type: %s" % dest_type)

                elapsed = time.monotonic() - t_start
                log_event(
                    logger,
                    "debug",
                    "ATTACHMENT",
                    "uploaded attachment %s → %s (%.1fms)"
                    % (name, dest_type, elapsed * 1000),
                    doc_id=doc.get("_id", doc.get("id", "<unknown>")),
                )
                access_url = ""
                if dest_type == "s3" and self._dest.presigned_urls.enabled:
                    access_url = await self._generate_presigned_url(key)

                return AttachmentUploadResult(
                    attachment_name=name,
                    destination_type=dest_type,
                    key=key,
                    location=location,
                    content_type=content_type,
                    length=len(data),
                    digest=stub.get("digest", ""),
                    uploaded_at=datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    etag=etag,
                    access_url=access_url,
                )

            except Exception as exc:
                last_exc = exc
                if not self._is_transient(exc, dest_type):
                    raise
                if attempt >= self._retry.max_retries:
                    raise
                delay = min(
                    self._retry.backoff_base_seconds * (2 ** (attempt - 1)),
                    self._retry.backoff_max_seconds,
                )
                log_event(
                    logger,
                    "warn",
                    "ATTACHMENT",
                    "upload retry %d/%d for %s: %s"
                    % (attempt, self._retry.max_retries, name, exc),
                    doc_id=doc.get("_id", doc.get("id", "<unknown>")),
                )
                await asyncio.sleep(delay)

        raise last_exc  # type: ignore[misc]

    # -- key rendering ------------------------------------------------------

    def _render_attachment_key(
        self, doc: dict, name: str, stub: dict, data: bytes
    ) -> str:
        extra = {
            "attachment_name": name,
            "content_type": stub.get("content_type", "application/octet-stream"),
            "digest": stub.get("digest", ""),
            "length": len(data),
            "revpos": stub.get("revpos", ""),
        }
        cfg_dict = {
            "key_prefix": self._dest.key_prefix,
            "scope": self._gateway_cfg.get("scope", ""),
            "collection": self._gateway_cfg.get("collection", ""),
            "database": self._gateway_cfg.get("database", ""),
        }
        return render_key(self._dest.key_template, doc, cfg_dict, extra_vars=extra)

    # -- S3 upload ----------------------------------------------------------

    async def _upload_s3(
        self,
        key: str,
        data: bytes,
        content_type: str,
        doc: dict,
        name: str,
        stub: dict,
    ) -> str:
        """Upload to S3 using boto3 in a thread pool executor."""
        client = await self._ensure_s3_client()
        loop = asyncio.get_event_loop()

        s3_cfg = self._dest.s3
        put_kwargs: dict = {
            "Bucket": s3_cfg.bucket,
            "Key": key,
            "Body": data,
            "ContentType": content_type,
            "ContentLength": len(data),
        }
        if s3_cfg.storage_class:
            put_kwargs["StorageClass"] = s3_cfg.storage_class
        if s3_cfg.server_side_encryption:
            put_kwargs["ServerSideEncryption"] = s3_cfg.server_side_encryption
        if s3_cfg.kms_key_id and s3_cfg.server_side_encryption == "aws:kms":
            put_kwargs["SSEKMSKeyId"] = s3_cfg.kms_key_id

        metadata = dict(s3_cfg.metadata)
        metadata["doc_id"] = doc.get("_id", doc.get("id", ""))
        rev = doc.get("_rev", doc.get("rev", ""))
        if rev:
            metadata["rev"] = rev
        metadata["attachment_name"] = name
        digest = stub.get("digest", "")
        if digest:
            metadata["digest"] = digest
        put_kwargs["Metadata"] = {k: str(v) for k, v in metadata.items()}

        resp = await loop.run_in_executor(
            self._s3_executor,
            functools.partial(client.put_object, **put_kwargs),
        )
        return resp.get("ETag", "")

    async def _generate_presigned_url(self, key: str) -> str:
        """Generate a pre-signed GET URL for an S3 object."""
        client = await self._ensure_s3_client()
        loop = asyncio.get_event_loop()
        url = await loop.run_in_executor(
            self._s3_executor,
            functools.partial(
                client.generate_presigned_url,
                "get_object",
                Params={"Bucket": self._dest.s3.bucket, "Key": key},
                ExpiresIn=self._dest.presigned_urls.expiry_seconds,
            ),
        )
        return url

    async def _ensure_s3_client(self):
        if self._s3_client is not None:
            return self._s3_client

        try:
            import boto3
            from botocore.config import Config as BotoConfig
        except ImportError:
            raise RuntimeError(
                "boto3 is not installed – pip install boto3  "
                "(required for S3 attachment destination)"
            )

        s3_cfg = self._dest.s3
        kwargs: dict = {
            "service_name": "s3",
            "region_name": s3_cfg.region,
            "config": BotoConfig(
                retries={"max_attempts": 1, "mode": "standard"},
                connect_timeout=10,
                read_timeout=60,
            ),
        }
        if s3_cfg.endpoint_url:
            kwargs["endpoint_url"] = s3_cfg.endpoint_url
        if s3_cfg.access_key_id and s3_cfg.secret_access_key:
            kwargs["aws_access_key_id"] = s3_cfg.access_key_id
            kwargs["aws_secret_access_key"] = s3_cfg.secret_access_key
        if s3_cfg.session_token:
            kwargs["aws_session_token"] = s3_cfg.session_token

        self._s3_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="att-s3"
        )
        loop = asyncio.get_event_loop()
        self._s3_client = await loop.run_in_executor(
            self._s3_executor, functools.partial(boto3.client, **kwargs)
        )
        return self._s3_client

    # -- HTTP upload --------------------------------------------------------

    async def _upload_http(
        self,
        key: str,
        data: bytes,
        content_type: str,
        doc: dict,
        name: str,
        stub: dict,
    ) -> str:
        """Upload via HTTP PUT/POST to a configured endpoint."""
        http_cfg = self._dest.http
        if not http_cfg.url_template:
            raise ValueError("attachments.destination.http.url_template is required")

        url = http_cfg.url_template.format(
            doc_id=doc.get("_id", doc.get("id", "")),
            attachment_name=name,
            key=key,
        )
        method = http_cfg.method or "PUT"
        headers = dict(http_cfg.headers)
        headers["Content-Type"] = content_type
        headers["Content-Length"] = str(len(data))

        if self._http_session is None:
            self._http_session = aiohttp.ClientSession()

        async with self._http_session.request(
            method, url, data=data, headers=headers
        ) as resp:
            if 200 <= resp.status < 300:
                return resp.headers.get("ETag", "")
            body = await resp.text()
            raise _HTTPUploadError(resp.status, body)

    # -- Filesystem upload --------------------------------------------------

    async def _upload_filesystem(self, key: str, data: bytes) -> str:
        """Write attachment to filesystem atomically."""
        fs_cfg = self._dest.filesystem
        if not fs_cfg.base_path:
            raise ValueError("attachments.destination.filesystem.base_path is required")

        base = Path(fs_cfg.base_path).resolve()
        target = (base / key.lstrip("/")).resolve()

        # Guard against path traversal
        if not str(target).startswith(str(base)):
            raise ValueError("attachment key resolves outside base_path: %s" % key)

        # Write atomically via temp file + rename
        await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=str(target.parent))
        try:
            await asyncio.to_thread(os.write, fd, data)
            await asyncio.to_thread(os.close, fd)
            await asyncio.to_thread(os.replace, tmp_path, str(target))
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        return ""

    # -- error classification -----------------------------------------------

    def _is_transient(self, exc: Exception, dest_type: str) -> bool:
        if isinstance(exc, (ConnectionError, OSError, TimeoutError)):
            return True
        if isinstance(exc, asyncio.TimeoutError):
            return True
        if isinstance(exc, aiohttp.ClientConnectionError):
            return True
        if isinstance(exc, _HTTPUploadError):
            return exc.status in set(self._retry.retry_on_status)
        if dest_type == "s3":
            return self._is_transient_s3(exc)
        if isinstance(exc, ValueError):
            return False
        return False

    @staticmethod
    def _is_transient_s3(exc: Exception) -> bool:
        try:
            from botocore.exceptions import (
                ClientError,
                EndpointConnectionError,
                ConnectTimeoutError,
                ReadTimeoutError,
            )
        except ImportError:
            return False

        if isinstance(
            exc, (EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError)
        ):
            return True
        if isinstance(exc, ClientError):
            code = exc.response.get("Error", {}).get("Code", "")
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            if code in (
                "RequestTimeout",
                "SlowDown",
                "InternalError",
                "ServiceUnavailable",
            ):
                return True
            if status >= 500 or status == 429:
                return True
            return False
        return False

    # -- cleanup ------------------------------------------------------------

    async def close(self) -> None:
        if self._http_session:
            await self._http_session.close()
            self._http_session = None
        if self._s3_client:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(self._s3_executor, self._s3_client.close)
            except Exception:
                pass
            self._s3_client = None
        if self._s3_executor:
            self._s3_executor.shutdown(wait=False)
            self._s3_executor = None

    # -- metrics helper -----------------------------------------------------

    def _inc(self, counter: str, amount: int = 1) -> None:
        if self._metrics:
            self._metrics.inc(counter, amount)


# ---------------------------------------------------------------------------
# Internal error type for HTTP uploads
# ---------------------------------------------------------------------------


class _HTTPUploadError(Exception):
    def __init__(self, status: int, body: str):
        self.status = status
        super().__init__("HTTP %d: %s" % (status, body[:200]))
