"""
Streaming attachment transfer (Phase 4 — Item 16).

Pipes GET responses directly to destination PUT without buffering the
entire binary in memory.  Each attachment is streamed in fixed-size
chunks (default 64 KiB), keeping peak memory usage bounded regardless
of attachment size.

Supported destinations:
  - **S3** — multipart upload via ``create_multipart_upload`` /
    ``upload_part`` / ``complete_multipart_upload``
  - **HTTP** — chunked PUT/POST to the configured endpoint
  - **Filesystem** — incremental write to disk via temp file + rename
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import hashlib
import logging
import os
import tempfile
import time
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
from rest.attachment_upload import AttachmentUploadResult

if TYPE_CHECKING:
    from main import MetricsCollector

logger = logging.getLogger("changes_worker")

# Default chunk size for streaming reads (64 KiB)
DEFAULT_CHUNK_SIZE = 65_536

# S3 minimum part size for multipart upload (5 MiB)
S3_MIN_PART_SIZE = 5 * 1024 * 1024


# ---------------------------------------------------------------------------
# Streamer
# ---------------------------------------------------------------------------


class AttachmentStreamer:
    """Stream attachment bytes from source to destination without full buffering."""

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
        self._chunk_size = DEFAULT_CHUNK_SIZE

        # Lazy-initialised
        self._s3_client = None
        self._s3_executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._http_session: aiohttp.ClientSession | None = None

    # -- public entry point -------------------------------------------------

    async def stream_attachment(
        self,
        doc: dict,
        name: str,
        stub: dict,
        source_resp: aiohttp.ClientResponse,
    ) -> AttachmentUploadResult:
        """Stream a single attachment from *source_resp* to the destination.

        The response body is consumed in chunks — the full binary is never
        held in memory.  Returns an :class:`AttachmentUploadResult` on success.
        """
        dest_type = self._dest.type
        content_type = stub.get("content_type", "application/octet-stream")
        expected_length = stub.get("length", 0)
        key = self._render_key(doc, name, stub)

        t_start = time.monotonic()

        if dest_type == "s3":
            etag, length, digest = await self._stream_to_s3(
                key, content_type, source_resp, expected_length, stub
            )
            location = "s3://%s/%s" % (self._dest.s3.bucket, key)
        elif dest_type == "http":
            etag, length, digest = await self._stream_to_http(
                key, content_type, source_resp, doc, name
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
            etag, length, digest = await self._stream_to_filesystem(key, source_resp)
            location = str(
                (Path(self._dest.filesystem.base_path) / key.lstrip("/")).resolve()
            )
        else:
            raise ValueError("unsupported destination type: %s" % dest_type)

        elapsed = time.monotonic() - t_start
        doc_id = doc.get("_id", doc.get("id", "<unknown>"))
        log_event(
            logger,
            "debug",
            "ATTACHMENT",
            "streamed attachment %s → %s (%d bytes, %.1fms)"
            % (name, dest_type, length, elapsed * 1000),
            doc_id=doc_id,
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
            length=length,
            digest=digest,
            uploaded_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            etag=etag,
            access_url=access_url,
        )

    # -- S3 multipart upload ------------------------------------------------

    async def _stream_to_s3(
        self,
        key: str,
        content_type: str,
        source_resp: aiohttp.ClientResponse,
        expected_length: int,
        stub: dict,
    ) -> tuple[str, int, str]:
        """Stream to S3 via multipart upload.

        Returns ``(etag, total_bytes, digest_str)``.
        """
        client = await self._ensure_s3_client()
        loop = asyncio.get_event_loop()
        s3_cfg = self._dest.s3

        # Create multipart upload
        create_kwargs: dict = {
            "Bucket": s3_cfg.bucket,
            "Key": key,
            "ContentType": content_type,
        }
        if s3_cfg.storage_class:
            create_kwargs["StorageClass"] = s3_cfg.storage_class
        if s3_cfg.server_side_encryption:
            create_kwargs["ServerSideEncryption"] = s3_cfg.server_side_encryption
        if s3_cfg.kms_key_id and s3_cfg.server_side_encryption == "aws:kms":
            create_kwargs["SSEKMSKeyId"] = s3_cfg.kms_key_id

        create_resp = await loop.run_in_executor(
            self._s3_executor,
            functools.partial(client.create_multipart_upload, **create_kwargs),
        )
        upload_id = create_resp["UploadId"]

        parts: list[dict] = []
        total_bytes = 0
        hasher = hashlib.md5()
        part_number = 1
        buffer = bytearray()

        try:
            async for chunk in source_resp.content.iter_chunked(self._chunk_size):
                buffer.extend(chunk)
                hasher.update(chunk)
                total_bytes += len(chunk)

                # S3 requires minimum 5 MiB per part (except last)
                while len(buffer) >= S3_MIN_PART_SIZE:
                    part_data = bytes(buffer[:S3_MIN_PART_SIZE])
                    buffer = bytearray(buffer[S3_MIN_PART_SIZE:])

                    part_resp = await loop.run_in_executor(
                        self._s3_executor,
                        functools.partial(
                            client.upload_part,
                            Bucket=s3_cfg.bucket,
                            Key=key,
                            UploadId=upload_id,
                            PartNumber=part_number,
                            Body=part_data,
                        ),
                    )
                    parts.append({"ETag": part_resp["ETag"], "PartNumber": part_number})
                    part_number += 1

            # Upload remaining buffer as final part
            if buffer or part_number == 1:
                part_resp = await loop.run_in_executor(
                    self._s3_executor,
                    functools.partial(
                        client.upload_part,
                        Bucket=s3_cfg.bucket,
                        Key=key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=bytes(buffer),
                    ),
                )
                parts.append({"ETag": part_resp["ETag"], "PartNumber": part_number})

            # Complete multipart upload
            complete_resp = await loop.run_in_executor(
                self._s3_executor,
                functools.partial(
                    client.complete_multipart_upload,
                    Bucket=s3_cfg.bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                ),
            )
            etag = complete_resp.get("ETag", "")

        except BaseException:
            # Abort multipart upload on any failure
            try:
                await loop.run_in_executor(
                    self._s3_executor,
                    functools.partial(
                        client.abort_multipart_upload,
                        Bucket=s3_cfg.bucket,
                        Key=key,
                        UploadId=upload_id,
                    ),
                )
            except Exception:
                pass
            raise

        import base64

        digest = "md5-%s" % base64.b64encode(hasher.digest()).decode("ascii")
        return etag, total_bytes, digest

    # -- HTTP chunked upload ------------------------------------------------

    async def _stream_to_http(
        self,
        key: str,
        content_type: str,
        source_resp: aiohttp.ClientResponse,
        doc: dict,
        name: str,
    ) -> tuple[str, int, str]:
        """Stream to an HTTP endpoint using chunked transfer.

        Returns ``(etag, total_bytes, digest_str)``.
        """
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
        headers["Transfer-Encoding"] = "chunked"

        if self._http_session is None:
            self._http_session = aiohttp.ClientSession()

        total_bytes = 0
        hasher = hashlib.md5()

        async def _chunk_generator():
            nonlocal total_bytes
            async for chunk in source_resp.content.iter_chunked(self._chunk_size):
                hasher.update(chunk)
                total_bytes += len(chunk)
                yield chunk

        async with self._http_session.request(
            method, url, data=_chunk_generator(), headers=headers
        ) as resp:
            if resp.status < 200 or resp.status >= 300:
                body = await resp.text()
                raise RuntimeError("HTTP %d: %s" % (resp.status, body[:200]))
            etag = resp.headers.get("ETag", "")

        import base64

        digest = "md5-%s" % base64.b64encode(hasher.digest()).decode("ascii")
        return etag, total_bytes, digest

    # -- Filesystem streaming write -----------------------------------------

    async def _stream_to_filesystem(
        self,
        key: str,
        source_resp: aiohttp.ClientResponse,
    ) -> tuple[str, int, str]:
        """Stream to filesystem via temp file + atomic rename.

        Returns ``("", total_bytes, digest_str)``.
        """
        fs_cfg = self._dest.filesystem
        if not fs_cfg.base_path:
            raise ValueError("attachments.destination.filesystem.base_path is required")

        base = Path(fs_cfg.base_path).resolve()
        target = (base / key.lstrip("/")).resolve()

        if not str(target).startswith(str(base)):
            raise ValueError("attachment key resolves outside base_path: %s" % key)

        await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=str(target.parent))

        total_bytes = 0
        hasher = hashlib.md5()

        try:
            async for chunk in source_resp.content.iter_chunked(self._chunk_size):
                hasher.update(chunk)
                total_bytes += len(chunk)
                await asyncio.to_thread(os.write, fd, chunk)
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

        import base64

        digest = "md5-%s" % base64.b64encode(hasher.digest()).decode("ascii")
        return "", total_bytes, digest

    # -- key rendering ------------------------------------------------------

    def _render_key(self, doc: dict, name: str, stub: dict) -> str:
        extra = {
            "attachment_name": name,
            "content_type": stub.get("content_type", "application/octet-stream"),
            "digest": stub.get("digest", ""),
            "length": stub.get("length", 0),
            "revpos": stub.get("revpos", ""),
        }
        cfg_dict = {
            "key_prefix": self._dest.key_prefix,
            "scope": self._gateway_cfg.get("scope", ""),
            "collection": self._gateway_cfg.get("collection", ""),
            "database": self._gateway_cfg.get("database", ""),
        }
        return render_key(self._dest.key_template, doc, cfg_dict, extra_vars=extra)

    # -- S3 helpers ---------------------------------------------------------

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
                read_timeout=120,
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
            max_workers=4, thread_name_prefix="att-s3-stream"
        )
        loop = asyncio.get_event_loop()
        self._s3_client = await loop.run_in_executor(
            self._s3_executor, functools.partial(boto3.client, **kwargs)
        )
        return self._s3_client

    async def _generate_presigned_url(self, key: str) -> str:
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

    # -- metrics ------------------------------------------------------------

    def _inc(self, counter: str, amount: int = 1) -> None:
        if self._metrics:
            self._metrics.inc(counter, amount)
