"""
Attachment processing for the changes_worker pipeline.

Implements Phase 0 (detect-only / noop) and Phase 1 (core detection,
filtering, individual fetch) from the ATTACHMENTS.md design.

Sits between the MIDDLE and RIGHT stages of the pipeline:
  LEFT (fetch) → MIDDLE (map) → **attachments** → RIGHT (output)
"""

from __future__ import annotations

import asyncio
import base64
import fnmatch
import hashlib
import json
import logging

try:
    import orjson

    _json_loads = orjson.loads
except ImportError:
    _json_loads = json.loads
import re
import time
from typing import TYPE_CHECKING
from urllib.parse import quote

import aiohttp

try:
    from icecream import ic
except ImportError:  # pragma: no cover
    ic = lambda *a, **kw: None  # noqa: E731

from pipeline.pipeline_logging import log_event
from rest.attachment_config import AttachmentConfig, parse_attachment_config  # noqa: F401
from rest.attachment_upload import AttachmentUploader, AttachmentUploadResult  # noqa: F401
from rest.attachment_postprocess import AttachmentPostProcessor  # noqa: F401
from rest.attachment_stream import AttachmentStreamer  # noqa: F401
from rest.changes_http import RetryableHTTP

if TYPE_CHECKING:
    from main import MetricsCollector

logger = logging.getLogger("changes_worker")


# ---------------------------------------------------------------------------
# Error
# ---------------------------------------------------------------------------


class AttachmentError(Exception):
    """Raised when attachment processing fails and halt_on_failure=True."""

    pass


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------


class AttachmentProcessor:
    """Processes attachments between MIDDLE and RIGHT stages of the pipeline."""

    def __init__(
        self,
        config: AttachmentConfig,
        metrics: MetricsCollector | None = None,
        gateway_cfg: dict | None = None,
    ):
        self._config = config
        self._metrics = metrics
        self._uploader: AttachmentUploader | None = None
        self._streamer: AttachmentStreamer | None = None
        self._post_processor: AttachmentPostProcessor | None = None
        if config.enabled and config.destination.type:
            if config.mode == "stream":
                self._streamer = AttachmentStreamer(
                    config, gateway_cfg=gateway_cfg, metrics=metrics
                )
            else:
                self._uploader = AttachmentUploader(
                    config, gateway_cfg=gateway_cfg, metrics=metrics
                )
        if config.enabled and config.post_process.action != "none":
            self._post_processor = AttachmentPostProcessor(config, metrics=metrics)

    # -- public entry point -------------------------------------------------

    async def process(
        self,
        doc: dict,
        base_url: str,
        http: RetryableHTTP,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
        src: str,
    ) -> tuple[dict, bool]:
        """Process attachments for a single document.

        Returns ``(modified_doc, should_skip_output)``.
        *should_skip_output* is ``True`` only in dry_run mode.

        On error with ``halt_on_failure=True``, raises :class:`AttachmentError`.
        On error with ``halt_on_failure=False``, logs warning and continues.
        """
        cfg = self._config

        if not cfg.enabled:
            return doc, False

        doc_id = doc.get("_id", doc.get("id", "<unknown>"))

        # Edge Server skip
        if src == "edge_server" and cfg.skip_on_edge_server:
            log_event(
                logger,
                "debug",
                "ATTACHMENT",
                "skipping attachment processing for edge_server source",
                doc_id=doc_id,
            )
            self._inc("attachments_skipped_total")
            return doc, False

        # Phase 0: detect
        stubs = self._detect(doc)
        if not stubs:
            return doc, False

        t0 = time.monotonic()
        detected_count = len(stubs)
        self._inc("attachments_detected_total", detected_count)
        log_event(
            logger,
            "debug",
            "ATTACHMENT",
            "detected %d attachment(s)" % detected_count,
            doc_id=doc_id,
        )

        # Phase 0.5: filter
        filtered = self._apply_filters(stubs, doc_id)
        if not filtered:
            log_event(
                logger,
                "debug",
                "ATTACHMENT",
                "all attachments filtered out",
                doc_id=doc_id,
            )
            self._inc("attachments_skipped_total", detected_count)
            return doc, False

        skipped_count = detected_count - len(filtered)
        if skipped_count:
            self._inc("attachments_skipped_total", skipped_count)

        # Dry-run: log stats, skip fetch/upload/post-process
        if cfg.dry_run:
            total_bytes = sum(s.get("length", 0) for s in filtered.values())
            log_event(
                logger,
                "info",
                "ATTACHMENT",
                "dry_run: would fetch %d attachment(s), %d bytes total"
                % (len(filtered), total_bytes),
                doc_id=doc_id,
            )
            return doc, False

        # -- Summary tracking --
        fetched_count = 0
        fetched_bytes = 0
        uploaded_count = 0
        uploaded_bytes = 0
        dest_type = cfg.destination.type or "none"
        post_action = cfg.post_process.action if self._post_processor else "none"
        errors: list[str] = []

        # Streaming path — fetch+upload are fused (no in-memory buffer)
        if self._streamer:
            try:
                uploaded_results = await self._fetch_and_upload_streaming(
                    doc_id, filtered, base_url, http, auth, headers, doc
                )
            except AttachmentError:
                raise
            except Exception as exc:
                self._inc("attachments_download_errors_total")
                if cfg.halt_on_failure:
                    raise AttachmentError(
                        "streaming transfer failed for doc %s: %s" % (doc_id, exc)
                    ) from exc
                errors.append("stream: %s" % exc)
                log_event(
                    logger,
                    "warn",
                    "ATTACHMENT",
                    "streaming transfer error (continuing): %s" % exc,
                    doc_id=doc_id,
                )
                self._log_attachment_summary(
                    doc_id,
                    detected_count,
                    skipped_count,
                    len(filtered),
                    0,
                    0,
                    0,
                    0,
                    dest_type,
                    "none",
                    errors,
                    t0,
                )
                return doc, False

            if uploaded_results:
                uploaded_count = len(uploaded_results)
                uploaded_bytes = sum(r.length for r in uploaded_results.values())
                fetched_count = uploaded_count  # streamed = fetched+uploaded fused
                fetched_bytes = uploaded_bytes
                log_event(
                    logger,
                    "debug",
                    "ATTACHMENT",
                    "streamed %d attachment(s) to %s" % (uploaded_count, dest_type),
                    doc_id=doc_id,
                )

            # Phase 3 – post-process
            if self._post_processor and uploaded_results:
                try:
                    doc = await self._post_processor.post_process(
                        doc, uploaded_results, base_url, http, auth, headers
                    )
                except AttachmentError:
                    raise
                except Exception as exc:
                    self._inc("attachments_post_process_errors_total")
                    if cfg.halt_on_failure:
                        raise AttachmentError(
                            "attachment post-process failed for doc %s: %s"
                            % (doc_id, exc)
                        ) from exc
                    errors.append("post-process: %s" % exc)
                    log_event(
                        logger,
                        "warn",
                        "ATTACHMENT",
                        "attachment post-process error (continuing): %s" % exc,
                        doc_id=doc_id,
                    )

            self._log_attachment_summary(
                doc_id,
                detected_count,
                skipped_count,
                len(filtered),
                fetched_count,
                fetched_bytes,
                uploaded_count,
                uploaded_bytes,
                dest_type,
                post_action,
                errors,
                t0,
            )
            return doc, False

        # Phase 1: fetch
        try:
            fetch_mode = self._resolve_fetch_mode()
            if fetch_mode == "bulk":
                fetched = await self._fetch_bulk(
                    doc_id, filtered, base_url, http, auth, headers
                )
            elif fetch_mode == "multipart":
                fetched = await self._fetch_multipart(
                    doc_id, filtered, base_url, http, auth, headers, src
                )
            else:
                fetched = await self._fetch_individual(
                    doc_id, filtered, base_url, http, auth, headers
                )
        except AttachmentError:
            raise
        except Exception as exc:
            self._inc("attachments_download_errors_total")
            if cfg.halt_on_failure:
                raise AttachmentError(
                    "attachment fetch failed for doc %s: %s" % (doc_id, exc)
                ) from exc
            errors.append("fetch: %s" % exc)
            log_event(
                logger,
                "warn",
                "ATTACHMENT",
                "attachment fetch error (continuing): %s" % exc,
                doc_id=doc_id,
            )
            self._log_attachment_summary(
                doc_id,
                detected_count,
                skipped_count,
                len(filtered),
                0,
                0,
                0,
                0,
                dest_type,
                "none",
                errors,
                t0,
            )
            return doc, False

        if fetched:
            fetched_count = len(fetched)
            fetched_bytes = sum(len(v) for v in fetched.values())
            self._inc("attachments_bytes_downloaded_total", fetched_bytes)
            log_event(
                logger,
                "debug",
                "ATTACHMENT",
                "fetched %d attachment(s), %d bytes total"
                % (fetched_count, fetched_bytes),
                doc_id=doc_id,
            )

        # Phase 2 – upload fetched data to destination
        uploaded_results: dict[str, AttachmentUploadResult] = {}
        if self._uploader and fetched:
            try:
                uploaded_results = await self._uploader.upload_many(
                    doc, filtered, fetched
                )
            except AttachmentError:
                raise
            except Exception as exc:
                self._inc("attachments_upload_errors_total")
                if cfg.halt_on_failure:
                    raise AttachmentError(
                        "attachment upload failed for doc %s: %s" % (doc_id, exc)
                    ) from exc
                errors.append("upload: %s" % exc)
                log_event(
                    logger,
                    "warn",
                    "ATTACHMENT",
                    "attachment upload error (continuing): %s" % exc,
                    doc_id=doc_id,
                )
                self._log_attachment_summary(
                    doc_id,
                    detected_count,
                    skipped_count,
                    len(filtered),
                    fetched_count,
                    fetched_bytes,
                    0,
                    0,
                    dest_type,
                    "none",
                    errors,
                    t0,
                )
                return doc, False

            if uploaded_results:
                uploaded_count = len(uploaded_results)
                uploaded_bytes = sum(r.length for r in uploaded_results.values())
                log_event(
                    logger,
                    "debug",
                    "ATTACHMENT",
                    "uploaded %d attachment(s) to %s" % (uploaded_count, dest_type),
                    doc_id=doc_id,
                )

        # Phase 3 – post-process (update doc metadata, delete attachments, etc.)
        if self._post_processor and fetched:
            try:
                doc = await self._post_processor.post_process(
                    doc, uploaded_results, base_url, http, auth, headers
                )
            except AttachmentError:
                raise
            except Exception as exc:
                self._inc("attachments_post_process_errors_total")
                if cfg.halt_on_failure:
                    raise AttachmentError(
                        "attachment post-process failed for doc %s: %s" % (doc_id, exc)
                    ) from exc
                errors.append("post-process: %s" % exc)
                log_event(
                    logger,
                    "warn",
                    "ATTACHMENT",
                    "attachment post-process error (continuing): %s" % exc,
                    doc_id=doc_id,
                )

        self._log_attachment_summary(
            doc_id,
            detected_count,
            skipped_count,
            len(filtered),
            fetched_count,
            fetched_bytes,
            uploaded_count,
            uploaded_bytes,
            dest_type,
            post_action,
            errors,
            t0,
        )
        return doc, False

    def _log_attachment_summary(
        self,
        doc_id: str,
        detected: int,
        skipped: int,
        eligible: int,
        fetched: int,
        fetched_bytes: int,
        uploaded: int,
        uploaded_bytes: int,
        dest_type: str,
        post_action: str,
        errors: list[str],
        t0: float,
    ) -> None:
        """Emit consolidated per-doc attachment summary at INFO."""
        elapsed_ms = (time.monotonic() - t0) * 1000
        log_event(
            logger,
            "info",
            "ATTACHMENT",
            "%d detected | %d fetched | %d uploaded to %s | post=%s | %.0fms%s"
            % (
                detected,
                fetched,
                uploaded,
                dest_type,
                post_action,
                elapsed_ms,
                " | %d error(s)" % len(errors) if errors else "",
            ),
            doc_id=doc_id,
            batch_size=detected,
            filtered_count=skipped if skipped else None,
            docs_fetched=fetched,
            bytes=fetched_bytes if fetched_bytes else None,
            mode=dest_type,
            operation=post_action if post_action != "none" else None,
            failed=len(errors) if errors else None,
            duration_ms=round(elapsed_ms, 1),
        )

    async def close(self) -> None:
        """Release resources held by the uploader / streamer."""
        if self._uploader:
            await self._uploader.close()
        if self._streamer:
            await self._streamer.close()

    # -- fetch-mode dispatch ------------------------------------------------

    def _resolve_fetch_mode(self) -> str:
        """Return the effective fetch mode: 'individual', 'bulk', or 'multipart'."""
        if self._config.mode == "multipart":
            return "multipart"
        if self._config.mode == "bulk":
            return "bulk"
        if self._config.fetch.use_bulk_get:
            return "bulk"
        return "individual"

    # -- streaming fetch+upload (fused) ------------------------------------

    async def _fetch_and_upload_streaming(
        self,
        doc_id: str,
        stubs: dict,
        base_url: str,
        http: RetryableHTTP,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
        doc: dict,
    ) -> dict[str, AttachmentUploadResult]:
        """Stream each attachment GET→PUT without full buffering.

        Opens a GET request per attachment and pipes the response body
        directly to the destination via :class:`AttachmentStreamer`.
        Attachments are processed sequentially to keep memory bounded.

        Returns a mapping of ``attachment_name → AttachmentUploadResult``.
        """
        sem = asyncio.Semaphore(self._config.fetch.max_concurrent_downloads)
        results: dict[str, AttachmentUploadResult] = {}
        errors: list[str] = []

        async def _stream_one(name: str, stub: dict) -> None:
            async with sem:
                url = "%s/%s/%s" % (
                    base_url.rstrip("/"),
                    quote(doc_id, safe=""),
                    quote(name, safe=""),
                )
                resp = None
                try:
                    resp = await http.request(
                        "GET",
                        url,
                        auth=auth,
                        headers=headers,
                    )
                    result = await self._streamer.stream_attachment(
                        doc, name, stub, resp
                    )
                    self._inc("attachments_downloaded_total")
                    self._inc("attachments_uploaded_total")
                    self._inc("attachments_bytes_downloaded_total", result.length)
                    self._inc("attachments_bytes_uploaded_total", result.length)
                    results[name] = result
                except Exception as exc:
                    status = getattr(exc, "status", None)
                    if status == 404:
                        self._inc("attachments_missing_total")
                        action = self._config.on_missing_attachment
                        if action == "skip":
                            log_event(
                                logger,
                                "warn",
                                "ATTACHMENT",
                                "attachment not found (skipping): %s" % name,
                                doc_id=doc_id,
                            )
                            return
                    self._inc("attachments_download_errors_total")
                    log_event(
                        logger,
                        "warn",
                        "ATTACHMENT",
                        "streaming transfer error: %s – %s" % (name, exc),
                        doc_id=doc_id,
                    )
                    errors.append(name)
                finally:
                    if resp is not None:
                        resp.release()

        tasks = [_stream_one(name, stub) for name, stub in stubs.items()]
        await asyncio.gather(*tasks)

        if errors:
            if self._config.halt_on_failure:
                raise AttachmentError(
                    "failed to stream attachment(s) for doc %s: %s"
                    % (doc_id, ", ".join(errors))
                )
            log_event(
                logger,
                "warn",
                "ATTACHMENT",
                "%d attachment(s) failed to stream" % len(errors),
                doc_id=doc_id,
            )

        return results

    # -- bulk fetch ---------------------------------------------------------

    async def _fetch_bulk(
        self,
        doc_id: str,
        stubs: dict,
        base_url: str,
        http: RetryableHTTP,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
    ) -> dict[str, bytes]:
        """Fetch attachments via ``_bulk_get?attachments=true``.

        Returns a mapping of attachment_name → raw bytes.
        """
        url = "%s/_bulk_get?attachments=true" % base_url.rstrip("/")
        body = json.dumps({"docs": [{"id": doc_id}]})
        req_headers = dict(headers)
        req_headers["Content-Type"] = "application/json"

        results: dict[str, bytes] = {}
        errors: list[str] = []

        try:
            resp = await http.request(
                "POST",
                url,
                auth=auth,
                headers=req_headers,
                data=body,
            )
            resp_body = await resp.read()
            resp.release()
        except Exception as exc:
            self._inc("attachments_download_errors_total")
            if self._config.halt_on_failure:
                raise AttachmentError(
                    "bulk_get failed for doc %s: %s" % (doc_id, exc)
                ) from exc
            log_event(
                logger,
                "warn",
                "ATTACHMENT",
                "bulk_get error (continuing): %s" % exc,
                doc_id=doc_id,
            )
            return results

        try:
            payload = _json_loads(resp_body)
        except (json.JSONDecodeError, ValueError) as exc:
            self._inc("attachments_download_errors_total")
            if self._config.halt_on_failure:
                raise AttachmentError(
                    "bulk_get JSON parse failed for doc %s: %s" % (doc_id, exc)
                ) from exc
            log_event(
                logger,
                "warn",
                "ATTACHMENT",
                "bulk_get response parse error (continuing): %s" % exc,
                doc_id=doc_id,
            )
            return results

        # Navigate: {"results": [{"docs": [{"ok": {...}}]}]}
        doc_results = payload.get("results", [])
        if not doc_results:
            return results

        inner_docs = doc_results[0].get("docs", [])
        if not inner_docs:
            return results

        inner = inner_docs[0]
        if "error" in inner:
            err = inner["error"]
            self._inc("attachments_download_errors_total")
            msg = "bulk_get returned error for doc %s: %s" % (doc_id, err)
            if self._config.halt_on_failure:
                raise AttachmentError(msg)
            log_event(logger, "warn", "ATTACHMENT", msg, doc_id=doc_id)
            return results

        ok_doc = inner.get("ok", {})
        attachments = ok_doc.get("_attachments", {})

        for name, att in attachments.items():
            if name not in stubs:
                continue

            data_b64 = att.get("data")
            if not data_b64:
                self._inc("attachments_download_errors_total")
                log_event(
                    logger,
                    "warn",
                    "ATTACHMENT",
                    "bulk_get: missing data for attachment %s" % name,
                    doc_id=doc_id,
                )
                errors.append(name)
                continue

            try:
                data = base64.b64decode(data_b64)
            except Exception as exc:
                self._inc("attachments_download_errors_total")
                log_event(
                    logger,
                    "warn",
                    "ATTACHMENT",
                    "bulk_get: base64 decode error for %s: %s" % (name, exc),
                    doc_id=doc_id,
                )
                errors.append(name)
                continue

            stub = stubs[name]

            # Verify length
            if self._config.fetch.verify_length:
                expected_len = stub.get("length", 0)
                if expected_len and len(data) != expected_len:
                    self._inc("attachments_download_errors_total")
                    log_event(
                        logger,
                        "warn",
                        "ATTACHMENT",
                        "attachment length mismatch: %s expected=%d got=%d"
                        % (name, expected_len, len(data)),
                        doc_id=doc_id,
                    )
                    if self._config.halt_on_failure:
                        errors.append(name)
                        continue

            # Verify digest
            if self._config.fetch.verify_digest:
                digest_str = stub.get("digest", "")
                if digest_str and not self._verify_digest(data, digest_str):
                    self._inc("attachments_digest_mismatch_total")
                    log_event(
                        logger,
                        "warn",
                        "ATTACHMENT",
                        "attachment digest mismatch: %s digest=%s" % (name, digest_str),
                        doc_id=doc_id,
                    )
                    if self._config.halt_on_failure:
                        errors.append(name)
                        continue

            self._inc("attachments_downloaded_total")
            results[name] = data

        if errors:
            if self._config.halt_on_failure:
                raise AttachmentError(
                    "failed to fetch attachment(s) for doc %s: %s"
                    % (doc_id, ", ".join(errors))
                )
            log_event(
                logger,
                "warn",
                "ATTACHMENT",
                "%d attachment(s) failed in bulk_get" % len(errors),
                doc_id=doc_id,
            )

        return results

    # -- multipart fetch (stub) ---------------------------------------------

    async def _fetch_multipart(
        self,
        doc_id: str,
        stubs: dict,
        base_url: str,
        http: RetryableHTTP,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
        src: str = "sync_gateway",
    ) -> dict[str, bytes]:
        """Fetch attachments via ``GET /{docId}?attachments=true`` with
        ``Accept: multipart/related``.

        Returns a mapping of attachment_name → raw bytes for the
        attachments that survived filtering (i.e. are in *stubs*).
        """
        from rest.attachment_multipart import (
            MultipartParseError,
            parse_multipart_response,
        )

        url = "%s/%s?attachments=true" % (
            base_url.rstrip("/"),
            quote(doc_id, safe=""),
        )
        req_headers = dict(headers)
        req_headers["Accept"] = "multipart/related"

        resp = await http.request("GET", url, auth=auth, headers=req_headers)
        try:
            doc, raw_attachments = await parse_multipart_response(
                resp,
                expected_names=set(stubs.keys()),
                src=src,
            )
        except MultipartParseError:
            resp.release()
            raise
        finally:
            resp.release()

        # Verify length / digest for each fetched attachment
        results: dict[str, bytes] = {}
        errors: list[str] = []
        for name, data in raw_attachments.items():
            stub = stubs.get(name, {})
            if self._config.fetch.verify_length:
                expected_len = stub.get("length", 0)
                if expected_len and len(data) != expected_len:
                    self._inc("attachments_download_errors_total")
                    log_event(
                        logger,
                        "warn",
                        "ATTACHMENT",
                        "attachment length mismatch: %s expected=%d got=%d"
                        % (name, expected_len, len(data)),
                        doc_id=doc_id,
                    )
                    if self._config.halt_on_failure:
                        errors.append(name)
                        continue

            if self._config.fetch.verify_digest:
                digest_str = stub.get("digest", "")
                if digest_str and not self._verify_digest(data, digest_str):
                    self._inc("attachments_digest_mismatch_total")
                    log_event(
                        logger,
                        "warn",
                        "ATTACHMENT",
                        "attachment digest mismatch: %s digest=%s" % (name, digest_str),
                        doc_id=doc_id,
                    )
                    if self._config.halt_on_failure:
                        errors.append(name)
                        continue

            self._inc("attachments_downloaded_total")
            results[name] = data

        if errors:
            if self._config.halt_on_failure:
                raise AttachmentError(
                    "failed to fetch attachment(s) for doc %s: %s"
                    % (doc_id, ", ".join(errors))
                )
            log_event(
                logger,
                "warn",
                "ATTACHMENT",
                "%d attachment(s) failed verification" % len(errors),
                doc_id=doc_id,
            )

        return results

    # -- detection ----------------------------------------------------------

    @staticmethod
    def _detect(doc: dict) -> dict:
        """Return the ``_attachments`` stub dict, or empty dict."""
        attachments = doc.get("_attachments")
        if isinstance(attachments, dict) and attachments:
            return attachments
        return {}

    # -- filtering ----------------------------------------------------------

    def _apply_filters(self, stubs: dict, doc_id: str) -> dict:
        """Apply configured filters and return the surviving stubs."""
        cfg = self._config.filter
        result: dict = {}

        # Pre-check: max_total_bytes_per_doc
        if cfg.max_total_bytes_per_doc > 0:
            total = sum(s.get("length", 0) for s in stubs.values())
            if total > cfg.max_total_bytes_per_doc:
                log_event(
                    logger,
                    "debug",
                    "ATTACHMENT",
                    "total attachment size %d exceeds max_total_bytes_per_doc %d"
                    % (total, cfg.max_total_bytes_per_doc),
                    doc_id=doc_id,
                )
                return {}

        name_re = re.compile(cfg.name_pattern) if cfg.name_pattern else None

        for name, stub in stubs.items():
            content_type = stub.get("content_type", "application/octet-stream")
            length = stub.get("length", 0)

            # content_type allow-list (glob)
            if cfg.content_types:
                if not any(fnmatch.fnmatch(content_type, p) for p in cfg.content_types):
                    continue

            # content_type reject-list (glob)
            if cfg.reject_content_types:
                if any(
                    fnmatch.fnmatch(content_type, p) for p in cfg.reject_content_types
                ):
                    continue

            # size bounds
            if cfg.min_size_bytes > 0 and length < cfg.min_size_bytes:
                continue
            if cfg.max_size_bytes > 0 and length > cfg.max_size_bytes:
                continue

            # name pattern
            if name_re and not name_re.search(name):
                continue

            result[name] = stub

        return result

    # -- individual fetch ---------------------------------------------------

    async def _fetch_individual(
        self,
        doc_id: str,
        stubs: dict,
        base_url: str,
        http: RetryableHTTP,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
    ) -> dict[str, bytes]:
        """Fetch each attachment individually via GET.

        Returns a mapping of attachment_name → raw bytes.
        """
        sem = asyncio.Semaphore(self._config.fetch.max_concurrent_downloads)
        results: dict[str, bytes] = {}
        errors: list[str] = []

        async def _download(name: str, stub: dict) -> None:
            async with sem:
                url = "%s/%s/%s" % (
                    base_url.rstrip("/"),
                    quote(doc_id, safe=""),
                    quote(name, safe=""),
                )
                try:
                    resp = await http.request(
                        "GET",
                        url,
                        auth=auth,
                        headers=headers,
                    )
                    data = await resp.read()
                    resp.release()
                except Exception as exc:
                    # Handle 404 per config
                    status = getattr(exc, "status", None)
                    if status == 404:
                        self._inc("attachments_missing_total")
                        action = self._config.on_missing_attachment
                        if action == "skip":
                            log_event(
                                logger,
                                "warn",
                                "ATTACHMENT",
                                "attachment not found (skipping): %s" % name,
                                doc_id=doc_id,
                            )
                            return
                        elif action == "fail":
                            errors.append(name)
                            return
                        # "retry" falls through to normal retry logic in RetryableHTTP
                    self._inc("attachments_download_errors_total")
                    log_event(
                        logger,
                        "warn",
                        "ATTACHMENT",
                        "attachment download error: %s – %s" % (name, exc),
                        doc_id=doc_id,
                    )
                    errors.append(name)
                    return

                # Verify length
                if self._config.fetch.verify_length:
                    expected_len = stub.get("length", 0)
                    if expected_len and len(data) != expected_len:
                        self._inc("attachments_download_errors_total")
                        log_event(
                            logger,
                            "warn",
                            "ATTACHMENT",
                            "attachment length mismatch: %s expected=%d got=%d"
                            % (name, expected_len, len(data)),
                            doc_id=doc_id,
                        )
                        if self._config.halt_on_failure:
                            errors.append(name)
                            return

                # Verify digest
                if self._config.fetch.verify_digest:
                    digest_str = stub.get("digest", "")
                    if digest_str and not self._verify_digest(data, digest_str):
                        self._inc("attachments_digest_mismatch_total")
                        log_event(
                            logger,
                            "warn",
                            "ATTACHMENT",
                            "attachment digest mismatch: %s digest=%s"
                            % (name, digest_str),
                            doc_id=doc_id,
                        )
                        if self._config.halt_on_failure:
                            errors.append(name)
                            return

                self._inc("attachments_downloaded_total")
                results[name] = data

        tasks = [_download(name, stub) for name, stub in stubs.items()]
        await asyncio.gather(*tasks)

        if errors:
            if self._config.halt_on_failure:
                raise AttachmentError(
                    "failed to fetch attachment(s) for doc %s: %s"
                    % (doc_id, ", ".join(errors))
                )
            log_event(
                logger,
                "warn",
                "ATTACHMENT",
                "%d attachment(s) failed to download" % len(errors),
                doc_id=doc_id,
            )

        return results

    # -- digest verification ------------------------------------------------

    @staticmethod
    def _verify_digest(data: bytes, digest_str: str) -> bool:
        """Verify data against a CouchDB-style digest string.

        Digest format: ``"md5-<base64>"`` or ``"sha1-<base64>"``.
        """
        if "-" not in digest_str:
            return True  # unknown format, skip verification

        algo, expected_b64 = digest_str.split("-", 1)
        algo = algo.lower()

        if algo == "md5":
            h = hashlib.md5(data).digest()
        elif algo in ("sha1", "sha"):
            h = hashlib.sha1(data).digest()
        else:
            return True  # unsupported algorithm, skip

        actual_b64 = base64.b64encode(h).decode("ascii")
        return actual_b64 == expected_b64

    # -- metrics helper -----------------------------------------------------

    def _inc(self, counter: str, amount: int = 1) -> None:
        if self._metrics:
            self._metrics.inc(counter, amount)
