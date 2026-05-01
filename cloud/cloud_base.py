"""
Base cloud blob storage output forwarder.

Provides shared logic for key templating, JSON serialization,
retry with exponential backoff, metrics proxy, and stats logging.
Each cloud subclass only needs to implement the driver-specific pieces:
client creation, object upload/delete, error classification, and health check.
"""

import abc
import asyncio
import json
import logging
import random
import re
import threading
import time
import urllib.parse
from collections import deque
from datetime import datetime, timezone

from pipeline.pipeline_logging import log_event, infer_operation

try:
    from icecream import ic
except ImportError:
    ic = lambda *a, **kw: None  # noqa: E731

logger = logging.getLogger("changes_worker")


# ── Per-provider / per-job metrics proxy ────────────────────────────────────


class CloudMetrics:
    """
    Lightweight metrics wrapper that tracks counters with (provider, job_id)
    labels while also delegating to the global MetricsCollector for totals.

    Usage in Prometheus:
        # Global totals (backward compat – existing dashboards keep working)
        changes_worker_output_requests_total{src="...",database="..."} 500

        # Per-provider / per-job breakdowns
        changes_worker_cloud_uploads_total{provider="s3",job_id="orders_archive"} 300
    """

    _registry_lock = threading.Lock()
    _registry: list["CloudMetrics"] = []

    def __init__(self, provider: str, job_id: str, global_metrics=None):
        self.provider = provider
        self.job_id = job_id or provider
        self._global = global_metrics
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._resp_times: deque[float] = deque(maxlen=10_000)

        with CloudMetrics._registry_lock:
            CloudMetrics._registry.append(self)

    def inc(self, name: str, value: int = 1) -> None:
        """Increment both the local labeled counter AND the global total."""
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + value
        if self._global:
            self._global.inc(name, value)

    def record_output_response_time(self, seconds: float) -> None:
        with self._lock:
            self._resp_times.append(seconds)
        if self._global:
            self._global.record_output_response_time(seconds)

    def snapshot(self) -> tuple[dict[str, int], list[float]]:
        """Return a copy of counters + resp_times under the lock."""
        with self._lock:
            return dict(self._counters), list(self._resp_times)

    def unregister(self) -> None:
        with CloudMetrics._registry_lock:
            try:
                CloudMetrics._registry.remove(self)
            except ValueError:
                pass

    @classmethod
    def render_all(cls) -> str:
        """
        Render per-provider/per-job cloud metrics in Prometheus text format.

        Returns a string of lines that the main MetricsCollector.render()
        can append to its output.
        """
        with cls._registry_lock:
            instances = list(cls._registry)

        if not instances:
            return ""

        snapshots: list[tuple[str, str, dict[str, int], list[float]]] = []
        for inst in instances:
            counters, resp_times = inst.snapshot()
            snapshots.append((inst.provider, inst.job_id, counters, resp_times))

        # Gather all counter names across all instances
        all_counter_names: set[str] = set()
        for _, _, counters, _ in snapshots:
            all_counter_names.update(counters.keys())

        lines: list[str] = []

        # Emit one HELP/TYPE block per counter, with one line per (provider, job_id)
        for name in sorted(all_counter_names):
            prom_name = f"changes_worker_cloud_{name}"
            lines.append(
                f"# HELP {prom_name} Cloud output counter: {name} (per provider/job)"
            )
            lines.append(f"# TYPE {prom_name} counter")
            for provider, job_id, counters, _ in snapshots:
                val = counters.get(name, 0)
                if val:
                    lines.append(
                        f'{prom_name}{{provider="{provider}",job_id="{job_id}"}} {val}'
                    )

        # Emit per-instance response time summaries
        has_resp = any(rt for _, _, _, rt in snapshots)
        if has_resp:
            prom_name = "changes_worker_cloud_output_response_time_seconds"
            lines.append(
                f"# HELP {prom_name} Cloud output response time (per provider/job)"
            )
            lines.append(f"# TYPE {prom_name} summary")
            for provider, job_id, _, resp_times in snapshots:
                if not resp_times:
                    continue
                s = sorted(resp_times)
                count = len(s)
                total = sum(s)
                for q in (0.5, 0.9, 0.99):
                    idx = int(q * (count - 1))
                    lines.append(
                        f'{prom_name}{{provider="{provider}",job_id="{job_id}",'
                        f'quantile="{q}"}} {s[idx]:.6f}'
                    )
                lines.append(
                    f'{prom_name}_sum{{provider="{provider}",job_id="{job_id}"}} {total:.6f}'
                )
                lines.append(
                    f'{prom_name}_count{{provider="{provider}",job_id="{job_id}"}} {count}'
                )

        return "\n".join(lines)


# ── Key template helpers ────────────────────────────────────────────────────

_KEY_VAR_RE = re.compile(r"\{(\w+)\}")


def _sanitize_key_part(value: str) -> str:
    """Sanitize a value for use in an object key (replace : with _, URL-encode others)."""
    value = value.replace(":", "_")
    return urllib.parse.quote(value, safe="/._-")


def render_key(
    template: str,
    doc: dict,
    cfg: dict,
    sanitize: bool = True,
    extra_vars: dict[str, object] | None = None,
) -> str:
    """Render an object key from a template and document fields.

    *extra_vars* (if provided) are merged after the standard variables,
    allowing callers to add context-specific placeholders such as
    ``{attachment_name}`` or ``{content_type}``.
    """
    now = datetime.now(timezone.utc)
    doc_id = doc.get("_id", doc.get("id", "unknown"))
    variables = {
        "doc_id": doc_id,
        "rev": doc.get("_rev", doc.get("rev", "")),
        "seq": str(doc.get("_seq", doc.get("seq", ""))),
        "timestamp": str(int(now.timestamp())),
        "iso_date": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "year": now.strftime("%Y"),
        "month": now.strftime("%m"),
        "day": now.strftime("%d"),
        "scope": cfg.get("scope", ""),
        "collection": cfg.get("collection", ""),
        "database": cfg.get("database", ""),
        "prefix": cfg.get("key_prefix", "couchdb-changes"),
    }
    if extra_vars:
        variables.update({k: str(v) for k, v in extra_vars.items()})

    def _replace(m):
        val = variables.get(m.group(1), m.group(0))
        return _sanitize_key_part(val) if sanitize else val

    key = _KEY_VAR_RE.sub(_replace, template)
    key_bytes = len(key.encode("utf-8"))
    if key_bytes > 1024:
        log_event(
            logger,
            "warn",
            "OUTPUT",
            "rendered key exceeds 1024-byte cloud provider limit (%d bytes)"
            % key_bytes,
        )
    return key


# ── Abstract base forwarder ─────────────────────────────────────────────────


class BaseCloudForwarder(abc.ABC):
    """
    Abstract async cloud blob storage output forwarder.

    Subclasses must implement:
        _provider          – property returning provider name (e.g. "s3")
        _get_provider_cfg()– extract the provider-specific config dict
        _create_client()   – create the async/sync cloud client
        _close_client()    – close/cleanup the client
        _upload_object()   – upload a single object
        _delete_object()   – delete a single object
        _test_bucket()     – check that the bucket/container is accessible
        _is_transient()    – classify whether an exception is retryable
        _error_class()     – return a short error classification string
    """

    def __init__(self, out_cfg: dict, dry_run: bool = False, metrics=None):
        self._dry_run = dry_run
        self._halt_on_failure = out_cfg.get("halt_on_failure", True)
        self._data_error_action = out_cfg.get("data_error_action", "dlq")
        self._metrics_global = metrics

        cfg = self._get_provider_cfg(out_cfg)
        self._key_prefix = cfg.get("key_prefix", "couchdb-changes")
        self._key_template = cfg.get("key_template", "{prefix}/{doc_id}.json")
        self._key_sanitize = cfg.get("key_sanitize", True)
        self._content_type = cfg.get("content_type", "application/json")
        self._on_delete = cfg.get("on_delete", "delete")
        self._max_retries = cfg.get("max_retries", 3)
        self._backoff_base = cfg.get("backoff_base_seconds", 0.5)
        self._backoff_max = cfg.get("backoff_max_seconds", 10)

        # Batch config
        batch_cfg = cfg.get("batch", {})
        self._batch_enabled = batch_cfg.get("enabled", False)
        self._batch_max_docs = batch_cfg.get("max_docs", 100)
        self._batch_max_bytes = batch_cfg.get("max_bytes", 1_048_576)  # 1 MB
        self._batch_max_seconds = batch_cfg.get("max_seconds", 5.0)
        self._batch_buffer: list[bytes] = []
        self._batch_bytes: int = 0
        self._batch_lock = asyncio.Lock()
        self._batch_timer_task: asyncio.Task | None = None
        self._sse_error_logged = False

        self._job_id = out_cfg.get("job_id", "")

        self._metrics: CloudMetrics | None = None
        self._resp_times: deque[float] = deque(maxlen=10_000)
        self._lock = asyncio.Lock()

        ic(
            "BaseCloudForwarder init",
            self._key_prefix,
            self._key_template,
            self._batch_enabled,
            self._dry_run,
        )

    def _init_metrics(self) -> None:
        """Call from subclass __init__ after the provider property is available."""
        self._metrics = CloudMetrics(
            provider=self._provider,
            job_id=self._job_id,
            global_metrics=self._metrics_global,
        )

    @property
    @abc.abstractmethod
    def _provider(self) -> str:
        """Return the provider name (e.g. 's3', 'gcs')."""

    @property
    def _mode(self) -> str:
        """Alias for _provider used by main.py logging."""
        return self._provider

    @abc.abstractmethod
    def _get_provider_cfg(self, out_cfg: dict) -> dict:
        """Extract the provider-specific config dict from out_cfg."""

    # ── Client lifecycle (subclass hooks) ───────────────────────────────

    @abc.abstractmethod
    async def _create_client(self) -> None:
        """Create the cloud client."""

    @abc.abstractmethod
    async def _close_client(self) -> None:
        """Close the cloud client."""

    async def connect(self) -> None:
        """Create the cloud client and validate bucket access."""
        ic("connect", self._provider)
        await self._create_client()
        try:
            await self._test_bucket()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            ic(
                "connect: bucket test failed",
                self._provider,
                type(exc).__name__,
                str(exc),
            )
            log_event(
                logger,
                "error",
                "OUTPUT",
                "cloud bucket/container not accessible",
                mode=self._provider,
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            await self._close_client()
            raise
        log_event(
            logger, "info", "OUTPUT", "cloud client connected", mode=self._provider
        )

    async def close(self) -> None:
        """Close the cloud client, flush pending batch, and unregister metrics."""
        ic("close", self._provider)
        # Flush any remaining batch buffer
        if self._batch_enabled and self._batch_buffer:
            try:
                await self._flush_batch()
            except Exception as exc:
                ic("close: batch flush failed", type(exc).__name__, str(exc))
                log_event(
                    logger,
                    "warn",
                    "OUTPUT",
                    "failed to flush batch buffer on close",
                    mode=self._provider,
                    error_detail=f"{type(exc).__name__}: {exc}",
                )
        # Cancel batch timer
        if self._batch_timer_task and not self._batch_timer_task.done():
            self._batch_timer_task.cancel()
            try:
                await self._batch_timer_task
            except asyncio.CancelledError:
                pass
        try:
            await self._close_client()
        finally:
            if self._metrics:
                self._metrics.unregister()
        log_event(logger, "info", "OUTPUT", "cloud client closed", mode=self._provider)

    # ── Object operations (subclass hooks) ──────────────────────────────

    @abc.abstractmethod
    async def _upload_object(
        self, key: str, body: bytes, content_type: str, metadata: dict
    ) -> dict:
        """Upload a single object. Return response info dict."""

    @abc.abstractmethod
    async def _delete_object(self, key: str) -> dict:
        """Delete a single object. Return response info dict."""

    @abc.abstractmethod
    async def _test_bucket(self) -> bool:
        """Check that the bucket/container is accessible. Raise on failure."""

    # ── Error classification (subclass hooks) ───────────────────────────

    @abc.abstractmethod
    def _is_transient(self, exc: Exception) -> bool:
        """Return True if the error is transient and worth retrying."""

    @abc.abstractmethod
    def _error_class(self, exc: Exception) -> str:
        """Return a short classification string for the error."""

    # ── Health check ────────────────────────────────────────────────────

    async def test_reachable(self) -> bool:
        """Test that the cloud store is reachable."""
        ic("test_reachable", self._provider)
        try:
            await self._test_bucket()
            ic("test_reachable: OK", self._provider)
            log_event(
                logger, "info", "OUTPUT", "cloud store reachable", mode=self._provider
            )
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            ic("test_reachable: FAIL", self._provider, type(exc).__name__, str(exc))
            log_event(
                logger,
                "error",
                "OUTPUT",
                "cloud store unreachable",
                mode=self._provider,
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            return False

    # ── Key rendering ───────────────────────────────────────────────────

    def _render_key(self, doc: dict) -> str:
        cfg = {
            "key_prefix": self._key_prefix,
        }
        return render_key(self._key_template, doc, cfg, sanitize=self._key_sanitize)

    # ── Serialization ───────────────────────────────────────────────────

    def _serialize(self, doc: dict) -> bytes:
        return json.dumps(doc, default=str, ensure_ascii=False).encode("utf-8")

    # ── send() — the main document processing method ────────────────────

    async def send(self, doc: dict, method: str = "PUT") -> dict:
        """
        Process a single document: serialize and upload/delete.

        Transient errors are retried with exponential backoff.
        Permanent errors return immediately for DLQ routing.

        Returns result dict with 'ok' bool plus 'retryable' and
        'error_class' on failure.
        """
        ic("send", doc.get("_id", doc.get("id", "unknown")) if doc else "None", method)
        if doc is None:
            log_event(
                logger,
                "debug",
                "OUTPUT",
                "received None doc – skipped",
                doc_id="unknown",
            )
            if self._metrics:
                self._metrics.inc("output_skipped_total")
            return {"ok": True, "doc_id": "unknown", "skipped": True}

        doc_id = doc.get("_id", doc.get("id", "unknown"))
        is_delete = method == "DELETE"

        key = self._render_key(doc)

        if is_delete:
            if self._on_delete == "ignore":
                ic("send: delete ignored", doc_id, key)
                log_event(
                    logger,
                    "debug",
                    "OUTPUT",
                    "delete ignored (on_delete=ignore) – skipped",
                    doc_id=doc_id,
                )
                if self._metrics:
                    self._metrics.inc("output_skipped_total")
                return {"ok": True, "doc_id": doc_id, "key": key, "skipped": True}
            if self._on_delete == "tombstone":
                ic("send: delete → tombstone", doc_id, key)
                is_delete = False
                doc = {
                    "_id": doc_id,
                    "_deleted": True,
                    "_rev": doc.get("_rev", doc.get("rev", "")),
                    "deleted_at": datetime.now(timezone.utc).isoformat(),
                }

        if self._dry_run:
            action = "DELETE" if is_delete else "PUT"
            log_event(
                logger,
                "debug",
                "OUTPUT",
                "[DRY RUN] %s %s" % (action, key),
                doc_id=doc_id,
                mode=self._provider,
            )
            return {"ok": True, "doc_id": doc_id, "key": key, "dry_run": True}

        # Batch mode: accumulate docs and flush when threshold is hit
        if self._batch_enabled and not is_delete:
            return await self._batch_add(doc, doc_id, key)

        # -- Execute with retry for transient errors --
        return await self._send_with_retry(doc, doc_id, key, is_delete)

    async def _batch_add(self, doc: dict, doc_id: str, key: str) -> dict:
        """Add a document to the batch buffer; flush if thresholds are met."""
        body = self._serialize(doc)
        body_len = len(body)

        flush_needed = False
        async with self._batch_lock:
            self._batch_buffer.append(body)
            self._batch_bytes += body_len

            if (
                len(self._batch_buffer) >= self._batch_max_docs
                or self._batch_bytes >= self._batch_max_bytes
            ):
                flush_needed = True

            # Start timer on first doc in buffer
            if len(self._batch_buffer) == 1 and not flush_needed:
                self._batch_timer_task = asyncio.ensure_future(
                    self._batch_timer_callback()
                )

        if self._metrics:
            self._metrics.inc("output_requests_total")
            self._metrics.inc("bytes_uploaded_total", body_len)
            self._metrics.inc("bytes_output_total", body_len)

        if flush_needed:
            await self._flush_batch()

        ic("batch_add", doc_id, body_len, len(self._batch_buffer))
        return {"ok": True, "doc_id": doc_id, "key": key, "batched": True}

    async def _batch_timer_callback(self) -> None:
        """Timer that flushes the batch after max_seconds."""
        try:
            await asyncio.sleep(self._batch_max_seconds)
            async with self._batch_lock:
                has_items = len(self._batch_buffer) > 0
            if has_items:
                ic("batch_timer: flushing", len(self._batch_buffer))
                await self._flush_batch()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            ic("batch_timer: flush failed", type(exc).__name__, str(exc))
            log_event(
                logger,
                "error",
                "OUTPUT",
                "batch timer flush failed",
                mode=self._provider,
                error_detail=f"{type(exc).__name__}: {exc}",
            )

    async def _flush_batch(self) -> dict:
        """Flush the batch buffer as a single NDJSON upload."""
        async with self._batch_lock:
            if not self._batch_buffer:
                return {"ok": True, "flushed": 0}
            items = list(self._batch_buffer)
            total_bytes = self._batch_bytes
            # Cancel pending timer
            if self._batch_timer_task and not self._batch_timer_task.done():
                self._batch_timer_task.cancel()
                self._batch_timer_task = None

        # Build NDJSON body
        ndjson_body = b"\n".join(items) + b"\n"
        now = datetime.now(timezone.utc)
        batch_key = (
            f"{self._key_prefix}/batch_{now.strftime('%Y%m%dT%H%M%SZ')}"
            f"_{len(items)}.ndjson"
        )
        metadata = {
            "batch_size": str(len(items)),
            "batch_bytes": str(total_bytes),
        }

        ic("flush_batch", batch_key, len(items), len(ndjson_body))
        log_event(
            logger,
            "debug",
            "OUTPUT",
            "flushing batch",
            mode=self._provider,
            batch_size=len(items),
            bytes=len(ndjson_body),
        )

        t_start = time.monotonic()
        last_exc: Exception | None = None

        for attempt in range(1, self._max_retries + 1):
            try:
                await self._upload_object(
                    batch_key,
                    ndjson_body,
                    "application/x-ndjson",
                    metadata,
                )
                elapsed_ms = (time.monotonic() - t_start) * 1000
                async with self._lock:
                    self._resp_times.append(elapsed_ms)
                if self._metrics:
                    self._metrics.inc("uploads_total")
                    self._metrics.inc("batches_flushed_total")
                    self._metrics.inc("output_success_total", len(items))
                    self._metrics.record_output_response_time(elapsed_ms / 1000)
                async with self._batch_lock:
                    self._batch_buffer = []
                    self._batch_bytes = 0
                ic("flush_batch: OK", batch_key, len(items), round(elapsed_ms, 1))
                log_event(
                    logger,
                    "info",
                    "OUTPUT",
                    "batch uploaded",
                    mode=self._provider,
                    batch_size=len(items),
                    bytes=len(ndjson_body),
                    elapsed_ms=round(elapsed_ms, 1),
                )
                return {
                    "ok": True,
                    "key": batch_key,
                    "docs": len(items),
                    "bytes": len(ndjson_body),
                    "elapsed_ms": round(elapsed_ms, 1),
                }
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_exc = exc
                if not self._is_transient(exc):
                    ic(
                        "flush_batch: permanent error",
                        batch_key,
                        type(exc).__name__,
                        str(exc),
                    )
                    log_event(
                        logger,
                        "error",
                        "OUTPUT",
                        "batch upload permanent error",
                        mode=self._provider,
                        batch_size=len(items),
                        error_detail=f"{type(exc).__name__}: {exc}",
                    )
                    break
                ic(
                    "flush_batch: transient error, retry",
                    attempt,
                    self._max_retries,
                    type(exc).__name__,
                )
                log_event(
                    logger,
                    "warn",
                    "OUTPUT",
                    "batch upload transient error",
                    mode=self._provider,
                    attempt=attempt,
                    error_detail=f"{type(exc).__name__}: {exc}",
                )
                if attempt < self._max_retries:
                    delay = min(
                        self._backoff_base * (2 ** (attempt - 1)),
                        self._backoff_max,
                    )
                    delay *= 0.5 + random.random()  # jitter: 50–150% of base delay
                    await asyncio.sleep(delay)

        # Batch upload failed
        if self._metrics:
            self._metrics.inc("output_errors_total")
            self._metrics.inc("batch_errors_total")
        log_event(
            logger,
            "error",
            "OUTPUT",
            "batch upload failed after retries",
            mode=self._provider,
            batch_size=len(items),
            error_detail=(
                f"{type(last_exc).__name__}: {last_exc}" if last_exc else "unknown"
            ),
        )
        return {
            "ok": False,
            "key": batch_key,
            "docs": len(items),
            "error": str(last_exc)[:500] if last_exc else "unknown",
        }

    async def _send_with_retry(
        self, doc: dict, doc_id: str, key: str, is_delete: bool
    ) -> dict:
        """Upload or delete a single object with retry + backoff."""
        t_start = time.monotonic()
        last_exc: Exception | None = None

        for attempt in range(1, self._max_retries + 1):
            try:
                if is_delete:
                    await self._delete_object(key)
                    action = "DELETE"
                else:
                    body = self._serialize(doc)
                    metadata = {"doc_id": doc_id}
                    rev = doc.get("_rev", doc.get("rev", ""))
                    if rev:
                        metadata["rev"] = rev
                    await self._upload_object(key, body, self._content_type, metadata)
                    action = "PUT"

                # Success
                elapsed_ms = (time.monotonic() - t_start) * 1000
                async with self._lock:
                    self._resp_times.append(elapsed_ms)
                if self._metrics:
                    self._metrics.inc("output_requests_total")
                    self._metrics.inc("output_success_total")
                    if is_delete:
                        self._metrics.inc("output_delete_total")
                        self._metrics.inc("deletes_forwarded_total")
                    else:
                        body_len = len(body)
                        self._metrics.inc("uploads_total")
                        self._metrics.inc("bytes_uploaded_total", body_len)
                        self._metrics.inc("bytes_output_total", body_len)
                    self._metrics.record_output_response_time(elapsed_ms / 1000)

                ic("send: OK", doc_id, action, key, round(elapsed_ms, 1))
                log_event(
                    logger,
                    "debug",
                    "OUTPUT",
                    "cloud %s" % action,
                    operation=action,
                    doc_id=doc_id,
                    elapsed_ms=round(elapsed_ms, 1),
                    mode=self._provider,
                    http_method=action,
                )
                return {
                    "ok": True,
                    "doc_id": doc_id,
                    "key": key,
                    "action": action,
                    "elapsed_ms": round(elapsed_ms, 1),
                }

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_exc = exc
                eclass = self._error_class(exc)

                if not self._is_transient(exc):
                    elapsed_ms = (time.monotonic() - t_start) * 1000
                    if self._metrics:
                        self._metrics.inc("output_requests_total")
                        self._metrics.inc("output_errors_total")
                        self._metrics.inc("permanent_errors_total")
                    ic(
                        "send: permanent error",
                        doc_id,
                        eclass,
                        type(exc).__name__,
                        str(exc),
                    )
                    log_event(
                        logger,
                        "error",
                        "OUTPUT",
                        "permanent error",
                        doc_id=doc_id,
                        mode=self._provider,
                        error_detail=f"{type(exc).__name__}: {exc}",
                    )
                    # §3.22: First-occurrence warning for SSE/KMS config errors
                    if not self._sse_error_logged:
                        exc_str = str(exc).lower()
                        if "kms" in exc_str or "sse" in exc_str or "encrypt" in exc_str:
                            self._sse_error_logged = True
                            log_event(
                                logger,
                                "critical",
                                "OUTPUT",
                                "Server-side encryption config error — check "
                                "'server_side_encryption' and 'kms_key_id' in output config.",
                                mode=self._provider,
                                error_detail=f"{type(exc).__name__}: {exc}",
                            )

                    return {
                        "ok": False,
                        "doc_id": doc_id,
                        "key": key,
                        "error": str(exc)[:500],
                        "retryable": False,
                        "error_class": eclass,
                        "data_error_action": self._data_error_action,
                    }

                # Transient error — retry
                if self._metrics:
                    self._metrics.inc("transient_errors_total")
                    self._metrics.inc("retries_total")

                ic("send: transient error", doc_id, eclass, attempt, self._max_retries)
                log_event(
                    logger,
                    "warn",
                    "OUTPUT",
                    "transient error – retrying",
                    doc_id=doc_id,
                    mode=self._provider,
                    attempt=attempt,
                    error_detail=f"{type(exc).__name__}: {exc}",
                )

                if attempt < self._max_retries:
                    delay = min(
                        self._backoff_base * (2 ** (attempt - 1)),
                        self._backoff_max,
                    )
                    delay *= 0.5 + random.random()  # jitter: 50–150% of base delay
                    await asyncio.sleep(delay)

        # All retries exhausted
        elapsed_ms = (time.monotonic() - t_start) * 1000
        eclass = self._error_class(last_exc) if last_exc else "unknown"
        if self._metrics:
            self._metrics.inc("output_requests_total")
            self._metrics.inc("output_errors_total")
            self._metrics.inc("retry_exhausted_total")
        ic("send: retries exhausted", doc_id, eclass, self._max_retries)
        log_event(
            logger,
            "error",
            "OUTPUT",
            "retries exhausted",
            doc_id=doc_id,
            mode=self._provider,
            attempt=self._max_retries,
            error_detail=(
                f"{type(last_exc).__name__}: {last_exc}" if last_exc else "unknown"
            ),
        )

        if self._halt_on_failure:
            from rest import OutputEndpointDown

            raise OutputEndpointDown(
                f"{self._provider.upper()} retries exhausted for {doc_id} "
                f"[{eclass}]: {last_exc}"
            ) from last_exc
        return {
            "ok": False,
            "doc_id": doc_id,
            "key": key,
            "error": str(last_exc)[:500],
            "retryable": False,
            "error_class": eclass,
        }

    # ── stats logging ───────────────────────────────────────────────────

    def log_stats(self, force_info: bool = False) -> None:
        """Log accumulated response time statistics."""
        if not self._resp_times:
            return
        n = len(self._resp_times)
        avg = sum(self._resp_times) / n
        lo = min(self._resp_times)
        hi = max(self._resp_times)
        log_event(
            logger,
            "info" if force_info else "debug",
            "OUTPUT",
            "%s stats: %d ops | avg=%.1fms | min=%.1fms | max=%.1fms"
            % (self._provider.upper(), n, avg, lo, hi),
            mode=self._provider,
        )
