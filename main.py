#!/usr/bin/env python3
"""
Production-ready _changes feed processor for:
  - Couchbase Sync Gateway
  - Capella App Services
  - Couchbase Edge Server

Supports longpoll with configurable intervals, checkpoint management,
bulk_get fallback, async parallel or sequential processing, and
forwarding results to external systems (HTTP, RDBMS, Cloud).
"""

__version__ = "2.3.0"

import argparse
import asyncio
import gc
import hashlib
import json
import logging
import os
import signal
import ssl
import sys
import time
import threading
from collections import deque
from pathlib import Path

import psutil

import aiohttp
import aiohttp.web
from icecream import ic

from rest import (
    OutputForwarder,
    DeadLetterQueue,
    VALID_OUTPUT_FORMATS,
)
from rest.output_http import check_serialization_library
from rest.api_v2 import (
    api_get_inputs_changes,
    api_post_inputs_changes,
    api_put_inputs_changes_entry,
    api_delete_inputs_changes_entry,
    api_get_outputs,
    api_post_outputs,
    api_put_outputs_entry,
    api_delete_outputs_entry,
    api_get_jobs,
    api_get_job,
    api_post_jobs,
    api_put_job,
    api_delete_job,
    api_refresh_job_input,
    api_refresh_job_output,
)
from rest.api_v2_jobs_control import register_job_control_routes
from rest.changes_http import (
    ShutdownRequested,
    RetryableHTTP,
    ClientHTTPError,
    RedirectHTTPError,
    ServerHTTPError,
    fetch_docs,
    fetch_db_update_seq,
    _fetch_docs_bulk_get,
    _fetch_docs_individually,
    _build_changes_body,
    _parse_seq_number,
    _sleep_with_backoff,
    _process_changes_batch,
    _catch_up_normal,
    _consume_continuous_stream,
    _consume_websocket_stream,
    _replay_dead_letter_queue,
    _sleep_or_shutdown,
    _chunked,
    _json_loads,
    _maybe_backpressure,
)
from rest import determine_method  # re-export for backward compat
from storage.cbl_store import (
    USE_CBL,
    CBLStore,
    CBLMaintenanceScheduler,
    close_db,
    migrate_files_to_cbl,
    migrate_default_to_collections,
    migrate_mappings_to_jobs,
    COLL_CHECKPOINTS,
)
from rest.attachment_config import parse_attachment_config
from rest.attachments import AttachmentProcessor
from pipeline.pipeline_logging import (
    configure_logging,
    generate_session_id,
    get_redactor,
    get_session_id,
    log_event,
    set_job_tag,
    set_session_id,
)
from pipeline.pipeline_manager import PipelineManager

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("changes_worker")


# ---------------------------------------------------------------------------
# Prometheus Metrics
# ---------------------------------------------------------------------------


class MetricsCollector:
    """
    Thread-safe metrics collector that renders Prometheus text exposition format.

    All counters/gauges are stored as simple numeric values and rendered
    on demand when the /_metrics endpoint is hit.
    """

    def __init__(
        self, src: str, database: str, log_dir: str = "logs", cbl_db_dir: str = ""
    ):
        self._lock = threading.Lock()
        self._start_time = time.monotonic()
        self._labels = f'src="{src}",database="{database}"'
        self._process = psutil.Process()
        self._log_dir = log_dir
        self._cbl_db_dir = cbl_db_dir

        # Counters (monotonically increasing)
        self.poll_cycles_total: int = 0
        self.poll_errors_total: int = 0
        self.changes_received_total: int = 0
        self.changes_processed_total: int = 0
        self.changes_filtered_total: int = 0
        self.changes_deleted_total: int = 0
        self.changes_removed_total: int = 0
        self.output_requests_total: int = 0
        self.output_errors_total: int = 0
        self.docs_fetched_total: int = 0
        self.checkpoint_saves_total: int = 0
        self.checkpoint_save_errors_total: int = 0
        self.retries_total: int = 0
        self.retry_exhausted_total: int = 0

        # Output by HTTP method (PUT / DELETE)
        self.output_put_total: int = 0
        self.output_delete_total: int = 0
        self.output_put_errors_total: int = 0
        self.output_delete_errors_total: int = 0
        self.output_success_total: int = 0
        self.output_skipped_total: int = 0
        self.dead_letter_total: int = 0
        self.dlq_write_failures_total: int = 0
        self.dlq_pending_count: int = 0
        self.dlq_last_write_epoch: float = 0  # unix timestamp of last DLQ write

        # Batch processing
        self.batches_total: int = 0
        self.batches_failed_total: int = 0

        # Bytes tracking
        self.bytes_received_total: int = 0  # bytes from _changes + bulk_get/GETs
        self.bytes_output_total: int = 0  # bytes sent to output endpoint

        # _changes feed content tracking (always counted, regardless of filter settings)
        self.feed_deletes_seen_total: int = 0  # changes with deleted=true in the feed
        self.feed_removes_seen_total: int = 0  # changes with removed=true in the feed
        self.deletes_forwarded_total: int = (
            0  # Tombstones forwarded to output (deleted=true, not filtered)
        )

        # Doc fetch
        self.doc_fetch_requests_total: int = 0
        self.doc_fetch_errors_total: int = 0
        self.docs_fetch_skipped_total: int = 0

        # Mapper (DB mode)
        self.mapper_matched_total: int = 0
        self.mapper_skipped_total: int = 0
        self.mapper_errors_total: int = 0
        self.mapper_ops_total: int = 0
        self.output_ops_total: int = 0

        # DB transaction retry / error classification
        self.db_retries_total: int = 0
        self.db_retry_exhausted_total: int = 0
        self.db_transient_errors_total: int = 0
        self.db_permanent_errors_total: int = 0
        self.db_pool_reconnects_total: int = 0

        # Stream (continuous/websocket)
        self.stream_reconnects_total: int = 0
        self.stream_messages_total: int = 0
        self.stream_parse_errors_total: int = 0

        # Health check probes
        self.health_probes_total: int = 0
        self.health_probe_failures_total: int = 0

        # Auth tracking – inbound (gateway / _changes feed)
        self.inbound_auth_total: int = 0
        self.inbound_auth_success_total: int = 0
        self.inbound_auth_failure_total: int = 0

        # Auth tracking – outbound (output endpoint)
        self.outbound_auth_total: int = 0
        self.outbound_auth_success_total: int = 0
        self.outbound_auth_failure_total: int = 0

        # Checkpoint loads
        self.checkpoint_loads_total: int = 0
        self.checkpoint_load_errors_total: int = 0

        # Attachment processing
        self.attachments_detected_total: int = 0
        self.attachments_downloaded_total: int = 0
        self.attachments_download_errors_total: int = 0
        self.attachments_uploaded_total: int = 0
        self.attachments_upload_errors_total: int = 0
        self.attachments_bytes_downloaded_total: int = 0
        self.attachments_bytes_uploaded_total: int = 0
        self.attachments_post_process_total: int = 0
        self.attachments_post_process_errors_total: int = 0
        self.attachments_skipped_total: int = 0
        self.attachments_missing_total: int = 0
        self.attachments_digest_mismatch_total: int = 0
        self.attachments_stale_total: int = 0
        self.attachments_post_process_skipped_total: int = 0
        self.attachments_conflict_retries_total: int = 0
        self.attachments_orphaned_uploads_total: int = 0
        self.attachments_partial_success_total: int = 0
        self.attachments_temp_files_cleaned_total: int = 0

        # Eventing (JS OnUpdate/OnDelete handlers)
        self.eventing_invocations_total: int = (
            0  # total handler calls (update + delete)
        )
        self.eventing_updates_total: int = 0  # OnUpdate calls
        self.eventing_deletes_total: int = 0  # OnDelete calls
        self.eventing_passed_total: int = 0  # docs that passed through
        self.eventing_rejected_total: int = 0  # docs rejected by handler
        self.eventing_errors_total: int = 0  # JS exceptions
        self.eventing_timeouts_total: int = 0  # handler exceeded timeout_ms
        self.eventing_halts_total: int = 0  # on_error/on_timeout=halt triggered
        self.eventing_v8_heap_used_bytes: int = (
            0  # V8 heap used (gauge, updated periodically)
        )
        self.eventing_v8_heap_total_bytes: int = 0  # V8 heap total (gauge)

        # Recursion guard (write-back echo suppression)
        self.recursion_guard_suppressed_total: int = 0

        # Flood / backpressure detection
        self.largest_batch_received: int = 0
        self.flood_batches_total: int = 0  # batches exceeding flood threshold
        self.flood_threshold: int = 10000  # configurable via set()

        # Output backpressure
        self.backpressure_delays_total: int = 0
        self.backpressure_delay_seconds_total: float = 0.0
        self.backpressure_active: int = 0  # 1 when currently throttling

        # Gauges (can go up and down)
        self.changes_pending: int = 0  # received - processed (backpressure)
        self.last_batch_size: int = 0
        self.last_poll_timestamp: float = 0.0
        self.checkpoint_seq: str = "0"
        self.output_endpoint_up: int = 1
        self.active_tasks: int = 0

        # Output response time tracking (for summary) – capped to avoid unbounded growth
        self._output_resp_times: deque[float] = deque(maxlen=10000)

        # Stage timing deques
        self._changes_request_times: deque[float] = deque(maxlen=10000)
        self._batch_processing_times: deque[float] = deque(maxlen=10000)
        self._doc_fetch_times: deque[float] = deque(maxlen=10000)
        self._health_probe_times: deque[float] = deque(maxlen=10000)

        # Auth timing deques
        self._inbound_auth_times: deque[float] = deque(maxlen=10000)
        self._outbound_auth_times: deque[float] = deque(maxlen=10000)

        # Eventing timing deque
        self._eventing_handler_times: deque[float] = deque(maxlen=10000)

        # Timing summary cache: avoid re-sorting unchanged deques on every scrape
        self._timing_versions: dict[str, int] = {
            "output": 0,
            "changes": 0,
            "batch": 0,
            "fetch": 0,
            "health": 0,
            "inbound_auth": 0,
            "outbound_auth": 0,
            "eventing": 0,
        }
        self._timing_stats_cache: dict[str, tuple[int, int, float, list[float]]] = {}

        # System metrics cache (TTL=15s for psutil, 60s for directory walks)
        self._system_metrics_cache: dict | None = None
        self._system_metrics_cache_time: float = 0
        self._dir_walk_cache: dict | None = None
        self._dir_walk_cache_time: float = 0
        # Process-level psutil cache (TTL=15s — same as system metrics)
        self._process_metrics_cache: dict | None = None
        self._process_metrics_cache_time: float = 0

    def inc(self, name: str, value: int = 1) -> None:
        with self._lock:
            setattr(self, name, getattr(self, name) + value)

    def set(self, name: str, value) -> None:
        with self._lock:
            setattr(self, name, value)

    def record_output_response_time(self, seconds: float) -> None:
        with self._lock:
            self._output_resp_times.append(seconds)
            self._timing_versions["output"] += 1

    def record_changes_request_time(self, seconds: float) -> None:
        with self._lock:
            self._changes_request_times.append(seconds)
            self._timing_versions["changes"] += 1

    def record_batch_processing_time(self, seconds: float) -> None:
        with self._lock:
            self._batch_processing_times.append(seconds)
            self._timing_versions["batch"] += 1

    def record_doc_fetch_time(self, seconds: float) -> None:
        with self._lock:
            self._doc_fetch_times.append(seconds)
            self._timing_versions["fetch"] += 1

    def record_health_probe_time(self, seconds: float) -> None:
        with self._lock:
            self._health_probe_times.append(seconds)
            self._timing_versions["health"] += 1

    def record_inbound_auth_time(self, seconds: float) -> None:
        with self._lock:
            self._inbound_auth_times.append(seconds)
            self._timing_versions["inbound_auth"] += 1

    def record_outbound_auth_time(self, seconds: float) -> None:
        with self._lock:
            self._outbound_auth_times.append(seconds)
            self._timing_versions["outbound_auth"] += 1

    def record_eventing_handler_time(self, seconds: float) -> None:
        with self._lock:
            self._eventing_handler_times.append(seconds)
            self._timing_versions["eventing"] += 1

    def get_output_latency_avg(self) -> float:
        """Return rolling average output response time in seconds (0 if none)."""
        with self._lock:
            if not self._output_resp_times:
                return 0.0
            return sum(self._output_resp_times) / len(self._output_resp_times)

    def record_batch_received(self, batch_size: int) -> None:
        with self._lock:
            if batch_size > self.largest_batch_received:
                self.largest_batch_received = batch_size
            if batch_size >= self.flood_threshold:
                self.flood_batches_total += 1
            self.changes_pending = (
                self.changes_received_total - self.changes_processed_total
            )

    def _get_cached_system_metrics(self) -> dict:
        """Cache psutil calls with 15s TTL to avoid syscalls on every scrape."""
        now = time.monotonic()
        if (
            self._system_metrics_cache is not None
            and now - self._system_metrics_cache_time < 15
        ):
            return self._system_metrics_cache

        cache = {}
        try:
            cache["gc_counts"] = gc.get_count()
            cache["gc_stats"] = gc.get_stats()
            cache["cpu_count"] = psutil.cpu_count(logical=True)
            cache["cpu_percent"] = psutil.cpu_percent(interval=0)
            cache["virtual_memory"] = psutil.virtual_memory()
            cache["swap_memory"] = psutil.swap_memory()
            try:
                cache["disk_usage"] = psutil.disk_usage("/")
            except OSError:
                cache["disk_usage"] = None
            cache["net_io_counters"] = psutil.net_io_counters()
        except Exception:
            pass  # system metrics are best-effort
        self._system_metrics_cache = cache
        self._system_metrics_cache_time = now
        return cache

    def _get_cached_process_metrics(self) -> dict:
        """Cache process-level psutil calls with 15s TTL."""
        now = time.monotonic()
        if (
            self._process_metrics_cache is not None
            and now - self._process_metrics_cache_time < 15
        ):
            return self._process_metrics_cache

        cache = {}
        try:
            proc = self._process
            cache["cpu_times"] = proc.cpu_times()
            cache["cpu_percent"] = proc.cpu_percent(interval=0)
            cache["memory_info"] = proc.memory_info()
            cache["memory_percent"] = proc.memory_percent()
            cache["num_threads"] = proc.num_threads()
            try:
                cache["num_fds"] = proc.num_fds()
            except AttributeError:
                pass  # num_fds() not available on Windows
        except Exception:
            pass  # process metrics are best-effort
        self._process_metrics_cache = cache
        self._process_metrics_cache_time = now
        return cache

    def _get_cached_dir_walk_sizes(self) -> dict:
        """Cache directory walk results with 60s TTL to avoid filesystem hits."""
        now = time.monotonic()
        if self._dir_walk_cache is not None and now - self._dir_walk_cache_time < 60:
            return self._dir_walk_cache

        cache = {"log_bytes": 0, "cbl_bytes": 0}
        try:
            log_dir = self._log_dir
            if log_dir and os.path.isdir(log_dir):
                total_log_bytes = 0
                for dirpath, _, filenames in os.walk(log_dir):
                    for fname in filenames:
                        try:
                            total_log_bytes += os.path.getsize(
                                os.path.join(dirpath, fname)
                            )
                        except OSError:
                            pass
                cache["log_bytes"] = total_log_bytes

            cbl_dir = self._cbl_db_dir
            if cbl_dir and os.path.exists(cbl_dir):
                total_cbl_bytes = 0
                if os.path.isdir(cbl_dir):
                    for dirpath, _, filenames in os.walk(cbl_dir):
                        for fname in filenames:
                            try:
                                total_cbl_bytes += os.path.getsize(
                                    os.path.join(dirpath, fname)
                                )
                            except OSError:
                                pass
                else:
                    try:
                        total_cbl_bytes = os.path.getsize(cbl_dir)
                    except OSError:
                        pass
                cache["cbl_bytes"] = total_cbl_bytes
        except Exception:
            pass  # directory walks are best-effort
        self._dir_walk_cache = cache
        self._dir_walk_cache_time = now
        return cache

    def _get_cached_timing_stats(self) -> dict[str, tuple[int, float, list[float]]]:
        """Return timing stats, recomputing only for deques that changed."""
        with self._lock:
            series = {
                "output": self._output_resp_times,
                "changes": self._changes_request_times,
                "batch": self._batch_processing_times,
                "fetch": self._doc_fetch_times,
                "health": self._health_probe_times,
                "inbound_auth": self._inbound_auth_times,
                "outbound_auth": self._outbound_auth_times,
                "eventing": self._eventing_handler_times,
            }
            versions = dict(self._timing_versions)

            cached_stats: dict[str, tuple[int, float, list[float]]] = {}
            pending: dict[str, tuple[int, list[float]]] = {}

            for key, data in series.items():
                version = versions[key]
                entry = self._timing_stats_cache.get(key)
                if entry is not None and entry[0] == version:
                    cached_stats[key] = (entry[1], entry[2], entry[3])
                else:
                    pending[key] = (version, list(data))

        computed_cache_entries: dict[str, tuple[int, int, float, list[float]]] = {}
        for key, (version, data) in pending.items():
            sorted_data = sorted(data) if data else []
            count = len(data)
            total = sum(data) if data else 0.0
            computed_cache_entries[key] = (version, count, total, sorted_data)
            cached_stats[key] = (count, total, sorted_data)

        if computed_cache_entries:
            with self._lock:
                for key, entry in computed_cache_entries.items():
                    # Only publish if no newer samples arrived while computing.
                    if self._timing_versions.get(key) == entry[0]:
                        self._timing_stats_cache[key] = entry

        return cached_stats

    def render(self) -> str:
        """Render all metrics in Prometheus text exposition format."""
        with self._lock:
            uptime = time.monotonic() - self._start_time
            labels = self._labels

        # Pre-compute sorted arrays/stats once per data change (not per scrape)
        timing_stats = self._get_cached_timing_stats()

        def _quantile(sorted_data: list[float], q: float) -> float:
            if not sorted_data:
                return 0.0
            idx = int(q * (len(sorted_data) - 1))
            return sorted_data[idx]

        ort_count, ort_sum, ort_sorted = timing_stats["output"]
        crt_count, crt_sum, crt_sorted = timing_stats["changes"]
        bpt_count, bpt_sum, bpt_sorted = timing_stats["batch"]
        dft_count, dft_sum, dft_sorted = timing_stats["fetch"]
        hpt_count, hpt_sum, hpt_sorted = timing_stats["health"]
        iat_count, iat_sum, iat_sorted = timing_stats["inbound_auth"]
        oat_count, oat_sum, oat_sorted = timing_stats["outbound_auth"]
        evt_count, evt_sum, evt_sorted = timing_stats["eventing"]

        lines: list[str] = []

        def _counter(name: str, help_text: str, value):
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name}{{{labels}}} {value}")

        def _gauge(name: str, help_text: str, value):
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name}{{{labels}}} {value}")

        def _summary(
            name: str,
            help_text: str,
            sorted_data: list[float],
            s_count: int,
            s_sum: float,
        ):
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} summary")
            for q in (0.5, 0.9, 0.99):
                lines.append(
                    f'{name}{{{labels},quantile="{q}"}} {_quantile(sorted_data, q):.6f}'
                )
            lines.append(f"{name}_sum{{{labels}}} {s_sum:.6f}")
            lines.append(f"{name}_count{{{labels}}} {s_count}")

        # -- Process info --
        _gauge(
            "changes_worker_uptime_seconds",
            "Time in seconds since the worker started.",
            f"{uptime:.3f}",
        )

        # -- Poll loop --
        _counter(
            "changes_worker_poll_cycles_total",
            "Total number of _changes poll cycles completed.",
            self.poll_cycles_total,
        )
        _counter(
            "changes_worker_poll_errors_total",
            "Total number of _changes poll errors.",
            self.poll_errors_total,
        )
        _gauge(
            "changes_worker_last_poll_timestamp_seconds",
            "Unix timestamp of the last successful _changes poll.",
            f"{self.last_poll_timestamp:.3f}",
        )
        _gauge(
            "changes_worker_last_batch_size",
            "Number of changes in the last batch received.",
            self.last_batch_size,
        )

        # -- Changes --
        _counter(
            "changes_worker_changes_received_total",
            "Total number of changes received from the _changes feed.",
            self.changes_received_total,
        )
        _counter(
            "changes_worker_changes_processed_total",
            "Total number of changes processed and forwarded.",
            self.changes_processed_total,
        )
        _counter(
            "changes_worker_changes_filtered_total",
            "Total number of changes filtered out (deletes + removes).",
            self.changes_filtered_total,
        )
        _counter(
            "changes_worker_changes_deleted_total",
            "Total number of deleted changes filtered out.",
            self.changes_deleted_total,
        )
        _counter(
            "changes_worker_changes_removed_total",
            "Total number of removed changes filtered out.",
            self.changes_removed_total,
        )

        # -- Feed content (always counted, regardless of filter settings) --
        _counter(
            "changes_worker_feed_deletes_seen_total",
            "Total changes with deleted=true seen in the feed.",
            self.feed_deletes_seen_total,
        )
        _counter(
            "changes_worker_feed_removes_seen_total",
            "Total changes with removed=true seen in the feed.",
            self.feed_removes_seen_total,
        )
        _counter(
            "changes_worker_deletes_forwarded_total",
            "Total tombstones (deleted=true) forwarded to the output (not filtered).",
            self.deletes_forwarded_total,
        )

        # -- Bytes --
        _counter(
            "changes_worker_bytes_received_total",
            "Total bytes received from _changes feed, bulk_get, and individual doc GETs.",
            self.bytes_received_total,
        )
        _counter(
            "changes_worker_bytes_output_total",
            "Total bytes sent to the output endpoint.",
            self.bytes_output_total,
        )

        # -- Doc fetching --
        _counter(
            "changes_worker_docs_fetched_total",
            "Total documents fetched via bulk_get or individual GET.",
            self.docs_fetched_total,
        )
        _counter(
            "changes_worker_doc_fetch_requests_total",
            "Total doc fetch requests (bulk_get or individual batch).",
            self.doc_fetch_requests_total,
        )
        _counter(
            "changes_worker_doc_fetch_errors_total",
            "Total doc fetch errors.",
            self.doc_fetch_errors_total,
        )
        _counter(
            "changes_worker_docs_fetch_skipped_total",
            "Docs skipped because they vanished between _changes and GET.",
            self.docs_fetch_skipped_total,
        )

        # -- Output --
        _counter(
            "changes_worker_output_requests_total",
            "Total output requests sent to the downstream endpoint.",
            self.output_requests_total,
        )
        _counter(
            "changes_worker_output_errors_total",
            "Total output request errors.",
            self.output_errors_total,
        )

        # Output by HTTP method
        lines.append(
            "# HELP changes_worker_output_requests_by_method_total Output requests broken down by HTTP method."
        )
        lines.append("# TYPE changes_worker_output_requests_by_method_total counter")
        lines.append(
            f'changes_worker_output_requests_by_method_total{{{labels},method="PUT"}} {self.output_put_total}'
        )
        lines.append(
            f'changes_worker_output_requests_by_method_total{{{labels},method="DELETE"}} {self.output_delete_total}'
        )

        lines.append(
            "# HELP changes_worker_output_errors_by_method_total Output errors broken down by HTTP method."
        )
        lines.append("# TYPE changes_worker_output_errors_by_method_total counter")
        lines.append(
            f'changes_worker_output_errors_by_method_total{{{labels},method="PUT"}} {self.output_put_errors_total}'
        )
        lines.append(
            f'changes_worker_output_errors_by_method_total{{{labels},method="DELETE"}} {self.output_delete_errors_total}'
        )

        _counter(
            "changes_worker_output_success_total",
            "Total output requests that succeeded.",
            self.output_success_total,
        )
        _counter(
            "changes_worker_output_skipped_total",
            "Total documents skipped at output (no mapper match or empty ops).",
            self.output_skipped_total,
        )
        _counter(
            "changes_worker_dead_letter_total",
            "Total documents written to the dead letter queue.",
            self.dead_letter_total,
        )
        _counter(
            "changes_worker_dlq_write_failures_total",
            "Total DLQ write failures (data potentially lost).",
            self.dlq_write_failures_total,
        )
        _gauge(
            "changes_worker_dlq_pending_count",
            "Current number of pending entries in the dead letter queue.",
            self.dlq_pending_count,
        )
        _gauge(
            "changes_worker_dlq_last_write_epoch",
            "Unix timestamp of the last DLQ write (0 = never).",
            self.dlq_last_write_epoch,
        )

        _gauge(
            "changes_worker_output_endpoint_up",
            "Whether the output endpoint is reachable (1=up, 0=down).",
            self.output_endpoint_up,
        )

        # Output response time summary
        _summary(
            "changes_worker_output_response_time_seconds",
            "Output HTTP response time in seconds.",
            ort_sorted,
            ort_count,
            ort_sum,
        )

        # -- Checkpoint --
        _counter(
            "changes_worker_checkpoint_saves_total",
            "Total checkpoint save operations.",
            self.checkpoint_saves_total,
        )
        _counter(
            "changes_worker_checkpoint_save_errors_total",
            "Total checkpoint save errors (fell back to local file).",
            self.checkpoint_save_errors_total,
        )
        _counter(
            "changes_worker_checkpoint_loads_total",
            "Total checkpoint load operations.",
            self.checkpoint_loads_total,
        )
        _counter(
            "changes_worker_checkpoint_load_errors_total",
            "Total checkpoint load errors.",
            self.checkpoint_load_errors_total,
        )
        lines.append(
            "# HELP changes_worker_checkpoint_seq Current checkpoint sequence value."
        )
        lines.append("# TYPE changes_worker_checkpoint_seq gauge")
        # Sequence can be a non-numeric string (e.g. "12:34"), expose as info label
        lines.append(
            f'changes_worker_checkpoint_seq{{{labels},seq="{self.checkpoint_seq}"}} 1'
        )

        # -- Retries --
        _counter(
            "changes_worker_retries_total",
            "Total HTTP retry attempts across all requests.",
            self.retries_total,
        )
        _counter(
            "changes_worker_retry_exhausted_total",
            "Total times all retries were exhausted.",
            self.retry_exhausted_total,
        )

        # -- Batches --
        _counter(
            "changes_worker_batches_total",
            "Total batches processed.",
            self.batches_total,
        )
        _counter(
            "changes_worker_batches_failed_total",
            "Total batches that failed (output down).",
            self.batches_failed_total,
        )

        # -- Mapper (DB mode) --
        _counter(
            "changes_worker_mapper_matched_total",
            "Total documents matched by a schema mapper.",
            self.mapper_matched_total,
        )
        _counter(
            "changes_worker_mapper_skipped_total",
            "Total documents skipped (no mapper match).",
            self.mapper_skipped_total,
        )
        _counter(
            "changes_worker_mapper_errors_total",
            "Total mapper errors.",
            self.mapper_errors_total,
        )
        _counter(
            "changes_worker_mapper_ops_total",
            "Total SQL operations generated by mappers.",
            self.mapper_ops_total,
        )

        # -- DB transaction resilience --
        _counter(
            "changes_worker_db_retries_total",
            "Total DB transaction retry attempts.",
            self.db_retries_total,
        )
        _counter(
            "changes_worker_db_retry_exhausted_total",
            "Total times all DB retries were exhausted.",
            self.db_retry_exhausted_total,
        )
        _counter(
            "changes_worker_db_transient_errors_total",
            "Total transient DB errors (connection, deadlock, serialization).",
            self.db_transient_errors_total,
        )
        _counter(
            "changes_worker_db_permanent_errors_total",
            "Total permanent DB errors (constraint, type mismatch).",
            self.db_permanent_errors_total,
        )
        _counter(
            "changes_worker_db_pool_reconnects_total",
            "Total DB connection pool reconnections.",
            self.db_pool_reconnects_total,
        )

        # -- Stream (continuous/websocket) --
        _counter(
            "changes_worker_stream_reconnects_total",
            "Total stream reconnections.",
            self.stream_reconnects_total,
        )
        _counter(
            "changes_worker_stream_messages_total",
            "Total stream messages received.",
            self.stream_messages_total,
        )
        _counter(
            "changes_worker_stream_parse_errors_total",
            "Total stream message parse errors.",
            self.stream_parse_errors_total,
        )

        # -- Health check probes --
        _counter(
            "changes_worker_health_probes_total",
            "Total health check probes sent.",
            self.health_probes_total,
        )
        _counter(
            "changes_worker_health_probe_failures_total",
            "Total health check probe failures.",
            self.health_probe_failures_total,
        )

        # -- Auth tracking (inbound – gateway) --
        _counter(
            "changes_worker_inbound_auth_total",
            "Total inbound (gateway) auth attempts.",
            self.inbound_auth_total,
        )
        _counter(
            "changes_worker_inbound_auth_success_total",
            "Total inbound (gateway) auth successes.",
            self.inbound_auth_success_total,
        )
        _counter(
            "changes_worker_inbound_auth_failure_total",
            "Total inbound (gateway) auth failures (401/403).",
            self.inbound_auth_failure_total,
        )
        _summary(
            "changes_worker_inbound_auth_time_seconds",
            "Inbound (gateway) auth request time in seconds.",
            iat_sorted,
            iat_count,
            iat_sum,
        )

        # -- Auth tracking (outbound – output endpoint) --
        _counter(
            "changes_worker_outbound_auth_total",
            "Total outbound (output endpoint) auth attempts.",
            self.outbound_auth_total,
        )
        _counter(
            "changes_worker_outbound_auth_success_total",
            "Total outbound (output endpoint) auth successes.",
            self.outbound_auth_success_total,
        )
        _counter(
            "changes_worker_outbound_auth_failure_total",
            "Total outbound (output endpoint) auth failures (401/403).",
            self.outbound_auth_failure_total,
        )
        _summary(
            "changes_worker_outbound_auth_time_seconds",
            "Outbound (output endpoint) auth request time in seconds.",
            oat_sorted,
            oat_count,
            oat_sum,
        )

        # -- Active tasks gauge --
        _gauge(
            "changes_worker_active_tasks",
            "Number of currently active document processing tasks.",
            self.active_tasks,
        )

        # -- Flood / backpressure --
        _gauge(
            "changes_worker_changes_pending",
            "Changes received but not yet processed (backpressure indicator).",
            self.changes_pending,
        )
        _gauge(
            "changes_worker_largest_batch_received",
            "Largest single batch of changes received since startup.",
            self.largest_batch_received,
        )
        _counter(
            "changes_worker_flood_batches_total",
            "Number of batches that exceeded the flood threshold.",
            self.flood_batches_total,
        )

        # -- Output backpressure --
        _counter(
            "changes_worker_backpressure_delays_total",
            "Number of times backpressure throttling was applied.",
            self.backpressure_delays_total,
        )
        _counter(
            "changes_worker_backpressure_delay_seconds_total",
            "Total seconds spent in backpressure delays.",
            self.backpressure_delay_seconds_total,
        )
        _gauge(
            "changes_worker_backpressure_active",
            "Whether backpressure throttling is currently active (1=yes, 0=no).",
            self.backpressure_active,
        )

        # -- Timing summaries --
        _summary(
            "changes_worker_changes_request_time_seconds",
            "Time to complete a _changes HTTP request in seconds.",
            crt_sorted,
            crt_count,
            crt_sum,
        )

        _summary(
            "changes_worker_batch_processing_time_seconds",
            "Time to process a batch of changes in seconds.",
            bpt_sorted,
            bpt_count,
            bpt_sum,
        )

        _summary(
            "changes_worker_doc_fetch_time_seconds",
            "Time to fetch documents (bulk_get or individual) in seconds.",
            dft_sorted,
            dft_count,
            dft_sum,
        )

        _summary(
            "changes_worker_health_probe_time_seconds",
            "Time for a health check probe in seconds.",
            hpt_sorted,
            hpt_count,
            hpt_sum,
        )

        # -- Attachments --
        _counter(
            "changes_worker_attachments_detected_total",
            "Documents with _attachments seen.",
            self.attachments_detected_total,
        )
        _counter(
            "changes_worker_attachments_downloaded_total",
            "Individual attachment downloads completed.",
            self.attachments_downloaded_total,
        )
        _counter(
            "changes_worker_attachments_download_errors_total",
            "Failed attachment downloads.",
            self.attachments_download_errors_total,
        )
        _counter(
            "changes_worker_attachments_uploaded_total",
            "Attachments uploaded to destination.",
            self.attachments_uploaded_total,
        )
        _counter(
            "changes_worker_attachments_upload_errors_total",
            "Failed attachment uploads.",
            self.attachments_upload_errors_total,
        )
        _counter(
            "changes_worker_attachments_bytes_downloaded_total",
            "Total bytes downloaded from source.",
            self.attachments_bytes_downloaded_total,
        )
        _counter(
            "changes_worker_attachments_bytes_uploaded_total",
            "Total bytes uploaded to destination.",
            self.attachments_bytes_uploaded_total,
        )
        _counter(
            "changes_worker_attachments_post_process_total",
            "Post-processing operations completed.",
            self.attachments_post_process_total,
        )
        _counter(
            "changes_worker_attachments_post_process_errors_total",
            "Failed post-processing operations.",
            self.attachments_post_process_errors_total,
        )
        _counter(
            "changes_worker_attachments_skipped_total",
            "Attachments skipped by filter.",
            self.attachments_skipped_total,
        )
        _counter(
            "changes_worker_attachments_missing_total",
            "Attachments listed in _attachments but returned 404 on fetch.",
            self.attachments_missing_total,
        )
        _counter(
            "changes_worker_attachments_digest_mismatch_total",
            "Downloads where digest didn't match (re-downloaded).",
            self.attachments_digest_mismatch_total,
        )
        _counter(
            "changes_worker_attachments_stale_total",
            "Attachments skipped because the parent doc revision was superseded.",
            self.attachments_stale_total,
        )
        _counter(
            "changes_worker_attachments_post_process_skipped_total",
            "Post-processing steps skipped (e.g. no matching rule).",
            self.attachments_post_process_skipped_total,
        )
        _counter(
            "changes_worker_attachments_conflict_retries_total",
            "Attachment conflict retries (revision conflict during post-process).",
            self.attachments_conflict_retries_total,
        )
        _counter(
            "changes_worker_attachments_orphaned_uploads_total",
            "Uploads that became orphaned (parent doc deleted or superseded).",
            self.attachments_orphaned_uploads_total,
        )
        _counter(
            "changes_worker_attachments_partial_success_total",
            "Documents where some but not all attachments succeeded.",
            self.attachments_partial_success_total,
        )
        _counter(
            "changes_worker_attachments_temp_files_cleaned_total",
            "Temporary attachment files cleaned up from disk.",
            self.attachments_temp_files_cleaned_total,
        )

        # -- Eventing (JS handlers) --
        _counter(
            "changes_worker_eventing_invocations_total",
            "Total eventing handler invocations (OnUpdate + OnDelete).",
            self.eventing_invocations_total,
        )
        _counter(
            "changes_worker_eventing_updates_total",
            "Total OnUpdate handler calls.",
            self.eventing_updates_total,
        )
        _counter(
            "changes_worker_eventing_deletes_total",
            "Total OnDelete handler calls.",
            self.eventing_deletes_total,
        )
        _counter(
            "changes_worker_eventing_passed_total",
            "Documents passed through by eventing handler.",
            self.eventing_passed_total,
        )
        _counter(
            "changes_worker_eventing_rejected_total",
            "Documents rejected by eventing handler.",
            self.eventing_rejected_total,
        )
        _counter(
            "changes_worker_eventing_errors_total",
            "JS handler exceptions (on_error policy applied).",
            self.eventing_errors_total,
        )
        _counter(
            "changes_worker_eventing_timeouts_total",
            "JS handler timeout_ms exceeded (on_timeout policy applied).",
            self.eventing_timeouts_total,
        )
        _counter(
            "changes_worker_eventing_halts_total",
            "Eventing halt events (on_error=halt or on_timeout=halt triggered).",
            self.eventing_halts_total,
        )
        _gauge(
            "changes_worker_eventing_v8_heap_used_bytes",
            "V8 isolate heap used bytes (latest reading).",
            self.eventing_v8_heap_used_bytes,
        )
        _gauge(
            "changes_worker_eventing_v8_heap_total_bytes",
            "V8 isolate heap total bytes (latest reading).",
            self.eventing_v8_heap_total_bytes,
        )
        _summary(
            "changes_worker_eventing_handler_duration_seconds",
            "Time spent in JS handler per invocation.",
            evt_sorted,
            evt_count,
            evt_sum,
        )

        # -- Recursion Guard --
        _counter(
            "changes_worker_recursion_guard_suppressed_total",
            "Changes suppressed by the recursion guard (write-back echo detected).",
            self.recursion_guard_suppressed_total,
        )

        # ── SYSTEM metrics (psutil / gc / threading) ────────────────────
        try:
            # Process-level metrics (cached with 15s TTL)
            proc_metrics = self._get_cached_process_metrics()
            cpu_times = proc_metrics.get("cpu_times")
            mem_info = proc_metrics.get("memory_info")

            _gauge(
                "changes_worker_process_cpu_percent",
                "Process CPU usage as a percentage of one core.",
                proc_metrics.get("cpu_percent", 0),
            )
            if cpu_times:
                _counter(
                    "changes_worker_process_cpu_user_seconds_total",
                    "User-space CPU seconds consumed by the worker process.",
                    f"{cpu_times.user:.3f}",
                )
                _counter(
                    "changes_worker_process_cpu_system_seconds_total",
                    "Kernel-space CPU seconds consumed by the worker process.",
                    f"{cpu_times.system:.3f}",
                )
            if mem_info:
                _gauge(
                    "changes_worker_process_memory_rss_bytes",
                    "Resident Set Size of the worker process in bytes.",
                    mem_info.rss,
                )
                _gauge(
                    "changes_worker_process_memory_vms_bytes",
                    "Virtual Memory Size of the worker process in bytes.",
                    mem_info.vms,
                )
            _gauge(
                "changes_worker_process_memory_percent",
                "Percentage of system RAM used by the worker process.",
                f"{proc_metrics.get('memory_percent', 0):.2f}",
            )
            _gauge(
                "changes_worker_process_threads",
                "Number of OS threads used by the worker process.",
                proc_metrics.get("num_threads", 0),
            )
            if "num_fds" in proc_metrics:
                _gauge(
                    "changes_worker_process_open_fds",
                    "Number of open file descriptors.",
                    proc_metrics["num_fds"],
                )

            _gauge(
                "changes_worker_python_threads_active",
                "Number of active Python threads.",
                threading.active_count(),
            )

            # GC stats per generation (cached with 15s TTL)
            sys_metrics = self._get_cached_system_metrics()
            gc_counts = sys_metrics.get("gc_counts", gc.get_count())
            gc_stats = sys_metrics.get("gc_stats", gc.get_stats())
            for gen in range(3):
                _gauge(
                    f"changes_worker_python_gc_gen{gen}_count",
                    f"Number of objects tracked by GC generation {gen}.",
                    gc_counts[gen],
                )
                _counter(
                    f"changes_worker_python_gc_gen{gen}_collections_total",
                    f"Total GC collection runs for generation {gen}.",
                    gc_stats[gen]["collections"],
                )

            # System-wide metrics (cached with 15s TTL to avoid syscalls)
            _gauge(
                "changes_worker_system_cpu_count",
                "Number of logical CPU cores on the host.",
                sys_metrics.get("cpu_count", 0),
            )
            _gauge(
                "changes_worker_system_cpu_percent",
                "Host-wide CPU usage percentage.",
                sys_metrics.get("cpu_percent", 0),
            )

            vmem = sys_metrics.get("virtual_memory")
            if vmem:
                _gauge(
                    "changes_worker_system_memory_total_bytes",
                    "Total physical memory on the host.",
                    vmem.total,
                )
                _gauge(
                    "changes_worker_system_memory_available_bytes",
                    "Available physical memory on the host.",
                    vmem.available,
                )
                _gauge(
                    "changes_worker_system_memory_used_bytes",
                    "Used physical memory on the host.",
                    vmem.used,
                )
                _gauge(
                    "changes_worker_system_memory_percent",
                    "Host memory usage percentage.",
                    vmem.percent,
                )

            swap = sys_metrics.get("swap_memory")
            if swap:
                _gauge(
                    "changes_worker_system_swap_total_bytes",
                    "Total swap space on the host.",
                    swap.total,
                )
                _gauge(
                    "changes_worker_system_swap_used_bytes",
                    "Used swap space on the host.",
                    swap.used,
                )

            disk = sys_metrics.get("disk_usage")
            if disk:
                _gauge(
                    "changes_worker_system_disk_total_bytes",
                    "Total disk space.",
                    disk.total,
                )
                _gauge(
                    "changes_worker_system_disk_used_bytes",
                    "Used disk space.",
                    disk.used,
                )
                _gauge(
                    "changes_worker_system_disk_free_bytes",
                    "Free disk space.",
                    disk.free,
                )
                _gauge(
                    "changes_worker_system_disk_percent",
                    "Disk usage percentage.",
                    disk.percent,
                )

            net = sys_metrics.get("net_io_counters")
            if net:
                _counter(
                    "changes_worker_system_network_bytes_sent_total",
                    "Total bytes sent over all network interfaces.",
                    net.bytes_sent,
                )
                _counter(
                    "changes_worker_system_network_bytes_recv_total",
                    "Total bytes received over all network interfaces.",
                    net.bytes_recv,
                )
                _counter(
                    "changes_worker_system_network_packets_sent_total",
                    "Total packets sent over all network interfaces.",
                    net.packets_sent,
                )
                _counter(
                    "changes_worker_system_network_packets_recv_total",
                    "Total packets received over all network interfaces.",
                    net.packets_recv,
                )
                _counter(
                    "changes_worker_system_network_errin_total",
                    "Total incoming network errors.",
                    net.errin,
                )
                _counter(
                    "changes_worker_system_network_errout_total",
                    "Total outgoing network errors.",
                    net.errout,
                )

            # Directory sizes (cached with 60s TTL to avoid filesystem hits)
            dir_sizes = self._get_cached_dir_walk_sizes()
            _gauge(
                "changes_worker_log_dir_size_bytes",
                "Total size of the log directory in bytes.",
                dir_sizes["log_bytes"],
            )
            _gauge(
                "changes_worker_cbl_db_size_bytes",
                "Total size of the Couchbase Lite database in bytes.",
                dir_sizes["cbl_bytes"],
            )
        except Exception:
            pass  # system metrics are best-effort

        # ── Per-engine / per-job DB metrics ────────────────────────────────
        try:
            from db.db_base import DbMetrics

            db_lines = DbMetrics.render_all()
            if db_lines:
                lines.append("")
                lines.append(db_lines)
        except Exception:
            pass  # db_base may not be loaded if no DB output is configured

        # ── Per-provider / per-job cloud metrics ──────────────────────────
        try:
            from cloud.cloud_base import CloudMetrics

            cloud_lines = CloudMetrics.render_all()
            if cloud_lines:
                lines.append("")
                lines.append(cloud_lines)
        except Exception:
            pass  # cloud_base may not be loaded if no cloud output is configured

        lines.append("")
        return "\n".join(lines)


async def _metrics_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    """aiohttp handler for GET /_metrics"""
    metrics: MetricsCollector = request.app["metrics"]
    body = metrics.render()
    return aiohttp.web.Response(
        text=body,
        content_type="text/plain",
        charset="utf-8",
        headers={"X-Content-Type-Options": "nosniff"},
    )


async def _restart_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    """POST /_restart — restart all jobs via PipelineManager."""
    manager: PipelineManager | None = request.app.get("pipeline_manager")
    if manager is None:
        return aiohttp.web.json_response({"error": "restart not supported"}, status=500)
    log_event(logger, "info", "CONTROL", "restart requested via /_restart endpoint")
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, manager.restart_all)
    states = await loop.run_in_executor(None, manager.list_job_states)
    return aiohttp.web.json_response(
        {"ok": True, "message": "restart signal sent", "jobs": states}
    )


async def _shutdown_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    """POST /_shutdown — graceful shutdown: stop feed, wait for in-flight
    processing and outputs to finish, then respond.

    CBL database close is handled by main()'s finally block after the
    event loop finishes, ensuring all async generators (which may write
    DLQ entries) complete before the database is closed.

    Behaviour:
      1. Set shutdown_event → _changes loops stop, RetryableHTTP aborts retries.
      2. Wait up to ``drain_timeout_seconds`` for active tasks to finish.
         - If tasks drain in time → checkpoint was NOT advanced past pending
           work, so nothing is lost.
         - If drain times out → remaining docs could not be delivered.
           * If ``dlq_inflight_on_shutdown`` is true the batch handler already
             wrote them to the dead-letter queue (CBL or .jsonl) so they can
             be reprocessed later even if a newer revision arrives on the feed.
           * If false the checkpoint was held back; the same docs will be
             re-fetched on the next startup.
      3. Return summary JSON.
    """
    shutdown_event: asyncio.Event | None = request.app.get("shutdown_event")
    if shutdown_event is None:
        return aiohttp.web.json_response(
            {"error": "shutdown not supported"}, status=500
        )

    log_event(
        logger, "info", "CONTROL", "graceful shutdown requested via /_shutdown endpoint"
    )

    # Read shutdown config from the app (set by start_metrics_server)
    shutdown_cfg: dict = request.app.get("shutdown_cfg", {})
    drain_timeout = shutdown_cfg.get("drain_timeout_seconds", 60)
    dlq_policy = shutdown_cfg.get("dlq_inflight_on_shutdown", False)

    # 1. Signal the _changes feed loops & retry loops to stop
    shutdown_event.set()

    # 2. Wait for all in-flight processing / output tasks to drain
    metrics: MetricsCollector | None = request.app.get("metrics")
    drained = True
    tasks_remaining = 0
    if metrics is not None:
        t0 = time.monotonic()
        while metrics.active_tasks > 0:
            elapsed = time.monotonic() - t0
            if elapsed > drain_timeout:
                tasks_remaining = metrics.active_tasks
                drained = False
                log_event(
                    logger,
                    "warn",
                    "CONTROL",
                    "shutdown drain timed out after %ds with %d tasks still active"
                    % (drain_timeout, tasks_remaining),
                )
                break
            log_event(
                logger,
                "debug",
                "CONTROL",
                "waiting for %d active tasks to finish" % metrics.active_tasks,
            )
            await asyncio.sleep(0.5)
        if drained:
            log_event(logger, "info", "CONTROL", "all active tasks drained")

    # 3. Build response summary
    summary: dict = {
        "ok": True,
        "drained": drained,
        "drain_timeout_seconds": drain_timeout,
        "dlq_inflight_on_shutdown": dlq_policy,
    }
    if not drained:
        summary["tasks_remaining"] = tasks_remaining
        if dlq_policy:
            summary["message"] = (
                "shutdown complete – drain timed out, %d in-flight docs written to dead-letter queue, "
                "checkpoint was NOT advanced past them" % tasks_remaining
            )
        else:
            summary["message"] = (
                "shutdown complete – drain timed out, %d in-flight docs NOT delivered, "
                "checkpoint was NOT advanced – they will be re-fetched on next startup"
                % tasks_remaining
            )
    else:
        summary["message"] = (
            "shutdown complete – feeds stopped, outputs drained, database closed"
        )

    log_event(
        logger, "info", "CONTROL", "graceful shutdown complete: %s" % summary["message"]
    )
    return aiohttp.web.json_response(summary)


async def _offline_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    """POST /_offline — pause all jobs. Worker stays alive."""
    manager: PipelineManager | None = request.app.get("pipeline_manager")
    if manager is None:
        return aiohttp.web.json_response({"error": "offline not supported"}, status=500)
    if manager.is_offline():
        return aiohttp.web.json_response({"ok": True, "message": "already offline"})
    log_event(logger, "info", "CONTROL", "offline requested via /_offline endpoint")
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, manager.go_offline)
    return aiohttp.web.json_response({"ok": True, "message": "going offline"})


async def _online_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    """POST /_online — resume all enabled jobs."""
    manager: PipelineManager | None = request.app.get("pipeline_manager")
    if manager is None:
        return aiohttp.web.json_response({"error": "online not supported"}, status=500)
    if not manager.is_offline():
        return aiohttp.web.json_response({"ok": True, "message": "already online"})
    log_event(logger, "info", "CONTROL", "online requested via /_online endpoint")
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, manager.go_online)
    states = await loop.run_in_executor(None, manager.list_job_states)
    return aiohttp.web.json_response(
        {"ok": True, "message": "going online", "jobs": states}
    )


async def _status_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    """GET /_status — return worker online/offline state."""
    manager: PipelineManager | None = request.app.get("pipeline_manager")
    is_offline = manager.is_offline() if manager is not None else False
    return aiohttp.web.json_response({"online": not is_offline})


async def _collect_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
    """POST /_collect — generate diagnostic zip and stream it back."""
    from rest.log_collect import DiagnosticsCollector
    from pipeline.pipeline_logging import get_redactor

    cfg = request.app.get("config", {})
    metrics = request.app.get("metrics")
    redactor = get_redactor()

    include_profiling = request.query.get("include_profiling", "true").lower() == "true"

    zip_path = None
    try:
        collector = DiagnosticsCollector(cfg, metrics, redactor)
        zip_path = await collector.collect(include_profiling=include_profiling)

        log_event(
            logger,
            "info",
            "CONTROL",
            "diagnostics collection complete: %s" % os.path.basename(zip_path),
        )

        # Stream the zip and clean up after sending
        resp = aiohttp.web.StreamResponse(
            headers={
                "Content-Type": "application/zip",
                "Content-Disposition": f'attachment; filename="{os.path.basename(zip_path)}"',
            },
        )
        await resp.prepare(request)
        with open(zip_path, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                await resp.write(chunk)
        await resp.write_eof()
        return resp
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error generating diagnostics",
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return aiohttp.web.json_response(
            {"error": f"Failed to collect diagnostics: {e}"}, status=500
        )
    finally:
        if zip_path:
            try:
                os.remove(zip_path)
            except FileNotFoundError:
                pass


async def start_metrics_server(
    metrics: MetricsCollector,
    host: str,
    port: int,
    restart_event: asyncio.Event | None = None,
    shutdown_event: asyncio.Event | None = None,
    offline_event: asyncio.Event | None = None,
    cbl_scheduler: CBLMaintenanceScheduler | None = None,
    shutdown_cfg: dict | None = None,
    extra_routes_cb=None,
    cfg: dict | None = None,
) -> aiohttp.web.AppRunner:
    """Start a lightweight HTTP server that serves /_metrics in Prometheus format."""
    from aiohttp import web

    app = web.Application()
    app["metrics"] = metrics
    app["config"] = cfg or {}
    app["shutdown_cfg"] = shutdown_cfg or {}
    if restart_event is not None:
        app["restart_event"] = restart_event
    if shutdown_event is not None:
        app["shutdown_event"] = shutdown_event
    if offline_event is not None:
        app["offline_event"] = offline_event
    if cbl_scheduler is not None:
        app["cbl_scheduler"] = cbl_scheduler
    app.router.add_get("/_metrics", _metrics_handler)
    app.router.add_get("/metrics", _metrics_handler)
    app.router.add_post("/_collect", _collect_handler)
    app.router.add_post("/_restart", _restart_handler)
    app.router.add_post("/_shutdown", _shutdown_handler)
    app.router.add_post("/_offline", _offline_handler)
    app.router.add_post("/_online", _online_handler)
    app.router.add_get("/_status", _status_handler)

    # API v2.0 routes (inputs, outputs, jobs, sessions)
    app.router.add_get("/api/inputs_changes", api_get_inputs_changes)
    app.router.add_post("/api/inputs_changes", api_post_inputs_changes)
    app.router.add_put("/api/inputs_changes/{id}", api_put_inputs_changes_entry)
    app.router.add_delete("/api/inputs_changes/{id}", api_delete_inputs_changes_entry)

    app.router.add_get(r"/api/outputs_{type:rdbms|http|cloud}", api_get_outputs)
    app.router.add_post(r"/api/outputs_{type:rdbms|http|cloud}", api_post_outputs)
    app.router.add_put(
        r"/api/outputs_{type:rdbms|http|cloud}/{id}", api_put_outputs_entry
    )
    app.router.add_delete(
        r"/api/outputs_{type:rdbms|http|cloud}/{id}", api_delete_outputs_entry
    )

    # Register job control endpoints BEFORE generic /api/jobs/{id} routes
    # so more specific routes take precedence
    if extra_routes_cb is not None:
        extra_routes_cb(app)

    app.router.add_get("/api/jobs", api_get_jobs)
    app.router.add_get("/api/jobs/{id}", api_get_job)
    app.router.add_post("/api/jobs", api_post_jobs)
    app.router.add_put("/api/jobs/{id}", api_put_job)
    app.router.add_delete("/api/jobs/{id}", api_delete_job)
    app.router.add_post("/api/jobs/{id}/refresh-input", api_refresh_job_input)
    app.router.add_post("/api/jobs/{id}/refresh-output", api_refresh_job_output)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    log_event(
        logger, "info", "METRICS", "metrics server listening", host=host, port=port
    )
    return runner


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def load_config(path: str | None = None) -> dict:
    """Load config from CBL (source of truth) or fall back to config.json.

    When CBL is available, config.json is only used as a seed on the very
    first startup.  After that, all config changes go through CBL via the
    Admin UI.  To re-seed from config.json, delete the CBL volume.
    """
    if USE_CBL:
        store = CBLStore()
        cfg = store.load_config()
        if cfg:
            log_event(
                logger,
                "info",
                "CONTROL",
                "config loaded from CBL (config.json is ignored)",
            )
            ic(cfg)
            return cfg
        # First run: seed from file → CBL
        if path:
            with open(path) as f:
                cfg = json.load(f)
            store.save_config(cfg)
            log_event(
                logger,
                "info",
                "CONTROL",
                "first start — seeded config from %s into CBL" % path,
            )
            ic(cfg)
            return cfg
    # Fallback: no CBL — read from file directly
    with open(path or "config.json") as f:
        cfg = json.load(f)
    ic(cfg)
    return cfg


def _ensure_full_logging_config(cfg: dict) -> None:
    """Upgrade legacy ``{"level": "DEBUG"}`` logging config to full SG-style format."""
    logging_cfg = cfg.get("logging", {})
    if "console" in logging_cfg or "file" in logging_cfg:
        return  # already in full format

    old_level = logging_cfg.get("level", "info").lower()

    cfg["logging"] = {
        "redaction_level": "partial",
        "console": {
            "enabled": True,
            "log_level": old_level,
            "log_keys": ["*"],
            "key_levels": {},
            "color_enabled": False,
        },
        "file": {
            "enabled": True,
            "path": "logs/changes_worker.log",
            "log_level": old_level,
            "log_keys": ["*"],
            "key_levels": {},
            "rotation": {
                "max_size": 100,
                "max_age": 7,
                "rotated_logs_size_limit": 1024,
            },
        },
    }

    if USE_CBL:
        CBLStore().save_config(cfg)


VALID_SOURCES = ("sync_gateway", "app_services", "edge_server", "couchdb")


def validate_config(cfg: dict) -> tuple[str, list[str], list[str]]:
    """
    Validate the entire config against the selected gateway.src.

    Returns (src, warnings, errors).
    Errors are fatal – the process should not start.
    Warnings are logged but execution continues.
    """
    warnings: list[str] = []
    errors: list[str] = []

    # v2.0 schema stores gateway/auth/changes_feed inside job docs, not in the
    # top-level config.  Skip connection-level validation when those keys are
    # absent (i.e. after migration).
    is_v2 = cfg.get("schema_version") == "2.0"

    gw = cfg.get("gateway", {})
    auth_cfg = cfg.get("auth", {})
    feed_cfg = cfg.get("changes_feed", {})

    # -- gateway.src -----------------------------------------------------------
    src = gw.get("src", "sync_gateway")
    if src not in VALID_SOURCES:
        if is_v2 and not gw:
            # v2.0 infra-only config has no gateway section – that's fine
            return src, warnings, errors
        errors.append(f"gateway.src must be one of {VALID_SOURCES}, got '{src}'")
        return src, warnings, errors  # can't validate further

    # -- gateway basics --------------------------------------------------------
    if not is_v2:
        if not gw.get("url"):
            errors.append("gateway.url is required")
        if not gw.get("database"):
            errors.append("gateway.database is required")

    # App Services is always HTTPS
    url = gw.get("url", "")
    if src == "app_services" and url.startswith("http://"):
        warnings.append(
            "App Services endpoints are typically HTTPS – "
            "gateway.url starts with http://, verify this is correct"
        )

    # CouchDB does not have scopes/collections
    if src == "couchdb" and (gw.get("scope") or gw.get("collection")):
        warnings.append(
            "CouchDB does not support scopes or collections – "
            "gateway.scope and gateway.collection will be ignored"
        )

    # -- auth ------------------------------------------------------------------
    if not is_v2:
        auth_method = auth_cfg.get("method", "basic")

        if auth_method == "bearer" and src == "edge_server":
            errors.append(
                "auth.method=bearer is not supported by Edge Server – "
                "use 'basic' or 'session' instead"
            )

        if auth_method == "session" and src == "couchdb":
            errors.append(
                "auth.method=session is not supported by CouchDB – "
                "use 'basic' or 'bearer' instead"
            )

        if auth_method == "basic":
            if not auth_cfg.get("username"):
                errors.append("auth.username is required when auth.method=basic")
            if not auth_cfg.get("password"):
                errors.append("auth.password is required when auth.method=basic")
        elif auth_method == "session":
            if not auth_cfg.get("session_cookie"):
                errors.append(
                    "auth.session_cookie is required when auth.method=session"
                )
        elif auth_method == "bearer":
            if not auth_cfg.get("bearer_token"):
                errors.append("auth.bearer_token is required when auth.method=bearer")
        elif auth_method != "none":
            errors.append(
                f"auth.method must be 'basic', 'session', 'bearer', or 'none' – got '{auth_method}'"
            )

    # -- changes_feed ----------------------------------------------------------
    if is_v2:
        return src, warnings, errors

    feed_type = feed_cfg.get("feed_type", "longpoll")

    if feed_type == "websocket" and src == "couchdb":
        errors.append(
            "changes_feed.feed_type=websocket is not supported by CouchDB – "
            "use 'longpoll', 'continuous', or 'eventsource'"
        )

    if feed_type == "websocket" and src == "edge_server":
        errors.append(
            "changes_feed.feed_type=websocket is not supported by Edge Server – "
            "use 'longpoll', 'continuous', or 'sse'"
        )

    if feed_type == "sse" and src != "edge_server":
        errors.append(
            f"changes_feed.feed_type=sse is only supported by Edge Server – "
            f"not available on {src.replace('_', ' ').title()}"
        )

    valid_feeds_by_src = {
        "sync_gateway": ("longpoll", "continuous", "websocket", "normal"),
        "app_services": ("longpoll", "continuous", "websocket", "normal"),
        "edge_server": ("longpoll", "continuous", "sse", "normal"),
        "couchdb": ("longpoll", "continuous", "eventsource", "normal"),
    }
    if feed_type not in valid_feeds_by_src.get(src, ()):
        errors.append(
            f"changes_feed.feed_type='{feed_type}' is not valid for {src} – "
            f"allowed: {valid_feeds_by_src[src]}"
        )

    version_type = feed_cfg.get("version_type", "rev")
    if version_type != "rev" and src == "edge_server":
        errors.append(
            f"changes_feed.version_type='{version_type}' is not supported by Edge Server – "
            "Edge Server does not support the version_type parameter"
        )
    if version_type != "rev" and src == "couchdb":
        errors.append(
            f"changes_feed.version_type='{version_type}' is not supported by CouchDB – "
            "CouchDB does not support the version_type parameter"
        )
    if version_type not in ("rev", "cv"):
        errors.append(
            f"changes_feed.version_type must be 'rev' or 'cv', got '{version_type}'"
        )

    timeout_ms = feed_cfg.get("timeout_ms", 60000)
    if src == "edge_server" and timeout_ms > 900000:
        warnings.append(
            f"changes_feed.timeout_ms={timeout_ms} exceeds Edge Server's "
            f"max of 900000ms (15 min) – it will be clamped"
        )

    include_docs = feed_cfg.get("include_docs", True)
    if not include_docs and src == "edge_server":
        warnings.append(
            "changes_feed.include_docs=false with Edge Server – "
            "Edge Server has no _bulk_get endpoint, docs will be fetched "
            "individually via GET /{keyspace}/{docid} (slower for large batches)"
        )
    if not include_docs and src == "couchdb":
        warnings.append(
            "changes_feed.include_docs=false with CouchDB – "
            "docs will be fetched via POST /{db}/_bulk_get"
        )

    heartbeat_ms = feed_cfg.get("heartbeat_ms", 0)
    if src == "edge_server" and heartbeat_ms > 0 and heartbeat_ms < 25000:
        warnings.append(
            f"changes_feed.heartbeat_ms={heartbeat_ms} is below Edge Server's "
            f"minimum of 25000ms – server may reject it"
        )

    poll_interval = feed_cfg.get("poll_interval_seconds", 10)
    if poll_interval < 1:
        warnings.append(
            f"changes_feed.poll_interval_seconds={poll_interval} is very aggressive – "
            "consider at least 1 second to avoid hammering the server"
        )

    http_timeout = feed_cfg.get("http_timeout_seconds", 300)
    if http_timeout < 10:
        warnings.append(
            f"changes_feed.http_timeout_seconds={http_timeout} is very low – "
            "large feeds (since=0) may time out before completing"
        )

    # -- output ----------------------------------------------------------------
    out_cfg = cfg.get("output", {})
    out_mode = out_cfg.get("mode")
    _DB_ENGINE_ALIASES = {"postgres", "mysql", "mssql", "oracle"}
    if out_mode is None:
        errors.append("output.mode is required (http, db, s3, or a db engine name)")
    elif (
        out_mode not in ("http", "db", "s3", "stdout")
        and out_mode not in _DB_ENGINE_ALIASES
    ):
        errors.append(
            f"output.mode must be 'http', 'db', 's3', or a db engine name "
            f"(postgres/mysql/mssql/oracle), got '{out_mode}'"
        )
    if out_mode == "http" and not out_cfg.get("target_url"):
        errors.append("output.target_url is required when output.mode=http")

    # -- output_format ---------------------------------------------------------
    out_fmt = out_cfg.get("output_format", "json")
    if out_fmt not in VALID_OUTPUT_FORMATS:
        errors.append(
            f"output.output_format must be one of {VALID_OUTPUT_FORMATS}, got '{out_fmt}'"
        )
    # Check if the required library is installed for binary/yaml formats
    missing = check_serialization_library(out_fmt)
    if missing:
        fmt_name, pip_name = missing
        errors.append(
            f"output.output_format='{fmt_name}' requires the '{pip_name}' library – "
            f"pip install {pip_name}"
        )

    if out_mode == "http":
        valid_methods = ("PUT", "POST", "PATCH", "DELETE")
        write_method = out_cfg.get("write_method", "PUT").upper()
        delete_method = out_cfg.get("delete_method", "DELETE").upper()
        if write_method not in valid_methods:
            errors.append(
                f"output.write_method must be one of {valid_methods}, got '{write_method}'"
            )
        if delete_method not in valid_methods:
            errors.append(
                f"output.delete_method must be one of {valid_methods}, got '{delete_method}'"
            )

        req_timeout = out_cfg.get("request_timeout_seconds", 30)
        if req_timeout <= 0:
            errors.append(
                f"output.request_timeout_seconds must be > 0, got {req_timeout}"
            )

        out_auth_method = out_cfg.get("target_auth", {}).get("method", "none")
        if out_auth_method == "basic":
            if not out_cfg.get("target_auth", {}).get("username"):
                errors.append(
                    "output.target_auth.username is required when target_auth.method=basic"
                )
            if not out_cfg.get("target_auth", {}).get("password"):
                errors.append(
                    "output.target_auth.password is required when target_auth.method=basic"
                )
        elif out_auth_method == "session":
            if not out_cfg.get("target_auth", {}).get("session_cookie"):
                errors.append(
                    "output.target_auth.session_cookie is required when target_auth.method=session"
                )
        elif out_auth_method == "bearer":
            if not out_cfg.get("target_auth", {}).get("bearer_token"):
                errors.append(
                    "output.target_auth.bearer_token is required when target_auth.method=bearer"
                )

        out_retry = out_cfg.get("retry", {})
        out_max_retries = out_retry.get("max_retries", 3)
        if out_max_retries < 0:
            errors.append(
                f"output.retry.max_retries must be >= 0, got {out_max_retries}"
            )

        if not out_cfg.get("halt_on_failure", True):
            warnings.append(
                "output.halt_on_failure=false – if the output endpoint fails, "
                "docs will be skipped and the checkpoint will still advance"
            )

        data_error_action = out_cfg.get("data_error_action", "dlq")
        if data_error_action not in ("dlq", "skip"):
            errors.append(
                f"output.data_error_action must be 'dlq' or 'skip', got '{data_error_action}'"
            )

    # -- non-sequential + no DLQ -----------------------------------------------
    proc_cfg = cfg.get("processing", cfg.get("gateway", {}).get("processing", {}))
    is_sequential = proc_cfg.get("sequential", False)
    dlq_path = cfg.get("output", {}).get("dead_letter_path", "")
    has_dlq = bool(dlq_path)
    # CBL is always available as DLQ backend, so only warn when no CBL either
    try:
        from storage.cbl_store import USE_CBL as _use_cbl_check
    except ImportError:
        _use_cbl_check = False
    if not is_sequential and not has_dlq and not _use_cbl_check:
        warnings.append(
            "RISK: non-sequential (parallel) mode is enabled WITHOUT a Dead Letter Queue. "
            "If the output goes down or the worker shuts down mid-batch, in-flight documents "
            "will be lost — there is no DLQ to catch them and no way to replay them. "
            "Either enable the DLQ (set output.dead_letter_path) or switch to sequential mode "
            "(set processing.sequential=true)."
        )

    # -- retry -----------------------------------------------------------------
    retry_cfg = cfg.get("retry", {})
    max_retries = retry_cfg.get("max_retries", 5)
    if max_retries < 0:
        errors.append(f"retry.max_retries must be >= 0, got {max_retries}")

    # -- metrics ---------------------------------------------------------------
    metrics_cfg = cfg.get("metrics", {})
    if metrics_cfg.get("enabled", False):
        metrics_port = metrics_cfg.get("port", 9090)
        if (
            not isinstance(metrics_port, int)
            or metrics_port < 1
            or metrics_port > 65535
        ):
            errors.append(
                f"metrics.port must be an integer between 1 and 65535, got {metrics_port}"
            )

    # -- attachments -----------------------------------------------------------
    att_cfg = cfg.get("attachments", {})
    if att_cfg.get("enabled", False):
        att_mode = att_cfg.get("mode", "individual")
        if att_mode not in ("individual", "bulk", "multipart"):
            errors.append(
                f"attachments.mode must be 'individual', 'bulk', or 'multipart', got '{att_mode}'"
            )
        if src == "edge_server" and not att_cfg.get("skip_on_edge_server", True):
            warnings.append(
                "attachments enabled with edge_server source and skip_on_edge_server=false – "
                "Edge Server has no attachment API, downloads will fail"
            )
        att_on_missing = att_cfg.get("on_missing_attachment", "skip")
        if att_on_missing not in ("skip", "fail", "retry"):
            errors.append(
                f"attachments.on_missing_attachment must be 'skip', 'fail', or 'retry', "
                f"got '{att_on_missing}'"
            )
        att_partial = att_cfg.get("partial_success", "continue")
        if att_partial not in ("continue", "fail_doc", "require_all"):
            errors.append(
                f"attachments.partial_success must be 'continue', 'fail_doc', or 'require_all', "
                f"got '{att_partial}'"
            )

    return src, warnings, errors


def build_base_url(gw: dict) -> str:
    """Build the keyspace URL: {url}/{db}.{scope}.{collection}"""
    url = gw.get("url") or gw.get("host")
    if not url:
        raise KeyError("'url'")
    if "database" not in gw:
        raise KeyError("'database'")
    base = url.rstrip("/")
    db = gw["database"]
    src = gw.get("src", gw.get("source_type", "sync_gateway"))
    # CouchDB has no scopes/collections concept
    if src == "couchdb":
        return f"{base}/{db}"
    scope = gw.get("scope", "")
    collection = gw.get("collection", "")
    if scope and collection:
        keyspace = f"{db}.{scope}.{collection}"
    else:
        keyspace = db
    return f"{base}/{keyspace}"


def build_ssl_context(gw: dict) -> ssl.SSLContext | None:
    url = gw.get("url") or gw.get("host", "")
    if not url.startswith("https"):
        return None
    ctx = ssl.create_default_context()
    if gw.get("accept_self_signed_certs"):
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def build_auth_headers(
    auth_cfg: dict, src: str = "sync_gateway", compress: bool = False
) -> dict:
    method = auth_cfg.get("method", "basic")
    headers: dict[str, str] = {}
    if compress:
        headers["Accept-Encoding"] = "gzip"
    if method == "bearer":
        if src == "edge_server":
            log_event(
                logger,
                "warn",
                "HTTP",
                "bearer token auth is not supported by Edge Server – falling back to basic",
            )
        else:
            headers["Authorization"] = f"Bearer {auth_cfg['bearer_token']}"
    elif method == "session":
        if src == "couchdb":
            log_event(
                logger,
                "warn",
                "HTTP",
                "session cookie auth is not supported by CouchDB – falling back to basic",
            )
        else:
            headers["Cookie"] = f"SyncGatewaySession={auth_cfg['session_cookie']}"
    return headers


def build_basic_auth(auth_cfg: dict) -> aiohttp.BasicAuth | None:
    if auth_cfg.get("method", "basic") == "basic" and auth_cfg.get("username"):
        return aiohttp.BasicAuth(auth_cfg["username"], auth_cfg.get("password", ""))
    return None


# ---------------------------------------------------------------------------
# Phase 6: Job-Based Startup
# ---------------------------------------------------------------------------


def load_enabled_jobs(db: CBLStore | None) -> list[dict]:
    """
    Load all enabled job documents from CBL.

    Returns list of job documents with fields:
        {
            "_id": "job_uuid",
            "type": "job",
            "name": "...",
            "enabled": true,
            "inputs": [input_doc],
            "outputs": [output_doc],
            "output_type": "...",
            "mapping": {...},
            "system": {...}
        }
    """
    if not db:
        log_event(logger, "warn", "CONTROL", "CBL not available – no jobs")
        return []

    try:
        jobs = db.list_jobs()  # Returns all jobs from CBL
        return [j for j in jobs if j.get("enabled", True)]
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "failed to load jobs",
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return []


def build_pipeline_config_from_job(job_doc: dict) -> dict:
    """
    Convert a job document to pipeline config format.

    Returns config with keys:
        {
            "job_id": "...",
            "job_name": "...",
            "gateway": input_entry,
            "auth": {...},
            "changes_feed": {...},
            "processing": {...},
            "output": output_entry,
            "checkpoint": {...with job_id suffix},
            "mapping": {...},
            "system": {...}
        }
    """
    job_id = job_doc.get("_id") or job_doc.get("id")
    job_name = job_doc.get("name", "Unnamed Job")

    # Extract input/output entries
    inputs = job_doc.get("inputs", [])
    outputs = job_doc.get("outputs", [])

    if not inputs or not outputs:
        raise ValueError(f"Job {job_id} missing inputs or outputs")

    input_entry = inputs[0]
    output_entry = outputs[0]

    # Build pipeline config by taking the input/output entries
    # and merging with defaults
    return {
        "job_id": job_id,
        "job_name": job_name,
        "gateway": input_entry,  # {url, database, src, scope, collection, auth}
        "auth": input_entry.get("auth", {}),
        "changes_feed": input_entry.get("changes_feed", {}),
        "processing": input_entry.get("processing", {}),
        "output": output_entry,  # {mode, target_url, ...}
        "output_type": job_doc.get("output_type", "http"),
        "checkpoint": {
            "enabled": True,
            "file": f"checkpoint_{job_id}.json",
        },
        "mapping": job_doc.get("mapping"),
        "system": job_doc.get("system", {}),
        "retry": job_doc.get("retry", {}),
        "metrics": job_doc.get("metrics", {}),
        "logging": job_doc.get("logging", {}),
    }


def migrate_legacy_config_to_job(db: CBLStore, cfg: dict) -> dict | None:
    """
    Auto-migrate v1.x config.json to a job document.

    Returns the migrated job document, or None if migration failed.
    """
    try:
        gw = cfg.get("gateway", {})
        out = cfg.get("output", {})

        if not gw or not out:
            log_event(
                logger,
                "warn",
                "CONTROL",
                "legacy config missing gateway or output – cannot auto-migrate",
            )
            return None

        job_id = "legacy_auto_migrated_" + str(int(time.time()))
        job_name = "Auto-migrated v1.x config"

        job_data = {
            "name": job_name,
            "enabled": True,
            "inputs": [gw],
            "outputs": [out],
            "output_type": out.get("mode", "http"),
            "mapping": None,
            "system": cfg.get("system", {}),
            "retry": cfg.get("retry", {}),
        }

        # Save to CBL (save_job expects job_id and job_data separately)
        db.save_job(job_id, job_data)
        log_event(
            logger,
            "info",
            "CONTROL",
            "auto-migrated legacy config.json to job %s" % job_id,
            job_id=job_id,
        )

        # Return the full document as it would be retrieved
        job_doc = {"_id": job_id, "id": job_id, **job_data}
        return job_doc
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "failed to auto-migrate legacy config",
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return None


# ---------------------------------------------------------------------------
# Checkpoint persistence
# ---------------------------------------------------------------------------


class Checkpoint:
    """
    CBL-style checkpoint stored on Sync Gateway as a _local document.

    Key derivation (mirrors CBL):
        UUID = SHA1(local_client_id + SG_URL + channels)
        doc id = _sync:local:{UUID}
        SG REST path = {keyspace}/_local/checkpoint-{UUID}

    The checkpoint document contains (CBL-compatible):
        {
            "client_id": "<local_client_id>",
            "time": <epoch timestamp>,
            "remote": "<last_seq>"
        }
    """

    def __init__(
        self, cfg: dict, gw_cfg: dict, channels: list[str], job_id: str | None = None
    ):
        self._enabled = cfg.get("enabled", True)
        self._job_id = job_id  # Phase 6: per-job checkpoint isolation
        self._lock = asyncio.Lock()
        self._seq: str = "0"
        self._rev: str | None = None  # SG doc _rev for updates
        self._initial_sync_done: bool = False

        # Build the deterministic UUID the same way CBL does:
        #   HASH(local_client_id + SG URL + channel_names + job_id)
        client_id = cfg.get("client_id", "changes_worker")
        sg_url = build_base_url(gw_cfg)
        channel_str = ",".join(sorted(channels)) if channels else ""
        job_str = job_id or ""  # Phase 6: include job_id in UUID for isolation
        raw = f"{client_id}{sg_url}{channel_str}{job_str}"
        self._uuid = hashlib.sha1(raw.encode()).hexdigest()
        self._client_id = client_id
        self._local_doc_id = f"checkpoint-{self._uuid}"

        # Fallback to local file when SG is unreachable for checkpoint ops
        # Phase 6: use job_id in fallback filename for isolation
        fallback_file = cfg.get("file", "checkpoint.json")
        if job_id:
            # Transform "checkpoint.json" -> "checkpoint_<job_id>.json"
            path = Path(fallback_file)
            fallback_file = str(path.parent / f"{path.stem}_{job_id}{path.suffix}")
        self._fallback_path = Path(fallback_file)
        self._fallback_store: CBLStore | None = None

        ic(self._uuid, self._local_doc_id, raw)

        self._metrics = None

    def set_metrics(self, metrics: "MetricsCollector | None") -> None:
        self._metrics = metrics

    @property
    def local_doc_path(self) -> str:
        """Returns the REST path segment: _local/checkpoint-{uuid}"""
        return f"_local/{self._local_doc_id}"

    @property
    def seq(self) -> str:
        return self._seq

    @property
    def initial_sync_done(self) -> bool:
        return self._initial_sync_done

    # -- SG-backed load/save ---------------------------------------------------

    async def load(
        self,
        http: "RetryableHTTP",
        base_url: str,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
    ) -> str:
        """GET {keyspace}/_local/checkpoint-{uuid} from Sync Gateway."""
        if not self._enabled:
            return self._seq

        url = f"{base_url}/{self.local_doc_path}"
        ic("checkpoint load", url)
        try:
            resp = await http.request("GET", url, auth=auth, headers=headers)
            data = await resp.json()
            resp.release()
            self._seq = str(data.get("remote", data.get("SGs_Seq", "0")))
            self._rev = data.get("_rev")
            raw_isd = data.get("initial_sync_done", None)
            if raw_isd is None:
                self._initial_sync_done = self._seq != "0"
            else:
                self._initial_sync_done = bool(raw_isd)
            log_event(
                logger,
                "info",
                "CHECKPOINT",
                "checkpoint loaded",
                operation="SELECT",
                storage="sg",
            )
            log_event(
                logger,
                "debug",
                "CHECKPOINT",
                "checkpoint detail",
                operation="SELECT",
                seq=self._seq,
                doc_id=self._local_doc_id,
                storage="sg",
            )
            if self._metrics:
                self._metrics.inc("checkpoint_loads_total")
        except ClientHTTPError as exc:
            if exc.status == 404:
                log_event(
                    logger,
                    "info",
                    "CHECKPOINT",
                    "no existing checkpoint on SG – starting from 0",
                    operation="SELECT",
                    storage="sg",
                )
                self._seq = "0"
            else:
                log_event(
                    logger,
                    "warn",
                    "CHECKPOINT",
                    "checkpoint load fell back to local storage",
                    operation="SELECT",
                    status=exc.status,
                    storage="fallback",
                )
                self._seq = self._load_fallback()
                if self._metrics:
                    self._metrics.inc("checkpoint_loads_total")
                    self._metrics.inc("checkpoint_load_errors_total")
        except Exception as exc:
            log_event(
                logger,
                "warn",
                "CHECKPOINT",
                "checkpoint load fell back to local storage: %s" % exc,
                operation="SELECT",
                storage="fallback",
            )
            self._seq = self._load_fallback()
            if self._metrics:
                self._metrics.inc("checkpoint_loads_total")
                self._metrics.inc("checkpoint_load_errors_total")

        return self._seq

    async def save(
        self,
        seq: str,
        http: "RetryableHTTP",
        base_url: str,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
    ) -> None:
        """PUT {keyspace}/_local/checkpoint-{uuid} on Sync Gateway."""
        if not self._enabled:
            return

        async with self._lock:
            self._seq = seq
            body: dict = {
                "client_id": self._client_id,
                "time": int(time.time()),
                "remote": seq,
                "initial_sync_done": self._initial_sync_done,
            }
            if self._rev:
                body["_rev"] = self._rev

            url = f"{base_url}/{self.local_doc_path}"
            ic("checkpoint save", url, seq)
            try:
                req_headers = {**headers, "Content-Type": "application/json"}
                resp = await http.request(
                    "PUT", url, json=body, auth=auth, headers=req_headers
                )
                resp_data = await resp.json()
                resp.release()
                self._rev = resp_data.get("rev", self._rev)
                log_event(
                    logger,
                    "debug",
                    "CHECKPOINT",
                    "checkpoint saved",
                    operation="UPDATE",
                    storage="sg",
                )
                log_event(
                    logger,
                    "debug",
                    "CHECKPOINT",
                    "checkpoint save detail",
                    operation="UPDATE",
                    seq=seq,
                    doc_id=self._local_doc_id,
                    storage="sg",
                )
            except Exception as exc:
                log_event(
                    logger,
                    "warn",
                    "CHECKPOINT",
                    "checkpoint save fell back to local storage: %s" % exc,
                    operation="UPDATE",
                    seq=seq,
                    storage="fallback",
                )
                self._save_fallback(seq)
                if self._metrics:
                    self._metrics.inc("checkpoint_save_errors_total")

    # -- Local file fallback ---------------------------------------------------

    def _get_fallback_store(self) -> CBLStore:
        """Lazily create and reuse a CBLStore for fallback checkpoint operations."""
        if self._fallback_store is None:
            self._fallback_store = CBLStore()
        return self._fallback_store

    def _load_fallback(self) -> str:
        if USE_CBL:
            data = self._get_fallback_store().load_checkpoint(self._uuid)
            if data:
                seq = str(data.get("remote", data.get("SGs_Seq", "0")))
                raw_isd = data.get("initial_sync_done", None)
                if raw_isd is None:
                    self._initial_sync_done = seq != "0"
                else:
                    self._initial_sync_done = bool(raw_isd)
                ic("checkpoint loaded from CBL", seq)
                return seq
            return "0"
        # Original file fallback
        if self._fallback_path.exists():
            data = json.loads(self._fallback_path.read_text())
            seq = str(
                data.get("remote", data.get("SGs_Seq", data.get("last_seq", "0")))
            )
            raw_isd = data.get("initial_sync_done", None)
            if raw_isd is None:
                self._initial_sync_done = seq != "0"
            else:
                self._initial_sync_done = bool(raw_isd)
            ic("checkpoint loaded from file", seq)
            return seq
        return "0"

    def _save_fallback(self, seq: str) -> None:
        if USE_CBL:
            self._get_fallback_store().save_checkpoint(self._uuid, seq, self._client_id)
            ic("checkpoint saved to CBL", seq)
            return
        # Original file fallback
        self._fallback_path.write_text(
            json.dumps(
                {
                    "client_id": self._client_id,
                    "time": int(time.time()),
                    "remote": seq,
                    "initial_sync_done": self._initial_sync_done,
                }
            )
        )
        ic("checkpoint saved to file", seq)


# ---------------------------------------------------------------------------
# Core: changes feed loop
# ---------------------------------------------------------------------------

# Keys whose values are always sensitive and should never appear in logs,
# even when the Redactor misses them (e.g., nested inside provider blocks).
_SENSITIVE_CONFIG_KEYS = frozenset(
    {
        "password",
        "passwd",
        "pass",
        "secret",
        "api_key",
        "access_key_id",
        "secret_access_key",
        "session_token",
        "bearer_token",
        "token",
        "session_cookie",
        "authorization",
        "cookie",
        "refresh_token",
        "username",
        "user",
    }
)


def _sanitize_config(obj, *, _depth: int = 0):
    """Deep-copy *obj* with sensitive values replaced by '***'.

    Works on nested dicts/lists.  Uses the Redactor for string
    pattern matching and additionally strips any key in
    ``_SENSITIVE_CONFIG_KEYS`` regardless of nesting.
    """
    if _depth > 20:
        return "..."
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k.lower() in _SENSITIVE_CONFIG_KEYS:
                out[k] = "***"
            else:
                out[k] = _sanitize_config(v, _depth=_depth + 1)
        return out
    if isinstance(obj, (list, tuple)):
        return [_sanitize_config(i, _depth=_depth + 1) for i in obj]
    # Redact URLs with embedded credentials
    if isinstance(obj, str):
        return get_redactor().redact_string(obj)
    return obj


def _log_job_config(
    job_id: str | None,
    src: str,
    gw: dict,
    feed_cfg: dict,
    proc_cfg: dict,
    out_cfg: dict,
    retry_cfg: dict,
    cfg: dict,
) -> None:
    """Emit a block of INFO-level log lines describing the job's
    non-sensitive configuration.  Called once at job startup so
    operators can trace what a job was set up to do.
    """
    # -- Source / Gateway --
    safe_gw = _sanitize_config(gw)
    log_event(
        logger,
        "info",
        "CONTROL",
        "job config: source",
        job_id=job_id,
        mode="source=%s url=%s db=%s scope=%s collection=%s"
        % (
            src,
            safe_gw.get("url", safe_gw.get("host", "?")),
            safe_gw.get("database", "?"),
            safe_gw.get("scope", ""),
            safe_gw.get("collection", ""),
        ),
    )

    # -- Changes Feed --
    log_event(
        logger,
        "info",
        "CONTROL",
        "job config: changes_feed",
        job_id=job_id,
        mode="feed_type=%s include_docs=%s active_only=%s since=%s "
        "timeout_ms=%s heartbeat_ms=%s channels=%s limit=%s throttle=%s "
        "optimize_initial=%s catchup_limit=%s flood_threshold=%s"
        % (
            feed_cfg.get("feed_type", "longpoll"),
            feed_cfg.get("include_docs", False),
            feed_cfg.get("active_only", False),
            feed_cfg.get("since", "0"),
            feed_cfg.get("timeout_ms", 60000),
            feed_cfg.get("heartbeat_ms", 30000),
            feed_cfg.get("channels", []),
            feed_cfg.get("limit", 0),
            feed_cfg.get("throttle_feed", 0),
            feed_cfg.get("optimize_initial_sync", False),
            feed_cfg.get("continuous_catchup_limit", 500),
            feed_cfg.get("flood_threshold", 10000),
        ),
    )

    # -- Processing --
    log_event(
        logger,
        "info",
        "CONTROL",
        "job config: processing",
        job_id=job_id,
        mode="sequential=%s max_concurrent=%s dry_run=%s "
        "ignore_delete=%s ignore_remove=%s write_method=%s "
        "get_batch_number=%s"
        % (
            proc_cfg.get("sequential", False),
            proc_cfg.get("max_concurrent", 20),
            proc_cfg.get("dry_run", False),
            proc_cfg.get("ignore_delete", False),
            proc_cfg.get("ignore_remove", False),
            proc_cfg.get("write_method", "PUT"),
            proc_cfg.get("get_batch_number", 100),
        ),
    )

    # -- Output --
    safe_out = _sanitize_config(out_cfg)
    output_mode = safe_out.get("mode", "http")
    out_summary = "mode=%s" % output_mode
    if output_mode == "http":
        out_summary += " target_url=%s write_method=%s delete_method=%s" % (
            safe_out.get("target_url", ""),
            safe_out.get("write_method", "PUT"),
            safe_out.get("delete_method", "DELETE"),
        )
        out_summary += " url_template=%s send_delete_body=%s" % (
            safe_out.get("url_template", ""),
            safe_out.get("send_delete_body", False),
        )
        out_summary += " timeout=%ss halt_on_failure=%s data_error_action=%s" % (
            safe_out.get("request_timeout_seconds", 30),
            safe_out.get("halt_on_failure", True),
            safe_out.get("data_error_action", "dlq"),
        )
    elif output_mode in ("postgres", "mysql", "mssql", "oracle", "db"):
        db_block = safe_out.get(output_mode, safe_out.get("db", {}))
        out_summary += " host=%s port=%s database=%s schema=%s" % (
            db_block.get("host", "?"),
            db_block.get("port", "?"),
            db_block.get("database", "?"),
            db_block.get("schema", "public"),
        )
        out_summary += " pool_min=%s pool_max=%s ssl=%s" % (
            db_block.get("pool_min", 2),
            db_block.get("pool_max", 10),
            db_block.get("ssl", False),
        )
    elif output_mode in ("s3", "gcs", "azure"):
        cloud_block = safe_out.get(output_mode, {})
        out_summary += " bucket=%s region=%s key_prefix=%s" % (
            cloud_block.get("bucket", ""),
            cloud_block.get("region", ""),
            cloud_block.get("key_prefix", ""),
        )
    log_event(
        logger,
        "info",
        "CONTROL",
        "job config: output",
        job_id=job_id,
        mode=out_summary,
    )

    # -- DLQ --
    dlq_cfg = out_cfg.get("dlq") or {}
    log_event(
        logger,
        "info",
        "CONTROL",
        "job config: dlq",
        job_id=job_id,
        mode="dead_letter_path=%s retention_s=%s max_replay=%s"
        % (
            out_cfg.get("dead_letter_path", ""),
            dlq_cfg.get("retention_seconds", 86400),
            dlq_cfg.get("max_replay_attempts", 10),
        ),
    )

    # -- Retry --
    log_event(
        logger,
        "info",
        "CONTROL",
        "job config: retry",
        job_id=job_id,
        mode="max_retries=%s backoff_base=%ss backoff_max=%ss retry_on_status=%s"
        % (
            retry_cfg.get("max_retries", 5),
            retry_cfg.get("backoff_base_seconds", 1),
            retry_cfg.get("backoff_max_seconds", 60),
            retry_cfg.get("retry_on_status", [500, 502, 503, 504]),
        ),
    )

    # -- Checkpoint --
    chk_cfg = cfg.get("checkpoint", {})
    log_event(
        logger,
        "info",
        "CONTROL",
        "job config: checkpoint",
        job_id=job_id,
        mode="enabled=%s client_id=%s every_n_docs=%s"
        % (
            chk_cfg.get("enabled", True),
            chk_cfg.get("client_id", "changes_worker"),
            chk_cfg.get("every_n_docs", 0),
        ),
    )

    # -- Shutdown --
    shut_cfg = cfg.get("shutdown", {})
    log_event(
        logger,
        "info",
        "CONTROL",
        "job config: shutdown",
        job_id=job_id,
        mode="drain_timeout=%ss dlq_inflight=%s"
        % (
            shut_cfg.get("drain_timeout_seconds", 60),
            shut_cfg.get("dlq_inflight_on_shutdown", False),
        ),
    )

    # -- Attachments (only if configured) --
    att_cfg = cfg.get("attachments", {})
    if att_cfg.get("enabled", False):
        safe_att = _sanitize_config(att_cfg)
        dest = safe_att.get("destination", {})
        log_event(
            logger,
            "info",
            "CONTROL",
            "job config: attachments",
            job_id=job_id,
            mode="mode=%s dry_run=%s dest_type=%s "
            "partial_success=%s halt_on_failure=%s"
            % (
                safe_att.get("mode", "individual"),
                safe_att.get("dry_run", False),
                dest.get("type", "s3"),
                safe_att.get("partial_success", "continue"),
                safe_att.get("halt_on_failure", True),
            ),
        )

    # -- Eventing (only if configured) --
    ev_cfg = cfg.get("eventing", {})
    if ev_cfg.get("handlers") or ev_cfg.get("source") or ev_cfg.get("source_file"):
        log_event(
            logger,
            "info",
            "CONTROL",
            "job config: eventing",
            job_id=job_id,
            mode="source=%s timeout_ms=%s"
            % (
                "inline" if ev_cfg.get("source") else ev_cfg.get("source_file", "?"),
                ev_cfg.get("timeout_ms", 5000),
            ),
        )

    # -- Mapping (only if configured) --
    map_cfg = cfg.get("mapping", {})
    if map_cfg:
        tables = map_cfg.get("tables", [])
        table_names = [t.get("table", "?") for t in tables] if tables else []
        log_event(
            logger,
            "info",
            "CONTROL",
            "job config: mapping",
            job_id=job_id,
            mode="tables=%s" % table_names,
        )

    # -- Recursion guard (only if configured) --
    rg_cfg = cfg.get("recursion_guard", {})
    if rg_cfg.get("enabled", False):
        log_event(
            logger,
            "info",
            "CONTROL",
            "job config: recursion_guard",
            job_id=job_id,
            mode="max_tracked=%s ttl=%ss"
            % (
                rg_cfg.get("max_tracked_docs", 50000),
                rg_cfg.get("ttl_seconds", 300),
            ),
        )


async def poll_changes(
    cfg: dict,
    src: str,
    shutdown_event: asyncio.Event,
    metrics: MetricsCollector | None = None,
    restart_event: asyncio.Event | None = None,
    job_id: str | None = None,  # Phase 6: job-specific identifier
    map_executor=None,  # ThreadPoolExecutor for CPU-bound schema mapping
) -> None:
    # Set job tag and session ID in context so every log_event in this
    # async context automatically includes them in the prefix.
    if job_id:
        set_job_tag(job_id)
    # Generate a unique session ID for this job run.  If Pipeline.run()
    # already set one (thread context), reuse it; otherwise create one.
    if not get_session_id():
        set_session_id(generate_session_id())
    session_id = get_session_id()

    # Log the full session UUID once at startup so operators can correlate
    # the short #s:.. tag in subsequent lines back to this session.
    log_event(
        logger,
        "info",
        "CONTROL",
        "session started",
        job_id=job_id,
        mode="session=%s" % session_id,
    )

    gw = cfg.get(
        "gateway", cfg.get("inputs", [{}])[0]
    )  # Support both old and new configs
    auth_cfg = cfg.get("auth", gw.get("auth", {}))  # Phase 6: auth from gateway
    feed_cfg = cfg.get(
        "changes_feed", gw.get("changes_feed", {})
    )  # Phase 6: changes_feed from gateway
    proc_cfg = cfg.get(
        "processing", gw.get("processing", {})
    )  # Phase 6: processing from gateway
    out_cfg = cfg.get(
        "output", cfg.get("outputs", [{}])[0]
    )  # Support both old and new configs
    retry_cfg = cfg.get("retry", {})

    # Dump the full job configuration at startup (non-sensitive fields only).
    # These config lines share the same #s:.. session tag, so you can
    # always connect a session's config to its runtime log lines.
    _log_job_config(job_id, src, gw, feed_cfg, proc_cfg, out_cfg, retry_cfg, cfg)

    log_event(logger, "info", "PROCESSING", "source type: %s" % src)

    # Combine shutdown + restart into a single stop_event so all inner loops
    # (catch-up, continuous, websocket, longpoll) break on either signal.
    stop_event = asyncio.Event()

    async def _watch_events() -> None:
        waiters = [asyncio.ensure_future(shutdown_event.wait())]
        if restart_event is not None:
            waiters.append(asyncio.ensure_future(restart_event.wait()))
        done, pending = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        stop_event.set()
        for f in pending:
            f.cancel()

    watcher_task = asyncio.create_task(_watch_events())

    try:
        base_url = build_base_url(gw)
    except KeyError as e:
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass
        raise KeyError(
            f"Missing gateway field {e} — check that the job's input has "
            f"'url' (or 'host') and 'database' configured"
        ) from e
    ssl_ctx = build_ssl_context(gw)
    basic_auth = build_basic_auth(auth_cfg)
    auth_headers = build_auth_headers(auth_cfg, src, compress=gw.get("compress", False))

    channels = feed_cfg.get("channels", [])
    checkpoint = Checkpoint(
        cfg.get("checkpoint", {}), gw, channels, job_id=job_id
    )  # Phase 6: pass job_id
    if metrics:
        checkpoint.set_metrics(metrics)

    # Session-level timeout is kept loose; the _changes request uses its own.
    timeout = aiohttp.ClientTimeout(total=None, sock_read=None)
    connector = aiohttp.TCPConnector(ssl=ssl_ctx) if ssl_ctx else aiohttp.TCPConnector()

    # Per-request timeout for _changes calls.
    # since=0 can return 100K+ rows and take minutes, so this must be much
    # higher than a typical 30-75s HTTP timeout.  Default 300s (5 min).
    changes_http_timeout = aiohttp.ClientTimeout(
        total=feed_cfg.get("http_timeout_seconds", 300),
    )

    max_concurrent = proc_cfg.get("max_concurrent", 20)
    dry_run = proc_cfg.get("dry_run", False)
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        http = RetryableHTTP(session, retry_cfg)
        if metrics:
            http.set_metrics(metrics)
        http.set_shutdown_event(stop_event)

        output_mode = out_cfg.get("mode")
        db_output = None  # track DB forwarder for cleanup
        cloud_output = None  # track cloud forwarder for cleanup

        _DB_ENGINE_ALIASES = {"postgres", "mysql", "mssql", "oracle"}
        if output_mode in _DB_ENGINE_ALIASES:
            db_engine = output_mode
            output_mode = "db"
        elif output_mode == "db":
            db_engine = out_cfg.get("db", {}).get("engine", "postgres")

        if output_mode == "db":
            if db_engine == "postgres":
                from db.db_postgres import PostgresOutputForwarder

                output = PostgresOutputForwarder(out_cfg, dry_run, metrics=metrics)
            elif db_engine == "mysql":
                from db.db_mysql import MySQLOutputForwarder

                output = MySQLOutputForwarder(out_cfg, dry_run, metrics=metrics)
            elif db_engine == "mssql":
                from db.db_mssql import MSSQLOutputForwarder

                output = MSSQLOutputForwarder(out_cfg, dry_run, metrics=metrics)
            elif db_engine == "oracle":
                from db.db_oracle import OracleOutputForwarder

                output = OracleOutputForwarder(out_cfg, dry_run, metrics=metrics)
            else:
                raise ValueError(f"Unsupported db engine: {db_engine}")
            if map_executor is not None:
                output.set_map_executor(map_executor)
            await output.connect()
            db_output = output
            log_event(
                logger, "info", "OUTPUT", f"database output ready (engine={db_engine})"
            )
        elif output_mode in ("s3", "gcs", "azure"):
            from cloud import create_cloud_output

            output = create_cloud_output(out_cfg, dry_run, metrics=metrics)
            cloud_output = output
            # §3.1: Connect-with-retry for cloud outputs (consistent with HTTP pattern)
            cloud_connect_failure_count = 0
            backoff_base = retry_cfg.get("backoff_base_seconds", 1)
            backoff_max = min(retry_cfg.get("backoff_max_seconds", 60), 300)
            while not stop_event.is_set():
                try:
                    await output.connect()
                    log_event(
                        logger,
                        "info",
                        "OUTPUT",
                        "cloud output ready (provider=%s)" % output_mode,
                    )
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    cloud_connect_failure_count += 1
                    if cloud_connect_failure_count == 1:
                        log_event(
                            logger,
                            "warn",
                            "OUTPUT",
                            "Cloud output unreachable — waiting for cloud store to become available (will retry with backoff)",
                        )
                    else:
                        log_event(
                            logger,
                            "debug",
                            "OUTPUT",
                            "cloud output connect failed (attempt #%d)"
                            % cloud_connect_failure_count,
                            error_detail=str(exc),
                        )
                    if cloud_connect_failure_count >= 100:
                        log_event(
                            logger,
                            "error",
                            "OUTPUT",
                            "cloud output unreachable after 100 retries – aborting",
                        )
                        return
                    delay = min(
                        backoff_base * (2 ** (cloud_connect_failure_count - 1)),
                        backoff_max,
                    )
                    await _sleep_or_shutdown(delay, stop_event)
        else:
            output = OutputForwarder(
                session,
                out_cfg,
                dry_run,
                metrics=metrics,
                build_basic_auth_fn=build_basic_auth,
                build_auth_headers_fn=build_auth_headers,
                retryable_http_cls=RetryableHTTP,
            )
            # Make output retries shutdown-aware
            if hasattr(output, "_http") and output._http is not None:
                output._http.set_shutdown_event(shutdown_event)

        dlq = DeadLetterQueue(
            out_cfg.get("dead_letter_path", ""),
            dlq_cfg=out_cfg.get("dlq"),
        )
        every_n_docs = cfg.get("checkpoint", {}).get("every_n_docs", 0)

        # Warn at runtime if non-sequential + no DLQ
        is_seq = proc_cfg.get("sequential", False)
        if not is_seq and not dlq.enabled:
            log_event(
                logger,
                "warn",
                "PROCESSING",
                "RISK: running in non-sequential (parallel) mode WITHOUT a Dead Letter Queue. "
                "If the output goes down mid-batch, in-flight documents may be lost. "
                "Enable the DLQ or switch to sequential mode.",
            )

        # Warn if parallel mode + POST method
        if not is_seq and proc_cfg.get("write_method", "PUT").upper() == "POST":
            log_event(
                logger,
                "warn",
                "PROCESSING",
                "Parallel mode with POST method may produce duplicate requests on retry. "
                "Consider using PUT (idempotent) or sequential mode for POST endpoints.",
            )

        # §3.1: If output is HTTP, startup retry loop until endpoint is reachable
        if output_mode == "http":
            output_failure_count = 0
            backoff_base = retry_cfg.get("backoff_base_seconds", 1)
            backoff_max = min(
                retry_cfg.get("backoff_max_seconds", 60), 300
            )  # cap at 300s
            while not stop_event.is_set():
                if await output.test_reachable():
                    log_event(
                        logger,
                        "info",
                        "OUTPUT",
                        "output endpoint is reachable",
                    )
                    break
                output_failure_count += 1
                if output_failure_count == 1:
                    log_event(
                        logger,
                        "warn",
                        "OUTPUT",
                        "HTTP output endpoint unreachable — waiting for endpoint to become available (will retry with backoff)",
                    )
                else:
                    log_event(
                        logger,
                        "debug",
                        "OUTPUT",
                        "output endpoint reachability check failed (attempt #%d)"
                        % output_failure_count,
                    )
                if output_failure_count < 100:  # Safety limit to prevent infinite loop
                    delay = min(
                        backoff_base * (2 ** (output_failure_count - 1)), backoff_max
                    )
                    await _sleep_or_shutdown(delay, stop_event)
                else:
                    # If we've retried 100 times, something is seriously wrong
                    log_event(
                        logger,
                        "error",
                        "OUTPUT",
                        "output endpoint unreachable after 100 retries – aborting",
                    )
                    return
            # Start periodic heartbeat if configured
            await output.start_heartbeat(stop_event)

        # Load checkpoint from SG _local doc (CBL-style)
        since = feed_cfg.get("since", "0")
        if since == "0" and cfg.get("checkpoint", {}).get("enabled", True):
            since = await checkpoint.load(http, base_url, basic_auth, auth_headers)

        # ── Helper: replay DLQ on recovery (called from feed outer loops) ──
        async def _replay_dlq_on_recovery(
            dlq, output, metrics, shutdown_event, out_cfg
        ):
            """Replay pending DLQ entries after output recovery, before
            resuming the changes feed.  Only runs if the output is
            actually reachable (pool is alive)."""
            if stop_event.is_set():
                return
            # Only replay if the output pool is alive
            if hasattr(output, "_pool") and output._pool is None:
                return
            pending_count = (
                dlq._store.dlq_count() if dlq._store else len(dlq.list_pending())
            )
            if pending_count == 0:
                return
            log_event(
                logger,
                "info",
                "DLQ",
                "output recovered — replaying %d DLQ entries before resuming feed"
                % pending_count,
            )
            dlq_summary = await _replay_dead_letter_queue(
                dlq,
                output,
                metrics,
                shutdown_event,
                current_target_url=out_cfg.get("target_url", ""),
            )
            if dlq_summary["total"] > 0:
                log_event(
                    logger,
                    "info",
                    "DLQ",
                    "recovery replay summary: %s" % dlq_summary,
                )
            if metrics:
                count = (
                    dlq._store.dlq_count() if dlq._store else len(dlq.list_pending())
                )
                metrics.set("dlq_pending_count", count)

        # ── Replay dead-letter queue before processing new changes ────
        if dlq.enabled and not stop_event.is_set():
            dlq_summary = await _replay_dead_letter_queue(
                dlq,
                output,
                metrics,
                shutdown_event,
                current_target_url=out_cfg.get("target_url", ""),
            )
            if dlq_summary["total"] > 0:
                log_event(logger, "info", "DLQ", "DLQ replay summary: %s" % dlq_summary)
            # Update DLQ pending count gauge after replay
            if metrics:
                # Use dlq_count() instead of list_pending() to avoid loading all docs into memory
                count = (
                    dlq._store.dlq_count() if dlq._store else len(dlq.list_pending())
                )
                metrics.set("dlq_pending_count", count)

        # DLQ replay-on-recovery: when the output recovers after a failure,
        # replay pending DLQ entries before resuming the changes feed.
        dlq_cfg = out_cfg.get("dlq") or {}
        replay_on_recovery = dlq_cfg.get("replay_on_recovery", False)

        throttle = feed_cfg.get("throttle_feed", 0)

        # Initial sync: when starting from since=0 (or resuming an
        # interrupted initial pull), optimise the _changes feed:
        #   Couchbase: active_only=true so deleted/removed are excluded
        #   CouchDB:   no active_only support → ignore deletes in processing
        # In both cases: include_docs=false.
        #
        # optimize_initial_sync=true  → chunked with limit (faster for
        #   huge feeds but may miss deletes between chunks)
        # optimize_initial_sync=false (default) → single large request,
        #   no limit, long timeout — simpler and no consistency gap
        requested_since = feed_cfg.get("since", "0")
        initial_sync = requested_since == "0" and not checkpoint.initial_sync_done
        optimize_initial = feed_cfg.get("optimize_initial_sync", False)
        if initial_sync:
            if optimize_initial:
                log_event(
                    logger,
                    "info",
                    "CHANGES",
                    "initial sync mode (optimized/chunked) – %s, "
                    "include_docs=false, limit=%d"
                    % (
                        "active_only=true"
                        if src != "couchdb"
                        else "ignoring deletes/removes (CouchDB)",
                        feed_cfg.get("continuous_catchup_limit", 10000),
                    ),
                )
            else:
                log_event(
                    logger,
                    "info",
                    "CHANGES",
                    "initial sync mode – %s, include_docs=false, "
                    "single request (http_timeout=%ds)"
                    % (
                        "active_only=true"
                        if src != "couchdb"
                        else "ignoring deletes/removes (CouchDB)",
                        feed_cfg.get("http_timeout_seconds", 300),
                    ),
                )

        # feed_type / timeout validation is handled by validate_config()
        # at startup — no runtime fallbacks needed here.
        feed_type = feed_cfg.get("feed_type", "longpoll")
        timeout_ms = feed_cfg.get("timeout_ms", 60000)

        changes_url = f"{base_url}/_changes"

        # ── Attachment processor ──────────────────────────────────────
        att_cfg = parse_attachment_config(cfg.get("attachments", {}))
        att_processor = (
            AttachmentProcessor(
                att_cfg, metrics=metrics, gateway_cfg=cfg.get("gateway", {})
            )
            if att_cfg.enabled
            else None
        )
        if att_processor:
            log_event(
                logger,
                "info",
                "PROCESSING",
                "attachment processing enabled (mode=%s, dry_run=%s)"
                % (att_cfg.mode, att_cfg.dry_run),
            )

        # ── Eventing handler (JS OnUpdate/OnDelete) ─────────────────
        from eventing.eventing import create_eventing_handler

        eventing_cfg = cfg.get("eventing", {})
        eventing_handler = create_eventing_handler(eventing_cfg, metrics=metrics)
        if eventing_handler:
            log_event(
                logger,
                "info",
                "EVENTING",
                "eventing enabled — JS handlers loaded",
            )

        # ── Recursion guard (write-back echo suppression) ────────────
        from eventing.recursion_guard import create_recursion_guard

        recursion_guard_cfg = cfg.get("recursion_guard", {})
        recursion_guard = create_recursion_guard(recursion_guard_cfg)
        if recursion_guard:
            log_event(
                logger,
                "info",
                "RECURSION_GUARD",
                "recursion guard enabled — max_tracked=%d ttl=%ds"
                % (
                    recursion_guard_cfg.get("max_tracked_docs", 50000),
                    recursion_guard_cfg.get("ttl_seconds", 300),
                ),
            )

        # Shared kwargs for _process_changes_batch / catch-up / continuous
        shutdown_cfg = cfg.get("shutdown", {})
        batch_kwargs = dict(
            feed_cfg=feed_cfg,
            proc_cfg=proc_cfg,
            output=output,
            dlq=dlq,
            checkpoint=checkpoint,
            http=http,
            base_url=base_url,
            basic_auth=basic_auth,
            auth_headers=auth_headers,
            semaphore=semaphore,
            src=src,
            metrics=metrics,
            every_n_docs=every_n_docs,
            max_concurrent=max_concurrent,
            shutdown_cfg=shutdown_cfg,
            attachment_processor=att_processor,
            eventing_handler=eventing_handler,
            recursion_guard=recursion_guard,
        )

        # Log replication settings at startup
        log_event(
            logger,
            "info",
            "CHANGES",
            "replication config: feed=%s, active_only=%s, include_docs=%s, "
            "since=%s, initial_sync=%s, initial_sync_done=%s, "
            "optimize_initial_sync=%s"
            % (
                feed_type,
                feed_cfg.get("active_only", False),
                feed_cfg.get("include_docs", False),
                since,
                initial_sync,
                checkpoint.initial_sync_done,
                optimize_initial,
            ),
        )

        try:
            # ── Continuous mode: 2-phase catch-up then stream ────────────────
            if feed_type == "continuous":
                log_event(
                    logger,
                    "info",
                    "CHANGES",
                    "feed mode: continuous (catch-up → stream)",
                )
                while not stop_event.is_set():
                    # Replay DLQ before resuming changes feed (on recovery)
                    if dlq.enabled and replay_on_recovery and not initial_sync:
                        await _replay_dlq_on_recovery(
                            dlq, output, metrics, shutdown_event, out_cfg
                        )
                    since = await _catch_up_normal(
                        since=since,
                        changes_url=changes_url,
                        retry_cfg=retry_cfg,
                        shutdown_event=stop_event,
                        timeout_ms=timeout_ms,
                        changes_http_timeout=changes_http_timeout,
                        initial_sync=initial_sync,
                        **batch_kwargs,
                    )
                    initial_sync = False
                    if stop_event.is_set():
                        break
                    since = await _consume_continuous_stream(
                        since=since,
                        changes_url=changes_url,
                        retry_cfg=retry_cfg,
                        session=session,
                        shutdown_event=stop_event,
                        timeout_ms=timeout_ms,
                        **batch_kwargs,
                    )
                return

            # ── WebSocket mode: catch-up then stream via ws:// ───────────────
            if feed_type == "websocket":
                log_event(
                    logger,
                    "info",
                    "CHANGES",
                    "feed mode: websocket (catch-up → ws stream)",
                )
                while not stop_event.is_set():
                    # Replay DLQ before resuming changes feed (on recovery)
                    if dlq.enabled and replay_on_recovery and not initial_sync:
                        await _replay_dlq_on_recovery(
                            dlq, output, metrics, shutdown_event, out_cfg
                        )
                    # Phase 1: catch up using normal HTTP requests
                    since = await _catch_up_normal(
                        since=since,
                        changes_url=changes_url,
                        retry_cfg=retry_cfg,
                        shutdown_event=stop_event,
                        timeout_ms=timeout_ms,
                        changes_http_timeout=changes_http_timeout,
                        initial_sync=initial_sync,
                        **batch_kwargs,
                    )
                    initial_sync = False
                    if stop_event.is_set():
                        break
                    # Phase 2: switch to WebSocket stream
                    since = await _consume_websocket_stream(
                        since=since,
                        changes_url=changes_url,
                        retry_cfg=retry_cfg,
                        session=session,
                        shutdown_event=stop_event,
                        timeout_ms=timeout_ms,
                        **batch_kwargs,
                    )
                return

            # ── Polled mode (longpoll / normal / sse) ────────────────────────
            # For optimized initial sync, fetch the database update_seq
            # as a completion target so we know when to stop ignoring
            # deletes and switch to steady-state.
            poll_target_seq: int | None = None
            if initial_sync and optimize_initial:
                poll_target_seq = await fetch_db_update_seq(
                    http, base_url, basic_auth, auth_headers
                )

            while not stop_event.is_set():
                # During initial sync use feed=normal so the server
                # returns immediately with a limited result set instead
                # of blocking on longpoll.
                effective_feed = "normal" if initial_sync else feed_type
                body_payload = _build_changes_body(
                    feed_cfg,
                    src,
                    since,
                    effective_feed,
                    timeout_ms,
                    active_only_override=(
                        True if initial_sync and src != "couchdb" else None
                    ),
                    include_docs_override=False if initial_sync else None,
                )
                # throttle_feed overrides limit – eat the feed one bite at a time
                if throttle > 0:
                    body_payload["limit"] = throttle
                elif initial_sync and optimize_initial:
                    # Chunked initial sync: page through the feed.
                    body_payload["limit"] = feed_cfg.get(
                        "continuous_catchup_limit", 10000
                    )
                elif feed_cfg.get("limit", 0) > 0:
                    body_payload["limit"] = feed_cfg["limit"]

                ic(changes_url, body_payload, since)
                log_event(
                    logger,
                    "debug",
                    "CHANGES",
                    "polling _changes (since=%s, feed=%s)" % (since, feed_type),
                )

                try:
                    t0_changes = time.monotonic()
                    resp = await http.request(
                        "POST",
                        changes_url,
                        json=body_payload,
                        auth=basic_auth,
                        headers={**auth_headers, "Content-Type": "application/json"},
                        timeout=changes_http_timeout,
                    )
                    raw_body = await resp.read()
                    body = _json_loads(raw_body)
                    if metrics:
                        metrics.inc("bytes_received_total", len(raw_body))
                        metrics.record_changes_request_time(
                            time.monotonic() - t0_changes
                        )
                    resp.release()
                except (ClientHTTPError, RedirectHTTPError) as exc:
                    log_event(
                        logger,
                        "error",
                        "CHANGES",
                        "non-retryable error polling _changes",
                        error_detail=str(exc),
                    )
                    if metrics:
                        metrics.inc("poll_errors_total")
                    break
                except (ConnectionError, ServerHTTPError, asyncio.TimeoutError) as exc:
                    log_event(
                        logger,
                        "error",
                        "CHANGES",
                        "retries exhausted polling _changes",
                        error_detail=str(exc),
                    )
                    if metrics:
                        metrics.inc("poll_errors_total")
                    await _sleep_or_shutdown(
                        feed_cfg.get("poll_interval_seconds", 10), stop_event
                    )
                    continue

                results = body.get("results", [])
                last_seq = body.get("last_seq", since)
                ic(len(results), last_seq)

                since, output_failed = await _process_changes_batch(
                    results,
                    str(last_seq),
                    since,
                    initial_sync=initial_sync,
                    **batch_kwargs,
                )

                if output_failed:
                    log_event(
                        logger,
                        "warn",
                        "CHANGES",
                        "waiting %ds before retrying (checkpoint held)"
                        % feed_cfg.get("poll_interval_seconds", 10),
                        seq=since,
                    )
                    await _sleep_or_shutdown(
                        feed_cfg.get("poll_interval_seconds", 10), stop_event
                    )
                    continue

                # ── Output backpressure check ─────────────────────────
                await _maybe_backpressure(metrics, stop_event)

                # Check if optimized initial sync reached its target
                reached_poll_target = (
                    initial_sync
                    and poll_target_seq is not None
                    and results
                    and _parse_seq_number(last_seq) >= poll_target_seq
                )

                if not results or reached_poll_target:
                    if initial_sync:
                        initial_sync = False
                        checkpoint._initial_sync_done = True
                        await checkpoint.save(
                            since, http, base_url, basic_auth, auth_headers
                        )
                        log_event(
                            logger,
                            "info",
                            "CHANGES",
                            "initial sync complete – reverting to config settings"
                            + (
                                " (reached target_seq=%d)" % poll_target_seq
                                if reached_poll_target
                                else ""
                            ),
                        )
                    if not results:
                        await _sleep_or_shutdown(
                            feed_cfg.get("poll_interval_seconds", 10), stop_event
                        )
                    continue

                # When throttling: if we got a full batch there are more rows
                # waiting — loop immediately for the next bite. Only sleep once
                # we get a partial batch (caught up).
                if throttle > 0 and len(results) >= throttle:
                    log_event(
                        logger,
                        "info",
                        "CHANGES",
                        "throttle: got full batch (%d), fetching next bite immediately"
                        % len(results),
                    )
                    continue

                await _sleep_or_shutdown(
                    feed_cfg.get("poll_interval_seconds", 10), stop_event
                )
        finally:
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass
            await output.stop_heartbeat() if hasattr(output, "stop_heartbeat") else None
            if db_output is not None:
                await db_output.close()
            if cloud_output is not None:
                await cloud_output.close()


# ---------------------------------------------------------------------------
# Test connection
# ---------------------------------------------------------------------------


async def test_connection(cfg: dict, src: str) -> bool:
    """
    Verify the SG / App Service / Edge Server endpoint is reachable.

    Checks performed:
      1. GET {base_url}/  – server root (returns db info / welcome)
      2. GET {base_url}/_changes?since=0&limit=1 – confirm _changes endpoint responds
      3. Checkpoint read
    """
    gw = cfg["gateway"]
    auth_cfg = cfg["auth"]
    retry_cfg = cfg.get("retry", {})
    base_url = build_base_url(gw)
    root_url = (gw.get("url") or gw.get("host", "")).rstrip("/")
    ssl_ctx = build_ssl_context(gw)
    basic_auth = build_basic_auth(auth_cfg)
    auth_headers = build_auth_headers(auth_cfg, src, compress=gw.get("compress", False))

    connector = aiohttp.TCPConnector(ssl=ssl_ctx) if ssl_ctx else aiohttp.TCPConnector()
    timeout = aiohttp.ClientTimeout(total=15)
    ok = True

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        http = RetryableHTTP(session, {**retry_cfg, "max_retries": 1})

        # 1) Server root
        src_label = src.replace("_", " ").title()
        print(f"\n{'=' * 60}")
        print(f"  Source type:           {src_label}")
        print(f"  Testing connection to: {root_url}")
        print(f"  Keyspace:              {base_url}")
        print(f"  Auth method:           {auth_cfg.get('method', 'basic')}")
        print(f"{'=' * 60}\n")

        try:
            resp = await http.request(
                "GET", f"{root_url}/", auth=basic_auth, headers=auth_headers
            )
            body = await resp.json()
            resp.release()
            print(f"  [✓] Server root reachable")
            ic(body)
            for key in ("version", "vendor", "couchdb", "ADMIN"):
                if key in body:
                    print(f"      {key}: {body[key]}")
        except Exception as exc:
            print(f"  [✗] Server root UNREACHABLE: {exc}")
            ok = False

        # 2) Database / keyspace info
        try:
            resp = await http.request(
                "GET", f"{base_url}/", auth=basic_auth, headers=auth_headers
            )
            body = await resp.json()
            resp.release()
            db_name = body.get("db_name", body.get("name", "?"))
            state = body.get("state", "?")
            print(f"  [✓] Keyspace reachable  (db_name={db_name}, state={state})")
        except Exception as exc:
            print(f"  [✗] Keyspace UNREACHABLE: {exc}")
            ok = False

        # 3) _changes endpoint
        try:
            resp = await http.request(
                "POST",
                f"{base_url}/_changes",
                json={"since": "0", "limit": 1},
                auth=basic_auth,
                headers={**auth_headers, "Content-Type": "application/json"},
            )
            body = await resp.json()
            resp.release()
            last_seq = body.get("last_seq", "?")
            n_results = len(body.get("results", []))
            print(
                f"  [✓] _changes endpoint OK  (last_seq={last_seq}, sample_results={n_results})"
            )
        except Exception as exc:
            print(f"  [✗] _changes endpoint FAILED: {exc}")
            ok = False

        # 4) Checkpoint (read-only)
        channels = cfg.get("changes_feed", {}).get("channels", [])
        checkpoint = Checkpoint(cfg.get("checkpoint", {}), gw, channels)
        try:
            seq = await checkpoint.load(http, base_url, basic_auth, auth_headers)
            print(f"  [✓] Checkpoint readable   (saved since={seq})")
        except Exception as exc:
            print(f"  [✗] Checkpoint read FAILED: {exc}")
            ok = False

        # 5) Output / consumer endpoint (only when mode=http)
        out_cfg = cfg.get("output", {})
        if out_cfg.get("mode") == "http":
            output = OutputForwarder(
                session,
                out_cfg,
                dry_run=False,
                build_basic_auth_fn=build_basic_auth,
                build_auth_headers_fn=build_auth_headers,
                retryable_http_cls=RetryableHTTP,
            )
            if await output.test_reachable():
                print(
                    f"  [✓] Output endpoint reachable ({out_cfg.get('target_url', '')})"
                )
            else:
                print(
                    f"  [✗] Output endpoint UNREACHABLE ({out_cfg.get('target_url', '')})"
                )
                ok = False
        else:
            print(
                f"  [–] Output mode={out_cfg.get('mode', '?')} (no endpoint to check)"
            )

    print(f"\n{'=' * 60}")
    if ok:
        print("  Result: ALL CHECKS PASSED ✓")
    else:
        print("  Result: SOME CHECKS FAILED ✗")
    print(f"{'=' * 60}\n")
    return ok


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Couchbase _changes feed worker")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    parser.add_argument(
        "--test", action="store_true", help="Test connectivity and exit"
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    _ensure_full_logging_config(cfg)
    configure_logging(cfg.get("logging", {}))

    # Disable icecream unless TRACE level is configured — ic() does expensive
    # AST parsing and stack inspection on every call even when silent.
    log_cfg = cfg.get("logging", {})
    console_level = log_cfg.get("console", {}).get("log_level", "info").lower()
    if console_level != "trace":
        ic.disable()

    log_event(
        logger,
        "info",
        "PROCESSING",
        "changes_worker v%s starting (CBL=%s)" % (__version__, USE_CBL),
    )

    # Run migrations after logging is configured so we can see output
    if USE_CBL:
        migrate_files_to_cbl(args.config)
        migrate_default_to_collections()
        migrate_mappings_to_jobs()  # Phase 9: embed mappings into jobs

    # ── Startup config validation ────────────────────────────────────────
    src, warnings, errors = validate_config(cfg)

    src_label = src.replace("_", " ").title()
    log_event(logger, "info", "CONTROL", "source type: %s" % src_label)

    for w in warnings:
        log_event(logger, "warn", "CONTROL", "config warning: %s" % w)

    if errors:
        log_event(
            logger,
            "error",
            "CONTROL",
            "startup aborted – config errors detected",
            errors=errors,
            config_file=args.config,
        )
        sys.exit(1)

    if warnings:
        log_event(
            logger,
            "info",
            "CONTROL",
            "config validation passed with %d warning(s)" % len(warnings),
        )
    else:
        log_event(
            logger, "info", "CONTROL", "config validation passed – all settings OK"
        )
    # ─────────────────────────────────────────────────────────────────────

    if args.test:
        ok = asyncio.run(test_connection(cfg, src))
        sys.exit(0 if ok else 1)

    shutdown_event = asyncio.Event()
    restart_event = asyncio.Event()
    offline_event = asyncio.Event()

    def _signal_handler() -> None:
        log_event(logger, "info", "SHUTDOWN", "shutdown signal received")
        shutdown_event.set()

    # ── CBL maintenance scheduler ───────────────────────────────────────
    cbl_scheduler: CBLMaintenanceScheduler | None = None
    if USE_CBL:
        # Apply CBL config (db_dir / db_name) before any DB access
        cbl_cfg = cfg.get("couchbase_lite", {})
        # Backward compat: fall back to legacy "cbl_maintenance" key
        maint_cfg = cbl_cfg.get("maintenance", cfg.get("cbl_maintenance", {}))
        if cbl_cfg.get("db_dir") or cbl_cfg.get("db_name"):
            from storage.cbl_store import configure_cbl

            configure_cbl(cbl_cfg.get("db_dir"), cbl_cfg.get("db_name"))
        if maint_cfg.get("enabled", True):
            interval = maint_cfg.get("interval_hours", 24)
            cbl_scheduler = CBLMaintenanceScheduler(interval_hours=interval)
            cbl_scheduler.start()

    # ── Metrics server ───────────────────────────────────────────────────
    metrics_cfg = cfg.get("metrics", {})
    metrics: MetricsCollector | None = None
    metrics_runner: aiohttp.web.AppRunner | None = None

    if metrics_cfg.get("enabled", False):
        database = cfg.get("gateway", {}).get("database", "")
        log_dir = (
            cfg.get("logging", {})
            .get("file", {})
            .get("path", "logs/changes_worker.log")
        )
        log_dir = os.path.dirname(log_dir) or "logs"
        cbl_db_dir = ""
        if USE_CBL:
            from storage.cbl_store import CBL_DB_DIR, CBL_DB_NAME

            cbl_db_dir = os.path.join(CBL_DB_DIR, f"{CBL_DB_NAME}.cblite2")
        metrics = MetricsCollector(
            src, database, log_dir=log_dir, cbl_db_dir=cbl_db_dir
        )
        flood_threshold = cfg.get("changes_feed", {}).get("flood_threshold", 10000)
        metrics.set("flood_threshold", flood_threshold)

    loop = asyncio.new_event_loop()
    manager_thread = None
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        # ── Phase 6: PipelineManager-Based Job Orchestration ────────────
        db = None
        if USE_CBL:
            db = CBLStore()

        # Load enabled jobs for backward compatibility check
        enabled_jobs = []
        if db:
            enabled_jobs = load_enabled_jobs(db)

        # Backward compatibility: if no jobs and old config exists, auto-migrate
        if not enabled_jobs and cfg.get("gateway") and cfg.get("output"):
            job_doc = migrate_legacy_config_to_job(db, cfg)
            if job_doc:
                enabled_jobs = [job_doc]

        if not enabled_jobs:
            log_event(
                logger,
                "warn",
                "CONTROL",
                "no enabled jobs found. Visit the web UI to create jobs",
            )
            # Keep running for UI management
            log_event(logger, "info", "CONTROL", "waiting for jobs via web UI")

        # Create PipelineManager
        pipeline_manager = PipelineManager(
            cbl_store=db,
            config=cfg,
            metrics=metrics,
            logger=logger,
            poll_changes_func=poll_changes,
        )

        if metrics is not None:
            metrics_host = metrics_cfg.get("host", "0.0.0.0")
            metrics_port = metrics_cfg.get("port", 9090)

            def _register_extra_routes(app: aiohttp.web.Application) -> None:
                app["pipeline_manager"] = pipeline_manager
                app["main_loop"] = loop
                register_job_control_routes(app, pipeline_manager)
                log_event(
                    logger,
                    "debug",
                    "CONTROL",
                    "registered job control endpoints",
                )

            metrics_runner = loop.run_until_complete(
                start_metrics_server(
                    metrics,
                    metrics_host,
                    metrics_port,
                    restart_event=restart_event,
                    shutdown_event=shutdown_event,
                    offline_event=offline_event,
                    cbl_scheduler=cbl_scheduler,
                    shutdown_cfg=cfg.get("shutdown", {}),
                    extra_routes_cb=_register_extra_routes,
                    cfg=cfg,
                )
            )

        # Wire signal handler to PipelineManager
        def _pipeline_signal_handler() -> None:
            log_event(logger, "info", "SHUTDOWN", "shutdown signal received")
            pipeline_manager.trigger_shutdown()
            loop.call_soon_threadsafe(loop.stop)

        # Replace signal handler with PipelineManager-aware one
        loop.remove_signal_handler(signal.SIGINT)
        loop.remove_signal_handler(signal.SIGTERM)
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _pipeline_signal_handler)

        # Start PipelineManager in a background thread so the asyncio
        # event loop keeps running (needed for the metrics/API server).
        manager_thread = threading.Thread(
            target=pipeline_manager.start,
            name="pipeline-manager",
            daemon=False,
        )
        manager_thread.start()

        # Run the event loop on the main thread — this keeps the
        # aiohttp metrics server responsive to incoming requests.
        loop.run_forever()

    except KeyboardInterrupt:
        log_event(logger, "info", "SHUTDOWN", "interrupted")
        pipeline_manager.trigger_shutdown()
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "fatal error",
            error_detail="%s: %s" % (type(e).__name__, e),
        )
    finally:
        # Wait for manager thread to finish
        if manager_thread is not None and manager_thread.is_alive():
            manager_thread.join(timeout=60)
        if cbl_scheduler is not None:
            cbl_scheduler.stop()
        if metrics_runner is not None:
            loop.run_until_complete(metrics_runner.cleanup())
        loop.run_until_complete(loop.shutdown_asyncgens())
        if USE_CBL:
            close_db()
        loop.close()
        log_event(logger, "info", "SHUTDOWN", "shutdown complete")


if __name__ == "__main__":
    main()
