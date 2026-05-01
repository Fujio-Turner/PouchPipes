#!/usr/bin/env python3
"""
Pipeline — per-job thread wrapper for running a single job's _changes feed.

One Pipeline per job. Each has:
  - A threading.Thread that runs asyncio.run(poll_changes(...))
  - Its own HTTP session, checkpoint, metrics, output connection
  - Its own ThreadPoolExecutor for async middleware
  - Exception handling → DLQ
"""

import asyncio
import logging
import threading
import time
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor

from storage.cbl_store import CBLStore
from pipeline.pipeline_logging import (
    log_event,
    set_job_tag,
    generate_session_id,
    set_session_id,
)
from rest.changes_http import ClientHTTPError


class Pipeline:
    """Wraps one job's _changes feed in its own thread."""

    def __init__(
        self,
        job_id: str,
        job_doc: Dict[str, Any],
        cbl_store: CBLStore,
        metrics: Optional[Any],
        logger: logging.Logger,
        middleware_threads: int = 2,
        poll_changes_func: Optional[any] = None,
    ):
        """
        Parameters:
            job_id: UUID of the job (from job::{uuid})
            job_doc: Full job document with inputs[0], outputs[0], system config
            cbl_store: CBLStore instance
            metrics: MetricsCollector instance
            logger: Logger instance
            middleware_threads: Number of threads for async middleware pool
            poll_changes_func: The poll_changes coroutine to run for this job.
        """
        self.job_id = job_id
        self.job_doc = job_doc
        self.cbl_store = cbl_store
        self.metrics = metrics
        self.logger = logger
        self.middleware_threads = middleware_threads
        self.poll_changes_func = poll_changes_func

        # Thread state
        self._thread: Optional[threading.Thread] = None
        self._shutdown_event: Optional[asyncio.Event] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running = False
        self._error: Optional[str] = None
        self._error_count = 0
        self._auth_failure = False
        self._start_time: Optional[float] = None
        self._lock = threading.Lock()

        # Middleware executor (shared across all middleware in this job)
        self.middleware_executor = ThreadPoolExecutor(
            max_workers=middleware_threads,
            thread_name_prefix=f"middleware-{job_id[:8]}",
        )

        # Per-job logger with job_id tag
        self.logger = logging.getLogger(f"pipeline.{job_id[:8]}")

    def run(self) -> None:
        """
        Main thread entry point.  Uses self.poll_changes_func if set.
        """
        try:
            self._running = True
            self._start_time = time.time()
            self._error = None
            self._auth_failure = False

            # Tag all log lines from this thread with the job's short ID
            # and a unique session ID for this run.
            set_job_tag(self.job_id)
            session_id = generate_session_id()
            set_session_id(session_id)

            if not self.poll_changes_func:
                raise RuntimeError(
                    f"Pipeline started without poll_changes_func for job {self.job_id}"
                )

            # Build the config for this job from resolved input/output/system
            cfg = self._build_job_config()

            log_event(
                self.logger,
                "info",
                "CHANGES",
                "pipeline starting for job",
                job_id=self.job_id,
            )

            # Create and run the event loop for this job
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._shutdown_event = asyncio.Event()
            self._loop = loop

            try:
                # Call the provided poll_changes function
                loop.run_until_complete(
                    self.poll_changes_func(
                        cfg,
                        src=self.job_doc.get("inputs", [{}])[0].get(
                            "src",
                            self.job_doc.get("inputs", [{}])[0].get(
                                "source_type", "sync_gateway"
                            ),
                        ),
                        shutdown_event=self._shutdown_event,
                        metrics=self.metrics,
                        job_id=self.job_id,
                        map_executor=self.middleware_executor,
                    )
                )
            finally:
                self._loop = None
                loop.close()

            log_event(
                self.logger,
                "info",
                "CHANGES",
                "pipeline stopped cleanly",
                job_id=self.job_id,
            )

        except Exception as e:
            self._error = str(e)
            self._error_count += 1
            if isinstance(e, ClientHTTPError) and e.status in (401, 403):
                self._auth_failure = True
            # §3.3: Cloud auth failure detection (NoCredentialsError, InvalidAccessKeyId, etc.)
            try:
                from botocore.exceptions import NoCredentialsError, ClientError

                if isinstance(e, NoCredentialsError):
                    self._auth_failure = True
                elif isinstance(e, ClientError):
                    code = e.response.get("Error", {}).get("Code", "")
                    if code in (
                        "InvalidAccessKeyId",
                        "SignatureDoesNotMatch",
                        "ExpiredToken",
                    ):
                        self._auth_failure = True
                if self._auth_failure and not (
                    isinstance(e, ClientHTTPError) and e.status in (401, 403)
                ):
                    log_event(
                        self.logger,
                        "error",
                        "CHANGES",
                        "cloud authentication failed — fix credentials and restart the job manually.",
                        job_id=self.job_id,
                    )
            except ImportError:
                pass
            log_event(
                self.logger,
                "error",
                "CHANGES",
                "pipeline crashed: %s" % e,
                job_id=self.job_id,
            )
            # Write to DLQ if available
            try:
                self._write_crash_to_dlq(e)
            except Exception as dlq_err:
                log_event(
                    self.logger,
                    "error",
                    "DLQ",
                    "failed to write crash to DLQ: %s" % dlq_err,
                    job_id=self.job_id,
                )

        finally:
            with self._lock:
                self._running = False
                self._thread = None
                self._loop = None
                self._shutdown_event = None

    def stop(self, timeout_seconds: float = 30) -> bool:
        """
        Signal the pipeline to shut down and wait for thread to finish.

        Returns:
            True if stopped cleanly, False if timeout occurred.
        """
        with self._lock:
            if not self._running or self._thread is None:
                return True

            log_event(
                self.logger,
                "info",
                "CHANGES",
                "signaling pipeline to stop",
                job_id=self.job_id,
            )

            # Grab references under the lock
            thread = self._thread
            loop = self._loop
            shutdown_event = self._shutdown_event

        # Signal the asyncio shutdown event from this thread safely
        if loop is not None and shutdown_event is not None:
            try:
                loop.call_soon_threadsafe(shutdown_event.set)
            except RuntimeError:
                pass  # loop already closed

        # Wait for thread to exit
        if thread:
            thread.join(timeout=timeout_seconds)
            if thread.is_alive():
                log_event(
                    self.logger,
                    "warn",
                    "CHANGES",
                    "pipeline did not stop within %ss" % timeout_seconds,
                    job_id=self.job_id,
                )
                return False

        log_event(
            self.logger,
            "info",
            "CHANGES",
            "pipeline stopped",
            job_id=self.job_id,
        )
        return True

    def start(self) -> None:
        """Create and start the pipeline thread."""
        with self._lock:
            if self._running:
                log_event(
                    self.logger,
                    "warn",
                    "CHANGES",
                    "pipeline already running",
                    job_id=self.job_id,
                )
                return

            self._thread = threading.Thread(
                target=self.run,
                name=f"pipeline-{self.job_id[:8]}",
                daemon=False,
            )
            self._thread.start()

    def restart(self, timeout_seconds: float = 30) -> None:
        """Stop and restart the pipeline."""
        log_event(
            self.logger,
            "info",
            "CHANGES",
            "pipeline restarting",
            job_id=self.job_id,
        )
        self.stop(timeout_seconds)
        self.start()

    def is_running(self) -> bool:
        """Check if the pipeline thread is alive."""
        with self._lock:
            if self._thread is None:
                return False
            return self._thread.is_alive() and self._running

    def get_state(self) -> Dict[str, Any]:
        """Get the current state of the pipeline."""
        with self._lock:
            uptime = None
            if self._start_time:
                uptime = time.time() - self._start_time

            status = "stopped"
            if self._running:
                status = "running"
            elif self._error:
                status = "error"

            return {
                "job_id": self.job_id,
                "status": status,
                "uptime_seconds": uptime,
                "error_count": self._error_count,
                "last_error": self._error,
                "auth_failure": self._auth_failure,
            }

    def _build_job_config(self) -> Dict[str, Any]:
        """
        Build the config dict for poll_changes from the job document.

        This extracts inputs[0], outputs[0], system config and builds
        the legacy config shape that poll_changes expects.

        v2.0 input entries use ``host`` / ``source_type`` while the
        legacy ``poll_changes`` code expects ``url`` / ``src``.  This
        method normalises the fields so both schemas work.
        """
        cfg: Dict[str, Any] = {}

        # Extract input — job inputs[0] should already contain the
        # exact fields the pipeline expects (url, src, auth, etc.)
        input_entry = {}
        if self.job_doc.get("inputs"):
            input_entry = self.job_doc["inputs"][0]
            cfg["gateway"] = input_entry

        # poll_changes falls back to gw.get("auth"), gw.get("changes_feed"), etc.
        # but also checks top-level cfg["auth"], cfg["changes_feed"], cfg["processing"]
        if input_entry.get("auth"):
            cfg["auth"] = input_entry["auth"]
        if input_entry.get("changes_feed"):
            cfg["changes_feed"] = input_entry["changes_feed"]
        if input_entry.get("processing"):
            cfg["processing"] = input_entry["processing"]

        # Extract output — job outputs[0] should already contain the
        # exact fields the pipeline expects (mode, engine, username, etc.)
        if self.job_doc.get("outputs"):
            cfg["output"] = self.job_doc["outputs"][0]

        # Extract system config
        system = self.job_doc.get("system", {})
        cfg["checkpoint"] = system.get("checkpoint", {})
        cfg["processing"] = cfg.get("processing") or system.get("processing", {})
        cfg["retry"] = system.get("retry", {})
        cfg["shutdown"] = system.get("shutdown", {})
        cfg["attachments"] = system.get("attachments", {})

        # Extract mapping
        if self.job_doc.get("mapping"):
            cfg["mapping"] = self.job_doc["mapping"]

        # Extract eventing
        if self.job_doc.get("eventing"):
            cfg["eventing"] = self.job_doc["eventing"]

        # Extract recursion guard
        if self.job_doc.get("recursion_guard"):
            cfg["recursion_guard"] = self.job_doc["recursion_guard"]

        return cfg

    def _write_crash_to_dlq(self, error: Exception) -> None:
        """Write a pipeline crash entry to the DLQ."""
        dlq_entry = {
            "job_id": self.job_id,
            "doc_id": f"pipeline_crash_{self.job_id}",
            "seq": "crash",
            "error": str(error),
            "error_type": type(error).__name__,
            "doc_data": None,
            "timestamp": int(time.time() * 1000),
        }
        # TODO: use cbl_store to write to DLQ when the API is available
        # self.cbl_store.add_dlq_entry(dlq_entry)
