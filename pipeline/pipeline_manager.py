#!/usr/bin/env python3
"""
PipelineManager — owns and manages all job threads.

Responsibilities:
  - Load enabled jobs from CBL
  - Create one Pipeline per job
  - Start/stop/restart individual jobs (via REST API or lifecycle)
  - Enforce global max_threads config
  - Monitor threads for crashes; auto-restart with exponential backoff
  - Graceful shutdown: drain all jobs, save checkpoints
"""

import asyncio
import logging
import threading
import time
from typing import Dict, Optional, Any, List
from collections import deque

from pipeline.pipeline import Pipeline
from storage.cbl_store import CBLStore
from pipeline.pipeline_logging import log_event


class PipelineManager:
    """Manages all job pipelines (threads)."""

    def __init__(
        self,
        cbl_store: CBLStore,
        config: Dict[str, Any],
        metrics: Optional[Any],
        logger: logging.Logger,
        poll_changes_func=None,
    ):
        """
        Parameters:
            cbl_store: CBLStore instance for loading jobs
            config: Global config with max_threads, etc.
            metrics: MetricsCollector instance
            logger: Logger instance
            poll_changes_func: The poll_changes coroutine to run for each job.
        """
        self.cbl_store = cbl_store
        self.config = config
        self.metrics = metrics
        self.logger = logger
        self.poll_changes_func = poll_changes_func

        # Global limits
        self.max_threads = config.get("max_threads", 10)

        # Job registry: job_id -> Pipeline
        self._pipelines: Dict[str, Pipeline] = {}
        self._lock = threading.RLock()

        # Graceful shutdown signal
        self._shutdown_event = threading.Event()
        self._running = False
        self._offline = False

        # Monitor thread for crash recovery
        self._monitor_thread: Optional[threading.Thread] = None

        # Backoff state for crashed jobs: job_id -> (attempt_count, last_restart_time)
        self._crash_backoff: Dict[str, tuple] = {}

    def start(self) -> None:
        """
        Load all enabled jobs and start pipelines.

        This blocks until shutdown signal is received.
        """
        try:
            log_event(self.logger, "info", "CONTROL", "PipelineManager starting")

            # Load all enabled jobs from CBL
            jobs = self._load_enabled_jobs()
            log_event(
                self.logger,
                "info",
                "CONTROL",
                "loaded %d enabled jobs" % len(jobs),
            )

            # Create Pipeline for each job
            for job_doc in jobs:
                # Job ID can be in doc_id (from Meta), _id (document field), or id
                job_id = (
                    job_doc.get("doc_id")  # N1QL Meta().id
                    or job_doc.get("_id")  # Document ID field
                    or job_doc.get("id")  # Job's internal id field
                    or "unknown"
                )
                # Ensure job_id has job:: prefix if not already present
                if job_id != "unknown" and not job_id.startswith("job::"):
                    job_id = f"job::{job_id}"

                try:
                    self._start_job_internal(job_id, job_doc)
                except Exception as e:
                    log_event(
                        self.logger,
                        "error",
                        "CONTROL",
                        "failed to start job: %s" % e,
                        job_id=job_id,
                    )

            self._running = True

            # Start monitor thread for crash recovery
            self._monitor_thread = threading.Thread(
                target=self._monitor_threads,
                name="pipeline-monitor",
                daemon=False,
            )
            self._monitor_thread.start()

            log_event(
                self.logger,
                "info",
                "CONTROL",
                "PipelineManager ready with %d jobs" % len(self._pipelines),
            )

            # Block until shutdown signal
            self._shutdown_event.wait()

        except Exception as e:
            log_event(
                self.logger,
                "error",
                "CONTROL",
                "PipelineManager error: %s" % e,
            )
            raise

        finally:
            self.stop()

    def stop(self, timeout_seconds: float = 30) -> None:
        """
        Gracefully shut down all pipelines.

        Wait for all jobs to drain and save checkpoints.
        """
        log_event(
            self.logger,
            "info",
            "CONTROL",
            "shutting down all pipelines",
        )

        self._running = False
        self._shutdown_event.set()  # unblock start() if still waiting

        with self._lock:
            # Stop all pipelines
            job_ids = list(self._pipelines.keys())

        for job_id in job_ids:
            try:
                self.stop_job(job_id, timeout_seconds=timeout_seconds)
            except Exception as e:
                log_event(
                    self.logger,
                    "error",
                    "CONTROL",
                    "error stopping job: %s" % e,
                    job_id=job_id,
                )

        # Stop monitor thread
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)

        log_event(
            self.logger,
            "info",
            "CONTROL",
            "PipelineManager stopped",
        )

    def start_job(self, job_id: str) -> bool:
        """
        Start a single job. Load from CBL if not already loaded.

        Returns:
            True if started, False if already running or error.
        """
        # Normalise: ensure job:: prefix for registry lookups
        if not job_id.startswith("job::"):
            job_id = f"job::{job_id}"

        with self._lock:
            if job_id in self._pipelines:
                if self._pipelines[job_id].is_running():
                    log_event(
                        self.logger,
                        "warn",
                        "CONTROL",
                        "job already running",
                        job_id=job_id,
                    )
                    return False
                else:
                    # Pipeline exists but not running; just start it
                    self._pipelines[job_id].start()
                    return True

        # Load job from CBL
        try:
            # Strip job:: prefix — load_job adds it internally
            raw_id = job_id.removeprefix("job::")
            job_doc = self.cbl_store.load_job(raw_id)
            if not job_doc:
                log_event(
                    self.logger,
                    "error",
                    "CONTROL",
                    "job not found",
                    job_id=job_id,
                )
                return False

            self._start_job_internal(job_id, job_doc)
            return True

        except Exception as e:
            log_event(
                self.logger,
                "error",
                "CONTROL",
                "error starting job: %s" % e,
                job_id=job_id,
            )
            return False

    def stop_job(self, job_id: str, timeout_seconds: float = 30) -> bool:
        """
        Stop a single job.

        Returns:
            True if stopped, False if not running or timeout.
        """
        if not job_id.startswith("job::"):
            job_id = f"job::{job_id}"

        with self._lock:
            if job_id not in self._pipelines:
                log_event(
                    self.logger,
                    "warn",
                    "CONTROL",
                    "job not in registry",
                    job_id=job_id,
                )
                return True

            pipeline = self._pipelines[job_id]

        # Stop outside the lock to avoid deadlock
        success = pipeline.stop(timeout_seconds)

        if success:
            with self._lock:
                del self._pipelines[job_id]
                self._crash_backoff.pop(job_id, None)

        return success

    def restart_job(self, job_id: str, timeout_seconds: float = 30) -> bool:
        """Restart a single job."""
        if not self.stop_job(job_id, timeout_seconds):
            log_event(
                self.logger,
                "warn",
                "CONTROL",
                "job did not stop within timeout",
                job_id=job_id,
            )
        return self.start_job(job_id)

    def restart_all(self, timeout_seconds: float = 30) -> None:
        """Restart all running jobs."""
        log_event(
            self.logger,
            "info",
            "CONTROL",
            "restarting all jobs",
        )

        with self._lock:
            job_ids = list(self._pipelines.keys())

        for job_id in job_ids:
            try:
                self.restart_job(job_id, timeout_seconds)
            except Exception as e:
                log_event(
                    self.logger,
                    "error",
                    "CONTROL",
                    "error restarting job: %s" % e,
                    job_id=job_id,
                )

    def get_job_state(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get the state of a single job."""
        if not job_id.startswith("job::"):
            job_id = f"job::{job_id}"
        with self._lock:
            if job_id not in self._pipelines:
                return None
            return self._pipelines[job_id].get_state()

    def list_job_states(self) -> List[Dict[str, Any]]:
        """Get the state of all jobs."""
        with self._lock:
            job_ids = list(self._pipelines.keys())

        states = []
        for job_id in job_ids:
            state = self.get_job_state(job_id)
            if state:
                states.append(state)

        return states

    def is_offline(self) -> bool:
        """Return True if the manager is in offline (paused) mode."""
        with self._lock:
            return self._offline

    def go_offline(self, timeout_seconds: float = 30) -> None:
        """Pause all jobs. Manager stays alive and can be brought back online."""
        log_event(
            self.logger,
            "info",
            "CONTROL",
            "going offline — stopping all jobs",
        )
        with self._lock:
            self._offline = True
            job_ids = list(self._pipelines.keys())

        for job_id in job_ids:
            try:
                self.stop_job(job_id, timeout_seconds=timeout_seconds)
            except Exception as e:
                log_event(
                    self.logger,
                    "error",
                    "CONTROL",
                    "error stopping job during offline: %s" % e,
                    job_id=job_id,
                )

        log_event(self.logger, "info", "CONTROL", "all jobs stopped — offline")

    def go_online(self) -> None:
        """Resume all enabled jobs after an offline pause."""
        log_event(
            self.logger,
            "info",
            "CONTROL",
            "going online — starting enabled jobs",
        )
        with self._lock:
            self._offline = False

        jobs = self._load_enabled_jobs()
        for job_doc in jobs:
            job_id = (
                job_doc.get("doc_id")
                or job_doc.get("_id")
                or job_doc.get("id")
                or "unknown"
            )
            if job_id != "unknown" and not job_id.startswith("job::"):
                job_id = f"job::{job_id}"
            try:
                self.start_job(job_id)
            except Exception as e:
                log_event(
                    self.logger,
                    "error",
                    "CONTROL",
                    "error starting job during online: %s" % e,
                    job_id=job_id,
                )

        log_event(
            self.logger,
            "info",
            "CONTROL",
            "online — %d jobs running" % len(self._pipelines),
        )

    def trigger_shutdown(self) -> None:
        """Trigger graceful shutdown (called by signal handler)."""
        log_event(
            self.logger,
            "info",
            "SHUTDOWN",
            "shutdown signal received",
        )
        self._shutdown_event.set()

    def _start_job_internal(self, job_id: str, job_doc: Dict[str, Any]) -> None:
        """Internal: create and start a pipeline for a job."""
        with self._lock:
            if job_id in self._pipelines and self._pipelines[job_id].is_running():
                log_event(
                    self.logger,
                    "warn",
                    "CONTROL",
                    "job already running",
                    job_id=job_id,
                )
                return

            # Check max_threads limit
            running_count = sum(1 for p in self._pipelines.values() if p.is_running())
            if running_count >= self.max_threads:
                log_event(
                    self.logger,
                    "warn",
                    "CONTROL",
                    "max threads limit (%d) reached; queuing job" % self.max_threads,
                    job_id=job_id,
                )
                # For now, just log. In the future, we could queue this.
                return

            # Create pipeline
            pipeline = Pipeline(
                job_id=job_id,
                job_doc=job_doc,
                cbl_store=self.cbl_store,
                metrics=self.metrics,
                logger=self.logger,
                middleware_threads=job_doc.get("system", {}).get(
                    "middleware_threads", 2
                ),
                poll_changes_func=self.poll_changes_func,
            )

            # Start the thread
            pipeline.start()
            self._pipelines[job_id] = pipeline

            log_event(
                self.logger,
                "info",
                "CONTROL",
                "job started",
                job_id=job_id,
            )

    def _monitor_threads(self) -> None:
        """
        Monitor job threads for crashes and auto-restart with exponential backoff.

        Runs in its own thread.
        """
        log_event(
            self.logger,
            "info",
            "CONTROL",
            "thread monitor started",
        )

        while self._running:
            try:
                time.sleep(5)  # Check every 5 seconds

                # Don't auto-restart jobs while offline
                if self.is_offline():
                    continue

                with self._lock:
                    job_ids = list(self._pipelines.keys())

                for job_id in job_ids:
                    try:
                        pipeline = self._pipelines.get(job_id)
                        if not pipeline:
                            continue

                        # Check if pipeline crashed
                        if not pipeline.is_running():
                            state = pipeline.get_state()
                            if state and state["status"] == "error":
                                if state.get("auth_failure"):
                                    if self._crash_backoff.get(job_id) != "auth":
                                        log_event(
                                            self.logger,
                                            "error",
                                            "CONTROL",
                                            "Job stopped due to authentication failure "
                                            "(401/403). Will not auto-restart. "
                                            "Fix credentials and restart manually.",
                                            job_id=job_id,
                                        )
                                        self._crash_backoff[job_id] = "auth"
                                    continue
                                self._handle_job_crash(job_id)
                        else:
                            # Pipeline is running — reset backoff after
                            # sustained uptime (60s) to prove output is
                            # reachable again.
                            if job_id in self._crash_backoff:
                                attempt, restart_time = self._crash_backoff[job_id]
                                if attempt > 0 and time.time() - restart_time >= 60:
                                    log_event(
                                        self.logger,
                                        "info",
                                        "RETRY",
                                        "output recovered after %d retries, "
                                        "resetting backoff" % attempt,
                                        job_id=job_id,
                                    )
                                    self._crash_backoff[job_id] = (0, 0)

                    except Exception as e:
                        log_event(
                            self.logger,
                            "error",
                            "CONTROL",
                            "error monitoring job: %s" % e,
                            job_id=job_id,
                        )

            except Exception as e:
                log_event(
                    self.logger,
                    "error",
                    "CONTROL",
                    "monitor thread crashed: %s" % e,
                )
                break

        log_event(
            self.logger,
            "info",
            "CONTROL",
            "thread monitor stopped",
        )

    def _handle_job_crash(self, job_id: str) -> None:
        """Handle a crashed job with exponential backoff restart.

        Backoff grows: 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, 300s (cap).
        Retries forever until the output comes back.
        Backoff resets only after the pipeline has been running successfully
        for at least 60 seconds (checked by _monitor_threads).
        """
        attempt, last_time = self._crash_backoff.get(job_id, (0, 0))
        attempt += 1

        # Exponential backoff: 2s, 4s, 8s, ... up to 300s (5 minutes)
        backoff_seconds = min(2**attempt, 300)
        now = time.time()

        if now - last_time < backoff_seconds:
            # Still in backoff period
            remaining = backoff_seconds - (now - last_time)
            log_event(
                self.logger,
                "warn",
                "RETRY",
                "output down – waiting to retry (attempt %d, "
                "backoff %ds, %.0fs remaining)" % (attempt, backoff_seconds, remaining),
                job_id=job_id,
            )
            self._crash_backoff[job_id] = (attempt, last_time)
            return

        log_event(
            self.logger,
            "warn",
            "RETRY",
            "restarting crashed job (attempt %d, "
            "next backoff %ds)" % (attempt, backoff_seconds),
            job_id=job_id,
        )

        # Try to restart — do NOT reset backoff here.
        # Backoff is only reset by _monitor_threads after sustained uptime.
        if self.start_job(job_id):
            self._crash_backoff[job_id] = (attempt, now)
        else:
            self._crash_backoff[job_id] = (attempt, now)

    def _load_enabled_jobs(self) -> List[Dict[str, Any]]:
        """Load all enabled jobs from CBL (full documents, not summaries)."""
        try:
            job_summaries = self.cbl_store.list_jobs()
            enabled = []
            for summary in job_summaries:
                # list_jobs returns summary rows; load the full document
                raw_id = summary.get("id") or summary.get("doc_id", "").removeprefix(
                    "job::"
                )
                if not raw_id:
                    continue
                full_doc = self.cbl_store.load_job(raw_id)
                if full_doc and full_doc.get("enabled", True):
                    # Preserve the doc_id for _start_job_internal
                    full_doc["doc_id"] = summary.get("doc_id")
                    enabled.append(full_doc)
            return enabled
        except Exception as e:
            log_event(
                self.logger,
                "error",
                "CONTROL",
                "error loading jobs: %s" % e,
            )
            return []
