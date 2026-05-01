"""
Diagnostics collection module for change_stream_db.

Collects logs, system info, profiling data, and metrics into a portable .zip file,
similar to Sync Gateway's sgcollect_info tool.
"""

import asyncio
import gc
import json
import logging
import os
import platform
import psutil
import subprocess
import sys
import tempfile
import time
import traceback
import tracemalloc
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile

from pipeline.pipeline_logging import log_event

logger = logging.getLogger("changes_worker")


class DiagnosticsCollector:
    """Collects diagnostics and packages them into a zip file."""

    def __init__(self, cfg: dict, metrics=None, redactor=None):
        self.cfg = cfg
        self.metrics = metrics
        self.redactor = redactor
        self._hostname = platform.node()
        self._timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self._collect_dir_name = f"csdb_collect_{self._hostname}_{self._timestamp}"
        self._temp_dir = None
        self._warnings = []

    async def collect(self, include_profiling: bool = True) -> str:
        """Run all collectors, return path to generated .zip file.

        The returned zip path is a standalone temp file that the caller
        is responsible for deleting after use.
        """
        try:
            # Create temp directory for staging collected files
            self._temp_dir = tempfile.mkdtemp(prefix="csdb_collect_")
            collect_root = os.path.join(self._temp_dir, self._collect_dir_name)
            os.makedirs(collect_root, exist_ok=True)

            collect_start = time.monotonic()

            # Run collectors
            await self._collect_project_logs(collect_root)
            await self._collect_cbl_logs(collect_root)
            await self._collect_system_info(collect_root)
            if include_profiling:
                await self._collect_profiling(collect_root)
            await self._collect_config(collect_root)
            await self._collect_metrics(collect_root)
            await self._collect_status(collect_root)

            collect_elapsed = time.monotonic() - collect_start

            # Write metadata
            self._write_collect_info(collect_root, collect_elapsed)

            # Create zip *outside* the staging dir so cleanup doesn't delete it
            zip_path = await self._create_zip(collect_root)
            log_event(
                logger,
                "info",
                "CONTROL",
                "diagnostics collection complete: %s (%.1fs)"
                % (os.path.basename(zip_path), collect_elapsed),
            )
            return zip_path

        except Exception as e:
            log_event(
                logger,
                "error",
                "CONTROL",
                "error during diagnostics collection",
                error_detail="%s: %s" % (type(e).__name__, e),
            )
            raise
        finally:
            # Cleanup staging directory (zip lives outside it)
            if self._temp_dir and os.path.exists(self._temp_dir):
                import shutil

                shutil.rmtree(self._temp_dir, ignore_errors=True)

    async def _collect_project_logs(self, collect_root: str) -> None:
        """Copy project rotating logs (changes_worker.log*)."""
        try:
            import shutil

            log_path = Path(
                self.cfg.get("logging", {})
                .get("file", {})
                .get("path", "logs/changes_worker.log")
            )
            log_dir = (
                log_path.parent
                if str(log_path.parent) not in ("", ".")
                else Path("logs")
            )
            log_prefix = log_path.name or "changes_worker.log"

            project_logs_dir = os.path.join(collect_root, "project_logs")
            os.makedirs(project_logs_dir, exist_ok=True)

            if not log_dir.exists():
                log_event(
                    logger, "debug", "CONTROL", "log directory does not exist, skipping"
                )
                return

            log_files = [p for p in log_dir.glob(f"{log_prefix}*") if p.is_file()]
            log_files.sort(
                key=lambda p: p.stat().st_mtime, reverse=True
            )  # newest first

            max_size_bytes = (
                self.cfg.get("collect", {}).get("max_log_size_mb", 200) * 1024 * 1024
            )

            collected_size = 0
            collected_count = 0
            for log_file in log_files:
                file_size = log_file.stat().st_size
                if collected_size + file_size > max_size_bytes:
                    self._warnings.append(
                        f"Project logs truncated (exceeded {max_size_bytes // (1024 * 1024)}MB cap)"
                    )
                    break
                dest = os.path.join(project_logs_dir, log_file.name)
                shutil.copy2(log_file, dest)
                collected_size += file_size
                collected_count += 1

            log_event(
                logger,
                "debug",
                "CONTROL",
                "collected %d project log file(s)" % collected_count,
            )
        except Exception as e:
            log_event(
                logger,
                "warn",
                "CONTROL",
                "error collecting project logs",
                error_detail="%s: %s" % (type(e).__name__, e),
            )
            self._write_error_file(collect_root, "project_logs", e)

    async def _collect_cbl_logs(self, collect_root: str) -> None:
        """Copy Couchbase Lite file logs from db_dir (capped to max_log_size_mb)."""
        try:
            import shutil

            from storage.cbl_store import CBL_DB_DIR

            cbl_logs = [p for p in Path(CBL_DB_DIR).glob("*.cbllog*") if p.is_file()]
            if not cbl_logs:
                log_event(logger, "debug", "CONTROL", "no CBL log files found")
                return

            cbl_logs.sort(key=lambda p: p.stat().st_mtime, reverse=True)

            cbl_logs_dir = os.path.join(collect_root, "cbl_logs")
            os.makedirs(cbl_logs_dir, exist_ok=True)

            max_size_bytes = (
                self.cfg.get("collect", {}).get("max_log_size_mb", 200) * 1024 * 1024
            )
            collected_size = 0
            collected_count = 0
            for log_file in cbl_logs:
                file_size = log_file.stat().st_size
                if collected_size + file_size > max_size_bytes:
                    self._warnings.append(
                        f"CBL logs truncated (exceeded {max_size_bytes // (1024 * 1024)}MB cap)"
                    )
                    break
                dest = os.path.join(cbl_logs_dir, log_file.name)
                shutil.copy2(log_file, dest)
                collected_size += file_size
                collected_count += 1

            log_event(
                logger,
                "debug",
                "CONTROL",
                "collected %d CBL log file(s)" % collected_count,
            )
        except ImportError:
            log_event(logger, "debug", "CONTROL", "CBL not enabled, skipping CBL logs")
        except Exception as e:
            log_event(
                logger,
                "warn",
                "CONTROL",
                "error collecting CBL logs",
                error_detail="%s: %s" % (type(e).__name__, e),
            )
            self._write_error_file(collect_root, "cbl_logs", e)

    async def _collect_system_info(self, collect_root: str) -> None:
        """Collect OS-level diagnostics (uname, ps, df, netstat, etc.)."""
        try:
            import shutil as _shutil

            system_dir = os.path.join(collect_root, "system")
            os.makedirs(system_dir, exist_ok=True)

            system_commands = self._get_system_commands()
            timeout = self.cfg.get("collect", {}).get(
                "system_command_timeout_seconds", 30
            )

            for name, cmd in system_commands.items():
                # Skip commands that aren't installed in this container/OS
                if not _shutil.which(cmd[0]):
                    log_event(
                        logger,
                        "debug",
                        "CONTROL",
                        "skipping system command %s (%s not found)" % (name, cmd[0]),
                    )
                    continue
                try:
                    result = self._run_command_sync(cmd, timeout)
                    output_file = os.path.join(system_dir, f"{name}.txt")
                    with open(output_file, "w") as f:
                        f.write(result)
                except Exception as e:
                    log_event(
                        logger,
                        "debug",
                        "CONTROL",
                        "system command failed: %s" % name,
                        error_detail=str(e),
                    )
                    self._write_error_file(system_dir, name, e)

            # Write safe env vars (curated allowlist, no secrets)
            self._collect_safe_env(system_dir)

            log_event(
                logger,
                "debug",
                "CONTROL",
                "collected system info (%d commands)" % len(system_commands),
            )
        except Exception as e:
            log_event(
                logger,
                "warn",
                "CONTROL",
                "error collecting system info",
                error_detail="%s: %s" % (type(e).__name__, e),
            )
            self._write_error_file(collect_root, "system", e)

    async def _collect_profiling(self, collect_root: str) -> None:
        """Collect memory profile, thread stacks, asyncio tasks, and process stats."""
        try:
            profiling_dir = os.path.join(collect_root, "profiling")
            os.makedirs(profiling_dir, exist_ok=True)

            # Asyncio task dump (replaces useless cProfile-of-sleeping-thread)
            self._collect_asyncio_tasks(profiling_dir)

            # Memory profile (only if tracemalloc was enabled at startup)
            self._profile_memory(profiling_dir)

            # Thread stacks
            self._collect_thread_stacks(profiling_dir)

            # psutil process stats
            self._collect_process_stats(profiling_dir)

            # GC stats
            self._collect_gc_stats(profiling_dir)

            log_event(logger, "debug", "CONTROL", "collected profiling data")
        except Exception as e:
            log_event(
                logger,
                "warn",
                "CONTROL",
                "error collecting profiling data",
                error_detail="%s: %s" % (type(e).__name__, e),
            )
            self._write_error_file(collect_root, "profiling", e)

    async def _collect_config(self, collect_root: str) -> None:
        """Dump redacted config and version info."""
        try:
            config_dir = os.path.join(collect_root, "config")
            os.makedirs(config_dir, exist_ok=True)

            # Redact config if redactor is available
            cfg_to_write = self.cfg
            if self.redactor:
                cfg_to_write = self.redactor.redact_dict(self.cfg)

            config_file = os.path.join(config_dir, "config_redacted.json")
            with open(config_file, "w") as f:
                json.dump(cfg_to_write, f, indent=2, default=str)

            # Version info
            version_info = {
                "version": self._get_version(),
                "python_version": platform.python_version(),
                "platform": platform.platform(),
                "hostname": self._hostname,
            }
            version_file = os.path.join(config_dir, "version.json")
            with open(version_file, "w") as f:
                json.dump(version_info, f, indent=2)

            log_event(logger, "debug", "CONTROL", "collected config info")
        except Exception as e:
            log_event(
                logger,
                "warn",
                "CONTROL",
                "error collecting config",
                error_detail="%s: %s" % (type(e).__name__, e),
            )
            self._write_error_file(collect_root, "config", e)

    async def _collect_metrics(self, collect_root: str) -> None:
        """Capture Prometheus metrics snapshot."""
        try:
            if not self.metrics:
                log_event(
                    logger,
                    "debug",
                    "CONTROL",
                    "no metrics collector available, skipping metrics",
                )
                return

            metrics_file = os.path.join(collect_root, "metrics_snapshot.txt")
            metrics_output = self.metrics.render()
            with open(metrics_file, "w") as f:
                f.write(metrics_output)

            log_event(logger, "debug", "CONTROL", "collected metrics snapshot")
        except Exception as e:
            log_event(
                logger,
                "warn",
                "CONTROL",
                "error collecting metrics",
                error_detail="%s: %s" % (type(e).__name__, e),
            )
            self._write_error_file(collect_root, "metrics", e)

    async def _collect_status(self, collect_root: str) -> None:
        """Capture status endpoint snapshot."""
        try:
            # For now, just write empty status (can be enhanced later)
            status_file = os.path.join(collect_root, "status.json")
            status = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "ok",
            }
            with open(status_file, "w") as f:
                json.dump(status, f, indent=2)

            log_event(logger, "debug", "CONTROL", "collected status snapshot")
        except Exception as e:
            log_event(
                logger,
                "warn",
                "CONTROL",
                "error collecting status",
                error_detail="%s: %s" % (type(e).__name__, e),
            )
            self._write_error_file(collect_root, "status", e)

    # ─── Helper methods ───────────────────────────────────────────────────

    def _get_system_commands(self) -> dict[str, str]:
        """Return platform-appropriate system commands."""
        commands = {
            "uname": ["uname", "-a"],
            "ps_aux": ["ps", "aux"],
            "df": ["df", "-h"],
            "ulimit": ["sh", "-c", "ulimit -a"],
        }

        if platform.system() == "Linux":
            commands.update(
                {
                    "top": ["top", "-bn1"],
                    "free": ["free", "-m"],
                    "netstat": ["ss", "-an"],
                }
            )
        elif platform.system() == "Darwin":  # macOS
            commands.update(
                {
                    "top": ["top", "-l1"],
                    "vm_stat": ["vm_stat"],
                    "netstat": ["netstat", "-an"],
                }
            )

        commands.update(
            {
                "ifconfig": (
                    ["ifconfig"] if platform.system() == "Darwin" else ["ip", "addr"]
                ),
            }
        )

        return commands

    def _collect_safe_env(self, system_dir: str) -> None:
        """Write a curated subset of environment variables (no secrets)."""
        allowed = {
            "PATH",
            "LANG",
            "LC_ALL",
            "TZ",
            "HOSTNAME",
            "PYTHONPATH",
            "HOME",
            "USER",
            "PWD",
            "SHELL",
            "TERM",
            "VIRTUAL_ENV",
            "DOCKER_HOST",
            "CONTAINER",
            "container",
        }
        env_data = {k: os.environ[k] for k in sorted(os.environ) if k in allowed}
        output_file = os.path.join(system_dir, "env.txt")
        with open(output_file, "w") as f:
            for k, v in env_data.items():
                f.write(f"{k}={v}\n")

    def _run_command_sync(self, cmd: list[str], timeout: int = 30) -> str:
        """Synchronously run a command with timeout."""
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return result.stdout + (result.stderr if result.returncode != 0 else "")
        except subprocess.TimeoutExpired:
            raise Exception(f"Command timeout after {timeout}s: {' '.join(cmd)}")
        except Exception as e:
            raise Exception(f"Command failed: {' '.join(cmd)}: {e}")

    def _collect_asyncio_tasks(self, profiling_dir: str) -> None:
        """Dump all active asyncio tasks (much more useful than cProfile in async apps)."""
        output_file = os.path.join(profiling_dir, "asyncio_tasks.txt")
        try:
            all_tasks = asyncio.all_tasks()
            with open(output_file, "w") as f:
                f.write(f"[ {len(all_tasks)} active asyncio tasks ]\n\n")
                for task in sorted(all_tasks, key=lambda t: t.get_name()):
                    f.write(f"=== Task: {task.get_name()} ===\n")
                    f.write(f"  state: {task._state}\n")
                    coro = task.get_coro()
                    if coro:
                        f.write(f"  coro:  {coro}\n")
                    stack = task.get_stack(limit=20)
                    if stack:
                        f.write("  stack:\n")
                        for frame in stack:
                            f.write(
                                f"    {frame.f_code.co_filename}:{frame.f_lineno} in {frame.f_code.co_name}\n"
                            )
                    f.write("\n")
        except Exception as e:
            with open(output_file, "w") as f:
                f.write(f"Error collecting asyncio tasks: {e}\n")

    def _profile_memory(self, profiling_dir: str) -> None:
        """Take tracemalloc snapshot only if tracing is already enabled."""
        output_file = os.path.join(profiling_dir, "tracemalloc_top50.txt")
        if not tracemalloc.is_tracing():
            self._warnings.append(
                "Skipped tracemalloc snapshot — tracing was not enabled at startup"
            )
            with open(output_file, "w") as f:
                f.write(
                    "tracemalloc was not enabled. "
                    "Start with PYTHONTRACEMALLOC=1 or tracemalloc.start() "
                    "to get memory allocation snapshots.\n"
                )
            return

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")

        with open(output_file, "w") as f:
            f.write("[ Top 50 Memory Allocations ]\n\n")
            for stat in top_stats[:50]:
                f.write(f"{stat}\n")

    def _collect_thread_stacks(self, profiling_dir: str) -> None:
        """Collect stack traces from all threads."""
        output_file = os.path.join(profiling_dir, "thread_stacks.txt")
        with open(output_file, "w") as f:
            for thread_id, frame in sys._current_frames().items():
                f.write(f"\n=== Thread {thread_id} ===\n")
                traceback.print_stack(frame, file=f)

    def _collect_process_stats(self, profiling_dir: str) -> None:
        """Collect psutil process stats (resilient to missing/restricted fields)."""

        def _safe(fn, default=None):
            try:
                return fn()
            except Exception:
                return default

        proc = psutil.Process()
        stats = {
            "pid": proc.pid,
            "name": _safe(proc.name, "unknown"),
            "status": _safe(proc.status, "unknown"),
            "memory_info": _safe(
                lambda: {
                    "rss": proc.memory_info().rss,
                    "vms": proc.memory_info().vms,
                },
                {},
            ),
            "memory_percent": _safe(proc.memory_percent),
            "cpu_times": _safe(
                lambda: {
                    "user": proc.cpu_times().user,
                    "system": proc.cpu_times().system,
                },
                {},
            ),
            "cpu_num": _safe(proc.cpu_num),
            "num_threads": _safe(proc.num_threads),
            "num_fds": _safe(proc.num_fds),
            "open_files": _safe(lambda: [str(f) for f in proc.open_files()[:10]], []),
        }

        output_file = os.path.join(profiling_dir, "psutil_process.json")
        with open(output_file, "w") as f:
            json.dump(stats, f, indent=2, default=str)

    def _collect_gc_stats(self, profiling_dir: str) -> None:
        """Collect garbage collector stats."""
        stats = {
            "count": gc.get_count(),
            "stats": gc.get_stats(),
        }

        output_file = os.path.join(profiling_dir, "gc_stats.json")
        with open(output_file, "w") as f:
            json.dump(stats, f, indent=2, default=str)

    def _write_collect_info(self, collect_root: str, elapsed: float) -> None:
        """Write metadata about the collection."""
        info = {
            "timestamp": self._timestamp,
            "hostname": self._hostname,
            "collection_duration_seconds": elapsed,
            "warnings": self._warnings,
        }
        info_file = os.path.join(collect_root, "collect_info.json")
        with open(info_file, "w") as f:
            json.dump(info, f, indent=2)

    def _write_error_file(self, dir_path: str, category: str, error: Exception) -> None:
        """Write error details to a file."""
        error_file = os.path.join(dir_path, f"{category}_error.txt")
        with open(error_file, "w") as f:
            f.write(f"Error collecting {category}:\n\n")
            f.write(f"{type(error).__name__}: {error}\n\n")
            f.write(traceback.format_exc())

    async def _create_zip(self, collect_root: str) -> str:
        """Create a zip file from the collected data."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._create_zip_sync, collect_root)

    def _create_zip_sync(self, collect_root: str) -> str:
        """Synchronously create the zip file outside the staging directory."""
        from zipfile import ZIP_DEFLATED

        tmp_dir = self.cfg.get("collect", {}).get("tmp_dir", tempfile.gettempdir())
        fd, zip_path = tempfile.mkstemp(
            prefix=f"{self._collect_dir_name}_",
            suffix=".zip",
            dir=tmp_dir,
        )
        os.close(fd)

        parent_dir = os.path.dirname(collect_root)
        with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(collect_root):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, parent_dir)
                    zf.write(file_path, arcname)

        return zip_path

    def _get_version(self) -> str:
        """Get the application version."""
        try:
            import main

            return getattr(main, "__version__", "unknown")
        except Exception:
            return "unknown"
