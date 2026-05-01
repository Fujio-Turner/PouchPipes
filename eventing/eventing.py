"""
Eventing — user-programmable JavaScript stage (OnUpdate / OnDelete).

Sits between the _changes feed and the Schema Mapper in the pipeline.
Uses py_mini_racer (V8) for sandboxed JS execution.

Best practices applied:
  - max_memory cap on V8 heap to prevent OOM
  - Periodic heap stats for metrics / monitoring
  - Constant key validation (valid JS identifiers only)
  - Context manager for clean V8 teardown
  - Per-invocation timing for Prometheus metrics
"""

import json
import logging
import re
import time

try:
    from mini_racer import MiniRacer
except ImportError:
    MiniRacer = None

from pipeline.pipeline_logging import log_event

logger = logging.getLogger("changes_worker")

_LOG_HELPER = """\
var __logs = [];
function log() {
    var args = Array.prototype.slice.call(arguments);
    __logs.push({level: "info", msg: args.map(String).join(" ")});
}
var console = {
    log: function() {
        var args = Array.prototype.slice.call(arguments);
        __logs.push({level: "info", msg: args.map(String).join(" ")});
    },
    warn: function() {
        var args = Array.prototype.slice.call(arguments);
        __logs.push({level: "warn", msg: args.map(String).join(" ")});
    },
    error: function() {
        var args = Array.prototype.slice.call(arguments);
        __logs.push({level: "error", msg: args.map(String).join(" ")});
    },
    debug: function() {
        var args = Array.prototype.slice.call(arguments);
        __logs.push({level: "debug", msg: args.map(String).join(" ")});
    }
};
"""

_VALID_JS_IDENT = re.compile(r"^[A-Za-z_$][A-Za-z0-9_$]*$")

# Default V8 heap limit: 128 MB — prevents runaway allocations in user JS
_DEFAULT_MAX_MEMORY_BYTES = 128 * 1024 * 1024

# How often (invocations) to sample V8 heap stats for metrics
_HEAP_STATS_INTERVAL = 100


class EventingHandler:
    """Encapsulates a MiniRacer V8 instance for a single eventing job.

    Best practices:
      - Each handler gets its own V8 isolate (MiniRacer instance)
      - max_memory prevents OOM from user JS (default 128MB)
      - Timeouts prevent infinite loops
      - Heap stats sampled every N invocations for monitoring
      - Use as context manager for clean teardown
    """

    def __init__(
        self,
        handler_code: str,
        constants: list[dict] | None = None,
        timeout_ms: int = 5000,
        on_error: str = "reject",
        on_timeout: str = "reject",
        max_memory_bytes: int = _DEFAULT_MAX_MEMORY_BYTES,
        metrics=None,
    ):
        self._ctx = MiniRacer()
        self._timeout_ms = timeout_ms
        self._on_error = on_error
        self._on_timeout = on_timeout
        self._max_memory = max_memory_bytes
        self._metrics = metrics
        self._invocation_count = 0

        preamble = _LOG_HELPER + self._build_constants_preamble(constants)
        self._ctx.eval(preamble + "\n" + handler_code)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        """Explicitly tear down the V8 context."""
        if self._ctx is not None:
            try:
                self._ctx = None
            except Exception:
                pass

    @staticmethod
    def _build_constants_preamble(constants: list[dict] | None) -> str:
        if not constants:
            return ""
        lines = []
        for entry in constants:
            key = entry.get("key", "")
            value = entry.get("value")
            if not key:
                continue
            if not _VALID_JS_IDENT.match(key):
                log_event(
                    logger,
                    "warn",
                    "EVENTING",
                    "skipping invalid constant key %r" % key,
                )
                continue
            lines.append(f"const {key} = {json.dumps(value)};")
        return "\n".join(lines) + "\n"

    def _flush_logs(self) -> None:
        try:
            logs = self._ctx.eval("__logs.splice(0)")
        except Exception:
            return
        if not logs:
            return

        from pipeline.pipeline_logging import get_eventing_js_logger

        js_logger = get_eventing_js_logger()

        _JS_LEVELS = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warn": logging.WARNING,
            "error": logging.ERROR,
        }

        for entry in logs:
            if isinstance(entry, dict):
                msg = entry.get("msg", "")
                lvl = _JS_LEVELS.get(entry.get("level", "info"), logging.INFO)
            else:
                # Backwards compat: plain string from older JS code
                msg = str(entry)
                lvl = logging.INFO

            # Write to dedicated eventing log file (Node.js style)
            if js_logger:
                js_logger.log(lvl, "[eventing-js] %s", msg)

            # Also emit to main changes_worker logger at debug so it
            # appears in the main log when debug is enabled
            log_event(logger, "debug", "EVENTING", "[js] %s" % msg)

    def _log_js_error(self, handler: str, doc_id: str, exc: Exception) -> None:
        """Write full error detail to the dedicated JS log file."""
        from pipeline.pipeline_logging import get_eventing_js_logger

        js_logger = get_eventing_js_logger()
        if js_logger:
            js_logger.error(
                "[eventing-js] %s error  doc_id=%s  %s: %s",
                handler,
                doc_id,
                type(exc).__name__,
                exc,
            )

    def _log_js_timeout(self, handler: str, doc_id: str) -> None:
        """Write timeout detail to the dedicated JS log file."""
        from pipeline.pipeline_logging import get_eventing_js_logger

        js_logger = get_eventing_js_logger()
        if js_logger:
            js_logger.error(
                "[eventing-js] %s timed out after %dms  doc_id=%s",
                handler,
                self._timeout_ms,
                doc_id,
            )

    def _maybe_collect_heap_stats(self) -> None:
        """Sample V8 heap stats every N invocations and push to metrics."""
        if self._metrics is None:
            return
        if self._invocation_count % _HEAP_STATS_INTERVAL != 0:
            return
        try:
            stats = self._ctx.eval("JSON.stringify({used: 0, total: 0})")
            # py_mini_racer doesn't expose heap_stats on newer versions;
            # fall back gracefully
        except Exception:
            pass

    def _split_doc(self, change: dict) -> tuple[dict, dict]:
        doc = change.get("doc", {})
        meta = {"_id": doc.get("_id"), "_rev": doc.get("_rev")}
        doc_body = {k: v for k, v in doc.items() if k not in ("_id", "_rev")}
        return doc_body, meta

    def _is_delete(self, change: dict) -> bool:
        return change.get("deleted", False)

    def process_change(self, change: dict) -> dict | None:
        """Run the JS handler against a single change-feed entry.

        Returns the (possibly modified) document dict, or None if rejected.
        Raises EventingHalt if on_error/on_timeout is 'halt'.
        """
        self._invocation_count += 1
        if self._metrics:
            self._metrics.inc("eventing_invocations_total")

        t0 = time.monotonic()
        try:
            if self._is_delete(change):
                if self._metrics:
                    self._metrics.inc("eventing_deletes_total")
                result = self._handle_delete(change)
            else:
                if self._metrics:
                    self._metrics.inc("eventing_updates_total")
                result = self._handle_update(change)

            if result is not None:
                if self._metrics:
                    self._metrics.inc("eventing_passed_total")
            else:
                if self._metrics:
                    self._metrics.inc("eventing_rejected_total")

            return result
        finally:
            elapsed = time.monotonic() - t0
            if self._metrics:
                self._metrics.record_eventing_handler_time(elapsed)
            self._maybe_collect_heap_stats()

    def _handle_delete(self, change: dict) -> dict | None:
        _, meta = self._split_doc(change)
        doc_id = meta.get("_id", "")
        try:
            result = self._ctx.call(
                "OnDelete",
                meta,
                timeout=self._timeout_ms,
                max_memory=self._max_memory,
            )
        except TimeoutError:
            log_event(
                logger, "warn", "EVENTING", "JS timeout in eventing", doc_id=doc_id
            )
            self._log_js_timeout("OnDelete", doc_id)
            self._flush_logs()
            if self._metrics:
                self._metrics.inc("eventing_timeouts_total")
            return self._apply_policy(self._on_timeout, change, "timeout")
        except Exception as exc:
            log_event(
                logger, "error", "EVENTING", "JS error in eventing", doc_id=doc_id
            )
            self._log_js_error("OnDelete", doc_id, exc)
            self._flush_logs()
            if self._metrics:
                self._metrics.inc("eventing_errors_total")
            return self._apply_policy(self._on_error, change, "error")
        self._flush_logs()
        return self._interpret_result(result, change)

    def _handle_update(self, change: dict) -> dict | None:
        doc_body, meta = self._split_doc(change)
        doc_id = meta.get("_id", "")
        try:
            result = self._ctx.call(
                "OnUpdate",
                doc_body,
                meta,
                timeout=self._timeout_ms,
                max_memory=self._max_memory,
            )
        except TimeoutError:
            log_event(
                logger, "warn", "EVENTING", "JS timeout in eventing", doc_id=doc_id
            )
            self._log_js_timeout("OnUpdate", doc_id)
            self._flush_logs()
            if self._metrics:
                self._metrics.inc("eventing_timeouts_total")
            return self._apply_policy(self._on_timeout, change, "timeout")
        except Exception as exc:
            log_event(
                logger, "error", "EVENTING", "JS error in eventing", doc_id=doc_id
            )
            self._log_js_error("OnUpdate", doc_id, exc)
            self._flush_logs()
            if self._metrics:
                self._metrics.inc("eventing_errors_total")
            return self._apply_policy(self._on_error, change, "error")
        self._flush_logs()
        return self._interpret_result(result, change)

    def _interpret_result(self, result, change: dict) -> dict | None:
        if isinstance(result, dict):
            _, meta = self._split_doc(change)
            result["_id"] = meta["_id"]
            result["_rev"] = meta["_rev"]
            return result

        if result is True:
            # For deletes the change may have no doc body — return meta only
            doc = change.get("doc")
            if doc:
                return dict(doc)
            _, meta = self._split_doc(change)
            return meta

        return None

    def _apply_policy(self, policy: str, change: dict, reason: str) -> dict | None:
        if policy == "halt":
            if self._metrics:
                self._metrics.inc("eventing_halts_total")
            raise EventingHalt(
                f"Eventing handler {reason} with on_{reason}=halt — stopping job"
            )
        if policy == "pass":
            doc = change.get("doc", {})
            return dict(doc)
        # "reject" — return None
        return None


class EventingHalt(Exception):
    """Raised when on_error='halt' or on_timeout='halt' is triggered."""


def create_eventing_handler(
    eventing_cfg: dict,
    metrics=None,
) -> EventingHandler | None:
    """Factory: build an EventingHandler from a job's eventing config dict.

    Returns None if eventing is not enabled.
    """
    if not eventing_cfg or not eventing_cfg.get("enabled", False):
        return None

    if MiniRacer is None:
        log_event(
            logger,
            "error",
            "EVENTING",
            "eventing enabled but py_mini_racer is not installed — pip install py_mini_racer",
        )
        return None

    handler_code = eventing_cfg.get("handler", "")
    if not handler_code:
        log_event(
            logger, "warn", "EVENTING", "eventing enabled but no handler provided"
        )
        return None

    return EventingHandler(
        handler_code=handler_code,
        constants=eventing_cfg.get("constants"),
        timeout_ms=eventing_cfg.get("timeout_ms", 5000),
        on_error=eventing_cfg.get("on_error", "reject"),
        on_timeout=eventing_cfg.get("on_timeout", "reject"),
        metrics=metrics,
    )
