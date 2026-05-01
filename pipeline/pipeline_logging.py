"""
Production logging module for changes_worker.

Inspired by Couchbase Sync Gateway's logging configuration, this module
provides:
  - Multiple log levels including TRACE
  - Per-handler log_key filtering (CHANGES, PROCESSING, MAPPING, OUTPUT, etc.)
  - Per-key level overrides (e.g. HTTP→warn, MAPPING→debug)
  - File rotation with max_size, max_age, and rotated_logs_size_limit
  - Redaction of sensitive data (none / partial / full)
  - Operation tagging (INSERT, UPDATE, DELETE, SELECT)
"""

import atexit
import contextvars
import gzip
import glob as _glob
import logging
import os
import queue
import random
import re
import shutil
import string
import time
import uuid
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler

# ---------------------------------------------------------------------------
# Custom TRACE level (below DEBUG)
# ---------------------------------------------------------------------------
TRACE = 5
logging.addLevelName(TRACE, "TRACE")


def _trace(self, message, *args, **kwargs):
    if self.isEnabledFor(TRACE):
        self._log(TRACE, message, args, **kwargs)


logging.Logger.trace = _trace  # type: ignore[attr-defined]

# ---------------------------------------------------------------------------
# Batch tracing ID — 6-char alphanumeric tag for correlating all log lines
# produced while processing a single _changes batch.
# ---------------------------------------------------------------------------
_BATCH_ID_CHARS = string.ascii_lowercase + string.digits  # a-z 0-9
_BATCH_ID_LEN = 6
_JOB_TAG_LEN = 5  # last 5 chars of the full job_id
_SESSION_TAG_LEN = 6  # last 6 chars of the session UUID

# ContextVars so each async task / thread gets its own IDs automatically.
_current_batch_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "batch_id", default=None
)
_current_job_tag: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "job_tag", default=None
)
_current_session_tag: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "session_tag", default=None
)
# Full session UUID — stored separately so startup can log the complete value.
_current_session_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "session_id", default=None
)


def generate_batch_id() -> str:
    """Return a random 6-char lowercase alphanumeric string (e.g. 'a8b0tz')."""
    return "".join(random.choices(_BATCH_ID_CHARS, k=_BATCH_ID_LEN))


def set_batch_id(batch_id: str | None) -> contextvars.Token:
    """Set the batch tracing ID for the current context. Returns a reset token."""
    return _current_batch_id.set(batch_id)


def get_batch_id() -> str | None:
    """Return the current batch tracing ID, or None if not set."""
    return _current_batch_id.get()


def set_job_tag(job_id: str | None) -> contextvars.Token:
    """Set the job tag for the current context.

    Stores the last 5 characters of *job_id* (rendered as ``job=..xxxxx``
    in the log prefix).  Pass ``None`` to clear.
    """
    tag = job_id[-_JOB_TAG_LEN:] if job_id else None
    return _current_job_tag.set(tag)


def get_job_tag() -> str | None:
    """Return the current job tag (last 5 chars), or None if not set."""
    return _current_job_tag.get()


def generate_session_id() -> str:
    """Generate a new UUID4 session ID for a job run."""
    return str(uuid.uuid4())


def set_session_id(session_id: str | None) -> None:
    """Set the session ID for the current context.

    Stores the full UUID in ``_current_session_id`` (for startup logging)
    and the last 6 characters in ``_current_session_tag`` (for the prefix).
    """
    _current_session_id.set(session_id)
    tag = session_id[-_SESSION_TAG_LEN:] if session_id else None
    _current_session_tag.set(tag)


def get_session_id() -> str | None:
    """Return the full session UUID, or None if not set."""
    return _current_session_id.get()


def get_session_tag() -> str | None:
    """Return the session tag (last 6 chars), or None if not set."""
    return _current_session_tag.get()


# Background queue listener — started by configure_logging(), stopped at exit.
_queue_listener: QueueListener | None = None

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LOG_KEYS = frozenset(
    {
        "CHANGES",  # _changes feed input
        "PROCESSING",  # filtering, routing, batch summaries
        "MAPPING",  # schema mapping
        "EVENTING",  # JavaScript eventing handlers (OnUpdate/OnDelete)
        "ATTACHMENT",  # attachment detect/filter/fetch/upload/post-process
        "OUTPUT",  # stdout / HTTP / DB / cloud output
        "HTTP",  # HTTP requests / responses
        "CHECKPOINT",  # checkpoint load / save
        "RETRY",  # retry / backoff decisions
        "METRICS",  # metrics server
        "CBL",  # Couchbase Lite operations (read/write/open/close/maintenance)
        "DLQ",  # dead letter queue operations (add/retry/purge/list)
        "FLOOD",  # flood detection / throttle
        "CONTROL",  # admin API actions (/_restart, /_config)
        "SHUTDOWN",  # graceful drain, signal handling
    }
)

LEVELS = {
    "trace": TRACE,
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

# ---------------------------------------------------------------------------
# Redactor
# ---------------------------------------------------------------------------
_SENSITIVE_FIELDS = re.compile(
    r"(password|passwd|pass|token|bearer_token|session_cookie|"
    r"authorization|cookie|secret|api_key|access_token|refresh_token)",
    re.IGNORECASE,
)

_URL_USERINFO_RE = re.compile(r"(https?://)([^@]+)@")
_HEADER_BEARER_RE = re.compile(r"(Bearer\s+)\S+", re.IGNORECASE)


class Redactor:
    """Redacts sensitive data from strings, dicts, and log messages."""

    def __init__(self, level: str = "partial"):
        self.level = level  # none | partial | full

    def redact_string(self, value: str) -> str:
        if self.level == "none":
            return value
        # Redact URL userinfo
        value = _URL_USERINFO_RE.sub(
            r"\1<ud>***:***</ud>@" if self.level == "partial" else r"\1<ud>XXXXX</ud>@",
            value,
        )
        # Redact Bearer tokens in strings
        if self.level == "partial":
            value = _HEADER_BEARER_RE.sub(
                lambda m: (
                    m.group(1)
                    + m.group(0)[-4:].rjust(len(m.group(0)) - len(m.group(1)), "*")
                ),
                value,
            )
        elif self.level == "full":
            value = _HEADER_BEARER_RE.sub(r"\1<ud>XXXXX</ud>", value)
        return value

    def redact_value(self, key: str, value) -> str:
        if self.level == "none":
            return str(value)
        if not _SENSITIVE_FIELDS.search(key):
            return str(value)
        s = str(value)
        if self.level == "full":
            return "<ud>XXXXX</ud>"
        # partial: show first and last char
        if len(s) <= 2:
            return "<ud>XXXXX</ud>"
        return f"<ud>{s[0]}{'*' * (len(s) - 2)}{s[-1]}</ud>"

    def redact_dict(self, d: dict) -> dict:
        if self.level == "none":
            return d
        out = {}
        for k, v in d.items():
            if isinstance(v, dict):
                out[k] = self.redact_dict(v)
            elif _SENSITIVE_FIELDS.search(k):
                out[k] = self.redact_value(k, v)
            else:
                out[k] = v
        return out


# Module-level redactor (configured during setup)
_redactor = Redactor("none")


def get_redactor() -> Redactor:
    return _redactor


# ---------------------------------------------------------------------------
# Log key / level filter
# ---------------------------------------------------------------------------
class LogKeyLevelFilter(logging.Filter):
    """
    Handler-level filter that checks:
      1. Is record.log_key in the allowed set?
      2. Does record.levelno meet the threshold (base or per-key override)?
    """

    def __init__(
        self, log_keys: list[str], base_level: int, key_levels: dict[str, int]
    ):
        super().__init__()
        self.allow_all = "*" in log_keys
        self.log_keys = set(k.upper() for k in log_keys)
        self.base_level = base_level
        self.key_levels = {k.upper(): v for k, v in key_levels.items()}

    def filter(self, record: logging.LogRecord) -> bool:
        log_key = getattr(record, "log_key", None)
        if log_key is None:
            # Messages without a log_key pass if they meet the base level
            return record.levelno >= self.base_level

        log_key = log_key.upper()
        if not self.allow_all and log_key not in self.log_keys:
            return False

        threshold = self.key_levels.get(log_key, self.base_level)
        return record.levelno >= threshold


# ---------------------------------------------------------------------------
# Redacting formatter
# ---------------------------------------------------------------------------
# Ordered list of (internal_attr, emitted_key).
# Call sites use the internal_attr name in log_event(**fields).
# The formatter emits the shorter emitted_key in the log line.
_EXTRA_FIELDS = (
    ("log_key", "log_key"),
    ("batch_id", "batch_id"),
    ("session_tag", "session_tag"),
    ("job_tag", "job_tag"),
    ("job_id", "job"),
    ("operation", "op"),
    ("doc_id", "doc_id"),
    ("seq", "seq"),
    ("status", "status"),
    ("url", "url"),
    ("attempt", "attempt"),
    ("elapsed_ms", "el_ms"),
    ("mode", "mode"),
    ("http_method", "method"),
    ("bytes", "bytes"),
    ("storage", "store"),
    ("batch_size", "batch"),
    ("input_count", "in_count"),
    ("filtered_count", "filt_count"),
    ("host", "host"),
    ("port", "port"),
    ("delay_seconds", "delay_s"),
    ("field_count", "field_ct"),
    # CBL-specific fields
    ("db_name", "db_name"),
    ("db_path", "db_path"),
    ("db_size_mb", "db_mb"),
    ("doc_count", "doc_count"),
    ("doc_type", "doc_type"),
    ("manifest_id", "manifest"),
    ("maintenance_type", "maint"),
    ("trigger", "trigger"),
    ("duration_ms", "dur_ms"),
    ("error_detail", "err"),
    # Batch summary fields
    ("out_ms", "out_ms"),
    ("seq_from", "seq_from"),
    ("seq_to", "seq_to"),
    ("include_docs", "inc_docs"),
    ("docs_fetched", "fetched"),
    ("docs_missing", "docs_miss"),
    ("attachments", "attach"),
    ("succeeded", "ok"),
    ("failed", "failed"),
    ("filtered_out", "filt_out"),
    ("checkpoint", "chkpt"),
    # DLQ replay fields
    ("dlq_id", "dlq_id"),
    ("replay_attempts", "replay_att"),
    ("replay_attempt", "replay_att"),
    ("ttl_seconds", "ttl_s"),
    # HTTP output fields
    ("error_class", "err_cls"),
    ("content_type", "ct"),
    # Source mutation audit fields (attachment post-process PUT/DELETE)
    ("doc_rev", "rev"),
    ("new_rev", "new_rev"),
    ("pp_action", "pp_action"),
)


class RedactingFormatter(logging.Formatter):
    """
    Formatter that:
      - Prints structured key=value context from record extras
      - Redacts sensitive data in messages
    """

    def __init__(
        self, redactor: Redactor, fmt: str | None = None, datefmt: str | None = None
    ):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.default_msec_format = "%s.%03d"
        self.redactor = redactor

    # Fields rendered inline in the prefix (after [LEVEL][KEY]) rather
    # than as pipe-delimited fields in the suffix.
    _PREFIX_FIELDS = frozenset({"log_key", "batch_id", "session_tag", "job_tag"})

    def format(self, record: logging.LogRecord) -> str:
        # Build pipe-delimited structured fields
        field_parts: list[str] = []
        for attr_name, out_name in _EXTRA_FIELDS:
            if attr_name in self._PREFIX_FIELDS:
                continue
            val = getattr(record, attr_name, None)
            if val is not None:
                if attr_name == "url" and isinstance(val, str):
                    val = self.redactor.redact_string(val)
                field_parts.append(f"{out_name} {val}")

        # Redact the message itself
        record.msg = self.redactor.redact_string(str(record.msg))

        base = super().format(record)

        # Insert [LOG_KEY] and tracing tags right after the [LEVEL] tag.
        # Order: [LEVEL] [KEY] job=..tag #s:..session #b:batch name: message
        log_key = getattr(record, "log_key", None)
        job_tag = getattr(record, "job_tag", None)
        session_tag = getattr(record, "session_tag", None)
        batch_id = getattr(record, "batch_id", None)

        prefix_parts: list[str] = []
        if log_key:
            prefix_parts.append(f"[{log_key}]")
        if job_tag:
            prefix_parts.append(f"job=..{job_tag}")
        if session_tag:
            prefix_parts.append(f"#s:..{session_tag}")
        if batch_id:
            prefix_parts.append(f"#b:{batch_id}")

        if prefix_parts:
            insert = " ".join(prefix_parts)
            base = base.replace(
                f"[{record.levelname}] ",
                f"[{record.levelname}] {insert} ",
                1,
            )

        if field_parts:
            return f"{base} | {' | '.join(field_parts)}"
        return base


# ---------------------------------------------------------------------------
# Managed rotating file handler
# ---------------------------------------------------------------------------
class ManagedRotatingFileHandler(RotatingFileHandler):
    """
    RotatingFileHandler with SG-style retention:
      - max_size: MB per file before rollover
      - max_age: days to retain rotated files
      - rotated_logs_size_limit: total MB cap for rotated files
      - compress: gzip rotated files to .tar.gz (default True)
    """

    def __init__(
        self,
        filename: str,
        max_size_mb: int = 100,
        max_age_days: int = 7,
        rotated_logs_size_limit_mb: int = 1024,
        compress: bool = True,
        **kwargs,
    ):
        self.max_age_days = max_age_days
        self.rotated_logs_size_limit = rotated_logs_size_limit_mb * 1024 * 1024
        self.compress = compress
        os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
        super().__init__(
            filename,
            maxBytes=max_size_mb * 1024 * 1024,
            backupCount=999,  # we manage cleanup ourselves
            **kwargs,
        )

    def doRollover(self):
        super().doRollover()
        if self.compress:
            self._compress_latest_rotated()
        self._cleanup_rotated_files()

    def _compress_latest_rotated(self):
        """Gzip the most recently rotated file (e.g. .log.1 → .log.1.gz)."""
        latest = f"{self.baseFilename}.1"
        if not os.path.exists(latest) or latest.endswith(".gz"):
            return
        gz_path = latest + ".gz"
        try:
            with open(latest, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(latest)
        except OSError:
            pass

    def _cleanup_rotated_files(self):
        base = self.baseFilename
        # Match both .log.N and .log.N.gz
        rotated = sorted(
            _glob.glob(f"{base}.*"),
            key=lambda p: os.path.getmtime(p) if os.path.exists(p) else 0,
        )

        now = time.time()
        max_age_secs = self.max_age_days * 86400

        # Remove files older than max_age
        remaining = []
        for path in rotated:
            try:
                age = now - os.path.getmtime(path)
                if age > max_age_secs:
                    os.remove(path)
                else:
                    remaining.append(path)
            except OSError:
                pass

        # Enforce total size limit (delete oldest first)
        total = sum(os.path.getsize(p) for p in remaining if os.path.exists(p))
        while total > self.rotated_logs_size_limit and remaining:
            oldest = remaining.pop(0)
            try:
                total -= os.path.getsize(oldest)
                os.remove(oldest)
            except OSError:
                pass


class LevelRangeFilter(logging.Filter):
    """Only accept records within [min_level, max_level).

    Used to split log output into per-level files so that e.g.
    the info log doesn't also contain debug lines.
    """

    def __init__(self, min_level: int, max_level: int = logging.CRITICAL + 1):
        super().__init__()
        self.min_level = min_level
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return self.min_level <= record.levelno < self.max_level


# ---------------------------------------------------------------------------
# Operation inference
# ---------------------------------------------------------------------------
def infer_operation(
    change: dict | None = None, doc: dict | None = None, method: str | None = None
) -> str:
    """
    Infer the logical DB operation from a change/doc/method.

    Returns one of: INSERT, UPDATE, DELETE, SELECT.
    """
    if method == "DELETE":
        return "DELETE"
    if change and (change.get("deleted") or change.get("removed")):
        return "DELETE"
    if method == "GET":
        return "SELECT"

    # Check revision to distinguish INSERT vs UPDATE
    rev = None
    if doc:
        rev = doc.get("_rev", "")
    if change and not rev:
        changes_list = change.get("changes", [])
        if changes_list:
            rev = changes_list[0].get("rev", "")

    if rev and rev.startswith("1-"):
        return "INSERT"
    return "UPDATE"


# ---------------------------------------------------------------------------
# Thin helpers for structured logging
# ---------------------------------------------------------------------------
def log_event(
    logger: logging.Logger, level: str, log_key: str, message: str, **fields
) -> None:
    """Log a structured event with a log_key and extra fields.

    Automatically attaches the current batch tracing ID (if set via
    ``set_batch_id()``) so callers don't need to pass it explicitly.
    An explicit ``batch_id=`` kwarg overrides the context value.
    """
    lvl = LEVELS.get(level, logging.INFO)
    if not logger.isEnabledFor(lvl):
        return
    if "batch_id" not in fields:
        bid = _current_batch_id.get()
        if bid is not None:
            fields["batch_id"] = bid
    if "session_tag" not in fields:
        stag = _current_session_tag.get()
        if stag is not None:
            fields["session_tag"] = stag
    if "job_tag" not in fields:
        jtag = _current_job_tag.get()
        if jtag is not None:
            fields["job_tag"] = jtag
    logger.log(lvl, message, extra={"log_key": log_key, **fields})


# ---------------------------------------------------------------------------
# Main configuration entry point
# ---------------------------------------------------------------------------
def configure_logging(cfg: dict) -> None:
    """
    Configure the logging system from the config.logging dict.

    Supports both the legacy {"level": "DEBUG"} format and the full
    SG-inspired config with console/file/rotation/redaction.
    """
    global _redactor, _queue_listener

    # Stop any previous background listener before reconfiguring.
    if _queue_listener is not None:
        if getattr(_queue_listener, "_thread", None) is not None:
            _queue_listener.stop()
        _queue_listener = None

    root = logging.getLogger()
    # Clear existing handlers
    root.handlers.clear()
    root.setLevel(TRACE)

    # Collect real handlers; we'll attach them to a background QueueListener
    # instead of the root logger so emit()/flush() never block the event loop.
    real_handlers: list[logging.Handler] = []

    # Legacy mode: simple level string
    if "console" not in cfg and "file" not in cfg:
        level_str = cfg.get("level", "DEBUG").lower()
        level = LEVELS.get(level_str, logging.DEBUG)
        _redactor = Redactor(cfg.get("redaction_level", "none"))

        handler = logging.StreamHandler()
        handler.setLevel(level)
        fmt = RedactingFormatter(
            _redactor,
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
        handler.setFormatter(fmt)
        real_handlers.append(handler)

        # Route icecream to TRACE
        try:
            from icecream import ic

            ic.configureOutput(
                prefix="ic| ",
                outputFunction=lambda s: logging.getLogger("changes_worker").log(
                    TRACE, s
                ),
            )
        except ImportError:
            pass

        _queue_listener = _start_queue_logging(root, real_handlers)
        return

    # Full SG-style config
    redaction_level = cfg.get("redaction_level", "partial")
    _redactor = Redactor(redaction_level)

    # Console handler
    console_cfg = cfg.get("console", {})
    if console_cfg.get("enabled", True):
        base_level = LEVELS.get(
            console_cfg.get("log_level", "info").lower(), logging.INFO
        )
        log_keys = console_cfg.get("log_keys", ["*"])
        key_levels = {
            k: LEVELS.get(v.lower(), logging.INFO)
            for k, v in console_cfg.get("key_levels", {}).items()
        }

        handler = logging.StreamHandler()
        handler.setLevel(TRACE)  # actual filtering via LogKeyLevelFilter
        handler.addFilter(LogKeyLevelFilter(log_keys, base_level, key_levels))
        fmt = RedactingFormatter(
            _redactor,
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
        handler.setFormatter(fmt)
        real_handlers.append(handler)

    # File handlers — split by level into separate log files:
    #   _info.log   — INFO and above (operator-facing)
    #   _debug.log  — DEBUG and above (developer detail)
    #   _error.log  — ERROR and above (alerting / on-call)
    #   _trace.log  — TRACE and above (only when log_level=trace)
    #
    # Each file has its own rotation/retention and rotated files are
    # gzip-compressed automatically.
    file_cfg = cfg.get("file", {})
    if file_cfg.get("enabled", False):
        file_path = file_cfg.get("path", "logs/changes_worker.log")
        base_level = LEVELS.get(
            file_cfg.get("log_level", "debug").lower(), logging.DEBUG
        )
        log_keys = file_cfg.get("log_keys", ["*"])
        key_levels = {
            k: LEVELS.get(v.lower(), logging.DEBUG)
            for k, v in file_cfg.get("key_levels", {}).items()
        }
        compress = file_cfg.get("compress_rotated", True)

        rotation = file_cfg.get("rotation", {})
        max_size = rotation.get("max_size", 100)
        max_age = rotation.get("max_age", 7)
        rotated_limit = rotation.get("rotated_logs_size_limit", 1024)

        # Derive per-level filenames from the base path.
        # "logs/changes_worker.log" → "logs/changes_worker_info.log" etc.
        stem, ext = os.path.splitext(file_path)

        _LOG_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

        # The level tiers to create.  Each entry:
        #   (suffix, min_level, max_level, always_create)
        _LEVEL_TIERS = [
            ("_error", logging.ERROR, logging.CRITICAL + 1, True),
            ("_info", logging.INFO, logging.ERROR, True),
            ("_debug", logging.DEBUG, logging.INFO, True),
            ("_trace", TRACE, logging.DEBUG, False),  # only if trace enabled
        ]

        for suffix, min_lvl, max_lvl, always in _LEVEL_TIERS:
            # Skip trace file unless the configured level is trace
            if not always and base_level > TRACE:
                continue
            # Skip tiers below the configured base level
            # (e.g. if base_level=INFO, don't create _debug.log)
            if min_lvl < base_level:
                continue

            tier_path = f"{stem}{suffix}{ext}"
            handler = ManagedRotatingFileHandler(
                tier_path,
                max_size_mb=max_size,
                max_age_days=max_age,
                rotated_logs_size_limit_mb=rotated_limit,
                compress=compress,
            )
            handler.setLevel(TRACE)  # actual filtering via filters below
            handler.addFilter(LevelRangeFilter(min_lvl, max_lvl))
            handler.addFilter(LogKeyLevelFilter(log_keys, min_lvl, key_levels))
            fmt = RedactingFormatter(_redactor, fmt=_LOG_FMT)
            handler.setFormatter(fmt)
            real_handlers.append(handler)

    # Configure specific logger levels
    logger_levels = cfg.get("logger_levels", {})
    for logger_name, level_str in logger_levels.items():
        level = LEVELS.get(level_str.lower(), logging.INFO)
        logging.getLogger(logger_name).setLevel(level)

    # Route icecream to TRACE
    try:
        from icecream import ic

        ic.configureOutput(
            prefix="ic| ",
            outputFunction=lambda s: logging.getLogger("changes_worker").log(TRACE, s),
        )
    except ImportError:
        pass

    _queue_listener = _start_queue_logging(root, real_handlers)

    # Set up the dedicated eventing JS logger (file output only)
    configure_eventing_logger(cfg)


# ---------------------------------------------------------------------------
# Eventing JS logger — dedicated file for user JS log() / console.* output
# ---------------------------------------------------------------------------
# Separate logger instance, NOT a child of root, so JS output goes only to
# the eventing log file (+ optionally console) and never pollutes the main
# changes_worker log.

_eventing_js_logger: logging.Logger | None = None


def get_eventing_js_logger() -> logging.Logger | None:
    """Return the configured eventing JS logger, or None if not set up."""
    return _eventing_js_logger


def configure_eventing_logger(cfg: dict) -> None:
    """
    Set up the dedicated eventing JS logger.

    Reads rotation settings from the main logging config so it follows the
    same retention policy.  Output format mimics Node.js console output::

        2026-04-24T14:06:34.796Z  INFO  [eventing-js] hello from OnUpdate
        2026-04-24T14:06:34.800Z  WARN  [eventing-js] missing field "price"
        2026-04-24T14:06:34.801Z ERROR  [eventing-js] ReferenceError: x is not defined

    Called automatically by ``configure_logging()`` when a file handler is
    enabled.  Can also be called standalone for tests.
    """
    global _eventing_js_logger

    log_cfg = cfg  # the full logging config dict
    file_cfg = log_cfg.get("file", {})
    rotation = file_cfg.get("rotation", {})

    # Derive the eventing log directory from the main log path
    main_log_path = file_cfg.get("path", "logs/changes_worker.log")
    log_dir = os.path.dirname(main_log_path) or "logs"
    eventing_log_path = os.path.join(log_dir, "eventing", "eventing.log")

    eventing_logger = logging.getLogger("eventing_js")
    eventing_logger.handlers.clear()
    eventing_logger.setLevel(logging.DEBUG)
    eventing_logger.propagate = False  # do NOT bubble up to root/changes_worker

    # Node.js-style formatter: ISO timestamp, level, message
    class _NodeStyleFormatter(logging.Formatter):
        """Formatter that mimics Node.js console output with WARN instead of WARNING."""

        _LEVEL_NAMES = {
            "WARNING": " WARN",
            "CRITICAL": "FATAL",
        }

        def format(self, record: logging.LogRecord) -> str:
            record.levelname = self._LEVEL_NAMES.get(
                record.levelname, record.levelname.rjust(5)
            )
            return super().format(record)

        def formatTime(self, record, datefmt=None):
            from datetime import datetime, timezone

            dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % record.msecs

    node_fmt = _NodeStyleFormatter(
        fmt="%(asctime)s %(levelname)s  %(message)s",
    )

    # File handler with same rotation policy as the project
    file_handler = ManagedRotatingFileHandler(
        eventing_log_path,
        max_size_mb=rotation.get("max_size", 100),
        max_age_days=rotation.get("max_age", 7),
        rotated_logs_size_limit_mb=rotation.get("rotated_logs_size_limit", 1024),
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(node_fmt)
    eventing_logger.addHandler(file_handler)

    _eventing_js_logger = eventing_logger


def _start_queue_logging(
    root: logging.Logger, handlers: list[logging.Handler]
) -> QueueListener:
    """Attach a QueueHandler to root and drain to *handlers* on a background thread."""
    log_queue: queue.Queue = queue.Queue(-1)  # unbounded
    root.addHandler(QueueHandler(log_queue))
    listener = QueueListener(log_queue, *handlers, respect_handler_level=True)
    listener.start()

    def _safe_stop(ref=listener):
        if getattr(ref, "_thread", None) is not None:
            ref.stop()

    atexit.register(_safe_stop)
    return listener
