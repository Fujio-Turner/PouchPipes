"""aiohttp web server for the changes_worker admin UI."""

import argparse
import asyncio
import json
import logging
import os
import re
from pathlib import Path

import aiohttp as _aiohttp
from aiohttp import web

import datetime

from storage.cbl_store import USE_CBL, CBLStore
from pipeline.pipeline_logging import configure_logging, log_event
from schema.mapper import SchemaMapper
from db.db_base import group_insert_ops, _MultiRowInsert
from db.db_postgres import PostgresOutputForwarder
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
    api_put_job_mapping,
    api_get_tables_rdbms,
    api_post_tables_rdbms,
    api_get_table_rdbms_entry,
    api_put_table_rdbms_entry,
    api_delete_table_rdbms_entry,
    api_get_table_rdbms_used_by,
    api_get_job_eventing,
    api_put_job_eventing,
)

logger = logging.getLogger("changes_worker")

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "web"
CONFIG_PATH = ROOT / "config.json"
MAPPINGS_DIR = ROOT / "mappings"
SOURCES_DIR = ROOT / "sources"


def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, PUT, PATCH, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }


def json_response(data, status=200):
    return web.json_response(data, status=status, headers=cors_headers())


def error_response(msg, status=400):
    return json_response({"error": msg}, status=status)


@web.middleware
async def cors_middleware(request, handler):
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=cors_headers())
    resp = await handler(request)
    resp.headers.update(cors_headers())
    return resp


# --- Pages ---


async def favicon(request):
    return web.FileResponse(
        WEB / "static" / "favicon.svg", headers={"Content-Type": "image/svg+xml"}
    )


async def page_index(request):
    return web.FileResponse(WEB / "templates" / "index.html")


async def page_config(request):
    return web.FileResponse(WEB / "templates" / "settings.html")


async def page_schema(request):
    return web.FileResponse(WEB / "templates" / "schema.html")


async def page_transforms(request):
    return web.FileResponse(WEB / "templates" / "glossary.html")


async def page_wizard(request):
    return web.FileResponse(WEB / "templates" / "wizard.html")


async def page_jobs(request):
    return web.FileResponse(WEB / "templates" / "jobs.html")


async def page_inputs(request):
    return web.FileResponse(WEB / "templates" / "inputs.html")


async def page_outputs(request):
    return web.FileResponse(WEB / "templates" / "outputs.html")


async def page_help(request):
    return web.FileResponse(WEB / "templates" / "help.html")


async def page_logs(request):
    return web.FileResponse(WEB / "templates" / "logs.html")


async def page_dlq(request):
    return web.FileResponse(WEB / "templates" / "dlq.html")


async def page_eventing(request):
    return web.FileResponse(WEB / "templates" / "eventing.html")


# --- Logs API ---

# New format: TIMESTAMP [LEVEL] [KEY] job=..JOB #s:..SESSION #b:BATCH LOGGER: MESSAGE | field value | ...
_LOG_LINE_NEW_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[.,]\d{3})\s+"  # timestamp
    r"\[(\w+)\]\s*"  # level
    r"\[([A-Z_]+)\]\s+"  # log_key
    r"(.*?)"  # prefix (job=.. #s:.. #b:..)
    r"([\w.]+):\s+"  # logger
    r"(.+)$"  # rest (message | fields)
)

# Legacy format: TIMESTAMP [LEVEL] LOGGER: MESSAGE [LOG_KEY] key=value ...
_LOG_LINE_OLD_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[.,]\d{3})\s+"  # timestamp
    r"\[(\w+)\]\s+"  # level
    r"([\w.]+):\s+"  # logger
    r"(.+)$"  # rest
)

_LOG_KEY_RE = re.compile(r"\[([A-Z_]+)\]")

# Context tag patterns in the prefix
_JOB_TAG_RE = re.compile(r"job=\.\.(\S+)")
_SESSION_TAG_RE = re.compile(r"#s:\.\.(\S+)")
_BATCH_TAG_RE = re.compile(r"#b:(\S+)")

# Known simple fields — legacy key=value format
_SIMPLE_FIELDS = {
    "doc_id",
    "seq",
    "status",
    "url",
    "attempt",
    "elapsed_ms",
    "mode",
    "http_method",
    "bytes",
    "storage",
    "batch_size",
    "input_count",
    "filtered_count",
    "host",
    "port",
    "delay_seconds",
    "field_count",
    "db_name",
    "db_path",
    "db_size_mb",
    "doc_count",
    "doc_type",
    "manifest_id",
    "maintenance_type",
    "duration_ms",
    "operation",
    "job_id",
}


def _parse_log_line(line: str) -> dict | None:
    stripped = line.strip()
    if not stripped:
        return None

    # Try new format first
    m = _LOG_LINE_NEW_RE.match(stripped)
    if m:
        timestamp, level, log_key, prefix, logger_name, rest = m.groups()

        # Extract context tags from prefix
        job_tag = ""
        session_tag = ""
        batch_tag = ""
        jm = _JOB_TAG_RE.search(prefix)
        if jm:
            job_tag = jm.group(1)
        sm = _SESSION_TAG_RE.search(prefix)
        if sm:
            session_tag = sm.group(1)
        bm = _BATCH_TAG_RE.search(prefix)
        if bm:
            batch_tag = bm.group(1)

        # Parse pipe-delimited: MESSAGE_WITH_PIPES | field1 value | field2 value
        # Fields are at the END.  Scan backwards from the last segment to find
        # where the contiguous run of known-key segments starts.
        fields = {}
        message = rest
        pipe_idx = rest.find(" | ")
        if pipe_idx != -1:
            segments = rest.split(" | ")
            # Find the boundary: last non-field segment (scanning from end)
            first_field = len(segments)
            for i in range(len(segments) - 1, 0, -1):
                seg = segments[i].strip()
                if not seg:
                    continue
                sp = seg.find(" ")
                key = seg[:sp] if sp != -1 else seg
                if key in _PIPE_FIELD_KEYS:
                    first_field = i
                    fields[key] = seg[sp + 1 :] if sp != -1 else ""
                else:
                    break  # hit a non-field segment, stop
            message = " | ".join(segments[:first_field])

        # Inject context tags as fields for filtering
        if job_tag:
            fields["job_tag"] = job_tag
        if session_tag:
            fields["session"] = session_tag
        if batch_tag:
            fields["batch"] = batch_tag

        return {
            "timestamp": timestamp,
            "level": level,
            "logger": logger_name,
            "message": message,
            "log_key": log_key,
            "fields": fields,
            "job_tag": job_tag,
            "session_tag": session_tag,
            "batch_tag": batch_tag,
        }

    # Fallback: legacy format
    m = _LOG_LINE_OLD_RE.match(stripped)
    if not m:
        return None
    timestamp, level, logger_name, rest = m.groups()

    # Extract log_key
    log_key = None
    key_match = _LOG_KEY_RE.search(rest)
    if key_match:
        log_key = key_match.group(1)
        message = rest[: key_match.start()].strip()
        fields_str = rest[key_match.end() :].strip()
    else:
        message = rest.strip()
        fields_str = ""

    # Parse key=value fields
    fields = {}
    if fields_str:
        ed_idx = fields_str.find("error_detail=")
        if ed_idx >= 0:
            before = fields_str[:ed_idx].strip()
            fields["error_detail"] = fields_str[ed_idx + len("error_detail=") :]
            fields_str = before

        for part in fields_str.split():
            if "=" in part:
                k, v = part.split("=", 1)
                if k in _SIMPLE_FIELDS:
                    fields[k] = v

    return {
        "timestamp": timestamp,
        "level": level,
        "logger": logger_name,
        "message": message,
        "log_key": log_key,
        "fields": fields,
        "job_tag": "",
        "session_tag": "",
        "batch_tag": "",
    }


_LEVEL_RANK = {"ERROR": 0, "WARNING": 1, "INFO": 2, "DEBUG": 3, "TRACE": 4}


_CHUNK = 32_768  # 32 KB read chunk
_INDEX_STEP = 1000  # record byte offset every N lines


# ── Line-count + sparse index cache ───────────────────────
# Keyed by resolved path string.  Invalidated when mtime or size changes.
# {
#   "mtime": float,
#   "size":  int,
#   "total": int,                     # total line count
#   "index": { 0: 0, 1000: 82341, …} # line_number → byte_offset
# }
_line_cache: dict[str, dict] = {}


def _get_line_info(path: Path) -> dict:
    """Return total line count + sparse index for *path*.

    First call scans the file counting newlines in 32 KB chunks — O(1) memory,
    sequential I/O.  Records the byte offset every _INDEX_STEP lines for fast
    seeking later.  Result is cached until the file's mtime or size changes.
    """
    stat = path.stat()
    key = str(path)
    cached = _line_cache.get(key)
    if cached and cached["mtime"] == stat.st_mtime and cached["size"] == stat.st_size:
        return cached

    index: dict[int, int] = {0: 0}
    total = 0
    with open(path, "rb") as f:
        while True:
            offset_before = f.tell()
            chunk = f.read(_CHUNK)
            if not chunk:
                break
            pos = 0
            while True:
                nl = chunk.find(b"\n", pos)
                if nl == -1:
                    break
                total += 1
                if total % _INDEX_STEP == 0:
                    index[total] = offset_before + nl + 1
                pos = nl + 1

    entry = {
        "mtime": stat.st_mtime,
        "size": stat.st_size,
        "total": total,
        "index": index,
    }
    _line_cache[key] = entry
    return entry


def _seek_to_line(path: Path, from_line: int):
    """Seek a file to *from_line* using the sparse index.  Returns (file, info)."""
    info = _get_line_info(path)
    idx = info["index"]
    nearest = 0
    for k in idx:
        if k <= from_line and k > nearest:
            nearest = k
    f = open(path, "rb")
    f.seek(idx[nearest])
    for _ in range(from_line - nearest):
        f.readline()
    return f, info


# Maps log_key → pipeline stage (server-side, mirrors the frontend)
_LOG_KEY_STAGE = {
    "CHANGES": "source",
    "HTTP": "source",
    "PROCESSING": "process",
    "EVENTING": "process",
    "ATTACHMENT": "process",
    "MAPPING": "process",
    "FLOOD": "process",
    "OUTPUT": "output",
    "CHECKPOINT": "output",
    "RETRY": "output",
    "DLQ": "dlq",
    "CBL": "infra",
    "METRICS": "infra",
    "CONTROL": "infra",
    "SHUTDOWN": "infra",
}

_LEVEL_BADGE_CLS = {
    "ERROR": "badge-error",
    "CRITICAL": "badge-error",
    "WARNING": "badge-warning",
    "INFO": "badge-info",
    "TRACE": "badge-ghost opacity-50",
}

_SKIP_FIELDS = {"doc_id", "job_tag", "session", "batch", "job_id"}

# Known pipe-delimited field keys (log keys from GUIDE_LOGGING.md)
# Used to distinguish "| field value" from message continuations
_PIPE_FIELD_KEYS = {
    "job",
    "op",
    "doc_id",
    "seq",
    "status",
    "url",
    "attempt",
    "el_ms",
    "dur_ms",
    "out_ms",
    "mode",
    "method",
    "bytes",
    "store",
    "batch",
    "in_count",
    "filt_count",
    "host",
    "port",
    "delay_s",
    "field_ct",
    "err",
    "doc_count",
    "doc_type",
    "seq_from",
    "seq_to",
    "inc_docs",
    "fetched",
    "docs_miss",
    "attach",
    "ok",
    "failed",
    "filt_out",
    "chkpt",
    "db_name",
    "db_path",
    "db_mb",
    "manifest",
    "maint",
    "trigger",
    "session",
    # Source mutation audit fields (attachment post-process PUT/DELETE)
    "rev",
    "new_rev",
    "pp_action",
}


def _esc(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _render_line_html(entry: dict, idx: int) -> str:
    """Build the HTML for a single log line — same output as the frontend renderLogs()."""
    stage = _LOG_KEY_STAGE.get(entry.get("log_key") or "", "")
    stage_cls = f" log-stage-{stage}" if stage else ""
    parts = [
        f'<div class="log-line flex flex-wrap items-start gap-2 px-3 py-1{stage_cls}" data-idx="{idx}" onclick="onLogClick({idx})">'
    ]

    # Timestamp
    parts.append(f'<span class="log-ts">{_esc(entry.get("timestamp", ""))}</span>')

    # Level badge
    level = entry.get("level", "")
    badge_cls = _LEVEL_BADGE_CLS.get(level, "badge-ghost")
    parts.append(f'<span class="badge badge-xs {badge_cls}">{_esc(level)}</span>')

    # Log key
    log_key = entry.get("log_key") or ""
    if log_key:
        parts.append(
            f'<span class="badge badge-outline badge-xs">{_esc(log_key)}</span>'
        )

    # Context tags
    job_tag = entry.get("job_tag", "")
    if job_tag:
        parts.append(
            f'<span class="badge badge-secondary badge-xs font-mono" title="Job tag">job:..{_esc(job_tag)}</span>'
        )
    session_tag = entry.get("session_tag", "")
    if session_tag:
        parts.append(
            f'<span class="badge badge-accent badge-xs font-mono" title="Session ID">#s:..{_esc(session_tag)}</span>'
        )
    batch_tag = entry.get("batch_tag", "")
    if batch_tag:
        parts.append(
            f'<span class="badge badge-primary badge-xs font-mono" title="Batch ID">#b:{_esc(batch_tag)}</span>'
        )

    # Doc ID
    fields = entry.get("fields") or {}
    doc_id = fields.get("doc_id", "")
    if doc_id:
        parts.append(
            f'<span class="badge badge-primary badge-xs font-mono">📄 {_esc(doc_id)}</span>'
        )

    # Message
    parts.append(
        f'<span class="log-msg flex-1">{_esc(entry.get("message", ""))}</span>'
    )

    # Extra fields as badges
    for k, v in fields.items():
        if k in _SKIP_FIELDS:
            continue
        v_str = str(v)
        if len(v_str) > 60:
            v_str = v_str[:57] + "…"
        parts.append(
            f'<span class="badge badge-ghost badge-xs font-mono">{_esc(k)}={_esc(v_str)}</span>'
        )

    parts.append("</div>")
    return "".join(parts)


# Timestamp regex for binary search
_TS_PREFIX_RE = re.compile(rb"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def _find_line_for_time(path: Path, target: str) -> int:
    """Binary search a chronologically-sorted log for *target* timestamp.

    Returns the line number (0-based from top) of the first line whose
    timestamp >= target.  Uses the sparse index to convert byte offset
    to line number.
    """
    size = path.stat().st_size
    if size == 0:
        return 0
    target_b = target.encode("utf-8")

    # Binary search on byte offsets to find the target offset
    with open(path, "rb") as f:
        lo, hi = 0, size
        while lo < hi:
            mid = (lo + hi) // 2
            f.seek(mid)
            if mid > 0:
                f.readline()  # skip to next complete line
            line_start = f.tell()
            if line_start >= size:
                hi = mid
                continue
            line = f.readline()
            m = _TS_PREFIX_RE.match(line)
            if not m:
                lo = line_start + len(line)
                continue
            if m.group(1) < target_b:
                lo = line_start + len(line)
            else:
                hi = mid
    target_offset = lo

    # Convert byte offset → line number using the sparse index.
    # Find the highest index entry whose offset <= target_offset,
    # then count lines forward from there.
    info = _get_line_info(path)
    nearest_line = 0
    nearest_offset = 0
    for ln, off in info["index"].items():
        if off <= target_offset and ln > nearest_line:
            nearest_line = ln
            nearest_offset = off

    # Count lines from nearest_offset to target_offset
    line_num = nearest_line
    with open(path, "rb") as f:
        f.seek(nearest_offset)
        while f.tell() < target_offset:
            raw = f.readline()
            if not raw:
                break
            line_num += 1

    return line_num


async def get_logs(request):
    page_size = min(int(request.query.get("page_size", "500")), 5000)
    file_name = request.query.get("file", "changes_worker.log")
    min_level = request.query.get("level", "").upper()
    from_line_param = request.query.get("from_line", "")
    before_time = request.query.get("before_time", "")

    if not file_name.endswith(".log") or "\\" in file_name or ".." in file_name:
        return error_response("Invalid file name", 400)
    log_path = (ROOT / "logs" / file_name).resolve()
    if not str(log_path).startswith(str((ROOT / "logs").resolve())):
        return error_response("Invalid file path", 400)
    if not log_path.is_file():
        return json_response([])

    level_threshold = _LEVEL_RANK.get(min_level, -1)

    try:
        # Determine start line
        if before_time:
            target_line = _find_line_for_time(log_path, before_time)
            start = max(0, target_line - page_size)
        elif from_line_param:
            start = int(from_line_param)
        else:
            info = _get_line_info(log_path)
            start = max(0, info["total"] - page_size)

        # Single pass: seek → read → parse → filter → build HTML → aggregate
        f, info = _seek_to_line(log_path, start)
        total = info["total"]
        size = info["size"]

        entries = []  # lightweight entries for client-side filtering/charts
        html_lines = []  # pre-rendered HTML per line
        level_counts = {"ERROR": 0, "WARNING": 0, "INFO": 0, "DEBUG": 0, "TRACE": 0}
        stage_counts = {"source": 0, "process": 0, "output": 0, "dlq": 0, "infra": 0}
        time_buckets = {}  # "YYYY-MM-DD HH:MM" → {errors,warnings,info,debug,source,process,...}
        line_idx = 0

        try:
            while line_idx < page_size:
                raw = f.readline()
                if not raw:
                    break
                parsed = _parse_log_line(raw.decode("utf-8", errors="replace"))
                if not parsed:
                    line_idx += 1
                    continue

                line_idx += 1

                # Level filter
                if level_threshold >= 0:
                    entry_rank = _LEVEL_RANK.get(parsed["level"], 4)
                    if entry_rank > level_threshold:
                        continue

                # Aggregate: level + stage counts
                lv = parsed["level"]
                if lv in level_counts:
                    level_counts[lv] += 1
                stage = _LOG_KEY_STAGE.get(parsed.get("log_key") or "", "process")
                if stage in stage_counts:
                    stage_counts[stage] += 1

                # Aggregate: time buckets for charts
                ts = parsed.get("timestamp", "")
                if len(ts) >= 16:
                    bucket_key = ts[:16].replace(",", ".")  # "YYYY-MM-DD HH:MM"
                    bkt = time_buckets.get(bucket_key)
                    if not bkt:
                        bkt = {
                            "errors": 0,
                            "warnings": 0,
                            "info": 0,
                            "debug": 0,
                            "source": 0,
                            "process": 0,
                            "output": 0,
                            "dlq": 0,
                            "infra": 0,
                            "docs_in": 0,
                            "docs_out_ok": 0,
                            "docs_out_fail": 0,
                            "retries": 0,
                        }
                        time_buckets[bucket_key] = bkt
                    if lv == "ERROR":
                        bkt["errors"] += 1
                    elif lv == "WARNING":
                        bkt["warnings"] += 1
                    elif lv == "INFO":
                        bkt["info"] += 1
                    elif lv == "DEBUG":
                        bkt["debug"] += 1
                    bkt[stage] = bkt.get(stage, 0) + 1
                    fields = parsed.get("fields") or {}
                    doc_count = fields.get("doc_count") or fields.get("batch")
                    if doc_count:
                        try:
                            bkt["docs_in"] += int(doc_count)
                        except ValueError:
                            pass
                    if stage == "output" and fields.get("doc_id"):
                        if lv == "ERROR":
                            bkt["docs_out_fail"] += 1
                        else:
                            bkt["docs_out_ok"] += 1
                    if parsed.get("log_key") == "RETRY":
                        bkt["retries"] += 1

                # Build HTML for this line
                html_lines.append(_render_line_html(parsed, len(entries)))

                # Keep lightweight entry for client-side (search, insight, stakes)
                entries.append(parsed)
        finally:
            f.close()

        actual_from = start
        actual_to = min(start + line_idx, total)

    except Exception as exc:
        return error_response(str(exc), 500)

    return json_response(
        {
            "entries": entries,
            "html": "".join(html_lines),
            "counts": {"levels": level_counts, "stages": stage_counts},
            "time_buckets": time_buckets,
            "from_line": actual_from,
            "to_line": actual_to,
            "total_lines": total,
            "file_size": size,
            "has_older": actual_from > 0,
            "has_newer": actual_to < total,
        }
    )


async def get_log_files(request):
    logs_dir = ROOT / "logs"
    if not logs_dir.is_dir():
        return json_response([])
    files = []
    for p in logs_dir.rglob("*.log"):
        if p.is_file():
            stat = p.stat()
            # Use path relative to logs/ so subdirectory files are addressable
            rel = p.relative_to(logs_dir)
            files.append(
                {
                    "name": str(rel),
                    "size_bytes": stat.st_size,
                    "modified": datetime.datetime.fromtimestamp(
                        stat.st_mtime, tz=datetime.timezone.utc
                    ).isoformat(),
                }
            )
    files.sort(key=lambda f: f["modified"], reverse=True)
    return json_response(files)


# --- Config API ---


async def get_config(request):
    if USE_CBL:
        return json_response(CBLStore().load_config() or {})
    try:
        data = json.loads(CONFIG_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        return error_response(str(exc), 500)
    return json_response(data)


async def put_config(request):
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return error_response("Invalid JSON")

    # ── Phase 7: Settings Cleanup ──
    # Validate: reject job configuration fields
    REJECTED_FIELDS = {"gateway", "auth", "changes_feed", "output"}
    found_rejected = [f for f in REJECTED_FIELDS if f in body]
    if found_rejected:
        return error_response(
            f"Job configuration ('{', '.join(found_rejected)}') cannot be edited in Settings. "
            "Use the Wizard to create and manage jobs instead.",
            status=400,
        )

    # ── Allowed infrastructure fields ──
    ALLOWED_FIELDS = {
        "couchbase_lite",
        "logging",
        "admin_ui",
        "metrics",
        "shutdown",
        "threads",
        "checkpoint",
        "retry",
        "processing",
        "attachments",
    }
    if body:
        keys = set(body.keys())
        unexpected = keys - ALLOWED_FIELDS
        if unexpected:
            # Log unexpected fields for debugging but allow (for forward compatibility)
            import logging as _logging

            _log = _logging.getLogger("changes_worker")
            _log.warning(
                f"put_config: unexpected fields {unexpected}. Allowed: {ALLOWED_FIELDS}"
            )

    if USE_CBL:
        CBLStore().save_config(body)
    else:
        CONFIG_PATH.write_text(json.dumps(body, indent=2) + "\n")

    # Signal the worker to restart its changes feed with the new config
    restart_result = await _signal_worker_restart()
    return json_response({"ok": True, "restart": restart_result})


async def _signal_worker_restart() -> str:
    """POST to the worker's /_restart endpoint to trigger a feed restart."""
    import os
    import aiohttp as _aiohttp

    worker_host = os.environ.get("METRICS_HOST")
    if not worker_host:
        return "skipped"  # running locally, no separate worker
    try:
        cfg = (
            CBLStore().load_config() if USE_CBL else json.loads(CONFIG_PATH.read_text())
        )
        port = cfg.get("metrics", {}).get("port", 9090)
    except Exception:
        port = 9090
    url = f"http://{worker_host}:{port}/_restart"
    try:
        async with _aiohttp.ClientSession() as session:
            async with session.post(
                url, timeout=_aiohttp.ClientTimeout(total=5)
            ) as resp:
                return "ok" if resp.status == 200 else f"error:{resp.status}"
    except Exception as exc:
        return f"error:{exc}"


# --- Mappings API ---


def _valid_mapping_name(name: str) -> bool:
    return (
        name.endswith((".yaml", ".yml", ".json"))
        and "/" not in name
        and "\\" not in name
    )


async def list_mappings(request):
    if USE_CBL:
        return json_response(CBLStore().list_mappings())
    MAPPINGS_DIR.mkdir(exist_ok=True)
    files = sorted(
        p
        for p in MAPPINGS_DIR.iterdir()
        if p.is_file() and p.suffix in (".yaml", ".yml", ".json")
    )
    import os as _os

    result = []
    for p in files:
        content = p.read_text()
        meta = {"active": True, "updated_at": ""}
        try:
            parsed = json.loads(content)
            m = parsed.get("meta", {})
            meta["active"] = m.get("active", True)
            meta["updated_at"] = m.get("updated_at", "")
        except (json.JSONDecodeError, AttributeError):
            pass
        if not meta["updated_at"]:
            meta["updated_at"] = datetime.datetime.fromtimestamp(
                _os.path.getmtime(p), tz=datetime.timezone.utc
            ).isoformat()
        result.append({"name": p.name, "content": content, **meta})
    return json_response(result)


async def get_mapping(request):
    name = request.match_info["name"]
    if not _valid_mapping_name(name):
        return error_response("Invalid filename")
    if USE_CBL:
        content = CBLStore().get_mapping(name)
        if content is None:
            return error_response("Not found", 404)
        return json_response({"name": name, "content": content})
    path = MAPPINGS_DIR / name
    if not path.is_file():
        return error_response("Not found", 404)
    return json_response({"name": name, "content": path.read_text()})


async def put_mapping(request):
    name = request.match_info["name"]
    if not _valid_mapping_name(name):
        return error_response("Invalid filename")
    content = await request.text()
    if USE_CBL:
        CBLStore().save_mapping(name, content)
    # Inject meta into JSON content before writing to filesystem
    try:
        parsed = json.loads(content)
        meta = parsed.get("meta", {})
        meta["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        if "active" not in meta:
            meta["active"] = True
        parsed["meta"] = meta
        content = json.dumps(parsed, indent=2)
    except (json.JSONDecodeError, ValueError):
        pass  # not valid JSON, save as-is
    # Write to filesystem as fallback cache (CBL is the source of truth when available)
    MAPPINGS_DIR.mkdir(exist_ok=True)
    (MAPPINGS_DIR / name).write_text(content)
    return json_response({"ok": True})


async def patch_mapping_active(request):
    name = request.match_info["name"]
    if not _valid_mapping_name(name):
        return error_response("Invalid filename")
    try:
        body = await request.json()
    except Exception:
        return error_response("Invalid JSON body")
    active = body.get("active")
    if active is None or not isinstance(active, bool):
        return error_response("'active' must be a boolean")

    if USE_CBL:
        if not CBLStore().set_mapping_active(name, active):
            return error_response("Not found", 404)

    # Also update on filesystem
    path = MAPPINGS_DIR / name
    if path.is_file():
        try:
            parsed = json.loads(path.read_text())
            meta = parsed.get("meta", {})
            meta["active"] = active
            meta["updated_at"] = datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat()
            parsed["meta"] = meta
            path.write_text(json.dumps(parsed, indent=2))
        except (json.JSONDecodeError, ValueError):
            pass

    return json_response({"ok": True, "active": active})


async def delete_mapping(request):
    name = request.match_info["name"]
    if not _valid_mapping_name(name):
        return error_response("Invalid filename")
    if USE_CBL:
        CBLStore().delete_mapping(name)
    # Always remove from filesystem too
    path = MAPPINGS_DIR / name
    if path.is_file():
        path.unlink()
    return json_response({"ok": True})


# --- DLQ API ---


async def list_dlq(request):
    if not USE_CBL:
        return json_response({"entries": [], "total": 0, "filtered": 0})
    q = request.query
    limit = min(int(q.get("limit", 20)), 200)
    offset = int(q.get("offset", 0))
    sort = q.get("sort", "time")
    order = q.get("order", "desc")
    reason_filter = q.get("reason", "")
    return json_response(
        CBLStore().list_dlq_page(
            limit=limit,
            offset=offset,
            sort=sort,
            order=order,
            reason_filter=reason_filter,
        )
    )


async def dlq_stats(request):
    if not USE_CBL:
        return json_response(
            {
                "total": 0,
                "pending": 0,
                "retried": 0,
                "oldest_time": None,
                "reason_counts": {},
                "timeline": {},
            }
        )
    return json_response(CBLStore().dlq_stats())


async def get_dlq_entry(request):
    if not USE_CBL:
        return error_response("DLQ requires CBL", 501)
    dlq_id = request.match_info["id"]
    entry = CBLStore().get_dlq_entry(dlq_id)
    if not entry:
        return error_response("Not found", 404)
    return json_response(entry)


async def retry_dlq_entry(request):
    if not USE_CBL:
        return error_response("DLQ requires CBL", 501)
    dlq_id = request.match_info["id"]
    CBLStore().mark_dlq_retried(dlq_id)
    return json_response({"ok": True})


async def delete_dlq_entry(request):
    if not USE_CBL:
        return error_response("DLQ requires CBL", 501)
    dlq_id = request.match_info["id"]
    CBLStore().delete_dlq_entry(dlq_id)
    return json_response({"ok": True})


async def clear_dlq(request):
    if not USE_CBL:
        return error_response("DLQ requires CBL", 501)
    CBLStore().clear_dlq()
    return json_response({"ok": True})


async def dlq_count(request):
    if not USE_CBL:
        return json_response({"count": 0})
    return json_response({"count": CBLStore().dlq_count()})


async def dlq_meta(request):
    if not USE_CBL:
        return json_response(
            {
                "last_inserted_at": None,
                "last_drained_at": None,
                "last_inserted_job": None,
                "last_drained_job": None,
                "jobs": {},
            }
        )
    return json_response(CBLStore().get_dlq_meta())


async def dlq_explain(request):
    """Return EXPLAIN output for DLQ queries to verify index usage."""
    if not USE_CBL:
        return error_response("DLQ requires CBL", 501)
    return json_response(CBLStore().dlq_explain_queries())


async def replay_dlq(request):
    """Trigger a DLQ replay without requiring a full worker restart."""
    if not USE_CBL:
        return error_response("DLQ requires CBL", 501)
    store = CBLStore()
    stats = store.dlq_stats()
    pending = stats.get("pending", 0)
    if not pending:
        return json_response({"total": 0, "message": "no pending entries to replay"})
    return json_response(
        {
            "total": pending,
            "message": "use worker restart or POST /api/restart to trigger replay — on-demand replay requires the output forwarder context",
        }
    )


# --- Status API ---


async def get_status(request):
    """Return health status for dashboard indicators."""
    return json_response(
        {
            "cbl": "green" if USE_CBL else "red",
        }
    )


async def get_jobs(request):
    """GET /api/jobs — List all jobs with basic info for dropdown population.

    Returns:
    {
        "jobs": [
            {
                "id": "job::2162fb33-6213-456d-93c1-213a64654e59",
                "job_id": "job::2162fb33-6213-456d-93c1-213a64654e59",
                "name": "Job Name"
            }
        ],
        "count": 1
    }
    """
    if not USE_CBL:
        return json_response({"jobs": [], "count": 0})

    try:
        store = CBLStore()
        jobs = store.list_jobs()

        result_jobs = []
        for job in jobs:
            # Get the raw job ID (with job:: prefix if present)
            raw_job_id = job.get("doc_id") or job.get("_id") or ""
            # Ensure job:: prefix for consistency with logs
            if raw_job_id and not raw_job_id.startswith("job::"):
                job_id_with_prefix = f"job::{raw_job_id}"
            else:
                job_id_with_prefix = raw_job_id

            result_jobs.append(
                {
                    "id": job_id_with_prefix,
                    "job_id": job_id_with_prefix,
                    "name": job.get("name", raw_job_id),
                }
            )

        return json_response({"jobs": result_jobs, "count": len(result_jobs)})
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error listing jobs",
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return error_response(str(e), status=500)


async def get_jobs_status(request):
    """GET /api/jobs/status — Return list of all jobs with status, checkpoint, and metrics.

    Enriches stored job data with live pipeline state from the worker.

    Returns:
    {
        "jobs": [
            {
                "job_id": "job-123",
                "name": "Job Name",
                "enabled": true,
                "status": "running|idle|error|stopped",
                "uptime_seconds": 123.4,
                "last_sync_time": "2024-01-01T10:00:00Z",
                "docs_processed": 1234,
                "errors": 5
            }
        ],
        "count": 1
    }
    """
    if not USE_CBL:
        return json_response({"jobs": [], "count": 0})

    try:
        store = CBLStore()
        jobs = store.list_jobs()

        # Fetch live pipeline states from the worker
        live_states = {}
        try:
            worker_host = os.environ.get("METRICS_HOST")
            if worker_host:
                cfg = store.load_config() or {}
                port = cfg.get("metrics", {}).get("port", 9090)
                async with _aiohttp.ClientSession() as session:
                    for job in jobs:
                        jid = (
                            (job.get("doc_id") or job.get("_id") or "")
                            .replace("job::", "")
                            .replace("job:", "")
                        )
                        try:
                            url = f"http://{worker_host}:{port}/api/jobs/{jid}/state"
                            async with session.get(
                                url, timeout=_aiohttp.ClientTimeout(total=3)
                            ) as resp:
                                if resp.status == 200:
                                    live_states[jid] = await resp.json()
                        except Exception:
                            pass
        except Exception:
            pass

        result_jobs = []
        for job in jobs:
            job_id = (
                (job.get("doc_id") or job.get("_id") or "")
                .replace("job::", "")
                .replace("job:", "")
            )

            # Load checkpoint for this job
            checkpoint = store.load_checkpoint(job_id) or {}

            # Use live state if available, otherwise fall back to stored state
            live = live_states.get(job_id)
            if live:
                status = live.get("status", "unknown")
                uptime = live.get("uptime_seconds")
                error_count = live.get("error_count", 0)
            else:
                state = job.get("state", {})
                status = state.get("status", "idle")
                uptime = None
                error_count = 0

            # Extract input/source info
            inputs = job.get("inputs") or []
            first_input = inputs[0] if inputs else {}
            input_name = first_input.get("name") or first_input.get("id") or ""
            input_type = first_input.get("source_type") or ""

            # Extract output info
            outputs = job.get("outputs") or []
            first_output = outputs[0] if outputs else {}
            output_name = first_output.get("name") or first_output.get("id") or ""
            output_type = job.get("output_type") or ""

            # Extract threads
            system = job.get("system") or {}
            threads = system.get("threads") or job.get("threads") or 1

            status_entry = {
                "job_id": job_id,
                "name": job.get("name") or job_id,
                "enabled": job.get("enabled", True),
                "status": status,
                "uptime_seconds": uptime,
                "last_sync_time": checkpoint.get("updated_at")
                or checkpoint.get("timestamp"),
                "docs_processed": checkpoint.get("seq", 0),
                "errors": error_count,
                "input_name": input_name,
                "input_type": input_type,
                "output_name": output_name,
                "output_type": output_type,
                "threads": threads,
            }
            result_jobs.append(status_entry)

        return json_response({"jobs": result_jobs, "count": len(result_jobs)})
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error loading jobs status",
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return json_response({"jobs": [], "count": 0, "error": str(e)})


# --- Metrics API ---


async def get_metrics(request):
    try:
        if USE_CBL:
            cfg = CBLStore().load_config() or {}
        else:
            cfg = json.loads(CONFIG_PATH.read_text())
        m = cfg.get("metrics", {})
        if not m.get("enabled"):
            return json_response({"error": "metrics_disabled"})
        host = m.get("host", "127.0.0.1")
        # 0.0.0.0 is a bind address, not connectable;
        # In Docker Compose the worker is a separate service, use its
        # service name.  Fall back to loopback for local dev.
        import os

        worker_host = os.environ.get("METRICS_HOST")
        if worker_host:
            host = worker_host
        elif host in ("0.0.0.0", "::"):
            host = "127.0.0.1"
        port = m.get("port", 9090)
        import aiohttp

        url = f"http://{host}:{port}/_metrics"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                text = await resp.text()
                return web.Response(
                    text=text, content_type="text/plain", headers=cors_headers()
                )
    except Exception as exc:
        return json_response({"error": "metrics_unreachable", "detail": str(exc)})


# --- Maintenance API ---


async def post_maintenance(request):
    """POST /api/maintenance — Run CBL maintenance now (compact + optimize)."""
    if not USE_CBL:
        return error_response("CBL is not enabled", 503)
    try:
        store = CBLStore()
        results = {}
        results["compact"] = store.compact()
        results["reindex"] = store.reindex()
        results["optimize"] = store.optimize()
        all_ok = all(results.values())
        ops = ", ".join(k for k, v in results.items() if v)
        failed = ", ".join(k for k, v in results.items() if not v)
        if all_ok:
            log_event(
                logger,
                "info",
                "CBL",
                "manual maintenance completed (%s)" % ops,
                operation="MAINTENANCE",
                trigger="manual",
            )
        else:
            log_event(
                logger,
                "warn",
                "CBL",
                "manual maintenance partial (%s ok, %s failed)"
                % (ops or "none", failed),
                operation="MAINTENANCE",
                trigger="manual",
            )
        return json_response(
            {
                "ok": all_ok,
                "results": results,
                "message": "maintenance completed"
                if all_ok
                else "some operations failed",
            }
        )
    except Exception as exc:
        log_event(
            logger,
            "error",
            "CBL",
            "manual maintenance error: %s" % exc,
            operation="MAINTENANCE",
            trigger="manual",
        )
        return json_response({"ok": False, "error": str(exc)}, status=500)


# --- Worker Control API ---


async def post_restart(request):
    """Proxy POST to the worker's /_restart endpoint."""
    result = await _signal_worker_restart()
    if result == "ok":
        return json_response({"ok": True, "message": "restart signal sent"})
    return json_response({"ok": False, "error": result}, status=502)


async def post_shutdown(request):
    """Proxy POST to the worker's /_shutdown endpoint."""
    worker_host = os.environ.get("METRICS_HOST")
    if not worker_host:
        return json_response({"ok": False, "error": "skipped"}, status=400)
    try:
        cfg = (
            CBLStore().load_config() if USE_CBL else json.loads(CONFIG_PATH.read_text())
        )
        port = cfg.get("metrics", {}).get("port", 9090)
    except Exception:
        port = 9090
    url = f"http://{worker_host}:{port}/_shutdown"
    try:
        async with _aiohttp.ClientSession() as session:
            async with session.post(
                url, timeout=_aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    return json_response(
                        {"ok": True, "message": "shutdown signal sent"}
                    )
                return json_response(
                    {"ok": False, "error": f"HTTP {resp.status}"}, status=502
                )
    except Exception as exc:
        return json_response({"ok": False, "error": str(exc)}, status=502)


async def _worker_control(endpoint: str, method: str = "POST"):
    """Generic helper to proxy a request to the worker's metrics server."""
    worker_host = os.environ.get("METRICS_HOST")
    if not worker_host:
        return {"ok": False, "error": "skipped"}
    try:
        cfg = (
            CBLStore().load_config() if USE_CBL else json.loads(CONFIG_PATH.read_text())
        )
        port = cfg.get("metrics", {}).get("port", 9090)
    except Exception:
        port = 9090
    url = f"http://{worker_host}:{port}/{endpoint}"
    try:
        async with _aiohttp.ClientSession() as session:
            req = session.post if method == "POST" else session.get
            async with req(url, timeout=_aiohttp.ClientTimeout(total=5)) as resp:
                return await resp.json()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def post_offline(request):
    """Proxy POST to the worker's /_offline endpoint."""
    result = await _worker_control("_offline")
    status = 200 if result.get("ok") else 502
    return json_response(result, status=status)


async def post_online(request):
    """Proxy POST to the worker's /_online endpoint."""
    result = await _worker_control("_online")
    status = 200 if result.get("ok") else 502
    return json_response(result, status=status)


async def get_worker_status(request):
    """Proxy GET to the worker's /_status endpoint."""
    result = await _worker_control("_status", method="GET")
    return json_response(result)


# --- Job Control API (proxy to metrics server) ---


async def job_control_proxy(request, endpoint: str):
    """Generic proxy for job control endpoints to the metrics server."""
    try:
        worker_host = os.environ.get("METRICS_HOST")
        if not worker_host:
            return json_response({"error": "metrics_unreachable"}, status=502)

        cfg = (
            CBLStore().load_config() if USE_CBL else json.loads(CONFIG_PATH.read_text())
        )
        port = cfg.get("metrics", {}).get("port", 9090)
    except Exception:
        port = 9090

    url = f"http://{worker_host}:{port}/{endpoint}"
    log_event(logger, "debug", "CONTROL", "proxying request", url=url)
    try:
        # Increased timeout from 5s to 30s for job control operations
        # Job operations may take time due to thread pool executor and CBL operations
        async with _aiohttp.ClientSession() as session:
            async with session.post(
                url, timeout=_aiohttp.ClientTimeout(total=30)
            ) as resp:
                try:
                    data = await resp.json()
                except:
                    # If response isn't JSON, get text instead
                    text = await resp.text()
                    data = {"error": f"Invalid response from metrics server: {text}"}
                log_event(
                    logger, "debug", "CONTROL", "proxy response", status=resp.status
                )
                return json_response(data, status=resp.status)
    except _aiohttp.ClientConnectorError as exc:
        log_event(
            logger,
            "error",
            "CONTROL",
            "cannot connect to metrics server",
            url=url,
            error_detail="%s" % exc,
        )
        return json_response(
            {"error": f"Metrics server unreachable: {exc}"}, status=502
        )
    except asyncio.TimeoutError as exc:
        log_event(
            logger,
            "error",
            "CONTROL",
            "timeout calling metrics server",
            url=url,
            timeout_s=30,
        )
        return json_response(
            {"error": "Job operation timed out - may be slow"}, status=504
        )
    except Exception as exc:
        log_event(
            logger,
            "error",
            "CONTROL",
            "job control proxy error",
            error_detail="%s" % exc,
        )
        return json_response({"error": f"Proxy error: {str(exc)}"}, status=502)


async def post_job_start(request):
    """POST /api/jobs/{job_id}/start"""
    job_id = request.match_info.get("job_id")
    if not job_id:
        return json_response({"error": "job_id required"}, status=400)
    return await job_control_proxy(request, f"api/jobs/{job_id}/start")


async def post_job_stop(request):
    """POST /api/jobs/{job_id}/stop"""
    job_id = request.match_info.get("job_id")
    if not job_id:
        return json_response({"error": "job_id required"}, status=400)
    return await job_control_proxy(request, f"api/jobs/{job_id}/stop")


async def post_job_restart(request):
    """POST /api/jobs/{job_id}/restart"""
    job_id = request.match_info.get("job_id")
    if not job_id:
        return json_response({"error": "job_id required"}, status=400)
    return await job_control_proxy(request, f"api/jobs/{job_id}/restart")


async def post_job_kill(request):
    """POST /api/jobs/{job_id}/kill"""
    job_id = request.match_info.get("job_id")
    if not job_id:
        return json_response({"error": "job_id required"}, status=400)
    return await job_control_proxy(request, f"api/jobs/{job_id}/kill")


# --- Sample Doc API (fetch one doc from changes feed in dry-run mode) ---


async def get_sample_doc(request):
    """Fetch 100 docs from the changes feed and return one at random."""
    import random

    try:
        if USE_CBL:
            cfg = CBLStore().load_config() or {}
        else:
            cfg = json.loads(CONFIG_PATH.read_text())
        gw = cfg.get("gateway", {})
        auth_cfg = cfg.get("auth", {})
        url = gw.get("url", "").rstrip("/")
        db = gw.get("database", "")
        scope = gw.get("scope", "_default")
        collection = gw.get("collection", "_default")
        src = gw.get("src", "sync_gateway")

        if src == "sync_gateway":
            changes_url = f"{url}/{db}.{scope}.{collection}/_changes"
        else:
            changes_url = f"{url}/{db}/_changes"

        params = {"limit": "100", "include_docs": "true", "since": "0"}

        import aiohttp as _aiohttp

        ssl_ctx = None
        if gw.get("accept_self_signed_certs"):
            import ssl

            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE

        headers = {}
        method = auth_cfg.get("method", "none")
        basic_auth = None
        if method == "basic" and auth_cfg.get("username"):
            basic_auth = _aiohttp.BasicAuth(
                auth_cfg["username"], auth_cfg.get("password", "")
            )
        elif method == "bearer" and auth_cfg.get("bearer_token"):
            headers["Authorization"] = f"Bearer {auth_cfg['bearer_token']}"
        elif method == "session" and auth_cfg.get("session_cookie"):
            headers["Cookie"] = f"SyncGatewaySession={auth_cfg['session_cookie']}"

        connector = (
            _aiohttp.TCPConnector(ssl=ssl_ctx) if ssl_ctx else _aiohttp.TCPConnector()
        )
        async with _aiohttp.ClientSession(connector=connector) as session:
            async with session.get(
                changes_url,
                params=params,
                auth=basic_auth,
                headers=headers,
                timeout=_aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                results = data.get("results", [])
                if not results:
                    # Connection ok, feed empty. Surface as a warning
                    # rather than an error so the GUI does not falsely
                    # report a connection failure.
                    return json_response(
                        {
                            "ok": True,
                            "warning": "no_docs",
                            "detail": "Connected, but the changes feed has no documents.",
                            "pool_size": 0,
                        }
                    )
                pick = random.choice(results)
                doc = pick.get("doc", pick)
                return json_response({"doc": doc, "pool_size": len(results)})
    except Exception as exc:
        return json_response({"error": "fetch_failed", "detail": str(exc)}, status=500)


# --- DB Introspection API ---

# Supported RDBMS drivers (auto-detected based on what's installed)
_DB_DRIVERS = {}


def _detect_db_drivers():
    """Check which RDBMS drivers are installed."""
    global _DB_DRIVERS
    _DB_DRIVERS = {}
    try:
        import asyncpg  # noqa: F401

        _DB_DRIVERS["postgres"] = {"name": "PostgreSQL", "driver": "asyncpg"}
    except ImportError:
        pass
    try:
        import aiomysql  # noqa: F401

        _DB_DRIVERS["mysql"] = {"name": "MySQL", "driver": "aiomysql"}
    except ImportError:
        pass
    try:
        import aioodbc  # noqa: F401

        _DB_DRIVERS["mssql"] = {"name": "SQL Server", "driver": "aioodbc"}
    except ImportError:
        pass
    try:
        import oracledb  # noqa: F401

        _DB_DRIVERS["oracle"] = {"name": "Oracle", "driver": "oracledb"}
    except ImportError:
        pass
    return _DB_DRIVERS


_detect_db_drivers()


async def list_db_drivers(request):
    """Return which RDBMS drivers are installed and available."""
    drivers = _detect_db_drivers()
    return json_response({"drivers": drivers})


async def db_introspect(request):
    """
    Connect to an RDBMS and return all tables + columns.
    POST body: {"db_type": "postgres", "host": "...", "port": 5432, ...}
    """
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return error_response("Invalid JSON")

    db_type = body.get("db_type", "")
    if db_type not in _DB_DRIVERS:
        installed = list(_DB_DRIVERS.keys())
        return error_response(
            f"Unknown or unavailable db_type '{db_type}'. "
            f"Installed drivers: {installed}"
        )

    try:
        if db_type == "postgres":
            from db.db_postgres import introspect_tables

            tables = await introspect_tables(body)
            return json_response({"tables": tables})
        elif db_type == "mysql":
            from db.db_mysql import introspect_tables as mysql_introspect

            tables = await mysql_introspect(body)
            return json_response({"tables": tables})
        elif db_type == "mssql":
            from db.db_mssql import introspect_tables as mssql_introspect

            tables = await mssql_introspect(body)
            return json_response({"tables": tables})
        elif db_type == "oracle":
            from db.db_oracle import introspect_tables as ora_introspect

            tables = await ora_introspect(body)
            return json_response({"tables": tables})
        else:
            return error_response(
                f"Introspection not yet implemented for {db_type}", 501
            )
    except Exception as exc:
        return json_response(
            {"error": "introspect_failed", "detail": str(exc)}, status=500
        )


async def db_test_connection(request):
    """Test connectivity to an RDBMS. POST body same as introspect."""
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return error_response("Invalid JSON")

    db_type = body.get("db_type", "")
    if db_type not in _DB_DRIVERS:
        return error_response(f"Driver not installed for '{db_type}'")

    try:
        if db_type == "postgres":
            import asyncpg

            ssl_ctx = None
            if body.get("ssl"):
                import ssl as _ssl

                ssl_ctx = _ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = _ssl.CERT_NONE
            conn = await asyncpg.connect(
                host=body.get("host", "localhost"),
                port=body.get("port", 5432),
                database=body.get("database", ""),
                user=body.get("username") or body.get("user", "postgres"),
                password=body.get("password", ""),
                ssl=ssl_ctx,
            )
            ver = await conn.fetchval("SELECT version()")
            await conn.close()
            return json_response({"ok": True, "version": ver})
        elif db_type == "mysql":
            import aiomysql as _aiomysql

            ssl_ctx = None
            if body.get("ssl"):
                import ssl as _ssl

                ssl_ctx = _ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = _ssl.CERT_NONE
            conn = await _aiomysql.connect(
                host=body.get("host", "localhost"),
                port=body.get("port", 3306),
                db=body.get("database", ""),
                user=body.get("username") or body.get("user", "root"),
                password=body.get("password", ""),
                ssl=ssl_ctx,
            )
            cur = await conn.cursor()
            await cur.execute("SELECT version()")
            row = await cur.fetchone()
            ver = row[0] if row else "MySQL (version unknown)"
            await cur.close()
            conn.close()
            return json_response({"ok": True, "version": ver})
        elif db_type == "mssql":
            import aioodbc as _aioodbc

            driver = body.get("odbc_driver", "ODBC Driver 18 for SQL Server")
            host = body.get("host", "localhost")
            port = body.get("port", 1433)
            dsn_parts = [
                f"DRIVER={{{driver}}}",
                f"SERVER={host},{port}",
                f"DATABASE={body.get('database', '')}",
                f"UID={body.get('username') or body.get('user', 'sa')}",
                f"PWD={body.get('password', '')}",
            ]
            if body.get("trust_server_certificate", True):
                dsn_parts.append("TrustServerCertificate=yes")
            conn = await _aioodbc.connect(dsn=";".join(dsn_parts))
            cur = await conn.cursor()
            await cur.execute("SELECT @@VERSION")
            row = await cur.fetchone()
            ver = row[0] if row else "SQL Server (version unknown)"
            await cur.close()
            await conn.close()
            return json_response({"ok": True, "version": ver})
        elif db_type == "oracle":
            import oracledb as _oracledb

            dsn = body.get("dsn", "")
            if not dsn:
                host = body.get("host", "localhost")
                port = body.get("port", 1521)
                database = body.get("database", "")
                dsn = f"{host}:{port}/{database}"
            conn = await _oracledb.connect_async(
                user=body.get("username") or body.get("user", ""),
                password=body.get("password", ""),
                dsn=dsn,
            )
            cur = conn.cursor()
            await cur.execute("SELECT banner FROM v$version WHERE ROWNUM = 1")
            row = await cur.fetchone()
            ver = row[0] if row else "Oracle (version unknown)"
            await cur.close()
            await conn.close()
            return json_response({"ok": True, "version": ver})
        else:
            return error_response(f"Test not yet implemented for {db_type}", 501)
    except Exception as exc:
        return json_response({"ok": False, "error": str(exc)}, status=200)


async def parse_ddl(request):
    """
    Parse a CREATE TABLE DDL statement and return column definitions.
    POST body: {"ddl": "CREATE TABLE orders (id INT PRIMARY KEY, ...)"}
    Supports Postgres/MySQL/MSSQL/Oracle syntax.
    """
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return error_response("Invalid JSON")

    ddl = body.get("ddl", "").strip()
    if not ddl:
        return error_response("No DDL provided")

    try:
        tables = _parse_create_tables(ddl)
        return json_response({"tables": tables})
    except Exception as exc:
        return json_response({"error": "parse_failed", "detail": str(exc)}, status=400)


def _parse_create_tables(ddl: str) -> list[dict]:
    """
    Parse one or more CREATE TABLE statements from DDL text.
    Returns a list of table definitions compatible with the mapping format.
    """
    import re

    results = []
    # Find CREATE TABLE header, then extract balanced parentheses body
    header_re = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r'(?:`|"|\[)?(\w+)(?:`|"|\])?\s*'  # table name or schema
        r'(?:\.(?:`|"|\[)?(\w+)(?:`|"|\])?\s*)?'  # optional .table
        r"\(",
        re.IGNORECASE,
    )

    for m in header_re.finditer(ddl):
        table_name = m.group(2) or m.group(1)
        # Extract balanced parentheses body starting after the opening '('
        start = m.end()
        depth = 1
        pos = start
        while pos < len(ddl) and depth > 0:
            if ddl[pos] == "(":
                depth += 1
            elif ddl[pos] == ")":
                depth -= 1
            pos += 1
        body = ddl[start : pos - 1].strip()

        columns = []
        pk_cols = []

        # Split on commas, but respect parentheses (for types like NUMERIC(10,2))
        parts = _split_ddl_body(body)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # Check for PRIMARY KEY constraint
            pk_match = re.match(
                r"(?:CONSTRAINT\s+\w+\s+)?PRIMARY\s+KEY\s*\((.+?)\)",
                part,
                re.IGNORECASE,
            )
            if pk_match:
                pk_cols = [
                    c.strip().strip('`"[]') for c in pk_match.group(1).split(",")
                ]
                continue

            # Check for FOREIGN KEY / other constraints — skip
            if re.match(
                r"(?:CONSTRAINT|FOREIGN\s+KEY|UNIQUE|CHECK|INDEX)",
                part,
                re.IGNORECASE,
            ):
                continue

            # Parse column: name type [NOT NULL] [DEFAULT ...] [PRIMARY KEY] ...
            col_match = re.match(
                r'(?:`|"|\[)?(\w+)(?:`|"|\])?\s+'
                r"([\w]+(?:\s*\([^)]*\))?(?:\s+(?:UNSIGNED|VARYING|PRECISION|WITHOUT\s+TIME\s+ZONE|WITH\s+TIME\s+ZONE))*)",
                part,
                re.IGNORECASE,
            )
            if not col_match:
                continue

            col_name = col_match.group(1)
            col_type = col_match.group(2).strip()

            nullable = "NOT NULL" not in part.upper()

            if re.search(r"PRIMARY\s+KEY", part, re.IGNORECASE):
                pk_cols.append(col_name)

            columns.append(
                {
                    "name": col_name,
                    "type": col_type.lower(),
                    "display_type": col_type.lower(),
                    "nullable": nullable,
                    "default": None,
                }
            )

        results.append(
            {
                "table_name": table_name,
                "columns": columns,
                "primary_key": pk_cols,
                "foreign_keys": [],
            }
        )

    if not results:
        raise ValueError("No CREATE TABLE statements found in DDL")

    return results


def _split_ddl_body(body: str) -> list[str]:
    """Split DDL column definitions on commas, respecting parentheses."""
    parts = []
    depth = 0
    current = []
    for ch in body:
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


# --- Auto-Map API ---


async def auto_map_columns(request):
    """
    Score-based auto-mapping of JSON source fields to SQL table columns.
    POST body: {
      "source_fields": [{"path": "$.order_date", "type": "string", "sample": "2024-09-15"}, ...],
      "tables": [{"name": "orders", "columns": {"order_date": "date", "total": "numeric(10,2)"}}]
    }
    Returns: {"mappings": {"orders": {"order_date": "$.order_date", ...}}}
    """
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return error_response("Invalid JSON")

    src_fields = body.get("source_fields", [])
    table_defs = body.get("tables", [])
    if not src_fields or not table_defs:
        return error_response("source_fields and tables are required")

    result = _auto_map(src_fields, table_defs)
    return json_response({"mappings": result})


def _auto_map(src_fields: list, table_defs: list) -> dict:
    import re
    import difflib

    def norm_tokens(name: str) -> list[str]:
        s = re.sub(r"([a-z])([A-Z])", r"\1_\2", name).lower()
        return [t for t in re.split(r"[^a-z0-9]+", s) if t]

    def norm_compact(name: str) -> str:
        return "".join(norm_tokens(name))

    # Synonym dictionary for semantic matching without ML
    SYNONYMS: dict[str, list[str]] = {
        "cost": ["price", "amount", "fee", "rate", "charge"],
        "price": ["cost", "amount", "fee", "rate", "charge"],
        "amount": ["price", "cost", "total", "sum", "fee"],
        "total": ["amount", "sum"],
        "vendor": ["supplier", "provider", "merchant", "seller"],
        "supplier": ["vendor", "provider", "merchant"],
        "customer": ["client", "buyer", "patron", "account"],
        "client": ["customer", "buyer", "patron"],
        "user": ["account", "member", "person", "owner"],
        "name": ["title", "label"],
        "description": ["desc", "summary", "note", "details", "comment", "memo"],
        "desc": ["description", "summary", "note"],
        "created": ["created_at", "creation_date", "date_created"],
        "updated": ["updated_at", "modified", "modified_at", "last_modified"],
        "id": ["identifier", "key", "pk", "ref"],
        "uuid": ["guid", "unique_id", "external_id"],
        "email": ["mail", "email_address"],
        "phone": ["tel", "telephone", "mobile", "cell", "fax"],
        "qty": ["quantity", "count", "num", "units"],
        "quantity": ["qty", "count", "num", "units"],
        "status": ["state", "condition", "flag"],
        "active": ["enabled", "is_active"],
        "category": ["type", "kind", "class", "group"],
        "type": ["kind", "category", "class"],
        "city": ["town", "municipality", "locality"],
        "zip": ["postal", "postal_code", "zipcode", "postcode"],
        "country": ["nation", "country_code"],
        "url": ["uri", "link", "href", "website"],
        "image": ["img", "photo", "picture", "thumbnail", "avatar"],
        "product": ["item", "sku", "goods", "article"],
        "order": ["purchase", "transaction", "booking"],
        "comment": ["remark", "feedback", "review", "note"],
    }

    SEMANTIC_GROUPS = [
        {"id", "key", "identifier", "uuid", "guid"},
        {"date", "time", "timestamp", "created", "updated", "modified", "datetime"},
        {"name", "title", "label", "display"},
        {"email", "mail"},
        {"phone", "tel", "mobile", "fax"},
        {"price", "cost", "amount", "total", "fee", "rate"},
        {"qty", "quantity", "count", "num", "number"},
        {"status", "state", "flag", "active", "enabled"},
        {"desc", "description", "note", "notes", "comment", "summary"},
        {"address", "street", "city", "zip", "postal", "country", "region"},
    ]

    FALSE_FRIENDS = [
        ("id", "uuid"),
        ("created", "updated"),
        ("start", "end"),
        ("first", "last"),
        ("source", "target"),
        ("min", "max"),
        ("from", "to"),
        ("width", "height"),
        ("latitude", "longitude"),
    ]

    # Sniff patterns for data profiling
    SNIFF_PATTERNS = [
        (
            "uuid",
            re.compile(
                r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
            ),
        ),
        ("iso_date", re.compile(r"^\d{4}-\d{2}-\d{2}")),
        ("email", re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")),
        ("url", re.compile(r"^https?://", re.I)),
        ("phone", re.compile(r"^[+]?\d[\d\s\-()\.]{6,}$")),
        ("bool_str", re.compile(r"^(true|false|yes|no|on|off|0|1)$", re.I)),
    ]
    SNIFF_AFFINITY: dict[str, list[str]] = {
        "uuid": ["uuid", "varchar", "text", "char"],
        "iso_date": ["date", "timestamp", "datetime"],
        "email": ["varchar", "text"],
        "url": ["varchar", "text"],
        "phone": ["varchar", "text"],
        "bool_str": ["boolean", "bool", "bit", "smallint"],
    }

    def sniff_value(sample: str) -> str | None:
        if not sample:
            return None
        for name, pat in SNIFF_PATTERNS:
            if pat.match(sample):
                return name
        return None

    def sniff_type_boost(sample: str, sql_type: str) -> float:
        if not sql_type:
            return 0.0
        fmt = sniff_value(sample)
        if not fmt:
            return 0.0
        affinities = SNIFF_AFFINITY.get(fmt, [])
        st = sql_type.lower()
        for a in affinities:
            if a in st:
                return 0.2
        if fmt == "iso_date" and re.search(r"int|serial", st):
            return -0.15
        if fmt == "uuid" and re.search(r"int|serial|numeric", st):
            return -0.15
        return 0.0

    def synonym_score(col: str, field_leaf: str) -> float:
        ct = norm_tokens(col)
        ft = norm_tokens(field_leaf)
        best = 0.0
        for c in ct:
            for f in ft:
                if c == f:
                    best = max(best, 1.0)
                    continue
                syns = SYNONYMS.get(c, [])
                for syn in syns:
                    if f in norm_tokens(syn):
                        best = max(best, 0.85)
                        break
        return best

    def name_similarity(col: str, field_leaf: str) -> float:
        cc = norm_compact(col)
        fc = norm_compact(field_leaf)
        if not cc or not fc:
            return 0.0
        if cc == fc:
            return 1.0
        ratio = difflib.SequenceMatcher(None, cc, fc).ratio()
        ct = set(norm_tokens(col))
        ft = set(norm_tokens(field_leaf))
        if ct and ft:
            overlap = len(ct & ft) / max(len(ct), len(ft))
            ratio = max(ratio, 0.5 + 0.4 * overlap)
        return ratio

    def type_compat(json_type: str, sample: str, sql_type: str) -> float:
        if not sql_type:
            return 0.5
        st = sql_type.lower()
        if json_type == "string":
            if re.search(r"varchar|text|char|json", st):
                return 0.8
            if re.search(r"date|timestamp|time", st):
                return 1.3 if (sample and re.match(r"\d{4}-\d{2}", sample)) else 0.6
            return 0.3
        if json_type == "number":
            return (
                1.0
                if re.search(r"int|numeric|decimal|float|double|real|serial|money", st)
                else 0.3
            )
        if json_type == "boolean":
            return 1.2 if "bool" in st else (0.7 if re.search(r"int|bit", st) else 0.3)
        return 0.3

    def semantic_group_boost(col: str, field_leaf: str) -> float:
        ct = set(norm_tokens(col))
        ft = set(norm_tokens(field_leaf))
        for group in SEMANTIC_GROUPS:
            if ct & group and ft & group:
                return 0.1
        return 0.0

    def path_context_score(col: str, field_path: str, tbl_name: str) -> float:
        col_toks = norm_tokens(col)
        segs = field_path.lstrip("$.").split(".")
        if len(segs) < 2:
            return 0.0
        parent_toks = []
        for s in segs[:-1]:
            parent_toks.extend(norm_tokens(s.replace("[]", "")))
        score = 0.0
        for ct in col_toks:
            for pt in parent_toks:
                if ct == pt:
                    score += 0.12
                elif len(ct) > 2 and len(pt) > 2 and (ct in pt or pt in ct):
                    score += 0.06
                syns = SYNONYMS.get(ct, [])
                for syn in syns:
                    if pt in norm_tokens(syn):
                        score += 0.08
                        break
        tbl_toks = norm_tokens(tbl_name)
        for pt in parent_toks:
            if pt in tbl_toks:
                score += 0.05
        return min(score, 0.25)

    def false_friend_penalty(col: str, field_leaf: str) -> float:
        ct = norm_tokens(col)
        ft = norm_tokens(field_leaf)
        for a, b in FALSE_FRIENDS:
            col0, col1 = a in ct, b in ct
            fld0, fld1 = a in ft, b in ft
            if (col0 and fld1 and not fld0) or (col1 and fld0 and not fld1):
                return -0.3
        return 0.0

    # Build field list
    fields = []
    for f in src_fields:
        if f.get("type") in ("object", "array"):
            continue
        path = f.get("path", "")
        segments = path.split(".")
        leaf = segments[-1].replace("[]", "") if segments else ""
        if leaf:
            fields.append(
                {
                    "path": path,
                    "leaf": leaf,
                    "type": f.get("type", "string"),
                    "sample": f.get("sample", ""),
                }
            )

    result = {}
    for tbl in table_defs:
        tbl_name = tbl.get("name", "")
        col_types = tbl.get("columns", {})
        candidates = []
        for col_name, sql_type in col_types.items():
            for fld in fields:
                ns = name_similarity(col_name, fld["leaf"])
                syn = synonym_score(col_name, fld["leaf"])
                best_name = max(ns, syn)
                tc = type_compat(fld["type"], fld["sample"], sql_type or "")
                sniff = sniff_type_boost(fld["sample"], sql_type or "")
                sem = semantic_group_boost(col_name, fld["leaf"])
                path_ctx = path_context_score(col_name, fld["path"], tbl_name)
                ff = false_friend_penalty(col_name, fld["leaf"])
                total = 0.45 * best_name + 0.2 * tc + sniff + sem + path_ctx + ff
                if total >= 0.4:
                    candidates.append(
                        {"col": col_name, "path": fld["path"], "score": round(total, 4)}
                    )

        candidates.sort(key=lambda c: c["score"], reverse=True)
        used_cols: set[str] = set()
        used_paths: set[str] = set()
        mapping = {}
        for c in candidates:
            if c["col"] not in used_cols and c["path"] not in used_paths:
                mapping[c["col"]] = {"path": c["path"], "confidence": c["score"]}
                used_cols.add(c["col"])
                used_paths.add(c["path"])
        if mapping:
            result[tbl_name] = mapping

    return result


# --- Wizard API ---


async def wizard_test_source(request):
    """Test connectivity to SG/App Services/Edge Server and return a random sample doc.

    Every test attempt is logged via the GUIDE_LOGGING.md ``[CONTROL]`` /
    ``[HTTP]`` log keys with the **full URL**, auth method, status code,
    elapsed time, and error class so operators can grep the Logs page to
    diagnose failures.  The full URL is also echoed in every JSON response
    so the GUI can display "we tried this exact URL" on success or failure.
    """
    import random
    import time as _time

    try:
        body = await request.json()
    except json.JSONDecodeError:
        log_event(
            logger,
            "warn",
            "CONTROL",
            "test-source: invalid JSON body",
        )
        return error_response("Invalid JSON")

    gw = body.get("gateway", {})
    auth_cfg = body.get("auth", {})
    url = gw.get("url", "").rstrip("/")
    db = gw.get("database", "")
    scope = gw.get("scope", "_default")
    collection = gw.get("collection", "_default")
    src = gw.get("src", "sync_gateway")

    if not url or not db:
        log_event(
            logger,
            "warn",
            "CONTROL",
            "test-source: missing url or database",
            mode=src,
        )
        return error_response("URL and database are required")

    # All Couchbase sources use keyspace format: db.scope.collection
    if scope and scope != "_default" and collection and collection != "_default":
        changes_url = f"{url}/{db}.{scope}.{collection}/_changes"
    elif collection and collection != "_default":
        changes_url = f"{url}/{db}.{collection}/_changes"
    else:
        changes_url = f"{url}/{db}/_changes"

    params = {"limit": "100", "include_docs": "true", "since": "0"}
    auth_method = auth_cfg.get("method", "none")

    log_event(
        logger,
        "info",
        "CONTROL",
        "test-source: begin (auth=%s, src=%s)" % (auth_method, src),
        http_method="GET",
        url=changes_url,
        mode=src,
    )

    import aiohttp as _aiohttp

    ssl_ctx = None
    if gw.get("accept_self_signed_certs"):
        import ssl

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    headers = {}
    basic_auth = None
    if auth_method == "basic" and auth_cfg.get("username"):
        basic_auth = _aiohttp.BasicAuth(
            auth_cfg["username"], auth_cfg.get("password", "")
        )
    elif auth_method == "bearer" and auth_cfg.get("bearer_token"):
        headers["Authorization"] = f"Bearer {auth_cfg['bearer_token']}"
    elif auth_method == "session" and auth_cfg.get("session_cookie"):
        headers["Cookie"] = f"SyncGatewaySession={auth_cfg['session_cookie']}"

    t0 = _time.monotonic()
    try:
        connector = (
            _aiohttp.TCPConnector(ssl=ssl_ctx) if ssl_ctx else _aiohttp.TCPConnector()
        )
        async with _aiohttp.ClientSession(connector=connector) as session:
            async with session.get(
                changes_url,
                params=params,
                auth=basic_auth,
                headers=headers,
                timeout=_aiohttp.ClientTimeout(total=10),
            ) as resp:
                elapsed_ms = (_time.monotonic() - t0) * 1000.0
                # Log the response — even a 4xx/5xx is "got something back"
                if 400 <= resp.status < 600:
                    body_preview = ""
                    try:
                        body_preview = (await resp.text())[:200]
                    except Exception:
                        pass
                    log_event(
                        logger,
                        "warn",
                        "HTTP",
                        "test-source: got HTTP %d" % resp.status,
                        http_method="GET",
                        url=changes_url,
                        status=resp.status,
                        elapsed_ms=round(elapsed_ms, 1),
                        mode=src,
                        error_detail=body_preview,
                    )
                    return json_response(
                        {
                            "error": "http_%d" % resp.status,
                            "detail": body_preview or ("HTTP %d" % resp.status),
                            "url": changes_url,
                            "status": resp.status,
                            "auth_method": auth_method,
                            "elapsed_ms": round(elapsed_ms, 1),
                        },
                        status=200,
                    )
                try:
                    data = await resp.json()
                except Exception as exc:
                    log_event(
                        logger,
                        "error",
                        "HTTP",
                        "test-source: response was not JSON",
                        http_method="GET",
                        url=changes_url,
                        status=resp.status,
                        elapsed_ms=round(elapsed_ms, 1),
                        mode=src,
                        error_class=type(exc).__name__,
                        error_detail=str(exc)[:200],
                    )
                    return json_response(
                        {
                            "error": "invalid_response",
                            "detail": "Response was not JSON: %s" % exc,
                            "url": changes_url,
                            "status": resp.status,
                            "auth_method": auth_method,
                        },
                        status=200,
                    )
                results = data.get("results", [])
                if not results:
                    log_event(
                        logger,
                        "warn",
                        "CONTROL",
                        "test-source: connected but no docs in feed",
                        http_method="GET",
                        url=changes_url,
                        status=resp.status,
                        elapsed_ms=round(elapsed_ms, 1),
                        mode=src,
                    )
                    # Connection succeeded — the changes feed is just
                    # empty. Treat as ok so the GUI does not show
                    # "Connection failed" for what is really a healthy
                    # but empty source.
                    return json_response(
                        {
                            "ok": True,
                            "warning": "no_docs",
                            "detail": (
                                "Connected successfully (HTTP %d) but the "
                                "changes feed has no documents to sample."
                            )
                            % resp.status,
                            "url": changes_url,
                            "status": resp.status,
                            "auth_method": auth_method,
                            "elapsed_ms": round(elapsed_ms, 1),
                            "pool_size": 0,
                        }
                    )
                pick = random.choice(results)
                doc = pick.get("doc", pick)
                log_event(
                    logger,
                    "info",
                    "CONTROL",
                    "test-source: ok (%d docs sampled)" % len(results),
                    http_method="GET",
                    url=changes_url,
                    status=resp.status,
                    elapsed_ms=round(elapsed_ms, 1),
                    mode=src,
                    doc_count=len(results),
                )
                return json_response(
                    {
                        "ok": True,
                        "doc": doc,
                        "pool_size": len(results),
                        "url": changes_url,
                        "status": resp.status,
                        "auth_method": auth_method,
                        "elapsed_ms": round(elapsed_ms, 1),
                    }
                )
    except _aiohttp.ClientConnectorError as exc:
        elapsed_ms = (_time.monotonic() - t0) * 1000.0
        # Auto-diagnose common scheme/cert misconfigurations so the
        # operator doesn't have to read mbedTLS errors.
        hint = None
        s = str(exc).lower()
        if "ssl" in s or "certificate" in s or "tls" in s:
            if not gw.get("accept_self_signed_certs"):
                hint = (
                    "TLS/SSL handshake failed. If the source uses a "
                    "self-signed certificate (e.g. local Couchbase Edge "
                    "Server), enable 'Accept self-signed certs' on this "
                    "input."
                )
            else:
                hint = (
                    "TLS handshake failed even with self-signed certs "
                    "accepted. Check that the URL scheme matches the "
                    "server (https vs http) and the port is correct."
                )
        elif changes_url.startswith("http://"):
            hint = (
                "Connection refused/dropped on http://. If the server "
                "is HTTPS-only (Couchbase Edge Server defaults to TLS), "
                "change the URL to https://."
            )
        log_event(
            logger,
            "error",
            "HTTP",
            "test-source: connect failed (DNS/refused/SSL)"
            + (" — %s" % hint if hint else ""),
            http_method="GET",
            url=changes_url,
            elapsed_ms=round(elapsed_ms, 1),
            mode=src,
            error_class=type(exc).__name__,
            error_detail=str(exc)[:300],
        )
        return json_response(
            {
                "error": "connect_failed",
                "detail": str(exc),
                "hint": hint,
                "url": changes_url,
                "auth_method": auth_method,
                "error_class": type(exc).__name__,
                "elapsed_ms": round(elapsed_ms, 1),
            },
            status=200,
        )
    except _aiohttp.ClientError as exc:
        # Catches ServerDisconnectedError, ClientPayloadError,
        # ClientResponseError, etc.  This is what fires when you point
        # http:// at an https-only port and the server resets/disconnects
        # mid-stream (the symptom that used to fall through to
        # "fetch_failed").
        elapsed_ms = (_time.monotonic() - t0) * 1000.0
        hint = None
        if changes_url.startswith("http://"):
            hint = (
                "The server closed the connection. If the source is "
                "HTTPS-only (e.g. Couchbase Edge Server with TLS), change "
                "the URL scheme to https:// and enable 'Accept self-signed "
                "certs' if using a self-signed cert."
            )
        log_event(
            logger,
            "error",
            "HTTP",
            "test-source: client error (%s)" % type(exc).__name__
            + (" — %s" % hint if hint else ""),
            http_method="GET",
            url=changes_url,
            elapsed_ms=round(elapsed_ms, 1),
            mode=src,
            error_class=type(exc).__name__,
            error_detail=str(exc)[:300],
        )
        return json_response(
            {
                "error": "client_error",
                "detail": str(exc) or type(exc).__name__,
                "hint": hint,
                "url": changes_url,
                "auth_method": auth_method,
                "error_class": type(exc).__name__,
                "elapsed_ms": round(elapsed_ms, 1),
            },
            status=200,
        )
    except asyncio.TimeoutError as exc:
        elapsed_ms = (_time.monotonic() - t0) * 1000.0
        log_event(
            logger,
            "error",
            "HTTP",
            "test-source: timeout (10s)",
            http_method="GET",
            url=changes_url,
            elapsed_ms=round(elapsed_ms, 1),
            mode=src,
            error_class=type(exc).__name__,
            error_detail="Connection timed out after 10s",
        )
        return json_response(
            {
                "error": "timeout",
                "detail": "Connection timed out after 10s",
                "url": changes_url,
                "auth_method": auth_method,
                "error_class": "TimeoutError",
                "elapsed_ms": round(elapsed_ms, 1),
            },
            status=200,
        )
    except Exception as exc:
        elapsed_ms = (_time.monotonic() - t0) * 1000.0
        log_event(
            logger,
            "error",
            "HTTP",
            "test-source: unexpected error",
            http_method="GET",
            url=changes_url,
            elapsed_ms=round(elapsed_ms, 1),
            mode=src,
            error_class=type(exc).__name__,
            error_detail=str(exc)[:300],
        )
        return json_response(
            {
                "error": "fetch_failed",
                "detail": str(exc),
                "url": changes_url,
                "auth_method": auth_method,
                "error_class": type(exc).__name__,
                "elapsed_ms": round(elapsed_ms, 1),
            },
            status=500,
        )


async def wizard_test_output(request):
    """Test connectivity to an HTTP output endpoint."""
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return error_response("Invalid JSON")

    target_url = body.get("target_url", "").strip()
    if not target_url:
        return error_response("target_url is required")

    import aiohttp as _aiohttp

    ssl_ctx = None
    if body.get("accept_self_signed_certs"):
        import ssl

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    headers = {}
    auth_cfg = body.get("auth", {})
    method = auth_cfg.get("method", "none")
    basic_auth = None
    if method == "basic" and auth_cfg.get("username"):
        basic_auth = _aiohttp.BasicAuth(
            auth_cfg["username"], auth_cfg.get("password", "")
        )
    elif method == "bearer" and auth_cfg.get("bearer_token"):
        headers["Authorization"] = f"Bearer {auth_cfg['bearer_token']}"

    try:
        connector = (
            _aiohttp.TCPConnector(ssl=ssl_ctx) if ssl_ctx else _aiohttp.TCPConnector()
        )
        async with _aiohttp.ClientSession(connector=connector) as session:
            async with session.head(
                target_url,
                auth=basic_auth,
                headers=headers,
                timeout=_aiohttp.ClientTimeout(total=10),
                allow_redirects=True,
            ) as resp:
                return json_response(
                    {
                        "ok": True,
                        "status": resp.status,
                        "content_type": resp.headers.get("Content-Type", ""),
                    }
                )
    except Exception as exc:
        return json_response({"ok": False, "error": str(exc)}, status=200)


# --- Validate Mapping ---


async def validate_mapping(request):
    try:
        body = await request.json()
    except Exception:
        return error_response("Invalid JSON body")

    mapping = body.get("mapping")
    doc = body.get("doc")
    is_delete = bool(body.get("is_delete", False))
    if mapping is None or doc is None:
        return error_response("Both 'mapping' and 'doc' are required")

    try:
        mapper = SchemaMapper(mapping)
        matched = mapper.matches(doc)
        if not matched:
            return json_response({"matches": False, "ops": []})

        ops, _diag = mapper.map_document(doc, is_delete=is_delete)
        grouped = group_insert_ops(ops)
        result_ops = []
        for op in grouped:
            if isinstance(op, _MultiRowInsert):
                sql, params = PostgresOutputForwarder._multi_row_insert_sql(op)
                safe_params = []
                for p in params:
                    if isinstance(p, (datetime.date, datetime.datetime)):
                        safe_params.append(str(p))
                    else:
                        safe_params.append(p)
                result_ops.append(
                    {
                        "type": "INSERT",
                        "table": op.table,
                        "sql": sql,
                        "params": params,
                        "params_display": safe_params,
                        "multi_row": len(op.rows),
                    }
                )
            else:
                sql, params = op.to_sql()
                safe_params = []
                for p in params:
                    if isinstance(p, (datetime.date, datetime.datetime)):
                        safe_params.append(str(p))
                    else:
                        safe_params.append(p)
                result_ops.append(
                    {
                        "type": op.op_type,
                        "table": op.table,
                        "sql": sql,
                        "params": params,
                        "params_display": safe_params,
                    }
                )

        # Run EXPLAIN on each statement against the real database
        explain_results = await _explain_ops(result_ops)
        for ro, ex in zip(result_ops, explain_results):
            ro["explain"] = ex
            # Replace params with display-safe version for JSON response
            ro["params"] = ro.pop("params_display")

        all_ok = all(e.get("ok") for e in explain_results)
        return json_response(
            {
                "matches": True,
                "ops": result_ops,
                "explain_ok": all_ok,
                "original_op_count": len(ops),
                "grouped_op_count": len(grouped),
            }
        )
    except Exception as exc:
        return error_response(str(exc), status=500)


async def _explain_ops(ops: list[dict]) -> list[dict]:
    """Run EXPLAIN on each SQL statement against the configured database.

    Returns a list of dicts with ``ok``, ``plan`` (on success), or ``error``
    (on failure) for each operation.
    """
    try:
        if USE_CBL:
            cfg = CBLStore().load_config() or {}
        else:
            cfg = json.loads(CONFIG_PATH.read_text())
        db_cfg = cfg.get("output", {}).get("db", {})
        engine = db_cfg.get("engine", "")
    except Exception:
        return [{"ok": None, "error": "Could not load config"}] * len(ops)

    if engine != "postgres":
        return [
            {"ok": None, "error": f"EXPLAIN not supported for engine: {engine}"}
        ] * len(ops)

    try:
        import asyncpg

        ssl_ctx = None
        if db_cfg.get("ssl"):
            import ssl as _ssl

            ssl_ctx = _ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = _ssl.CERT_NONE

        conn = await asyncpg.connect(
            host=db_cfg.get("host", "localhost"),
            port=db_cfg.get("port", 5432),
            database=db_cfg.get("database", ""),
            user=db_cfg.get("username", "postgres"),
            password=db_cfg.get("password", ""),
            ssl=ssl_ctx,
        )
    except Exception as exc:
        return [{"ok": None, "error": f"DB connect failed: {exc}"}] * len(ops)

    results = []
    try:
        for op in ops:
            sql = op["sql"]
            params = op["params"]
            try:
                rows = await conn.fetch(f"EXPLAIN {sql}", *params)
                plan = [r[0] for r in rows]
                results.append({"ok": True, "plan": plan})
            except Exception as exc:
                err_msg = str(exc)
                # Extract the most useful part of the error
                if hasattr(exc, "message"):
                    err_msg = exc.message
                results.append({"ok": False, "error": err_msg})
    finally:
        await conn.close()

    return results


# --- Sources API ---


async def list_sources(request):
    """List all saved data source configurations."""
    if USE_CBL:
        sources = CBLStore().load_sources()
        # Return with doc_id included
        result = []
        for doc_id, doc in sources.items():
            result.append({**doc, "doc_id": doc_id})
        return json_response({"sources": result})
    SOURCES_DIR.mkdir(exist_ok=True)
    files = sorted(
        p for p in SOURCES_DIR.iterdir() if p.is_file() and p.suffix == ".json"
    )
    sources = []

    for p in files:
        try:
            data = json.loads(p.read_text())
            sources.append(data)
        except json.JSONDecodeError:
            pass
    return json_response({"sources": sources})


async def save_source(request):
    """Save a data source configuration."""
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return error_response("Invalid JSON")

    # Validate required fields
    if body.get("type") != "source":
        return error_response("type must be 'source'")
    if not body.get("system"):
        return error_response("system is required")
    if not body.get("config", {}).get("src_name"):
        return error_response("config.src_name is required")

    source_name = body["config"]["src_name"]
    source_doc = {
        "type": "source",
        "system": body["system"],
        "config": body["config"],
        "meta": {
            **body.get("meta", {}),
            "saved_at": datetime.datetime.utcnow().isoformat(),
        },
    }

    if USE_CBL:
        try:
            CBLStore().save_source(source_name, source_doc)
            return json_response({"ok": True, "name": source_name})
        except Exception as e:
            return error_response(str(e), 500)
    else:
        try:
            SOURCES_DIR.mkdir(exist_ok=True)
            file_path = SOURCES_DIR / f"{source_name}.json"
            file_path.write_text(json.dumps(source_doc, indent=2) + "\n")
            return json_response({"ok": True, "name": source_name})
        except Exception as e:
            return error_response(str(e), 500)


async def delete_source(request):
    """Delete a saved source configuration."""
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return error_response("Invalid JSON")

    name = body.get("name")
    if not name:
        return error_response("name is required")

    if USE_CBL:
        try:
            CBLStore().delete_source(name)
            return json_response({"ok": True})
        except Exception as e:
            return error_response(str(e), 500)
    else:
        try:
            file_path = SOURCES_DIR / f"{name}.json"
            if file_path.exists():
                file_path.unlink()
            return json_response({"ok": True})
        except Exception as e:
            return error_response(str(e), 500)


async def clear_all_sources(request):
    """Delete all saved source configurations."""
    if USE_CBL:
        try:
            CBLStore().clear_all_sources()
            return json_response({"ok": True})
        except Exception as e:
            return error_response(str(e), 500)
    else:
        try:
            SOURCES_DIR.mkdir(exist_ok=True)
            count = 0
            for p in SOURCES_DIR.iterdir():
                if p.is_file() and p.suffix == ".json":
                    p.unlink()
                    count += 1
            return json_response({"ok": True, "deleted": count})
        except Exception as e:
            return error_response(str(e), 500)


async def test_source(request):
    """Test connection to a Couchbase Sync Gateway.

    Audit-logged via ``[CONTROL]`` / ``[HTTP]`` per GUIDE_LOGGING.md.
    Every JSON response carries the full ``url`` that was tried so the
    GUI can show the operator exactly which endpoint we hit.
    """
    import time as _time

    try:
        body = await request.json()
    except json.JSONDecodeError:
        log_event(logger, "warn", "CONTROL", "source/test: invalid JSON body")
        return error_response("Invalid JSON")

    url = body.get("url", "").strip()
    database = body.get("database", "").strip()
    scope = body.get("scope", "_default").strip()
    collection = body.get("collection", "_default").strip()
    auth_method = body.get("auth_method", "none").strip()

    if not url or not database:
        log_event(
            logger,
            "warn",
            "CONTROL",
            "source/test: missing url or database",
        )
        return error_response("url and database are required")

    # Construct the changes endpoint URL
    # Sync Gateway uses keyspace format: db.scope.collection
    if scope and scope != "_default" and collection and collection != "_default":
        test_url = f"{url}/{database}.{scope}.{collection}/_changes"
    elif collection and collection != "_default":
        test_url = f"{url}/{database}.{collection}/_changes"
    else:
        test_url = f"{url}/{database}/_changes"

    log_event(
        logger,
        "info",
        "CONTROL",
        "source/test: begin (auth=%s)" % auth_method,
        http_method="GET",
        url=test_url,
    )

    headers = {}
    if auth_method == "basic":
        username = body.get("username", "").strip()
        password = body.get("password", "").strip()
        if username and password:
            import base64

            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {credentials}"
    elif auth_method == "session":
        session_cookie = body.get("session_cookie", "").strip()
        if session_cookie:
            headers["Cookie"] = f"SyncGatewaySession={session_cookie}"
    elif auth_method == "bearer":
        bearer_token = body.get("bearer_token", "").strip()
        if bearer_token:
            headers["Authorization"] = f"Bearer {bearer_token}"

    t0 = _time.monotonic()
    try:
        # Make a request to test connection
        async with _aiohttp.ClientSession() as session:
            async with session.get(
                test_url,
                headers=headers,
                timeout=_aiohttp.ClientTimeout(total=10),
                ssl=False,
            ) as resp:
                elapsed_ms = (_time.monotonic() - t0) * 1000.0
                if resp.status in [200, 400, 401, 403]:
                    log_event(
                        logger,
                        "info",
                        "CONTROL",
                        "source/test: reachable (HTTP %d)" % resp.status,
                        http_method="GET",
                        url=test_url,
                        status=resp.status,
                        elapsed_ms=round(elapsed_ms, 1),
                    )
                    return json_response(
                        {
                            "ok": True,
                            "status": resp.status,
                            "url": test_url,
                            "auth_method": auth_method,
                            "elapsed_ms": round(elapsed_ms, 1),
                        }
                    )
                else:
                    log_event(
                        logger,
                        "warn",
                        "HTTP",
                        "source/test: unexpected HTTP %d" % resp.status,
                        http_method="GET",
                        url=test_url,
                        status=resp.status,
                        elapsed_ms=round(elapsed_ms, 1),
                    )
                    return json_response(
                        {
                            "ok": False,
                            "error": f"HTTP {resp.status}",
                            "url": test_url,
                            "status": resp.status,
                            "auth_method": auth_method,
                            "elapsed_ms": round(elapsed_ms, 1),
                        },
                        status=200,
                    )
    except _aiohttp.ClientConnectorError as exc:
        elapsed_ms = (_time.monotonic() - t0) * 1000.0
        log_event(
            logger,
            "error",
            "HTTP",
            "source/test: connect failed (DNS/refused/SSL)",
            http_method="GET",
            url=test_url,
            elapsed_ms=round(elapsed_ms, 1),
            error_class=type(exc).__name__,
            error_detail=str(exc)[:300],
        )
        return json_response(
            {
                "ok": False,
                "error": str(exc),
                "url": test_url,
                "auth_method": auth_method,
                "error_class": type(exc).__name__,
                "elapsed_ms": round(elapsed_ms, 1),
            },
            status=200,
        )
    except asyncio.TimeoutError as exc:
        elapsed_ms = (_time.monotonic() - t0) * 1000.0
        log_event(
            logger,
            "error",
            "HTTP",
            "source/test: timeout (10s)",
            http_method="GET",
            url=test_url,
            elapsed_ms=round(elapsed_ms, 1),
            error_class=type(exc).__name__,
            error_detail="Connection timed out after 10s",
        )
        return json_response(
            {
                "ok": False,
                "error": "Connection timed out after 10s",
                "url": test_url,
                "auth_method": auth_method,
                "error_class": "TimeoutError",
                "elapsed_ms": round(elapsed_ms, 1),
            },
            status=200,
        )
    except Exception as exc:
        elapsed_ms = (_time.monotonic() - t0) * 1000.0
        log_event(
            logger,
            "error",
            "HTTP",
            "source/test: unexpected error",
            http_method="GET",
            url=test_url,
            elapsed_ms=round(elapsed_ms, 1),
            error_class=type(exc).__name__,
            error_detail=str(exc)[:300],
        )
        return json_response(
            {
                "ok": False,
                "error": str(exc),
                "url": test_url,
                "auth_method": auth_method,
                "error_class": type(exc).__name__,
                "elapsed_ms": round(elapsed_ms, 1),
            },
            status=200,
        )


# --- App factory ---


def create_app():
    # Configure logging so log_event() calls write to the log file
    try:
        if USE_CBL:
            log_cfg = (CBLStore().load_config() or {}).get("logging", {})
        else:
            log_cfg = json.loads(CONFIG_PATH.read_text()).get("logging", {})
    except Exception:
        log_cfg = {}
    configure_logging(log_cfg)

    app = web.Application(middlewares=[cors_middleware])

    # Pages
    app.router.add_get("/favicon.ico", favicon)
    app.router.add_get("/", page_index)
    app.router.add_get("/settings", page_config)
    app.router.add_get("/jobs", page_jobs)
    app.router.add_get("/inputs", page_inputs)
    app.router.add_get("/outputs", page_outputs)
    app.router.add_get("/schema", page_schema)
    app.router.add_get("/glossary", page_transforms)
    app.router.add_get("/wizard", page_wizard)
    app.router.add_get("/help", page_help)
    app.router.add_get("/logs", page_logs)
    app.router.add_get("/dlq", page_dlq)
    app.router.add_get("/eventing", page_eventing)

    # Logs API
    app.router.add_get("/api/logs", get_logs)
    app.router.add_get("/api/log-files", get_log_files)

    # Config API
    app.router.add_get("/api/config", get_config)
    app.router.add_put("/api/config", put_config)

    # Mappings API
    app.router.add_get("/api/mappings", list_mappings)
    app.router.add_get("/api/mappings/{name}", get_mapping)
    app.router.add_put("/api/mappings/{name}", put_mapping)
    app.router.add_patch("/api/mappings/{name}/active", patch_mapping_active)
    app.router.add_delete("/api/mappings/{name}", delete_mapping)
    app.router.add_post("/api/mappings/validate", validate_mapping)

    # DLQ API
    app.router.add_get("/api/dlq", list_dlq)
    app.router.add_get("/api/dlq/count", dlq_count)
    app.router.add_get("/api/dlq/meta", dlq_meta)
    app.router.add_get("/api/dlq/stats", dlq_stats)
    app.router.add_get("/api/dlq/explain", dlq_explain)
    app.router.add_post("/api/dlq/replay", replay_dlq)
    app.router.add_get("/api/dlq/{id}", get_dlq_entry)
    app.router.add_post("/api/dlq/{id}/retry", retry_dlq_entry)
    app.router.add_delete("/api/dlq/{id}", delete_dlq_entry)
    app.router.add_delete("/api/dlq", clear_dlq)

    # Maintenance API
    app.router.add_post("/api/maintenance", post_maintenance)

    # Status API
    app.router.add_get("/api/status", get_status)
    app.router.add_get("/api/jobs", get_jobs)
    app.router.add_get("/api/jobs/status", get_jobs_status)

    # Metrics API
    app.router.add_get("/api/metrics", get_metrics)

    # Worker Control API
    app.router.add_post("/api/restart", post_restart)
    app.router.add_post("/api/shutdown", post_shutdown)
    app.router.add_post("/api/offline", post_offline)
    app.router.add_post("/api/online", post_online)
    app.router.add_get("/api/worker-status", get_worker_status)

    # Job Control API
    app.router.add_post("/api/jobs/{job_id}/start", post_job_start)
    app.router.add_post("/api/jobs/{job_id}/stop", post_job_stop)
    app.router.add_post("/api/jobs/{job_id}/restart", post_job_restart)
    app.router.add_post("/api/jobs/{job_id}/kill", post_job_kill)

    # Sample Doc API
    app.router.add_get("/api/sample-doc", get_sample_doc)

    # DB Introspection API
    app.router.add_get("/api/db/drivers", list_db_drivers)
    app.router.add_post("/api/db/test", db_test_connection)
    app.router.add_post("/api/db/introspect", db_introspect)
    app.router.add_post("/api/db/parse-ddl", parse_ddl)

    # Auto-Map API
    app.router.add_post("/api/auto-map", auto_map_columns)

    # Wizard API
    app.router.add_post("/api/wizard/test-source", wizard_test_source)
    app.router.add_post("/api/wizard/test-output", wizard_test_output)

    # Sources API
    app.router.add_get("/api/source/list", list_sources)
    app.router.add_post("/api/source/save", save_source)
    app.router.add_post("/api/source/delete", delete_source)
    app.router.add_post("/api/source/clear", clear_all_sources)
    app.router.add_post("/api/source/test", test_source)

    # API v2.0 - Inputs (changes)
    app.router.add_get("/api/inputs_changes", api_get_inputs_changes)
    app.router.add_post("/api/inputs_changes", api_post_inputs_changes)
    app.router.add_put("/api/inputs_changes/{id}", api_put_inputs_changes_entry)
    app.router.add_delete("/api/inputs_changes/{id}", api_delete_inputs_changes_entry)

    # API v2.0 - Outputs (dynamic type: rdbms, http, cloud, stdout)
    app.router.add_get(r"/api/outputs_{type:rdbms|http|cloud|stdout}", api_get_outputs)
    app.router.add_post(
        r"/api/outputs_{type:rdbms|http|cloud|stdout}", api_post_outputs
    )
    app.router.add_put(
        r"/api/outputs_{type:rdbms|http|cloud|stdout}/{id}", api_put_outputs_entry
    )
    app.router.add_delete(
        r"/api/outputs_{type:rdbms|http|cloud|stdout}/{id}", api_delete_outputs_entry
    )

    # API v2.0 - Jobs
    app.router.add_get("/api/v2/jobs", api_get_jobs)
    app.router.add_post("/api/v2/jobs", api_post_jobs)
    app.router.add_get("/api/v2/jobs/{id}", api_get_job)
    app.router.add_put("/api/v2/jobs/{id}", api_put_job)
    app.router.add_delete("/api/v2/jobs/{id}", api_delete_job)
    app.router.add_post("/api/v2/jobs/{id}/refresh-input", api_refresh_job_input)
    app.router.add_post("/api/v2/jobs/{id}/refresh-output", api_refresh_job_output)
    app.router.add_put("/api/v2/jobs/{id}/mapping", api_put_job_mapping)

    # API v2.0 - Eventing
    app.router.add_get("/api/v2/jobs/{id}/eventing", api_get_job_eventing)
    app.router.add_put("/api/v2/jobs/{id}/eventing", api_put_job_eventing)

    # API v2.0 - RDBMS Table Definitions
    app.router.add_get("/api/v2/tables_rdbms", api_get_tables_rdbms)
    app.router.add_post("/api/v2/tables_rdbms", api_post_tables_rdbms)
    app.router.add_get("/api/v2/tables_rdbms/{id}/used-by", api_get_table_rdbms_used_by)
    app.router.add_get("/api/v2/tables_rdbms/{id}", api_get_table_rdbms_entry)
    app.router.add_put("/api/v2/tables_rdbms/{id}", api_put_table_rdbms_entry)
    app.router.add_delete("/api/v2/tables_rdbms/{id}", api_delete_table_rdbms_entry)

    # Static files
    app.router.add_static("/static/", WEB / "static", show_index=False)

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="changes_worker admin UI")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--host", default="0.0.0.0")
    args = parser.parse_args()
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    web.run_app(create_app(), host=args.host, port=args.port)
