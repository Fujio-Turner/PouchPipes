# Logging Standards Guide

**Project**: Change Stream DB (PouchPipes)  
**Scope**: All Python modules in changes-worker  
**Date**: April 24, 2026  
**Status**: Active  
**Implementation**: `pipeline/pipeline_logging.py`

---

## Overview

This document defines the logging standards for the changes-worker codebase. Every log line emitted by the application uses **structured, tagged logging** built on Python's standard `logging` module. The system is inspired by [Couchbase Sync Gateway's logging architecture](https://docs.couchbase.com/sync-gateway/current/logging.html) and adds operation tagging, field-level redaction, and per-key level overrides.

**Goals**:

1. Any log line can be instantly identified as belonging to a **pipeline stage** (SOURCE → PROCESS → OUTPUT → DLQ)
2. Operators can filter logs by tag and level without code changes (via `config.json`)
3. Developers follow a single function — `log_event()` — for all structured logging
4. Log volume is controlled: **INFO** for summaries, **DEBUG** for per-operation detail

---

## Architecture: The Data Pipeline & Log Tags

Every document flows through a pipeline of ordered stages. Each stage has a dedicated **log key** (tag) that appears in square brackets in the log output. When reading logs, the tag tells you *exactly* where in the pipeline the work is happening:

```
SOURCE              PROCESS                                     OUTPUT              ERROR HANDLING
┌──────────┐   ┌────────────────────────────────────────┐   ┌──────────────┐   ┌──────────────┐
│ CHANGES  │──▶│ PROCESSING ──▶ EVENTING ──▶            │──▶│ OUTPUT       │──▶│ DLQ / RETRY  │
│ HTTP     │   │ FLOOD         ATTACHMENT ──▶            │   │ CHECKPOINT   │   │              │
│          │   │               ENRICHMENT* ──▶          │   │              │   │              │
│          │   │               INFERENCE* ──▶ MAPPING   │   │              │   │              │
└──────────┘   └────────────────────────────────────────┘   └──────────────┘   └──────────────┘

INFRASTRUCTURE
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│ CBL  │  METRICS  │  CONTROL  │  SHUTDOWN                                                     │
└──────────────────────────────────────────────────────────────────────────────────────────────┘

* = planned future stages (see "Adding New Inline Stages" below)
```

The processing stage runs multiple sub-stages **in order** for each document:

1. **PROCESSING** — Filtering (tombstones, removes), batch orchestration
2. **EVENTING** — User-supplied JavaScript handlers (OnUpdate / OnDelete) via V8
3. **ATTACHMENT** — Detect, filter, fetch, upload to cloud, post-process
4. *(future)* **ENRICHMENT** — Data cleaning, validation, geocoding, normalization
5. *(future)* **INFERENCE** — ML/AI model inference (sentiment, embeddings, classification)
6. **MAPPING** — Schema mapping, JSONPath extraction, SQL op generation
7. **OUTPUT** — Delivery to target (PostgreSQL, HTTP, S3, etc.)

---

## The `log_event()` Function

**Every structured log line MUST use `log_event()`.** Do not call `logger.info()`, `logger.debug()`, etc. directly except for unstructured third-party integration (e.g., `icecream`).

### Signature

```python
from pipeline.pipeline_logging import log_event

log_event(logger, level, log_key, message, **fields)
```

### Parameters

| Parameter | Type   | Description |
|-----------|--------|-------------|
| `logger`  | `logging.Logger` | The logger instance (typically `logging.getLogger("changes_worker")`) |
| `level`   | `str`  | One of: `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`, `"critical"` |
| `log_key` | `str`  | The pipeline stage tag (see [Log Keys](#log-keys-reference) below). UPPERCASE. |
| `message` | `str`  | Human-readable summary. Use `%`-formatting for inline values. |
| `**fields`| `kwargs` | Structured key=value pairs appended to the log line. Only use [registered field names](#structured-fields-reference). |

### Example

```python
log_event(
    logger,
    "info",
    "PROCESSING",
    "%.0fms | batch %d changes | %d ok | %d failed"
    % (elapsed_ms, batch_size, succeeded, failed),
    out_ms=round(out_ms, 1),
    seq_from=since,
    seq_to=str(last_seq),
    include_docs=include_docs,
    checkpoint="moved",
)
```

### Output

```
2026-04-24 14:06:34.796 [INFO] [PROCESSING] job=..b1c2d #s:..fff134 #b:a8b0tz changes_worker: batch 100 changes | 99 ok | 1 failed | 312ms | out_ms 245.1 | seq_from 625000 | seq_to 625100 | inc_docs False | chkpt moved
```

---

## Log Line Anatomy

Every structured log line follows this format:

```
TIMESTAMP [LEVEL] [KEY] job=..JOB #s:..SESSION #b:BATCH LOGGER: MESSAGE | field value | ...
```

| Segment | Source | Example | When Present |
|---------|--------|---------|--------------|
| `TIMESTAMP` | Python logging | `2026-04-24 14:06:34.796` | Always |
| `[LEVEL]` | Python logging | `[INFO]` | Always |
| `[KEY]` | `log_event()` log_key arg | `[PROCESSING]` | Always |
| `job=..JOB` | `set_job_tag()` context var | `job=..b1c2d` | When job context is set |
| `#s:..SESSION` | `set_session_id()` context var | `#s:..fff134` | When session is active |
| `#b:BATCH` | `set_batch_id()` context var | `#b:a8b0tz` | During batch processing |
| `LOGGER` | `logging.getLogger()` | `changes_worker` | Always |
| `MESSAGE` | `log_event()` message arg | `batch 100 changes...` | Always |
| `\| field value` | `log_event()` kwargs | `\| seq_from 625000` | When fields provided |

All tracing tags (`[KEY]`, `job=`, `#s:`, `#b:`) are injected automatically by the formatter. They appear between `[LEVEL]` and the logger name. Structured fields are pipe-delimited after the message.

---

## Job Tag

In a multi-job world, every log line must be attributable to a specific job. The **job tag** is the last 5 characters of the full `job_id`, rendered as `job=..xxxxx` in the log prefix.

### Why Last 5 Characters?

Job IDs are typically UUIDs (e.g. `job::a1b2c3d4-e5f6-7890-abcd-ef1234567890`). Logging the full ID wastes space on every line. The last 5 characters provide enough uniqueness to distinguish concurrent jobs while keeping logs compact.

### How It Works

1. `set_job_tag(job_id)` is called once at job startup — in `Pipeline.run()` (per-job thread) and `poll_changes()` (async context)
2. It stores the last 5 chars in a `contextvars.ContextVar`
3. `log_event()` automatically reads it and attaches `job_tag` to every log record
4. The `RedactingFormatter` renders it as `job=..xxxxx` in the prefix
5. The tag persists for the lifetime of the job — no per-batch or per-call management needed

### Example: Multi-Job Logs

```
2026-04-24 15:26:29.973 [INFO] [PROCESSING] job=..12345 #s:..fff134 #b:8otn2v changes_worker: batch 1 changes | 1 ok
2026-04-24 15:26:29.980 [INFO] [PROCESSING] job=..98765 #s:..3ed932 #b:n1wor9 changes_worker: batch 5 changes | 5 ok
2026-04-24 15:26:30.001 [INFO] [CHECKPOINT] job=..12345 #s:..fff134 changes_worker: checkpoint saved
2026-04-24 15:26:30.005 [INFO] [CHECKPOINT] job=..98765 #s:..3ed932 changes_worker: checkpoint saved
```

### Filtering by Job

```bash
# See all logs for job ending in "12345"
grep 'job=..12345' logs/changes_worker_info.log

# Combine with batch ID to trace one batch in one job
grep '#b:8otn2v' logs/changes_worker_debug.log
```

### Legacy Single-Job Mode

When no `job_id` is set (legacy `config.json` mode), the `job=..` tag is simply omitted from log output. Behaviour is fully backward-compatible.

---

## Session ID

A **session** represents a single run of a job — from startup to shutdown (or crash). Every time a job starts or restarts, it gets a new UUID4 session ID. This lets you connect a specific config dump to the runtime log lines from that run.

### Why Sessions?

The same job can be restarted with different configurations. Without sessions, you can't tell which config was active when a particular batch ran:

- **Session A** (`#s:..fff134`): `sequential=True, max_concurrent=1`
- **Session B** (`#s:..3ed932`): `sequential=False, max_concurrent=20` (after config change + restart)

Every log line from session A has `#s:..fff134`, and the config dump at the start of that session also has `#s:..fff134`. `grep '#s:..fff134'` gives you everything.

### How It Works

1. `generate_session_id()` creates a UUID4 when the job starts (in `Pipeline.run()` or `poll_changes()`)
2. `set_session_id(uuid)` stores the full UUID and the last 6 chars as the tag
3. The full UUID is logged once at startup: `session started ... session=<full-uuid>`
4. All subsequent log lines get `#s:..xxxxxx` (last 6 chars) in the prefix
5. On job restart, a new session UUID is generated — the old one is gone

### Example: Session Lifecycle (real output)

```
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: session started | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode session=...551b6a
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: source | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode source=sync_gateway url=http://host.docker.internal:4984 db=db scope=us collection=prices
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: processing | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode sequential=False max_concurrent=20 dry_run=False ignore_delete=False ignore_remove=False write_method=PUT get_batch_number=100
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: output | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode mode=postgres host=? port=? database=? schema=public pool_min=2 pool_max=10 ssl=False
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: dlq | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode dead_letter_path= retention_s=86400 max_replay=10
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: retry | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode max_retries=5 backoff_base=1s backoff_max=60s retry_on_status=[500, 502, 503, 504]
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: checkpoint | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode enabled=True client_id=changes_worker every_n_docs=0
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: shutdown | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode drain_timeout=60s dlq_inflight=False
```

### Filtering by Session

```bash
# Find the full session UUID (logged once at startup)
grep 'session started' logs/changes_worker_info.log | grep 'job=..12345'

# See everything from a specific session
grep '#s:..fff134' logs/changes_worker_info.log

# See the config that was active for this session
grep '#s:..fff134' logs/changes_worker_info.log | grep 'job config:'
```

---

## Job Config Dump at Startup

When a job starts, the system emits a block of `INFO`-level `[CONTROL]` log lines describing the job's full configuration with **sensitive values redacted**. These lines share the same `#s:..` session tag, so you can always connect a session's config to its runtime log lines.

### What Gets Logged

| Config Section | Log Message | Key Fields |
|---------------|-------------|------------|
| Source/Gateway | `job config: source` | source type, URL (redacted), database, scope, collection |
| Changes Feed | `job config: changes_feed` | feed_type, include_docs, active_only, since, timeout, channels, throttle |
| Processing | `job config: processing` | sequential, max_concurrent, dry_run, ignore_delete, write_method |
| Output | `job config: output` | mode, target_url/host/bucket (by mode), pool sizes, halt_on_failure |
| DLQ | `job config: dlq` | dead_letter_path, retention, max_replay_attempts |
| Retry | `job config: retry` | max_retries, backoff_base, backoff_max, retry_on_status |
| Checkpoint | `job config: checkpoint` | enabled, client_id, every_n_docs |
| Shutdown | `job config: shutdown` | drain_timeout, dlq_inflight_on_shutdown |
| Attachments | `job config: attachments` | *(only if enabled)* mode, dest_type, halt_on_failure |
| Eventing | `job config: eventing` | *(only if configured)* source, timeout_ms |
| Mapping | `job config: mapping` | *(only if configured)* table names |
| Recursion Guard | `job config: recursion_guard` | *(only if enabled)* max_tracked, ttl |

### What Gets Redacted

The following fields are **always** replaced with `***`, regardless of nesting depth:

`password`, `passwd`, `pass`, `secret`, `api_key`, `access_key_id`, `secret_access_key`, `session_token`, `bearer_token`, `token`, `session_cookie`, `authorization`, `cookie`, `refresh_token`, `username`, `user`

Additionally, URLs with embedded credentials (e.g. `http://admin:secret@host`) are redacted by the `Redactor` class.

### Implementation

- **Function**: `_log_job_config()` in `main.py`
- **Sanitizer**: `_sanitize_config()` in `main.py` — deep-copies the config, replaces sensitive keys with `***`, runs URL redaction
- **Called from**: `poll_changes()` at job startup, after session ID is set

---

## Batch Tracing ID

Every batch of changes receives a **6-character alphanumeric tracing ID** (e.g. `#b:a8b0tz`) when it enters `_process_changes_batch`. The ID appears as `#b:xxxxxx` after the `[LEVEL]` tag in every log line produced during that batch — across all pipeline stages.

### Purpose

When a batch flows through SOURCE → PROCESS → OUTPUT → DLQ, dozens of log lines are emitted across different log keys. The batch ID lets you **correlate them all with a single grep**:

```bash
# Find everything that happened to batch #b:a8b0tz
grep '#b:a8b0tz' logs/changes_worker_debug.log
```

### How It Works

1. A random 6-char string (`[a-z0-9]`) is generated at the top of `_process_changes_batch()`
2. It is stored in a `contextvars.ContextVar` via `set_batch_id()`
3. `log_event()` automatically reads the context var and attaches it to every log record
4. The `RedactingFormatter` renders it as `#b:id` after the `[LEVEL]` tag
5. At the end of the batch, `set_batch_id(None)` clears it

### Example: Tracing a Batch

```
2026-04-24 15:26:29.973 [DEBUG] [CHANGES] job=..12345 #s:..fff134 #b:8otn2v changes_worker: _changes batch: 1 changes | batch 1
2026-04-24 15:26:29.980 [DEBUG] [OUTPUT] job=..12345 #s:..fff134 #b:8otn2v changes_worker: sent doc | doc_id hotel_42
2026-04-24 15:26:29.985 [INFO]  [PROCESSING] job=..12345 #s:..fff134 #b:8otn2v changes_worker: batch 1 changes | 1 ok | 0 failed | 32ms | chkpt moved
```

### Sequential Mode (batch of 1)

In sequential mode (`sequential: true`), each change is processed individually — the batch size is 1. Each single-doc "batch" still gets its own unique tracing ID, so the behaviour is identical.

### Implementation

- **Batch ID generator**: `generate_batch_id()` in `pipeline/pipeline_logging.py`
- **Session ID generator**: `generate_session_id()` in `pipeline/pipeline_logging.py`
- **Job tag setter**: `set_job_tag(job_id)` — stores last 5 chars
- **Session setter**: `set_session_id(uuid)` — stores full UUID + last 6 chars tag
- **Context vars**: `set_batch_id()` / `set_session_id()` / `set_job_tag()` — per-context, auto-propagation
- **Auto-attach**: `log_event()` reads all three context vars — zero call-site changes
- **Formatter**: `RedactingFormatter.format()` inserts `#b:batch #s:..session job=..tag` after `[LEVEL]`

---

## Log Keys Reference

Log keys identify **where in the pipeline** the work is happening. They appear as `[KEY]` in log output and can be independently filtered via `config.json`.

### Source Stage (Ingestion)

| Log Key     | Purpose | Typical Source Files |
|-------------|---------|---------------------|
| `CHANGES`   | `_changes` feed polling, batch receipt, tombstone counting, sequence tracking | `rest/changes_http.py`, `pipeline/pipeline.py` |
| `HTTP`      | All HTTP requests/responses: `_bulk_get`, individual doc fetches, retries, status codes | `rest/changes_http.py`, `rest/output_http.py` |

### Process Stage (Transformation)

| Log Key      | Purpose | Typical Source Files |
|--------------|---------|---------------------|
| `PROCESSING` | Batch summaries, filtering decisions, attachment processing | `rest/changes_http.py`, `rest/attachments.py` |
| `EVENTING`   | JavaScript handler execution (OnUpdate/OnDelete), V8 timeouts, JS `log()` output, handler halts, rejections | `eventing/eventing.py`, `rest/changes_http.py` |
| `ATTACHMENT` | Attachment detect, filter, fetch, upload (S3/HTTP/filesystem), post-process, multipart parsing | `rest/attachments.py`, `rest/attachment_upload.py`, `rest/attachment_stream.py`, `rest/attachment_postprocess.py`, `rest/attachment_multipart.py` |
| `MAPPING`    | Schema mapper load/match/skip, JSONPath extraction, SQL op generation | `db/db_base.py`, `schema/mapper.py` |
| `FLOOD`      | Flood detection thresholds exceeded, throttle activation | `rest/changes_http.py`, `rest/output_http.py` |

### Output Stage (Delivery)

| Log Key      | Purpose | Typical Source Files |
|--------------|---------|---------------------|
| `OUTPUT`     | SQL execution, HTTP forwarding, cloud upload, pool connect/disconnect, response stats | `db/db_postgres.py`, `db/db_base.py`, `rest/output_http.py`, `cloud/cloud_base.py` |
| `CHECKPOINT` | Checkpoint save/load, sequence advancement, fallback storage | `main.py` (Checkpoint class) |

### Error Handling Stage

| Log Key  | Purpose | Typical Source Files |
|----------|---------|---------------------|
| `DLQ`    | Dead letter queue writes, replays, purges, pending counts | `rest/changes_http.py`, `storage/cbl_store.py` |
| `RETRY`  | Retry decisions, backoff delays, `Retry-After` header parsing, exhaustion | `rest/changes_http.py`, `db/db_base.py` |

### Infrastructure (Cross-Cutting)

| Log Key    | Purpose | Typical Source Files |
|------------|---------|---------------------|
| `CBL`      | Couchbase Lite database open/close, maintenance, compaction | `storage/cbl_store.py` |
| `METRICS`  | Metrics server start, Prometheus snapshots | `main.py` |
| `CONTROL`  | Admin API actions: `/_restart`, `/_config`, job management | `main.py`, `pipeline/pipeline_manager.py` |
| `SHUTDOWN` | Graceful drain, in-flight DLQ, signal handling | `main.py` |

### Adding a New Log Key

1. Add the key to `LOG_KEYS` in `pipeline/pipeline_logging.py`
2. Add the key to this guide with its purpose and source files
3. Use UPPERCASE, single word preferred (e.g., `EVENTING`, `VALIDATION`)

### Adding New Inline Processing Stages

The pipeline is designed to grow. Future stages will slot between EVENTING and MAPPING in the per-document processing chain. Each new stage gets its own log key so operators can filter and debug independently.

**Planned stages** (see [PouchPipes issue #20](https://github.com/Fujio-Turner/PouchPipes/issues/20)):

| Future Log Key   | Purpose | Position in Chain |
|------------------|---------|-------------------|
| `ENRICHMENT`     | Data cleaning, Pydantic validation, geocoding, normalization, field transforms | After ATTACHMENT, before INFERENCE |
| `INFERENCE`      | ML/AI model inference — sentiment analysis, embeddings, classification, LLM summarization | After ENRICHMENT, before MAPPING |
| `VECTORSTORE`    | Vector store writes — pgvector, Qdrant, Weaviate embedding storage | After INFERENCE |

**When you implement a new inline stage:**

1. Create the log key following "Adding a New Log Key" above
2. Tag all log lines in the new stage with the new key — **not** with `PROCESSING`
3. Follow the level rules: lifecycle events at `info`, per-doc execution at `debug`, errors at `error`
4. Include `doc_id` on every per-document log line for traceability
5. Include `duration_ms` on the stage's per-doc timing so operators can spot bottlenecks
6. Add the new key to the quick reference card at the bottom of this guide

**Template for a new inline stage:**

```python
# In your new module (e.g., enrichment/enrichment.py)
from pipeline.pipeline_logging import log_event

logger = logging.getLogger("changes_worker")

# Lifecycle: INFO (fires once)
log_event(logger, "info", "ENRICHMENT", "enrichment engine loaded",
    model="pydantic-v2")

# Per-doc execution: DEBUG
log_event(logger, "debug", "ENRICHMENT", "doc enriched",
    doc_id=doc_id, duration_ms=round(elapsed_ms, 1))

# Per-doc error: ERROR with detail
log_event(logger, "error", "ENRICHMENT", "enrichment failed",
    doc_id=doc_id, error_detail=f"{type(exc).__name__}: {exc}")

# Per-doc rejection/skip: DEBUG
log_event(logger, "debug", "ENRICHMENT", "doc skipped by enrichment rules",
    doc_id=doc_id)
```

---

## Log Levels: What Goes Where

### Level Definitions

| Level      | Numeric | Use For |
|------------|---------|---------|
| `critical` | 50      | Process cannot continue. Data loss imminent. |
| `error`    | 40      | Operation failed. Document will be DLQ'd or skipped. Requires attention. |
| `warn`     | 30      | Unexpected condition but operation continues. DLQ writes, fallback paths, deprecations. |
| `info`     | 20      | **Batch-level summaries.** One line per batch or per lifecycle event. Operator-facing. |
| `debug`    | 10      | **Per-operation detail.** Individual doc processing, SQL statements, HTTP requests. Developer-facing. |
| `trace`    | 5       | Internal state dumps, `icecream` output, raw payloads. Only for local development. |

### The Golden Rule: INFO = Summaries, DEBUG = Per-Operation

This is the single most important rule for controlling log volume:

| ✅ INFO (batch-level) | ❌ NOT INFO (per-operation) |
|-----------------------|----------------------------|
| `312ms \| batch 100 changes \| 99 ok \| 1 failed` | `_changes batch: 1 changes` |
| `PostgreSQL pool created` | `SQL exec INSERT` |
| `Pipeline starting for job` | `_bulk_get: requesting 100 docs` |
| `DLQ replay: 5 entries reprocessed, 3 succeeded` | `checkpoint saved` |
| `schema mapping loaded: 4 tables` | `fetch batch 2/3: 100 docs` |

**Rule of thumb**: If a message fires more than once per batch at steady state, it belongs at `debug` or lower.

### Level Selection Checklist

Before choosing a level, ask:

1. **Will this fire once per document?** → `debug` (or `trace` for raw payloads)
2. **Will this fire once per batch?** → `debug` for routine, `info` only for the consolidated summary
3. **Will this fire once per lifecycle event (connect, start, stop)?** → `info`
4. **Is something wrong but recoverable?** → `warn`
5. **Is something wrong and the operation failed?** → `error`
6. **Is the process going to exit?** → `critical`

---

## Structured Fields Reference

Structured fields are appended as pipe-delimited `| key value` pairs after the message. Only fields registered in `_EXTRA_FIELDS` (in `pipeline/pipeline_logging.py`) are rendered. Fields with a value of `None` are suppressed.

### Registered Fields

Fields are passed by their **internal name** (left column) in `log_event()` kwargs. The formatter emits the shorter **log key** (right column) in the output.

| Internal Name | Log Key | Type | Description | Example |
|---------------|---------|------|-------------|---------|
| `job_id` | `job` | str | Job identifier for multi-job deployments | `\| job ingest-orders` |
| `operation` | `op` | str | SQL/HTTP operation: `INSERT`, `UPDATE`, `DELETE`, `SELECT`, `CONNECT`, `DISCONNECT` | `\| op INSERT` |
| `doc_id` | `doc_id` | str | Document ID being processed | `\| doc_id order::12345` |
| `seq` | `seq` | str | Change sequence number | `\| seq 625810` |
| `status` | `status` | int/str | HTTP status code or operation status | `\| status 200` |
| `url` | `url` | str | Target URL (auto-redacted) | `\| url http://***@host/db` |
| `attempt` | `attempt` | int | Retry attempt number | `\| attempt 2` |
| `elapsed_ms` | `el_ms` | float | General elapsed time | `\| el_ms 45.2` |
| `duration_ms` | `dur_ms` | float | Total batch/operation wall-clock duration | `\| dur_ms 312.4` |
| `out_ms` | `out_ms` | float | Output-side wall-clock time (sum of send calls in batch) | `\| out_ms 245.1` |
| `mode` | `mode` | str | Output mode: `postgres`, `mysql`, `mssql`, `http`, `s3`, `gcs`, `azure` | `\| mode postgres` |
| `http_method` | `method` | str | HTTP method used | `\| method PUT` |
| `bytes` | `bytes` | int | Byte count for transfer tracking | `\| bytes 102400` |
| `storage` | `store` | str | Storage backend: `sg`, `cbl`, `file` | `\| store sg` |
| `batch_size` | `batch` | int | Number of items in the current batch | `\| batch 100` |
| `input_count` | `in_count` | int | Count before filtering | `\| in_count 100` |
| `filtered_count` | `filt_count` | int | Count after filtering | `\| filt_count 95` |
| `host` | `host` | str | Database/server hostname | `\| host db.example.com` |
| `port` | `port` | int | Database/server port | `\| port 5432` |
| `delay_seconds` | `delay_s` | float | Backoff/retry delay | `\| delay_s 2.5` |
| `field_count` | `field_ct` | int | Number of fields in a mapping/schema | `\| field_ct 12` |
| `error_detail` | `err` | str | Exception message for error context | `\| err connection refused` |
| `doc_count` | `doc_count` | int | Number of documents in a fetch/result | `\| doc_count 100` |
| `doc_type` | `doc_type` | str | Document type classifier | `\| doc_type dlq` |

#### Batch Summary Fields

These fields are used in the consolidated per-batch INFO summary:

| Internal Name | Log Key | Type | Description | Example |
|---------------|---------|------|-------------|---------|
| `seq_from` | `seq_from` | str | Starting sequence of the batch | `\| seq_from 625000` |
| `seq_to` | `seq_to` | str | Ending sequence of the batch | `\| seq_to 625100` |
| `include_docs` | `inc_docs` | bool | Whether `_changes` included doc bodies | `\| inc_docs False` |
| `docs_fetched` | `fetched` | int | Docs successfully retrieved (when `include_docs=False`) | `\| fetched 99` |
| `docs_missing` | `docs_miss` | int | Docs requested but not returned (omit if 0) | `\| docs_miss 1` |
| `attachments` | `attach` | int | Whether attachment processing is enabled (0/1) | `\| attach 0` |
| `succeeded` | `ok` | int | Docs successfully delivered to output | `\| ok 99` |
| `failed` | `failed` | int | Docs that failed delivery | `\| failed 1` |
| `filtered_out` | `filt_out` | int | Tombstones/deletes filtered from batch (omit if 0) | `\| filt_out 5` |
| `checkpoint` | `chkpt` | str | Checkpoint status: `moved` or `held` | `\| chkpt moved` |

#### CBL-Specific Fields

| Internal Name | Log Key | Type | Description | Example |
|---------------|---------|------|-------------|---------|
| `db_name` | `db_name` | str | CBL database name | `\| db_name changes_worker_db` |
| `db_path` | `db_path` | str | CBL database file path | `\| db_path /app/data/` |
| `db_size_mb` | `db_mb` | float | Database size on disk | `\| db_mb 124.5` |
| `manifest_id` | `manifest` | str | Mapping manifest identifier | `\| manifest v2-orders` |
| `maintenance_type` | `maint` | str | CBL maintenance operation type | `\| maint compact` |
| `trigger` | `trigger` | str | What triggered the operation | `\| trigger scheduled` |

#### Source Mutation Audit Fields (Attachment Post-Process PUT/DELETE)

These fields are used to audit and DEBUG the writes the worker makes back to
the **source** database during the attachment post-process stage
(`update_doc`, `set_ttl`, `delete_doc`, `delete_attachments`, `purge`). On
success, the source returns a **new `_rev`** which is logged as the *receipt*
proving the operation was applied. On failure, the HTTP status and error
class identify which class of failure occurred (timeout, 401/403, 404, 409,
or generic).

| Internal Name | Log Key | Type | Description | Example |
|---------------|---------|------|-------------|---------|
| `doc_rev` | `rev` | str | The current/old `_rev` sent in the PUT/DELETE request | `\| rev 12-abc123` |
| `new_rev` | `new_rev` | str | The new `_rev` returned by the source on success — the receipt for the mutation | `\| new_rev 13-def456` |
| `pp_action` | `pp_action` | str | The post-process action: `update_doc`, `set_ttl`, `delete_doc`, `delete_attachments`, `purge` | `\| pp_action delete_doc` |

##### Outcome Coverage

`rest/attachment_postprocess.py` emits a `[ATTACHMENT]` log line for every
HTTP outcome on a source mutation. Each line carries `pp_action`, `doc_id`,
`doc_rev` (old rev), `http_method`, `elapsed_ms`, and — when applicable —
`status`, `new_rev`, `error_class`, and `error_detail`:

| Outcome | Level | Message | Receipt Field |
|---------|-------|---------|---------------|
| 2xx success | `info` | `source PUT ok (receipt)` / `source DELETE ok (receipt)` | `new_rev` (the proof) |
| Timeout / connection error | `error` | `source ... timeout/connection error` | `error_class`, `error_detail` |
| 401 / 403 unauthorized | `error` | `source ... unauthorized` | `status`, `error_detail` |
| 404 not found (doc gone) | `warn` | `source ... 404 not_found (doc missing)` | `status` |
| 409 conflict (doc changed) | `warn` | `source ... 409 conflict (doc changed)` | `status` (then re-fetch + retry) |
| Other HTTP error | `error` | `source ... http error` | `status`, `error_class`, `error_detail` |
| Unexpected exception | `error` | `source ... unexpected error` | `error_class`, `error_detail` |

Per-attempt DEBUG lines (`source ... begin`) are also emitted so you can
trace conflict-retry loops end-to-end. To audit a specific document's
mutations:

```bash
# Every PUT/DELETE the worker performed against the source for a doc
grep '\[ATTACHMENT\]' logs/*_info.log logs/*_debug.log | grep 'doc_id order::12345'

# Just the success receipts (new_rev = audit trail)
grep '\[ATTACHMENT\]' logs/*_info.log | grep 'receipt' | grep 'doc_id order::12345'

# All failed mutations across the cluster
grep '\[ATTACHMENT\]' logs/*_error.log | grep -E 'pp_action (update_doc|set_ttl|delete_doc|delete_attachments|purge)'
```

### Adding a New Field

1. Add a `(internal_name, log_key)` tuple to `_EXTRA_FIELDS` in `pipeline/pipeline_logging.py`
2. Add the field to this guide with internal name, log key, type, description, and example
3. Use `snake_case` for internal names (consistent with project JSON standards)
4. Keep log keys short but recognisable (e.g., `dur_ms`, `chkpt`, `el_ms`)
5. Pass `None` to suppress the field when not applicable — do NOT pass empty strings

---

## Patterns & Examples

### Pattern 1: Lifecycle Events (INFO)

Log once when a subsystem connects, starts, or stops.

```python
# ✅ GOOD: one-time lifecycle event at INFO
log_event(
    logger, "info", "OUTPUT",
    "PostgreSQL pool created",
    host=self._host, port=self._port,
    operation="CONNECT",
)

# ✅ GOOD: pipeline start
log_event(
    logger, "info", "CHANGES",
    "Pipeline starting for job",
    job_id=job_id, mode=output_mode,
)

# ❌ BAD: lifecycle-level message at debug (too easy to miss)
log_event(logger, "debug", "OUTPUT", "PostgreSQL pool created", ...)
```

### Pattern 2: Batch Summary (INFO)

The **single consolidated line** emitted per batch at INFO level. This is the most important log line — it tells the operator everything about what just happened.

```python
# ✅ GOOD: single INFO line per batch with all context
log_event(
    logger, "info", "PROCESSING",
    "%.0fms | batch %d changes | %d ok | %d failed"
    % (batch_elapsed_ms, len(results), batch_success, batch_fail),
    out_ms=round(batch_out_ms, 1),
    seq_from=since,
    seq_to=str(last_seq),
    include_docs=include_docs,
    docs_fetched=docs_fetched_count if not include_docs else None,
    docs_missing=docs_missing if docs_missing else None,
    checkpoint="moved",
)
# Output:
# [INFO] [PROCESSING] job=..54e59 #s:..fff134 #b:a8b0tz changes_worker:
#   312ms | batch 100 changes | 99 ok | 1 failed
#   | out_ms 245.1 | seq_from 625000 | seq_to 625100
#   | inc_docs False | fetched 99 | docs_miss 1 | chkpt moved
```

### Pattern 3: Per-Operation Detail (DEBUG)

Individual doc processing, SQL execution, HTTP requests — anything that fires per-document or per-sub-operation.

```python
# ✅ GOOD: per-doc detail at DEBUG
log_event(
    logger, "debug", "OUTPUT",
    "SQL exec",
    operation="INSERT", doc_id=doc_id,
)

# ✅ GOOD: per-fetch detail at DEBUG
log_event(
    logger, "debug", "HTTP",
    "_bulk_get: requesting %d docs" % count,
    doc_count=count,
)

# ✅ GOOD: checkpoint save at DEBUG (fires every batch)
log_event(
    logger, "debug", "CHECKPOINT",
    "checkpoint saved",
    operation="UPDATE", storage="sg",
)

# ❌ BAD: per-doc detail at INFO (noisy at scale)
log_event(logger, "info", "OUTPUT", "SQL exec", operation="INSERT", doc_id=doc_id)
```

### Pattern 4: Errors and Failures (ERROR/WARN)

```python
# ✅ GOOD: permanent doc failure at ERROR with context
log_event(
    logger, "error", "OUTPUT",
    "permanent error",
    doc_id=doc_id, mode=self._mode,
    error_detail=f"{type(exc).__name__}: {exc}",
)

# ✅ GOOD: DLQ write at WARN (recoverable but notable)
log_event(
    logger, "warn", "DLQ",
    "entry added",
    operation="INSERT", doc_id=dlq_doc_id,
    seq=seq, status=0, doc_type="dlq",
    duration_ms=round(elapsed, 1),
)

# ❌ BAD: swallowing error detail
log_event(logger, "error", "OUTPUT", "something went wrong")

# ❌ BAD: error at WARN (errors are errors)
log_event(logger, "warn", "OUTPUT", "permanent error", doc_id=doc_id)
```

### Pattern 5: Conditional Level (INFO on failure, DEBUG on success)

For stats and summaries that are always useful during failures but noisy during normal operation:

```python
# ✅ GOOD: elevate to INFO only when there's something to investigate
level = "info" if batch_fail else "debug"
log_event(
    logger, level, "PROCESSING",
    "batch complete: %d/%d succeeded, %d failed" % (ok, total, fail),
)

# ✅ GOOD: output stats — useful alongside errors, noise otherwise
def log_stats(self, force_info: bool = False) -> None:
    log_event(
        logger,
        "info" if force_info else "debug",
        "OUTPUT",
        "%s stats: %d ops | avg=%.1fms" % (engine, n, avg),
        mode=self._engine,
    )
```

### Pattern 6: Eventing / Inline Stages (EVENTING)

JavaScript and future inline stages run per-document. Log execution at DEBUG, errors/timeouts at ERROR, and JS `log()` output at INFO (since the user explicitly asked to see it).

```python
# ✅ GOOD: per-doc eventing execution at DEBUG
log_event(logger, "debug", "EVENTING",
    "document rejected by eventing handler",
    doc_id=doc_id)

# ✅ GOOD: JS timeout at ERROR with context
log_event(logger, "error", "EVENTING",
    "OnUpdate timed out after %dms" % timeout_ms,
    doc_id=doc_id, duration_ms=timeout_ms)

# ✅ GOOD: JS runtime error with full detail
log_event(logger, "error", "EVENTING",
    "unexpected handler error: %s: %s" % (type(exc).__name__, exc),
    doc_id=doc_id, error_detail=str(exc))

# ✅ GOOD: handler halt (pipeline-stopping) at ERROR
log_event(logger, "error", "EVENTING",
    "handler halt — stopping pipeline: %s" % halt_exc,
    doc_id=doc_id)

# ✅ GOOD: JS log() output at INFO (user explicitly called log())
log_event(logger, "info", "EVENTING",
    "[js] %s" % msg, doc_id=doc_id)

# ❌ BAD: eventing tagged as PROCESSING
log_event(logger, "error", "PROCESSING", "JS timeout", doc_id=doc_id)
```

### Pattern 7: Retry and Backoff (RETRY)

```python
# ✅ GOOD: retry decision with delay
log_event(
    logger, "warn", "RETRY",
    "retryable error, backing off",
    attempt=attempt, delay_seconds=delay,
    error_detail=str(exc),
)

# ✅ GOOD: retry exhaustion (escalation point)
log_event(
    logger, "error", "RETRY",
    "max retries exhausted",
    attempt=max_retries, doc_id=doc_id,
)
```

---

## Message Formatting Rules

### 1. Use `%`-Style Formatting in the Message String

```python
# ✅ GOOD: %-formatting (consistent with Python logging best practices)
log_event(logger, "info", "PROCESSING",
    "%.0fms | batch %d changes | %d ok | %d failed" % (ms, total, ok, fail))

# ❌ BAD: f-string (evaluates even when level is filtered)
log_event(logger, "info", "PROCESSING",
    f"batch {total} changes | {ok} ok | {fail} failed")
```

**Why**: `%`-formatting in the message string is the Python logging convention. While `log_event` does not use lazy formatting (it accepts a final string), `%`-style keeps the codebase consistent and avoids f-string habits that are costly when used directly with `logger.debug()`.

### 2. Message = Human Summary, Fields = Machine-Parseable Context

The message should be a readable sentence or summary. Put machine-parseable data in structured fields.

```python
# ✅ GOOD: message is readable, fields are parseable
log_event(logger, "error", "OUTPUT",
    "permanent error",
    doc_id=doc_id, mode="postgres",
    error_detail="IntegrityError: duplicate key")

# ❌ BAD: dumping everything into the message
log_event(logger, "error", "OUTPUT",
    "permanent error doc_id=foo_42 mode=postgres error=IntegrityError: duplicate key")
```

### 3. Keep Messages Short and Consistent

Use the same message text for the same event type. This makes `grep` reliable.

```python
# ✅ GOOD: consistent message, varying fields
log_event(logger, "error", "OUTPUT", "permanent error", doc_id=doc_id, ...)
log_event(logger, "error", "OUTPUT", "permanent error", doc_id=other_id, ...)

# ❌ BAD: different messages for the same event
log_event(logger, "error", "OUTPUT", "failed to insert doc foo_42 into orders table", ...)
log_event(logger, "error", "OUTPUT", "could not write doc foo_43", ...)
```

### 4. Do NOT Log Sensitive Data

Passwords, tokens, credentials, and connection strings are auto-redacted by the `Redactor` class, but do not rely on this as the only defense:

```python
# ✅ GOOD: log host/port, NOT credentials
log_event(logger, "info", "OUTPUT", "pool created", host=host, port=port)

# ❌ NEVER: log credentials, even at debug
log_event(logger, "debug", "OUTPUT", "connecting", password=password)
```

---

## Configuration

Logging is configured in `config.json` under the `logging` key. Operators can control verbosity **per log key** without code changes.

### Log File Architecture

File logging splits output into **separate files per level tier**, each with its own rotation and retention:

| File | Contains | Audience |
|------|----------|----------|
| `changes_worker_info.log` | INFO + WARNING | Operators — batch summaries, lifecycle events |
| `changes_worker_debug.log` | DEBUG only | Developers — per-doc detail, SQL, HTTP requests |
| `changes_worker_error.log` | ERROR + CRITICAL | Alerting / on-call — failures, crashes |
| `changes_worker_trace.log` | TRACE only | Local dev — raw payloads, icecream *(only when `log_level: "trace"`)* |

The files are derived from the `path` setting. If `path` is `logs/changes_worker.log`, the system creates `logs/changes_worker_info.log`, `logs/changes_worker_debug.log`, etc.

**Which files are created depends on `log_level`:**

| `log_level` | Files Created |
|-------------|---------------|
| `"info"` | `_info.log`, `_error.log` |
| `"debug"` | `_debug.log`, `_info.log`, `_error.log` |
| `"trace"` | `_trace.log`, `_debug.log`, `_info.log`, `_error.log` |

### Rotation and Compression

When a log file exceeds `max_size` MB, it is rotated:

1. The current file is closed and renamed (e.g. `_info.log` → `_info.log.1`)
2. The rotated file is **gzip-compressed** to `_info.log.1.gz` (saves ~80% disk)
3. Files older than `max_age` days are deleted
4. If total rotated size exceeds `rotated_logs_size_limit` MB, oldest files are pruned

Set `compress_rotated: false` to disable gzip compression.

### Default Configuration

```json
{
  "logging": {
    "redaction_level": "partial",
    "console": {
      "enabled": true,
      "log_level": "info",
      "log_keys": ["*"],
      "key_levels": {}
    },
    "file": {
      "enabled": true,
      "path": "logs/changes_worker.log",
      "log_level": "debug",
      "log_keys": ["*"],
      "key_levels": {},
      "compress_rotated": true,
      "rotation": {
        "max_size": 100,
        "max_age": 7,
        "rotated_logs_size_limit": 1024
      }
    }
  }
}
```

This creates three files: `changes_worker_debug.log`, `changes_worker_info.log`, `changes_worker_error.log`.

**All tiers use the same format.** Here's what each file looks like for the same batch:

**`_info.log`** — startup sequence + batch summary (real output):
```
[INFO] [CONTROL] changes_worker: PipelineManager starting
[INFO] [CONTROL] changes_worker: loaded 1 enabled jobs
[INFO] [CHANGES] changes_worker: job started | job job::2162fb33-6213-456d-93c1-213a64654e59
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: session started | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode session=...551b6a
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: source | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode source=sync_gateway url=http://host.docker.internal:4984 db=db scope=us collection=prices
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: processing | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode sequential=False max_concurrent=20 dry_run=False ignore_delete=False ignore_remove=False write_method=PUT get_batch_number=100
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: output | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode mode=postgres host=? port=? database=? schema=public pool_min=2 pool_max=10 ssl=False
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: dlq | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode dead_letter_path= retention_s=86400 max_replay=10
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: retry | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode max_retries=5 backoff_base=1s backoff_max=60s retry_on_status=[500, 502, 503, 504]
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: checkpoint | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode enabled=True client_id=changes_worker every_n_docs=0
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: shutdown | job job::2162fb33-6213-456d-93c1-213a64654e59 | mode drain_timeout=60s dlq_inflight=False
[INFO] [PROCESSING] job=..54e59 #s:..551b6a changes_worker: source type: sync_gateway
[INFO] [CONTROL] changes_worker: thread monitor started
[INFO] [CONTROL] changes_worker: PipelineManager ready with 1 jobs
[INFO] [OUTPUT] job=..54e59 #s:..551b6a changes_worker: PostgreSQL pool created | op CONNECT | host host.docker.internal | port 5432
[INFO] [MAPPING] job=..54e59 #s:..551b6a changes_worker: loaded schema mapping from CBL | doc_id orders.json | store cbl
[INFO] [OUTPUT] job=..54e59 #s:..551b6a changes_worker: connection pool created | mode postgres
[INFO] [OUTPUT] job=..54e59 #s:..551b6a changes_worker: database output ready (engine=postgres)
[INFO] [CHECKPOINT] job=..54e59 #s:..551b6a changes_worker: checkpoint loaded | op SELECT | store sg
[INFO] [DLQ] job=..54e59 #s:..551b6a changes_worker: no pending dead-letter entries to replay
[INFO] [CHANGES] job=..54e59 #s:..551b6a changes_worker: replication config: feed=websocket, active_only=False, include_docs=False, since=2004869, initial_sync=False, initial_sync_done=True, optimize_initial_sync=False
[INFO] [CHANGES] job=..54e59 #s:..551b6a changes_worker: feed mode: websocket (catch-up → ws stream)
[INFO] [CHANGES] job=..54e59 #s:..551b6a changes_worker: catch-up starting (limit=10000, active_only=False, include_docs=False) | seq 2004869
```

**`_debug.log`** — startup + per-doc detail (real output):
```
# ── Startup (CBL bootstrap, config, job loading) ────────────────────────────
[DEBUG] asyncio: Using selector: EpollSelector
[DEBUG] [CBL] changes_worker: config already in CBL — ignoring config.json | op SELECT | doc_type config
[DEBUG] [CBL] changes_worker: config already migrated to v2.0 | op MIGRATE
[DEBUG] [CBL] changes_worker: collection ready
[DEBUG] [CBL] changes_worker: mapping loaded | op SELECT | doc_id mapping:orders.meta.json | doc_type mapping
[DEBUG] [CBL] changes_worker: mapping loaded | op SELECT | doc_id mapping:orders.json | doc_type mapping
[DEBUG] [CBL] changes_worker: listed mappings | op SELECT | doc_count 2 | doc_type mapping
[DEBUG] [CBL] changes_worker: collection migration: already done
[DEBUG] [CBL] changes_worker: listed 1 jobs | op SELECT | doc_type job | dur_ms 0.5
[DEBUG] [CBL] changes_worker: mapping not found | op SELECT | doc_id mapping:2162fb33-...json | doc_type mapping
[DEBUG] [CONTROL] changes_worker: registered job control endpoints
[DEBUG] [CBL] changes_worker: job loaded | job 2162fb33-... | op SELECT | doc_id job::2162fb33-... | dur_ms 0.1

# ── Per-batch processing detail ─────────────────────────────────────────────
[DEBUG] [PROCESSING] job=..54e59 #s:..c6c6f0 #b:4bck5g changes_worker: batch complete: 1/1 succeeded, 0 failed
```

**`_error.log`** — failures only:
```
[ERROR] [OUTPUT] job=..54e59 #s:..f7ef55 #b:mddtmi changes_worker: permanent error | doc_id foo_99 | err IntegrityError: dup key
```

**`_trace.log`** — raw payloads (only when `log_level: "trace"`):
```
[TRACE] [OUTPUT] job=..54e59 #s:..f7ef55 #b:mddtmi changes_worker: raw payload dump | doc_id foo_0
```

### Per-Key Level Overrides

To get DEBUG-level HTTP logs on console while keeping everything else at INFO:

```json
{
  "logging": {
    "console": {
      "log_level": "info",
      "key_levels": {
        "HTTP": "debug"
      }
    }
  }
}
```

To silence checkpoint logs and show only errors for output:

```json
{
  "logging": {
    "console": {
      "log_level": "info",
      "key_levels": {
        "CHECKPOINT": "error",
        "OUTPUT": "error"
      }
    }
  }
}
```

### Filtering by Log Key

To only see changes feed and DLQ activity:

```json
{
  "logging": {
    "console": {
      "log_keys": ["CHANGES", "DLQ", "PROCESSING"]
    }
  }
}
```

Use `["*"]` to see all keys (default).

---

## Admin API & REST Endpoint Logging

All HTTP endpoints that perform **lifecycle or config-mutating operations** log at `info` level with `[CONTROL]` log key. This ensures operators can see who hit which endpoint and when.

### Lifecycle Endpoints (`/_*` and `/api/_*`)

These are one-shot admin actions. Every call logs on entry:

| Endpoint | Log Message | Fields |
|----------|------------|--------|
| `POST /_restart` | `restart requested via /_restart endpoint` | — |
| `POST /_shutdown` | `graceful shutdown requested via /_shutdown endpoint` | — |
| `POST /_offline` | `offline requested via /_offline endpoint` | — |
| `POST /_online` | `online requested via /_online endpoint` | — |
| `POST /_collect` | `diagnostics collection complete: <filename>` | — |
| `POST /api/jobs/{id}/start` | `starting job` | `job_id` |
| `POST /api/jobs/{id}/stop` | `stopping job` | `job_id` |
| `POST /api/jobs/{id}/restart` | `restarting job` | `job_id` |
| `POST /api/jobs/{id}/kill` | `killing job` | `job_id` |
| `POST /api/_restart` | `restart all requested via /api/_restart` | — |
| `POST /api/_offline` | `offline requested via /api/_offline` | — |
| `POST /api/_online` | `online requested via /api/_online` | — |

Read-only endpoints (`GET /_metrics`, `GET /_status`, `GET /api/jobs/{id}/state`) do **not** log — they fire too frequently.

### CRUD Endpoints (`/api/jobs`, `/api/inputs_changes`, `/api/outputs_*`)

Mutating operations (POST/PUT/DELETE) log on **success** at `info` level:

| Endpoint | Log Message | Fields |
|----------|------------|--------|
| `POST /api/wizard/test-source` | `test-source: begin / ok / got HTTP N / connect failed / timeout / unexpected error` | `http_method`, **`url` (full URL)**, `status`, `elapsed_ms`, `mode` (src type), `error_class`, `error_detail`, `doc_count` |
| `POST /api/source/test` | `source/test: begin / reachable / unexpected HTTP N / connect failed / timeout / unexpected error` | `http_method`, **`url` (full URL)**, `status`, `elapsed_ms`, `error_class`, `error_detail` |
| `POST /api/inputs_changes` | `inputs_changes saved via API` | `doc_count` |
| `PUT /api/inputs_changes/{id}` | `input entry updated via API` | `doc_id` |
| `DELETE /api/inputs_changes/{id}` | `input entry deleted via API` | `doc_id` |
| `POST /api/outputs_{type}` | `outputs_%s saved via API` | `doc_count` |
| `PUT /api/outputs_{type}/{id}` | `output entry updated via API` | `doc_id` |
| `DELETE /api/outputs_{type}/{id}` | `output entry deleted via API` | `doc_id` |
| `POST /api/jobs` | `job created via API` | `job_id` |
| `PUT /api/jobs/{id}` | `job updated via API` | `job_id` |
| `DELETE /api/jobs/{id}` | `job deleted via API` | `job_id` |
| `POST /api/jobs/{id}/refresh-input` | `job input refreshed via API` | `job_id` |
| `POST /api/jobs/{id}/refresh-output` | `job output refreshed via API` | `job_id` |
| `PUT /api/v2/jobs/{id}/mapping` | `job mapping updated via API` | `job_id` |
| `PUT /api/v2/jobs/{id}/eventing` | `job eventing updated via API` | `job_id` |

GET endpoints do not log (read-only, high frequency).

All error paths log at `error` level with `error_detail=` containing the exception type and message.

### Example: Full Startup Sequence (real output)

The complete startup sequence shows the pipeline stages flowing left-to-right (SOURCE → PROCESS → OUTPUT):

```
# ── Infrastructure ──────────────────────────────────────────────────────────
[INFO] [CONTROL] changes_worker: PipelineManager starting
[INFO] [CONTROL] changes_worker: loaded 1 enabled jobs
[INFO] [CHANGES] changes_worker: job started | job job::2162fb33-6213-456d-93c1-213a64654e59

# ── Session & Config Dump (CONTROL) ─────────────────────────────────────────
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: session started | job job::2162fb33-... | mode session=...551b6a
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: source | ... | mode source=sync_gateway ...
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: processing | ... | mode sequential=False max_concurrent=20 ...
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: output | ... | mode mode=postgres host=? port=? ...
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: dlq | ... | mode dead_letter_path= retention_s=86400 max_replay=10
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: retry | ... | mode max_retries=5 backoff_base=1s ...
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: checkpoint | ... | mode enabled=True client_id=changes_worker ...
[INFO] [CONTROL] job=..54e59 #s:..551b6a changes_worker: job config: shutdown | ... | mode drain_timeout=60s dlq_inflight=False
[INFO] [PROCESSING] job=..54e59 #s:..551b6a changes_worker: source type: sync_gateway
[INFO] [CONTROL] changes_worker: thread monitor started
[INFO] [CONTROL] changes_worker: PipelineManager ready with 1 jobs

# ── Output Setup (RIGHT side) ───────────────────────────────────────────────
[INFO] [OUTPUT] job=..54e59 #s:..551b6a changes_worker: PostgreSQL pool created | op CONNECT | host host.docker.internal | port 5432
[INFO] [MAPPING] job=..54e59 #s:..551b6a changes_worker: loaded schema mapping from CBL | doc_id orders.json | store cbl
[INFO] [OUTPUT] job=..54e59 #s:..551b6a changes_worker: connection pool created | mode postgres
[INFO] [OUTPUT] job=..54e59 #s:..551b6a changes_worker: database output ready (engine=postgres)

# ── Checkpoint & DLQ (recovery) ─────────────────────────────────────────────
[INFO] [CHECKPOINT] job=..54e59 #s:..551b6a changes_worker: checkpoint loaded | op SELECT | store sg
[INFO] [DLQ] job=..54e59 #s:..551b6a changes_worker: no pending dead-letter entries to replay

# ── Source Feed (LEFT side) ─────────────────────────────────────────────────
[INFO] [CHANGES] job=..54e59 #s:..551b6a changes_worker: replication config: feed=websocket, active_only=False, include_docs=False, since=2004869, initial_sync=False, initial_sync_done=True, optimize_initial_sync=False
[INFO] [CHANGES] job=..54e59 #s:..551b6a changes_worker: feed mode: websocket (catch-up → ws stream)
[INFO] [CHANGES] job=..54e59 #s:..551b6a changes_worker: catch-up starting (limit=10000, active_only=False, include_docs=False) | seq 2004869
```

### Example: Admin API Calls

```
[INFO] [CONTROL] changes_worker: restart requested via /_restart endpoint
[INFO] [CONTROL] changes_worker: job created via API | job 2162fb33-6213-456d-93c1-213a64654e59
[INFO] [CONTROL] changes_worker: stopping job | job job::2162fb33-6213-456d-93c1-213a64654e59
[ERROR] [CONTROL] changes_worker: error starting job | job job::abc123 | err RuntimeError: job not found
```

### Diagnosing Source-Test Failures from the GUI

When the **Inputs** or **Jobs** page Test button reports a connection
failure, the alert dialog shows the full URL that was tried, the HTTP
status (when available), the error class (e.g. `ClientConnectorError`,
`TimeoutError`), and a pointer to the Logs page. The same data is
emitted as structured log lines so you can correlate the GUI alert with
the audit trail:

```
# Operator clicks "Test" on an input pointing at the wrong port
[INFO] [CONTROL] changes_worker: test-source: begin (auth=basic, src=sync_gateway) | method GET | url http://host.docker.internal:9999/db.us.prices/_changes | mode sync_gateway
[ERROR] [HTTP]  changes_worker: test-source: connect failed (DNS/refused/SSL) | method GET | url http://host.docker.internal:9999/db.us.prices/_changes | el_ms 12.4 | mode sync_gateway | err_cls ClientConnectorError | err Cannot connect to host host.docker.internal:9999 ssl:default [Connection refused]

# Auth fails (wrong password)
[INFO] [CONTROL] changes_worker: test-source: begin (auth=basic, src=sync_gateway) | method GET | url http://host.docker.internal:4984/db.us.prices/_changes | mode sync_gateway
[WARN] [HTTP]  changes_worker: test-source: got HTTP 401 | method GET | url http://...:4984/db.us.prices/_changes | status 401 | el_ms 18.1 | mode sync_gateway | err Unauthorized

# Connected but the changes feed is empty
[INFO] [CONTROL] changes_worker: test-source: begin (auth=basic, src=sync_gateway) | method GET | url http://...:4984/db.us.prices/_changes | mode sync_gateway
[WARN] [CONTROL] changes_worker: test-source: connected but no docs in feed | method GET | url http://...:4984/db.us.prices/_changes | status 200 | el_ms 22.0 | mode sync_gateway

# Happy path
[INFO] [CONTROL] changes_worker: test-source: begin (auth=basic, src=sync_gateway) | method GET | url http://...:4984/db.us.prices/_changes | mode sync_gateway
[INFO] [CONTROL] changes_worker: test-source: ok (100 docs sampled) | method GET | url http://...:4984/db.us.prices/_changes | status 200 | el_ms 38.2 | mode sync_gateway | doc_count 100
```

To diagnose a failed test from the Logs page (filter `[CONTROL]` and/or
`[HTTP]`):

```bash
# Recent source-test attempts
grep 'test-source:' logs/changes_worker_info.log | tail -20

# Just the failures and their full URLs
grep -E 'test-source: (got HTTP|connect failed|timeout|unexpected)' logs/changes_worker_*.log

# All connect refused / DNS / SSL errors across all tests
grep 'test-source: connect failed' logs/changes_worker_error.log
```

The exact `url` field on every line is the **full URL the worker dialed** — including keyspace (`db.scope.collection`) — so you can copy/paste it into `curl` to reproduce.

---

## Anti-Patterns

### ❌ Using `logger.info()` Directly

```python
# ❌ BAD: bypasses structured logging, no log_key tag
logger.info("batch complete: 100 docs processed")

# ✅ GOOD: use log_event
log_event(logger, "info", "PROCESSING", "batch complete", batch_size=100)
```

### ❌ Logging Per-Doc at INFO

```python
# ❌ BAD: 10,000 INFO lines for a 10,000-doc batch
for doc in docs:
    log_event(logger, "info", "OUTPUT", "sent doc", doc_id=doc["_id"])

# ✅ GOOD: one summary at INFO, per-doc at DEBUG
for doc in docs:
    log_event(logger, "debug", "OUTPUT", "sent doc", doc_id=doc["_id"])
log_event(logger, "info", "PROCESSING", "batch sent", batch_size=len(docs))
```

### ❌ Wrong Log Key

```python
# ❌ BAD: checkpoint work tagged as OUTPUT
log_event(logger, "info", "OUTPUT", "checkpoint saved", seq=seq)

# ✅ GOOD: use the correct stage tag
log_event(logger, "info", "CHECKPOINT", "checkpoint saved", seq=seq)
```

### ❌ Unregistered Fields

```python
# ❌ BAD: 'table_name' is not in _EXTRA_FIELDS — it will be silently dropped
log_event(logger, "info", "OUTPUT", "insert", table_name="orders")

# ✅ GOOD: use a registered field, or add the new field to _EXTRA_FIELDS first
log_event(logger, "info", "MAPPING", "table matched", doc_id=doc_id)
```

### ❌ Empty String Instead of None

```python
# ❌ BAD: empty string renders as | key  (ugly, unparseable)
log_event(logger, "info", "PROCESSING", "batch done", docs_missing="")

# ✅ GOOD: None suppresses the field entirely
log_event(logger, "info", "PROCESSING", "batch done", docs_missing=None)
```

---

## Checklist for New Code

Before submitting a PR that adds logging, verify:

- [ ] All log lines use `log_event()` — not `logger.info()` / `logger.debug()` directly
- [ ] Each `log_event()` call uses a valid log key from the [Log Keys Reference](#log-keys-reference)
- [ ] The log key matches the **pipeline stage** where the work is happening
- [ ] Per-document/per-operation messages are at `debug` or lower
- [ ] Batch summaries and lifecycle events are at `info`
- [ ] Error messages include `error_detail=` with the exception info
- [ ] No sensitive data (passwords, tokens, keys) in messages or fields
- [ ] Any new structured fields are added to `_EXTRA_FIELDS` in `pipeline/pipeline_logging.py` AND documented in this guide
- [ ] Any new log key is added to `LOG_KEYS` in `pipeline/pipeline_logging.py` AND documented in this guide
- [ ] Messages use `%`-formatting, not f-strings
- [ ] Messages are short, consistent, and greppable
- [ ] `None` is passed (not `""`) to suppress optional fields
- [ ] REST endpoint handlers that mutate state (POST/PUT/DELETE) have a `log_event` on the success path
- [ ] Lifecycle endpoint handlers (`/_restart`, `/_shutdown`, job start/stop) log at `info` with `[CONTROL]`

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────────────────────┐
│  LEVEL      │  WHEN                                                    │
├─────────────┼──────────────────────────────────────────────────────────┤
│  critical   │  Process cannot continue                                 │
│  error      │  Operation failed, doc goes to DLQ                       │
│  warn       │  Unexpected but recoverable (DLQ write, fallback)        │
│  info       │  Batch summary, lifecycle event (1 per batch max)        │
│  debug      │  Per-operation detail (SQL exec, HTTP req, checkpoint)   │
│  trace      │  Raw payloads, icecream, internal state dumps            │
├─────────────┼──────────────────────────────────────────────────────────┤
│  LOG KEY    │  PIPELINE STAGE                                          │
├─────────────┼──────────────────────────────────────────────────────────┤
│  CHANGES    │  Source: _changes feed polling                           │
│  HTTP       │  Source: HTTP requests (bulk_get, doc fetch)             │
│  PROCESSING │  Process: batch summary, filtering                       │
│  EVENTING   │  Process: JavaScript OnUpdate/OnDelete handlers          │
│  ATTACHMENT │  Process: attachment detect/fetch/upload/post-process     │
│  ENRICHMENT*│  Process: data cleaning, validation, normalization       │
│  INFERENCE* │  Process: ML/AI model inference, embeddings              │
│  MAPPING    │  Process: schema mapping, JSONPath, SQL generation       │
│  FLOOD      │  Process: flood detection / throttle                     │
│  OUTPUT     │  Output: SQL exec, HTTP forward, cloud upload, pool      │
│  CHECKPOINT │  Output: checkpoint save/load                            │
│  DLQ        │  Error: dead letter queue writes/replays                 │
│  RETRY      │  Error: retry decisions, backoff                         │
│  CBL        │  Infra: Couchbase Lite operations                        │
│  METRICS    │  Infra: metrics server                                   │
│  CONTROL    │  Infra: admin API actions                                │
│  SHUTDOWN   │  Infra: graceful shutdown                                │
├─────────────┼──────────────────────────────────────────────────────────┤
│  * = planned future log keys (not yet in LOG_KEYS)                     │
└─────────────┴──────────────────────────────────────────────────────────┘
```

---

## Web UI Log Processing (`server.py` + `logs.html`)

The admin UI Logs & Debugging page reads, parses, and renders log files via a tightly coupled pair:

- **Backend**: `web/server.py` — API endpoint `GET /api/logs`, log file reader, line parser, HTML renderer, chart aggregator
- **Frontend**: `web/templates/logs.html` — pagination controls, filters, charts, insight panel

### ⚠️ Change Impact Matrix

**If you change the log line format** (timestamp, level, key, prefix, or field layout), you MUST update all three layers:

| What Changed | `pipeline/pipeline_logging.py` | `web/server.py` | `web/templates/logs.html` |
|---|---|---|---|
| Timestamp format | `RedactingFormatter` | `_LOG_LINE_NEW_RE`, `_TS_PREFIX_RE` | `LOG_RE_NEW`, `parseTS()` |
| Level names (add/rename) | `LOG_KEYS` | `_LEVEL_RANK`, `_LEVEL_BADGE_CLS` | `activeLevels`, level filter buttons, `levelBadge()` |
| Log key (add/rename) | `LOG_KEYS` | `_LOG_KEY_STAGE`, `_PIPE_FIELD_KEYS` | `LOG_KEY_STAGE`, `activeLogKeys` |
| Prefix tags (job/session/batch) | `RedactingFormatter` | `_JOB_TAG_RE`, `_SESSION_TAG_RE`, `_BATCH_TAG_RE` | prefix regex in `parseLogLine()` |
| Structured field (add/rename) | `_EXTRA_FIELDS` | `_PIPE_FIELD_KEYS` | `KNOWN_FIELDS` in `parseLogLine()` |
| Pipeline stage mapping | — | `_LOG_KEY_STAGE` | `LOG_KEY_STAGE`, stage filter buttons, chart colors |

### Architecture: Single-Pass Processing

The backend processes each log line exactly **once**. When a page of logs is requested, a single loop performs all work:

```
seek to line N (sparse index)  →  readline()  →  parse  →  level filter
                                                    ↓
                                         ┌──────────┼──────────┐
                                         ↓          ↓          ↓
                                    build HTML   aggregate   append entry
                                    (pre-render)  counts     (for client
                                                  + time      filtering)
                                                  buckets
```

The response contains:

| Field | Purpose | Used By |
|---|---|---|
| `html` | Pre-rendered log viewer HTML | `renderLogs()` — direct `innerHTML` set |
| `entries` | Parsed entry objects | Client-side search/filter, insight panel, stakes |
| `counts.levels` | `{ERROR: N, WARNING: N, ...}` | Badge counts, bar chart |
| `counts.stages` | `{source: N, process: N, ...}` | Badge counts, bar chart |
| `time_buckets` | `{"YYYY-MM-DD HH:MM": {...}}` | Activity Timeline, Pipeline Timeline, Log Volume Timeline charts |
| `from_line` / `to_line` / `total_lines` | Line-number pagination state | Pagination controls |

The frontend uses `serverHtml` directly when no client-side filters are active (no search, no toggled-off levels/stages, no time range). When filters ARE active, it falls back to client-side rendering from `entries`.

### Line Number Pagination

Log files append at the bottom, so **line numbers from the top are stable references** — line 3000 is always line 3000 regardless of how many new lines are appended.

| API Parameter | Behavior |
|---|---|
| *(none)* | Latest page (tail): `total_lines - page_size` to end |
| `from_line=2500&page_size=500` | Lines 2500–3000 |
| `before_time=2026-04-24 14:06:34&page_size=2000` | 2000 lines before that timestamp |

Internally:
- **Sparse line index** (`_get_line_info`): counts newlines in 32 KB chunks, records byte offset every 1000 lines. Cached by file mtime+size.
- **Seeking** (`_seek_to_line`): uses the index to jump near the target, then skips remaining lines with `readline()` — read-and-discard, O(1) memory.
- **Time search** (`_find_line_for_time`): binary search on byte offsets (O(log₂ filesize) seeks), then index lookup to convert byte offset → line number.

### File Discovery

`GET /api/log-files` scans `logs/` recursively (`rglob("*.log")`) and returns relative paths. The file picker sends paths like `eventing/eventing.log` to `GET /api/logs?file=...`. Path traversal is blocked (no `..`, resolved path must stay under `logs/`).

### Pipe-Delimited Field Parsing

The message can contain `|` characters as natural-language separators. Structured fields are always at the **end** of the line:

```
LOGGER: 16ms | batch 2 changes | 2 ok | 0 failed | out_ms 245.1 | seq_from 625000 | chkpt moved
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                    MESSAGE                                    STRUCTURED FIELDS
```

The parser scans **backwards** from the last `|`-segment. Segments whose first word is a known field key (from `_PIPE_FIELD_KEYS` / `KNOWN_FIELDS`) are extracted as fields. The scan stops at the first non-field segment. Everything before that boundary is the message.

**Adding a new structured field requires updating both sets:**
1. `_PIPE_FIELD_KEYS` in `web/server.py`
2. `KNOWN_FIELDS` in `web/templates/logs.html` (inside `parseLogLine()`)

### Legacy Format Support

Both parsers try the new format first, then fall back to the legacy format:

| Format | Regex | Field Style |
|---|---|---|
| **New** (`_LOG_LINE_NEW_RE`) | `TIMESTAMP [LEVEL] [KEY] job=.. #s:.. #b:.. LOGGER: MSG \| fields` | pipe-delimited, scan from end |
| **Legacy** (`_LOG_LINE_OLD_RE`) | `TIMESTAMP [LEVEL] LOGGER: MSG [KEY] key=value ...` | `key=value` pairs, whitelist (`_SIMPLE_FIELDS`) |

---

## Related Documentation

- **Implementation**: [`pipeline/pipeline_logging.py`](../pipeline/pipeline_logging.py) — `log_event()`, `_EXTRA_FIELDS`, `LOG_KEYS`, `RedactingFormatter`, `set_batch_id()`, `set_job_tag()`
- **Web UI Backend**: [`web/server.py`](../web/server.py) — `get_logs()`, `_parse_log_line()`, `_render_line_html()`, `_get_line_info()`, `_seek_to_line()`, `_find_line_for_time()`
- **Web UI Frontend**: [`web/templates/logs.html`](../web/templates/logs.html) — `parseLogLine()`, `renderLogs()`, `updateCharts()`, pagination, filters
- **Configuration**: [`config.json`](../config.json) — `logging` section
- **JSON Schema Standards**: [`guides/JSON_SCHEMA.md`](./JSON_SCHEMA.md) — field naming conventions (snake_case)
- **Log Collection**: [`docs/LOG_COLLECTION_API.md`](../docs/LOG_COLLECTION_API.md) — `/_collect` endpoint for gathering diagnostics

---

## Version History

| Version | Date       | Changes |
|---------|------------|---------|
| 1.0     | 2026-04-24 | Initial guide: log keys, levels, fields, patterns, anti-patterns |
| 1.1     | 2026-04-24 | Added batch tracing ID (`#batch_id`) and job tag (`job=..xxxxx`) with contextvars auto-propagation |
| 1.2     | 2026-04-24 | Added job config dump at startup (`_log_job_config`) with sensitive field redaction |
| 1.3     | 2026-04-24 | Split file logging into per-level tiers (`_info`, `_debug`, `_error`, `_trace`), gzip compression of rotated files |
| 1.4     | 2026-04-24 | Renamed batch prefix to `#b:`, added session ID (`#s:..`) for correlating config to runtime across job restarts |
| 1.5     | 2026-04-24 | Moved `[KEY]` to right after `[LEVEL]`, reordered prefix to `job= #s: #b:`, changed structured fields from `key=value` to pipe-delimited `\| key value` |
| 1.6     | 2026-04-24 | Duration-first batch summary (`16ms \| batch 2 ...`), updated all examples to new format, added per-tier output samples |
| 1.7     | 2026-04-24 | Added Web UI Log Processing section: change impact matrix, single-pass architecture, line-number pagination, sparse index, pipe-field parsing, legacy format support |
| 1.8     | 2026-04-24 | Added Admin API & REST Endpoint Logging section. All `/_*` lifecycle and `/api/*` CRUD endpoints now log at INFO with `[CONTROL]`. Fixed 27 f-string → `%`-formatting violations in `storage/cbl_store.py`. Consolidated `docs/LOGGING.md` (v1) into redirect to this guide. |
