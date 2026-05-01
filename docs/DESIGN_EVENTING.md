# Eventing – Design & Architecture

This document describes the design of the Eventing subsystem — a lightweight, Couchbase Eventing–inspired feature that lets users write JavaScript `OnUpdate` / `OnDelete` handler functions that execute inline during changes processing, **after** the `_changes` feed and **before** the Schema Mapper.

**Status:** 🚧 Dev Preview — experimental, under active development. Core implementation complete and wired into the pipeline.

**Related docs:**
- [`DESIGN.md`](DESIGN.md) -- Core pipeline architecture and failure modes
- [`CHANGES_PROCESSING.md`](CHANGES_PROCESSING.md) -- `_changes` feed processing, checkpoints
- [`SCHEMA_MAPPING.md`](SCHEMA_MAPPING.md) -- Schema mapping definitions and transforms
- [`ATTACHMENTS.md`](ATTACHMENTS.md) -- Attachment processing (detect, fetch, upload, post-process)
- [`FAILURE_OPTION_OUTPUT_RDBMS.md`](FAILURE_OPTION_OUTPUT_RDBMS.md) -- Failure analysis: eventing error handling (§2.4, §2.5, §2.6)

**Implementation files:**
- [`eventing/eventing.py`](../eventing/eventing.py) -- Core module: `EventingHandler`, `EventingHalt`, `create_eventing_handler`
- [`eventing/recursion_guard.py`](../eventing/recursion_guard.py) -- Write-back echo suppression: `RecursionGuard`, `create_recursion_guard`
- [`eventing/__init__.py`](../eventing/__init__.py) -- Package exports
- [`rest/api_v2.py`](../rest/api_v2.py) -- REST API: `GET/PUT /api/v2/jobs/{id}/eventing`
- [`rest/changes_http.py`](../rest/changes_http.py) -- Pipeline integration in `_process_one_inner`
- [`main.py`](../main.py) -- Handler creation in `poll_changes()`, Prometheus metrics in `MetricsCollector`
- [`pipeline/pipeline.py`](../pipeline/pipeline.py) -- Eventing config extraction in `_build_job_config`
- [`web/templates/eventing.html`](../web/templates/eventing.html) -- Function Editor UI
- [`json_schema/changes-worker/jobs/schema.json`](../json_schema/changes-worker/jobs/schema.json) -- Job schema with `eventing` property

---

## Overview

Eventing adds a **user-programmable JavaScript stage** to the changes pipeline. The JS handler sits between the `_changes` feed and the Schema Mapper, giving users the ability to inspect, modify, enrich, or reject documents before they reach mapping/output.

```
_changes feed ──► Eventing (JS) ──► Schema Mapper ──► Output
```

Each eventing function is **always connected to a Job** (`json_schema/changes-worker/jobs/`). When a job has eventing enabled, every document passes through the JS handler before proceeding to the schema mapper. When eventing is disabled (the default), documents flow directly to the schema mapper — no V8 overhead.

### Why

- **Familiar model** — developers who know Couchbase Eventing can use the same `OnUpdate` / `OnDelete` pattern.
- **Inline processing** — no separate eventing service to deploy; the JS runs inside the worker process.
- **Gate-keeping** — reject documents that shouldn't reach the schema mapper (return `false`).
- **Pre-processing** — add, remove, or modify fields before the schema mapper sees them.

---

## Pipeline Position

```
┌─────────┐     ┌───────────────────┐     ┌──────────────┐     ┌────────┐
│ _changes│────►│ Eventing (JS)     │────►│ Schema Mapper│────►│ Output │
│  feed   │     │ OnUpdate / OnDelete│     │              │     │        │
└─────────┘     └───────────────────┘     └──────────────┘     └────────┘
                  │                                               
                  └─► return false ──► REJECTED (doc stops here)
```

Eventing is an **optional** stage. The pipeline with and without:

| Mode | Flow |
|---|---|
| **Eventing disabled** (default) | `_changes` → Schema Mapper → Output |
| **Eventing enabled** | `_changes` → JS `OnUpdate`/`OnDelete` → Schema Mapper → Output |

---

## Document Splitting: `doc` vs `meta`

When a document arrives from the `_changes` feed, it is **split** into two objects before being passed to the JS handler:

### Example

Raw change from `_changes`:
```json
{"_id": "foo", "_rev": "1-11111", "some": "data", "count": 42}
```

Split into:

| Parameter | Contents | Description |
|---|---|---|
| `meta` | `{"_id": "foo", "_rev": "1-11111"}` | The document identity — `_id` and `_rev` only |
| `doc` | `{"some": "data", "count": 42}` | The document body — everything **except** `_id` and `_rev` |

The JS handler receives these as two separate arguments:

```javascript
function OnUpdate(doc, meta) {
    // doc  = {"some": "data", "count": 42}
    // meta = {"_id": "foo", "_rev": "1-11111"}
}
```

---

## Handler Signatures & Return Semantics

### `OnUpdate(doc, meta)`

Called for every document create or update (non-delete).

```javascript
function OnUpdate(doc, meta) {
    log("processing", meta._id);

    // Modify the doc before it reaches the Schema Mapper
    doc.processed_at = Date.now();

    return doc;
}
```

**Return values — what they mean:**

| Return | Effect | Use case |
|---|---|---|
| `return doc;` | **Pass doc to Schema Mapper.** The `doc` object is forwarded — this is the most common return. The user may have added, removed, or changed fields on `doc` and all those changes carry forward. | Enrichment, field manipulation |
| `return true;` | **Pass doc to Schema Mapper.** The original (or modified-in-place) `doc` proceeds. | Simple pass-through when no changes needed |
| `return false;` | **Reject.** The document is **not** forwarded to the Schema Mapper. It stops here. | Filtering, gating, conditional skip |
| `return;` (undefined) | **Reject.** Same as `return false;`. | — |
| *(no return statement)* | **Reject.** A function with no `return` implicitly returns `undefined` in JS, which is treated as rejection. | — |

**Rule of thumb:** truthy return = pass, falsy/void return = reject.

> **Why no-return = reject?** It's safer to default to rejection. If a user forgets a `return` statement, the document silently disappearing from the pipeline is easier to debug ("where did my docs go?" → check the handler) than documents leaking through unprocessed. Explicit is better than implicit.

### `OnDelete(meta)`

Called when a document is deleted. Receives `meta` only — there is no document body for deletes.

```javascript
function OnDelete(meta) {
    log("deleted", meta._id);
    return meta;   // forward the delete to the Schema Mapper / Output
}
```

`meta` contains `{"_id": "...", "_rev": "..."}`.

**Return values — same semantics as `OnUpdate`:**

| Return | Effect | Use case |
|---|---|---|
| `return meta;` | **Pass delete to Schema Mapper.** The `meta` object is forwarded. | Normal delete forwarding |
| `return true;` | **Pass delete to Schema Mapper.** The original `meta` proceeds. | Simple pass-through |
| `return false;` | **Reject.** The delete is **not** forwarded to the Schema Mapper. It stops here. | Suppress deletes for certain docs |
| `return;` (undefined) | **Reject.** Same as `return false;`. | — |
| *(no return statement)* | **Reject.** Implicit `undefined` → rejection. | — |

**Example — selective delete forwarding:**

```javascript
function OnDelete(meta) {
    // Only forward deletes for hotel docs
    if (meta._id.startsWith("hotel::")) {
        return meta;
    }
    log("suppressing delete for", meta._id);
    return false;
}
```

---

## Runtime: py_mini_racer (V8)

JavaScript execution uses [py_mini_racer](https://github.com/bpcreech/PyMiniRacer) (V8 engine bindings for Python). One `EventingHandler` (wrapping a `MiniRacer` instance) is created per job and reused for all handler invocations within that job.

```
┌──────────────────────────────────────────────────────────────────┐
│  Python Worker Process                                           │
│                                                                  │
│  ┌─────────────┐    ┌──────────────────────┐    ┌─────────────┐ │
│  │ _changes    │───►│ EventingHandler      │───►│ Schema      │ │
│  │ feed        │    │  MiniRacer (V8)       │    │ Mapper      │ │
│  │             │    │  OnUpdate(doc, meta)  │    │             │ │
│  │             │    │  OnDelete(meta)       │    │             │ │
│  └─────────────┘    └──────────────────────┘    └─────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

**Key properties:**

- **Sandboxed** — V8 cannot access the filesystem, network, or Python internals. No external calls from JS (by design — see [Restrictions](#restrictions)).
- **Memory-limited** — every `mr.call()` is capped at `max_memory=128 MB` (default) to prevent runaway JS from OOM-killing the process.
- **Timeout-enforced** — every `mr.call()` passes `timeout=timeout_ms` to prevent infinite loops.
- **Synchronous** — `mr.call("OnUpdate", doc, meta)` is a synchronous call. The Python side interprets the return value before continuing.
- **Persistent state** — the JS environment persists for the life of the worker. Global variables in JS are shared across invocations (useful for counters, caches, lookup tables).
- **One isolate per job** — each job gets its own `MiniRacer` instance (V8 isolate). No cross-job state leakage. Avoids V8 startup cost per change.

---

## Processing Flow

For each change from the `_changes` feed:

```
1. Raw change arrives: {"_id":"foo", "_rev":"1-111", "some":"data"}
         │
         ▼
2. Split into doc + meta
   doc  = {"some":"data"}
   meta = {"_id":"foo", "_rev":"1-111"}
         │
         ▼                                    ┌──────────────────┐
3. Is it a delete?                            │  Metrics tracked │
    │                                         │  per invocation: │
    ├── YES ──► mr.call("OnDelete", meta)     │  • timing (ms)   │
    │           (timeout + max_memory)         │  • pass/reject   │
    │                                         │  • error/timeout  │
    └── NO  ──► mr.call("OnUpdate", doc, meta)│  • V8 heap stats │
                   (timeout + max_memory)      └──────────────────┘
                   │
                   ▼
               4. Interpret return value:
                   ├── doc/meta dict ──► forward to Schema Mapper
                   ├── true          ──► forward original doc/meta
                   ├── false         ──► REJECT (stop, do not forward)
                   └── undefined     ──► REJECT (stop, do not forward)
                   │
                   ▼
               5. On error/timeout → apply policy:
                    ├── "reject"  ──► REJECT, log, continue
                    ├── "pass"   ──► forward original doc, log, continue
                    └── "halt"   ──► raise EventingHalt, stop job (no auto-restart)
               
               5b. On unexpected exception (non-policy):
                    └── catch, log ERROR, route to DLQ (error_class: "eventing"), continue
                    │
                   ▼
               6. Schema Mapper ──► Output
```

---

## Job Binding

Every eventing function is **bound to a Job**. The job document (`json_schema/changes-worker/jobs/`) will contain eventing configuration:

```jsonc
{
  "type": "job",
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Hotel Sync Job",
  "inputs": [ ... ],
  "outputs": [ ... ],
  "output_type": "rdbms",

  "eventing": {
    "enabled": true,
    "handler": "function OnUpdate(doc, meta) {\n  return doc;\n}\n\nfunction OnDelete(meta) {\n  log(\"deleted\", meta._id);\n}",
    "timeout_ms": 5000,
    "on_error": "reject",
    "on_timeout": "reject",
    "description": "Filter and enrich hotel docs before mapping",
    "constants": [
      {"key": "pie", "value": 3.14},
      {"key": "max_retries", "value": 3},
      {"key": "region", "value": "us-east-1"}
    ]
  },

  "mapping": { ... },
  "state": { "status": "running" }
}
```

### Eventing Config Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | boolean | `false` | Whether eventing is active for this job |
| `handler` | string | — | The JS source code containing `OnUpdate` and/or `OnDelete` |
| `timeout_ms` | integer | `5000` | Max execution time per handler invocation (ms). V8 is terminated if exceeded. |
| `on_error` | string | `"reject"` | What happens when the JS handler throws an exception. See [Error & Timeout Behavior](#error--timeout-behavior). |
| `on_timeout` | string | `"reject"` | What happens when `timeout_ms` is exceeded. See [Error & Timeout Behavior](#error--timeout-behavior). |
| `description` | string | `""` | Human-readable description of what this handler does (shown in UI) |
| `constants` | array | `[]` | User-defined constants injected into the JS environment (see below) |

### Error & Timeout Behavior

When a JS handler throws an exception or exceeds `timeout_ms`, the `on_error` / `on_timeout` field controls what happens to that document:

| Value | Behavior |
|---|---|
| `"reject"` (default) | The document is **rejected** — it does not reach the Schema Mapper. The error is logged. Processing continues with the next document. |
| `"pass"` | The document is **passed through** to the Schema Mapper as-is (original `doc`, unmodified). The error is logged but the doc is not lost. |
| `"halt"` | The **job is stopped**. The checkpoint does NOT advance. Same behavior as `halt_on_failure` in the output stage. Use when eventing logic is critical and you'd rather stop than process incorrectly. |

### Constants

Constants are key-value pairs made available as global variables inside the JS environment. They are **read-only** from the user's perspective — injected once when the handler is loaded.

```jsonc
"constants": [
  {"key": "pie", "value": 3.14},
  {"key": "max_retries", "value": 3},
  {"key": "region", "value": "us-east-1"}
]
```

Inside the JS handler, these are available as globals:

```javascript
function OnUpdate(doc, meta) {
    log("region is", region);        // "us-east-1"
    doc.area = doc.radius * pie;     // 3.14
    return doc;
}
```

**Implementation:** Before evaluating the user's handler code, the worker generates a preamble that declares each constant:

```javascript
const pie = 3.14;
const max_retries = 3;
const region = "us-east-1";
```

This preamble is prepended to the handler source and evaluated together via `mr.eval(preamble + handler)`.

**Key validation:** Constant keys must be valid JavaScript identifiers (`^[A-Za-z_$][A-Za-z0-9_$]*$`). Invalid keys are logged and skipped — they do not cause the handler to fail.

---

## Built-in JS Helpers

The following functions are available inside the JS environment:

| Function | Description |
|---|---|
| `log(msg, ...)` | Log a message (forwarded to Python `logger.info`) |

---

## Restrictions

The following are **intentionally not supported** in the current design:

| Feature | Status | Notes |
|---|---|---|
| **External HTTP calls** (`curl`, `fetch`) | ❌ Not supported | V8 sandbox has no network access. May be added in a future version. |
| **Bucket aliases** (write to other collections) | ❌ Not supported | Planned for future. |
| **Timers** (`createTimer`) | ❌ Not supported | Planned for future. |
| **N1QL / SQL++** | ❌ Not supported | Planned for future. |
| **File system access** | ❌ Not supported | V8 sandbox — by design. |

The eventing handler is a **pure transform/filter** — it can modify or reject documents, but it cannot perform side effects.

---

## Module Layout

```
eventing/
├── __init__.py           # Package exports: EventingHandler, EventingHalt, create_eventing_handler, RecursionGuard, create_recursion_guard
├── eventing.py           # Core module: EventingHandler class, V8 lifecycle, metrics
└── recursion_guard.py    # Write-back echo suppression: RecursionGuard (TTL-bounded LRU)
```

### Key Classes & Functions

| Symbol | Purpose |
|---|---|
| `EventingHandler` | Wraps a `MiniRacer` V8 isolate. Handles doc/meta split, handler invocation, return-value interpretation, error/timeout policies, metrics collection. Supports context manager (`with`) for clean teardown. |
| `EventingHalt` | Exception raised when `on_error="halt"` or `on_timeout="halt"` is triggered. Caught by the pipeline to stop the job. `Pipeline` sets `_eventing_halt = True`, and `PipelineManager` **skips auto-restart** — the job stays in error state until manually restarted (see [FAILURE_OPTION_OUTPUT_RDBMS.md §2.4](FAILURE_OPTION_OUTPUT_RDBMS.md)). |
| `create_eventing_handler(cfg, metrics)` | Factory function. Reads a job's `eventing` config dict, returns an `EventingHandler` if `enabled=True`, else `None`. |
| `RecursionGuard` | TTL-bounded LRU cache (`OrderedDict`) that tracks `_id → _rev` for documents the pipeline has written back. Detects and suppresses echoes to prevent infinite recursion loops. |
| `create_recursion_guard(cfg)` | Factory function. Reads a job's `recursion_guard` config dict, returns a `RecursionGuard` if `enabled=True`, else `None`. |

### Pipeline Integration

The eventing handler and recursion guard are wired into the pipeline at these points:

| File | What happens |
|---|---|
| `pipeline/pipeline.py` → `_build_job_config()` | Extracts `eventing` and `recursion_guard` from the job document into the pipeline config dict. |
| `main.py` → `poll_changes()` | Calls `create_eventing_handler(cfg["eventing"], metrics)` and `create_recursion_guard(cfg["recursion_guard"])`, passes both into `batch_kwargs`. |
| `rest/changes_http.py` → `_process_one_inner()` | After `_resolve_doc`: (1) checks `recursion_guard.is_echo(doc_id, rev)` — if true, skips the doc; (2) then runs eventing handler before the attachment stage. |
| `rest/changes_http.py` → `_process_changes_batch()` | Accepts `eventing_handler=` and `recursion_guard=` kwargs, passes them to `_process_one_inner`. |
| `rest/changes_http.py` → `_catch_up_normal()`, `_consume_continuous_stream()`, `_consume_websocket_stream()` | All accept and forward `eventing_handler=` and `recursion_guard=` to `_process_changes_batch`. |

---

## UI: Function Editor

The Eventing UI (`/eventing`) provides a CodeMirror-based JavaScript editor modeled after the Couchbase Eventing function editor.

**Route:** `GET /eventing` → `web/templates/eventing.html`

### UI Features

- **Job selector dropdown** — loads all jobs via `GET /api/v2/jobs`, auto-selects from `?job_id=` URL param.
- **CodeMirror 5 editor** (local, no CDN) — JavaScript syntax highlighting, line numbers, bracket matching, code folding.
- **Tab bar** — JS editor tab and Settings tab.
- **Settings tab** — configure timeout (ms), on_error policy, on_timeout policy, description, and constants array (key/value pairs with add/remove).
- **Enabled toggle** — enable/disable eventing for the selected job.
- **Save button** — persists handler code + all settings to the job document via `PUT /api/v2/jobs/{id}/eventing`.
- **Reload button** — re-fetches eventing config from the API via `GET /api/v2/jobs/{id}/eventing`.
- **Debug panel** — test handlers against sample JSON documents with a live result preview. Supports simulating deletes. Runs in the browser (not V8) for instant feedback.
- **Dev Preview banner** — clearly marks the feature as experimental.

### REST API

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v2/jobs/{id}/eventing` | Get eventing config for a job. Returns default scaffold if none set. |
| `PUT` | `/api/v2/jobs/{id}/eventing` | Update eventing config. Validates handler (string), timeout_ms (100–60000), on_error/on_timeout (reject/pass/halt), constants (array of `{key, value}`). Merges into existing config. |

### Planned UI Features

- **Function list page** — view/manage all eventing functions across all jobs.
- **Log viewer** — show `log()` output from JS handlers in real time.
- **Handler diff** — show what changed before saving.

---

## Prometheus Metrics

Eventing exposes 11 metrics on the `/_metrics` endpoint, following the same `changes_worker_` prefix convention as all other metrics.

### Counters

| Metric | Description |
|---|---|
| `changes_worker_eventing_invocations_total` | Total handler calls (OnUpdate + OnDelete) |
| `changes_worker_eventing_updates_total` | OnUpdate calls |
| `changes_worker_eventing_deletes_total` | OnDelete calls |
| `changes_worker_eventing_passed_total` | Documents that passed through (truthy return) |
| `changes_worker_eventing_rejected_total` | Documents rejected by handler (falsy/void return) |
| `changes_worker_eventing_errors_total` | JS exceptions (on_error policy applied) |
| `changes_worker_eventing_timeouts_total` | Handler exceeded timeout_ms (on_timeout policy applied) |
| `changes_worker_eventing_halts_total` | `on_error=halt` or `on_timeout=halt` triggered — job stopped |

### Gauges

| Metric | Description |
|---|---|
| `changes_worker_eventing_v8_heap_used_bytes` | V8 isolate heap used bytes (sampled every 100 invocations) |
| `changes_worker_eventing_v8_heap_total_bytes` | V8 isolate heap total bytes |

### Summary

| Metric | Description |
|---|---|
| `changes_worker_eventing_handler_duration_seconds` | Time spent in JS handler per invocation (p50, p90, p99) |

### Implementation

Metrics are collected inside `EventingHandler.process_change()`:

- **Timing:** `time.monotonic()` before/after each invocation → `record_eventing_handler_time(elapsed)`.
- **Counters:** incremented via `metrics.inc("eventing_*_total")` at each decision point.
- **Heap stats:** sampled every `_HEAP_STATS_INTERVAL` (100) invocations to avoid per-call overhead.
- **MetricsCollector fields:** defined in `main.py` `MetricsCollector.__init__`, rendered in `MetricsCollector.render()`.

---

## V8 Best Practices

The following best practices are applied based on [PyMiniRacer architecture docs](https://bpcreech.com/PyMiniRacer/architecture/) and embedded-V8 community guidance:

| Practice | Implementation |
|---|---|
| **Memory limit** | `max_memory=128 MB` passed to every `mr.call()`. Prevents runaway JS (e.g., `while(true) arr.push(1)`) from OOM-killing the Python process. |
| **Timeout enforcement** | `timeout=timeout_ms` on every `mr.call()`. Prevents infinite loops from blocking the pipeline. |
| **One isolate per job** | Each `EventingHandler` creates its own `MiniRacer` instance. No cross-job state pollution. If one job's handler corrupts global state, other jobs are unaffected. |
| **Constant key validation** | Keys validated against `^[A-Za-z_$][A-Za-z0-9_$]*$`. Prevents JS injection via malformed constant names (e.g., `key: "x; eval('...')"` is rejected). |
| **Context manager** | `EventingHandler` supports `with handler:` for explicit V8 teardown. Ensures the V8 isolate is released even on exceptions. |
| **Periodic heap monitoring** | V8 heap stats sampled every 100 invocations and pushed to Prometheus gauges. Allows alerting before hitting the hard limit. |
| **Sandboxed by default** | V8 has no filesystem, network, or Python access. User JS is a pure function — cannot perform side effects. |
| **`EventingHalt` exception** | When `on_error=halt` or `on_timeout=halt`, a typed exception is raised and caught by the pipeline to stop the job cleanly (checkpoint not advanced). PipelineManager skips auto-restart for this failure type — manual restart required after fixing the issue. |
| **No global module-level V8** | No `MiniRacer()` at import time. V8 is only instantiated when a job with `eventing.enabled=true` starts. Zero overhead for jobs without eventing. |

---

## Design Decisions & Trade-offs

| Decision | Rationale |
|---|---|
| **Position: after `_changes`, before Schema Mapper** | Gives the user a chance to reshape or reject docs before mapping. The mapper sees a clean, pre-processed document. |
| **doc/meta split** | Mirrors Couchbase Eventing. Keeps identity (`_id`, `_rev`) separate from user data. The handler works on pure data without accidentally mangling the doc ID. |
| **Truthy = pass, falsy/void = reject** | Safer default — forgetting a `return` rejects rather than leaking docs through. Easy to debug: "docs missing" → check the handler. |
| **Same return semantics for OnDelete** | OnDelete uses the same truthy/falsy pattern as OnUpdate. `return meta` passes, `return false` rejects. Consistent mental model — one rule for both handlers. |
| **py_mini_racer (V8)** over a custom DSL | V8 gives full ECMAScript support; users can write real JS, not a restricted subset. Sandboxed by default. |
| **No external calls** | Keeps the handler fast and predictable. No network latency, no failure modes from external services inside the JS sandbox. Side effects belong in Python. |
| **Constants array** | Avoids hardcoded values in JS. Constants can be changed per-job without editing handler code. |
| **Bound to a Job** | One eventing function per job. The handler is stored in the job document — no separate eventing service or config. |
| **Synchronous JS calls** | `mr.call()` blocks the Python event loop briefly. Acceptable for simple transforms; may need worker-thread offloading for heavy computation. |
| **One MiniRacer per job** | Avoids V8 startup cost per change. Global JS state is shared within a job — by design, not a bug. Isolated across jobs. |
| **128 MB max_memory** | Large enough for realistic transforms (JSON manipulation, string ops). Small enough to prevent a single handler from killing the process. Configurable if needed. |
| **Local CodeMirror** | All JS/CSS assets bundled in `web/static/` — no CDN dependencies. Works fully offline / air-gapped. |
| **Dev Preview gating** | Exposed via `?dev=true` query param in sidebar. Not shown to users by default until stable. |

---

## Recursion Guard (Write-back Echo Suppression)

When the pipeline supports HTTP PUT write-backs (e.g. cURL from JS, bucket aliases), a document that is processed and PUT back to the same source will appear again on the `_changes` feed, creating an infinite recursion loop. The **Recursion Guard** detects and suppresses these echoes.

### How It Works

```
1. Pipeline processes doc "hotel::123" → eventing handler modifies it
2. Handler PUTs modified doc back to source → source returns new _rev "3-abc"
3. recursion_guard.record("hotel::123", "3-abc")     ← track the write-back
4. _changes feed delivers {"id":"hotel::123", "_rev":"3-abc"}
5. recursion_guard.is_echo("hotel::123", "3-abc")    ← returns True → SKIP
6. Document is suppressed, no infinite loop
```

### Pipeline Position

```
_changes feed ──► _resolve_doc ──► RECURSION GUARD ──► Eventing (JS) ──► Schema Mapper ──► Output
                                        │
                                        └─► echo detected ──► SUPPRESSED (doc stops here)
```

The guard sits **before** the eventing handler — echoes are suppressed before any JS execution overhead.

### Configuration

The recursion guard is configured per job via the `recursion_guard` property:

```jsonc
{
  "recursion_guard": {
    "enabled": true,
    "max_tracked_docs": 50000,
    "ttl_seconds": 300
  }
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | boolean | `false` | Whether the recursion guard is active for this job. |
| `max_tracked_docs` | integer | `50000` | Maximum number of doc IDs to track in the in-memory LRU cache. When exceeded, the oldest entries are evicted. Range: 100–1,000,000. |
| `ttl_seconds` | integer | `300` | Time-to-live in seconds for tracked entries. Entries older than this are lazily expired. Range: 10–86,400. |

### Implementation

- **Data structure:** `OrderedDict` used as an LRU cache with per-entry TTL. Pure stdlib — no external dependencies.
- **`record(doc_id, rev)`** — called after a successful write-back PUT. Stores `{doc_id: (rev, timestamp)}`. Evicts oldest when `max_tracked_docs` exceeded.
- **`is_echo(doc_id, rev)`** — called in `_process_one_inner` right after `_resolve_doc`. If `doc_id` is tracked and `rev` matches, returns `True` (echo), consumes the entry, and the doc is skipped. Also lazily evicts expired entries.
- **Memory:** Each entry ≈ 200 bytes. At 50,000 entries ≈ 10 MB — negligible.
- **Loss on restart:** The cache is in-memory only. On restart, the guard is empty — worst case is one re-process of recently written-back docs. This is acceptable because the pipeline is idempotent.

### Prometheus Metric

| Metric | Type | Description |
|---|---|---|
| `changes_worker_recursion_guard_suppressed_total` | **counter** | Changes suppressed by the recursion guard (write-back echo detected). |

### UI

The recursion guard settings appear in the **Advanced** section of the Job Builder (`web/templates/jobs.html`):
- **Enabled** toggle
- **Max Tracked Docs** input (default 50,000)
- **TTL (seconds)** input (default 300)

---

## Future Considerations

- **Bucket aliases** — named aliases like `cake32[docId] = doc` to write to other buckets/collections from inside JS handlers.
- **Timer callbacks** — `createTimer()` / `cancelTimer()` for scheduled/delayed operations.
- **cURL from JS** — `curl(url, options)` helper to make HTTP requests from inside JS handlers.
- **N1QL / SQL++ from JS** — `N1QL()` helper to run queries from inside JS handlers.
- **Worker-thread execution** — offload `mr.call()` to a thread pool to avoid blocking the event loop.
- **Handler versioning** — store handler code revisions in the job document so users can roll back.
- **Multi-function support** — allow multiple eventing functions per job, each with its own source filter.
- **Configurable max_memory** — expose `max_memory_bytes` in the job eventing config (currently hardcoded at 128 MB).
- **Live log viewer** — stream `log()` output from JS handlers to the UI in real time via WebSocket.
- **Metrics dashboard** — eventing-specific charts in the admin UI (invocation rate, rejection rate, p99 latency).
