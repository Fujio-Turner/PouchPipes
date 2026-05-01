# Dead Letter Queue (DLQ) — Deep Dive

The dead letter queue is where documents go to survive. When the output target rejects or cannot receive a document after all retries are exhausted, the DLQ captures it — full document body, error context, and metadata — so nothing is silently lost.

**Related docs:**
- [`DESIGN.md`](DESIGN.md) — Three-stage pipeline architecture & failure modes
- [`JOBS.md`](JOBS.md) — Job ID concept, per-engine/per-job metrics
- [`CBL_DATABASE.md`](CBL_DATABASE.md) — DLQ storage schema in Couchbase Lite
- [`CONFIGURATION.md`](CONFIGURATION.md) — Full config reference
- [`FAILURE_OPTION_OUTPUT_RDBMS.md`](FAILURE_OPTION_OUTPUT_RDBMS.md) — Failure analysis & resolutions for SOURCE → PROCESS → OUTPUT (RDBMS)

---

## What Triggers the DLQ

The DLQ is active in two scenarios:

1. **`halt_on_failure: false`** — the worker skips failed docs instead of stopping; all failures go to the DLQ
2. **`data_error_action: "dlq"`** (default) — permanent data errors (type mismatches, constraint violations, 4xx) are routed to the DLQ even when `halt_on_failure: true`. These errors will never self-heal on retry, so the pipeline advances past them.
3. **Process stage errors** — doc fetch failures (non-404/409), unexpected eventing handler exceptions, and attachment processing failures are routed to the DLQ when enabled. These are caught and classified instead of crashing the pipeline (see [FAILURE_OPTION_OUTPUT_RDBMS.md](FAILURE_OPTION_OUTPUT_RDBMS.md) §2.1, §2.6, §2.7).

A **storage backend** must also exist — either CBL is available (automatic) or `dead_letter_path` is set (file fallback).

### Error classification: transient vs. permanent

The system classifies every output error as either **transient** (retry with backoff) or **permanent** (straight to DLQ). This classification drives retry behavior, `halt_on_failure` applicability, and DLQ routing.

| Category | RDBMS examples | HTTP examples | Retried? | `halt_on_failure` applies? | DLQ behavior |
|---|---|---|---|---|---|
| **Transient** | Connection refused, timeout, deadlock, lock contention, resource exhaustion, table not found, server shutdown | 404, 408, 429, 500, 502, 503, 504, 507, connection error | ✅ Yes — exponential backoff | ✅ Yes — if retries exhaust | `halt_on_failure: true` → stop batch; `false` → DLQ |
| **Permanent (data)** | Constraint violation, data type mismatch, null violation | 400, 409, 410, 413, 422 | ❌ No | ❌ No — never halts | Controlled by `data_error_action`: `"dlq"` (store) or `"skip"` (drop) |
| **Permanent (config)** | Auth failure, permission denied, invalid database, read-only | 401, 403, 405, 415, 501 | ❌ No | ❌ No — never halts | Controlled by `data_error_action` |

**Key design principle:** Transient errors are "try again later" — the output may recover (server restart, table recreated, rate limit lifted). Permanent errors are "this data/config will never work" — retrying wastes time and blocks the pipeline.

**Table not found is transient.** A `DROP TABLE` followed by `CREATE TABLE` is a valid operational pattern (schema migrations, maintenance). The pipeline retries with backoff, and once the table is recreated, writes succeed without manual intervention. This applies across all RDBMS engines: PostgreSQL (`42P01`), MySQL (`1146`), MSSQL (`208`), Oracle (`ORA-00942`).

### The trigger path in code

```
document → output.send() → classify error
                               │
                    ┌──────────┴──────────┐
                data error             infrastructure error
                    │                         │
          data_error_action?          halt_on_failure?
          ┌────┴────┐              ┌─────┴─────┐
        "dlq"     "skip"         true         false
          │         │              │            │
     write DLQ,  log warn,     raise         return
     advance    advance     OutputEndpointDown {ok: false}
     checkpoint checkpoint        │            │
                            stop batch,    write to DLQ,
                            hold checkpoint advance checkpoint
```

### Specific triggers

| Scenario | Error class | Transient? | DLQ entry created? |
|---|---|---|---|
| DB constraint violation (unique, FK, null, check) | `constraint_violation` | ❌ Permanent | ✅ If `data_error_action: "dlq"` |
| DB data type mismatch (overflow, bad cast) | `data_type` | ❌ Permanent | ✅ If `data_error_action: "dlq"` |
| DB syntax / schema error (wrong column name) | `syntax_or_schema` | ❌ Permanent | ✅ If `data_error_action: "dlq"` |
| DB auth / permission denied | `auth_failure` | ❌ Permanent | ✅ If `data_error_action: "dlq"` |
| DB table not found (table dropped) | `table_not_found` | ✅ Transient | Only after retries exhaust + `halt_on_failure: false` |
| DB connection refused / timeout | `connection` | ✅ Transient | Only after retries exhaust + `halt_on_failure: false` |
| DB deadlock / lock contention | `deadlock`, `lock_contention` | ✅ Transient | Only after retries exhaust + `halt_on_failure: false` |
| DB resource exhaustion (memory, disk, connections) | `resource_exhaustion` | ✅ Transient | Only after retries exhaust + `halt_on_failure: false` |
| DB server shutdown / restart | `server_shutdown` | ✅ Transient | Only after retries exhaust + `halt_on_failure: false` |
| HTTP 400/422 (bad request) | `bad_request` | ❌ Permanent | ✅ If `data_error_action: "dlq"` |
| HTTP 401/403 (auth failure) | `auth_failure` | ❌ Permanent | ✅ If `data_error_action: "dlq"` |
| HTTP 409 (conflict) | `conflict` | ❌ Permanent | ✅ If `data_error_action: "dlq"` |
| HTTP 410 (gone) | `gone` | ❌ Permanent | ✅ If `data_error_action: "dlq"` |
| HTTP 404 (not found) | `not_found` | ✅ Transient | Only after retries exhaust + `halt_on_failure: false` |
| HTTP 408 (request timeout) | `client_timeout` | ✅ Transient | Only after retries exhaust + `halt_on_failure: false` |
| HTTP 429 (rate limited) | `rate_limited` | ✅ Transient | Only after retries exhaust + `halt_on_failure: false` |
| HTTP 500/502/503/504/507 (server error) | `service_unavailable`, `bad_gateway`, etc. | ✅ Transient | Only after retries exhaust + `halt_on_failure: false` |
| HTTP 501/505 (not implemented) | `server_permanent:5xx` | ❌ Permanent | ✅ If `data_error_action: "dlq"` |
| HTTP 3xx (redirect, `follow_redirects=false`) | `redirect` | ❌ | ✅ If `halt_on_failure: false` |
| Connection refused / DNS failure | `connection_failure` | ✅ Transient | Only after retries exhaust + `halt_on_failure: false` |
| Shutdown with `dlq_inflight_on_shutdown: true` | `shutdown_inflight` | — | ✅ Yes |
| Doc fetch failed (non-404/409, retries exhausted) | `fetch_failed` | ✅ Transient (retried) | ✅ If `data_error_action: "dlq"` |
| Eventing handler unexpected exception | `eventing` | ❌ Permanent | ✅ If `data_error_action: "dlq"` |
| Attachment processing failure | `attachment` | ❌ Permanent | ✅ If `data_error_action: "dlq"` |
| Validation strict mode failure | `validation` | ❌ Permanent | ✅ If `data_error_action: "dlq"` |
| Multi-row INSERT partial failure (row-by-row fallback) | `partial` | ❌ Permanent | ✅ If `data_error_action: "dlq"` |
| No schema mapping loaded | `no_mapping` | ❌ Config gap | ❌ Skipped (not a data error) |
| Successful delivery (2xx) | `success` | — | ❌ No |
| Transient failure + `halt_on_failure: true` | Exception raised, batch stops | — | ❌ No (checkpoint held instead) |
| Permanent error + `data_error_action: "skip"` | Logged and dropped | — | ❌ No (dropped) |

---

## How Data Flows In

### Per-document write (inside a batch)

When a document fails delivery in `_process_changes_batch`, the flow is:

```
for each change in batch:
    result = output.send(doc)
    if result.ok:
        batch_success++
    else:
        batch_fail++
        metrics.inc("dead_letter_total")    ← Prometheus counter
        dlq.write(doc, result, seq)         ← stores in CBL or file
```

Each failed document is written individually to the DLQ via `DeadLetterQueue.write()`. The write captures:

| Field | Value | Purpose |
|---|---|---|
| `doc_id_original` | The Couchbase document `_id` | Identify which doc failed |
| `seq` | The `_changes` sequence number | Know where in the feed this came from |
| `method` | `PUT` or `DELETE` | What operation was attempted |
| `status` | HTTP status code (0 = connection failure) | Classify the error type |
| `error` | Error message or response body | Debug the root cause |
| `reason` | Classification string (e.g., `data_error:data_type`, `server_error:500`) | Why this doc is in the DLQ — answers "what went wrong?" at a glance |
| `time` | Unix epoch when the failure occurred | Timeline of failures |
| `retried` | `false` | Tracks whether replay has been attempted |
| `replay_attempts` | `0` | Number of times replay has been attempted (incremented each failure) |
| `target_url` | The output URL at write time | Detect orphaned entries after endpoint changes |
| `doc_data` | Full document body (JSON string) | The actual data to retry |

### Reason codes

The `reason` / `error_class` field provides a machine-readable classification for why a document ended up in the DLQ:

#### RDBMS error classes

| Error class | Transient? | Meaning | Likely fix |
|---|---|---|---|
| `constraint_violation` | ❌ | Unique, FK, check, or null constraint violated | Fix the source data or schema mapping |
| `data_type` | ❌ | Value doesn't fit the target column type (overflow, bad cast) | Fix the schema mapping or add a transform |
| `syntax_or_schema` | ❌ | Invalid column name, bad SQL syntax | Fix the schema mapping |
| `table_not_found` | ✅ | Table/view does not exist (may be recreated) | Recreate the table; retries will succeed |
| `auth_failure` | ❌ | Invalid credentials or insufficient privileges | Fix DB user permissions |
| `invalid_database` | ❌ | Database or schema does not exist | Fix connection config |
| `read_only` | ❌ | Server is in read-only mode (MySQL) | Wait for maintenance to complete |
| `connection` | ✅ | TCP connection refused, reset, or lost | Check network/firewall; will auto-retry |
| `timeout` | ✅ | Query or connection timed out | May resolve on its own |
| `deadlock` | ✅ | Deadlock detected, transaction rolled back | Automatic retry succeeds |
| `lock_contention` | ✅ | Lock wait timeout or resource busy | Automatic retry succeeds |
| `resource_exhaustion` | ✅ | Out of memory, disk full, too many connections | Free resources; will auto-retry |
| `server_shutdown` | ✅ | DB server shutting down or restarting | Wait for restart; will auto-retry |
| `serialization` | ✅ | Snapshot too old or serialization failure (Oracle) | Automatic retry succeeds |
| `fetch_failed` | ✅ | Doc fetch returned non-404/409 error after retries | Check source availability; replay from DLQ |
| `eventing` | ❌ | Unexpected exception in user's eventing handler | Fix the JS handler code |
| `attachment` | ❌ | Attachment processing (detect/fetch/upload) failed | Check attachment config and cloud credentials |
| `validation` | ❌ | Strict validation failed — field cannot be coerced | Fix the source data or relax strict mode |
| `partial` | ❌ | Multi-row INSERT: some rows failed (row-by-row fallback) | Inspect `failed_ops` in DLQ entry for per-row errors |

#### HTTP error classes

| Error class | Transient? | Meaning | Likely fix |
|---|---|---|---|
| `bad_request` | ❌ | HTTP 400/422 — malformed or invalid payload | Fix the document or schema mapping |
| `auth_failure` | ❌ | HTTP 401/403/407 — authentication failure | Fix output auth config |
| `conflict` | ❌ | HTTP 409 — data conflict at the target | Resolve the conflict in the target system |
| `gone` | ❌ | HTTP 410 — resource permanently removed | Update target URL |
| `method_or_media` | ❌ | HTTP 405/415 — wrong method or content type | Fix output config |
| `payload_too_large` | ❌ | HTTP 413 — document too large for endpoint | Reduce doc size or increase server limit |
| `not_found` | ✅ | HTTP 404 — endpoint may come back | Will auto-retry; check target URL |
| `client_timeout` | ✅ | HTTP 408 — server timed out waiting | Automatic retry |
| `rate_limited` | ✅ | HTTP 429 — too many requests | Automatic retry with backoff |
| `service_unavailable` | ✅ | HTTP 503 — server overloaded or maintenance | Wait for recovery; will auto-retry |
| `bad_gateway` | ✅ | HTTP 502 — upstream server error | Will auto-retry |
| `gateway_timeout` | ✅ | HTTP 504 — upstream timeout | Will auto-retry |

#### Other error classes

| Error class | Meaning | Likely fix |
|---|---|---|
| `connection_failure` | TCP connection refused or DNS failure after all retries | Check network/firewall, then replay |
| `shutdown_inflight` | Worker was shut down while this doc was in-flight | Replay on next startup (automatic) |
| `unknown` | Unclassified error | Inspect the `error` field for details |

### CBL storage

With CBL, each entry becomes a document in the `changes-worker.dlq` collection:

```
doc_id: "dlq:order::12345:1713456789"
         ^^^  ^^^^^^^^^^^^^  ^^^^^^^^^^
         prefix  original ID   epoch timestamp
```

DLQ entries are queried directly via **N1QL** against the `changes-worker.dlq` collection. Three collection-level value indexes are created at startup for efficient querying:

| Index | Columns | Used by |
|---|---|---|
| `idx_dlq_type_time` | `type, time` | Page listing, purge expired, timeline |
| `idx_dlq_type_reason_time` | `type, reason, time` | Reason filtering, GROUP BY aggregation |
| `idx_dlq_type_retried` | `type, retried` | Count queries, retried filtering |

All queries use `SEARCH` (index seek) — there are no full collection scans. The query plans can be inspected via `GET /api/dlq/explain`.

### Document expiration (TTL)

Each DLQ document is created with a **CBL document expiration** via the C API (`CBLCollection_SetDocumentExpiration`). This sets the TTL directly on the `changes-worker.dlq` collection, ensuring CBL automatically purges the document when it expires — no manual cleanup needed.

- **Default TTL:** 86,400 seconds (24 hours)
- **Configurable:** Set `output.dlq.retention_seconds` in config
- **Set to 0:** Disables automatic expiration (entries live forever)

As a safety net, the worker also runs `purge_expired_dlq()` on startup before replay, which purges entries older than `retention_seconds` based on their `time` field. This catches entries that survived past their TTL (e.g., if the worker was down when the expiry was due to fire).

### File fallback

> **⚠️ Degraded mode.** The file fallback is an emergency-only storage mechanism. It lacks individual purge, retry tracking, metadata, and TTL support. If the file write fails, the error is raised (not silently swallowed) and the `dlq_write_failures_total` Prometheus counter is incremented.

Without CBL, entries are appended to a JSONL file (one JSON object per line):

```jsonl
{"doc_id":"order::12345","seq":"42","method":"PUT","status":500,"error":"Internal Server Error","time":1713456789,"target_url":"https://api.example.com","replay_attempts":0,"doc":{...}}
{"doc_id":"order::67890","seq":"43","method":"PUT","status":503,"error":"Service Unavailable","time":1713456790,"target_url":"https://api.example.com","replay_attempts":0,"doc":{...}}
```

### Batch-level metadata tracking

Writing DLQ metadata (timestamps, job ID) is deliberately **not** done per-document. If 500 documents fail in one batch, the DLQ meta document is updated **once** after the entire batch completes — not 500 times.

```
batch processes 1000 docs → 12 fail → 12 individual dlq.write() calls
                                    → 1 dlq.flush_insert_meta(job_id)  ← single CBL write
```

The `dlq:meta` document in CBL stores both **global** timestamps (latest across all jobs) and **per-job** timestamps so that multi-pipeline deployments preserve each job's DLQ history independently:

| Field | Type | Description |
|---|---|---|
| `last_inserted_at` | `int` (epoch) | When the most recent batch wrote entries to the DLQ (global) |
| `last_inserted_job` | `str` | The `checkpoint.client_id` (job identity) of that batch |
| `last_drained_at` | `int` (epoch) | When the most recent replay successfully drained entries (global) |
| `last_drained_job` | `str` | The job identity that performed the drain |
| `jobs` | `dict` | Per-job DLQ history (see below) |

The `jobs` field is a dictionary keyed by job ID, where each entry tracks that job's own insert/drain timestamps:

```json
{
  "type": "dlq_meta",
  "last_inserted_at": 1745678900,
  "last_inserted_job": "job::aaa",
  "last_drained_at": 1745679200,
  "last_drained_job": "job::bbb",
  "jobs": {
    "job::aaa": { "last_inserted_at": 1745678900, "last_drained_at": 1745679100 },
    "job::bbb": { "last_inserted_at": 1745670000, "last_drained_at": 1745679200 }
  }
}
```

The global fields are backward-compatible; the dashboard uses them for the "last incident" display. The per-job entries allow the UI (or API consumers) to show per-pipeline DLQ history. The document is fetched via a simple `GET("dlq:meta")` — no N1QL query needed.

---

## How Data Flows Out (Replay / Drain)

### Automatic replay on startup

Every time the worker starts, **before** processing any new `_changes`, it attempts to replay all pending DLQ entries:

```python
# main.py — startup sequence
since = checkpoint.load(...)

# ── This happens BEFORE the _changes loop starts ──
if dlq.enabled:
    dlq_summary = await _replay_dead_letter_queue(dlq, output, metrics, shutdown_event,
                                                   current_target_url=target_url)
    # summary = {total: 50, succeeded: 48, failed: 2, skipped: 3, expired: 5}

# ── Now start processing new changes ──
while not stop_event.is_set():
    poll _changes ...
```

The replay first purges expired entries, then iterates every pending entry (where `retried == false`), checking each against safety limits before sending:

```
1. purge_expired_dlq()           ← remove entries older than retention_seconds
2. for each pending DLQ entry:
       if replay_attempts >= max_replay_attempts:
           skip (log warning)    ← poison pill protection
       if entry.target_url != current_target_url:
           log warning           ← orphaned entry detection (still replays)
       doc = entry.doc_data
       result = output.send(doc, entry.method)
       if result.ok:
           dlq.purge(entry.id)   ← purges from CBL collection
           succeeded++
       else:
           entry.replay_attempts++  ← increment attempt counter
           failed++                 ← left in DLQ for next startup
```

After the replay batch completes, if any entries were successfully drained, the meta is updated once:

```
if succeeded > 0:
    dlq.flush_drain_meta()     ← single CBL write: last_drained_at = now
```

### Critical behavior: replay does NOT block startup

If the output endpoint is still down during replay, entries that fail are **left in the DLQ** and the worker **continues to process new changes**. The worker does not wait for the DLQ to fully drain before starting its main loop.

```
startup → purge 5 expired entries
        → replay 50 DLQ entries → 45 succeed, 2 fail, 3 skipped (max attempts)
        → worker starts processing new _changes normally
        → those 2 failed entries stay in the DLQ until next restart
        → those 3 skipped entries remain until manually cleared or they expire
```

This means DLQ entries are only retried **once per worker restart**. There is no periodic background retry during normal operation.

### Replay attempt tracking

Each time a DLQ entry fails replay, its `replay_attempts` counter is incremented. When the counter reaches `max_replay_attempts` (default: 10), the entry is **skipped** on subsequent replays. This prevents "poison pill" messages from consuming replay time indefinitely.

Skipped entries remain in the DLQ and can be:
- Inspected via `GET /api/dlq/{id}`
- Manually deleted via `DELETE /api/dlq/{id}`
- Automatically purged when their TTL expires

### Target URL mismatch detection

Each DLQ entry records the `target_url` that was configured when the failure occurred. During replay, if the current output `target_url` differs from the entry's stored `target_url`, a warning is logged:

```
DLQ entry target_url differs from current config
  entry_target=https://api-v1.example.com
  current_target=https://api-v2.example.com
```

The entry is **still replayed** (the new endpoint might accept it), but the warning alerts operators to potential data routing issues.

### Manual operations via REST API

| Endpoint | Method | Description |
|---|---|---|
| `/api/dlq` | `GET` | List all DLQ entries (without doc bodies) |
| `/api/dlq/count` | `GET` | Get the count of entries |
| `/api/dlq/meta` | `GET` | Get `last_inserted_at`, `last_drained_at`, job IDs |
| `/api/dlq/replay` | `POST` | Check pending DLQ entries (use `POST /api/restart` to trigger replay) |
| `/api/dlq/{id}` | `GET` | Get a single entry with full doc body (includes `replay_attempts`, `target_url`) |
| `/api/dlq/{id}/retry` | `POST` | Mark an entry as retried (sets `retried=true`) |
| `/api/dlq/{id}` | `DELETE` | Permanently delete an entry |
| `/api/dlq` | `DELETE` | Clear all entries (updates `last_drained_at`) |

---

## What Happens When the Output Is Down for Hours or Days

This is the most important scenario to understand. The behavior depends entirely on `halt_on_failure`:

### `halt_on_failure: true` (default) — output down for days

```
poll cycle 1: fetch 100 changes → send to output → connection refused
              → RetryableHTTP retries with backoff (1s, 2s, 4s, 8s, ...)
              → all retries exhausted
              → raise OutputEndpointDown
              → batch stops, checkpoint NOT advanced
              → sleep poll_interval_seconds (e.g., 10s)

poll cycle 2: re-fetch same 100 changes (checkpoint hasn't moved)
              → same failure
              → same sleep

... this repeats indefinitely until the endpoint comes back ...

poll cycle N: endpoint recovers → 100 changes succeed
              → checkpoint advances
              → next poll fetches new changes
```

**No data is lost. No DLQ is used. The worker is effectively paused.**

The worker will consume minimal resources (just the sleep + retry cycle), but it is not processing any new changes. The `_changes` feed accumulates a backlog on Sync Gateway.

When the endpoint recovers, the worker catches up from where it left off. If the outage lasted days, the catch-up may take a while depending on backlog size.

### `halt_on_failure: false` — output down for days

```
poll cycle 1: fetch 100 changes → send to output → connection refused
              → retries exhausted
              → send() returns {ok: false}
              → 100 entries written to DLQ
              → checkpoint ADVANCES (these docs are "done" from the feed's perspective)
              → sleep, poll again

poll cycle 2: fetch 100 new changes → same failure
              → 100 more entries in DLQ
              → checkpoint advances again

... the DLQ grows with every poll cycle ...
```

**After a 24-hour outage with 100 changes per 10-second cycle:**
- ~864,000 documents in the DLQ
- Checkpoint has advanced past all of them
- They will NOT be re-fetched from the `_changes` feed

**Recovery:**
1. Fix the endpoint
2. Restart the worker → automatic DLQ replay attempts to resend all 864K docs
3. If replay succeeds, entries are purged from the DLQ
4. If replay partially fails, remaining entries stay for the next restart

**This is why `halt_on_failure: true` is the default and recommended setting.** With `halt_on_failure: false`, a prolonged outage can create a massive DLQ that is difficult to drain.

### Retry backoff during output failure

The retry backoff is configured per output target:

```json
"output": {
    "retry": {
        "max_retries": 3,
        "backoff_base_seconds": 1,
        "backoff_max_seconds": 30,
        "retry_on_status": [500, 502, 503, 504]
    }
}
```

The backoff is **exponential**: `delay = min(base * 2^(attempt-1), max)`

| Attempt | Delay (base=1, max=30) |
|---|---|
| 1 | 1s |
| 2 | 2s |
| 3 | 4s |
| 4 | 8s |
| 5 | 16s |
| 6+ | 30s (capped) |

After `max_retries` attempts, the document either goes to the DLQ (`halt_on_failure: false`) or the batch stops (`halt_on_failure: true`).

There is also the top-level `retry` config that governs `_changes` feed retries (separate from output retries):

```json
"retry": {
    "max_retries": 5,
    "backoff_base_seconds": 1,
    "backoff_max_seconds": 60,
    "retry_on_status": [500, 502, 503, 504]
}
```

With `halt_on_failure: true`, after the batch-level stop, the worker sleeps for `poll_interval_seconds` and tries the entire poll cycle again. This is an **outer retry loop** — not exponential, just a flat interval. The worker will keep retrying forever (or until shutdown).

---

## Jobs and the DLQ

### What is a "job"?

A **job** is one complete pipeline: a `_changes` feed (source) + processing config + output destination. Today (v1.x), the worker runs a single job. The job is identified by the **`checkpoint.client_id`** (default: `"changes_worker"`).

See [`JOBS.md`](JOBS.md) for the full job ID concept and per-engine/per-job metrics.

### How job ID relates to the DLQ

The `dlq:meta` document records which job produced the most recent DLQ activity:

```json
{
    "type": "dlq_meta",
    "last_inserted_at": 1713456789,
    "last_inserted_job": "changes_worker",
    "last_drained_at": 1713460000,
    "last_drained_job": "changes_worker"
}
```

This answers: "Which job last put something in the DLQ?" and "Which job last drained it?"

In a future multi-pipeline world (v2.x), each pipeline would have its own DLQ scope and job ID, so you could distinguish which pipeline's output is failing.

### What happens when you change the output endpoint (the "orphaned DLQ" problem)

This is a real operational hazard. Consider this scenario:

```
Day 1: Worker runs with output → https://api-v1.example.com
        5 docs fail → 5 entries in the DLQ
        DLQ entries contain: method=PUT, target was api-v1

Day 2: You change config to output → https://api-v2.example.com
        Worker restarts
        DLQ replay runs: sends those 5 docs to api-v2 (the NEW endpoint)
```

**The DLQ now records which endpoint it was targeting.** Each `DeadLetterQueue.write()` call stores the current `target_url` alongside the document body, HTTP method, error, and status code. On replay, the worker **compares the entry's stored `target_url` with the current config** and logs a warning if they differ. The replay still proceeds (the new endpoint may accept the data), but the warning gives operators visibility into potential routing issues.

This can cause several problems:

| Scenario | What happens |
|---|---|
| New endpoint accepts the docs | ✅ Works — entries are purged. But the data may not belong in the new endpoint. |
| New endpoint rejects the docs (schema mismatch) | ❌ Replay fails → entries stay in DLQ → next restart tries again → same failure. |
| New endpoint doesn't exist yet | ❌ Connection refused → replay fails → entries stay → cycle repeats. |
| You switched from HTTP to RDBMS mode | ❌ The DLQ docs were PUT payloads for an HTTP endpoint. The RDBMS forwarder will try to map them as Couchbase documents, which may or may not work depending on the schema mapper. |

### Does the worker block on a non-draining DLQ?

**No.** The worker does **not** block startup on DLQ replay. The startup sequence is:

```
1. Load checkpoint
2. Replay DLQ entries (best effort, one pass)
3. Start _changes loop ← this always happens, even if all replays failed
```

If all 5 entries fail replay because the old endpoint is gone:
- `dlq_summary = {total: 5, succeeded: 0, failed: 5}`
- The worker logs the summary and **continues to process new changes**
- Those 5 entries remain in the DLQ indefinitely

The worker will attempt replay again on the **next restart**, and the cycle repeats.

### How to handle orphaned DLQ entries

**Option 1: Clear the DLQ manually before switching endpoints**

```bash
# Via REST API
curl -X DELETE http://localhost:8080/api/dlq

# Or from the admin UI: click the DLQ node → review entries → clear all
```

**Option 2: Drain specific entries via the API**

```bash
# List entries and inspect
curl http://localhost:8080/api/dlq | jq '.[].doc_id_original'

# Delete specific entries you don't need
curl -X DELETE http://localhost:8080/api/dlq/dlq:order::12345:1713456789
```

**Option 3: Let replay send to the new endpoint**

If the new endpoint can handle the old data (same schema, same API contract), the orphaned entries will drain naturally on the next restart.

**Recommendation:** Always review `/api/dlq` before changing your output endpoint. If there are pending entries, decide whether to clear them, drain them to the old endpoint first, or let them go to the new endpoint.

---

## Shutdown Behavior

### Normal shutdown (`dlq_inflight_on_shutdown: false`, the default)

```
SIGTERM received → shutdown_event.set()
                 → current batch finishes (up to drain_timeout_seconds)
                 → any unprocessed docs in the current batch are abandoned
                 → checkpoint is NOT advanced past the unfinished batch
                 → on restart, those docs are re-fetched from _changes
```

No DLQ involvement. Unfinished work is replayed from the feed.

### Aggressive shutdown (`dlq_inflight_on_shutdown: true`)

```
SIGTERM received → shutdown_event.set()
                 → current sub-batch: remaining unprocessed docs → DLQ
                 → checkpoint may advance (sub-batch level)
                 → on restart: those docs come from DLQ replay, not _changes
```

This is useful when you want to avoid re-fetching from Sync Gateway on restart — the in-flight docs are preserved locally in CBL. But it means those docs are now in the DLQ and subject to the replay-on-startup behavior.

Config:

```json
"shutdown": {
    "drain_timeout_seconds": 60,
    "dlq_inflight_on_shutdown": false
}
```

---

## Metrics

### Prometheus metrics

| Metric | Type | Description |
|---|---|---|
| `changes_worker_dead_letter_total` | Counter | Total documents written to the DLQ since startup. Monotonically increasing. |
| `changes_worker_dlq_write_failures_total` | Counter | Total DLQ file write failures. Each increment means a document may have been lost. |
| `changes_worker_dlq_pending_count` | Gauge | Current number of pending entries in the DLQ. Updated after each batch and after replay. |

The counter tracks how many documents have been DLQ'd in this worker's lifetime. The gauge tracks real-time queue depth — use it for alerting instead of polling `/api/dlq/count`.

### What the metrics tell you

| Pattern | Meaning |
|---|---|
| `dead_letter_total = 0` | No failures — everything delivered successfully |
| `dead_letter_total` rising slowly | Occasional failures — intermittent endpoint issues |
| `dead_letter_total` rising at the same rate as `changes_received_total` | **Every document is failing** — endpoint is down or misconfigured |
| `dead_letter_total` jumped once, then flat | A transient outage caused a burst of failures, then recovered |

### Related metrics for DLQ analysis

| Metric | Why it matters for DLQ |
|---|---|
| `output_errors_total` | Docs that failed delivery (these become DLQ entries when `halt_on_failure=false`) |
| `output_success_total` | Compare with errors to get the failure rate |
| `output_endpoint_up` | Gauge: `1` = reachable, `0` = down. When this is `0`, expect DLQ entries. |
| `retries_total` | How many retry attempts happened before docs were DLQ'd |
| `retry_exhausted_total` | Requests where all retries failed — these are the DLQ triggers |
| `output_response_time_seconds` | Slow responses may indicate the endpoint is degrading before failing |

### Alerting examples

```promql
# Any documents landing in the DLQ
rate(changes_worker_dead_letter_total[5m]) > 0

# DLQ rate exceeding 10 docs/minute (sustained failure)
rate(changes_worker_dead_letter_total[5m]) * 60 > 10

# DLQ has pending entries (gauge-based, no rate needed)
changes_worker_dlq_pending_count > 0

# DLQ file write failures (potential data loss)
changes_worker_dlq_write_failures_total > 0

# Output endpoint is down (precursor to DLQ entries)
changes_worker_output_endpoint_up == 0

# All retries exhausted (documents about to be DLQ'd)
rate(changes_worker_retry_exhausted_total[5m]) > 0
```

### DLQ metadata via REST

The `/api/dlq/meta` endpoint returns the batch-level timestamps with per-job history:

```json
{
    "last_inserted_at": 1713456789,
    "last_inserted_job": "job::aaa",
    "last_drained_at": 1713460000,
    "last_drained_job": "job::bbb",
    "jobs": {
        "job::aaa": { "last_inserted_at": 1713456789, "last_drained_at": 1713458000 },
        "job::bbb": { "last_inserted_at": 1713450000, "last_drained_at": 1713460000 }
    }
}
```

These are unix epoch integers. The admin UI dashboard converts them to human-readable format with relative time (e.g., "4/18/2026, 2:30:15 PM (3h ago)"). The dashboard polls this endpoint every 30 seconds.

### Dashboard visibility

The DLQ node in the architecture diagram shows:
- **Node label**: pending count (gauge) + cumulative total — e.g., "3 pending · 42 total"
- **Hover tooltip**: pending count, total count, DB retries, retry exhausted, last incident (relative time), last drain time, link to `/api/dlq`
- **Click modal**: pending (current) vs total (cumulative), last incident, last inserted/drained timestamps, job IDs, DB retries / retry exhausted / transient errors / permanent errors, output errors/success, uptime, plus two charts:
  - **Pie chart**: Output outcomes (success, errors, dead letters, retries)
  - **Line chart**: DLQ rate + DB retry rate over time

---

## Storage Comparison

| Feature | CBL (Couchbase Lite) | File (JSONL fallback) |
|---|---|---|
| Storage location | `changes-worker.dlq` collection in `/app/data/` | Append-only file at `dead_letter_path` |
| Persists across restarts | ✅ Yes (Docker volume) | ⚠️ Only if bind-mounted |
| Individual entry access | ✅ By `dlq:{id}` | ❌ Must parse entire file |
| Individual delete | ✅ Purge from collection | ❌ Not supported |
| Mark as retried | ✅ `retried` field update | ❌ Not supported |
| Replay attempt tracking | ✅ `replay_attempts` counter | ❌ Not supported |
| Target URL tracking | ✅ `target_url` field | ✅ `target_url` field |
| Document TTL / expiration | ✅ CBL native expiration | ❌ Not supported |
| Expired entry purge | ✅ Automatic (CBL + startup sweep) | ❌ Not supported |
| Automatic replay on startup | ✅ Yes (filters by `retried=false`) | ✅ Yes (replays all lines) |
| REST API support | ✅ Full CRUD | ❌ List only |
| Metadata tracking | ✅ `dlq:meta` document | ❌ Not available |
| Concurrent write safety | ✅ CBL handles it | ✅ Async lock per write |
| Write failure handling | ✅ Exception raised | ✅ Exception raised + `dlq_write_failures_total` counter |

---

## Configuration Reference

```jsonc
{
    "output": {
        "halt_on_failure": true,          // true = stop on infrastructure failure (no DLQ)
                                           // false = skip + DLQ
        "data_error_action": "dlq",       // "dlq" = store data errors in DLQ (default)
                                           // "skip" = drop data errors and move on
        "dead_letter_path": "failed_docs.jsonl",  // file fallback (ignored when CBL available)
        "dlq": {
            "retention_seconds": 86400,    // 24h — how long entries live before auto-purge (0 = forever)
            "max_replay_attempts": 10      // max replay attempts before skipping (0 = unlimited)
        },

        "retry": {
            "max_retries": 3,              // retries before giving up on a doc
            "backoff_base_seconds": 1,     // exponential backoff base
            "backoff_max_seconds": 30,     // backoff ceiling
            "retry_on_status": [500, 502, 503, 504, 507]  // which HTTP statuses trigger explicit retry
                                                            // Note: transient 4xx (404, 408, 429, etc.)
                                                            // are auto-retried by the classifier regardless
        },

        "health_check": {
            "enabled": true,               // periodic endpoint probe
            "interval_seconds": 30,        // probe interval
            "url": "",                     // custom health URL (default: target_url)
            "method": "GET",
            "timeout_seconds": 5
        }
    },

    "shutdown": {
        "drain_timeout_seconds": 60,       // max wait for current batch to finish
        "dlq_inflight_on_shutdown": false   // true = DLQ unfinished docs on SIGTERM
    },

    "checkpoint": {
        "client_id": "changes_worker"      // job identity (appears in dlq:meta)
    }
}
```

---

## DLQ Explorer UI (`/dlq`)

The DLQ Explorer is a dedicated admin UI page for inspecting, diagnosing, and managing dead letter queue entries. It answers the key questions an operator has when documents are stuck:

- **What is in the DLQ?** — Sortable table of all entries with doc ID, reason, error, time, replay attempts
- **When did it get in?** — Timestamp column + timeline chart showing arrival patterns (burst vs. steady trickle)
- **Did it ever try to get out?** — `replay_attempts` count and `retried` status badge
- **Why is it here?** — `reason` field displayed as a color-coded badge (data error, server error, connection failure, etc.)
- **How can I fix it?** — The reason code table maps each reason to a suggested fix; the inspector panel shows the full document body and error detail

### Layout

```
┌─────────────────────────────────────────────────────────────┐
│  DLQ Explorer                                    [Clear All]│
├──────────────────────┬──────────────────────────────────────┤
│                      │                                      │
│  Summary Cards       │  Charts                              │
│  ┌──────┐ ┌───────┐  │  ┌────────────────┐ ┌─────────────┐ │
│  │Total │ │Pending│  │  │ Arrivals Over  │ │ Reasons Pie │ │
│  │  42  │ │  38   │  │  │ Time (line)    │ │             │ │
│  └──────┘ └───────┘  │  └────────────────┘ └─────────────┘ │
│  ┌──────┐ ┌───────┐  │                                      │
│  │Retried│ │Oldest │  │                                      │
│  │   4  │ │ 3h ago│  │                                      │
│  └──────┘ └───────┘  │                                      │
├──────────────────────┴──────────────────────────────────────┤
│  Entry Table (sortable, filterable by reason)               │
│  ┌────────┬──────────┬────────┬───────┬────────┬──────────┐ │
│  │ Doc ID │ Reason   │ Error  │ Time  │Replays │ Actions  │ │
│  ├────────┼──────────┼────────┼───────┼────────┼──────────┤ │
│  │ ord_01 │ data_err │ int32  │ 3m    │ 0      │ [Inspect]│ │
│  │ ord_02 │ srv_500  │ timeout│ 1h    │ 2      │ [Inspect]│ │
│  └────────┴──────────┴────────┴───────┴────────┴──────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Inspector Panel (read-only, slides open when row clicked)  │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ Metadata: doc_id, seq, method, status, reason, target  │ │
│  │ Error Detail: full error string                        │ │
│  │ Document Body: JSON viewer (syntax highlighted)        │ │
│  │ Suggested Fix: based on reason code                    │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Charts

1. **Arrivals Over Time** (line chart) — plots DLQ entries by their `time` field, bucketed by minute. Reveals whether failures arrived as a burst (outage) or steady trickle (data quality issue).

2. **Reasons Breakdown** (pie chart) — groups entries by `reason` field. Immediately shows the dominant failure mode (e.g., "80% are data_error:data_type" → schema mapping issue).

### Data sources

| API | Used for |
|---|---|
| `GET /api/dlq` | Paginated entry list with server-side sort/filter (N1QL `LIMIT`/`OFFSET`) |
| `GET /api/dlq/stats` | Lightweight aggregation for summary cards + charts (N1QL `COUNT`/`MIN`/`GROUP BY`) |
| `GET /api/dlq/{id}` | Full entry with doc body (inspector) |
| `GET /api/dlq/meta` | Last inserted/drained timestamps |
| `GET /api/dlq/count` | Total count (N1QL `COUNT(*)`) |
| `GET /api/dlq/explain` | N1QL query plans for all DLQ queries (index verification) |
| `DELETE /api/dlq/{id}` | Delete single entry (× button in each row) |
| `DELETE /api/dlq` | Clear all (header button, uses transaction for atomicity) |

### Reason badge colors

| Error class pattern | Badge color | Meaning |
|---|---|---|
| `constraint_violation`, `data_type` | 🟡 Warning/yellow | Data problem — fix the source or mapping |
| `bad_request`, `conflict`, `gone`, `payload_too_large` | 🟡 Warning/yellow | Request/data problem — fix doc or config |
| `auth_failure`, `invalid_database`, `permission_denied` | 🟠 Orange | Auth/config problem — fix credentials or permissions |
| `table_not_found`, `not_found` | 🟠 Orange | Missing resource — may come back (was retried first) |
| `rate_limited` | 🟠 Orange | Rate limited — retried with backoff before DLQ |
| `connection`, `service_unavailable`, `bad_gateway` | 🔴 Error/red | Infrastructure failure — may self-heal |
| `resource_exhaustion`, `deadlock`, `lock_contention` | 🔴 Error/red | Resource/concurrency issue — retried first |
| `server_shutdown`, `timeout`, `gateway_timeout` | 🔴 Error/red | Server issue — retried before DLQ |
| `shutdown_inflight` | 🔵 Info/blue | Expected during shutdown — auto-replays on restart |
| `redirect:*` | ⚫ Neutral/gray | Redirect issue — update URL |
| `syntax_or_schema`, `method_or_media` | ⚫ Neutral/gray | Config/mapping problem — fix mapping definition |
| `unknown` | ⚫ Neutral/gray | Inspect error field for details |

---

## FAQ

### Q: Can I lose data with the DLQ?

**With CBL and Docker volumes:** Entries persist in the CBL database on a Docker named volume. However, entries will be **automatically purged** after `retention_seconds` (default 24 hours). If you need entries to live longer, increase the retention or set it to 0.

**With file fallback and no volume mount:** Yes. If the container is destroyed, the JSONL file is lost. Additionally, if the file write itself fails (disk full, permissions), the error is now raised and counted via `dlq_write_failures_total`.

**DLQ write failure safety net:** When `halt_on_failure: false` and a DLQ write itself fails (CBL unavailable), the batch processor now upgrades to a halt — the checkpoint does not advance and the doc will be re-processed on the next attempt. No document is ever silently lost regardless of `halt_on_failure` setting. See [FAILURE_OPTION_OUTPUT_RDBMS.md](FAILURE_OPTION_OUTPUT_RDBMS.md) §3.11.

### Q: Why doesn't the DLQ retry automatically during runtime?

By design. The DLQ is a **parking lot**, not a retry queue. Automatic background retries would add complexity (backoff management, concurrency with the main loop, metric confusion) and could mask persistent failures. The explicit replay-on-startup model keeps the behavior predictable.

### Q: What if I want continuous retry of failed docs?

Use `halt_on_failure: true` instead. The worker will keep retrying the entire batch (via the outer poll loop) until the endpoint recovers. This is effectively continuous retry — the worker just doesn't advance past the failure.

### Q: How big can the DLQ get?

With the default `retention_seconds: 86400` (24 hours), the DLQ is bounded by how many documents fail within one day. CBL document expiration automatically purges old entries, and the startup sweep catches anything that slipped through.

Without retention (`retention_seconds: 0`), each entry is a few KB (document body + metadata). A DLQ with 100,000 entries would be roughly 100–500 MB depending on document size. The CBL database handles this fine — all DLQ queries use N1QL with collection-level indexes (no full scans), so pagination and stats remain fast even with millions of entries. Replay on startup would take time (each entry is sent individually to the output). Additionally, entries that exceed `max_replay_attempts` are skipped during replay, so poison pills don't consume unbounded time.

Monitor `cbl_db_size_bytes`, `system_disk_percent`, and `changes_worker_dlq_pending_count` to watch for growth.

### Q: What happens if I change `halt_on_failure` from `false` to `true`?

Existing DLQ entries remain. On the next startup, replay still runs (it doesn't check `halt_on_failure`). If replay succeeds, entries are purged. If it fails, entries stay — but now the main loop will use `halt_on_failure: true` for new changes, so no new entries will be added.

### Q: Can two workers share a DLQ?

Not safely. The CBL database is a local embedded store — it is not designed for concurrent access from multiple processes. Each worker instance should have its own CBL database directory.
