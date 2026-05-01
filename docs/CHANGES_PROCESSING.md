# `_changes` Feed Processing

This document describes how the changes_worker consumes, processes, and
checkpoints the `_changes` feed — including the optimised initial sync
strategy, crash recovery, and logging conventions.

**Related docs:**
- [`DESIGN.md`](DESIGN.md) — Pipeline architecture, sequential vs parallel processing, failure modes
- [`SOURCE_TYPES.md`](SOURCE_TYPES.md) — Compatibility matrix across SG / App Services / Edge Server / CouchDB
- [`LOGGING.md`](LOGGING.md) — Logging architecture, log keys, structured fields
- [`CONFIGURATION.md`](CONFIGURATION.md) — Full config.json reference
- [`FAILURE_OPTION_OUTPUT_RDBMS.md`](FAILURE_OPTION_OUTPUT_RDBMS.md) — Failure analysis & resolutions (checkpoint save resilience §4.1)

---

## Overview

The `_changes` feed is the input stage of the pipeline.  The worker polls
(or streams) the endpoint, receives batches of change rows, optionally
fetches full document bodies, forwards them to the output, and saves a
checkpoint so it can resume after restarts.

```
 _changes endpoint
       │
       ▼
 ┌─────────────┐     ┌──────────────┐     ┌────────────┐
 │  _changes   │────▶│  Filter &    │────▶│  Output    │
 │  feed       │     │  Fetch docs  │     │  Forward   │
 └─────────────┘     └──────────────┘     └────────────┘
       │                                        │
       └──────── checkpoint(since) ◀────────────┘
```

---

## Two Phases of Sync

Every replication has two logical phases:

| Phase | Goal | Feed settings |
|---|---|---|
| **Initial sync** | Pull the current state of all active/live documents | `active_only=true`, `include_docs=false`, `feed=normal` |
| **Steady state** | Track ongoing mutations in real time | User's configured `active_only`, `include_docs`, `feed` type (longpoll / continuous / websocket) |

The worker detects which phase it is in automatically based on the
checkpoint.

---

## Phase 1 — Initial Sync

### The Problem

When starting from `since=0` the `_changes` feed may return hundreds of
thousands to millions of rows.  Many of those rows are historical deletes
and tombstones that the consumer doesn't need for an initial load.
Requesting `include_docs=true` on such a large feed puts enormous
pressure on the source endpoint.

### Default Mode (`optimize_initial_sync: false`)

The default — and recommended — approach is a single large `_changes`
request with no limit:

```
since=0 ──▶ _changes?feed=normal
                      &active_only=true          (Couchbase only)
                      &include_docs=false
                      (no limit — return everything)
            │
            ▼
      got results + last_seq
            │
            ▼
      process all rows
      checkpoint since=last_seq
            │
            ▼
      next request returns 0 results
            │
            ▼
      DONE ── mark initial_sync_done=true
              switch to Phase 2
```

**Why no limit?**  When you request `_changes` without a limit, the
response includes the true `last_seq` for the entire feed.  This
gives you a clean boundary: everything up to `last_seq` has been
accounted for, and any future mutation (including deletes) will have a
higher sequence number.

When you use a limit (e.g. `limit=4`), the `last_seq` in the response
is only the sequence of the last row in *that page*, not the true end
of the feed.  Between pages, a document you already processed could be
deleted — but because you're still in initial-sync mode (ignoring
deletes), you'd miss it.  Example:

```
Page 1: _changes?since=0&limit=4  →  last_seq=82  (got docs A,B,C,D)
  ↓  meanwhile doc B is deleted at seq=85
Page 2: _changes?since=82&limit=4&active_only=true  →  delete at seq=85 not sent
  ↓  you never find out about the delete
```

The single-request approach avoids this consistency gap entirely.

**Timeout handling:**  The `http_timeout_seconds` setting (default 300s
/ 5 minutes) controls how long the worker waits for the response.
Sync Gateway processes approximately 5,000 `_id`/`_rev`/`_seq` entries
per second from the index, so a 5-minute timeout covers feeds up to
~1.5 million entries.  For larger feeds, increase the timeout or
enable `optimize_initial_sync`.

### Optimized Mode (`optimize_initial_sync: true`)

For very large feeds (millions of entries) where a single request may
timeout, the worker can chunk the initial pull:

```json
{
  "changes_feed": {
    "optimize_initial_sync": true,
    "continuous_catchup_limit": 10000
  }
}
```

Before starting the paginated `_changes` requests, the worker calls
`GET {base_url}/` to fetch the database `update_seq`.  This endpoint
is supported across all source types:

- **Sync Gateway / App Services** — `GET /{db}/` or `GET /{db}.{scope}.{collection}/`
  returns `update_seq` as an integer.
- **Edge Server** — `GET /{keyspace}/` returns `update_seq` as an
  integer (per-collection).
- **CouchDB** — `GET /{db}/` returns `update_seq` as an opaque string
  (e.g. `"292786-g1AAAAF..."`); the worker extracts the leading integer.

This value acts as the "endpoint" — the worker knows the initial sync
is complete once `last_seq` from a `_changes` response reaches or
exceeds `update_seq`.

```
          GET {base_url}/ ──▶ { "update_seq": 150, ... }
                │
                ▼
          target_seq = 150
                │
                ▼
since=0 ──▶ _changes?feed=normal&limit=10000
                      &active_only=true          (Couchbase only)
                      &include_docs=false
            │
            ▼
      last_seq >= target_seq?
      ┌─────┴──────┐
      │ no          │ yes
      │             │
      ▼             ▼
  process batch   DONE ── mark initial_sync_done=true
  checkpoint        │     save checkpoint
  since=last_seq    │     switch to Phase 2
      │             │
      └─── loop ◀───┘
```

**Why `update_seq`?**  When using a limit, `last_seq` is only the
sequence of the last row in that page, so the previous approach of
waiting for zero results required an extra round-trip and left a window
where deletes between chunks could be missed.  By capturing `update_seq`
upfront, the worker has a definitive target: once `last_seq >= update_seq`,
every document that existed at the start of the sync has been accounted
for.  Any mutation (including deletes) that happened *after* the
`update_seq` snapshot will have a higher sequence number and will be
delivered by the steady-state feed.

If the `update_seq` fetch fails (e.g., the endpoint doesn't support it),
the worker falls back to the original zero-results completion strategy.

**Why 5,000 / 10,000?**  Sync Gateway's `_changes` index serves roughly
5,000 entries per second.  A `limit=5000` request takes ~1 second;
`limit=10000` takes ~2 seconds.  Going higher yields diminishing
returns.  Going lower adds unnecessary HTTP round-trip overhead.

### Common to Both Modes

1. **`active_only=true`** (Couchbase products) — the source excludes
   `_deleted` and `_removed` entries from the feed so the worker only
   sees live documents.  For **CouchDB** (which does not support
   `active_only`) the worker filters out deleted/removed rows in the
   processing layer instead.

2. **`include_docs=false`** — the feed returns only `_id`, `_rev`, and
   `_seq` per row.  Full document bodies are fetched separately via
   `_bulk_get` (or individual `GET` for Edge Server) in batches of
   `get_batch_number` (default 100).

3. **`feed=normal`** — one-shot request that returns immediately with
   available results, instead of blocking on longpoll waiting for new
   changes.

4. **Completion** — when a request returns **zero results** the initial
   sync is complete.  The worker sets `initial_sync_done=true` in the
   checkpoint and switches to steady-state mode with the user's
   configured feed settings.

### CouchDB Differences

CouchDB does not support the `active_only` parameter.  During initial
sync the worker forces `ignore_delete=true` and `ignore_remove=true` in
the processing layer so deleted/removed changes are skipped.  After the
initial sync completes these overrides are removed and the user's
`processing.ignore_delete` / `processing.ignore_remove` settings take
effect.

### Tombstone (Delete & Remove) Tracking

The `_changes` feed signals "remove this document" in two ways:

| Flag | Meaning | Document still exists at source? |
|---|---|---|
| `"deleted": true` | The document was permanently deleted.  The change entry is a **tombstone** — a minimal record with just `_id`, `_rev`, and `_deleted`. | No |
| `"removed": true` | The document still exists, but the consumer no longer has access (e.g., removed from the consumer's channel, or access revoked). | Yes — but not for the consumer |

Both are treated identically by the pipeline: the document is forwarded
to the output as a **DELETE** operation (unless filtered out by
`ignore_delete` / `ignore_remove`).

The worker tracks these at three levels:

1. **`feed_deletes_seen_total`** / **`feed_removes_seen_total`** —
   always incremented for every `deleted=true` / `removed=true` entry
   in the raw feed, regardless of filter settings.
2. **`deletes_forwarded_total`** — incremented for deletes **and**
   removes that survive filtering and are forwarded to the output as
   DELETE operations.
3. **`changes_deleted_total`** / **`changes_removed_total`** —
   incremented for entries that are filtered out (skipped) because
   `ignore_delete=true` / `ignore_remove=true`, or during CouchDB
   initial sync.

The relationship is:

```
deletes_forwarded = (feed_deletes_seen − changes_deleted)
                  + (feed_removes_seen − changes_removed)
```

When `ignore_delete=false` and `ignore_remove=false` (the defaults),
all entries are forwarded and the filtered counters stay at zero.

**What happens when a delete/remove reaches the output:**

| Output type | Action |
|---|---|
| **RDBMS** | `DELETE FROM table WHERE doc_id = $1` for each mapped table (child tables first for FK safety, wrapped in a transaction for multi-table mappings). Controlled per-table by `on_delete: "delete"` or `"ignore"`. For tombstones with minimal doc bodies, the mapper bypasses the content-based filter and generates DELETE ops using only the `doc_id`. |
| **HTTP** | Sends a DELETE request to the target URL. |
| **Cloud** | Deletes the object, uploads a tombstone marker, or ignores — per the `on_delete` setting (`"delete"`, `"tombstone"`, or `"ignore"`). |

### Continuous / WebSocket Initial Sync

For `feed_type=continuous` and `feed_type=websocket`, the worker uses a
two-phase approach:

1. **Catch-up** (Phase 1) — uses `feed=normal` requests to drain the
   backlog before switching to the real-time stream.
2. **Stream** (Phase 2) — opens the continuous or WebSocket connection
   with the user's configured settings.

During initial sync, the catch-up phase applies the same
`active_only=true` and `include_docs=false` overrides.  The question is:
*what happens when new changes (including deletes) arrive during the
catch-up?*

- **Default mode** (no limit): the catch-up request returns the full
  feed with the true `last_seq`.  When the worker switches to the
  stream at `since=last_seq`, all subsequent changes (including any
  deletes that occurred during the catch-up) will be delivered by the
  stream.  No gap.

- **Optimized mode** (chunked): the worker fetches `update_seq` before
  starting the catch-up and stops once `last_seq >= update_seq`.  Any
  deletes that occur *after* the `update_seq` snapshot will have higher
  sequence numbers and will be delivered by the continuous/websocket
  stream once it connects.  No consistency gap.

---

## Checkpoint & Crash Recovery

### The Checkpoint Document

The worker stores a checkpoint as a `_local` document on Sync Gateway
(with file and CBL fallbacks):

```json
{
  "client_id": "changes_worker",
  "SGs_Seq": "42:100",
  "time": 1713456789,
  "remote": 17,
  "initial_sync_done": false,
  "_rev": "0-3"
}
```

| Field | Purpose |
|---|---|
| `client_id` | Identifies this replicator instance |
| `SGs_Seq` | The `last_seq` value from the most recent processed batch |
| `time` | Unix epoch timestamp of the last save |
| `remote` | Monotonically increasing counter (CBL compatibility) |
| `initial_sync_done` | Whether the initial sync has completed at least once |
| `_rev` | SG document revision (for conflict-free updates) |

### The `initial_sync_done` Flag

This flag solves a critical corner case: **what happens if the worker
crashes mid-initial-sync?**

Without the flag, a restart would see `since=42:100` (non-zero) and
assume the initial sync was already done.  It would switch to steady-
state mode immediately, potentially missing large portions of the
initial document set.

With the flag:

| Checkpoint state | Behaviour on restart |
|---|---|
| No checkpoint (`since=0`) | Fresh start → initial sync mode |
| `since=X`, `initial_sync_done=false` | Interrupted initial sync → **resume** initial sync from `since=X` |
| `since=X`, `initial_sync_done=true` | Normal operation → steady-state mode |
| `since=X`, flag **missing** (legacy) | Treated as `initial_sync_done=true` to avoid re-syncing old deployments |

The flag is persisted on **every** checkpoint save during initial sync
(written as `false` until the first empty batch, then flipped to `true`).

### Decision Flow on Startup

```
                    ┌──────────────────────┐
                    │ Load checkpoint      │
                    │ (SG / file / CBL)    │
                    └──────────┬───────────┘
                               │
                    ┌──────────▼───────────┐
                    │ Was config since=0?  │
                    └──────────┬───────────┘
                          yes  │  no (user forced since=X)
                               │      │
                               │      └──▶ normal mode (no initial sync)
                               │
                    ┌──────────▼───────────┐
                    │ initial_sync_done?   │
                    └──────────┬───────────┘
                         false │  true
                               │    │
                               │    └──▶ normal mode
                               │
                    ┌──────────▼───────────┐
                    │ INITIAL SYNC MODE    │
                    │ (resume from since)  │
                    └──────────────────────┘
```

If the user explicitly sets `since` to a non-zero value in the config,
the worker skips initial-sync mode entirely — it assumes the user knows
what they're doing.

---

## Phase 2 — Steady State

Once `initial_sync_done=true` the worker uses the user's configured
settings:

| Setting | Config key | Default |
|---|---|---|
| Feed type | `changes_feed.feed_type` | `longpoll` |
| Active only | `changes_feed.active_only` | `true` |
| Include docs | `changes_feed.include_docs` | `false` |
| Limit | `changes_feed.limit` | `0` (no limit) |

In continuous and websocket modes the worker uses a two-phase approach:

1. **Catch-up** — `feed=normal` with `limit` to drain any backlog
2. **Stream** — `feed=continuous` or `feed=websocket` for real-time
   changes

The catch-up phase re-uses the same chunked logic as the initial sync
but with the user's `active_only` and `include_docs` settings.

### Stream Buffering (Continuous & WebSocket) — Greedy Drain

Both continuous and websocket streams use identical **greedy drain**
buffering logic.  Instead of a fixed timer, the worker uses the socket's
state to decide when to flush:

1. **Block indefinitely** on the first row/message (no CPU waste when idle).
2. Once a row arrives, **drain** everything already sitting in the socket
   buffer using a very short timeout (`stream_batch_timeout_ms`, default 5ms).
3. As soon as nothing more is immediately available **or** `get_batch_number`
   rows accumulate (default 100), flush the batch.

This gives:

- **Zero artificial delay** for single-document changes (low-traffic periods).
- **Natural batching under load** — rows arriving back-to-back are drained
  into one batch before flushing.
- **No magic numbers** — the system self-tunes based on actual network throughput.

> **History:** The original implementation used a fixed 100ms/100-item
> "whichever comes first" timer.  This added unnecessary latency on idle
> streams and, combined with the single-doc GET optimization, caused
> nearly all fetches to become individual GETs instead of `_bulk_get`.
> The greedy drain strategy fixes the root cause: single-doc batches now
> only occur when there genuinely is one change available, so the
> single-doc GET optimization works as intended — lighter than `_bulk_get`
> for true singletons, while bursts are naturally batched.

---

## Document Fetching

When `include_docs=false` (always the case during initial sync), the
worker must fetch full document bodies separately.

### `_bulk_get` (Sync Gateway / App Services / CouchDB)

- Eligible change rows are grouped into batches of `get_batch_number`
  (default 100).
- Each batch is sent as a `POST _bulk_get` request.
- **Single-doc optimization:** When a batch contains exactly one document,
  the worker uses a simple `GET /{keyspace}/{doc_id}?rev={rev}` instead
  of `_bulk_get`.  This avoids the overhead of constructing and parsing
  the bulk request/response envelope for a single document.  Combined
  with the greedy drain buffering strategy, single-doc batches only occur
  when there genuinely is just one change — not as a side effect of
  timer-based batching.
- If the response is missing documents, the worker falls back to
  individual `GET` requests for the missing IDs.

### Individual `GET` (Edge Server)

- Edge Server does not have a `_bulk_get` endpoint.
- Documents are fetched individually via `GET /{keyspace}/{doc_id}?rev=`.
- Requests are fanned out with a concurrency semaphore
  (`max_concurrent`, default 20).

---

## Logging Conventions

The `_changes` processing uses structured `log_event()` calls with the
`CHANGES`, `HTTP`, and `CHECKPOINT` log keys.  The guiding principle is:

> **INFO** = what is happening and how much.
> **DEBUG** = the individual items and their details.

### `CHANGES` Log Key

| Level | What is logged |
|---|---|
| **INFO** | Replication config on startup (feed type, active_only, include_docs, since, initial_sync state) |
| **INFO** | Initial sync mode entered / completed |
| **INFO** | Catch-up starting / batch received (change count) / complete |
| **INFO** | `_changes` batch: number of changes received |
| **DEBUG** | Individual change rows: `doc_id`, `seq` |

### `HTTP` Log Key (doc fetching)

| Level | What is logged |
|---|---|
| **INFO** | `fetch_docs`: total docs to fetch, number of batches |
| **INFO** | `_bulk_get`: requested doc count, received doc count |
| **INFO** | Individual fetch: total doc count |
| **DEBUG** | `_bulk_get` request items: individual `doc_id` |
| **DEBUG** | `_bulk_get` response: `doc_count`, `input_count` (requested), `bytes` (payload size) |
| **DEBUG** | `_bulk_get` result docs: individual `doc_id` |
| **DEBUG** | Individual `GET`: `doc_id`, `bytes` (payload size) |

### `CHECKPOINT` Log Key

| Level | What is logged |
|---|---|
| **INFO** | "checkpoint loaded" / "checkpoint saved" — operation + storage type only |
| **DEBUG** | Checkpoint detail — `seq`, `doc_id`, storage type |

This split avoids flooding INFO logs with checkpoint document IDs and
sequence values on every save (which can happen thousands of times per
run), while keeping the detail available in DEBUG for troubleshooting.

**Checkpoint save resilience:** Checkpoint save failures are now non-fatal. If `checkpoint.save()` fails after retries, the pipeline continues with the in-memory `since` value and retries on the next batch. A `WARNING` is logged on failure. The worst case on restart is re-processing docs since the last successful save (at-least-once delivery). See [FAILURE_OPTION_OUTPUT_RDBMS.md §4.1](FAILURE_OPTION_OUTPUT_RDBMS.md).

### `PROCESSING` Log Key

| Level | What is logged |
|---|---|
| **INFO** | Source type, batch completion summary (succeeded/failed counts) |
| **INFO** | Tombstones forwarded count (when `deletes_forwarded > 0`) |
| **DEBUG** | Filter results (input count, filtered count) |

---

## Flood Detection & Backpressure

### The Problem

When the `_changes` feed delivers a very large number of mutations in a
short period (e.g., 100K–1M changes in < 30 seconds), the worker faces
two risks:

1. **Memory pressure (longpoll mode):** The entire `_changes` JSON
   response is loaded into memory via `resp.read()` → `json.loads()`.
   A 500K-row response could consume hundreds of megabytes to gigabytes
   of RAM, risking an OOM kill.

2. **Processing lag:** Even with `max_concurrent: 20`, processing 100K
   docs at 50ms each takes ~4 minutes.  During that time the checkpoint
   has not advanced, so a crash means replaying the entire batch.

### Built-in Protections

| Protection | How it helps |
|---|---|
| **`throttle_feed`** (default 5,000) | Caps each `_changes` request to N rows.  The worker loops immediately for the next bite, so throughput stays high but memory per batch is bounded. |
| **`continuous_catchup_limit`** (default 5,000) | Same cap for the catch-up phase of continuous/websocket mode. |
| **Continuous mode greedy-drain buffering** | In Phase 2, the worker blocks on the first row, then drains everything already in the socket buffer (5ms drain timeout) before flushing as a batch.  This self-tunes: high load → big batches, low load → instant single-doc processing.  TCP backpressure naturally slows the server.  Memory stays flat regardless of flood size. |
| **WebSocket mode greedy-drain buffering** | Same greedy-drain buffering as continuous mode.  Incoming websocket messages are drained and batched together before processing. |
| **`every_n_docs` + `sequential`** | Sub-batch checkpointing limits replay on crash (e.g., `every_n_docs: 1000` means at most 1,000 docs replayed). |
| **Checkpoint hold** | The checkpoint only advances after a batch is fully processed.  If the worker crashes or OOMs, it restarts from `since=<last_saved_seq>` — no data is lost. |

### Flood Metrics

The worker exposes three metrics on `/_metrics` for monitoring flood
scenarios:

| Metric (prefixed `changes_worker_`) | Type | What it tells you |
|---|---|---|
| `changes_pending` | gauge | Gap between `changes_received_total` and `changes_processed_total`.  A steadily growing value means the worker is falling behind. |
| `largest_batch_received` | gauge | Biggest single batch of changes seen since startup.  Useful for capacity planning. |
| `flood_batches_total` | counter | Number of batches that exceeded the `flood_threshold` (default 10,000).  Any non-zero value means the feed has spiked. |

When a batch exceeds the flood threshold, the worker also emits a `FLOOD`
warning log:

```
WARN  FLOOD  flood detected: 150000 changes in single batch (threshold=10000)
```

### Output Backpressure

The `_maybe_backpressure()` mechanism monitors output endpoint latency to
prevent overwhelming a struggling downstream service.

**How it works:**

1. The first **50 samples** of output response times establish a baseline
   average.
2. After the baseline is established, a rolling average of recent response
   times is compared against the baseline.
3. If the rolling average exceeds **2× the baseline**, the worker sleeps
   proportionally to allow the output endpoint to recover (up to **5 seconds
   max**).

**Prometheus metrics:**

| Metric (prefixed `changes_worker_`) | Type | Description |
|---|---|---|
| `backpressure_delays_total` | counter | Total number of backpressure delay events |
| `backpressure_delay_seconds_total` | counter | Cumulative seconds spent in backpressure delays |
| `backpressure_active` | gauge | `1` when the worker is actively throttling, `0` otherwise |

**Log key:** `BACKPRESSURE`

```
WARN  BACKPRESSURE  output latency 2.3x baseline (460ms vs 200ms) – delaying 230ms
```

### Recommended Configuration

For environments that may experience large spikes:

```json
{
  "changes_feed": {
    "feed_type": "continuous",
    "throttle_feed": 5000,
    "continuous_catchup_limit": 5000,
    "flood_threshold": 10000
  },
  "processing": {
    "sequential": true
  },
  "checkpoint": {
    "every_n_docs": 1000
  }
}
```

### Grafana Alerts

```promql
# Changes piling up faster than the worker can process
changes_worker_changes_pending > 50000

# Any flood event in the last 5 minutes
increase(changes_worker_flood_batches_total[5m]) > 0

# RSS spiking during a flood
changes_worker_process_memory_rss_bytes > 1024 * 1024 * 1024
  and increase(changes_worker_changes_received_total[1m]) > 10000
```

---

## Config Reference

The following `config.json` fields control `_changes` processing:

```json
{
  "changes_feed": {
    "feed_type": "longpoll",
    "active_only": true,
    "include_docs": false,
    "since": "0",
    "limit": 0,
    "continuous_catchup_limit": 5000,
    "optimize_initial_sync": false,
    "poll_interval_seconds": 10,
    "heartbeat_ms": 30000,
    "timeout_ms": 60000,
    "http_timeout_seconds": 300,
    "throttle_feed": 5000,
    "flood_threshold": 10000,
    "stream_batch_timeout_ms": 5,
    "channels": []
  },
  "processing": {
    "ignore_delete": false,
    "ignore_remove": false,
    "sequential": false,
    "max_concurrent": 20,
    "get_batch_number": 100
  },
  "checkpoint": {
    "enabled": true,
    "client_id": "changes_worker",
    "every_n_docs": 0
  }
}
```

| Field | Effect on initial sync |
|---|---|
| `optimize_initial_sync` | `false` (default): single large request, no limit. `true`: chunked with `limit`. |
| `continuous_catchup_limit` | Page size when `optimize_initial_sync=true` (default 10,000) |
| `http_timeout_seconds` | Timeout for the single large request (default 300s / 5 min, covers ~1.5M entries) |
| `get_batch_number` | Batch size for `_bulk_get` requests (default 100) |
| `active_only` | Forced to `true` during initial sync (Couchbase); ignored for CouchDB |
| `flood_threshold` | Batch size above which a FLOOD warning is logged and `flood_batches_total` metric is incremented (default 10,000) |
| `stream_batch_timeout_ms` | Greedy drain timeout: after the first row arrives, how long to wait for more rows already in the socket buffer before flushing (default 5ms) |
| `include_docs` | Forced to `false` during initial sync |
| `feed_type` | Overridden to `normal` during initial sync |

After initial sync completes, all fields revert to the user's configured
values.
