# Changes Worker v2.0 – Architecture Redesign

> **Status:** ✅ Phase 10 Foundation Complete (Pipeline + PipelineManager built & tested)
> **Breaking change:** Yes – config format, CBL schema, cbl_store.py API, wizard UI, main.py startup  
> **Goal:** Replace the monolithic `config.json` with a job-centric, composable document model stored in Couchbase Lite collections.

---

## 📋 JSON Schema Standards

**All v2.0 documents must follow the [JSON Schema Standards Guide](../guides/JSON_SCHEMA.md).**

Key standards:
- **Field naming:** Mandatory `snake_case` (no camelCase, no PascalCase)
- **Reserved fields:** `_` prefix forbidden at top level except `meta` container
- **DateTime:** Unix epoch for perf-critical fields, ISO-8601 for UI/readability
- **Enums:** Always lowercase with underscores (e.g., `"sequential"`, `"halt"`)
- **Metadata:** Use `meta` field for application-level metadata (updated_at, saved_at, etc.)
- **Field order:** type/id → config → timestamps → meta

**All document schemas in v2.0 must pass validation against JSON Schema definitions in [`json_schema/changes-worker/`](../json_schema/changes-worker/).**

---

## 🎯 Phases Summary

**Phase 1** ✅ — CBL Schema & `cbl_store.py` Updates  
**Phase 2** ✅ — Migration Logic (v1.x → v2.0)  
**Phase 3** ✅ — Inputs Management API + Tests (14 tests)
**Phase 4** ✅ — Outputs Management API + Tests + UI (12 tests)
**Phase 5** ✅ — Jobs API + Tests (25 tests)
**Phase 6** 📋 — `main.py` Job-Based Startup  
**Phase 7** 📋 — Settings Page Cleanup  
**Phase 8** 📋 — Dashboard Updates  
**Phase 9** 📋 — Schema Mapping Migration  
**Phase 10** 📋 — Multi-Job Threading with PipelineManager (designed, ready to implement)
**Phase 11** 🔮 — MIDDLE Stage Middleware & Data Quality (v2.1)
**Phase 12** 🔮 — Additional Middleware (v2.1+)

**Related docs:**
- [`DESIGN.md`](DESIGN.md) – Current v1.x pipeline architecture
- [`JOBS.md`](JOBS.md) – Current job ID concept (OUTPUT-only)
- [`MULTI_PIPELINE_PLAN.md`](MULTI_PIPELINE_PLAN.md) – Multi-pipeline threading design (reference, folded into v2.0)
- [`HA.md`](HA.md) – High Availability via CBL replication (v3.0)
- [`CBL_DATABASE.md`](CBL_DATABASE.md) – Current CBL schema
- [`SCHEMA_MAPPING.md`](SCHEMA_MAPPING.md) – JSON-to-relational mapping definitions
- [`WIZARD.md`](WIZARD.md) – Source wizard UI
- [`SOURCE_TYPES.md`](SOURCE_TYPES.md) – Source type details (SG, App Services, Edge Server, CouchDB, BLIP)
- [`FAILURE_OPTION_OUTPUT_RDBMS.md`](FAILURE_OPTION_OUTPUT_RDBMS.md) – Failure analysis & resolutions for SOURCE → PROCESS → OUTPUT (RDBMS)
- [`FAILURE_OPTION_OUTPUT_HTTP.md`](FAILURE_OPTION_OUTPUT_HTTP.md) – Failure analysis & resolutions for SOURCE → PROCESS → OUTPUT (HTTP)
- [`FAILURE_OPTION_OUTPUT_CLOUD.md`](FAILURE_OPTION_OUTPUT_CLOUD.md) – Failure analysis & resolutions for SOURCE → PROCESS → OUTPUT (Cloud Blob Storage)

---

## Why This Redesign?

Today's `config.json` is a single monolithic document that holds **everything** – gateway source, auth, feed settings, output destination, processing config, schema mappings reference, checkpoint config, logging, metrics, attachments, and more. This creates several problems:

1. **You can't reuse a source across multiple outputs** – if you want the same `_changes` feed going to both PostgreSQL and an HTTP endpoint, you duplicate the entire config.
2. **You can't reuse an output across multiple sources** – if two `_changes` feeds write to the same PostgreSQL, you duplicate the DB connection config.
3. **The wizard UI has to understand and manipulate the entire config** – adding a new source means editing the same document that controls logging and metrics.
4. **Jobs are implicit** – there's no first-class "job" object that ties a source to an output with a schema mapping.
5. **No clean separation of concerns** – the config mixes infrastructure (logging, metrics, CBL paths) with pipeline logic (source, output, mapping).

---

## New Document Model

### Collections

The CBL database gets **16** collections in the `changes-worker` scope, organized by concern:

#### Pipeline Collections (core data model)

| Collection | Documents | Purpose |
|---|---|---|
| `inputs_changes` | 1 document: `inputs_changes` | Array of all `_changes` feed source definitions |
| `outputs_rdbms` | 1 document: `outputs_rdbms` | Array of RDBMS output configs (postgres, mysql, mssql, oracle) |
| `outputs_http` | 1 document: `outputs_http` | Array of HTTP/REST output configs |
| `outputs_cloud` | 1 document: `outputs_cloud` | Array of cloud blob output configs (S3, GCS, Azure) |
| `tables_rdbms` | 1 document: `tables_rdbms` | Reusable RDBMS table definitions library (DDL + parsed columns). Tables are copied into jobs on selection; the job owns its copy. See [`SCHEMA_MAPPING_IN_JOBS.md`](SCHEMA_MAPPING_IN_JOBS.md#rdbms-table-definitions-new-tables_rdbms-collection). |
| `jobs` | N documents: `job::{uuid}` | Each job connects one input → one output with a schema mapping |

#### Runtime Collections

| Collection | Documents | Purpose |
|---|---|---|
| `checkpoints` | N documents: `checkpoint::{job_uuid}` | Per-job checkpoint state (last_seq, remote counter) |
| `dlq` | N documents: `dlq::{doc_id}::{timestamp}` | Dead letter queue – single shared "trash can" across all jobs |
| `data_quality` | N documents: `dq::{job_id}::{doc_id}::{timestamp}` | Data coercion log — doc was delivered but values were changed to fit the target schema (TTL-purged) |
| `enrichments` | N documents: `enrich::{job_id}::{doc_id}::{timestamp}` | Async analysis results — ML/AI output, attachment analysis, external API enrichment (written async, read by output stage) |

#### Infrastructure Collections

| Collection | Documents | Purpose |
|---|---|---|
| `config` | 1 document: `config` | Global infrastructure config (logging, metrics, admin_ui, CBL, shutdown) |

#### Auth & Identity Collections (future)

| Collection | Documents | Purpose |
|---|---|---|
| `users` | N documents: `user::{username}` | RBAC user accounts (future — roles, permissions, API keys) |
| `sessions` | N documents: `session::{id}` | Sync Gateway sessions, bearer tokens with TTL for auto-refresh |

#### Observability Collections (future)

| Collection | Documents | Purpose |
|---|---|---|
| `audit_log` | N documents: `audit::{timestamp}::{uuid}` | Track who changed what — config edits, job starts/stops, DLQ replays |
| `notifications` | N documents: `notification::{uuid}` | Alert rules and notification history (webhook, email, Slack on job failure) |

### Why Split Outputs by Type?

Mixing RDBMS, HTTP, and cloud configs into one `outputs` document means every entry has a different schema — RDBMS has `engine`, `port`, `pool_max`, `tables[]`; HTTP has `target_url`, `write_method`, `health_check`; cloud has `bucket`, `region`, `storage_class`. Splitting into `outputs_rdbms`, `outputs_http`, `outputs_cloud` means:

1. **Each collection's documents have a consistent schema** — no `class` field to switch on, no optional fields that only apply to one type.
2. **The wizard UI can load/save one collection per tab** — the RDBMS form only touches `outputs_rdbms`, no risk of accidentally corrupting an HTTP output.
3. **Validation is simpler** — validate RDBMS-specific fields in the RDBMS collection, not behind an `if class == "rdbms"` branch.
4. **N1QL queries are cleaner** — `SELECT * FROM outputs_rdbms` vs `SELECT * FROM outputs WHERE class = "rdbms"`.

### Why One Shared DLQ?

Considered: `dlq_rdbms`, `dlq_http`, `dlq_cloud`. Decided against because:

1. **DLQ entries already carry `job_id` and output type metadata** — you can filter by job or output type without separate collections.
2. **One trash can is operationally simpler** — one page in the UI, one `GET /api/dlq` endpoint, one purge schedule.
3. **DLQ entries have the same schema regardless of output type** — `doc_id`, `seq`, `error`, `doc_data`. The output type doesn't change the shape of a failure.
4. **Cross-job DLQ views are useful** — "show me all failures in the last hour" shouldn't require querying 4 collections.

If a future use case demands per-output-type DLQ isolation, we can add a `dlq_type` index and partition logically without splitting physically.

### DLQ vs Data Quality — What's the Difference?

These are **two different outcomes** for a document:

| | DLQ (`dlq`) | Data Quality Log (`data_quality`) |
|---|---|---|
| **Was the doc delivered?** | ❌ No — delivery failed | ✅ Yes — delivered successfully |
| **What happened?** | Output rejected it (HTTP 500, constraint violation, timeout) | Output accepted it, but we had to **change values** to make it fit |
| **Example** | INT overflow → Postgres rejects `INSERT` entirely | INT overflow → we clamped `999999999999` to `2147483647`, inserted OK |
| **Action needed?** | Retry or fix the doc | Informational — review if the coerced value matters |
| **TTL?** | Configurable (`retention_seconds`) | Always TTL'd (default 7 days) — these pile up fast |
| **Blocking?** | Can block pipeline (`halt_on_failure`) | Never blocks — fire-and-forget log |

### Future-Proofing: Other Collections We May Need

| Collection | When | Purpose |
|---|---|---|
| `transforms` | v2.1+ | Reusable transform function libraries (beyond the 58 built-ins). User-defined JavaScript/Python transforms stored and referenced by name in schema mappings. |
| `middleware` | v2.1+ | Registered middleware plugins for the PROCESS stage (Pydantic validators, Pandas batch transforms, geo-enrichment, timestamp normalization). |
| `schedules` | v2.2+ | Cron-like job scheduling — "run this job only between 2am–6am" or "pause on weekends". |
| `secrets` | v2.2+ | Encrypted credential store. Instead of inline passwords in inputs/outputs, reference `secret::pg-prod-password`. Rotation without editing every job. |
| `metrics_snapshots` | v3.0+ | Periodic metric snapshots for historical trending (hourly/daily rollups). Enables "compare this week vs last week" without external Prometheus. |
| `templates` | v3.0+ | Reusable job templates — "PostgreSQL CDC standard" template with pre-filled system config and mapping patterns. |

---

## Document Schemas

### `inputs_changes` Document

**Collection:** `inputs_changes`  
**Doc ID:** `inputs_changes`

A single document holding an array of `_changes` feed source definitions. Each entry in the `src` array is a complete source config that the wizard can create/edit/delete independently.

```json
{
  "type": "input_changes",
  "src": [
    {
      "id": "sg-us-prices",
      "name": "US Prices – Sync Gateway",
      "enabled": true,
      "source_type": "sync_gateway",
      "host": "http://host.docker.internal:4984",
      "database": "db",
      "scope": "us",
      "collection": "prices",
      "accept_self_signed_certs": false,
      "auth": {
        "method": "basic",
        "username": "bob",
        "password": "password",
        "session_cookie": "",
        "bearer_token": ""
      },
      "changes_feed": {
        "feed_type": "longpoll",
        "poll_interval_seconds": 10,
        "active_only": true,
        "include_docs": false,
        "since": "0",
        "channels": [],
        "limit": 0,
        "heartbeat_ms": 30000,
        "timeout_ms": 60000,
        "http_timeout_seconds": 300,
        "throttle_feed": 5000,
        "continuous_catchup_limit": 5000,
        "flood_threshold": 10000,
        "optimize_initial_sync": false
      }
    },
    {
      "id": "sg-us-orders",
      "name": "US Orders – Sync Gateway",
      "enabled": true,
      "source_type": "sync_gateway",
      "host": "http://host.docker.internal:4984",
      "database": "db",
      "scope": "us",
      "collection": "orders",
      "accept_self_signed_certs": false,
      "auth": {
        "method": "basic",
        "username": "bob",
        "password": "password"
      },
      "changes_feed": {
        "feed_type": "continuous",
        "include_docs": true,
        "active_only": true
      }
    }
  ]
}
```

**Key points:**
- `id` is a short, user-friendly slug (auto-generated or user-set). Used to reference this source in a job.
- `name` is a display label for the wizard UI.
- Each `src[]` entry is self-contained: host, auth, feed config are all together. No shared auth.
- The wizard UI manages this array — add/edit/delete entries without touching any other collection.
- `source_type` values: `"sync_gateway"`, `"app_services"`, `"edge_server"`, `"couchdb"`.

#### BLIP Multiplexed Input (v2.5)

Today each `src[]` entry targets **one** scope + **one** collection via the public REST `_changes` API. If you need changes from 5 collections, you create 5 inputs and 5 jobs — 5 separate HTTP connections.

Sync Gateway's **BLIP replication protocol** (the WebSocket-based protocol used by Couchbase Lite) can multiplex `_changes` feeds from **multiple collections within the same scope** over a **single WebSocket connection**. This is an internal implementation detail of the BLIP protocol — not exposed via the public `_changes` REST API — but it's massively more efficient.

In v2.5, a new `source_type: "blip"` enables this:

```json
{
  "id": "sg-us-all",
  "name": "US Scope – All Collections (BLIP)",
  "enabled": true,
  "source_type": "blip",
  "host": "ws://host.docker.internal:4984",
  "database": "db",
  "scope": "us",
  "collections": [
    {
      "name": "prices",
      "channels": ["channel-retail", "channel-wholesale"],
      "include_docs": true
    },
    {
      "name": "orders",
      "channels": [],
      "include_docs": true
    },
    {
      "name": "inventory",
      "channels": ["channel-us-east"],
      "include_docs": false
    }
  ],
  "accept_self_signed_certs": false,
  "auth": {
    "method": "basic",
    "username": "bob",
    "password": "password"
  }
}
```

**What changes:**
- `source_type: "blip"` instead of `"sync_gateway"`.
- `host` uses `ws://` or `wss://` (WebSocket, not HTTP).
- `scope` is still one scope (BLIP multiplexes within a scope).
- `collection` (singular) is replaced by `collections[]` (array) — each with its own `channels` filter.
- One WebSocket connection, one thread, one checkpoint — but receiving demuxed changes from all listed collections.

**Architecture implications:**

```
v2.0 (REST _changes):                    v2.5 (BLIP multiplexed):

  Thread-1: GET /db.us.prices/_changes     Thread-1: ws://sg/db (BLIP)
  Thread-2: GET /db.us.orders/_changes       └── demux: prices changes → job A
  Thread-3: GET /db.us.inventory/_changes    └── demux: orders changes → job B
                                              └── demux: inventory changes → job C
  3 HTTP connections                       1 WebSocket connection
  3 threads                                1 thread (fan-out to jobs internally)
  3 checkpoints                            1 multiplexed checkpoint per collection
```

**Why this matters:**
1. **Connection efficiency** — 1 WebSocket vs N HTTP connections. Fewer sockets, fewer TCP handshakes, less load on SG.
2. **Atomic cross-collection visibility** — changes from all collections arrive on one stream in SG's internal ordering. Useful when orders + inventory must be processed together.
3. **Channel filtering per collection** — each collection in the BLIP config has its own `channels[]` filter. You can get all orders but only US-East inventory.
4. **Checkpoint coherence** — one replication session tracks progress across all collections atomically.

**Implementation notes (source code references):**
- SG server-side: `rest/blip_sync.go` (BLIP WebSocket entry point), `db/blip_handler_collections.go` (per-collection demux).
- The BLIP protocol uses collection IDs in message frames to demux on the client side.
- No public REST API exists for this — you must speak the BLIP protocol directly (or use Couchbase Lite's replicator as a reference).
- We would implement a custom BLIP client in Python using `websockets` library, speaking the same protocol that CBL uses.

**This is a v2.5 feature** — the v2.0 `inputs_changes` schema supports it via `source_type: "blip"`, but the BLIP client implementation comes later.

---

### `outputs_rdbms` Document

**Collection:** `outputs_rdbms`  
**Doc ID:** `outputs_rdbms`

RDBMS output destinations. Each `src[]` entry defines a database connection and its table DDL.

```json
{
  "type": "output_rdbms",
  "src": [
    {
      "id": "pg-local",
      "name": "Local PostgreSQL",
      "enabled": true,
      "engine": "postgres",
      "host": "host.docker.internal",
      "port": 5432,
      "database": "mydb",
      "user": "postgres",
      "password": "secret",
      "schema": "public",
      "ssl": false,
      "pool_min": 2,
      "pool_max": 10,
      "tables": [
        {
          "active": true,
          "name": "orders",
          "sql": "CREATE TABLE IF NOT EXISTS orders (doc_id TEXT PRIMARY KEY, rev TEXT, status TEXT, customer_id TEXT, order_date TIMESTAMP, total NUMERIC)"
        },
        {
          "active": true,
          "name": "order_items",
          "sql": "CREATE TABLE IF NOT EXISTS order_items (id SERIAL PRIMARY KEY, order_doc_id TEXT REFERENCES orders(doc_id), product_id TEXT, qty INTEGER, price NUMERIC)"
        }
      ]
    },
    {
      "id": "mysql-analytics",
      "name": "Analytics MySQL",
      "enabled": true,
      "engine": "mysql",
      "host": "mysql.internal",
      "port": 3306,
      "database": "analytics",
      "user": "etl_user",
      "password": "secret",
      "ssl": true,
      "pool_min": 1,
      "pool_max": 5,
      "tables": [
        {
          "active": true,
          "name": "events",
          "sql": "CREATE TABLE IF NOT EXISTS events (id VARCHAR(255) PRIMARY KEY, payload JSON, created_at DATETIME)"
        }
      ]
    }
  ]
}
```

**Key points:**
- `engine` differentiates RDBMS flavors: `"postgres"`, `"mysql"`, `"mssql"`, `"oracle"`.
- `tables[]` holds DDL definitions. The job runner can auto-create tables on startup.
- Each `tables[]` entry has `active` flag to enable/disable individual tables without deleting them.

> **Note:** Table definitions are migrating to the standalone `tables_rdbms` collection (see below). The `tables[]` field in `outputs_rdbms` remains for backward compatibility and as a "default tables" suggestion when creating new jobs, but the job's own embedded `outputs[].tables[]` is authoritative at runtime. See [`SCHEMA_MAPPING_IN_JOBS.md`](SCHEMA_MAPPING_IN_JOBS.md#rdbms-table-definitions-new-tables_rdbms-collection) for the full design.

---

### `tables_rdbms` Document

**Collection:** `tables_rdbms`  
**Doc ID:** `tables_rdbms`

A library of reusable RDBMS table definitions. Each entry stores the raw DDL and a parsed `columns[]` array. Tables are copied into jobs on selection — the job owns its copy, editing the job's copy does not affect the library (and vice versa).

```json
{
  "type": "tables_rdbms",
  "tables": [
    {
      "id": "tbl-orders",
      "name": "orders",
      "engine_hint": "postgres",
      "sql": "CREATE TABLE IF NOT EXISTS orders (doc_id TEXT PRIMARY KEY, rev TEXT, status TEXT, customer_id TEXT, total NUMERIC(10,2))",
      "columns": [
        { "name": "doc_id", "type": "TEXT", "primary_key": true, "nullable": false },
        { "name": "rev", "type": "TEXT", "primary_key": false, "nullable": true },
        { "name": "status", "type": "TEXT", "primary_key": false, "nullable": true },
        { "name": "customer_id", "type": "TEXT", "primary_key": false, "nullable": true },
        { "name": "total", "type": "NUMERIC(10,2)", "primary_key": false, "nullable": true }
      ],
      "meta": {
        "created_at": "2026-04-20T10:00:00Z",
        "updated_at": "2026-04-22T14:30:00Z",
        "source": "ddl_upload"
      }
    }
  ]
}
```

**Key points:**
- `sql` is the raw DDL — executed against the target DB to create/alter the table.
- `columns[]` is the parsed representation — used by the UI (column pickers, mapping editor, dry-run type validation).
- `engine_hint` records which engine the DDL was written for, but doesn't prevent use with a different engine.
- `parent_table` and `foreign_key` (optional) track table relationships for auto-suggesting child tables.
- When a table is selected for a job, a copy is embedded in `job.outputs[].tables[]` with a `library_ref` field pointing back to the library entry ID.

---

### `outputs_http` Document

**Collection:** `outputs_http`  
**Doc ID:** `outputs_http`

HTTP/REST output destinations.

```json
{
  "type": "output_http",
  "src": [
    {
      "id": "api-orders",
      "name": "Orders REST API",
      "enabled": true,
      "target_url": "https://api.example.com/orders",
      "url_template": "{target_url}/{doc_id}",
      "write_method": "PUT",
      "delete_method": "DELETE",
      "send_delete_body": false,
      "request_timeout_seconds": 30,
      "accept_self_signed_certs": false,
      "follow_redirects": false,
      "output_format": "json",
      "target_auth": {
        "method": "bearer",
        "bearer_token": "eyJ..."
      },
      "retry": {
        "max_retries": 3,
        "backoff_base_seconds": 1,
        "backoff_max_seconds": 30,
        "retry_on_status": [500, 502, 503, 504]
      },
      "request_options": {
        "params": {},
        "headers": {}
      },
      "health_check": {
        "enabled": true,
        "interval_seconds": 30,
        "url": "",
        "method": "GET",
        "timeout_seconds": 5
      }
    }
  ]
}
```

---

### `outputs_cloud` Document

**Collection:** `outputs_cloud`  
**Doc ID:** `outputs_cloud`

Cloud blob storage output destinations (S3, GCS, Azure Blob).

```json
{
  "type": "output_cloud",
  "src": [
    {
      "id": "s3-archive",
      "name": "S3 Archive Bucket",
      "enabled": true,
      "provider": "s3",
      "bucket": "my-archive-bucket",
      "region": "us-east-1",
      "key_prefix": "couchdb-changes",
      "key_template": "{prefix}/{doc_id}.json",
      "key_sanitize": true,
      "content_type": "application/json",
      "storage_class": "STANDARD_IA",
      "server_side_encryption": "",
      "kms_key_id": "",
      "metadata": {},
      "endpoint_url": "",
      "access_key_id": "",
      "secret_access_key": "",
      "session_token": "",
      "on_delete": "delete",
      "batch": {
        "enabled": false,
        "max_docs": 100,
        "max_bytes": 1048576,
        "max_seconds": 5.0
      },
      "max_retries": 3,
      "backoff_base_seconds": 0.5,
      "backoff_max_seconds": 10
    }
  ]
}
```

---

**Key points across all output collections:**
- Each `src[]` entry has a unique `id` used to reference it from a job.
- The job stores which output collection the entry came from (`output_type` field on the job).

---

### `job::{uuid}` Documents

**Collection:** `jobs`  
**Doc ID:** `job::{uuid}` (e.g., `job::a1b2c3d4-e5f6-7890-abcd-ef1234567890`)

A job document is the heart of the processing pipeline. It **copies** the relevant `src[]` entry from `inputs_changes` and an `outputs_*` collection into itself, so the job is fully self-contained at runtime. It also holds the schema mapping and processing config.

```json
{
  "type": "job",
  "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "name": "US Orders → PostgreSQL",
  "enabled": true,
  "created_at": 1768521600,
  "updated_at": 1768521600,
  "output_type": "rdbms",

  "inputs": [
    {
      "id": "sg-us-orders",
      "name": "US Orders – Sync Gateway",
      "source_type": "sync_gateway",
      "host": "http://host.docker.internal:4984",
      "database": "db",
      "scope": "us",
      "collection": "orders",
      "auth": {
        "method": "basic",
        "username": "bob",
        "password": "password"
      },
      "changes_feed": {
        "feed_type": "continuous",
        "include_docs": true,
        "active_only": true
      }
    }
  ],

  "outputs": [
    {
      "id": "pg-local",
      "name": "Local PostgreSQL",
      "engine": "postgres",
      "host": "host.docker.internal",
      "port": 5432,
      "database": "mydb",
      "user": "postgres",
      "password": "secret",
      "schema": "public",
      "ssl": false,
      "pool_min": 2,
      "pool_max": 10,
      "tables": [
        {
          "active": true,
          "name": "orders",
          "sql": "CREATE TABLE IF NOT EXISTS orders (...)"
        }
      ]
    }
  ],

  "schema_mapping": {
    "source": {
      "match": {
        "field": "type",
        "value": "order"
      }
    },
    "output_format": "tables",
    "tables": [
      {
        "name": "orders",
        "primary_key": "doc_id",
        "columns": {
          "doc_id": "$._id",
          "rev": "$._rev",
          "status": "$.status",
          "total": "$.total"
        },
        "on_delete": "delete"
      }
    ]
  },

  "system": {
    "threads": 1,
    "middleware_threads": 2,
    "sequential": false,
    "max_concurrent": 20,
    "dry_run": false,
    "ignore_delete": false,
    "ignore_remove": false,
    "get_batch_number": 100,
    "halt_on_failure": true,
    "data_error_action": "dlq",
    "dead_letter_path": "failed_docs.jsonl",
    "retry": {
      "max_retries": 5,
      "backoff_base_seconds": 1,
      "backoff_max_seconds": 60,
      "retry_on_status": [500, 502, 503, 504]
    },
    "checkpoint": {
      "enabled": true,
      "client_id": "changes_worker",
      "every_n_docs": 0
    },
    "attachments": {
      "enabled": false
    },
    "middleware": []
  },

  "state": {
    "status": "stopped",
    "last_seq": "0",
    "last_error": "",
    "halt_reason": null,
    "last_run_at": 0,
    "docs_processed": 0
  }
}
```

**Key design decisions:**

1. **`inputs` is an array** (currently with one entry) — future-proofs for fan-in (N sources → 1 output).
2. **`outputs` is an array** (currently with one entry) — future-proofs for fan-out (1 source → N outputs).
3. **`output_type`** records which `outputs_*` collection the output was copied from (`"rdbms"`, `"http"`, `"cloud"`). The worker uses this to know which output forwarder to instantiate.
4. **Data is copied, not referenced** — the job is self-contained. If you change the `inputs_changes` or `outputs_rdbms` document later, existing jobs are NOT affected until you explicitly update them. This prevents "changing a source and accidentally breaking 5 jobs".
5. **`schema_mapping` is embedded** — each job has its own mapping. The `mappings/` directory and `mappings` CBL collection are phased out as an edit surface; the mapping lives in the job.
6. **`system` holds all processing config** — threads, concurrency, retry, checkpoint, attachments. This is the "how to run" config.
7. **`state` is runtime state** — updated by the worker as it runs. The wizard/UI can read this to show job status. `halt_reason` is set to `"auth_failure"`, `"eventing_halt"`, or `"ssl_failure"` when the pipeline stops on a non-recoverable error (PipelineManager skips auto-restart). `null` for normal running/stopped states.
8. **Checkpoint is external** — stored in the `checkpoints` collection as `checkpoint::{job_uuid}`, not embedded in the job document. This separates config (rarely changes) from runtime state (changes every batch).

---

### `config` Document (Global)

**Collection:** `config`  
**Doc ID:** `config`

Slimmed down to infrastructure-only. No pipeline logic.

```json
{
  "type": "config",
  "data": {
    "admin_ui": {
      "enabled": true,
      "host": "0.0.0.0",
      "port": 8080
    },
    "metrics": {
      "enabled": true,
      "host": "0.0.0.0",
      "port": 9090
    },
    "logging": {
      "redaction_level": "partial",
      "console": {
        "enabled": true,
        "log_level": "info",
        "log_keys": ["*"],
        "color_enabled": false
      },
      "file": {
        "enabled": true,
        "path": "logs/changes_worker.log",
        "log_level": "debug",
        "rotation": {
          "max_size": 100,
          "max_age": 7,
          "rotated_logs_size_limit": 1024
        }
      }
    },
    "couchbase_lite": {
      "db_dir": "/app/data",
      "db_name": "changes_worker_db",
      "maintenance": {
        "enabled": true,
        "interval_hours": 24
      }
    },
    "shutdown": {
      "drain_timeout_seconds": 60,
      "dlq_inflight_on_shutdown": false
    }
  },
  "updated_at": 1768521600
}
```

---

### `checkpoint::{job_uuid}` Documents

**Collection:** `checkpoints`  
**Doc ID:** `checkpoint::{job_uuid}` (e.g., `checkpoint::a1b2c3d4-e5f6-7890-abcd-ef1234567890`)

Per-job checkpoint state. Written frequently (every batch or every N docs). Separated from the job document to avoid churning the job's revision on every checkpoint save.

```json
{
  "type": "checkpoint",
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "client_id": "changes_worker",
  "SGs_Seq": "1500",
  "time": 1768521600,
  "remote": 42,
  "initial_sync_done": true
}
```

**Key points:**
- The primary checkpoint remains on Sync Gateway as a `_local` document (unchanged from v1.x).
- This CBL checkpoint is the **fallback** — used when SG is unreachable for checkpoint ops.
- `job_id` links back to the owning job for cleanup when a job is deleted.

---

### `session::{id}` Documents

**Collection:** `sessions`  
**Doc ID:** `session::{id}` (e.g., `session::sg-prod-bob`)

Stores Sync Gateway session cookies and bearer tokens with TTL for auto-refresh. Instead of hardcoding a session cookie in the input config, the worker can manage session lifecycle here.

```json
{
  "type": "session",
  "id": "sg-prod-bob",
  "name": "SG Prod – Bob's session",
  "input_id": "sg-us-prices",
  "session_type": "session_cookie",
  "value": "SyncGatewaySession=abc123...",
  "created_at": 1768521600,
  "expires_at": 1768608000,
  "ttl_seconds": 86400,
  "auto_refresh": true,
  "refresh_url": "http://host.docker.internal:4985/_session",
  "refresh_auth": {
    "username": "bob",
    "password": "password"
  },
  "last_refreshed_at": 1768521600,
  "status": "active"
}
```

**Key points:**
- `session_type`: `"session_cookie"`, `"bearer_token"`, or `"oidc_token"`.
- `auto_refresh`: if true, the worker refreshes the session before `expires_at` using `refresh_url`.
- Inputs can reference a session by `session_id` instead of inlining credentials.
- TTL-based expiration — the worker purges expired sessions on the maintenance schedule.

---

### `user::{username}` Documents (Future – RBAC)

**Collection:** `users`  
**Doc ID:** `user::{username}` (e.g., `user::admin`)

Future RBAC user accounts for the admin UI. Not implemented in v2.0 — collection is created but empty.

```json
{
  "type": "user",
  "username": "admin",
  "display_name": "Admin User",
  "email": "admin@example.com",
  "password_hash": "$argon2id$v=19$...",
  "role": "admin",
  "permissions": ["jobs:read", "jobs:write", "config:write", "dlq:write", "users:write"],
  "api_key": "cwk_live_...",
  "enabled": true,
  "created_at": 1768521600,
  "last_login_at": 0
}
```

**Planned roles:**
- `admin` — full access to everything
- `operator` — start/stop jobs, view config, replay DLQ, but cannot edit users or global config
- `viewer` — read-only access to dashboard, metrics, DLQ, logs

---

### `dq::{job_id}::{doc_id}::{timestamp}` Documents

**Collection:** `data_quality`  
**Doc ID:** `dq::a1b2c3d4::order::12345::1768521600`

Logged when the MIDDLE stage coerces a value to fit the target schema. The document **was delivered successfully** — this is an informational audit trail, not a failure. TTL-purged automatically.

```json
{
  "type": "data_quality",
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "doc_id_original": "order::12345",
  "seq": "1500",
  "time": 1768521600,
  "expires_at": 1769126400,
  "table": "orders",
  "coercions": [
    {
      "column": "total",
      "original_value": "999999999999",
      "original_type": "int",
      "coerced_value": "2147483647",
      "coerced_type": "int",
      "reason": "int4_overflow",
      "action": "clamp_max",
      "rule": "schema_mapping.tables[0].columns.total"
    },
    {
      "column": "customer_name",
      "original_value": "John Jacob Jingleheimer Schmidt-Worthington III, Esq., PhD., etc etc etc...",
      "original_type": "str",
      "coerced_value": "John Jacob Jingleheimer Schmidt-Wort",
      "coerced_type": "str",
      "reason": "varchar_truncation",
      "action": "truncate",
      "rule": "VARCHAR(36) limit"
    }
  ],
  "delivered": true,
  "output_type": "rdbms",
  "engine": "postgres"
}
```

**Coercion actions (built-in):**

| `action` | What it does | Example |
|---|---|---|
| `clamp_max` | Clamp to max value for target type | INT8 `999999999999` → INT4 `2147483647` |
| `clamp_min` | Clamp to min value | Negative overflow → `-2147483648` |
| `truncate` | Truncate string to column max length | `VARCHAR(36)` limit |
| `cast` | Cast to target type | String `"42.5"` → float `42.5` |
| `nullify` | Replace with NULL | Unparseable date → `NULL` |
| `default` | Replace with column default value | Invalid enum → default value |
| `round` | Round numeric to target precision | `NUMERIC(10,2)`: `3.14159` → `3.14` |
| `epoch_to_timestamp` | Convert Unix epoch to timestamp | `1768521600` → `2026-01-16 00:00:00` |

**Key points:**
- **TTL-purged** — `expires_at` is set to `time + retention_seconds` (default 7 days). The CBL maintenance scheduler cleans these up.
- **Never blocks the pipeline** — coercion log is fire-and-forget. If the CBL write fails, log it and move on.
- **UI view** — "Data Quality" page shows recent coercions grouped by job, table, column. Helps identify schema mismatches that should be fixed at the source.

---

### `enrich::{job_id}::{doc_id}::{timestamp}` Documents

**Collection:** `enrichments`  
**Doc ID:** `enrich::a1b2c3d4::order::12345::1768521600`

Stores async analysis results from middleware hooks (ML/AI, external API calls, attachment analysis). Written asynchronously — the pipeline doesn't wait for these. The output stage can optionally read enrichments before sending to the target.

```json
{
  "type": "enrichment",
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "doc_id_original": "order::12345",
  "seq": "1500",
  "time": 1768521600,
  "expires_at": 1769126400,
  "source": "attachment_analysis",
  "attachment_url": "s3://my-bucket/attachments/order::12345/receipt.jpg",
  "status": "complete",
  "results": {
    "caption": "A scanned receipt from Acme Corp showing 3 line items totaling $142.50",
    "ocr_text": "ACME CORP\nInvoice #12345\nWidget A x2 $50.00\nWidget B x1 $42.50\nTotal: $142.50",
    "has_text": true,
    "detected_objects": ["receipt", "text", "logo"],
    "embedding": [0.023, -0.041, 0.087, "...512 floats..."],
    "content_type": "image/jpeg",
    "analysis_ms": 340
  },
  "error": null
}
```

**Key points:**
- Written by async middleware hooks (see "MIDDLE Stage Middleware" below).
- `source` identifies which middleware produced this: `"attachment_analysis"`, `"pydantic_validation"`, `"geo_enrichment"`, `"llm_summary"`, etc.
- `status`: `"pending"`, `"complete"`, `"error"`.
- The output stage can optionally merge enrichment data into the doc before sending (configured per-job).
- TTL-purged like `data_quality`.

---

## MIDDLE Stage Middleware Pipeline (v2.1)

The current three-stage pipeline is `LEFT (source) → MIDDLE (filter, fetch, transform) → RIGHT (output)`. In v2.0 we add **middleware hooks** to the MIDDLE stage — a chain of pluggable processing steps that run after the schema mapping transform and before the output send.

```
LEFT                    MIDDLE                                                RIGHT
─────                   ──────                                                ─────
_changes  ──►  filter  ──►  fetch docs  ──►  schema mapping  ──►  MIDDLEWARE CHAIN  ──►  output send
               │              │                    │                    │                    │
               │              │                    │           ┌───────┴────────┐           │
               │              │                    │           │  1. validate   │           │
               │              │                    │           │  2. coerce     │──► data_quality
               │              │                    │           │  3. enrich     │──► enrichments
               │              │                    │           │  4. transform  │           │
               │              │                    │           │  5. attach_ml  │──► enrichments
               │              │                    │           └───────┬────────┘           │
               │              │                    │                    │                    │
               skip           bulk_get             map JSON→SQL        doc (possibly        send to
               deletes/       or individual        columns             modified)            RDBMS/HTTP/
               removes        GET                                                          cloud
```

### Middleware Hook Points

Each job can configure an ordered list of middleware in `system.middleware`:

```json
{
  "system": {
    "middleware": [
      {
        "name": "pydantic_coerce",
        "enabled": true,
        "phase": "pre_output",
        "config": {
          "on_coercion": "log_and_continue",
          "coercion_ttl_seconds": 604800,
          "strict_fields": ["doc_id", "customer_id"],
          "coerce_fields": ["total", "order_date", "customer_name"]
        }
      },
      {
        "name": "timestamp_normalize",
        "enabled": true,
        "phase": "pre_output",
        "config": {
          "fields": ["order_date", "created_at", "updated_at"],
          "target_timezone": "UTC",
          "formats": ["iso8601", "epoch_seconds", "epoch_millis"]
        }
      },
      {
        "name": "geo_enrich",
        "enabled": false,
        "phase": "pre_output",
        "config": {
          "lat_field": "$.location.lat",
          "lng_field": "$.location.lng",
          "output_fields": ["country_code", "timezone", "city"]
        }
      },
      {
        "name": "attachment_ml",
        "enabled": true,
        "phase": "post_attachment_upload",
        "async": true,
        "config": {
          "analyses": ["caption", "ocr", "embedding"],
          "models": {
            "caption": "Salesforce/blip-image-captioning-base",
            "embedding": "sentence-transformers/all-MiniLM-L6-v2"
          },
          "inject_into_doc": true,
          "inject_field": "attachment_analysis",
          "store_in_enrichments": true
        }
      },
      {
        "name": "pandas_batch",
        "enabled": false,
        "phase": "pre_output_batch",
        "config": {
          "batch_size": 100,
          "batch_timeout_seconds": 5,
          "operations": [
            "deduplicate(subset=['doc_id'])",
            "normalize('customer_name', form='NFC')",
            "fill_na(column='status', value='unknown')"
          ]
        }
      }
    ]
  }
}
```

### Middleware Phases

| Phase | When it runs | Sync/Async | Use cases |
|---|---|---|---|
| `pre_output` | After schema mapping, before output send. Runs **per-doc**. | Sync (blocks pipeline) | Pydantic validation/coercion, type casting, timestamp normalization, geo-enrichment, field injection |
| `pre_output_batch` | After schema mapping, before output send. Runs on a **batch window** of docs. | Sync (blocks pipeline) | Pandas/Polars batch transforms, deduplication, join with reference data, derived columns |
| `post_attachment_upload` | After attachment upload succeeds. Has the cloud URL. | **Async** (fire-and-forget) | ML image analysis, OCR, embedding generation, LLM summarization |
| `post_output` | After successful output delivery. | **Async** (fire-and-forget) | Webhook notifications, audit logging, cache invalidation |

### Built-in Middleware (v2.1 targets)

| Middleware | Phase | Library | What it does |
|---|---|---|---|
| `pydantic_coerce` | `pre_output` | Pydantic + Pandera | Validate doc against target schema. Auto-coerce types (str→int, epoch→datetime). Log coercions to `data_quality`. Reject truly invalid docs to DLQ. |
| `timestamp_normalize` | `pre_output` | python-dateutil + pytz | Normalize timestamp fields across timezones. Parse any format (ISO, epoch sec/ms, custom) → target timezone. |
| `geo_enrich` | `pre_output` | GeoPy + country-converter | Reverse-geocode lat/lng → country code, timezone, city. Add fields to doc before output. |
| `pandas_batch` | `pre_output_batch` | Pandas (or Polars) | Batch N docs into a DataFrame, run transforms (deduplicate, normalize, derived columns, fill NA), output back to doc stream. |
| `attachment_ml` | `post_attachment_upload` | transformers, easyocr, sentence-transformers | Run image captioning, OCR, embedding on uploaded attachments. Store results in `enrichments` collection. Optionally inject a reference field into the doc. |

### How `pydantic_coerce` Works with `data_quality`

```
Doc arrives at pre_output middleware:
  {"doc_id": "order::12345", "total": 999999999999, "name": "Very Long Name..."}

1. pydantic_coerce checks total against Postgres INT4 range:
   → 999999999999 > 2147483647 → COERCE: clamp to 2147483647
   → Log to data_quality collection: {action: "clamp_max", original: 999999999999, coerced: 2147483647}

2. pydantic_coerce checks name against VARCHAR(36):
   → length 50 > 36 → COERCE: truncate to 36 chars
   → Log to data_quality: {action: "truncate", original: "Very Long...", coerced: "Very Lo..."}

3. Modified doc continues to RIGHT stage:
   {"doc_id": "order::12345", "total": 2147483647, "name": "Very Lo..."}
   → INSERT succeeds ✅

4. data_quality entry is TTL'd (expires in 7 days)
   → UI shows: "order::12345 had 2 coercions on this batch"
   → Operator can decide: fix the source data, widen the column, or accept the coercion
```

### How `attachment_ml` Works with `enrichments`

```
Attachment uploaded to S3:
  s3://bucket/attachments/order::12345/receipt.jpg → URL returned

1. attachment_ml middleware fires ASYNC (does not block pipeline)

2. In background thread/executor:
   → Download image from S3 URL
   → Run BLIP captioning: "A scanned receipt from Acme Corp..."
   → Run EasyOCR: "ACME CORP\nInvoice #12345\n..."
   → Generate embedding vector

3. Write to enrichments collection:
   enrich::a1b2c3d4::order::12345::1768521600

4. If inject_into_doc=true AND doc hasn't been sent to output yet:
   → Inject {"attachment_analysis": {"status": "pending", "enrichment_id": "enrich::..."}}
   → Output receives the doc with a pointer to where the ML results will be

5. If doc already sent (async arrived late):
   → Enrichment sits in collection for later batch query / join
```

---

## Uniform Schema Pattern

All top-level documents follow the same shape to keep things consistent:

```json
{
  "type": "<input_changes|output_rdbms|output_http|output_cloud>",
  "src": [...]        // catalog collections use src[]
}

{
  "type": "job",
  "output_type": "<rdbms|http|cloud>",
  "inputs": [...],    // jobs use inputs[] and outputs[]
  "outputs": [...]
}
```

The `src[]` pattern is shared across all catalog collections (`inputs_changes`, `outputs_rdbms`, `outputs_http`, `outputs_cloud`). When creating a job, the UI copies `inputs_changes.src[i]` into `job.inputs[0]` and `outputs_{type}.src[j]` into `job.outputs[0]`.

---

## Data Flow: Creating a Job

```
┌───────────────────────┐                              ┌───────────────────────────┐
│  inputs_changes doc   │                              │  job::{uuid} document     │
│                       │                              │                           │
│  src: [               │                              │  output_type: "rdbms",    │
│    {id:"sg-1",...}    │──── pick sg-1 ──────────────►│  inputs: [                │
│    {id:"sg-2",...}    │                              │    { ...copy of sg-1... } │
│  ]                    │                              │  ],                       │
└───────────────────────┘                              │  outputs: [               │
                                                       │    { ...copy of pg-1... } │
┌───────────────────────┐                              │  ],                       │
│  outputs_rdbms doc    │                              │  schema_mapping: {...},   │
│                       │                              │  system: {...},           │
│  src: [               │──── pick pg-1 ──────────────►│  state: {...}             │
│    {id:"pg-1",...}    │                              └──────────┬────────────────┘
│    {id:"mysql-1",...} │                                         │
│  ]                    │                              ┌──────────▼────────────────┐
└───────────────────────┘                              │  checkpoint::{uuid}       │
                                                       │  (checkpoints collection) │
┌───────────────────────┐                              │  SGs_Seq: "0"             │
│  outputs_http doc     │   (not used for this job)    └───────────────────────────┘
└───────────────────────┘

         Wizard: "pick a source"  (from inputs_changes)
         Wizard: "pick an output type"  (rdbms / http / cloud)
         Wizard: "pick an output"  (from outputs_{type})
         Wizard: "configure mapping"
         Wizard: "set processing config"
         → Save → job::{uuid} created + checkpoint::{uuid} initialized
```

---

## CBL Collection Layout (v2.0)

```
CBL Database: changes_worker_db
│
└── Scope: "changes-worker"
    │
    │── ── Pipeline ── ── ── ── ── ── ── ── ── ── ── ── ── ──
    ├── Collection: inputs_changes   ← 1 doc: "inputs_changes"
    ├── Collection: outputs_rdbms    ← 1 doc: "outputs_rdbms"
    ├── Collection: outputs_http     ← 1 doc: "outputs_http"
    ├── Collection: outputs_cloud    ← 1 doc: "outputs_cloud"
    ├── Collection: jobs             ← N docs: "job::{uuid}"
    │
    │── ── Runtime ── ── ── ── ── ── ── ── ── ── ── ── ── ──
    ├── Collection: checkpoints      ← N docs: "checkpoint::{job_uuid}"
    ├── Collection: dlq              ← N docs: "dlq::{doc_id}::{timestamp}"
    ├── Collection: data_quality     ← N docs: "dq::{job_id}::{doc_id}::{ts}" (TTL)
    ├── Collection: enrichments      ← N docs: "enrich::{job_id}::{doc_id}::{ts}" (TTL)
    │
    │── ── Infrastructure ── ── ── ── ── ── ── ── ── ── ── ──
    ├── Collection: config           ← 1 doc: "config" (infra only)
    │
    │── ── Auth & Identity (future) ── ── ── ── ── ── ── ── ──
    ├── Collection: users            ← N docs: "user::{username}"
    ├── Collection: sessions         ← N docs: "session::{id}"
    │
    │── ── Observability (future) ── ── ── ── ── ── ── ── ── ──
    ├── Collection: audit_log        ← N docs: "audit::{timestamp}::{uuid}"
    └── Collection: notifications    ← N docs: "notification::{uuid}"
```

---

## Migration Plan (v1.x → v2.0)

On first v2.0 startup, if the old `config` document exists in the `config` collection with the v1.x schema (contains `gateway`, `output`, `changes_feed` keys), run a one-time migration:

1. **Extract input** from `config.gateway` + `config.auth` + `config.changes_feed` → create `inputs_changes` document with one `src[]` entry.
2. **Extract output** from `config.output` → create the appropriate `outputs_{type}` document based on `output.mode`:
   - `mode` = `postgres`/`mysql`/`mssql`/`oracle`/`db` → `outputs_rdbms`
   - `mode` = `http` → `outputs_http`
   - `mode` = `s3` → `outputs_cloud`
3. **Create one job** from the extracted input + output + existing schema mappings → `job::{uuid}`.
4. **Create checkpoint** from existing checkpoint data → `checkpoint::{job_uuid}` in the `checkpoints` collection.
5. **Slim down config** to infrastructure-only keys.
6. **Mark migration complete** with a `"schema_version": "2.0"` field on the config document.

---

## Work Breakdown – Implementation Phases

Each phase is designed to be done in a **separate chat/thread**. Phases are ordered by dependency — later phases depend on earlier ones.

### Phase 1: CBL Schema & `cbl_store.py` Updates

**Status:** ✅ COMPLETED

**Goal:** Add new collections and CRUD methods to `cbl_store.py`.

- [x] Add constants:
  - `COLL_INPUTS_CHANGES = "inputs_changes"`
  - `COLL_OUTPUTS_RDBMS = "outputs_rdbms"`
  - `COLL_OUTPUTS_HTTP = "outputs_http"`
  - `COLL_OUTPUTS_CLOUD = "outputs_cloud"`
  - `COLL_JOBS = "jobs"`
  - `COLL_CHECKPOINTS = "checkpoints"` (repurposed — now per-job)
  - `COLL_SESSIONS = "sessions"`
  - `COLL_USERS = "users"`
  - `COLL_DATA_QUALITY = "data_quality"`
  - `COLL_ENRICHMENTS = "enrichments"`
  - `COLL_AUDIT_LOG = "audit_log"`
  - `COLL_NOTIFICATIONS = "notifications"`
- [x] Remove `COLL_MAPPINGS` constant (phased out)
- [x] Add `CBLStore` methods:
  - `load_inputs_changes() -> dict | None` — load the `inputs_changes` document
  - `save_inputs_changes(data: dict)` — save the `inputs_changes` document
  - `load_outputs(output_type: str) -> dict | None` — load `outputs_{type}` document
  - `save_outputs(output_type: str, data: dict)` — save `outputs_{type}` document
  - `load_job(job_id: str) -> dict | None` — load a `job::{uuid}` document
  - `save_job(job_id: str, data: dict)` — save a `job::{uuid}` document
  - `delete_job(job_id: str)` — purge a job document + its checkpoint
  - `list_jobs() -> list[dict]` — list all job documents (N1QL query)
  - `update_job_state(job_id: str, state: dict)` — update only the `state` sub-object
  - `load_checkpoint(job_id: str) -> dict | None` — load checkpoint for a job
  - `save_checkpoint(job_id: str, data: dict)` — save checkpoint for a job
  - `load_session(session_id: str) -> dict | None` — load a session
  - `save_session(session_id: str, data: dict)` — save a session
  - `list_sessions() -> list[dict]` — list all sessions
  - `delete_expired_sessions()` — purge sessions past `expires_at`
- [x] Keep existing `load_config()` / `save_config()` working (now for infra-only config)
- [x] Keep existing DLQ methods unchanged (add `job_id` field to DLQ entries)
- [x] Add `"schema_version"` field to config document
- [x] Write unit tests for all new methods

### Phase 2: Migration Logic

**Status:** ✅ COMPLETED

**Goal:** Auto-migrate v1.x config to v2.0 schema on startup.

- [x] Add `migrate_v1_to_v2()` function in `cbl_store.py`
- [x] Extract `gateway` + `auth` + `changes_feed` → `inputs_changes` document
- [x] Extract `output` → appropriate `outputs_{type}` document based on `output.mode`
- [x] Extract `processing` + `checkpoint` + `retry` + `attachments` → `system` in a new job
- [x] Copy existing checkpoint → `checkpoint::{job_uuid}` in `checkpoints` collection
- [x] Copy existing mappings from `mappings` collection into the job's `schema_mapping`
- [x] Slim the `config` document to infra-only keys
- [x] Set `config.schema_version = "2.0"` to prevent re-migration
- [x] Ensure idempotency — running migration twice is safe
- [x] Write integration test: start with v1.x config.json → verify v2.0 documents exist
- [x] Update `migrate_files_to_cbl()` to handle the new document layout

### Phase 3: Wizard UI – Inputs Management ✅

**Goal:** Update `wizard.html` to manage the `inputs_changes` document.

- [x] Add "Inputs" tab/section to wizard
- [x] Form to add a new `_changes` source (source_type dropdown, host, db, scope, collection, auth, feed config)
- [x] List existing inputs with edit/delete
- [x] Save button calls `POST /api/inputs_changes` → `CBLStore.save_inputs_changes()`
- [x] Add REST endpoints:
  - `GET /api/inputs_changes` — load inputs_changes document
  - `POST /api/inputs_changes` — save inputs_changes document
  - `PUT /api/inputs_changes/{id}` — update one `src[]` entry
  - `DELETE /api/inputs_changes/{id}` — remove one `src[]` entry
- [x] Validate each input entry on save (source_type, host required, auth fields)

### Phase 4: Wizard UI – Outputs Management

**Goal:** Update `wizard.html` to manage `outputs_rdbms`, `outputs_http`, `outputs_cloud`.

- [ ] Add "Outputs" tab/section to wizard with sub-tabs for each output type
- [ ] **RDBMS tab** (`outputs_rdbms`):
  - engine dropdown (postgres/mysql/mssql/oracle), host, port, db, user, password, SSL, pool config
  - Sub-form to add table DDL (`sql` field), mark tables active/inactive
  - Save → `POST /api/outputs_rdbms`
- [ ] **HTTP tab** (`outputs_http`):
  - target_url, url_template, write/delete method, auth, retry, health check, request options
  - Save → `POST /api/outputs_http`
- [ ] **Cloud tab** (`outputs_cloud`):
  - provider dropdown (s3/gcs/azure), bucket, region, keys, storage class, encryption
  - Save → `POST /api/outputs_cloud`
- [ ] List existing outputs per tab with edit/delete
- [ ] Add REST endpoints (same pattern for each type):
  - `GET /api/outputs_{type}` — load document
  - `POST /api/outputs_{type}` — save document
  - `PUT /api/outputs_{type}/{id}` — update one `src[]` entry
  - `DELETE /api/outputs_{type}/{id}` — remove one `src[]` entry

### Phase 5: Wizard UI – Job Creation

**Goal:** Add job creation flow to wizard.

- [ ] Add "Jobs" tab/section to wizard
- [ ] Job creation flow:
  1. Pick an input from `inputs_changes.src[]` dropdown
  2. Pick an output type (rdbms / http / cloud)
  3. Pick an output from `outputs_{type}.src[]` dropdown
  4. Configure schema mapping (reuse existing mapping editor from `schema.html`)
  5. Configure `system` settings (threads, concurrency, retry, etc.)
  6. Save → generate UUID, copy selected input/output entries, set `output_type`, create `job::{uuid}` + `checkpoint::{uuid}`
- [ ] List existing jobs with status indicator (`state.status`)
- [ ] Edit job (update system config, mapping — does NOT re-copy input/output unless user explicitly "refreshes" from source)
- [ ] Delete job (purge job document + its checkpoint)
- [ ] Add REST endpoints:
  - `GET /api/jobs` — list all jobs
  - `GET /api/jobs/{id}` — get one job
  - `POST /api/jobs` — create a new job
  - `PUT /api/jobs/{id}` — update a job
  - `DELETE /api/jobs/{id}` — delete a job + checkpoint
  - `POST /api/jobs/{id}/refresh-input` — re-copy input from `inputs_changes` document
  - `POST /api/jobs/{id}/refresh-output` — re-copy output from `outputs_{type}` document

### Phase 6: `main.py` – Job-Based Startup

**Goal:** Refactor `main.py` to read jobs and start pipelines from them.

- [ ] On startup: load all enabled jobs from `jobs` collection
- [ ] For each job: build the pipeline config from `job.inputs[0]` + `job.outputs[0]` + `job.system`
- [ ] Pass job config to `poll_changes()` (refactor to accept the new shape)
- [ ] Update `validate_config()` to validate job documents instead of the monolithic config
- [ ] Update `build_base_url()`, `build_auth_headers()`, `build_basic_auth()` to work with the new input shape
- [ ] Update `MetricsCollector` to accept `job_id` from the job document
- [ ] Update `Checkpoint` to read/write from `checkpoints` collection using `checkpoint::{job_uuid}`
- [ ] Instantiate the correct output forwarder based on `job.output_type`
- [ ] Backward compatibility: if no jobs exist and old v1.x config is present, auto-migrate (Phase 2)
- [ ] Update `--validate` CLI flag to validate the new format

### Phase 7: Settings Page Cleanup

**Goal:** Update `settings.html` to only show infrastructure config.

- [ ] Remove gateway, auth, changes_feed, output, processing, checkpoint sections from settings
- [ ] Keep: admin_ui, metrics, logging, couchbase_lite, shutdown
- [ ] Update `GET /api/config` and `POST /api/config` to use the slimmed config schema
- [ ] Add link to wizard for pipeline configuration

### Phase 8: Dashboard Updates

**Goal:** Update `index.html` dashboard to be job-aware.

- [ ] Add job selector dropdown (if multiple jobs exist)
- [ ] Show per-job metrics using job ID labels
- [ ] Show job status (`state.status`) on the dashboard
- [ ] Update charts to scope to selected job
- [ ] Show "No jobs configured" state with link to wizard

### Phase 9: Schema Mapping Migration

**Goal:** Move mappings from `mappings/` files + `mappings` collection into jobs.

- [ ] On startup migration: for each mapping file, find the job it belongs to and embed it
- [ ] Update `schema/mapper.py` to load mapping from the job document instead of the `mappings` collection
- [ ] Keep `mappings/` directory as an optional import surface (like v1.x `config.json` seeding)
- [ ] Remove `COLL_MAPPINGS` usage from `cbl_store.py` (keep for migration read-only)
- [ ] Update `schema.html` to edit the mapping within a job context

### Phase 10: Multi-Job Threading with PipelineManager

**Goal:** Run multiple jobs concurrently with clean separation of concerns. This is now v2.0 core because async middleware (ML, enrichments) requires proper threading from day one.

#### Design

**Three-layer threading model:**

```
main()
  │
  ├── validate config / run migrations
  ├── start shared services (metrics :9090, admin UI :8080, CBL maintenance)
  │
  ├── PipelineManager (main thread)
  │     │
  │     ├── Thread-1: Pipeline("job::aaa")
  │     │     ├── asyncio.run(poll_changes(job_config_1))
  │     │     └── ThreadPoolExecutor(2) → async middleware (ML, enrichment)
  │     │
  │     ├── Thread-2: Pipeline("job::bbb")
  │     │     ├── asyncio.run(poll_changes(job_config_2))
  │     │     └── ThreadPoolExecutor(2)
  │     │
  │     └── Thread-3: Pipeline("job::ccc")
  │           ├── asyncio.run(poll_changes(job_config_3))
  │           └── ThreadPoolExecutor(2)
  │
  ├── wait for shutdown signal (SIGINT / SIGTERM) ← main thread blocks here
  └── PipelineManager.stop() → drain all jobs → save checkpoints → close
```

**`PipelineManager` responsibilities:**
- Load all enabled jobs from `jobs` collection at startup
- Create one `Pipeline` instance per job
- Start/stop/restart individual jobs (via REST API or lifecycle events)
- Enforce global `max_threads` config (max concurrent pipelines running)
- Monitor job threads for crashes; restart with exponential backoff
- **Skip auto-restart for non-recoverable failures:** `_monitor_threads()` checks `Pipeline.get_state()` flags and skips restart when:
  - `_auth_failure = True` — source returned HTTP 401/403 (see [FAILURE_OPTION_OUTPUT_RDBMS.md §1.3](FAILURE_OPTION_OUTPUT_RDBMS.md))
  - `_eventing_halt = True` — user's eventing handler raised `EventingHalt` (see [FAILURE_OPTION_OUTPUT_RDBMS.md §2.4](FAILURE_OPTION_OUTPUT_RDBMS.md))
  - `_ssl_failure = True` — SSL/TLS handshake failed on RDBMS output (see [FAILURE_OPTION_OUTPUT_RDBMS.md §3.14](FAILURE_OPTION_OUTPUT_RDBMS.md))
  - In all three cases the job stays in `error` state with checkpoint preserved until manually restarted.
- Graceful shutdown: signal all pipelines, drain in-flight changes, save checkpoints
- Expose job state (running/stopped/error/halted) via REST `/api/jobs/{id}/state`

**`Pipeline` (per-job thread) responsibilities:**
- Wraps a `threading.Thread` + isolated `asyncio.run()` event loop
- Owns its HTTP session (persistent connection to Sync Gateway)
- Owns its checkpoint state (resumed from `checkpoints::{job_uuid}`)
- Owns its output forwarder (PostgreSQL, HTTP, S3)
- Owns its `ThreadPoolExecutor` for async middleware
- Accepts a job document + resolved input/output/mapping config
- Catches exceptions → writes to DLQ → logs with job_id tag
- Periodically writes checkpoint during the feed loop
- **Non-fatal checkpoint saves:** checkpoint save failures are caught and logged; pipeline continues with in-memory `since` and retries on next batch (see [FAILURE_OPTION_OUTPUT_RDBMS.md §4.1](FAILURE_OPTION_OUTPUT_RDBMS.md))
- **`get_state()` exposes halt flags:** `_auth_failure`, `_eventing_halt`, `_ssl_failure` — consumed by `PipelineManager._monitor_threads()` to skip auto-restart

**`MiddlewareExecutor` (per-pipeline thread pool):**
- `ThreadPoolExecutor(system.middleware_threads)` inside each Pipeline
- Runs CPU-bound work (ML, batch transforms) in parallel without blocking the main asyncio loop
- Size per job (default 2 threads per job) — configurable in job `system` config
- If 3 jobs × 2 middleware threads each = 6 OS threads for middleware, plus 3 main threads = 9 total

#### RDBMS Performance Optimizations

Three automatic optimizations work together to maximize RDBMS write throughput:

1. **Multi-row INSERT batching** — `group_insert_ops()` in `db/db_base.py` collapses consecutive same-table INSERTs into a single multi-row statement. For a document with 4 child array items, this reduces 4 round-trips to 1. All four engines have dialect-specific implementations.

2. **Async commit (`sync_commit: false`, default)** — Each engine sets a session-level option to skip waiting for durable log flush after each commit (e.g., `SET synchronous_commit = OFF` on PostgreSQL). **2-5x throughput improvement.** Safe because the pipeline's checkpoint-based recovery re-processes any lost commits. Use `sync_commit: true` for low-volume, high-value data where durability is more important than throughput (see [FAILURE_OPTION_OUTPUT_RDBMS.md §4.4](FAILURE_OPTION_OUTPUT_RDBMS.md)).

3. **Prepared statement caching (`prepared_statements: true`, default)** — For PostgreSQL, asyncpg caches prepared statements per connection (`statement_cache_size=100`), eliminating repeated parse+plan overhead for the same SQL shapes. **10-30% improvement.**

4. **Threaded schema mapping** — The CPU-bound `mapper.map_document()` call (JSONPath extraction + transforms) is offloaded to the Pipeline's `middleware_executor` ThreadPoolExecutor via `loop.run_in_executor()`. This releases the asyncio event loop so other docs can proceed with I/O (fetching, sending) while the mapper works. The executor is passed from `Pipeline` → `poll_changes()` → `BaseOutputForwarder.set_map_executor()`.

Combined, these optimizations enable throughput in the 5,000–20,000 docs/sec range for RDBMS outputs. See [`RDBMS_IMPLEMENTATION.md`](RDBMS_IMPLEMENTATION.md#multi-row-insert-batching) for details.

#### RDBMS Error Handling & Resilience

Comprehensive failure analysis and fixes are documented in [`FAILURE_OPTION_OUTPUT_RDBMS.md`](FAILURE_OPTION_OUTPUT_RDBMS.md). Key design decisions:

1. **Connect-with-retry at startup** — `poll_changes()` retries `output.connect()` with backoff up to 300s before entering the feed loop. The pipeline thread stays alive — no PipelineManager restart churn (§3.1).

2. **Pool reconnect stampede prevention** — `_reconnect_pool()` uses an `asyncio.Event` (`_pool_ready`) so only one task reconnects while others wait. Generation check (`_pool_generation`) is evaluated before acquiring the lock (§3.2).

3. **Selective pool reconnection** — A `_RECONNECT_SQLSTATES` set limits pool recreation to connection-lost errors (`08xxx`, `57P01/02`) and `42P01` (table_not_found). Pool exhaustion (`53300`) retries with the existing pool to avoid creating more connections (§3.3).

4. **Extended backoff for infrastructure failures** — Disk full (`53100`) and out-of-memory (`53200`) use a 15-min max backoff instead of 5-min, matching real-world DBA recovery times (§3.8).

5. **DLQ write failure safety net** — When `halt_on_failure: false` and a DLQ write fails, the batch processor upgrades to a halt. No doc is ever silently lost (§3.11, §4.3).

6. **`duplicate_strategy: "skip"`** — Mapping config option that generates `INSERT … ON CONFLICT DO NOTHING` instead of bare `INSERT`. Recommended for parallel mode (`sequential: false`) to safely handle re-processing (§3.12).

7. **Row-by-row fallback** — On non-transient multi-op transaction failure, `_execute_ops` falls back to row-by-row execution to isolate which specific rows failed. DLQ entries include per-row error details (§3.15).

8. **Eventing/attachment error isolation** — Unexpected exceptions from `eventing_handler.process_change()` and `attachment_processor.process()` are caught and routed to DLQ instead of crashing the pipeline. Only `EventingHalt` stops the pipeline (§2.6, §2.7).

#### Why threads, not processes?

The workload is I/O-bound (HTTP, DB writes). Python threads release the GIL during I/O. Each pipeline spends ~95-99% of time waiting for network. The `ThreadPoolExecutor` inside each pipeline handles CPU-bound middleware (ML inference) by offloading to OS threads where native C libraries (PyTorch, ONNX) release the GIL. If CPU bottleneck emerges (v3.x), swap to `multiprocessing` — same `PipelineManager` interface.

#### Implementation Checklist

**`pipeline.py` (new):**
- [ ] `Pipeline` class:
  - [ ] `__init__(job_id, job_doc, cbl_store, metrics, logger)`
  - [ ] `run()` — main thread entry point; wraps `asyncio.run(poll_changes(...))`
  - [ ] `stop()` — signal thread to shut down; save checkpoint; join with timeout
  - [ ] `is_running()` — thread alive check
  - [ ] `restart()` — stop + run
  - [ ] Exception handler → write to DLQ
- [ ] Accept resolved input/output/mapping (not raw job doc)
- [ ] Use per-pipeline logger with `job_id` in tag

**`pipeline_manager.py` (new):**
- [ ] `PipelineManager` class:
  - [ ] `__init__(cbl_store, config, metrics, logger)`
  - [ ] `start()` — load all enabled jobs; create `Pipeline` per job; start threads
  - [ ] `stop()` — signal all pipelines; graceful drain; save all checkpoints
  - [ ] `start_job(job_id)` — create + start a single job thread
  - [ ] `stop_job(job_id)` — signal + stop a single job thread
  - [ ] `restart_job(job_id)` — stop + start
  - [ ] `restart_all()` — restart every running job
  - [ ] `get_job_state(job_id)` → `{ status: "running|stopped|error|halted|starting", uptime_seconds: N, error_count: N, last_error: "...", halt_reason: "auth_failure|eventing_halt|ssl_failure|null" }`
  - [ ] `_monitor_threads()` — background task; detect crashes; restart with backoff; **skip restart** when `Pipeline.get_state()` reports `_auth_failure`, `_eventing_halt`, or `_ssl_failure` (log one-time event per halt reason)
- [ ] Thread-safe job registry (use `threading.Lock()`)
- [ ] Global `max_threads` enforcement (queue job starts if limit reached)
- [ ] Respect job `enabled` flag — skip disabled jobs at startup

**`main.py` refactor:**
- [ ] Replace the old monolithic `poll_changes()` loop with `PipelineManager.start()`
- [ ] Update startup flow:
  ```python
  config = load_config()
  validate_config(config)
  migrations.run_v1_to_v2_migrations(cbl_store)
  
  metrics_server = start_metrics_server(config.metrics.port)
  ui_server = start_ui_server(config.admin_ui.port)
  
  manager = PipelineManager(cbl_store, config, metrics, logger)
  manager.start()  # blocks until SIGINT
  
  manager.stop()
  ui_server.shutdown()
  metrics_server.shutdown()
  ```

**REST API endpoints:**
- [ ] `GET /api/jobs` — list all jobs with state
- [ ] `GET /api/jobs/{id}/state` — get job state: `{ status, uptime_seconds, error_count, last_error }`
- [ ] `POST /api/jobs/{id}/start` — start a single job
- [ ] `POST /api/jobs/{id}/stop` — stop a single job
- [ ] `POST /api/jobs/{id}/restart` — restart a single job
- [ ] `POST /api/_restart` — restart all jobs (global operation)
- [ ] `POST /api/_offline` — stop all jobs without removing them
- [ ] `POST /api/_online` — restart all jobs after `_offline`

**Logging & Metrics:**
- [ ] Update logger to include `job_id` tag in every message from Pipeline thread
- [ ] Metrics with job_id label:
  - [ ] `pipeline_uptime_seconds{job_id}`
  - [ ] `pipeline_crashes_total{job_id}`
  - [ ] `pipeline_restart_backoff_seconds{job_id}` — wait time before next restart
  - [ ] `jobs_running` — current count of running pipelines
- [ ] **Error handling metrics** (from [FAILURE_OPTION_OUTPUT_RDBMS.md](FAILURE_OPTION_OUTPUT_RDBMS.md)):
  - [ ] `docs_fetch_failed_total{job_id}` — doc fetch failures routed to DLQ (§2.1)
  - [ ] `eventing_errors_total{job_id}` — unexpected eventing handler exceptions (§2.6)
  - [ ] `attachment_errors_total{job_id}` — attachment processing failures (§2.7)
  - [ ] `mapper_no_mapping_total{job_id}` — docs skipped due to no mapping loaded (§2.8)
  - [ ] `validation_coercions_total{job_id}` — data coercions applied (§2.12, interim for v2.1)
  - [ ] `dlq_write_failures_total{job_id}` — DLQ writes that failed (§3.11)
  - [ ] `checkpoint_save_failures_total{job_id}` — checkpoint save failures (§4.1)
- [ ] Dashboard: show per-job uptime, crash count, error logs

**Graceful Shutdown:**
- [ ] Register signal handlers for `SIGINT` (Ctrl-C) and `SIGTERM` (docker stop)
- [ ] On signal:
  1. Log "shutting down..."
  2. Call `PipelineManager.stop()` (waits for all pipelines to drain)
  3. Save all checkpoints
  4. Close HTTP sessions
  5. Close DB connections
  6. Flush logs
  7. Exit code 0
- [ ] Timeout: if a pipeline doesn't shut down in 30s, force-kill thread + checkpoint is lost (will resync)

**Testing:**
- [ ] Unit test: `Pipeline` runs one job; can stop cleanly
- [ ] Unit test: `PipelineManager` starts N pipelines; state tracking works
- [ ] Unit test: job crash + auto-restart with backoff
- [ ] Unit test: graceful shutdown (all jobs drain, checkpoints saved)
- [ ] Integration test: 3 jobs concurrently; verify docs go to correct outputs
- [ ] Load test: 10 jobs; verify no GIL contention (should saturate I/O, not CPU)

### Phase 11: MIDDLE Stage Middleware & Data Quality (v2.1)

**Goal:** Add the middleware pipeline, data coercion, and async enrichment framework.

- [ ] **Middleware framework:**
  - [ ] Define `Middleware` base class with `async def process(doc, context) -> doc`
  - [ ] Add `MiddlewareChain` that runs an ordered list of middleware per doc
  - [ ] Support `phase` routing (`pre_output`, `pre_output_batch`, `post_attachment_upload`, `post_output`)
  - [ ] Wire middleware chain into `_process_changes_batch()` between schema mapping and output send
  - [ ] Add `system.middleware` config to job document schema
- [ ] **`pydantic_coerce` middleware:**
  - [ ] Auto-generate Pydantic model from RDBMS table DDL (`tables[].sql`)
  - [ ] Coerce types: str→int, int overflow→clamp, string truncate, epoch→datetime
  - [ ] `strict_fields` → reject to DLQ if invalid; `coerce_fields` → auto-fix and log
  - [ ] Write coercion entries to `data_quality` collection (replaces interim `validation_coercions_total` metric — see [FAILURE_OPTION_OUTPUT_RDBMS.md §2.12](FAILURE_OPTION_OUTPUT_RDBMS.md))
  - [ ] Add `CBLStore.add_data_quality_entry()` and `list_data_quality()`
  - [ ] Add TTL purge to CBL maintenance scheduler
  - **Note:** Interim improvements already landed: coercion logs promoted from DEBUG to INFO, `validation_coercions_total` metric added, strict validation now routes to DLQ (§2.13)
- [ ] **`timestamp_normalize` middleware:**
  - [ ] Parse any timestamp format (ISO, epoch sec/ms, custom strftime)
  - [ ] Convert to target timezone
  - [ ] Replace field value in doc
- [ ] **Data Quality UI:**
  - [ ] New "Data Quality" page in admin UI
  - [ ] Show coercions grouped by job, table, column
  - [ ] REST endpoints: `GET /api/data_quality`, `GET /api/data_quality/stats`
- [ ] **`attachment_ml` middleware (async):**
  - [ ] Run in `ThreadPoolExecutor` after attachment upload
  - [ ] Support pluggable analysis: captioning, OCR, embedding
  - [ ] Write results to `enrichments` collection
  - [ ] Optionally inject enrichment reference into doc before output
  - [ ] Add `CBLStore.add_enrichment()` and `list_enrichments()`
- [ ] **Metrics:**
  - [ ] `middleware_coercions_total{job_id, middleware}` counter
  - [ ] `middleware_enrichments_total{job_id, middleware}` counter
  - [ ] `middleware_processing_seconds{job_id, middleware}` summary
  - [ ] `middleware_errors_total{job_id, middleware}` counter

### Phase 12: Additional Middleware (v2.1+)

**Goal:** Build out the remaining middleware plugins.

- [ ] `geo_enrich` — reverse-geocode lat/lng using GeoPy + country-converter
- [ ] `pandas_batch` — batch docs into DataFrame for bulk transforms (deduplicate, normalize, derived columns)
- [ ] `custom_script` — user-defined Python/JS transform loaded from `transforms` collection
- [ ] Middleware management UI — enable/disable/reorder middleware per job in the wizard

---

## API Summary

### New REST Endpoints

#### Inputs

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/inputs_changes` | Get the inputs_changes document |
| `POST` | `/api/inputs_changes` | Save the inputs_changes document |
| `PUT` | `/api/inputs_changes/{id}` | Update one input entry |
| `DELETE` | `/api/inputs_changes/{id}` | Delete one input entry |

#### Outputs (same pattern × 3 types)

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/outputs_{type}` | Get outputs document (`type` = `rdbms`, `http`, `cloud`) |
| `POST` | `/api/outputs_{type}` | Save outputs document |
| `PUT` | `/api/outputs_{type}/{id}` | Update one output entry |
| `DELETE` | `/api/outputs_{type}/{id}` | Delete one output entry |

#### Jobs

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/jobs` | List all jobs |
| `GET` | `/api/jobs/{id}` | Get one job |
| `POST` | `/api/jobs` | Create a new job + checkpoint |
| `PUT` | `/api/jobs/{id}` | Update a job |
| `DELETE` | `/api/jobs/{id}` | Delete a job + its checkpoint |
| `POST` | `/api/jobs/{id}/start` | Start a job |
| `POST` | `/api/jobs/{id}/stop` | Stop a job |
| `POST` | `/api/jobs/{id}/restart` | Restart a job |
| `POST` | `/api/jobs/{id}/refresh-input` | Re-copy input from `inputs_changes` |
| `POST` | `/api/jobs/{id}/refresh-output` | Re-copy output from `outputs_{type}` |

#### Sessions

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/sessions` | List all sessions |
| `GET` | `/api/sessions/{id}` | Get one session |
| `POST` | `/api/sessions` | Create/save a session |
| `DELETE` | `/api/sessions/{id}` | Delete a session |

#### Data Quality & Enrichments (v2.1)

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/data_quality` | List coercion entries (filterable by `job_id`, `table`, `column`) |
| `GET` | `/api/data_quality/stats` | Aggregated stats (coercions by type, by job, by table) |
| `DELETE` | `/api/data_quality` | Clear all entries (or expired only) |
| `GET` | `/api/enrichments` | List enrichment entries (filterable by `job_id`, `source`, `status`) |
| `GET` | `/api/enrichments/{id}` | Get one enrichment with full results |
| `DELETE` | `/api/enrichments` | Clear all entries |

### Unchanged Endpoints

| Method | Path | Description |
|---|---|---|
| `GET/POST` | `/api/config` | Global infrastructure config (slimmed) |
| `GET/POST/DELETE` | `/api/dlq/*` | Dead letter queue (add `job_id` filter param) |
| `GET` | `/_metrics` | Prometheus metrics (now with job labels) |
| `POST` | `/_restart` | Restart all jobs |
| `POST` | `/_offline` / `/_online` | Pause/resume all jobs |

---

## File Changes Summary

| File | Change |
|---|---|
| `cbl_store.py` | Add 13 new collection constants; add CRUD methods for inputs_changes, outputs_*, jobs, checkpoints, sessions, data_quality, enrichments; add migration function |
| `main.py` | Refactor startup to load jobs; update `validate_config()`; update `Checkpoint` to use `checkpoints` collection; route `output_type` to correct forwarder; wire middleware chain |
| `web/templates/wizard.html` | Add inputs/outputs (4 sub-tabs)/jobs management tabs |
| `web/templates/settings.html` | Slim to infrastructure-only |
| `web/templates/index.html` | Add job selector; per-job metrics |
| `web/templates/schema.html` | Edit mapping in job context |
| `rest/__init__.py` | Add new API route handlers for inputs_changes, outputs_*, jobs, sessions, data_quality, enrichments |
| `schema/mapper.py` | Load mapping from job document |
| `middleware/` (new) | New package: `base.py` (framework), `pydantic_coerce.py`, `timestamp_normalize.py`, `geo_enrich.py`, `pandas_batch.py`, `attachment_ml.py` |
| `web/templates/data_quality.html` (new) | Data quality viewer — coercion log grouped by job/table/column |
| `config.json` | Becomes a seed file for infrastructure config only |
| `docs/CBL_DATABASE.md` | Update with new 15-collection layout |
| `docs/WIZARD.md` | Update with new wizard flow |
| `docs/JOBS.md` | Update to reflect first-class job documents |

---

## Open Questions

1. **Should catalog documents be single documents with `src[]` arrays, or one document per entry?** Current design uses single documents with `src[]` arrays for simplicity (the wizard loads/saves one document). Trade-off: if you have 50 RDBMS outputs, the document gets large. For now, single doc per type is fine — we don't expect more than ~20 entries per output type.

2. **Should jobs copy input/output data, or reference by ID?** Current design copies. This makes jobs self-contained and immune to source changes. Trade-off: if you update a DB password in `outputs_rdbms`, you have to "refresh" each job that uses it. Alternative: reference by ID and resolve at runtime. Decision: **copy** (explicit is better than implicit; the "refresh" button in the UI handles the update case).

3. **What happens to `mappings/` directory files?** Phase 9 embeds them into jobs. The directory becomes an optional import surface — drop a `.json`/`.yaml` file in there and the migration picks it up. After that, the authoritative copy lives in the job document.

4. **DLQ scoping per job?** DLQ entries include a `job_id` field so they can be filtered per job in the UI and API. The collection itself remains a single shared `dlq` — one trash can.

5. **When do `users` and `sessions` get implemented?** `sessions` is useful immediately (SG session TTL management). `users` waits for an RBAC sprint. Both collections are created on startup but unused until their respective phases.

6. **Multi-threading in v2.0 — is it too early?** No. Even with a single job, the async middleware (ML, enrichments) needs a `ThreadPoolExecutor` to avoid blocking the `_changes` feed. Multi-job threading is the same pattern scaled up. Building it in v2.0 avoids a later refactor.

7. **HA — when?** v3.0. The entire v2.0 document model is designed with HA in mind: all state in CBL, deterministic doc IDs, at-least-once delivery semantics, idempotent outputs. See [`HA.md`](HA.md) for the full design (Active/Passive via CBL replication, lease-based job ownership, split-brain mitigation).

8. **BLIP multiplexed input — when?** v2.5. The `inputs_changes` schema already supports `source_type: "blip"` with `collections[]` array. The hard part is implementing a Python BLIP client that speaks SG's WebSocket replication protocol. No public REST API for this — must reverse-engineer from `rest/blip_sync.go` and `db/blip_handler_collections.go` in the SG source, or use CBL's replicator as a reference implementation. This is a significant effort but pays off massively for multi-collection workloads (1 WebSocket vs N HTTP connections).
