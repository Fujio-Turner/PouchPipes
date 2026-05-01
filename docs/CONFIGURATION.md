# Configuration Reference

All settings live in a single `config.json` file. Here is a complete reference with defaults:

```jsonc
{
  "gateway": {
    "src": "sync_gateway",           // "sync_gateway" | "app_services" | "edge_server" | "couchdb"
    "url": "http://localhost:4984",   // Base URL of the gateway
    "database": "db",                 // Database name
    "scope": "us",                    // Scope (optional — omit for default scope)
    "collection": "prices",           // Collection (optional — omit for default collection)
    "accept_self_signed_certs": false // Set true for dev/test with self-signed TLS
  },

  "auth": {
    "method": "basic",               // "basic" | "session" | "bearer" | "none"
    "username": "bob",               // For method=basic
    "password": "password",          // For method=basic
    "session_cookie": "",            // For method=session (SyncGatewaySession cookie)
    "bearer_token": ""               // For method=bearer (SG / App Services only)
  },

  "changes_feed": {
    "feed_type": "longpoll",         // "longpoll" | "continuous" | "websocket" (SG/App Services) | "normal" | "sse" (Edge) | "eventsource" (CouchDB)
    "poll_interval_seconds": 10,     // Seconds to wait between longpoll cycles
    "active_only": true,             // Exclude deleted/revoked docs from the feed
    "include_docs": true,            // Inline doc bodies; false = bulk_get after
    "since": "0",                    // Starting sequence ("0" = use checkpoint)
    "channels": [],                  // Channel filter, e.g. ["channel-a", "channel-b"]
    "limit": 0,                      // Per-request limit (0 = no limit)
    "heartbeat_ms": 30000,           // Heartbeat interval to keep connection alive
    "timeout_ms": 60000,             // SG-side longpoll timeout
    "http_timeout_seconds": 300,     // Client-side HTTP timeout (for large since=0 catch-ups)
    "throttle_feed": 0,              // Eat the feed N docs at a time (0 = no throttle)
    "continuous_catchup_limit": 10000 // Batch size for continuous mode catch-up phase
  },

  "processing": {
    "ignore_delete": false,          // Skip deleted docs in the feed
    "ignore_remove": false,          // Skip removed-from-channel docs
    "sequential": false,             // true = process one doc at a time (strict order)
    "max_concurrent": 20,            // Semaphore limit for parallel processing
    "dry_run": false,                // Log what would happen without sending
    "get_batch_number": 100          // Batch size for bulk_get / individual doc fetches
  },

  "checkpoint": {
    "enabled": true,                 // Persist last_seq between runs
    "client_id": "changes_worker",   // Used in CBL-style checkpoint key derivation
    "file": "checkpoint.json",        // Local fallback file if SG is unreachable
    "every_n_docs": 0                // Save checkpoint every N docs within a batch (0 = per-batch)
  },

  "output": {
    "mode": "http",                  // "http" | "db" | "s3"
    "target_url": "",                // HTTP endpoint for mode=http
    "target_auth": {                 // Auth for the output endpoint
      "method": "none",              // "basic" | "session" | "bearer" | "none"
      "username": "",
      "password": "",
      "session_cookie": "",
      "bearer_token": ""
    },
    "retry": {                       // Output-specific retry (separate from gateway)
     "max_retries": 3,
     "backoff_base_seconds": 1,
     "backoff_max_seconds": 30,
     "retry_on_status": [500, 502, 503, 504, 507]
    },
    "halt_on_failure": true,         // Stop & freeze checkpoint if output fails
    "log_response_times": true,      // Track min/max/avg response times per batch
    "output_format": "json",         // "json"|"xml"|"form"|"msgpack"|"cbor"|"bson"|"yaml"
    "dead_letter_path": "failed_docs.jsonl", // JSONL file for docs that failed output delivery
    "request_options": {             // Extra options added to every output HTTP request
      "params": {},                  // Query-string parameters (e.g. {"batch":"ok"})
      "headers": {}                  // Custom headers (e.g. {"X-Source":"changes-worker"})
    }
  },

  "s3": {
    "bucket": "",
    "region": "us-east-1",
    "key_prefix": "",
    "key_template": "{prefix}/{doc_id}.json",
    "content_type": "application/json",
    "storage_class": "STANDARD",
    "server_side_encryption": "",
    "endpoint_url": "",
    "on_delete": "delete_object",
    "batch": {
      "enabled": false,
      "max_docs": 100,
      "max_bytes": 5242880,
      "max_seconds": 30
    },
    "max_retries": 5,
    "backoff_base_seconds": 1,
    "backoff_max_seconds": 60
  },

  "retry": {                         // Gateway-side retry (for _changes, bulk_get, etc.)
    "max_retries": 5,
    "backoff_base_seconds": 1,
    "backoff_max_seconds": 60,
    "retry_on_status": [500, 502, 503, 504, 507]
  },

  "attachments": {
    "enabled": false,                // false = Data Only mode, true = Attachments + Data mode
    "mode": "individual",            // "individual" | "bulk" | "stream"
    "dry_run": false,                // Detect attachments without downloading/uploading
    "halt_on_failure": true,         // Stop on attachment operation failure
    "destination": {
      "type": "s3",                  // "s3" | "http" | "filesystem"
      "key_template": "{prefix}/{doc_id}/{attachment_name}",
      "key_prefix": "attachments"
    }
  },

  "metrics": {
    "enabled": false,                // Enable Prometheus /_metrics endpoint
    "host": "0.0.0.0",              // Bind address
    "port": 9090                     // Port for the metrics HTTP server
  },

  "logging": {
    "level": "DEBUG"                 // "DEBUG" | "INFO" | "WARNING" | "ERROR"
  }
}
```

---

## v2.0 Job-Based Configuration

In v2.0 the configuration model changed significantly. Per-job settings have moved out of the global `config.json` and into **job documents** stored in Couchbase Lite.

### What stays in `config.json`

The global config file now contains **infrastructure-only** settings:

| Key               | Purpose                                    |
|-------------------|--------------------------------------------|
| `couchbase_lite`  | Local CBL database path and options        |
| `logging`         | Log level (`DEBUG`, `INFO`, …)             |
| `admin_ui`        | Admin web-UI host / port                   |
| `metrics`         | Prometheus `/_metrics` endpoint settings   |
| `shutdown`        | Graceful-shutdown behaviour                |

Settings such as `gateway`, `auth`, `changes_feed`, and `output` are **no longer** placed in `config.json`. They are defined inside individual job documents.

### Output entry schema (`outputs_rdbms` / embedded in jobs)

Each output entry must include:

| Field      | Required | Notes |
|------------|----------|-------|
| `mode`     | **yes**  | Output engine name: `"postgres"`, `"mysql"`, `"mssql"`, `"oracle"`, etc. This field is required. |
| `username` | preferred | Use `username` instead of `user`. An empty string falls through to engine defaults. |
| all connection fields | — | Place `host`, `port`, `database`, `password`, etc. at the **top level** of the entry — not nested under `db` or engine-specific keys. |

Example:

```jsonc
{
  "mode": "postgres",
  "host": "db.example.com",
  "port": 5432,
  "database": "analytics",
  "username": "etl_user",
  "password": "secret"
}
```

### Input entry schema (`inputs_changes`)

Each input entry must include **both** names for every field the pipeline reads:

| Canonical field | Alias field   | The pipeline reads |
|-----------------|---------------|--------------------|
| `host`          | `url`         | `url`              |
| `source_type`   | `src`         | `src`              |

Both must be present because the admin UI writes one name while the pipeline reads the other. There is no automatic translation.

### No field-name translation in the pipeline

The pipeline's `_build_job_config()` passes `inputs[0]` and `outputs[0]` **directly** to the processing code without renaming any fields. Source documents must therefore use the **exact field names** the pipeline expects.
