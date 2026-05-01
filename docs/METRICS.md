# Prometheus Metrics Reference

Complete catalog of every metric exposed on the `/_metrics` endpoint.

All metrics use the `changes_worker_` prefix and carry two default labels:
| Label      | Example          | Description                                      |
|------------|------------------|--------------------------------------------------|
| `src`      | `sync_gateway`   | Gateway type (`sync_gateway`, `app_services`, `edge_server`, `couchdb`) |
| `database` | `travel-sample`  | Database name from `gateway.database` in config  |

---

## Metric Type Quick Reference

| Type        | Behavior                    | How to query                         |
|-------------|-----------------------------|--------------------------------------|
| **Counter** | Only goes up (resets on restart) | Use `rate()` or `increase()`        |
| **Gauge**   | Can go up or down            | Query the raw value directly         |
| **Summary** | Pre-computed quantiles (p50/p90/p99) + `_sum` / `_count` | Use `_sum / _count` for the average |
| **Info**    | Gauge always = 1, metadata in labels | Query the label value           |

---

## 1 — Process Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 1 | `changes_worker_uptime_seconds` | **gauge** | Seconds since the worker process started. Resets to 0 on restart. |

---

## 2 — Poll Loop Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 2 | `changes_worker_poll_cycles_total` | **counter** | Total `_changes` poll cycles completed (including empty batches). |
| 3 | `changes_worker_poll_errors_total` | **counter** | Total `_changes` poll errors (4xx, exhausted retries, connection failures). |
| 4 | `changes_worker_last_poll_timestamp_seconds` | **gauge** | Unix timestamp of the last successful `_changes` poll. |
| 5 | `changes_worker_last_batch_size` | **gauge** | Number of changes in the most recent batch. |

---

## 3 — Changes Feed Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 6 | `changes_worker_changes_received_total` | **counter** | Raw change count from the `_changes` feed (before filtering). |
| 7 | `changes_worker_changes_processed_total` | **counter** | Changes that passed filtering and were forwarded to the output. |
| 8 | `changes_worker_changes_filtered_total` | **counter** | Changes filtered out (deletes + removes skipped by `ignore_delete`/`ignore_remove`). |
| 9 | `changes_worker_changes_deleted_total` | **counter** | Deleted changes filtered out (subset of filtered). |
| 10 | `changes_worker_changes_removed_total` | **counter** | Removed-from-channel changes filtered out (subset of filtered). |
| 11 | `changes_worker_feed_deletes_seen_total` | **counter** | Changes with `deleted=true` seen in the feed — **always counted**, regardless of filter settings. |
| 12 | `changes_worker_feed_removes_seen_total` | **counter** | Changes with `removed=true` seen in the feed — **always counted**. |
| 13 | `changes_worker_deletes_forwarded_total` | **counter** | Tombstones (`deleted=true`) actually forwarded to the output (not filtered). |

---

## 4 — Bytes / Data Volume Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 14 | `changes_worker_bytes_received_total` | **counter** | Bytes from `_changes`, `_bulk_get`, and individual doc GETs. |
| 15 | `changes_worker_bytes_output_total` | **counter** | Bytes sent to the output. For HTTP/Cloud outputs this is the request body size; for RDBMS outputs this is the generated SQL statement + parameter size (including DELETE operations). |

---

## 5 — Doc Fetching Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 16 | `changes_worker_docs_fetched_total` | **counter** | Docs fetched via `_bulk_get` or individual GET (when `include_docs=false`). |
| 17 | `changes_worker_doc_fetch_requests_total` | **counter** | Total doc-fetch requests (one per `_bulk_get` call or per individual batch). |
| 18 | `changes_worker_doc_fetch_errors_total` | **counter** | Failed doc-fetch requests. |

---

## 6 — Output Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 19 | `changes_worker_output_requests_total` | **counter** | Total output requests (success + failure). |
| 20 | `changes_worker_output_errors_total` | **counter** | Total output errors. |
| 21 | `changes_worker_output_requests_by_method_total` | **counter** | Output requests by HTTP method. Labels: `method="PUT"`, `method="DELETE"`. |
| 22 | `changes_worker_output_errors_by_method_total` | **counter** | Output errors by HTTP method. Labels: `method="PUT"`, `method="DELETE"`. |
| 23 | `changes_worker_output_success_total` | **counter** | Output requests that succeeded (2xx). |
| 24 | `changes_worker_output_skipped_total` | **counter** | Docs skipped at output (no mapper match or empty ops). |
| 25 | `changes_worker_output_endpoint_up` | **gauge** | `1` = output endpoint reachable, `0` = down (`halt_on_failure` triggered). |

---

## 7 — Dead Letter Queue (DLQ) Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 26 | `changes_worker_dead_letter_total` | **counter** | Docs written to the dead-letter queue. |
| 27 | `changes_worker_dlq_write_failures_total` | **counter** | DLQ write failures (data potentially lost). |
| 28 | `changes_worker_dlq_pending_count` | **gauge** | Current pending entries in the DLQ. |
| 29 | `changes_worker_dlq_last_write_epoch` | **gauge** | Unix timestamp of the last DLQ write (`0` = never). |

---

## 8 — Response Time Summaries

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 30 | `changes_worker_output_response_time_seconds` | **summary** | Output HTTP response time (p50, p90, p99, `_sum`, `_count`). |
| 31 | `changes_worker_changes_request_time_seconds` | **summary** | Time to complete a `_changes` HTTP request. |
| 32 | `changes_worker_batch_processing_time_seconds` | **summary** | Time to process a full batch of changes. |
| 33 | `changes_worker_doc_fetch_time_seconds` | **summary** | Time to fetch documents (`_bulk_get` or individual). |
| 34 | `changes_worker_health_probe_time_seconds` | **summary** | Time for a health-check probe round-trip. |

---

## 9 — Checkpoint Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 35 | `changes_worker_checkpoint_saves_total` | **counter** | Successful checkpoint save operations. |
| 36 | `changes_worker_checkpoint_save_errors_total` | **counter** | Checkpoint save errors (fell back to local file). |
| 37 | `changes_worker_checkpoint_loads_total` | **counter** | Total checkpoint load operations. |
| 38 | `changes_worker_checkpoint_load_errors_total` | **counter** | Checkpoint load errors. |
| 39 | `changes_worker_checkpoint_seq` | **info/gauge** | Current checkpoint sequence value (sequence is in the `seq` label because it can be non-numeric, e.g. `"1234:56"`). |

---

## 10 — Retry Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 40 | `changes_worker_retries_total` | **counter** | Total HTTP retry attempts across all request types. |
| 41 | `changes_worker_retry_exhausted_total` | **counter** | Times all retries were exhausted (request permanently failed). |

---

## 11 — Batch Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 42 | `changes_worker_batches_total` | **counter** | Total batches processed. |
| 43 | `changes_worker_batches_failed_total` | **counter** | Batches that failed (output down). |

---

## 12 — Mapper Metrics (DB / RDBMS Mode)

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 44 | `changes_worker_mapper_matched_total` | **counter** | Docs matched by a schema mapper. |
| 45 | `changes_worker_mapper_skipped_total` | **counter** | Docs skipped (no mapper match). |
| 46 | `changes_worker_mapper_errors_total` | **counter** | Mapper errors. |
| 47 | `changes_worker_mapper_ops_total` | **counter** | SQL operations generated by mappers. |

---

## 13 — DB Transaction Resilience Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 48 | `changes_worker_db_retries_total` | **counter** | DB transaction retry attempts. |
| 49 | `changes_worker_db_retry_exhausted_total` | **counter** | Times all DB retries were exhausted. |
| 50 | `changes_worker_db_transient_errors_total` | **counter** | Transient DB errors (connection, deadlock, serialization). |
| 51 | `changes_worker_db_permanent_errors_total` | **counter** | Permanent DB errors (constraint violations, type mismatches). |
| 52 | `changes_worker_db_pool_reconnects_total` | **counter** | DB connection pool reconnections. |

---

## 14 — Stream Metrics (Continuous / WebSocket)

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 53 | `changes_worker_stream_reconnects_total` | **counter** | Stream reconnections (server disconnect / network error). |
| 54 | `changes_worker_stream_messages_total` | **counter** | Stream messages received. |
| 55 | `changes_worker_stream_parse_errors_total` | **counter** | Unparseable stream messages. |

---

## 15 — Health Check Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 56 | `changes_worker_health_probes_total` | **counter** | Health check probes sent. |
| 57 | `changes_worker_health_probe_failures_total` | **counter** | Failed health check probes. |

---

## 16 — Auth Tracking Metrics

### Inbound (Gateway / `_changes` feed)

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 58 | `changes_worker_inbound_auth_total` | **counter** | Total inbound auth attempts. |
| 59 | `changes_worker_inbound_auth_success_total` | **counter** | Inbound auth successes. |
| 60 | `changes_worker_inbound_auth_failure_total` | **counter** | Inbound auth failures (401/403). |
| 61 | `changes_worker_inbound_auth_time_seconds` | **summary** | Inbound auth request timing (p50/p90/p99). |

### Outbound (Output endpoint)

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 62 | `changes_worker_outbound_auth_total` | **counter** | Total outbound auth attempts. |
| 63 | `changes_worker_outbound_auth_success_total` | **counter** | Outbound auth successes. |
| 64 | `changes_worker_outbound_auth_failure_total` | **counter** | Outbound auth failures (401/403). |
| 65 | `changes_worker_outbound_auth_time_seconds` | **summary** | Outbound auth request timing (p50/p90/p99). |

---

## 17 — Flood / Backpressure Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 66 | `changes_worker_changes_pending` | **gauge** | Changes received but not yet processed (received − processed). Backpressure indicator. |
| 67 | `changes_worker_largest_batch_received` | **gauge** | Largest single batch since startup. |
| 68 | `changes_worker_flood_batches_total` | **counter** | Batches exceeding the flood threshold (default 10 000). |
| 69 | `changes_worker_active_tasks` | **gauge** | Currently active doc processing tasks. |
| 70 | `changes_worker_backpressure_delays_total` | **counter** | Times backpressure throttling was applied. |
| 71 | `changes_worker_backpressure_delay_seconds_total` | **counter** | Total seconds spent in backpressure delays. |
| 72 | `changes_worker_backpressure_active` | **gauge** | `1` when currently throttling, `0` otherwise. |

---

## 18 — Attachment Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 73 | `changes_worker_attachments_detected_total` | **counter** | Docs with `_attachments` seen. |
| 74 | `changes_worker_attachments_downloaded_total` | **counter** | Individual attachment downloads completed. |
| 75 | `changes_worker_attachments_download_errors_total` | **counter** | Failed downloads. |
| 76 | `changes_worker_attachments_uploaded_total` | **counter** | Attachments uploaded to destination. |
| 77 | `changes_worker_attachments_upload_errors_total` | **counter** | Failed uploads. |
| 78 | `changes_worker_attachments_bytes_downloaded_total` | **counter** | Total bytes downloaded from source. |
| 79 | `changes_worker_attachments_bytes_uploaded_total` | **counter** | Total bytes uploaded to destination. |
| 80 | `changes_worker_attachments_post_process_total` | **counter** | Post-processing operations completed. |
| 81 | `changes_worker_attachments_post_process_errors_total` | **counter** | Failed post-processing operations. |
| 82 | `changes_worker_attachments_skipped_total` | **counter** | Attachments skipped by filter rules. |
| 83 | `changes_worker_attachments_missing_total` | **counter** | Attachments listed in `_attachments` but returned 404 on fetch. |
| 84 | `changes_worker_attachments_digest_mismatch_total` | **counter** | Downloads where digest didn't match (re-downloaded). |
| 85 | `changes_worker_attachments_stale_total` | **counter** | Attachments skipped because the parent doc revision was superseded. |
| 86 | `changes_worker_attachments_post_process_skipped_total` | **counter** | Post-processing steps skipped (no matching rule). |
| 87 | `changes_worker_attachments_conflict_retries_total` | **counter** | Revision conflict retries during attachment post-processing. |
| 88 | `changes_worker_attachments_orphaned_uploads_total` | **counter** | Uploads orphaned (parent doc deleted or superseded after upload). |
| 89 | `changes_worker_attachments_partial_success_total` | **counter** | Docs where some but not all attachments succeeded. |
| 90 | `changes_worker_attachments_temp_files_cleaned_total` | **counter** | Temp attachment files cleaned from disk. |

---

## 19 — Eventing Metrics (JS Handlers)

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 91 | `changes_worker_eventing_invocations_total` | **counter** | Total handler calls (OnUpdate + OnDelete). |
| 92 | `changes_worker_eventing_updates_total` | **counter** | OnUpdate calls. |
| 93 | `changes_worker_eventing_deletes_total` | **counter** | OnDelete calls. |
| 94 | `changes_worker_eventing_passed_total` | **counter** | Documents that passed through (truthy return). |
| 95 | `changes_worker_eventing_rejected_total` | **counter** | Documents rejected by handler (falsy/void return). |
| 96 | `changes_worker_eventing_errors_total` | **counter** | JS exceptions (on_error policy applied). |
| 97 | `changes_worker_eventing_timeouts_total` | **counter** | Handler exceeded timeout_ms (on_timeout policy applied). |
| 98 | `changes_worker_eventing_halts_total` | **counter** | `on_error=halt` or `on_timeout=halt` triggered — job stopped. |
| 99 | `changes_worker_eventing_v8_heap_used_bytes` | **gauge** | V8 isolate heap used bytes (sampled every 100 invocations). |
| 100 | `changes_worker_eventing_v8_heap_total_bytes` | **gauge** | V8 isolate heap total bytes. |
| 101 | `changes_worker_eventing_handler_duration_seconds` | **summary** | Time spent in JS handler per invocation (p50, p90, p99). |

---

## 20 — Recursion Guard Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 102 | `changes_worker_recursion_guard_suppressed_total` | **counter** | Changes suppressed by the recursion guard (write-back echo detected and skipped). |

---

## 21 — System / Process Metrics

| # | Metric | Type | Description |
|---|--------|------|-------------|
| 103 | `changes_worker_process_cpu_percent` | **gauge** | Worker process CPU usage (% of one core). |
| 104 | `changes_worker_process_cpu_user_seconds_total` | **counter** | User-space CPU seconds consumed. |
| 105 | `changes_worker_process_cpu_system_seconds_total` | **counter** | Kernel-space CPU seconds consumed. |
| 106 | `changes_worker_process_memory_rss_bytes` | **gauge** | Resident Set Size (physical RAM). |
| 107 | `changes_worker_process_memory_vms_bytes` | **gauge** | Virtual Memory Size. |
| 108 | `changes_worker_process_memory_percent` | **gauge** | % of host RAM used by worker. |
| 109 | `changes_worker_process_threads` | **gauge** | OS threads in the worker process. |
| 110 | `changes_worker_process_open_fds` | **gauge** | Open file descriptors (Linux/macOS only). |
| 111 | `changes_worker_python_threads_active` | **gauge** | Active Python threads. |
| 112 | `changes_worker_python_gc_gen{0,1,2}_count` | **gauge** | Objects tracked by GC generation 0/1/2. |
| 113 | `changes_worker_python_gc_gen{0,1,2}_collections_total` | **counter** | GC collection runs per generation. |
| 114 | `changes_worker_system_cpu_count` | **gauge** | Logical CPU cores on the host. |
| 115 | `changes_worker_system_cpu_percent` | **gauge** | Host-wide CPU usage %. |
| 116 | `changes_worker_system_memory_total_bytes` | **gauge** | Total physical memory. |
| 117 | `changes_worker_system_memory_available_bytes` | **gauge** | Available physical memory. |
| 118 | `changes_worker_system_memory_used_bytes` | **gauge** | Used physical memory. |
| 119 | `changes_worker_system_memory_percent` | **gauge** | Host memory usage %. |
| 120 | `changes_worker_system_swap_total_bytes` | **gauge** | Total swap space. |
| 121 | `changes_worker_system_swap_used_bytes` | **gauge** | Used swap space. |
| 122 | `changes_worker_system_disk_total_bytes` | **gauge** | Total disk space. |
| 123 | `changes_worker_system_disk_used_bytes` | **gauge** | Used disk space. |
| 124 | `changes_worker_system_disk_free_bytes` | **gauge** | Free disk space. |
| 125 | `changes_worker_system_disk_percent` | **gauge** | Disk usage %. |
| 126 | `changes_worker_system_network_bytes_sent_total` | **counter** | Total bytes sent (all NICs). |
| 127 | `changes_worker_system_network_bytes_recv_total` | **counter** | Total bytes received (all NICs). |
| 128 | `changes_worker_system_network_packets_sent_total` | **counter** | Total packets sent. |
| 129 | `changes_worker_system_network_packets_recv_total` | **counter** | Total packets received. |
| 130 | `changes_worker_system_network_errin_total` | **counter** | Incoming network errors. |
| 131 | `changes_worker_system_network_errout_total` | **counter** | Outgoing network errors. |
| 132 | `changes_worker_log_dir_size_bytes` | **gauge** | Total size of the `logs/` directory. |
| 133 | `changes_worker_cbl_db_size_bytes` | **gauge** | Total size of the Couchbase Lite database on disk. |

---

## 22 — Per-Engine / Per-Job DB Metrics

When using RDBMS output (Postgres, MySQL, Oracle, MSSQL), the `DbMetrics` proxy emits labeled counters with `engine` and `job_id` labels alongside the global totals.

| Metric pattern | Type | Labels | Description |
|----------------|------|--------|-------------|
| `changes_worker_db_{counter_name}` | **counter** | `engine`, `job_id` | Per-engine breakdowns of every counter the DB forwarder increments (e.g. `output_requests_total`, `output_errors_total`, `db_retries_total`). |
| `changes_worker_db_output_response_time_seconds` | **summary** | `engine`, `job_id` | Per-engine output response time (p50/p90/p99). |

Example:
```
changes_worker_db_output_requests_total{engine="postgres",job_id="orders_sync"} 300
changes_worker_db_output_requests_total{engine="oracle",job_id="analytics"} 200
```

---

## 23 — Per-Provider / Per-Job Cloud Metrics

When using Cloud output (S3, etc.), the `CloudMetrics` proxy emits labeled counters with `provider` and `job_id` labels.

| Metric pattern | Type | Labels | Description |
|----------------|------|--------|-------------|
| `changes_worker_cloud_{counter_name}` | **counter** | `provider`, `job_id` | Per-provider breakdowns of every counter the cloud forwarder increments. |
| `changes_worker_cloud_output_response_time_seconds` | **summary** | `provider`, `job_id` | Per-provider output response time (p50/p90/p99). |

Example:
```
changes_worker_cloud_uploads_total{provider="s3",job_id="orders_archive"} 300
```

---

## 🧠 PRO TIPS — Combining Metrics for Deeper Insight

### 💡 Filter Drop Rate (%)

```promql
rate(changes_worker_changes_filtered_total[5m])
  / rate(changes_worker_changes_received_total[5m]) * 100
```

**Insight:** What % of incoming changes are irrelevant to your pipeline. If this is 90%, 90% of the feed is deletes/removes — consider whether your source DB is doing excessive purging or if channel assignments are churning.

---

### 💡 Output Error Rate (%)

```promql
rate(changes_worker_output_errors_total[5m])
  / rate(changes_worker_output_requests_total[5m]) * 100
```

**Insight:** Should be **0%** in a healthy system. Even 1% means your downstream endpoint is rejecting docs. Compare `output_errors_by_method_total{method="PUT"}` vs `{method="DELETE"}` to see if writes or deletes are failing — e.g. if DELETE errors are high but PUT errors are zero, your endpoint may not support DELETE operations.

---

### 💡 Average Document Size (Ingest)

```promql
rate(changes_worker_bytes_received_total[5m])
  / rate(changes_worker_changes_received_total[5m])
```

**Insight:** Average bytes per incoming change. Tells you if your docs are 500 bytes or 50 KB. Critical for capacity planning — multiply by your expected changes/sec to predict bandwidth needs.

---

### 💡 Average Payload Size (Output)

```promql
rate(changes_worker_bytes_output_total[5m])
  / rate(changes_worker_output_requests_total[5m])
```

**Insight:** Compare against the ingest doc size above. If you switched `output_format` from JSON to msgpack, this should shrink. If you switched to XML, it should grow. A ratio of `bytes_output / bytes_received` ≈ 1.0 means JSON→JSON with minimal transformation.

---

### 💡 Input-to-Output Byte Ratio

```promql
rate(changes_worker_bytes_output_total[5m])
  / rate(changes_worker_bytes_received_total[5m])
```

**Insight:** Values < 1.0 mean compression (msgpack, filtering out fields). Values > 1.0 mean expansion (XML, adding envelope metadata). A sudden change means your transformation pipeline or serialization format changed.

---

### 💡 Staleness — Seconds Since Last Successful Poll

```promql
time() - changes_worker_last_poll_timestamp_seconds
```

**Insight:** How stale your data is. If this exceeds 2–3× your `poll_interval_seconds`, the worker is stuck or the gateway is unreachable. Alert on `> 120` for a 30s poll interval.

---

### 💡 Average Batch Size Over Time

```promql
rate(changes_worker_changes_received_total[5m])
  / rate(changes_worker_poll_cycles_total[5m])
```

**Insight:** How many changes arrive per poll. Full batches (= your `throttle_feed` limit) mean there's a backlog. Empty batches (≈ 0) mean the worker is caught up. Trending upward → write load is increasing on the source.

---

### 💡 Average Time Per Poll Cycle

```promql
changes_worker_uptime_seconds
  / changes_worker_poll_cycles_total
```

**Insight:** Rough average loop cadence. Should be close to your `poll_interval_seconds` when idle. If it's much higher, processing time or output latency is dominating.

---

### 💡 Feed Delete Rate (%)

```promql
rate(changes_worker_feed_deletes_seen_total[5m])
  / rate(changes_worker_changes_received_total[5m]) * 100
```

**Insight:** True deletion activity in the source DB, regardless of your `ignore_delete` setting. If this is high and `deletes_forwarded_total` is zero, your filter is working. If both are high, your downstream is processing a lot of tombstones — you may want to batch or throttle.

---

### 💡 PUT vs DELETE Split (%)

```promql
rate(changes_worker_output_requests_by_method_total{method="DELETE"}[5m])
  / rate(changes_worker_output_requests_total[5m]) * 100
```

**Insight:** What % of your output traffic is deletions. A value of 50% means half your downstream writes are deletes — that's unusual and worth investigating (TTL expiration? mass purge?).

---

### 💡 Docs Fetched Per Processed Change

```promql
rate(changes_worker_docs_fetched_total[5m])
  / rate(changes_worker_changes_processed_total[5m])
```

**Insight:** Only relevant when `include_docs=false`. Should be ≈ 1.0. If higher, docs are being fetched redundantly (e.g. a `_bulk_get` retry fetched some docs again).

---

### 💡 Poll Error Rate (%)

```promql
rate(changes_worker_poll_errors_total[5m])
  / rate(changes_worker_poll_cycles_total[5m]) * 100
```

**Insight:** Gateway reliability. If this is > 0 consistently, the gateway is flaky. Cross-reference with `retries_total` — if retries are high but poll errors are zero, the retries are succeeding (the gateway recovers). If both are high, the gateway is in trouble.

---

### 💡 Retry Success Rate

```promql
1 - (
  rate(changes_worker_retry_exhausted_total[5m])
  / rate(changes_worker_retries_total[5m])
)
```

**Insight:** What % of retries eventually succeed. A value of 1.0 (100%) means every transient error recovered. If `retry_exhausted_total` is climbing, your `max_retries` may be too low or the target is truly down.

---

### 💡 DLQ Growth Rate

```promql
rate(changes_worker_dead_letter_total[5m])
```

**Insight:** How fast the dead-letter queue is growing. Any sustained rate > 0 means docs are failing to process. Combine with `dlq_pending_count` to see the current backlog, and `dlq_last_write_epoch` to see how recently it grew.

---

### 💡 Attachment Download Success Rate (%)

```promql
rate(changes_worker_attachments_downloaded_total[5m])
  / (rate(changes_worker_attachments_downloaded_total[5m])
     + rate(changes_worker_attachments_download_errors_total[5m])) * 100
```

**Insight:** How reliable attachment downloads are. If below 100%, check `attachments_missing_total` (404s — the attachment was deleted) vs `attachments_download_errors_total` (network/timeout errors).

---

### 💡 Attachment Pipeline Efficiency

```promql
rate(changes_worker_attachments_uploaded_total[5m])
  / rate(changes_worker_attachments_detected_total[5m])
```

**Insight:** What fraction of detected attachments make it through download → upload. Values < 1.0 mean attachments are being lost to filters, errors, or stale revisions. Check `attachments_skipped_total`, `attachments_stale_total`, and `attachments_orphaned_uploads_total` to find where.

---

### 💡 DB Transient vs Permanent Error Ratio

```promql
rate(changes_worker_db_transient_errors_total[5m])
  / (rate(changes_worker_db_transient_errors_total[5m])
     + rate(changes_worker_db_permanent_errors_total[5m]))
```

**Insight:** If most errors are transient (deadlocks, connection drops), the system will self-heal via retries. If most are permanent (constraint violations, type mismatches), your schema mappings need fixing.

---

### 💡 Inbound vs Outbound Auth Failure Rate

```promql
# Inbound (gateway) auth failure rate
rate(changes_worker_inbound_auth_failure_total[5m])
  / rate(changes_worker_inbound_auth_total[5m]) * 100

# Outbound (output) auth failure rate
rate(changes_worker_outbound_auth_failure_total[5m])
  / rate(changes_worker_outbound_auth_total[5m]) * 100
```

**Insight:** Pinpoints which side has auth issues. If inbound auth failures are climbing, your gateway credentials (basic/session/bearer) are expiring. If outbound auth failures are climbing, your output endpoint's credentials need refreshing.

---

### 💡 Backpressure Impact — Wasted Time

```promql
changes_worker_backpressure_delay_seconds_total
  / changes_worker_uptime_seconds * 100
```

**Insight:** What % of the worker's lifetime has been spent throttled by backpressure. If this exceeds 10%, your output endpoint is too slow and is bottlenecking the entire pipeline.

---

### 💡 Process Memory Growth (Leak Detection)

```promql
deriv(changes_worker_process_memory_rss_bytes[1h])
```

**Insight:** If RSS memory is growing linearly over hours, you likely have a memory leak. Combine with `python_gc_gen2_count` — if gen2 objects are growing too, objects are surviving garbage collection.

---

## Source Files

| File | Role |
|------|------|
| `main.py` → `MetricsCollector` class | All global metrics: counters, gauges, summaries, system/process metrics |
| `main.py` → `_metrics_handler()` | HTTP handler for `GET /_metrics` |
| `db/db_base.py` → `DbMetrics` class | Per-engine/per-job RDBMS metric proxy + `render_all()` |
| `cloud/cloud_base.py` → `CloudMetrics` class | Per-provider/per-job cloud metric proxy + `render_all()` |
| `rest/changes_http.py` → `RetryableHTTP` | Increments `retries_total`, `retry_exhausted_total`, auth metrics |
| `rest/attachment_postprocess.py` | Increments attachment conflict/stale/orphaned/skipped metrics |
