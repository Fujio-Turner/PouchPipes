# Logging Guide

> **⚠️ This document has been superseded by [`guides/GUIDE_LOGGING.md`](../guides/GUIDE_LOGGING.md).**
>
> The authoritative logging standards, log line format, log keys, structured
> fields, anti-patterns, and Web UI integration guide all live in the
> consolidated guide.  This file is kept as a redirect.

## Quick Links

| Topic | Location |
|-------|----------|
| `log_event()` usage & signature | [GUIDE_LOGGING.md § The `log_event()` Function](../guides/GUIDE_LOGGING.md#the-log_event-function) |
| Log line anatomy & format | [GUIDE_LOGGING.md § Log Line Anatomy](../guides/GUIDE_LOGGING.md#log-line-anatomy) |
| Log keys reference (CHANGES, OUTPUT, CONTROL, …) | [GUIDE_LOGGING.md § Quick Reference Card](../guides/GUIDE_LOGGING.md#quick-reference-card) |
| Structured fields (`job_id`, `seq`, `error_detail`, …) | [GUIDE_LOGGING.md § Structured Fields Reference](../guides/GUIDE_LOGGING.md#structured-fields-reference) |
| Log levels & the INFO=summaries rule | [GUIDE_LOGGING.md § Log Levels](../guides/GUIDE_LOGGING.md#log-levels-what-goes-where) |
| Job tag, session ID, batch ID | [GUIDE_LOGGING.md § Job Tag](../guides/GUIDE_LOGGING.md#job-tag) / [Session ID](../guides/GUIDE_LOGGING.md#session-id) |
| `ic()` developer traces | [GUIDE_LOGGING.md § Adding New Inline Stages](../guides/GUIDE_LOGGING.md) |
| Sensitive data redaction | [GUIDE_LOGGING.md § Job Config Dump at Startup](../guides/GUIDE_LOGGING.md#job-config-dump-at-startup) |
| Per-level file tiers (`_info`, `_debug`, `_error`) | [GUIDE_LOGGING.md § Version History](../guides/GUIDE_LOGGING.md#version-history) (v1.3) |
| Web UI log parsing & change impact | [GUIDE_LOGGING.md § Web UI Log Processing](../guides/GUIDE_LOGGING.md#web-ui-log-processing-serverpy--logshtml) |
| Anti-patterns & PR checklist | [GUIDE_LOGGING.md § Anti-Patterns](../guides/GUIDE_LOGGING.md#anti-patterns) / [Checklist](../guides/GUIDE_LOGGING.md#checklist-for-new-code) |
| Configuration (`config.json` logging section) | [GUIDE_LOGGING.md § Filtering by Log Key](../guides/GUIDE_LOGGING.md#filtering-by-log-key) |

## What Changed (v1 → v2)

| Area | Old (`docs/LOGGING.md`) | Current (`guides/GUIDE_LOGGING.md`) |
|------|------------------------|-------------------------------------|
| Log format | `[LEVEL] LOGGER: message [KEY] key=value` | `[LEVEL] [KEY] job=.. #s:.. #b:.. LOGGER: message \| key value` |
| Log keys | 10 keys (no CONTROL, SHUTDOWN, EVENTING, FLOOD, ATTACHMENT) | 17 keys including CONTROL, SHUTDOWN, EVENTING, FLOOD, ATTACHMENT |
| Tracing | No job/session/batch context | `job=..xxxxx`, `#s:..session`, `#b:batchid` auto-injected |
| File output | Single `changes_worker.log` | Per-level tiers: `_info.log`, `_debug.log`, `_error.log`, `_trace.log` |
| Fields | `key=value` whitespace-delimited | `\| key value` pipe-delimited |
| Message format | f-strings allowed | `%`-formatting required |
| Admin API logging | Not covered | All `/_*` and `/api/*` endpoints log at INFO with `[CONTROL]` |
