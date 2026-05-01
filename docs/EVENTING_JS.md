# Eventing JS — Handler Examples & Reference

This document provides practical JavaScript examples for writing `OnUpdate` and `OnDelete` eventing handlers. For architecture and design details, see [DESIGN_EVENTING.md](DESIGN_EVENTING.md).

---

## Table of Contents

- [Handler Signatures](#handler-signatures)
- [Return Values — What They Mean](#return-values--what-they-mean)
- [Pass-Through (Default Behavior)](#pass-through-default-behavior)
- [Rejection & Filtering](#rejection--filtering)
- [Modifying Documents](#modifying-documents)
- [Delete Handling](#delete-handling)
- [Using Constants](#using-constants)
- [Utility Functions](#utility-functions)
- [Conditional Logic Patterns](#conditional-logic-patterns)
- [Limits & Restrictions](#limits--restrictions)
- [Error & Timeout Policies](#error--timeout-policies)

---

## Handler Signatures

Every eventing handler receives documents **split** into two objects:

| Parameter | Contents |
|---|---|
| `doc` | Document body — everything **except** `_id` and `_rev` |
| `meta` | Document identity — `{"_id": "...", "_rev": "..."}` only |

```javascript
function OnUpdate(doc, meta) {
    // doc  = {"type": "hotel", "name": "Test Hotel", "rating": 4.5}
    // meta = {"_id": "hotel::123", "_rev": "1-abc"}
}

function OnDelete(meta) {
    // meta = {"_id": "hotel::123", "_rev": "2-def"}
    // No doc body — the document was deleted
}
```

---

## Return Values — What They Mean

**Rule of thumb:** truthy return = pass, falsy/void return = reject.

### OnUpdate

| Return | Effect |
|---|---|
| `return doc;` | Pass the (possibly modified) document to the Schema Mapper |
| `return true;` | Pass the original document as-is |
| `return false;` | **Reject** — document stops here, does not reach the Schema Mapper |
| `return;` | **Reject** — `undefined` is falsy |
| *(no return)* | **Reject** — implicit `undefined` |

### OnDelete

| Return | Effect |
|---|---|
| `return meta;` | Pass the delete event to the Schema Mapper |
| `return true;` | Pass the delete event as-is |
| `return false;` | **Reject** — delete is suppressed |
| `return;` | **Reject** — delete is suppressed |
| *(no return)* | **Reject** — delete is suppressed |

> **Why no-return = reject?** It's safer to default to rejection. If you forget a `return`, the document silently stops — easier to debug ("where are my docs?" → check the handler) than documents leaking through unprocessed.

---

## Pass-Through (Default Behavior)

The simplest handler — pass everything unchanged:

```javascript
function OnUpdate(doc, meta) {
    return doc;
}

function OnDelete(meta) {
    return meta;
}
```

Or with `true` (same effect, original doc/meta forwarded):

```javascript
function OnUpdate(doc, meta) {
    return true;
}

function OnDelete(meta) {
    return true;
}
```

---

## Rejection & Filtering

### Reject all documents (nothing passes)

```javascript
function OnUpdate(doc, meta) {
    return false;
}
```

### Filter by document type

```javascript
function OnUpdate(doc, meta) {
    // Only allow "hotel" documents through
    if (doc.type === "hotel") {
        return doc;
    }
    log("rejected non-hotel doc:", meta._id, "type=" + doc.type);
    return false;
}
```

### Filter by document ID prefix

```javascript
function OnUpdate(doc, meta) {
    // Only process documents with "hotel::" prefix
    if (meta._id.startsWith("hotel::")) {
        return doc;
    }
    return false;
}
```

### Filter by field value

```javascript
function OnUpdate(doc, meta) {
    // Reject documents with rating below 3
    if (doc.rating && doc.rating >= 3) {
        return doc;
    }
    log("filtered low-rated doc:", meta._id, "rating=" + doc.rating);
    return false;
}
```

### Filter by multiple conditions

```javascript
function OnUpdate(doc, meta) {
    // Must be a hotel, have a name, and be in the US
    if (doc.type === "hotel" && doc.name && doc.country === "US") {
        return doc;
    }
    return false;
}
```

### Reject documents missing required fields

```javascript
function OnUpdate(doc, meta) {
    var required = ["type", "name", "email"];
    for (var i = 0; i < required.length; i++) {
        if (doc[required[i]] === undefined || doc[required[i]] === null) {
            log("missing required field:", required[i], "in", meta._id);
            return false;
        }
    }
    return doc;
}
```

---

## Modifying Documents

### Add a timestamp

```javascript
function OnUpdate(doc, meta) {
    doc.processed_at = Date.now();
    doc.processed_date = new Date().toISOString();
    return doc;
}
```

### Remove sensitive fields

```javascript
function OnUpdate(doc, meta) {
    delete doc.password;
    delete doc.ssn;
    delete doc.credit_card;
    return doc;
}
```

### Rename fields

```javascript
function OnUpdate(doc, meta) {
    if (doc.old_field_name !== undefined) {
        doc.new_field_name = doc.old_field_name;
        delete doc.old_field_name;
    }
    return doc;
}
```

### Add computed fields

```javascript
function OnUpdate(doc, meta) {
    // Full name from parts
    if (doc.first_name && doc.last_name) {
        doc.full_name = doc.first_name + " " + doc.last_name;
    }

    // Derive a category from price
    if (doc.price !== undefined) {
        if (doc.price > 200) doc.tier = "premium";
        else if (doc.price > 100) doc.tier = "standard";
        else doc.tier = "budget";
    }

    return doc;
}
```

### Flatten nested objects

```javascript
function OnUpdate(doc, meta) {
    // Flatten address into top-level fields
    if (doc.address && typeof doc.address === "object") {
        doc.address_street = doc.address.street || "";
        doc.address_city = doc.address.city || "";
        doc.address_state = doc.address.state || "";
        doc.address_zip = doc.address.zip || "";
        delete doc.address;
    }
    return doc;
}
```

### Set default values

```javascript
function OnUpdate(doc, meta) {
    if (doc.status === undefined) doc.status = "active";
    if (doc.priority === undefined) doc.priority = 0;
    if (doc.tags === undefined) doc.tags = [];
    return doc;
}
```

---

## Delete Handling

### Forward all deletes

```javascript
function OnDelete(meta) {
    log("delete:", meta._id);
    return meta;
}
```

### Suppress all deletes

```javascript
function OnDelete(meta) {
    log("suppressed delete for", meta._id);
    return false;
}
```

### Selective delete forwarding

```javascript
function OnDelete(meta) {
    // Only forward deletes for hotel documents
    if (meta._id.startsWith("hotel::")) {
        return meta;
    }
    log("suppressing delete for non-hotel:", meta._id);
    return false;
}
```

### Delete with logging

```javascript
function OnDelete(meta) {
    log("DELETE event:", meta._id, "rev=" + meta._rev);
    return meta;
}
```

---

## Using Constants

Constants are key-value pairs defined in the job's eventing settings. They are injected as global JS variables — available in both `OnUpdate` and `OnDelete`.

### Configuration example

```jsonc
"constants": [
  {"key": "target_region", "value": "us-east-1"},
  {"key": "min_rating", "value": 3.0},
  {"key": "max_retries", "value": 5},
  {"key": "enable_enrichment", "value": true},
  {"key": "pi", "value": 3.14159}
]
```

### Using constants in handlers

```javascript
function OnUpdate(doc, meta) {
    // Constants are available as globals
    doc.region = target_region;       // "us-east-1"

    if (doc.rating < min_rating) {    // 3.0
        log("below minimum rating:", doc.rating, "<", min_rating);
        return false;
    }

    if (enable_enrichment) {          // true
        doc.area = doc.radius * doc.radius * pi;
    }

    return doc;
}
```

### Constant key rules

Keys **must** be valid JavaScript identifiers: `^[A-Za-z_$][A-Za-z0-9_$]*$`

| Key | Valid? |
|---|---|
| `region` | ✅ |
| `max_retries` | ✅ |
| `$count` | ✅ |
| `_private` | ✅ |
| `my-key` | ❌ (hyphens not allowed) |
| `123abc` | ❌ (starts with number) |
| `my key` | ❌ (spaces not allowed) |

Invalid keys are **skipped** with a warning — they do not cause the handler to fail.

---

## Utility Functions

### Generate a UUID

```javascript
// Source - https://stackoverflow.com/a/873856
// Posted by Kevin Hakanson, modified by community. See post 'Timeline' for change history
// Retrieved 2026-04-23, License - CC BY-SA 3.0

function createUUID() {
    // http://www.ietf.org/rfc/rfc4122.txt
    var s = [];
    var hexDigits = "0123456789abcdef";
    for (var i = 0; i < 36; i++) {
        s[i] = hexDigits.substr(Math.floor(Math.random() * 0x10), 1);
    }
    s[14] = "4";  // bits 12-15 of the time_hi_and_version field to 0010
    s[19] = hexDigits.substr((s[19] & 0x3) | 0x8, 1);  // bits 6-7 of the clock_seq_hi_and_reserved to 01
    s[8] = s[13] = s[18] = s[23] = "-";

    var uuid = s.join("");
    return uuid;
}

function OnUpdate(doc, meta) {
    // Assign a unique processing ID
    doc.processing_id = createUUID();
    doc.processed_at = Date.now();
    return doc;
}
```

### Slug generator

```javascript
function slugify(text) {
    return text.toString().toLowerCase()
        .replace(/\s+/g, '-')
        .replace(/[^a-z0-9\-]/g, '')
        .replace(/\-\-+/g, '-')
        .replace(/^-+/, '')
        .replace(/-+$/, '');
}

function OnUpdate(doc, meta) {
    if (doc.name) {
        doc.slug = slugify(doc.name);
    }
    return doc;
}
```

### Hash function (simple)

```javascript
function simpleHash(str) {
    var hash = 0;
    for (var i = 0; i < str.length; i++) {
        var char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash).toString(16);
}

function OnUpdate(doc, meta) {
    doc.content_hash = simpleHash(JSON.stringify(doc));
    return doc;
}
```

---

## Conditional Logic Patterns

### Route documents by type

```javascript
function OnUpdate(doc, meta) {
    switch (doc.type) {
        case "hotel":
            doc.category = "accommodation";
            doc.searchable = true;
            return doc;

        case "airline":
            doc.category = "transport";
            return doc;

        case "route":
            // Enrich route docs
            if (doc.distance) {
                doc.distance_km = doc.distance * 1.60934;
            }
            return doc;

        case "_internal":
        case "_system":
            // Reject internal documents
            return false;

        default:
            // Pass unknown types unchanged
            return doc;
    }
}
```

### Deduplicate by tracking seen IDs (persistent state)

```javascript
// Global state persists across invocations within the same job
var seenIds = {};

function OnUpdate(doc, meta) {
    if (doc.external_id) {
        if (seenIds[doc.external_id]) {
            log("duplicate external_id:", doc.external_id, "in", meta._id);
            return false;
        }
        seenIds[doc.external_id] = true;
    }
    return doc;
}
```

> **Note:** Global variables persist for the lifetime of the worker process. They reset when the job restarts. Do not use for durable state — use constants or external storage.

### Counters (persistent state)

```javascript
var processedCount = 0;
var rejectedCount = 0;

function OnUpdate(doc, meta) {
    processedCount++;

    if (!doc.type) {
        rejectedCount++;
        if (rejectedCount % 100 === 0) {
            log("rejected count:", rejectedCount, "of", processedCount);
        }
        return false;
    }

    if (processedCount % 1000 === 0) {
        log("processed:", processedCount, "rejected:", rejectedCount);
    }

    return doc;
}
```

---

## Limits & Restrictions

### Handler code size

There is no hard limit on the handler source code string length, but keep handlers focused and concise. Very large handlers (>100 KB of JS source) may cause slower V8 initialization.

### Timeout

Each handler invocation has a timeout (default: **5000 ms**, configurable 100–60000 ms in settings).

If a handler exceeds the timeout, V8 execution is terminated. The configured `on_timeout` policy determines what happens to the document:

```javascript
// ❌ This will timeout — infinite loop
function OnUpdate(doc, meta) {
    while (true) {
        // never returns
    }
}

// ✅ Keep handlers fast — simple transforms only
function OnUpdate(doc, meta) {
    doc.processed = true;
    return doc;
}
```

### Memory limit

Each V8 isolate is capped at **128 MB** of heap memory. Exceeding this kills the handler call.

```javascript
// ❌ This will hit the memory limit
function OnUpdate(doc, meta) {
    var arr = [];
    while (true) {
        arr.push(new Array(10000));
    }
}

// ✅ Avoid accumulating large in-memory structures
function OnUpdate(doc, meta) {
    doc.summary = doc.description ? doc.description.substring(0, 200) : "";
    return doc;
}
```

### No external access

The V8 sandbox has **no** access to:
- Network (`fetch`, `XMLHttpRequest`, `curl`) — ❌
- File system (`fs`, `require`) — ❌
- Python internals — ❌
- Other databases — ❌
- Timers (`setTimeout`, `setInterval`) — ❌

The handler is a **pure transform/filter** — it can modify or reject documents but cannot perform side effects.

### Available built-ins

| Function | Description |
|---|---|
| `log(msg, ...)` | Log a message (forwarded to Python `logger.info`) |
| `JSON.parse()` / `JSON.stringify()` | Standard JSON operations |
| `Date`, `Math`, `String`, `Array`, `Object`, `RegExp` | Standard ECMAScript built-ins |

---

## Error & Timeout Policies

When a handler throws an error or times out, the **policy** setting controls what happens:

| Policy | Behavior |
|---|---|
| `"reject"` (default) | Document is dropped. Error is logged. Processing continues. |
| `"pass"` | Document is forwarded to the Schema Mapper as-is (unmodified). Error is logged. |
| `"halt"` | The **job is stopped**. Checkpoint does not advance. Use when correctness is critical. |

### Example: handler that might throw

```javascript
function OnUpdate(doc, meta) {
    // If doc.data is not valid JSON, this throws
    var parsed = JSON.parse(doc.data);
    doc.parsed_data = parsed;
    return doc;
}
```

With `on_error = "reject"`: documents with invalid `doc.data` are silently dropped.
With `on_error = "pass"`: documents with invalid `doc.data` pass through unchanged.
With `on_error = "halt"`: the job stops on the first invalid document.

### Defensive coding to avoid errors

```javascript
function OnUpdate(doc, meta) {
    // Safely parse JSON with fallback
    if (doc.data && typeof doc.data === "string") {
        try {
            doc.parsed_data = JSON.parse(doc.data);
        } catch (e) {
            log("failed to parse data for", meta._id, e.message);
            doc.parse_error = e.message;
        }
    }
    return doc;
}
```

---

## Full Example: Hotel Document Pipeline

A complete handler that filters, enriches, and cleans hotel documents:

```javascript
// Constants (defined in settings):
//   min_rating   = 2.0
//   target_region = "us-east-1"

// Source - https://stackoverflow.com/a/873856
// Posted by Kevin Hakanson, modified by community. See post 'Timeline' for change history
// Retrieved 2026-04-23, License - CC BY-SA 3.0

function createUUID() {
    var s = [];
    var hexDigits = "0123456789abcdef";
    for (var i = 0; i < 36; i++) {
        s[i] = hexDigits.substr(Math.floor(Math.random() * 0x10), 1);
    }
    s[14] = "4";
    s[19] = hexDigits.substr((s[19] & 0x3) | 0x8, 1);
    s[8] = s[13] = s[18] = s[23] = "-";
    return s.join("");
}

function OnUpdate(doc, meta) {
    // 1. Filter: only hotel docs
    if (doc.type !== "hotel") {
        return false;
    }

    // 2. Filter: minimum rating
    if (doc.rating !== undefined && doc.rating < min_rating) {
        log("rejected low-rated hotel:", meta._id, "rating=" + doc.rating);
        return false;
    }

    // 3. Remove sensitive fields
    delete doc.internal_notes;
    delete doc.cost_center;

    // 4. Enrich
    doc.processing_id = createUUID();
    doc.processed_at = new Date().toISOString();
    doc.region = target_region;

    // 5. Flatten address
    if (doc.address && typeof doc.address === "object") {
        doc.address_line1 = doc.address.street || "";
        doc.address_city = doc.address.city || "";
        doc.address_state = doc.address.state || "";
        doc.address_zip = doc.address.zip || "";
        delete doc.address;
    }

    // 6. Default values
    if (doc.currency === undefined) doc.currency = "USD";
    if (doc.active === undefined) doc.active = true;

    log("passed hotel:", meta._id);
    return doc;
}

function OnDelete(meta) {
    // Only forward deletes for hotel docs
    if (meta._id.startsWith("hotel::")) {
        log("forwarding hotel delete:", meta._id);
        return meta;
    }
    return false;
}
```
