# Data Validation & Coercion

## Overview

The Data Validation feature automatically validates and transforms incoming change documents against your RDBMS table schemas. When enabled, it:

1. **Extracts column definitions** from your schema mappings (table names + column types)
2. **Validates incoming data** against the defined schema
3. **Coerces values** to match expected types (e.g., string "42" → int 42)
4. **Tracks transformations** for audit and debugging (original vs. new values)
5. **Logs errors** when data cannot be coerced and optionally routes to DLQ

This is a **quick-win** for data quality without needing to add ML/enrichment pipelines.

## When to Use

Use data validation when:
- You want to **automatically fix type mismatches** (e.g., numeric strings → numbers)
- You need **audit trails** of what data was transformed
- Your **source system is unreliable** or sends inconsistent types
- You want to **prevent silent data corruption** from wrong types in the DB

## Configuration

Data validation is configured per RDBMS output in the Settings UI under **"Auto-Validate Against Table Schema"**.

### Basic Settings

| Setting | Description | Default |
|---------|-------------|---------|
| **Enabled** | Turn on automatic validation | `false` |
| **Strict Mode** | Reject documents with unknown columns not in schema | `false` |
| **Track Original Values** | Log original values when coerced (debug logs) | `true` |
| **Send invalid docs to DLQ** | Route docs with errors to Dead Letter Queue | `true` |

### In Config File

```json
{
  "output": {
    "rdbms": {
      "db": {
        "engine": "postgres",
        "host": "localhost",
        "database": "mydb",
        "schema_mappings": {
          "enabled": true,
          "path": "mappings/"
        },
        "validation": {
          "enabled": true,
          "strict": false,
          "track_originals": true,
          "dlq_on_error": true
        }
      }
    }
  }
}
```

## How It Works

### 1. Schema Extraction

Validation reads your schema mappings to understand table structures:

```json
{
  "tables": [
    {
      "table_name": "orders",
      "columns": {
        "order_id": "INT",
        "amount": "DECIMAL(10,2)",
        "status": "VARCHAR(50)",
        "created_at": "DATETIME",
        "flags": "BOOLEAN"
      }
    }
  ]
}
```

### 2. Type Coercion

When a document arrives, values are coerced to match column types:

| Incoming | Type | Coerced | Original |
|----------|------|---------|----------|
| `"42"` | `INT` | `42` | `"42"` |
| `123` | `VARCHAR(10)` | `"123"` | `123` |
| `"99.999"` | `DECIMAL(10,2)` | `100.00` | `"99.999"` |
| `"true"` | `BOOLEAN` | `true` | `"true"` |
| `"2024-01-15"` | `DATE` | `2024-01-15` | `"2024-01-15"` |
| `null` | `INT` | `null` | (unchanged) |

### 3. Logging

When values are coerced, logs show:

```
[VALIDATION] value coerced
  doc_id: order:12345
  table: orders
  field: amount
  old_value: "99.999"
  new_value: 100.00
```

When errors occur:

```
[VALIDATION] validation errors for orders: 1 errors | old values tracked
  doc_id: order:12346
  table: orders
  errors: {"_extra_columns": "columns not in schema: unknown_field"}
```

## Supported Data Types

Validators can coerce to these SQL types:

### Numeric
- `INT`, `INTEGER`, `BIGINT`, `SMALLINT`, `TINYINT` — converts to integer
- `DECIMAL(p,s)`, `NUMERIC(p,s)` — converts to float, rounds to scale
- `FLOAT`, `DOUBLE`, `REAL` — converts to float

### String
- `VARCHAR(n)`, `CHAR(n)`, `TEXT` — converts to string, truncates to max length
- `NVARCHAR(n)`, `NCHAR(n)`, `NTEXT` — same as above

### Temporal
- `DATE` — parses ISO date strings or date objects
- `DATETIME`, `TIMESTAMP` — parses ISO datetime strings
- `TIME` — parses time strings

### Boolean
- `BOOLEAN`, `BOOL`, `BIT` — converts "true"/"1"/"yes"/"on" → true, others → false

### Other
- `JSON`, `JSONB` — serializes dicts/lists as JSON strings
- `UUID`, `GUID`, `BYTEA`, `BLOB` — converts to string

## Example Workflow

### Before: Data Errors in DB

```
Incoming change: { order_id: "456", amount: "100.50" }
↓
No validation → INSERT with string values
↓
SELECT * FROM orders WHERE order_id = "456"  ← String comparison fails!
SELECT * FROM orders WHERE amount > 100      ← String comparison wrong!
```

### After: Automatic Coercion

```
Incoming change: { order_id: "456", amount: "100.50" }
↓
Validation enabled:
  order_id: "456" → 456 (INT coercion)
  amount: "100.50" → 100.50 (DECIMAL coercion)
↓
INSERT INTO orders (order_id, amount) VALUES (456, 100.50)
↓
SELECT * FROM orders WHERE order_id = 456    ✓ Works
SELECT * FROM orders WHERE amount > 100      ✓ Works
```

## Handling Errors

### Strict vs. Lenient

**Lenient mode** (strict=false, default):
- Extra columns are **ignored** (allowed)
- Unknown columns are just skipped
- Documents with partial data are accepted
- ✅ Forgiving, handles schema evolution

**Strict mode** (strict=true):
- Extra columns cause **validation error**
- Document is marked as invalid and **not sent to SQL execution**
- When `dlq_on_error=true`, the doc is routed to DLQ with `error_class: "validation"` (see [FAILURE_OPTION_OUTPUT_RDBMS.md](FAILURE_OPTION_OUTPUT_RDBMS.md) §2.13)
- ✅ Stricter data quality enforcement

### DLQ Routing

When `strict=true` and `dlq_on_error=true` (default), documents that fail validation are routed to the DLQ with `error_class: "validation"` and are **not** sent to SQL execution. In lenient mode (`strict=false`), coerced data proceeds to output normally.

Monitor with these metrics:

```
changes_worker_db_validation_errors_total{engine="postgres",job_id="orders_sync"} 5
changes_worker_validation_coercions_total{job_id="orders_sync"} 42
```

The `validation_coercions_total` metric tracks how many values were coerced (available in default log output at INFO level). Full data quality collection writes are planned for v2.1.

## Performance

Validation is **lightweight**:
- Type coercion is ~1-2 microseconds per field
- Runs on the same thread as mapping (no async overhead)
- Per-mapper instance caches validators
- Minimal memory footprint (~100 bytes per table schema)

For large documents (100+ columns), validation adds <1% overhead.

## Auditing

Enable detailed logging to see all transformations:

```bash
# In logging config
RUST_LOG=changes_worker=debug  # Shows all VALIDATION events
```

Logs include:
- Field-level coercions (old → new values)
- Validation errors (which columns failed)
- Schema mismatches (strict mode rejections)

You can then:
1. **Alert** on coercion count spikes
2. **Debug** source system issues
3. **Audit** which fields needed fixing
4. **Tune** your mappings based on patterns

## API & Config

### REST Endpoints

No new endpoints; validation config is part of RDBMS output settings:

```bash
# Get current validation config
GET /api/outputs_rdbms

# Update validation config  
POST /api/outputs_rdbms/{id}
Content-Type: application/json
{
  "validation": {
    "enabled": true,
    "strict": false,
    "track_originals": true,
    "dlq_on_error": true
  }
}
```

### Programmatic Usage

```python
from schema.validator import SchemaValidator, ValidatorConfig

# Define schema
schema = {
    "order_id": "INT",
    "amount": "DECIMAL(10,2)",
}
validator = SchemaValidator("orders", schema)

# Validate & coerce a document
doc = {"order_id": "123", "amount": "99.99"}
result = validator.validate_and_coerce(doc)

print(result.valid)  # True
print(result.coerced_doc)  # {"order_id": 123, "amount": 99.99}
print(result.coercions)  # {field: (old, new), ...}
print(result.errors)  # {field: error_msg, ...}
```

## FAQ

**Q: Does validation slow down the pipeline?**  
A: No, it adds <1% overhead for typical documents.

**Q: What if my schema changes?**  
A: Reload the mapping files (via REST API or file watch) and validators rebuild automatically.

**Q: Can I see which docs were coerced?**  
A: Yes, enable `debug` logs and search for `[VALIDATION] value coerced`.

**Q: What if a value can't be coerced?**  
A: In lenient mode (`strict=false`), it becomes NULL and a coercion is logged at INFO level. In strict mode (`strict=true`), the entire document is rejected — routed to DLQ if `dlq_on_error=true`, or skipped if `dlq_on_error=false`.

**Q: Can I use this without schema mappings?**  
A: No, validation requires column type info from mappings.

**Q: How do I disable validation for specific tables?**  
A: Omit that table from your schema mappings (it won't be validated).

## See Also

- [Schema Mappings](./SCHEMA_MAPPINGS.md)
- [RDBMS Outputs](./RDBMS.md)
- [DLQ (Dead Letter Queue)](./DLQ.md)
