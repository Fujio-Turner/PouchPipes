"""
Data validation and coercion for RDBMS outputs.

Provides Pydantic-based schema validation and type coercion
to automatically transform incoming documents against CREATE TABLE definitions.
Tracks original vs. transformed values for audit/debugging.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Optional, TypeVar
import re

try:
    from pydantic import BaseModel, ValidationError, field_validator, ConfigDict
except ImportError:
    BaseModel = object  # Fallback if pydantic not installed
    ValidationError = Exception

from pipeline.pipeline_logging import log_event

logger = logging.getLogger("changes_worker")

T = TypeVar("T")


# ---------------------------------------------------------------------------
# SQL Type Mapping
# ---------------------------------------------------------------------------

_SQL_TYPE_RE = re.compile(
    r"^(BIGINT|INT|INTEGER|SMALLINT|TINYINT|DECIMAL|NUMERIC|FLOAT|DOUBLE|REAL|"
    r"VARCHAR|CHAR|TEXT|NVARCHAR|NCHAR|NTEXT|"
    r"DATE|DATETIME|TIMESTAMP|TIME|"
    r"BOOLEAN|BOOL|BIT|"
    r"JSON|JSONB|BYTEA|BLOB|"
    r"UUID|GUID)"
    r"(\s*\(.*\))?",
    re.IGNORECASE,
)


def parse_sql_type(sql_type: str) -> tuple[str, Optional[dict]]:
    """
    Parse SQL type and extract base type + metadata.

    Examples:
        "VARCHAR(255)" -> ("VARCHAR", {"max_length": 255})
        "DECIMAL(10,2)" -> ("DECIMAL", {"precision": 10, "scale": 2})
        "INT" -> ("INT", {})
    """
    m = _SQL_TYPE_RE.match(sql_type.strip())
    if not m:
        return (sql_type, {})

    base = m.group(1).upper()
    params_str = m.group(2)
    metadata = {}

    if params_str:
        # Extract params from parentheses
        params = params_str.strip("()").split(",")
        if base.startswith("VARCHAR") or base.startswith("CHAR"):
            try:
                metadata["max_length"] = int(params[0].strip())
            except (ValueError, IndexError):
                pass
        elif base in ("DECIMAL", "NUMERIC"):
            try:
                metadata["precision"] = int(params[0].strip())
                if len(params) > 1:
                    metadata["scale"] = int(params[1].strip())
            except (ValueError, IndexError):
                pass

    return (base, metadata)


def coerce_value(value: Any, sql_type: str) -> Any:
    """
    Coerce *value* to match *sql_type*.

    Returns coerced value or None if coercion fails.
    """
    if value is None:
        return None

    base_type, metadata = parse_sql_type(sql_type)
    base = base_type.upper()

    # ---- Integer types ----
    if base in ("BIGINT", "INT", "INTEGER", "SMALLINT", "TINYINT"):
        try:
            if isinstance(value, bool):
                return int(value)
            return int(value)
        except (ValueError, TypeError):
            return None

    # ---- Float types ----
    elif base in ("DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL"):
        try:
            val = float(value)
            # Round to scale if provided
            if base in ("DECIMAL", "NUMERIC") and "scale" in metadata:
                scale = metadata["scale"]
                val = round(val, scale)
            return val
        except (ValueError, TypeError):
            return None

    # ---- String types ----
    elif base in ("VARCHAR", "CHAR", "TEXT", "NVARCHAR", "NCHAR", "NTEXT"):
        s = str(value)
        if "max_length" in metadata:
            s = s[: metadata["max_length"]]
        return s

    # ---- Boolean ----
    elif base in ("BOOLEAN", "BOOL", "BIT"):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        s = str(value).lower()
        return s in ("true", "1", "yes", "on")

    # ---- Date/Time ----
    elif base in ("DATE", "DATETIME", "TIMESTAMP", "TIME"):
        if isinstance(value, (datetime, date)):
            return value.isoformat() if isinstance(value, datetime) else str(value)
        # Try parsing ISO format
        try:
            if base == "DATE":
                return date.fromisoformat(str(value))
            else:
                return datetime.fromisoformat(str(value))
        except (ValueError, AttributeError):
            return str(value)

    # ---- JSON ----
    elif base in ("JSON", "JSONB"):
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)

    # ---- Default: return as string ----
    return str(value)


# ---------------------------------------------------------------------------
# Validation tracker
# ---------------------------------------------------------------------------


class ValidationResult:
    """Result of validating a document."""

    def __init__(self, doc_id: str):
        self.doc_id = doc_id
        self.valid = True
        self.coercions: dict[str, tuple[Any, Any]] = {}  # {field: (old, new)}
        self.errors: dict[str, str] = {}  # {field: error_msg}
        self.coerced_doc: dict[str, Any] = {}

    def add_coercion(self, field: str, old_val: Any, new_val: Any) -> None:
        """Record a value transformation."""
        self.coercions[field] = (old_val, new_val)

    def add_error(self, field: str, error: str) -> None:
        """Record a validation error."""
        self.valid = False
        self.errors[field] = error

    def summary(self) -> str:
        """Return human-readable summary."""
        parts = []
        if self.coercions:
            parts.append(f"{len(self.coercions)} coercions")
        if self.errors:
            parts.append(f"{len(self.errors)} errors")
        return " | ".join(parts) if parts else "OK"


# ---------------------------------------------------------------------------
# Schema validation (from CREATE TABLE)
# ---------------------------------------------------------------------------


class SchemaValidator:
    """Validates and coerces documents against RDBMS table schema."""

    def __init__(self, table_name: str, schema: dict[str, str]):
        """
        Args:
            table_name: Name of the target table
            schema: {column_name: sql_type, ...}
        """
        self.table_name = table_name
        self.schema = schema

    def validate_and_coerce(
        self,
        doc: dict[str, Any],
        doc_id: str = "unknown",
        strict: bool = False,
    ) -> ValidationResult:
        """
        Validate and coerce document against schema.

        Args:
            doc: Input document
            doc_id: Document ID for logging
            strict: If True, reject unknown columns. If False, allow extra columns.

        Returns:
            ValidationResult with coerced_doc and transformations.
        """
        result = ValidationResult(doc_id)
        coerced = {}

        # Process each column in the schema
        for col_name, sql_type in self.schema.items():
            if col_name not in doc:
                # Column not in doc — use NULL
                coerced[col_name] = None
                continue

            old_val = doc[col_name]
            new_val = coerce_value(old_val, sql_type)

            # Check if coercion changed the value
            if new_val != old_val and new_val is not None:
                result.add_coercion(col_name, old_val, new_val)

            coerced[col_name] = new_val

        # Check for extra columns
        if strict:
            extra = set(doc.keys()) - set(self.schema.keys())
            if extra:
                result.add_error(
                    "_extra_columns",
                    f"columns not in schema: {', '.join(sorted(extra))}",
                )

        result.coerced_doc = coerced

        log_event(
            logger,
            "debug" if result.valid else "warn",
            "MAPPING",
            "schema validation: %s" % result.summary(),
            doc_id=doc_id,
        )

        return result


# ---------------------------------------------------------------------------
# Integration with mapping
# ---------------------------------------------------------------------------


class ValidatorConfig:
    """Configuration for automatic schema validation."""

    def __init__(
        self,
        enabled: bool = False,
        strict: bool = False,
        track_originals: bool = True,
        dlq_on_error: bool = True,
    ):
        """
        Args:
            enabled: Whether to enable automatic validation
            strict: Whether to reject unknown columns
            track_originals: Whether to log original values when coerced
            dlq_on_error: Whether to send invalid docs to DLQ
        """
        self.enabled = enabled
        self.strict = strict
        self.track_originals = track_originals
        self.dlq_on_error = dlq_on_error


def build_schema_from_mapping(
    mapping_def: dict,
) -> dict[str, SchemaValidator]:
    """
    Build SchemaValidator instances from mapping definition.

    Expected structure:
        {
            "tables": [
                {
                    "table_name": "orders",
                    "columns": {"order_id": "INT", "amount": "DECIMAL(10,2)", ...}
                },
                ...
            ]
        }

    Returns:
        {table_name: SchemaValidator, ...}
    """
    validators = {}

    tables = mapping_def.get("tables", [])
    if not isinstance(tables, list):
        return validators

    for table_def in tables:
        if not isinstance(table_def, dict):
            continue

        table_name = table_def.get("table_name")
        columns = table_def.get("columns", {})

        if not table_name or not isinstance(columns, dict):
            continue

        validators[table_name] = SchemaValidator(table_name, columns)

    return validators
