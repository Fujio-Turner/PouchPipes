"""
Base output forwarder for RDBMS outputs.

Provides shared logic for metrics (with per-engine + per-job_id labels),
mapping loading, send() pre-flight, retry loop, and stats logging.
Each engine subclass only needs to implement the driver-specific pieces:
pool creation, SQL execution, error classification, and introspection.
"""

import abc
import asyncio
import json
import logging
import re
import threading
import time
from collections import deque
from pathlib import Path

from pipeline.pipeline_logging import log_event
from schema.validator import SchemaValidator, ValidatorConfig, ValidationResult

try:
    from icecream import ic
except ImportError:
    ic = lambda *a, **kw: None  # noqa: E731

logger = logging.getLogger("changes_worker")

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


# ---------------------------------------------------------------------------
# Multi-row INSERT grouping  (Level 1 batching)
# ---------------------------------------------------------------------------
# Consecutive INSERT ops targeting the same table with the same column set
# are collapsed into a single ``_MultiRowInsert`` that each engine can
# render as one ``INSERT … VALUES (…),(…),(…)`` round-trip.


class _MultiRowInsert:
    """A group of INSERT ops collapsed into one multi-row statement.

    Attributes:
        table:   Target table name.
        columns: Ordered list of column names (identical across all rows).
        rows:    List of per-row param lists.
    """

    __slots__ = ("table", "columns", "rows")

    def __init__(self, table: str, columns: list[str], rows: list[list]):
        self.table = table
        self.columns = columns
        self.rows = rows


def group_insert_ops(ops: list) -> list:
    """Return a new op list with consecutive same-table INSERTs merged.

    Non-INSERT ops (DELETE, UPSERT) and INSERTs that change column sets
    are emitted as-is, preserving original ordering.
    """
    grouped: list = []
    i = 0
    while i < len(ops):
        op = ops[i]
        if op.op_type != "INSERT":
            grouped.append(op)
            i += 1
            continue

        # Start a new multi-row group
        data = op.data or {}
        cols = list(data.keys())
        col_key = tuple(cols)
        table = op.table
        rows = [[data[c] for c in cols]]

        j = i + 1
        while j < len(ops):
            nxt = ops[j]
            if nxt.op_type != "INSERT" or nxt.table != table:
                break
            nxt_data = nxt.data or {}
            if tuple(nxt_data.keys()) != col_key:
                break
            rows.append([nxt_data[c] for c in cols])
            j += 1

        if len(rows) == 1:
            # Single INSERT — keep original op (no overhead)
            grouped.append(op)
        else:
            grouped.append(_MultiRowInsert(table, cols, rows))

        i = j
    return grouped


def validate_identifier(name: str, context: str = "identifier") -> str:
    """Validate a SQL identifier and return it unchanged.

    Raises ValueError if the name contains characters that could enable
    SQL injection.
    """
    if not _IDENT_RE.match(name):
        raise ValueError(
            f"Invalid SQL {context}: {name!r} — "
            "only letters, digits, underscores, and $ are allowed"
        )
    return name


# ── Per-engine / per-job metrics proxy ──────────────────────────────────────
# Wraps the global MetricsCollector so every inc() also records a labeled
# counter that can be broken out by engine and job_id on the /_metrics page.


class DbMetrics:
    """
    Lightweight metrics wrapper that tracks counters with (engine, job_id)
    labels while also delegating to the global MetricsCollector for totals.

    Usage in Prometheus:
        # Global totals (backward compat – existing dashboards keep working)
        changes_worker_output_requests_total{src="...",database="..."} 500

        # Per-engine / per-job breakdowns
        changes_worker_db_output_requests_total{engine="postgres",job_id="orders_sync"} 300
        changes_worker_db_output_requests_total{engine="oracle",job_id="analytics"} 200
    """

    # Class-level registry so the metrics endpoint can iterate all instances.
    _registry_lock = threading.Lock()
    _registry: list["DbMetrics"] = []

    def __init__(self, engine: str, job_id: str, global_metrics=None):
        self.engine = engine
        self.job_id = job_id or engine  # fallback: use engine name
        self._global = global_metrics  # MetricsCollector from main.py
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._resp_times: deque[float] = deque(maxlen=10_000)

        with DbMetrics._registry_lock:
            DbMetrics._registry.append(self)

    # ── counter / timing ────────────────────────────────────────────────

    def inc(self, name: str, value: int = 1) -> None:
        """Increment both the local labeled counter AND the global total."""
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + value
        if self._global:
            self._global.inc(name, value)

    def record_output_response_time(self, seconds: float) -> None:
        with self._lock:
            self._resp_times.append(seconds)
        if self._global:
            self._global.record_output_response_time(seconds)

    # ── snapshot (called by render) ─────────────────────────────────────

    def snapshot(self) -> tuple[dict[str, int], list[float]]:
        """Return a copy of counters + resp_times under the lock."""
        with self._lock:
            return dict(self._counters), list(self._resp_times)

    # ── cleanup ─────────────────────────────────────────────────────────

    def unregister(self) -> None:
        with DbMetrics._registry_lock:
            try:
                DbMetrics._registry.remove(self)
            except ValueError:
                pass

    # ── class-level render (called from MetricsCollector.render) ────────

    @classmethod
    def render_all(cls) -> str:
        """
        Render per-engine/per-job DB metrics in Prometheus text format.

        Returns a string of lines that the main MetricsCollector.render()
        can append to its output.
        """
        with cls._registry_lock:
            instances = list(cls._registry)

        if not instances:
            return ""

        # Collect snapshots keyed by (engine, job_id)
        snapshots: list[tuple[str, str, dict[str, int], list[float]]] = []
        for inst in instances:
            counters, resp_times = inst.snapshot()
            snapshots.append((inst.engine, inst.job_id, counters, resp_times))

        # Gather all counter names across all instances
        all_counter_names: set[str] = set()
        for _, _, counters, _ in snapshots:
            all_counter_names.update(counters.keys())

        lines: list[str] = []

        # Emit one HELP/TYPE block per counter, with one line per (engine, job_id)
        for name in sorted(all_counter_names):
            prom_name = f"changes_worker_db_{name}"
            lines.append(
                f"# HELP {prom_name} DB output counter: {name} (per engine/job)"
            )
            lines.append(f"# TYPE {prom_name} counter")
            for engine, job_id, counters, _ in snapshots:
                val = counters.get(name, 0)
                if val:
                    lines.append(
                        f'{prom_name}{{engine="{engine}",job_id="{job_id}"}} {val}'
                    )

        # Emit per-instance response time summaries
        has_resp = any(rt for _, _, _, rt in snapshots)
        if has_resp:
            prom_name = "changes_worker_db_output_response_time_seconds"
            lines.append(f"# HELP {prom_name} DB output response time (per engine/job)")
            lines.append(f"# TYPE {prom_name} summary")
            for engine, job_id, _, resp_times in snapshots:
                if not resp_times:
                    continue
                s = sorted(resp_times)
                count = len(s)
                total = sum(s)
                for q in (0.5, 0.9, 0.99):
                    idx = int(q * (count - 1))
                    lines.append(
                        f'{prom_name}{{engine="{engine}",job_id="{job_id}",'
                        f'quantile="{q}"}} {s[idx]:.6f}'
                    )
                lines.append(
                    f'{prom_name}_sum{{engine="{engine}",job_id="{job_id}"}} {total:.6f}'
                )
                lines.append(
                    f'{prom_name}_count{{engine="{engine}",job_id="{job_id}"}} {count}'
                )

        return "\n".join(lines)


# ── Abstract base forwarder ─────────────────────────────────────────────────


class BaseOutputForwarder(abc.ABC):
    """
    Abstract async RDBMS output forwarder.

    Subclasses must implement:
        _engine          – property returning engine name (e.g. "postgres")
        _connect_pool()  – create the async connection pool
        _close_pool()    – close the pool
        _execute_ops()   – acquire conn, run ops inside a transaction
        _test_connection()– run a simple health query (SELECT 1)
        _is_transient()  – classify whether an exception is retryable
        _error_class()   – return a short error classification string

    Provided by this base class:
        _reconnect_pool()– resilient close + re-create with stampede
                           protection, cooldown, and background health check
    """

    def __init__(self, out_cfg: dict, dry_run: bool = False, metrics=None):
        self._dry_run = dry_run
        self._halt_on_failure = out_cfg.get("halt_on_failure", True)
        self._data_error_action = out_cfg.get("data_error_action", "dlq")
        self._metrics_global = metrics

        # Engine-specific config is read by the subclass __init__.
        # Common config shared across all engines:
        engine_cfg = self._get_engine_cfg(out_cfg)
        self._max_retries = engine_cfg.get("max_retries", 3)
        self._backoff_base = engine_cfg.get("backoff_base_seconds", 0.5)
        self._backoff_max = engine_cfg.get("backoff_max_seconds", 10)
        self._sync_commit = engine_cfg.get("sync_commit", False)
        self._prepared_statements = engine_cfg.get("prepared_statements", True)

        # Mapping config
        self._mappers: list = []
        self._mapping_file = out_cfg.get("mapping_file", "")
        sm = engine_cfg.get("schema_mappings", {})
        self._mappings_dir = sm.get("path", "") if sm.get("enabled") else ""

        # Data validation config
        validation_cfg = engine_cfg.get("validation", {})
        self._validator_config = ValidatorConfig(
            enabled=validation_cfg.get("enabled", False),
            strict=validation_cfg.get("strict", False),
            track_originals=validation_cfg.get("track_originals", True),
            dlq_on_error=validation_cfg.get("dlq_on_error", True),
        )
        self._validators: dict[str, SchemaValidator] = {}  # {table_name: validator}

        # Job ID for per-job metric labels (falls back to engine name)
        self._job_id = out_cfg.get("job_id", "")

        # Per-engine/per-job metrics proxy (created after subclass sets _engine)
        # Subclass __init__ MUST call _init_metrics() at the end.
        self._metrics: DbMetrics | None = None

        # Thread pool for offloading CPU-bound schema mapping
        self._map_executor = None  # set via set_map_executor()

        # Response time tracking (local to this forwarder)
        self._resp_times: deque[float] = deque(maxlen=10_000)
        self._lock = asyncio.Lock()
        self._pool_lock = asyncio.Lock()
        self._pool_generation = 0
        self._pool_reconnecting = False
        self._pool_reconnect_time: float = 0.0  # monotonic timestamp of last attempt
        self._health_task: asyncio.Task | None = None
        self._no_mapping_warned = False
        self._reconnect_event: asyncio.Event = asyncio.Event()
        self._reconnect_event.set()  # start as "not reconnecting"

    def _init_metrics(self) -> None:
        """Call from subclass __init__ after the engine property is available."""
        self._metrics = DbMetrics(
            engine=self._engine,
            job_id=self._job_id,
            global_metrics=self._metrics_global,
        )

    @property
    @abc.abstractmethod
    def _engine(self) -> str:
        """Return the engine name (e.g. 'postgres', 'mysql')."""

    @property
    def _mode(self) -> str:
        """Alias for _engine used by main.py logging."""
        return self._engine

    @abc.abstractmethod
    def _get_engine_cfg(self, out_cfg: dict) -> dict:
        """Extract the engine-specific config dict from out_cfg."""

    def set_map_executor(self, executor) -> None:
        """Set a ThreadPoolExecutor for offloading CPU-bound schema mapping."""
        self._map_executor = executor

    # ── Mapping loading (engine-agnostic) ───────────────────────────────

    def _load_mappers(self) -> None:
        """
        Load schema mappers from CBL or filesystem.
        Called by connect() after the pool is created.
        Also builds validators if validation is enabled.
        """
        from schema.mapper import MappingDiagnostics, SchemaMapper

        new_mappers: list = []

        # Prefer CBL (single source of truth), fall back to filesystem
        cbl_loaded = False
        try:
            from storage.cbl_store import USE_CBL, CBLStore

            if USE_CBL:
                entries = CBLStore().list_mappings()
                for entry in entries:
                    name = entry.get("name", "")
                    if name.endswith(".meta.json"):
                        continue
                    raw = entry.get("content", "")
                    if not raw:
                        continue
                    data = json.loads(raw) if isinstance(raw, str) else raw
                    data["_source_name"] = name
                    mapper = SchemaMapper(data)
                    new_mappers.append(mapper)

                    # Build validators from this mapping if validation is enabled
                    if self._validator_config.enabled:
                        self._build_validators_from_mapping(data)

                    log_event(
                        logger,
                        "info",
                        "MAPPING",
                        "loaded schema mapping from CBL",
                        doc_id=name,
                        storage="cbl",
                    )
                cbl_loaded = True
        except Exception as exc:
            ic("_load_mappers: CBL fallback", type(exc).__name__, str(exc))
            log_event(
                logger,
                "warn",
                "MAPPING",
                "could not load mappings from CBL – falling back to filesystem",
                error_detail=f"{type(exc).__name__}: {exc}",
            )

        if not cbl_loaded:
            if self._mappings_dir:
                mdir = Path(self._mappings_dir)
                if mdir.is_dir():
                    for f in sorted(mdir.glob("*.json")):
                        if f.name.endswith(".meta.json"):
                            continue
                        try:
                            raw = json.loads(f.read_text())
                            meta = raw.get("meta", {})
                            if not meta.get("active", True):
                                log_event(
                                    logger,
                                    "info",
                                    "MAPPING",
                                    "skipping inactive mapping",
                                    doc_id=str(f),
                                )
                                continue
                            mapper = SchemaMapper(raw)
                            new_mappers.append(mapper)

                            # Build validators from this mapping if validation is enabled
                            if self._validator_config.enabled:
                                self._build_validators_from_mapping(raw)

                            log_event(
                                logger,
                                "info",
                                "MAPPING",
                                "loaded schema mapping from file",
                                doc_id=str(f),
                                storage="file",
                            )
                        except (json.JSONDecodeError, OSError) as exc:
                            log_event(
                                logger,
                                "warn",
                                "MAPPING",
                                "skipping invalid mapping file",
                                doc_id=str(f),
                                error_detail=f"{type(exc).__name__}: {exc}",
                            )
                            continue
                        except Exception as exc:
                            log_event(
                                logger,
                                "warn",
                                "MAPPING",
                                "skipping invalid mapping definition",
                                doc_id=str(f),
                                error_detail=f"{type(exc).__name__}: {exc}",
                            )
                            continue
            if self._mapping_file:
                try:
                    new_mappers.append(SchemaMapper.from_file(self._mapping_file))
                    log_event(
                        logger,
                        "info",
                        "MAPPING",
                        "loaded schema mapping from file",
                        doc_id=self._mapping_file,
                        storage="file",
                    )
                except Exception as exc:
                    log_event(
                        logger,
                        "warn",
                        "MAPPING",
                        "failed to load mapping file",
                        doc_id=self._mapping_file,
                        error_detail=f"{type(exc).__name__}: {exc}",
                    )

        if not new_mappers:
            log_event(
                logger,
                "warn",
                "MAPPING",
                "no schema mappings loaded – documents will be skipped",
            )

        # Warn about duplicate match filters (first-match wins)
        seen_matches: dict[str, str] = {}
        for m in new_mappers:
            key = f"{m.match.get('field', '')}={m.match.get('value', '')}"
            src = m.mapping.get("_source_name", "?")
            if key in seen_matches:
                log_event(
                    logger,
                    "warn",
                    "MAPPING",
                    "duplicate mapping: '%s' and '%s' both match %s — "
                    "only '%s' will be used (first-match wins)"
                    % (seen_matches[key], src, key, seen_matches[key]),
                )
            else:
                seen_matches[key] = src

        self._mappers = new_mappers
        self._no_mapping_warned = False
        ic("_load_mappers: done", len(self._mappers))

    def _build_validators_from_mapping(self, mapping_def: dict) -> None:
        """
        Build SchemaValidator instances from a mapping definition.

        Expects mapping to have a "tables" list with:
            {
                "table_name": "orders",
                "columns": {"order_id": "INT", "amount": "DECIMAL(10,2)", ...}
            }
        """
        tables = mapping_def.get("tables", [])
        if not isinstance(tables, list):
            return

        for table_def in tables:
            if not isinstance(table_def, dict):
                continue

            table_name = table_def.get("table_name")
            columns = table_def.get("columns", {})

            if not table_name or not isinstance(columns, dict):
                continue

            # Build a schema dict from the columns
            schema = {col: col_type for col, col_type in columns.items()}
            self._validators[table_name] = SchemaValidator(table_name, schema)

            log_event(
                logger,
                "debug",
                "VALIDATION",
                "built schema validator",
                table=table_name,
                columns=len(schema),
            )

    def _validate_row_for_table(
        self, row: dict, table_name: str, doc_id: str
    ) -> tuple[dict, ValidationResult | None]:
        """
        Validate and coerce a row against the table schema.

        Returns:
            (coerced_row, validation_result) or (row, None) if validation disabled
        """
        if not self._validator_config.enabled:
            return (row, None)

        validator = self._validators.get(table_name)
        if not validator:
            return (row, None)

        result = validator.validate_and_coerce(
            row,
            doc_id=doc_id,
            strict=self._validator_config.strict,
        )

        return (result.coerced_doc, result)

    def _validate_and_fix_ops(self, ops: list, doc_id: str) -> list:
        """
        Validate and coerce all SQL operations against table schemas.

        If validation is enabled, this will:
        1. For each operation with a table name, validate its data
        2. Replace data with coerced version
        3. Log coercions and errors
        4. Return updated ops (or original if validation disabled)
        """
        if not self._validator_config.enabled:
            return ops

        validated_ops = []
        for op in ops:
            # Skip operations without data (e.g., DELETE without row data)
            if not op.data:
                validated_ops.append(op)
                continue

            coerced, result = self._validate_row_for_table(op.data, op.table, doc_id)

            if result:
                # Log coercions
                if result.coercions and self._validator_config.track_originals:
                    for field, (old_val, new_val) in result.coercions.items():
                        log_event(
                            logger,
                            "debug",
                            "VALIDATION",
                            "value coerced",
                            doc_id=doc_id,
                            table=op.table,
                            field=field,
                            old_value=str(old_val)[:100],
                            new_value=str(new_val)[:100],
                        )

                # Log errors
                if result.errors:
                    log_event(
                        logger,
                        "warn",
                        "VALIDATION",
                        f"validation errors for {op.table}: {result.summary()}",
                        doc_id=doc_id,
                        table=op.table,
                        errors=result.errors,
                    )

                    if not result.valid and self._validator_config.dlq_on_error:
                        if self._metrics:
                            self._metrics.inc("validation_errors_total")
                        # Continue with coerced data; caller can decide to DLQ
                        # For now, just log and continue

            # Update op with coerced data
            op.data = coerced
            validated_ops.append(op)

        return validated_ops

    # ── Pool lifecycle (subclass hooks) ─────────────────────────────────

    @abc.abstractmethod
    async def _connect_pool(self) -> None:
        """Create the async connection pool."""

    @abc.abstractmethod
    async def _close_pool(self) -> None:
        """Close the async connection pool."""

    async def _reconnect_pool(self, _generation: int | None = None) -> None:
        """Close and re-create the pool (called on connection errors).

        Includes stampede protection, cooldown, generation-bump on failure,
        and a background health-check that keeps trying to restore a dead pool.
        """
        import time as _time

        # Fast path: if another task already reconnected *successfully*, skip.
        # The pool must be alive — if it's None the other task's reconnect
        # failed and we must not silently return.
        if (
            _generation is not None
            and _generation != self._pool_generation
            and self._pool is not None
        ):
            return

        # If another task is already reconnecting, wait on the event instead
        # of spin-looping.  The event is set when the reconnecting task
        # finishes (success or failure), waking all waiters at once.
        if self._pool_reconnecting:
            try:
                await asyncio.wait_for(self._reconnect_event.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                pass
            # Check if the generation advanced AND pool is alive (reconnect succeeded)
            if (
                _generation is not None
                and _generation != self._pool_generation
                and self._pool is not None
            ):
                return

        async with self._pool_lock:
            # Re-check after acquiring lock — only skip if pool is actually alive
            if (
                _generation is not None
                and _generation != self._pool_generation
                and self._pool is not None
            ):
                return

            # Cooldown: don't attempt reconnect more than once per second
            # BUT: if the pool is None (previous reconnect failed mid-way),
            # always allow the attempt — we must try to restore the pool.
            now = _time.monotonic()
            if self._pool is not None and now - self._pool_reconnect_time < 1.0:
                return

            self._pool_reconnecting = True
            self._reconnect_event.clear()
            self._pool_reconnect_time = now
            try:
                await self._close_pool()
                await self._connect_pool()
                self._pool_generation += 1
            except Exception:
                # connect failed — pool is None (close already ran).
                # Bump generation so callers holding a stale generation
                # don't skip the next reconnect attempt.
                self._pool_generation += 1
                self._ensure_health_check()
                raise
            finally:
                self._pool_reconnecting = False
                self._reconnect_event.set()

    # ── Background health check ───────────────────────────────────────────

    def _ensure_health_check(self) -> None:
        """Start the background health-check task if not already running."""
        if self._health_task and not self._health_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
            self._health_task = loop.create_task(self._health_check_loop())
        except RuntimeError:
            pass  # no running loop — skip

    async def _health_check_loop(self) -> None:
        """Periodically try to restore a dead pool.

        Runs with exponential backoff (2s → 300s) while the pool is None.
        Stops once the pool is successfully recreated.
        """
        attempt = 0
        while True:
            attempt += 1
            delay = min(2**attempt, 300)
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                return

            if self._pool is not None:
                log_event(
                    logger,
                    "info",
                    "OUTPUT",
                    "health check: pool is alive, stopping monitor",
                    mode=self._engine,
                )
                return

            log_event(
                logger,
                "info",
                "OUTPUT",
                "health check: pool is dead, attempting reconnect "
                "(attempt %d, next in %ds)" % (attempt, min(2 ** (attempt + 1), 300)),
                mode=self._engine,
            )
            try:
                async with self._pool_lock:
                    if self._pool is not None:
                        return  # restored between check and lock
                    self._pool_reconnecting = True
                    self._reconnect_event.clear()
                    try:
                        await self._connect_pool()
                        self._pool_generation += 1
                    except Exception:
                        self._pool_generation += 1
                        raise
                    finally:
                        self._pool_reconnecting = False
                        self._reconnect_event.set()

                log_event(
                    logger,
                    "info",
                    "OUTPUT",
                    "health check: pool restored successfully",
                    mode=self._engine,
                    operation="RECONNECT",
                )
                return
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log_event(
                    logger,
                    "debug",
                    "OUTPUT",
                    "health check: reconnect failed: %s" % exc,
                    mode=self._engine,
                )

    async def connect(self) -> None:
        """Create the connection pool and load schema mappings."""
        ic("connect", self._engine)
        async with self._pool_lock:
            await self._connect_pool()
            try:
                self._load_mappers()
            except Exception:
                await self._close_pool()
                raise
        log_event(
            logger, "info", "OUTPUT", "connection pool created", mode=self._engine
        )

    async def close(self) -> None:
        """Close the connection pool and unregister metrics."""
        if (
            hasattr(self, "_health_task")
            and self._health_task
            and not self._health_task.done()
        ):
            self._health_task.cancel()
            self._health_task = None
        ic("close", self._engine)
        try:
            async with self._pool_lock:
                await self._close_pool()
        finally:
            if self._metrics:
                self._metrics.unregister()
        log_event(logger, "info", "OUTPUT", "connection pool closed", mode=self._engine)

    # ── SQL execution (subclass hook) ───────────────────────────────────

    @abc.abstractmethod
    async def _execute_ops(self, ops: list) -> None:
        """
        Acquire a connection, open a transaction, and execute all
        SqlOperation objects.  Raise on any error.
        """

    # ── Error classification (subclass hooks) ───────────────────────────

    @abc.abstractmethod
    def _is_transient(self, exc: Exception) -> bool:
        """Return True if the error is transient and worth retrying."""

    @abc.abstractmethod
    def _error_class(self, exc: Exception) -> str:
        """Return a short classification string for the error."""

    # ── Health check (subclass hook) ────────────────────────────────────

    @abc.abstractmethod
    async def _test_connection(self) -> None:
        """Run a lightweight health check query (e.g. SELECT 1). Raise on failure."""

    async def test_reachable(self) -> bool:
        """Test that the database server is reachable."""
        ic("test_reachable", self._engine)
        try:
            if not hasattr(self, "_pool") or getattr(self, "_pool", None) is None:
                await self.connect()
            await self._test_connection()
            ic("test_reachable: OK", self._engine)
            log_event(logger, "info", "OUTPUT", "database reachable", mode=self._engine)
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            ic("test_reachable: FAIL", self._engine, type(exc).__name__, str(exc))
            log_event(
                logger,
                "error",
                "OUTPUT",
                "database unreachable",
                mode=self._engine,
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            return False

    # ── send() — the main document processing method ────────────────────

    async def send(self, doc: dict, method: str = "PUT") -> dict:
        """
        Process a single document: map to SQL ops and execute.

        Transient errors are retried with exponential backoff.
        Permanent errors return immediately for DLQ routing.

        Returns result dict with 'ok' bool plus 'retryable' and
        'error_class' on failure.
        """
        ic("send", doc.get("_id", doc.get("id", "unknown")) if doc else "None", method)
        if doc is None:
            log_event(
                logger,
                "debug",
                "OUTPUT",
                "received None doc – skipped",
                doc_id="unknown",
            )
            if self._metrics:
                self._metrics.inc("output_skipped_total")
            return {"ok": True, "doc_id": "unknown", "skipped": True}

        doc_id = doc.get("_id", doc.get("id", "unknown"))
        is_delete = method == "DELETE"

        if not self._mappers:
            if not self._no_mapping_warned:
                log_event(
                    logger,
                    "warn",
                    "MAPPING",
                    "No schema mappings loaded — all documents will be skipped "
                    "until mappings are configured.",
                )
                self._no_mapping_warned = True
            if self._metrics:
                self._metrics.inc("output_skipped_total")
            return {
                "ok": False,
                "doc_id": doc_id,
                "error": "no_mapping",
                "retryable": False,
                "error_class": "config",
            }

        # Find the first matching mapper and map the document.
        # This is CPU-bound (JSONPath extraction + transforms), so offload
        # to the map_executor thread pool when available.
        #
        # For deletes (tombstones), the doc body is minimal — typically just
        # {"_id": "...", "_deleted": true} — so the mapper's content-based
        # filter (e.g. type == "order") won't match.  Since a DELETE only
        # needs the doc_id / primary key, we fall back to trying ALL mappers
        # when no content match is found and is_delete is True.
        def _match_and_map():
            for m in self._mappers:
                if m.matches(doc):
                    return m, m.map_document(doc, is_delete=is_delete)
            # Tombstone fallback: try first mapper that produces DELETE ops
            if is_delete:
                for m in self._mappers:
                    result = m.map_document(doc, is_delete=True)
                    if result[0]:  # has ops
                        return m, result
            return None, None

        try:
            if self._map_executor is not None:
                loop = asyncio.get_event_loop()
                mapper, map_result = await loop.run_in_executor(
                    self._map_executor, _match_and_map
                )
            else:
                mapper, map_result = _match_and_map()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self._metrics:
                self._metrics.inc("output_requests_total")
                self._metrics.inc("output_errors_total")
                self._metrics.inc("mapper_errors_total")
                self._metrics.inc("db_permanent_errors_total")
            ic("send: mapping error", doc_id, type(exc).__name__, str(exc))
            log_event(
                logger,
                "error",
                "MAPPING",
                "mapping error",
                doc_id=doc_id,
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            return {
                "ok": False,
                "doc_id": doc_id,
                "error": f"mapping_error: {exc!s}"[:500],
                "retryable": False,
                "error_class": "mapping",
            }

        if not mapper:
            if is_delete:
                log_event(
                    logger,
                    "info",
                    "MAPPING",
                    "tombstone (deleted/removed) does not match any mapping – skipped",
                    doc_id=doc_id,
                )
            else:
                log_event(
                    logger,
                    "info",
                    "MAPPING",
                    "doc does not match any mapping filter – skipped",
                    doc_id=doc_id,
                )
            if self._metrics:
                self._metrics.inc("output_skipped_total")
                self._metrics.inc("mapper_skipped_total")
            return {"ok": True, "doc_id": doc_id, "skipped": True}

        ops, diag = map_result

        if diag.has_issues:
            doc_rev = doc.get("_rev", doc.get("rev", "?"))
            log_event(
                logger,
                "warn",
                "MAPPING",
                "mapping issues: %s" % diag.summary(),
                doc_id=doc_id,
            )

        if self._metrics:
            self._metrics.inc("mapper_matched_total")

        if not ops:
            log_event(
                logger,
                "info",
                "MAPPING",
                "mapper matched but produced no operations – skipped",
                doc_id=doc_id,
            )
            if self._metrics:
                self._metrics.inc("output_skipped_total")
            return {"ok": True, "doc_id": doc_id, "ops": 0}

        # Validate and coerce rows against table schemas if enabled
        ops = self._validate_and_fix_ops(ops, doc_id)

        if self._dry_run:
            for op in ops:
                sql, params = op.to_sql()
                log_event(
                    logger,
                    "debug",
                    "OUTPUT",
                    "[DRY RUN] %s | params=%s" % (sql, params),
                    doc_id=doc_id,
                )
            return {"ok": True, "doc_id": doc_id, "ops": len(ops), "dry_run": True}

        # -- Execute with retry for transient errors --
        t_start = time.monotonic()
        last_exc: Exception | None = None
        pool_gen = self._pool_generation

        for attempt in range(1, self._max_retries + 1):
            try:
                await self._execute_ops(ops)

                # Success
                elapsed_ms = (time.monotonic() - t_start) * 1000
                async with self._lock:
                    self._resp_times.append(elapsed_ms)
                if self._metrics:
                    self._metrics.inc("output_requests_total")
                    self._metrics.inc("output_success_total")
                    self._metrics.inc("mapper_ops_total", len(ops))
                    self._metrics.record_output_response_time(elapsed_ms / 1000)
                    # Measure bytes from generated SQL + params for accurate
                    # bytes_output_total tracking (applies to all op types
                    # including DELETE).
                    sql_bytes = 0
                    for op in ops:
                        sql, params = op.to_sql()
                        sql_bytes += len(sql.encode("utf-8"))
                        for p in params:
                            sql_bytes += len(str(p).encode("utf-8"))
                    self._metrics.inc("bytes_output_total", sql_bytes)
                    if self._metrics_global:
                        self._metrics_global.inc("output_ops_total", len(ops))

                doc_rev = doc.get("_rev", doc.get("rev", "?"))
                ic("send: OK", doc_id, len(ops), round(elapsed_ms, 1))

                # Track delete operations forwarded to output
                if is_delete and self._metrics:
                    self._metrics.inc("deletes_forwarded_total")

                log_event(
                    logger,
                    "debug",
                    "OUTPUT",
                    "executed SQL ops",
                    doc_id=doc_id,
                    operation="DELETE" if is_delete else "UPSERT",
                    elapsed_ms=round(elapsed_ms, 1),
                    mode=self._engine,
                    http_method=method,
                )
                return {
                    "ok": True,
                    "doc_id": doc_id,
                    "ops": len(ops),
                    "elapsed_ms": round(elapsed_ms, 1),
                }

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_exc = exc
                eclass = self._error_class(exc)

                if not self._is_transient(exc):
                    elapsed_ms = (time.monotonic() - t_start) * 1000
                    if self._metrics:
                        self._metrics.inc("output_requests_total")
                        self._metrics.inc("output_errors_total")
                        self._metrics.inc("db_permanent_errors_total")
                    ic(
                        "send: permanent error",
                        doc_id,
                        eclass,
                        type(exc).__name__,
                        str(exc),
                    )
                    log_event(
                        logger,
                        "error",
                        "OUTPUT",
                        "permanent error",
                        doc_id=doc_id,
                        mode=self._engine,
                        error_detail=f"{type(exc).__name__}: {exc}",
                    )

                    return {
                        "ok": False,
                        "doc_id": doc_id,
                        "error": str(exc)[:500],
                        "retryable": False,
                        "error_class": eclass,
                        "data_error_action": self._data_error_action,
                    }

                # Transient error — retry
                if self._metrics:
                    self._metrics.inc("db_transient_errors_total")
                    self._metrics.inc("db_retries_total")

                if eclass in ("connection", "table_not_found", "server_shutdown"):
                    try:
                        ic(
                            "send: %s error, reconnecting" % eclass,
                            doc_id,
                            attempt,
                            self._max_retries,
                        )
                        log_event(
                            logger,
                            "warn",
                            "OUTPUT",
                            "%s error – reconnecting pool" % eclass,
                            doc_id=doc_id,
                            mode=self._engine,
                            attempt=attempt,
                            error_detail=f"{type(exc).__name__}: {exc}",
                        )
                        await self._reconnect_pool(_generation=pool_gen)
                        pool_gen = self._pool_generation
                        if self._metrics:
                            self._metrics.inc("db_pool_reconnects_total")
                    except Exception as reconn_exc:
                        # Update pool_gen so the next retry attempt doesn't
                        # skip reconnection via the stale-generation fast path.
                        pool_gen = self._pool_generation
                        ic(
                            "send: pool reconnect failed",
                            type(reconn_exc).__name__,
                            str(reconn_exc),
                        )
                        log_event(
                            logger,
                            "error",
                            "OUTPUT",
                            "pool reconnect failed",
                            mode=self._engine,
                            error_detail=f"{type(reconn_exc).__name__}: {reconn_exc}",
                        )
                        if attempt == self._max_retries:
                            break
                else:
                    ic(
                        "send: transient error",
                        doc_id,
                        eclass,
                        attempt,
                        self._max_retries,
                    )
                    log_event(
                        logger,
                        "warn",
                        "OUTPUT",
                        "transient error",
                        doc_id=doc_id,
                        mode=self._engine,
                        attempt=attempt,
                        error_detail=f"{type(exc).__name__}: {exc}",
                    )

                if attempt < self._max_retries:
                    delay = min(
                        self._backoff_base * (2 ** (attempt - 1)), self._backoff_max
                    )
                    await asyncio.sleep(delay)

        # All retries exhausted
        elapsed_ms = (time.monotonic() - t_start) * 1000
        eclass = self._error_class(last_exc) if last_exc else "unknown"
        if self._metrics:
            self._metrics.inc("output_requests_total")
            self._metrics.inc("output_errors_total")
            self._metrics.inc("db_retry_exhausted_total")
        ic("send: retries exhausted", doc_id, eclass, self._max_retries)
        log_event(
            logger,
            "error",
            "OUTPUT",
            "retries exhausted",
            doc_id=doc_id,
            mode=self._engine,
            attempt=self._max_retries,
            error_detail=f"{type(last_exc).__name__}: {last_exc}"
            if last_exc
            else "unknown",
        )

        if self._halt_on_failure:
            from rest import OutputEndpointDown

            raise OutputEndpointDown(
                f"{self._engine.upper()} retries exhausted for {doc_id} "
                f"[{eclass}]: {last_exc}"
            ) from last_exc
        return {
            "ok": False,
            "doc_id": doc_id,
            "error": str(last_exc)[:500],
            "retryable": False,
            "error_class": eclass,
        }

    # ── stats logging ───────────────────────────────────────────────────

    def log_stats(self, force_info: bool = False) -> None:
        """Log accumulated response time statistics."""
        if not self._resp_times:
            return
        n = len(self._resp_times)
        avg = sum(self._resp_times) / n
        lo = min(self._resp_times)
        hi = max(self._resp_times)
        log_event(
            logger,
            "info" if force_info else "debug",
            "OUTPUT",
            "%s stats: %d ops | avg=%.1fms | min=%.1fms | max=%.1fms"
            % (self._engine.upper(), n, avg, lo, hi),
            mode=self._engine,
        )
