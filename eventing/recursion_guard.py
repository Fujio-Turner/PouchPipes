"""
Recursion Guard — TTL-bounded LRU cache for write-back echo suppression.

When the pipeline PUTs a document back to the same source bucket,
the resulting _changes entry must be detected and skipped to prevent
infinite recursion loops.

Implementation notes:
  - Pure-stdlib OrderedDict LRU (no external dependencies)
  - Lazy TTL eviction on reads — no background threads
  - Designed for asyncio single-threaded use (no locking)
"""

import logging
import time
from collections import OrderedDict

from pipeline.pipeline_logging import log_event

logger = logging.getLogger("changes_worker")

# Defaults
_DEFAULT_MAXSIZE = 50_000
_DEFAULT_TTL_SECONDS = 300


class RecursionGuard:
    """Tracks _id -> _rev for documents the pipeline has written back.

    Uses an OrderedDict as an LRU cache with per-entry TTL expiry.
    Entries are evicted lazily on ``is_echo()`` checks and eagerly
    when ``maxsize`` is exceeded on ``record()``.
    """

    def __init__(
        self, maxsize: int = _DEFAULT_MAXSIZE, ttl: int = _DEFAULT_TTL_SECONDS
    ):
        self._maxsize = maxsize
        self._ttl = ttl
        self._cache: OrderedDict[str, tuple[str, float]] = OrderedDict()

    def record(self, doc_id: str, rev: str) -> None:
        """Record a write-back so future echoes can be detected."""
        if doc_id in self._cache:
            self._cache.move_to_end(doc_id)
        self._cache[doc_id] = (rev, time.monotonic())
        while len(self._cache) > self._maxsize:
            self._cache.popitem(last=False)

    def is_echo(self, doc_id: str, rev: str) -> bool:
        """Return True if this change is our own echo (same _id and _rev).

        Also lazily evicts expired entries starting from the oldest.
        """
        self._evict_expired()
        entry = self._cache.get(doc_id)
        if entry is None:
            return False
        recorded_rev, _ = entry
        if recorded_rev == rev:
            log_event(
                logger,
                "debug",
                "EVENTING",
                "recursion guard: suppressing echo",
                doc_id=doc_id,
            )
            del self._cache[doc_id]
            return True
        return False

    def clear(self) -> None:
        """Reset the guard, discarding all tracked entries."""
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)

    def _evict_expired(self) -> None:
        """Remove entries older than TTL from the front of the OrderedDict."""
        cutoff = time.monotonic() - self._ttl
        while self._cache:
            _, (_, ts) = next(iter(self._cache.items()))
            if ts > cutoff:
                break
            self._cache.popitem(last=False)


def create_recursion_guard(cfg: dict) -> RecursionGuard | None:
    """Factory: build a RecursionGuard from a job's recursion_guard config dict.

    Returns None if not enabled.
    """
    if not cfg or not cfg.get("enabled", False):
        return None

    maxsize = cfg.get("max_tracked_docs", _DEFAULT_MAXSIZE)
    ttl = cfg.get("ttl_seconds", _DEFAULT_TTL_SECONDS)

    log_event(
        logger,
        "info",
        "EVENTING",
        "recursion guard enabled — tracking up to %d docs with %ds TTL"
        % (maxsize, ttl),
    )
    return RecursionGuard(maxsize=maxsize, ttl=ttl)
