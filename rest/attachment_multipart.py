"""
Multipart/related response parsing for attachment fetching (Phase 4).

Parses ``multipart/related`` HTTP responses from Sync Gateway and CouchDB
when fetching documents with ``?attachments=true&Accept=multipart/related``.

The first MIME part is always the JSON document body.  Subsequent parts are
raw attachment bytes whose names are resolved via Content-Disposition headers
(standard) or by positional matching against ``_attachments`` metadata with
``"follows": true`` (CouchDB fallback).
"""

from __future__ import annotations

import json
import logging
import re

import aiohttp

try:
    from icecream import ic
except ImportError:  # pragma: no cover
    ic = lambda *a, **kw: None  # noqa: E731

from pipeline.pipeline_logging import log_event

logger = logging.getLogger("changes_worker")


# ---------------------------------------------------------------------------
# Error
# ---------------------------------------------------------------------------


class MultipartParseError(Exception):
    """Raised when a multipart/related response cannot be parsed."""

    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_follows_names(doc: dict) -> list[str]:
    """Return ordered list of attachment names with ``"follows": true``."""
    attachments = doc.get("_attachments") or {}
    return [name for name, meta in attachments.items() if meta.get("follows")]


_FILENAME_RE = re.compile(r'filename="([^"]+)"')


def _filename_from_headers(part: aiohttp.BodyPartReader) -> str | None:
    """Extract filename from Content-Disposition header."""
    # Try aiohttp's built-in property first.
    fname = getattr(part, "filename", None)
    if fname:
        return fname
    # Fall back to manual parsing.
    cd = part.headers.get("Content-Disposition", "")
    m = _FILENAME_RE.search(cd)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------


async def parse_multipart_response(
    resp: aiohttp.ClientResponse,
    expected_names: set[str] | None = None,
    src: str = "sync_gateway",
) -> tuple[dict, dict[str, bytes]]:
    """Parse a multipart/related response into (doc_json, attachments_dict).

    Parameters
    ----------
    resp : aiohttp.ClientResponse
        The HTTP response with Content-Type: multipart/related.
    expected_names : set[str] | None
        If provided, only return attachments whose names are in this set.
        Others are read but discarded.
    src : str
        Source type ("sync_gateway", "couchdb", etc.) to control parsing mode.

    Returns
    -------
    (doc, attachments) where:
        doc : dict — the JSON document (first MIME part)
        attachments : dict[str, bytes] — mapping of attachment_name → raw bytes

    Raises
    ------
    MultipartParseError — if the response cannot be parsed.
    """
    try:
        reader = aiohttp.MultipartReader.from_response(resp)
    except Exception as exc:
        raise MultipartParseError(
            "failed to create multipart reader: %s" % exc
        ) from exc

    # ------------------------------------------------------------------
    # 1. First part — JSON document
    # ------------------------------------------------------------------
    first_part = await reader.next()
    if first_part is None:
        raise MultipartParseError("multipart response has no parts")

    first_ct = first_part.headers.get("Content-Type", "")
    if "json" not in first_ct:
        raise MultipartParseError(
            "first multipart part is not JSON (Content-Type: %s)" % first_ct
        )

    try:
        doc_bytes = await first_part.read()
        doc = json.loads(doc_bytes)
    except (json.JSONDecodeError, Exception) as exc:
        raise MultipartParseError(
            "failed to parse JSON document part: %s" % exc
        ) from exc

    doc_id = doc.get("_id", doc.get("id", "<unknown>"))

    # Build positional name list for CouchDB fallback (Strategy B).
    follows_names = _extract_follows_names(doc)

    log_event(
        logger,
        "debug",
        "ATTACHMENT",
        "multipart: parsed doc, expecting %d follows attachment(s)"
        % len(follows_names),
        doc_id=doc_id,
        src=src,
    )

    # ------------------------------------------------------------------
    # 2. Subsequent parts — attachment binary data
    # ------------------------------------------------------------------
    attachments: dict[str, bytes] = {}
    part_index = 0

    while True:
        part = await reader.next()
        if part is None:
            break

        # --- Resolve attachment name ---
        # Strategy A: Content-Disposition filename (SG / newer CouchDB).
        name = _filename_from_headers(part)

        # Strategy B: positional fallback (CouchDB).
        if not name and part_index < len(follows_names):
            name = follows_names[part_index]
            log_event(
                logger,
                "debug",
                "ATTACHMENT",
                "multipart: using positional name for part %d -> %s"
                % (part_index, name),
                doc_id=doc_id,
                src=src,
            )

        part_index += 1

        if not name:
            log_event(
                logger,
                "warn",
                "ATTACHMENT",
                "multipart: could not determine name for part %d, skipping"
                % (part_index - 1),
                doc_id=doc_id,
                src=src,
            )
            await part.read()  # drain the part
            continue

        # Filter check
        if expected_names is not None and name not in expected_names:
            log_event(
                logger,
                "debug",
                "ATTACHMENT",
                "multipart: discarding unwanted attachment %s" % name,
                doc_id=doc_id,
                src=src,
            )
            await part.read()  # drain the part
            continue

        data = await part.read()
        attachments[name] = data

    # ------------------------------------------------------------------
    # 3. Sanity check
    # ------------------------------------------------------------------
    if follows_names and part_index != len(follows_names):
        log_event(
            logger,
            "warn",
            "ATTACHMENT",
            "multipart: expected %d attachment part(s) but received %d"
            % (len(follows_names), part_index),
            doc_id=doc_id,
            src=src,
        )

    log_event(
        logger,
        "debug",
        "ATTACHMENT",
        "multipart: returning %d attachment(s)" % len(attachments),
        doc_id=doc_id,
        src=src,
    )

    return doc, attachments
