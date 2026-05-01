"""
Attachment post-processing (Phase 3).

After Phase 2 uploads attachments to a destination, the post-processor
performs an action on the source document before it continues to the
RIGHT (output) stage of the pipeline.

Supported actions:
  - ``none``               — do nothing (default)
  - ``update_doc``         — PUT the doc with external URLs added
  - ``set_ttl``            — PUT the doc with ``_exp`` set
  - ``delete_doc``         — DELETE the document
  - ``delete_attachments`` — DELETE individual attachments from the doc
  - ``purge``              — POST to ``_purge`` on the admin port
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING
from urllib.parse import quote

import aiohttp

try:
    from icecream import ic
except ImportError:  # pragma: no cover
    ic = lambda *a, **kw: None  # noqa: E731

from pipeline.pipeline_logging import log_event
from rest.attachment_config import AttachmentConfig
from rest.attachment_upload import AttachmentUploadResult
from rest.changes_http import ClientHTTPError, RetryableHTTP

if TYPE_CHECKING:
    from main import MetricsCollector

logger = logging.getLogger("changes_worker")


# ---------------------------------------------------------------------------
# Post-processor
# ---------------------------------------------------------------------------


class AttachmentPostProcessor:
    """Runs a post-process action on the source document after upload."""

    def __init__(
        self,
        config: AttachmentConfig,
        metrics: MetricsCollector | None = None,
    ):
        self._config = config
        self._pp = config.post_process
        self._metrics = metrics

    # -- public entry point -------------------------------------------------

    async def post_process(
        self,
        doc: dict,
        uploaded: dict[str, AttachmentUploadResult],
        base_url: str,
        http: RetryableHTTP,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
    ) -> dict:
        """Execute the configured post-process action.

        Returns the (possibly modified) document.
        """
        action = self._pp.action
        if action == "none":
            return doc

        doc_id = doc.get("_id", doc.get("id", "<unknown>"))
        self._inc("attachments_post_process_total")

        try:
            if action == "update_doc":
                return await self._action_update_doc(
                    doc, uploaded, base_url, http, auth, headers
                )
            elif action == "set_ttl":
                return await self._action_set_ttl(doc, base_url, http, auth, headers)
            elif action == "delete_doc":
                return await self._action_delete_doc(doc, base_url, http, auth, headers)
            elif action == "delete_attachments":
                return await self._action_delete_attachments(
                    doc, uploaded, base_url, http, auth, headers
                )
            elif action == "purge":
                return await self._action_purge(doc, base_url, http, auth, headers)
            else:
                log_event(
                    logger,
                    "warn",
                    "ATTACHMENT",
                    "unknown post_process action: %s" % action,
                    doc_id=doc_id,
                )
                return doc
        except Exception as exc:
            self._inc("attachments_post_process_errors_total")
            if self._config.halt_on_failure:
                from rest.attachments import AttachmentError

                raise AttachmentError(
                    "post-process '%s' failed for doc %s: %s" % (action, doc_id, exc)
                ) from exc
            log_event(
                logger,
                "warn",
                "ATTACHMENT",
                "post-process '%s' error (continuing): %s" % (action, exc),
                doc_id=doc_id,
            )
            return doc

    # -- action: update_doc -------------------------------------------------

    async def _action_update_doc(
        self,
        doc: dict,
        uploaded: dict[str, AttachmentUploadResult],
        base_url: str,
        http: RetryableHTTP,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
    ) -> dict:
        doc_id = doc.get("_id", doc.get("id", "<unknown>"))
        rev = doc.get("_rev", doc.get("rev", ""))

        # Build the external attachments map
        ext = self._build_external_map(uploaded)

        body = dict(doc)
        body[self._pp.update_field] = ext
        if self._pp.remove_attachments_after_upload:
            body.pop("_attachments", None)

        for attempt in range(1, self._pp.max_conflict_retries + 1):
            ok, new_rev = await self._put_doc(
                doc_id,
                rev,
                body,
                base_url,
                http,
                auth,
                headers,
                pp_action="update_doc",
            )
            if ok:
                body["_rev"] = new_rev
                log_event(
                    logger,
                    "info",
                    "ATTACHMENT",
                    "post-process update_doc succeeded",
                    pp_action="update_doc",
                    doc_id=doc_id,
                    doc_rev=rev,
                    new_rev=new_rev,
                    attempt=attempt,
                )
                return body

            # _put_doc returns (False, "") for missing doc (already handled)
            if not new_rev:
                return doc

            # Conflict – re-fetch and retry
            refreshed, fresh_rev = await self._handle_conflict(
                doc_id, uploaded, base_url, http, auth, headers
            )
            if refreshed is None:
                return doc
            rev = fresh_rev
            body = dict(refreshed)
            body[self._pp.update_field] = ext
            if self._pp.remove_attachments_after_upload:
                body.pop("_attachments", None)

        self._inc("attachments_post_process_errors_total")
        log_event(
            logger,
            "warn",
            "ATTACHMENT",
            "post-process update_doc exhausted conflict retries",
            doc_id=doc_id,
        )
        return doc

    # -- action: set_ttl ----------------------------------------------------

    async def _action_set_ttl(
        self,
        doc: dict,
        base_url: str,
        http: RetryableHTTP,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
    ) -> dict:
        doc_id = doc.get("_id", doc.get("id", "<unknown>"))
        rev = doc.get("_rev", doc.get("rev", ""))

        body = dict(doc)
        body["_exp"] = int(time.time()) + self._pp.ttl_seconds

        for attempt in range(1, self._pp.max_conflict_retries + 1):
            ok, new_rev = await self._put_doc(
                doc_id,
                rev,
                body,
                base_url,
                http,
                auth,
                headers,
                pp_action="set_ttl",
            )
            if ok:
                body["_rev"] = new_rev
                log_event(
                    logger,
                    "info",
                    "ATTACHMENT",
                    "post-process set_ttl succeeded (_exp=%d)" % body["_exp"],
                    pp_action="set_ttl",
                    doc_id=doc_id,
                    doc_rev=rev,
                    new_rev=new_rev,
                    attempt=attempt,
                )
                return body

            if not new_rev:
                return doc

            # Conflict
            refreshed, fresh_rev = await self._handle_conflict(
                doc_id, {}, base_url, http, auth, headers
            )
            if refreshed is None:
                return doc
            rev = fresh_rev
            body = dict(refreshed)
            body["_exp"] = int(time.time()) + self._pp.ttl_seconds

        self._inc("attachments_post_process_errors_total")
        log_event(
            logger,
            "warn",
            "ATTACHMENT",
            "post-process set_ttl exhausted conflict retries",
            doc_id=doc_id,
        )
        return doc

    # -- action: delete_doc -------------------------------------------------

    async def _action_delete_doc(
        self,
        doc: dict,
        base_url: str,
        http: RetryableHTTP,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
    ) -> dict:
        doc_id = doc.get("_id", doc.get("id", "<unknown>"))
        rev = doc.get("_rev", doc.get("rev", ""))

        for attempt in range(1, self._pp.max_conflict_retries + 1):
            url = "%s/%s?rev=%s" % (
                base_url.rstrip("/"),
                quote(doc_id, safe=""),
                quote(rev, safe=""),
            )
            log_event(
                logger,
                "debug",
                "ATTACHMENT",
                "source DELETE begin",
                pp_action="delete_doc",
                doc_id=doc_id,
                doc_rev=rev,
                http_method="DELETE",
                url=url,
                attempt=attempt,
            )
            t0 = time.monotonic()
            try:
                resp = await http.request("DELETE", url, auth=auth, headers=headers)
                # Try to read response body to capture the new (tombstone) rev
                new_rev = rev
                try:
                    resp_body = await resp.json()
                    new_rev = resp_body.get("rev", rev)
                except Exception:
                    pass
                resp.release()
                elapsed_ms = (time.monotonic() - t0) * 1000.0
                # Receipt: log the new tombstone _rev as proof of deletion
                log_event(
                    logger,
                    "info",
                    "ATTACHMENT",
                    "source DELETE ok (receipt) — post-process delete_doc succeeded",
                    pp_action="delete_doc",
                    doc_id=doc_id,
                    doc_rev=rev,
                    new_rev=new_rev,
                    http_method="DELETE",
                    status=200,
                    elapsed_ms=round(elapsed_ms, 1),
                    attempt=attempt,
                )
                doc["_rev"] = new_rev
                return doc
            except ClientHTTPError as exc:
                elapsed_ms = (time.monotonic() - t0) * 1000.0
                if exc.status == 404:
                    log_event(
                        logger,
                        "warn",
                        "ATTACHMENT",
                        "source DELETE 404 not_found (doc missing)",
                        pp_action="delete_doc",
                        doc_id=doc_id,
                        doc_rev=rev,
                        http_method="DELETE",
                        status=404,
                        elapsed_ms=round(elapsed_ms, 1),
                        attempt=attempt,
                        error_detail=str(exc)[:200],
                    )
                    self._handle_missing_doc(doc_id, "delete_doc")
                    return doc
                if exc.status == 409:
                    self._inc("attachments_conflict_retries_total")
                    log_event(
                        logger,
                        "warn",
                        "ATTACHMENT",
                        "source DELETE 409 conflict (doc changed)",
                        pp_action="delete_doc",
                        doc_id=doc_id,
                        doc_rev=rev,
                        http_method="DELETE",
                        status=409,
                        elapsed_ms=round(elapsed_ms, 1),
                        attempt=attempt,
                    )
                    refreshed, fresh_rev = await self._handle_conflict(
                        doc_id, {}, base_url, http, auth, headers
                    )
                    if refreshed is None:
                        return doc
                    rev = fresh_rev
                    continue
                if exc.status in (401, 403):
                    log_event(
                        logger,
                        "error",
                        "ATTACHMENT",
                        "source DELETE unauthorized",
                        pp_action="delete_doc",
                        doc_id=doc_id,
                        doc_rev=rev,
                        http_method="DELETE",
                        status=exc.status,
                        elapsed_ms=round(elapsed_ms, 1),
                        attempt=attempt,
                        error_detail=str(exc)[:200],
                    )
                    raise
                log_event(
                    logger,
                    "error",
                    "ATTACHMENT",
                    "source DELETE http error",
                    pp_action="delete_doc",
                    doc_id=doc_id,
                    doc_rev=rev,
                    http_method="DELETE",
                    status=exc.status,
                    elapsed_ms=round(elapsed_ms, 1),
                    attempt=attempt,
                    error_class=type(exc).__name__,
                    error_detail=str(exc)[:200],
                )
                raise
            except (asyncio.TimeoutError, ConnectionError) as exc:
                elapsed_ms = (time.monotonic() - t0) * 1000.0
                log_event(
                    logger,
                    "error",
                    "ATTACHMENT",
                    "source DELETE timeout/connection error",
                    pp_action="delete_doc",
                    doc_id=doc_id,
                    doc_rev=rev,
                    http_method="DELETE",
                    elapsed_ms=round(elapsed_ms, 1),
                    attempt=attempt,
                    error_class=type(exc).__name__,
                    error_detail=str(exc)[:200],
                )
                raise

        self._inc("attachments_post_process_errors_total")
        log_event(
            logger,
            "warn",
            "ATTACHMENT",
            "post-process delete_doc exhausted conflict retries",
            pp_action="delete_doc",
            doc_id=doc_id,
            doc_rev=rev,
        )
        return doc

    # -- action: delete_attachments -----------------------------------------

    async def _action_delete_attachments(
        self,
        doc: dict,
        uploaded: dict[str, AttachmentUploadResult],
        base_url: str,
        http: RetryableHTTP,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
    ) -> dict:
        doc_id = doc.get("_id", doc.get("id", "<unknown>"))
        current_rev = doc.get("_rev", doc.get("rev", ""))

        for name in uploaded:
            deleted = False
            prev_rev = current_rev
            for attempt in range(1, self._pp.max_conflict_retries + 1):
                url = "%s/%s/%s?rev=%s" % (
                    base_url.rstrip("/"),
                    quote(doc_id, safe=""),
                    quote(name, safe=""),
                    quote(current_rev, safe=""),
                )
                log_event(
                    logger,
                    "debug",
                    "ATTACHMENT",
                    "source DELETE attachment begin: %s" % name,
                    pp_action="delete_attachments",
                    doc_id=doc_id,
                    doc_rev=current_rev,
                    http_method="DELETE",
                    url=url,
                    attempt=attempt,
                )
                t0 = time.monotonic()
                try:
                    resp = await http.request("DELETE", url, auth=auth, headers=headers)
                    resp_body = await resp.json()
                    new_rev = resp_body.get("rev", current_rev)
                    resp.release()
                    elapsed_ms = (time.monotonic() - t0) * 1000.0
                    # Receipt: log the new _rev returned after attachment removal
                    log_event(
                        logger,
                        "info",
                        "ATTACHMENT",
                        "source DELETE attachment ok (receipt): %s" % name,
                        pp_action="delete_attachments",
                        doc_id=doc_id,
                        doc_rev=prev_rev,
                        new_rev=new_rev,
                        http_method="DELETE",
                        status=200,
                        elapsed_ms=round(elapsed_ms, 1),
                        attempt=attempt,
                    )
                    current_rev = new_rev
                    deleted = True
                    break
                except ClientHTTPError as exc:
                    elapsed_ms = (time.monotonic() - t0) * 1000.0
                    if exc.status == 404:
                        log_event(
                            logger,
                            "debug",
                            "ATTACHMENT",
                            "attachment already missing: %s" % name,
                            pp_action="delete_attachments",
                            doc_id=doc_id,
                            doc_rev=current_rev,
                            http_method="DELETE",
                            status=404,
                            elapsed_ms=round(elapsed_ms, 1),
                            attempt=attempt,
                        )
                        deleted = True
                        break
                    if exc.status == 409:
                        self._inc("attachments_conflict_retries_total")
                        log_event(
                            logger,
                            "warn",
                            "ATTACHMENT",
                            "source DELETE attachment 409 conflict: %s" % name,
                            pp_action="delete_attachments",
                            doc_id=doc_id,
                            doc_rev=current_rev,
                            http_method="DELETE",
                            status=409,
                            elapsed_ms=round(elapsed_ms, 1),
                            attempt=attempt,
                        )
                        refreshed, fresh_rev = await self._handle_conflict(
                            doc_id, uploaded, base_url, http, auth, headers
                        )
                        if refreshed is None:
                            break
                        current_rev = fresh_rev
                        continue
                    if exc.status in (401, 403):
                        log_event(
                            logger,
                            "error",
                            "ATTACHMENT",
                            "source DELETE attachment unauthorized: %s" % name,
                            pp_action="delete_attachments",
                            doc_id=doc_id,
                            doc_rev=current_rev,
                            http_method="DELETE",
                            status=exc.status,
                            elapsed_ms=round(elapsed_ms, 1),
                            attempt=attempt,
                            error_detail=str(exc)[:200],
                        )
                        raise
                    log_event(
                        logger,
                        "error",
                        "ATTACHMENT",
                        "source DELETE attachment http error: %s" % name,
                        pp_action="delete_attachments",
                        doc_id=doc_id,
                        doc_rev=current_rev,
                        http_method="DELETE",
                        status=exc.status,
                        elapsed_ms=round(elapsed_ms, 1),
                        attempt=attempt,
                        error_class=type(exc).__name__,
                        error_detail=str(exc)[:200],
                    )
                    raise
                except (asyncio.TimeoutError, ConnectionError) as exc:
                    elapsed_ms = (time.monotonic() - t0) * 1000.0
                    log_event(
                        logger,
                        "error",
                        "ATTACHMENT",
                        "source DELETE attachment timeout/connection error: %s" % name,
                        pp_action="delete_attachments",
                        doc_id=doc_id,
                        doc_rev=current_rev,
                        http_method="DELETE",
                        elapsed_ms=round(elapsed_ms, 1),
                        attempt=attempt,
                        error_class=type(exc).__name__,
                        error_detail=str(exc)[:200],
                    )
                    raise

            if not deleted:
                self._inc("attachments_post_process_errors_total")
                log_event(
                    logger,
                    "warn",
                    "ATTACHMENT",
                    "failed to delete attachment %s after retries" % name,
                    pp_action="delete_attachments",
                    doc_id=doc_id,
                    doc_rev=current_rev,
                )

        log_event(
            logger,
            "info",
            "ATTACHMENT",
            "post-process delete_attachments completed",
            pp_action="delete_attachments",
            doc_id=doc_id,
            new_rev=current_rev,
        )
        doc["_rev"] = current_rev
        return doc

    # -- action: purge ------------------------------------------------------

    async def _action_purge(
        self,
        doc: dict,
        base_url: str,
        http: RetryableHTTP,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
    ) -> dict:
        doc_id = doc.get("_id", doc.get("id", "<unknown>"))

        admin_url = self._pp.admin_url
        if not admin_url:
            raise RuntimeError("post_process.admin_url is required for purge action")

        # Extract keyspace from base_url (last path component)
        keyspace = base_url.rstrip("/").rsplit("/", 1)[-1]
        purge_url = "%s/%s/_purge" % (
            admin_url.rstrip("/"),
            quote(keyspace, safe=""),
        )

        admin_auth = None
        if self._pp.admin_auth.username:
            admin_auth = aiohttp.BasicAuth(
                self._pp.admin_auth.username,
                self._pp.admin_auth.password,
            )

        req_headers = dict(headers)
        req_headers["Content-Type"] = "application/json"
        purge_body = json.dumps({doc_id: ["*"]})

        log_event(
            logger,
            "debug",
            "ATTACHMENT",
            "source POST _purge begin",
            pp_action="purge",
            doc_id=doc_id,
            http_method="POST",
            url=purge_url,
        )
        t0 = time.monotonic()
        try:
            resp = await http.request(
                "POST",
                purge_url,
                data=purge_body,
                auth=admin_auth,
                headers=req_headers,
            )
            resp.release()
            elapsed_ms = (time.monotonic() - t0) * 1000.0
        except ClientHTTPError as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            if exc.status in (401, 403):
                log_event(
                    logger,
                    "error",
                    "ATTACHMENT",
                    "source POST _purge unauthorized",
                    pp_action="purge",
                    doc_id=doc_id,
                    http_method="POST",
                    status=exc.status,
                    elapsed_ms=round(elapsed_ms, 1),
                    error_detail=str(exc)[:200],
                )
            elif exc.status == 404:
                log_event(
                    logger,
                    "warn",
                    "ATTACHMENT",
                    "source POST _purge 404 not_found",
                    pp_action="purge",
                    doc_id=doc_id,
                    http_method="POST",
                    status=404,
                    elapsed_ms=round(elapsed_ms, 1),
                )
            else:
                log_event(
                    logger,
                    "error",
                    "ATTACHMENT",
                    "source POST _purge http error",
                    pp_action="purge",
                    doc_id=doc_id,
                    http_method="POST",
                    status=exc.status,
                    elapsed_ms=round(elapsed_ms, 1),
                    error_class=type(exc).__name__,
                    error_detail=str(exc)[:200],
                )
            raise
        except (asyncio.TimeoutError, ConnectionError) as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            log_event(
                logger,
                "error",
                "ATTACHMENT",
                "source POST _purge timeout/connection error",
                pp_action="purge",
                doc_id=doc_id,
                http_method="POST",
                elapsed_ms=round(elapsed_ms, 1),
                error_class=type(exc).__name__,
                error_detail=str(exc)[:200],
            )
            raise

        log_event(
            logger,
            "info",
            "ATTACHMENT",
            "source POST _purge ok — post-process purge succeeded",
            pp_action="purge",
            doc_id=doc_id,
            http_method="POST",
            status=200,
            elapsed_ms=round(elapsed_ms, 1),
        )
        return doc

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _build_external_map(
        uploaded: dict[str, AttachmentUploadResult],
    ) -> dict[str, dict]:
        ext: dict[str, dict] = {}
        for name, result in uploaded.items():
            ext[name] = {
                "url": result.access_url or result.location,
                "content_type": result.content_type,
                "length": result.length,
                "digest": result.digest,
                "uploaded_at": result.uploaded_at,
            }
        return ext

    async def _put_doc(
        self,
        doc_id: str,
        rev: str,
        body: dict,
        base_url: str,
        http: RetryableHTTP,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
        pp_action: str = "put_doc",
    ) -> tuple[bool, str]:
        """PUT a document back to the source.

        Returns ``(True, new_rev)`` on success, ``(False, "conflict")``
        on 409, or ``(False, "")`` when the doc is missing and
        ``on_doc_missing`` is ``"skip"``.

        Raises on other HTTP errors or if ``on_doc_missing`` is
        ``"fail"`` and the doc is 404.

        Every outcome is logged for audit/debug per GUIDE_LOGGING.md:
        success records the new ``_rev`` returned by the source as a
        receipt; failures record the HTTP status and error class.
        """
        url = "%s/%s?rev=%s" % (
            base_url.rstrip("/"),
            quote(doc_id, safe=""),
            quote(rev, safe=""),
        )
        req_headers = dict(headers)
        req_headers["Content-Type"] = "application/json"

        log_event(
            logger,
            "debug",
            "ATTACHMENT",
            "source PUT begin",
            pp_action=pp_action,
            doc_id=doc_id,
            doc_rev=rev,
            http_method="PUT",
            url=url,
        )

        t0 = time.monotonic()
        try:
            resp = await http.request(
                "PUT", url, data=json.dumps(body), auth=auth, headers=req_headers
            )
            resp_body = await resp.json()
            resp.release()
            new_rev = resp_body.get("rev", rev)
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            # Receipt: log the new _rev returned by the source on success
            log_event(
                logger,
                "info",
                "ATTACHMENT",
                "source PUT ok (receipt)",
                pp_action=pp_action,
                doc_id=doc_id,
                doc_rev=rev,
                new_rev=new_rev,
                http_method="PUT",
                status=200,
                elapsed_ms=round(elapsed_ms, 1),
            )
            return True, new_rev
        except ClientHTTPError as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            if exc.status == 404:
                log_event(
                    logger,
                    "warn",
                    "ATTACHMENT",
                    "source PUT 404 not_found (doc missing)",
                    pp_action=pp_action,
                    doc_id=doc_id,
                    doc_rev=rev,
                    http_method="PUT",
                    status=404,
                    elapsed_ms=round(elapsed_ms, 1),
                    error_detail=str(exc)[:200],
                )
                self._handle_missing_doc(doc_id, "put_doc")
                return False, ""
            if exc.status == 409:
                self._inc("attachments_conflict_retries_total")
                log_event(
                    logger,
                    "warn",
                    "ATTACHMENT",
                    "source PUT 409 conflict (doc changed)",
                    pp_action=pp_action,
                    doc_id=doc_id,
                    doc_rev=rev,
                    http_method="PUT",
                    status=409,
                    elapsed_ms=round(elapsed_ms, 1),
                )
                return False, "conflict"
            if exc.status in (401, 403):
                log_event(
                    logger,
                    "error",
                    "ATTACHMENT",
                    "source PUT unauthorized",
                    pp_action=pp_action,
                    doc_id=doc_id,
                    doc_rev=rev,
                    http_method="PUT",
                    status=exc.status,
                    elapsed_ms=round(elapsed_ms, 1),
                    error_detail=str(exc)[:200],
                )
                raise
            log_event(
                logger,
                "error",
                "ATTACHMENT",
                "source PUT http error",
                pp_action=pp_action,
                doc_id=doc_id,
                doc_rev=rev,
                http_method="PUT",
                status=exc.status,
                elapsed_ms=round(elapsed_ms, 1),
                error_class=type(exc).__name__,
                error_detail=str(exc)[:200],
            )
            raise
        except (asyncio.TimeoutError, ConnectionError) as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            log_event(
                logger,
                "error",
                "ATTACHMENT",
                "source PUT timeout/connection error",
                pp_action=pp_action,
                doc_id=doc_id,
                doc_rev=rev,
                http_method="PUT",
                elapsed_ms=round(elapsed_ms, 1),
                error_class=type(exc).__name__,
                error_detail=str(exc)[:200],
            )
            raise
        except Exception as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            log_event(
                logger,
                "error",
                "ATTACHMENT",
                "source PUT unexpected error",
                pp_action=pp_action,
                doc_id=doc_id,
                doc_rev=rev,
                http_method="PUT",
                elapsed_ms=round(elapsed_ms, 1),
                error_class=type(exc).__name__,
                error_detail=str(exc)[:200],
            )
            raise

    async def _handle_conflict(
        self,
        doc_id: str,
        uploaded: dict[str, AttachmentUploadResult],
        base_url: str,
        http: RetryableHTTP,
        auth: aiohttp.BasicAuth | None,
        headers: dict,
    ) -> tuple[dict | None, str]:
        """Re-fetch a doc after a 409 conflict.

        Returns ``(doc, rev)`` on success.  If the doc is gone or
        attachments have disappeared (stale), returns ``(None, "")``.
        """
        url = "%s/%s" % (base_url.rstrip("/"), quote(doc_id, safe=""))

        try:
            resp = await http.request("GET", url, auth=auth, headers=headers)
            refreshed = await resp.json()
            resp.release()
        except ClientHTTPError as exc:
            if exc.status == 404:
                self._handle_missing_doc(doc_id, "conflict_refetch")
                return None, ""
            log_event(
                logger,
                "warn",
                "ATTACHMENT",
                "conflict re-fetch returned %d" % exc.status,
                doc_id=doc_id,
            )
            return None, ""
        except Exception as exc:
            log_event(
                logger,
                "warn",
                "ATTACHMENT",
                "conflict re-fetch failed: %s" % exc,
                doc_id=doc_id,
            )
            return None, ""

        fresh_rev = refreshed.get("_rev", "")

        # Check that uploaded attachments still exist as stubs
        if uploaded:
            current_stubs = refreshed.get("_attachments", {})
            for name in uploaded:
                if name not in current_stubs:
                    self._inc("attachments_stale_total")
                    log_event(
                        logger,
                        "warn",
                        "ATTACHMENT",
                        "attachment %s no longer present after conflict" % name,
                        doc_id=doc_id,
                    )
                    if self._pp.cleanup_orphaned_uploads:
                        self._inc("attachments_orphaned_uploads_total")
                    return None, ""

        log_event(
            logger,
            "debug",
            "ATTACHMENT",
            "conflict re-fetch ok, new rev=%s" % fresh_rev,
            doc_id=doc_id,
        )
        return refreshed, fresh_rev

    def _handle_missing_doc(self, doc_id: str, action: str) -> None:
        """Handle a 404 for a document based on ``on_doc_missing`` config."""
        if self._pp.on_doc_missing == "fail":
            from rest.attachments import AttachmentError

            raise AttachmentError(
                "document %s not found during post-process %s" % (doc_id, action)
            )

        # "skip" (default)
        self._inc("attachments_post_process_skipped_total")
        log_event(
            logger,
            "warn",
            "ATTACHMENT",
            "document not found during post-process %s (skipping)" % action,
            doc_id=doc_id,
        )

    def _inc(self, counter: str, amount: int = 1) -> None:
        if self._metrics:
            self._metrics.inc(counter, amount)
