"""
HTTP output module for changes_worker.

Handles forwarding processed documents to a downstream REST endpoint
via PUT / POST / DELETE, with retry, response-time tracking, dead-letter
queue, and multiple serialization formats.
"""

import asyncio
import json
import logging
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET
from collections import deque
from pathlib import Path

import aiohttp

try:
    from icecream import ic
except ImportError:  # pragma: no cover
    ic = lambda *a, **kw: None  # noqa: E731

# Optional serialization libraries – imported lazily so the worker starts
# even if they are not installed (only errors if actually selected).
try:
    import msgpack  # type: ignore
except ImportError:
    msgpack = None

try:
    import cbor2  # type: ignore
except ImportError:
    cbor2 = None

try:
    import bson  # type: ignore
except ImportError:
    bson = None

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None

from pipeline.pipeline_logging import log_event, infer_operation

logger = logging.getLogger("changes_worker")


# ---------------------------------------------------------------------------
# HTTP status code classification
# ---------------------------------------------------------------------------
# Transient statuses: may succeed on retry (server busy, rate limit, timeout).
# Permanent statuses: same request will always fail (bad data, auth, conflict).

# 4xx codes that are TRANSIENT — retry with backoff
_TRANSIENT_4XX = frozenset(
    {
        404,  # Not Found — resource/endpoint may come back
        408,  # Request Timeout — server timed out waiting
        421,  # Misdirected Request — routing issue, retry may hit correct server
        423,  # Locked (WebDAV) — resource temporarily locked
        425,  # Too Early — server unwilling to risk replay, try again
        429,  # Too Many Requests — rate limited, retry after backoff
    }
)

# 4xx codes that are PERMANENT — straight to DLQ, will never succeed with same data
_PERMANENT_4XX = frozenset(
    {
        400,  # Bad Request — malformed payload
        401,  # Unauthorized — auth failure
        402,  # Payment Required
        403,  # Forbidden — permission denied
        405,  # Method Not Allowed — wrong HTTP method
        406,  # Not Acceptable — content negotiation failure
        407,  # Proxy Authentication Required
        409,  # Conflict — data conflict, same data won't resolve it
        410,  # Gone — resource permanently removed
        411,  # Length Required — request config issue
        412,  # Precondition Failed — conditional request failed
        413,  # Payload Too Large — doc too big for endpoint
        414,  # URI Too Long
        415,  # Unsupported Media Type — wrong content type
        416,  # Range Not Satisfiable
        417,  # Expectation Failed
        422,  # Unprocessable Entity — semantic errors in payload
        426,  # Upgrade Required — protocol mismatch
        428,  # Precondition Required
        431,  # Request Header Fields Too Large
        451,  # Unavailable For Legal Reasons
    }
)

# 5xx codes that are TRANSIENT — retry with backoff (default behavior)
_TRANSIENT_5XX = frozenset(
    {
        500,  # Internal Server Error — generic, may recover
        502,  # Bad Gateway — upstream issue
        503,  # Service Unavailable — overloaded or maintenance
        504,  # Gateway Timeout — upstream timeout
        507,  # Insufficient Storage (WebDAV) — may free up
    }
)

# 5xx codes that are PERMANENT — server config/capability issue
_PERMANENT_5XX = frozenset(
    {
        501,  # Not Implemented — server doesn't support this method
        505,  # HTTP Version Not Supported
        506,  # Variant Also Negotiates — server config error
        508,  # Loop Detected (WebDAV) — circular reference
        510,  # Not Extended
        511,  # Network Authentication Required — captive portal
    }
)


def classify_http_status(status: int) -> tuple[str, bool]:
    """Classify an HTTP status code.

    Returns (error_class, is_transient) where:
      - error_class: machine-readable classification string
      - is_transient: True if the request may succeed on retry

    Used by both the RetryableHTTP request loop and the OutputForwarder
    to decide retry-vs-DLQ behavior.
    """
    if 200 <= status < 300:
        return ("success", False)
    if 300 <= status < 400:
        return ("redirect", False)  # handled separately by follow_redirects
    if status in _TRANSIENT_4XX:
        if status == 429:
            return ("rate_limited", True)
        if status == 408:
            return ("client_timeout", True)
        if status == 404:
            return ("not_found", True)
        if status == 423:
            return ("locked", True)
        return (f"client_transient:{status}", True)
    if status in _PERMANENT_4XX:
        if status in (401, 403, 407):
            return ("auth_failure", False)
        if status == 409:
            return ("conflict", False)
        if status == 410:
            return ("gone", False)
        if status in (400, 422):
            return ("bad_request", False)
        if status in (405, 415):
            return ("method_or_media", False)
        if status == 413:
            return ("payload_too_large", False)
        return (f"client_error:{status}", False)
    if 400 <= status < 500:
        # Unknown 4xx — assume permanent
        return (f"client_error:{status}", False)
    if status in _TRANSIENT_5XX:
        if status == 503:
            return ("service_unavailable", True)
        if status == 502:
            return ("bad_gateway", True)
        if status == 504:
            return ("gateway_timeout", True)
        return (f"server_transient:{status}", True)
    if status in _PERMANENT_5XX:
        return (f"server_permanent:{status}", False)
    if 500 <= status < 600:
        # Unknown 5xx — assume transient (server may recover)
        return (f"server_error:{status}", True)
    return (f"unknown:{status}", False)


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

CONTENT_TYPES: dict[str, str] = {
    "json": "application/json",
    "xml": "application/xml",
    "form": "application/x-www-form-urlencoded",
    "msgpack": "application/msgpack",
    "cbor": "application/cbor",
    "bson": "application/bson",
    "yaml": "application/yaml",
}

VALID_OUTPUT_FORMATS = tuple(CONTENT_TYPES.keys())


def _dict_to_xml(doc: dict, root_tag: str = "doc") -> bytes:
    """Convert a flat-ish dict to XML bytes."""
    root = ET.Element(root_tag)
    _dict_to_xml_elements(root, doc)
    return ET.tostring(root, encoding="unicode", xml_declaration=True).encode("utf-8")


def _dict_to_xml_elements(parent: ET.Element, data) -> None:
    if isinstance(data, dict):
        for key, val in data.items():
            key_str = str(key).lstrip("_")
            child = ET.SubElement(parent, key_str if key_str else "item")
            _dict_to_xml_elements(child, val)
    elif isinstance(data, (list, tuple)):
        for item in data:
            child = ET.SubElement(parent, "item")
            _dict_to_xml_elements(child, item)
    else:
        parent.text = str(data) if data is not None else ""


def _flatten_dict(d: dict, prefix: str = "") -> dict[str, str]:
    """Flatten nested dict for form-encoding: {"a": {"b": 1}} → {"a.b": "1"}"""
    items: dict[str, str] = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            items.update(_flatten_dict(v, key))
        elif isinstance(v, (list, tuple)):
            items[key] = json.dumps(v, default=str)
        else:
            items[key] = str(v) if v is not None else ""
    return items


def serialize_doc(doc: dict, fmt: str) -> tuple[bytes | str, str]:
    """
    Serialize a document dict to the requested format.

    Returns (body_bytes_or_str, content_type).
    """
    ic("serialize_doc", fmt, list(doc.keys()) if doc else None)

    if fmt not in CONTENT_TYPES:
        log_event(
            logger,
            "error",
            "OUTPUT",
            "unknown serialization format",
            error_detail=f"output_format={fmt}, valid={VALID_OUTPUT_FORMATS}",
        )
        raise ValueError(f"Unknown output_format: {fmt}")

    try:
        if fmt == "json":
            return json.dumps(doc, default=str), CONTENT_TYPES["json"]

        if fmt == "xml":
            return _dict_to_xml(doc), CONTENT_TYPES["xml"]

        if fmt == "form":
            flat = _flatten_dict(doc)
            return urllib.parse.urlencode(flat), CONTENT_TYPES["form"]

        if fmt == "msgpack":
            if msgpack is None:
                raise RuntimeError(
                    "msgpack library not installed – pip install msgpack"
                )
            return msgpack.packb(doc, default=str), CONTENT_TYPES["msgpack"]

        if fmt == "cbor":
            if cbor2 is None:
                raise RuntimeError("cbor2 library not installed – pip install cbor2")
            return cbor2.dumps(doc), CONTENT_TYPES["cbor"]

        if fmt == "bson":
            if bson is None:
                raise RuntimeError(
                    "bson library not installed – pip install pymongo (provides bson)"
                )
            return bson.BSON.encode(doc), CONTENT_TYPES["bson"]

        if fmt == "yaml":
            if yaml is None:
                raise RuntimeError("pyyaml library not installed – pip install pyyaml")
            return yaml.dump(doc, default_flow_style=False), CONTENT_TYPES["yaml"]

    except RuntimeError:
        raise  # re-raise missing-library errors as-is
    except (TypeError, ValueError, OverflowError) as exc:
        doc_id = doc.get("_id", doc.get("id", "unknown"))
        log_event(
            logger,
            "error",
            "OUTPUT",
            "serialization failed",
            doc_id=doc_id,
            error_detail=f"{type(exc).__name__}: {exc}",
        )
        raise

    # Unreachable, but satisfies type-checkers
    raise ValueError(f"Unknown output_format: {fmt}")  # pragma: no cover


def check_serialization_library(out_fmt: str) -> tuple[str, str] | None:
    """
    Check if the required serialization library is installed for the given format.

    Returns (format_name, pip_name) tuple if the library is MISSING, or None if OK.
    """
    _lib_check: dict[str, tuple] = {
        "msgpack": (msgpack, "msgpack"),
        "cbor": (cbor2, "cbor2"),
        "bson": (bson, "pymongo"),
        "yaml": (yaml, "pyyaml"),
    }
    if out_fmt in _lib_check:
        mod, pip_name = _lib_check[out_fmt]
        if mod is None:
            return (out_fmt, pip_name)
    return None


# ---------------------------------------------------------------------------
# Output / forwarding
# ---------------------------------------------------------------------------


class OutputEndpointDown(Exception):
    """Raised when the output target is unreachable and halt_on_failure is set."""


class OutputForwarder:
    """
    Manages sending processed docs to the consumer endpoint.

    When mode=http:
      - Has its own RetryableHTTP with output-specific retry settings
      - Tracks per-request response times (min / max / avg / count)
      - On non-retryable failure (4xx) or exhausted retries (5xx):
          * If halt_on_failure=true → raises OutputEndpointDown so the
            main loop stops processing and does NOT advance the checkpoint
          * If halt_on_failure=false → logs the error and continues
      - Handles 3xx as non-retryable errors
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        out_cfg: dict,
        dry_run: bool,
        metrics=None,
        build_basic_auth_fn=None,
        build_auth_headers_fn=None,
        retryable_http_cls=None,
    ):
        self._mode = out_cfg.get("mode", "http")
        self._target_url = out_cfg.get("target_url", "").rstrip("/")
        self._dry_run = dry_run
        self._halt_on_failure = out_cfg.get("halt_on_failure", True)
        self._data_error_action = out_cfg.get("data_error_action", "dlq")
        ic("OutputForwarder init", self._mode, self._target_url, self._dry_run)
        self._log_response_times = out_cfg.get("log_response_times", True)
        self._output_format = out_cfg.get("output_format", "json")
        self._url_template = out_cfg.get("url_template", "{target_url}/{doc_id}")
        self._write_method = out_cfg.get("write_method", "PUT").upper()
        self._delete_method = out_cfg.get("delete_method", "DELETE").upper()
        self._send_delete_body = out_cfg.get("send_delete_body", False)
        self._request_timeout = out_cfg.get("request_timeout_seconds", 30)
        self._follow_redirects = out_cfg.get("follow_redirects", False)

        # §3.14: retry_on_conflict config
        self._retry_on_conflict = out_cfg.get("retry_on_conflict", False)

        # Flags for first-occurrence logging of permanent errors
        self._output_auth_failure_warned = False
        self._media_type_warned = False
        self._permanent_5xx_warned: dict[int, bool] = {}  # Track per-status
        self._ssl_failure_logged = False

        self._ssl_ctx = None
        if out_cfg.get("accept_self_signed_certs", False):
            import ssl as _ssl

            self._ssl_ctx = _ssl.create_default_context()
            self._ssl_ctx.check_hostname = False
            self._ssl_ctx.verify_mode = _ssl.CERT_NONE

        hc = out_cfg.get("health_check", {})
        self._hc_enabled = hc.get("enabled", False)
        self._hc_interval = hc.get("interval_seconds", 30)
        self._hc_url = hc.get("url", "") or self._target_url
        self._hc_method = hc.get("method", "GET").upper()
        self._hc_timeout = hc.get("timeout_seconds", 5)
        self._hc_task: asyncio.Task | None = None

        self._metrics = metrics

        # Auth for the output endpoint
        _build_basic = build_basic_auth_fn or _default_build_basic_auth
        _build_headers = build_auth_headers_fn or _default_build_auth_headers
        self._auth = _build_basic(out_cfg.get("target_auth", {}))
        self._headers = _build_headers(out_cfg.get("target_auth", {}))

        # Extra request options from config (query params, custom headers)
        req_opts = out_cfg.get("request_options", {})
        self._extra_params = req_opts.get("params", {})
        self._extra_headers = req_opts.get("headers", {})

        # Output-specific retry (separate from the gateway retry)
        # Default retry_on_status covers transient 5xx; transient 4xx (404,
        # 408, 429, etc.) are handled automatically by classify_http_status()
        # inside RetryableHTTP.request() regardless of this list.
        out_retry = out_cfg.get(
            "retry",
            {
                "max_retries": 3,
                "backoff_base_seconds": 1,
                "backoff_max_seconds": 30,
                "retry_on_status": [500, 502, 503, 504, 507],
            },
        )
        _http_cls = retryable_http_cls or _RetryableHTTPLazy
        self._http = _http_cls(session, out_retry) if self._mode == "http" else None

        # Response time tracking
        self._resp_times: deque[float] = deque(maxlen=10000)
        self._lock = asyncio.Lock()

    # -- Public API ------------------------------------------------------------

    @property
    def target_url(self) -> str:
        return self._target_url

    def _method_key(self, method: str) -> str:
        """Map HTTP method to metrics key prefix: 'put' or 'delete'."""
        return "delete" if method == "DELETE" else "put"

    def _send_stdout(self, doc: dict) -> None:
        """Write a document to stdout."""
        import sys

        body, content_type = serialize_doc(doc, self._output_format)
        if isinstance(body, bytes):
            sys.stdout.buffer.write(body)
            sys.stdout.buffer.write(b"\n")
            sys.stdout.buffer.flush()
        else:
            sys.stdout.write(body + "\n")
            sys.stdout.flush()

    async def send(self, doc: dict, method: str = "PUT") -> dict:
        """Send a single doc. Returns result dict with 'ok' bool. Raises OutputEndpointDown if halt_on_failure."""
        if doc is None:
            ic("send: None doc – skipped", method)
            log_event(
                logger,
                "debug",
                "OUTPUT",
                "received None doc – skipped",
                doc_id="unknown",
            )
            if self._metrics:
                self._metrics.inc("output_skipped_total")
            return {"ok": True, "doc_id": "unknown", "method": method, "skipped": True}

        doc_id = doc.get("_id", doc.get("id", "unknown"))
        encoded_doc_id = urllib.parse.quote(str(doc_id), safe="")
        url = self._url_template.format(
            target_url=self._target_url, doc_id=encoded_doc_id
        )
        ic("send", method, doc_id, url)

        try:
            body, content_type = serialize_doc(doc, self._output_format)
        except (ValueError, TypeError, RuntimeError) as exc:
            ic("send: serialization failed", doc_id, exc)
            log_event(
                logger,
                "error",
                "OUTPUT",
                "serialization failed – cannot send",
                doc_id=doc_id,
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            return {
                "ok": False,
                "doc_id": doc_id,
                "method": method,
                "status": 0,
                "error": f"serialization: {exc}",
            }
        body_len = len(body) if isinstance(body, (bytes, str)) else 0

        if self._dry_run:
            log_event(
                logger,
                "debug",
                "OUTPUT",
                "dry run",
                operation=infer_operation(doc=doc, method=method),
                doc_id=doc_id,
                http_method=method,
                url=url,
                bytes=body_len,
            )
            return {"ok": True, "doc_id": doc_id, "method": method, "dry_run": True}

        if self._mode == "stdout":
            self._send_stdout(doc)
            return {"ok": True, "doc_id": doc_id, "method": method}

        assert self._http is not None

        mk = self._method_key(method)

        t_start = time.monotonic()
        try:
            merged_headers = {**self._headers, **self._extra_headers}
            request_kwargs: dict = {
                "auth": self._auth,
                "headers": merged_headers,
                "params": self._extra_params or None,
                "timeout": aiohttp.ClientTimeout(total=self._request_timeout),
                "allow_redirects": self._follow_redirects,
            }
            if self._ssl_ctx is not None:
                request_kwargs["ssl"] = self._ssl_ctx
            if method == "DELETE" and not self._send_delete_body:
                body_len = 0
            else:
                request_kwargs["data"] = body
                merged_headers["Content-Type"] = content_type
            resp = await self._http.request(method, url, **request_kwargs)
            elapsed_ms = (time.monotonic() - t_start) * 1000
            status = resp.status
            resp.release()

            # Track outbound auth metrics
            if self._metrics:
                self._metrics.inc("outbound_auth_total")
                self._metrics.record_outbound_auth_time(elapsed_ms / 1000)
                if status in (401, 403):
                    self._metrics.inc("outbound_auth_failure_total")
                else:
                    self._metrics.inc("outbound_auth_success_total")

            ic("send: response", doc_id, status, round(elapsed_ms, 1))
            await self._record_time(elapsed_ms)
            if self._metrics:
                self._metrics.inc("output_requests_total")
                self._metrics.inc(f"output_{mk}_total")
                self._metrics.inc("bytes_output_total", body_len)
                self._metrics.record_output_response_time(elapsed_ms / 1000)
            log_event(
                logger,
                "debug",
                "OUTPUT",
                "forwarded document",
                operation=infer_operation(doc=doc, method=method),
                doc_id=doc_id,
                http_method=method,
                url=url,
                status=status,
                elapsed_ms=round(elapsed_ms, 1),
                bytes=body_len,
            )
            if self._metrics:
                self._metrics.inc("output_success_total")
                if method == "DELETE":
                    self._metrics.inc("deletes_forwarded_total")
                self._metrics.set("output_endpoint_up", 1)
            return {"ok": True, "doc_id": doc_id, "method": method, "status": status}

        except _ClientHTTPError as exc:
            elapsed_ms = (time.monotonic() - t_start) * 1000
            error_class, is_transient = classify_http_status(exc.status)

            # §3.14: Support retry_on_conflict — 409 becomes transient if enabled
            if self._retry_on_conflict and exc.status == 409:
                is_transient = True
                error_class = "conflict_retryable"

            ic(
                "send: client HTTP error",
                doc_id,
                exc.status,
                error_class,
                exc.body[:200],
            )
            await self._record_time(elapsed_ms)
            if self._metrics:
                self._metrics.inc("outbound_auth_total")
                self._metrics.record_outbound_auth_time(elapsed_ms / 1000)
                if exc.status in (401, 403):
                    self._metrics.inc("outbound_auth_failure_total")
                else:
                    self._metrics.inc("outbound_auth_success_total")
                self._metrics.inc("output_errors_total")
                self._metrics.inc(f"output_{mk}_errors_total")
                self._metrics.inc("bytes_output_total", body_len)
                self._metrics.record_output_response_time(elapsed_ms / 1000)

            # §3.6: Handle auth failures (401/403) — first failure logs CRITICAL,
            # upgrade to halt when halt_on_failure=false
            if exc.status in (401, 403) and not self._output_auth_failure_warned:
                log_event(
                    logger,
                    "critical",
                    "OUTPUT",
                    "Output endpoint authentication failed (HTTP %d) — every document will fail with the same credentials. "
                    "Fix 'target_auth' in output config and restart the job."
                    % exc.status,
                    doc_id=doc_id,
                    status=exc.status,
                )
                self._output_auth_failure_warned = True
                # §3.6: Upgrade to halt when halt_on_failure=false + auth failure
                if not self._halt_on_failure:
                    if self._metrics:
                        self._metrics.set("output_endpoint_up", 0)
                    raise OutputEndpointDown(
                        f"Output auth failure (HTTP {exc.status}) — halting to prevent DLQ flood. "
                        f"Fix credentials and restart."
                    )
            elif exc.status in (401, 403):
                # Subsequent auth failures logged at DEBUG
                log_event(
                    logger,
                    "debug",
                    "OUTPUT",
                    "output auth failure",
                    doc_id=doc_id,
                    status=exc.status,
                )
            # §3.13: Handle 415 (Unsupported Media Type)
            elif exc.status == 415 and not self._media_type_warned:
                log_event(
                    logger,
                    "critical",
                    "OUTPUT",
                    "Output endpoint rejected Content-Type '%s' (HTTP 415 Unsupported Media Type). "
                    "Check 'output_format' in output config — the endpoint does not accept '%s' format."
                    % (content_type, self._output_format),
                    doc_id=doc_id,
                    status=415,
                    content_type=content_type,
                )
                self._media_type_warned = True
            elif exc.status == 415:
                log_event(
                    logger,
                    "debug",
                    "OUTPUT",
                    "output 415 repeat",
                    doc_id=doc_id,
                    content_type=content_type,
                )

            log_event(
                logger,
                "error",
                "OUTPUT",
                "client error (4xx %s)"
                % ("retryable" if is_transient else "permanent"),
                operation=infer_operation(doc=doc, method=method),
                doc_id=doc_id,
                http_method=method,
                url=url,
                status=exc.status,
                error_class=error_class,
                elapsed_ms=round(elapsed_ms, 1),
                error_detail=exc.body[:500],
            )
            return {
                "ok": False,
                "doc_id": doc_id,
                "method": method,
                "status": exc.status,
                "error": exc.body[:500],
                "error_class": error_class,
                "data_error_action": self._data_error_action,
                "retryable": is_transient,
            }

        except _RedirectHTTPError as exc:
            elapsed_ms = (time.monotonic() - t_start) * 1000
            ic("send: redirect error", doc_id, exc.status)
            await self._record_time(elapsed_ms)
            if self._metrics:
                self._metrics.inc("output_errors_total")
                self._metrics.inc(f"output_{mk}_errors_total")
                self._metrics.inc("bytes_output_total", body_len)
                self._metrics.record_output_response_time(elapsed_ms / 1000)
            log_event(
                logger,
                "error",
                "OUTPUT",
                "redirect error (3xx)",
                operation=infer_operation(doc=doc, method=method),
                doc_id=doc_id,
                http_method=method,
                url=url,
                status=exc.status,
                elapsed_ms=round(elapsed_ms, 1),
                error_detail=exc.body[:500],
            )
            if self._halt_on_failure:
                if self._metrics:
                    self._metrics.set("output_endpoint_up", 0)
                raise OutputEndpointDown(
                    f"Output endpoint returned redirect {exc.status} for {method} {url}"
                ) from exc
            else:
                log_event(
                    logger,
                    "warn",
                    "OUTPUT",
                    "halt_on_failure=false – skipping doc",
                    doc_id=doc_id,
                    status=exc.status,
                )
                return {
                    "ok": False,
                    "doc_id": doc_id,
                    "method": method,
                    "status": exc.status,
                    "error": exc.body[:500],
                }

        except _ServerHTTPError as exc:
            elapsed_ms = (time.monotonic() - t_start) * 1000
            ic("send: server HTTP error (5xx)", doc_id, exc.status)
            await self._record_time(elapsed_ms)
            if self._metrics:
                self._metrics.inc("output_errors_total")
                self._metrics.inc(f"output_{mk}_errors_total")
                self._metrics.inc("bytes_output_total", body_len)
                self._metrics.record_output_response_time(elapsed_ms / 1000)

            # §3.4: Log permanent 5xx (501, 505, 506, 508, 510, 511) with special messages
            if exc.status == 501 and not self._permanent_5xx_warned.get(501):
                log_event(
                    logger,
                    "critical",
                    "OUTPUT",
                    "Output endpoint returned 501 Not Implemented for %s — the endpoint does not support this HTTP method. "
                    "Check 'write_method' / 'delete_method' in output config." % method,
                    status=exc.status,
                    method=method,
                )
                self._permanent_5xx_warned[501] = True
            elif exc.status in (
                505,
                506,
                508,
                510,
                511,
            ) and not self._permanent_5xx_warned.get(exc.status):
                log_event(
                    logger,
                    "warn",
                    "OUTPUT",
                    "Output endpoint returned permanent %d — server configuration issue. "
                    "All docs using this method will fail." % exc.status,
                    status=exc.status,
                )
                self._permanent_5xx_warned[exc.status] = True
            elif exc.status in (501, 505, 506, 508, 510, 511):
                # Subsequent occurrences logged at DEBUG
                log_event(
                    logger,
                    "debug",
                    "OUTPUT",
                    "permanent 5xx",
                    doc_id=doc_id,
                    status=exc.status,
                )

            log_event(
                logger,
                "error",
                "OUTPUT",
                "server error (5xx) after retries",
                operation=infer_operation(doc=doc, method=method),
                doc_id=doc_id,
                http_method=method,
                url=url,
                status=exc.status,
                elapsed_ms=round(elapsed_ms, 1),
                error_detail=exc.body[:500],
            )
            if self._halt_on_failure:
                if self._metrics:
                    self._metrics.set("output_endpoint_up", 0)
                raise OutputEndpointDown(
                    f"Output endpoint returned {exc.status} for {method} {url} – "
                    f"halting to preserve checkpoint"
                ) from exc
            else:
                log_event(
                    logger,
                    "warn",
                    "OUTPUT",
                    "halt_on_failure=false – skipping doc",
                    doc_id=doc_id,
                    status=exc.status,
                )
                return {
                    "ok": False,
                    "doc_id": doc_id,
                    "method": method,
                    "status": exc.status,
                    "error": exc.body[:500],
                }

        except asyncio.TimeoutError:
            elapsed_ms = (time.monotonic() - t_start) * 1000
            ic("send: timeout", doc_id, url, round(elapsed_ms, 1))
            await self._record_time(elapsed_ms)
            if self._metrics:
                self._metrics.inc("output_errors_total")
                self._metrics.inc(f"output_{mk}_errors_total")
                self._metrics.record_output_response_time(elapsed_ms / 1000)
            log_event(
                logger,
                "error",
                "OUTPUT",
                "request timed out",
                operation=infer_operation(doc=doc, method=method),
                doc_id=doc_id,
                http_method=method,
                url=url,
                elapsed_ms=round(elapsed_ms, 1),
                error_detail=f"timeout after {self._request_timeout}s",
            )
            if self._halt_on_failure:
                if self._metrics:
                    self._metrics.set("output_endpoint_up", 0)
                raise OutputEndpointDown(
                    f"Output endpoint timed out for {method} {url} – "
                    f"halting to preserve checkpoint"
                )
            return {
                "ok": False,
                "doc_id": doc_id,
                "method": method,
                "status": 0,
                "error": f"timeout after {self._request_timeout}s",
            }

        except aiohttp.ClientConnectorError as exc:
            elapsed_ms = (time.monotonic() - t_start) * 1000
            ic("send: connection error", doc_id, url, type(exc).__name__, str(exc))
            await self._record_time(elapsed_ms)
            if self._metrics:
                self._metrics.inc("output_errors_total")
                self._metrics.inc(f"output_{mk}_errors_total")
            log_event(
                logger,
                "error",
                "OUTPUT",
                "connection failed (DNS / refused / unreachable)",
                operation=infer_operation(doc=doc, method=method),
                doc_id=doc_id,
                http_method=method,
                url=url,
                elapsed_ms=round(elapsed_ms, 1),
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            if self._halt_on_failure:
                if self._metrics:
                    self._metrics.set("output_endpoint_up", 0)
                raise OutputEndpointDown(
                    f"Output endpoint unreachable for {method} {url} – "
                    f"halting to preserve checkpoint: {exc}"
                ) from exc
            return {
                "ok": False,
                "doc_id": doc_id,
                "method": method,
                "status": 0,
                "error": str(exc)[:500],
            }

        except aiohttp.ClientSSLError as exc:
            elapsed_ms = (time.monotonic() - t_start) * 1000
            ic("send: SSL/TLS error", doc_id, url, str(exc))
            await self._record_time(elapsed_ms)
            if self._metrics:
                self._metrics.inc("output_errors_total")
                self._metrics.inc(f"output_{mk}_errors_total")
            log_event(
                logger,
                "error",
                "OUTPUT",
                "SSL/TLS handshake failed",
                operation=infer_operation(doc=doc, method=method),
                doc_id=doc_id,
                http_method=method,
                url=url,
                elapsed_ms=round(elapsed_ms, 1),
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            if self._halt_on_failure:
                if self._metrics:
                    self._metrics.set("output_endpoint_up", 0)
                raise OutputEndpointDown(
                    f"SSL error for {method} {url} – halting to preserve checkpoint: {exc}"
                ) from exc
            return {
                "ok": False,
                "doc_id": doc_id,
                "method": method,
                "status": 0,
                "error": f"SSL: {exc}",
            }

        except aiohttp.InvalidURL as exc:
            ic("send: invalid URL", doc_id, url, str(exc))
            log_event(
                logger,
                "error",
                "OUTPUT",
                "invalid URL",
                doc_id=doc_id,
                http_method=method,
                url=url,
                error_detail=str(exc),
            )
            return {
                "ok": False,
                "doc_id": doc_id,
                "method": method,
                "status": 0,
                "error": f"invalid URL: {exc}",
            }

        except (ConnectionError, aiohttp.ClientError) as exc:
            elapsed_ms = (time.monotonic() - t_start) * 1000
            ic(
                "send: generic connection/client error",
                doc_id,
                type(exc).__name__,
                str(exc),
            )
            await self._record_time(elapsed_ms)
            if self._metrics:
                self._metrics.inc("output_errors_total")
                self._metrics.inc(f"output_{mk}_errors_total")
                self._metrics.inc("bytes_output_total", body_len)
                self._metrics.record_output_response_time(elapsed_ms / 1000)
            log_event(
                logger,
                "error",
                "OUTPUT",
                "output failed after retries",
                operation=infer_operation(doc=doc, method=method),
                doc_id=doc_id,
                http_method=method,
                url=url,
                elapsed_ms=round(elapsed_ms, 1),
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            if self._halt_on_failure:
                if self._metrics:
                    self._metrics.set("output_endpoint_up", 0)
                raise OutputEndpointDown(
                    f"Output endpoint unreachable for {method} {url} – "
                    f"halting to preserve checkpoint: {exc}"
                ) from exc
            else:
                log_event(
                    logger,
                    "warn",
                    "OUTPUT",
                    "halt_on_failure=false – skipping doc",
                    doc_id=doc_id,
                )
                return {
                    "ok": False,
                    "doc_id": doc_id,
                    "method": method,
                    "status": 0,
                    "error": str(exc)[:500],
                }

    async def test_reachable(self) -> bool:
        """Quick health check – HEAD or GET the target URL root."""
        if self._mode != "http" or not self._target_url:
            return True
        assert self._http is not None
        probe_url = self._hc_url or self._target_url
        ic("test_reachable", probe_url)
        try:
            t_start = time.monotonic()
            kwargs: dict = {
                "auth": self._auth,
                "headers": self._headers,
                "timeout": aiohttp.ClientTimeout(total=self._hc_timeout),
                "allow_redirects": self._follow_redirects,
            }
            if self._ssl_ctx is not None:
                kwargs["ssl"] = self._ssl_ctx
            resp = await self._http.request("GET", probe_url, **kwargs)
            elapsed_ms = (time.monotonic() - t_start) * 1000
            resp.release()
            ic("test_reachable: OK", resp.status, round(elapsed_ms, 1))
            log_event(
                logger,
                "info",
                "HTTP",
                "output endpoint reachable",
                operation="SELECT",
                url=self._target_url,
                status=resp.status,
                elapsed_ms=round(elapsed_ms, 1),
            )
            return True
        except aiohttp.ClientConnectorError as exc:
            ic("test_reachable: connection failed", type(exc).__name__, str(exc))
            log_event(
                logger,
                "error",
                "HTTP",
                "output endpoint unreachable (DNS / refused / unreachable)",
                url=self._target_url,
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            return False
        except aiohttp.ClientSSLError as exc:
            ic("test_reachable: SSL error", str(exc))
            log_event(
                logger,
                "error",
                "HTTP",
                "output endpoint SSL/TLS error",
                url=self._target_url,
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            return False
        except asyncio.TimeoutError:
            ic("test_reachable: timeout", probe_url)
            log_event(
                logger,
                "error",
                "HTTP",
                "output endpoint timed out",
                url=self._target_url,
                error_detail=f"timeout after {self._hc_timeout}s",
            )
            return False
        except (ConnectionError, aiohttp.ClientError, OSError) as exc:
            ic("test_reachable: error", type(exc).__name__, str(exc))
            log_event(
                logger,
                "error",
                "HTTP",
                "output endpoint unreachable",
                url=self._target_url,
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            return False

    def log_stats(self, force_info: bool = False) -> None:
        """Log accumulated response time statistics."""
        if not self._log_response_times or not self._resp_times:
            return
        n = len(self._resp_times)
        avg = sum(self._resp_times) / n
        lo = min(self._resp_times)
        hi = max(self._resp_times)
        log_event(
            logger,
            "info" if force_info else "debug",
            "OUTPUT",
            "output stats: %d requests | avg=%.1fms | min=%.1fms | max=%.1fms"
            % (n, avg, lo, hi),
        )

    async def start_heartbeat(self, shutdown_event: asyncio.Event) -> None:
        """Start the periodic health-check background task."""
        if self._mode != "http" or not self._hc_enabled:
            return
        self._hc_task = asyncio.create_task(self._heartbeat_loop(shutdown_event))

    async def stop_heartbeat(self) -> None:
        """Cancel the heartbeat task."""
        if self._hc_task and not self._hc_task.done():
            self._hc_task.cancel()
            try:
                await self._hc_task
            except asyncio.CancelledError:
                pass

    async def _heartbeat_loop(self, shutdown_event: asyncio.Event) -> None:
        """Periodically probe the output endpoint and update metrics."""
        while not shutdown_event.is_set():
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=self._hc_interval)
                break  # shutdown signalled
            except asyncio.TimeoutError:
                pass  # interval elapsed, do the check

            ok = await self._health_check()
            if self._metrics:
                self._metrics.set("output_endpoint_up", 1 if ok else 0)
            if ok:
                log_event(logger, "debug", "HTTP", "heartbeat OK", url=self._hc_url)
            else:
                log_event(logger, "warn", "HTTP", "heartbeat FAILED", url=self._hc_url)

    async def _health_check(self) -> bool:
        """Single health-check probe."""
        if not self._hc_url or self._http is None:
            return True
        if self._metrics:
            self._metrics.inc("health_probes_total")
        ic("_health_check", self._hc_method, self._hc_url)
        try:
            timeout = aiohttp.ClientTimeout(total=self._hc_timeout)
            kwargs: dict = {
                "auth": self._auth,
                "headers": self._headers,
                "timeout": timeout,
                "allow_redirects": self._follow_redirects,
            }
            if self._ssl_ctx is not None:
                kwargs["ssl"] = self._ssl_ctx
            t_start = time.monotonic()
            resp = await self._http.request(self._hc_method, self._hc_url, **kwargs)
            elapsed_ms = (time.monotonic() - t_start) * 1000
            resp.release()
            if self._metrics:
                self._metrics.record_health_probe_time(elapsed_ms / 1000)
            ic("_health_check: response", resp.status, round(elapsed_ms, 1))
            log_event(
                logger,
                "debug",
                "HTTP",
                "health check",
                url=self._hc_url,
                status=resp.status,
                elapsed_ms=round(elapsed_ms, 1),
            )
            return resp.status < 500
        except asyncio.TimeoutError:
            ic("_health_check: timeout", self._hc_url)
            if self._metrics:
                self._metrics.inc("health_probe_failures_total")
            log_event(
                logger,
                "warn",
                "HTTP",
                "health check timed out",
                url=self._hc_url,
                error_detail=f"timeout after {self._hc_timeout}s",
            )
            return False
        except aiohttp.ClientConnectorError as exc:
            ic("_health_check: connection error", type(exc).__name__, str(exc))
            if self._metrics:
                self._metrics.inc("health_probe_failures_total")
            log_event(
                logger,
                "warn",
                "HTTP",
                "health check connection failed",
                url=self._hc_url,
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            return False
        except aiohttp.ClientSSLError as exc:
            ic("_health_check: SSL error", str(exc))
            if self._metrics:
                self._metrics.inc("health_probe_failures_total")
            log_event(
                logger,
                "warn",
                "HTTP",
                "health check SSL/TLS error",
                url=self._hc_url,
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            return False
        except (ConnectionError, aiohttp.ClientError, OSError) as exc:
            ic("_health_check: error", type(exc).__name__, str(exc))
            if self._metrics:
                self._metrics.inc("health_probe_failures_total")
            log_event(
                logger,
                "warn",
                "HTTP",
                "health check failed",
                url=self._hc_url,
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            return False

    # -- Internal --------------------------------------------------------------

    async def _record_time(self, ms: float) -> None:
        if self._log_response_times:
            async with self._lock:
                self._resp_times.append(ms)


def determine_method(
    change: dict, write_method: str = "PUT", delete_method: str = "DELETE"
) -> str:
    if change.get("deleted") or change.get("removed"):
        return delete_method
    return write_method


def _build_dlq_reason(result: dict) -> str:
    """Build a human-readable reason string for a DLQ entry."""
    error_class = result.get("error_class", "")
    status = result.get("status", 0)
    if error_class:
        return f"data_error:{error_class}"
    if isinstance(status, int) and 400 <= status < 500:
        return f"client_error:{status}"
    if isinstance(status, int) and 300 <= status < 400:
        return f"redirect:{status}"
    if isinstance(status, int) and status >= 500:
        return f"server_error:{status}"
    if result.get("error") == "shutdown_inflight":
        return "shutdown_inflight"
    if isinstance(status, int) and status == 0:
        return "connection_failure"
    return "unknown"


class DeadLetterQueue:
    """
    Dead letter queue for documents that failed output delivery.

    When CBL is available, entries are stored as CBL documents.
    Otherwise falls back to append-only JSONL file.
    """

    def __init__(self, path: str, dlq_cfg: dict | None = None):
        from storage.cbl_store import USE_CBL as _use_cbl

        self._use_cbl = _use_cbl
        self._store = None
        if self._use_cbl:
            from storage.cbl_store import CBLStore

            self._store = CBLStore()
        self._path = Path(path) if path and not self._use_cbl else None
        self._lock = asyncio.Lock()
        cfg = dlq_cfg or {}
        self._retention_seconds = cfg.get("retention_seconds", 86400)
        self._max_replay_attempts = cfg.get("max_replay_attempts", 10)

    @property
    def enabled(self) -> bool:
        return self._use_cbl or self._path is not None

    async def write(
        self,
        doc: dict,
        result: dict,
        seq: str | int,
        target_url: str = "",
        metrics=None,
    ) -> None:
        ic("DLQ.write", result.get("doc_id"), seq, "cbl" if self._use_cbl else "file")
        reason = _build_dlq_reason(result)
        if self._use_cbl and self._store:
            self._store.add_dlq_entry(
                doc_id=result.get("doc_id", "unknown"),
                seq=str(seq),
                method=result.get("method", "PUT"),
                status=result.get("status", 0),
                error=result.get("error", ""),
                doc=doc,
                target_url=target_url,
                ttl_seconds=self._retention_seconds,
                reason=reason,
            )
            log_event(
                logger,
                "warn",
                "DLQ",
                "entry written to CBL",
                operation="INSERT",
                doc_id=result.get("doc_id"),
                seq=str(seq),
                storage="cbl",
                ttl_seconds=self._retention_seconds,
                url=target_url,
            )
            return
        # Original file fallback
        if not self._path:
            return
        entry = {
            "doc_id": result.get("doc_id", "unknown"),
            "seq": str(seq),
            "method": result.get("method", "PUT"),
            "status": result.get("status", 0),
            "error": result.get("error", ""),
            "reason": reason,
            "time": int(time.time()),
            "target_url": target_url,
            "replay_attempts": 0,
            "doc": doc,
        }
        try:
            async with self._lock:
                with open(self._path, "a") as f:
                    f.write(json.dumps(entry) + "\n")
        except OSError as exc:
            ic("DLQ.write: file write failed", self._path, exc)
            log_event(
                logger,
                "error",
                "DLQ",
                "failed to write DLQ entry to file — DATA MAY BE LOST",
                doc_id=result.get("doc_id"),
                seq=str(seq),
                storage="file",
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            if metrics:
                metrics.inc("dlq_write_failures_total")
            raise
        log_event(
            logger,
            "warn",
            "DLQ",
            "entry written to file",
            operation="INSERT",
            doc_id=result.get("doc_id"),
            seq=str(seq),
            storage="file",
        )

    async def purge(self, dlq_id: str) -> None:
        """Remove a DLQ entry after successful reprocessing."""
        ic("DLQ.purge", dlq_id, "cbl" if self._use_cbl else "file")
        if self._use_cbl and self._store:
            self._store.delete_dlq_entry(dlq_id)
            log_event(
                logger,
                "debug",
                "DLQ",
                "entry purged after successful reprocessing",
                operation="DELETE",
                doc_id=dlq_id,
                storage="cbl",
            )
            return
        # File-based DLQ does not support individual purge
        log_event(
            logger,
            "debug",
            "DLQ",
            "file-based DLQ does not support purge",
            doc_id=dlq_id,
            storage="file",
        )

    def flush_insert_meta(self, job_id: str = "") -> None:
        """Record last_inserted_at once after a batch of DLQ writes."""
        if self._use_cbl and self._store:
            self._store.update_dlq_meta("last_inserted_at", job_id)

    def flush_drain_meta(self, job_id: str = "") -> None:
        """Record last_drained_at once after a batch of DLQ drains."""
        if self._use_cbl and self._store:
            self._store.update_dlq_meta("last_drained_at", job_id)

    def list_pending(self) -> list[dict]:
        """Return all pending (not yet retried) DLQ entries."""
        if self._use_cbl and self._store:
            return [e for e in self._store.list_dlq() if not e.get("retried")]
        if self._path and self._path.exists():
            entries = []
            with open(self._path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            entries.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
            return entries
        return []

    def pending_count(self) -> int:
        """Return count of pending DLQ entries without loading all documents."""
        if self._use_cbl and self._store:
            # Use server-side COUNT query when available.
            if hasattr(self._store, "pending_dlq_count"):
                return self._store.pending_dlq_count()
            return self._store.dlq_count()
        return len(self.list_pending())

    def get_entry_doc(self, dlq_id: str) -> dict | None:
        """Return the full DLQ entry including doc_data for reprocessing."""
        if self._use_cbl and self._store:
            return self._store.get_dlq_entry(dlq_id)
        return None

    def purge_expired(self) -> int:
        """Remove DLQ entries older than retention_seconds. Returns count purged."""
        if self._use_cbl and self._store and self._retention_seconds > 0:
            return self._store.purge_expired_dlq(self._retention_seconds)
        return 0

    def increment_replay_attempts(self, dlq_id: str) -> int:
        """Increment replay_attempts on a CBL DLQ entry. Returns new count."""
        if self._use_cbl and self._store:
            return self._store.increment_dlq_replay_attempts(dlq_id)
        return 0

    @property
    def max_replay_attempts(self) -> int:
        return self._max_replay_attempts

    @property
    def retention_seconds(self) -> int:
        return self._retention_seconds


# ---------------------------------------------------------------------------
# Minimal auth helpers (so this module can work standalone)
# The main changes_worker passes its own build_basic_auth / build_auth_headers
# via constructor args; these are fallback defaults.
# ---------------------------------------------------------------------------


def _default_build_basic_auth(auth_cfg: dict) -> aiohttp.BasicAuth | None:
    if auth_cfg.get("method", "none") == "basic":
        username = auth_cfg.get("username", "")
        password = auth_cfg.get("password", "")
        if username:
            return aiohttp.BasicAuth(username, password)
    return None


def _default_build_auth_headers(auth_cfg: dict) -> dict:
    method = auth_cfg.get("method", "none")
    headers: dict[str, str] = {}
    if method == "bearer":
        headers["Authorization"] = f"Bearer {auth_cfg.get('bearer_token', '')}"
    elif method == "session":
        headers["Cookie"] = f"SyncGatewaySession={auth_cfg.get('session_cookie', '')}"
    return headers


# ---------------------------------------------------------------------------
# Lazy import shim for RetryableHTTP / exception classes from changes_worker.
# At import time we don't know if they exist yet, so we resolve on first use
# or accept them via constructor injection.
# ---------------------------------------------------------------------------


class _ClientHTTPError(Exception):
    def __init__(self, status: int, body: str):
        self.status = status
        self.body = body
        super().__init__(f"HTTP {status}: {body[:200]}")


class _RedirectHTTPError(Exception):
    def __init__(self, status: int, body: str):
        self.status = status
        self.body = body
        super().__init__(f"HTTP {status}: {body[:200]}")


class _ServerHTTPError(Exception):
    def __init__(self, status: int, body: str):
        self.status = status
        self.body = body
        super().__init__(f"HTTP {status}: {body[:200]}")


class _RetryableHTTPLazy:
    """Thin wrapper that delegates to the real RetryableHTTP from changes_worker."""

    def __init__(self, session: aiohttp.ClientSession, retry_cfg: dict):
        from main import RetryableHTTP

        self._inner = RetryableHTTP(session, retry_cfg)

    async def request(self, method: str, url: str, **kwargs) -> aiohttp.ClientResponse:
        return await self._inner.request(method, url, **kwargs)
