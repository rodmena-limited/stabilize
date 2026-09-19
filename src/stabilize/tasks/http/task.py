"""
HTTPTask for making HTTP requests using Python stdlib.

This module provides a production-ready HTTPTask with:
- All HTTP methods (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS)
- JSON and form-encoded request bodies
- File upload (multipart/form-data)
- File download (streaming)
- Custom headers and authentication (Basic, Bearer)
- Retry logic with configurable backoff
- SSL/TLS configuration (verification, client certs)
- Placeholder substitution in URL, headers, and body
- Secret masking in logs

Zero external dependencies - uses only Python stdlib.
"""

from __future__ import annotations

import ipaddress
import logging
import socket
import time
from datetime import timedelta
from typing import TYPE_CHECKING
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import BaseHandler, HTTPRedirectHandler, HTTPSHandler, build_opener

from resilient_circuit import ExponentialDelay, RetryWithBackoffPolicy
from resilient_circuit.exceptions import RetryLimitReached

from stabilize.redaction import redact_userinfo
from stabilize.tasks.http.constants import (
    DEFAULT_RETRY_ON_STATUS,
    DEFAULT_TIMEOUT,
    SUPPORTED_METHODS,
)
from stabilize.tasks.http.guarded_opener import BlockedPeerError, build_guarded_handlers
from stabilize.tasks.http.request import build_request
from stabilize.tasks.http.response import process_response
from stabilize.tasks.http.ssl_context import build_ssl_context
from stabilize.tasks.http.utils import (
    format_error,
    mask_secrets,
    substitute_placeholders,
)
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from http.client import HTTPMessage, HTTPResponse
    from typing import IO
    from urllib.request import Request

    from stabilize.models.stage import StageExecution

logger = logging.getLogger(__name__)

_BLOCKED_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]

_ALLOWED_SCHEMES = {"http", "https"}


def _normalize_address(addr: ipaddress.IPv4Address | ipaddress.IPv6Address) -> (
    ipaddress.IPv4Address | ipaddress.IPv6Address
):
    """Collapse IPv4-mapped and 6to4 IPv6 forms to the IPv4 address they denote.

    ``::ffff:127.0.0.1`` is loopback, but matches no IPv4 CIDR and is not
    caught by IPv6Address.is_loopback, so it must be unwrapped before any
    range check.
    """
    if isinstance(addr, ipaddress.IPv6Address):
        if addr.ipv4_mapped is not None:
            return addr.ipv4_mapped
        if addr.sixtofour is not None:
            return addr.sixtofour
        if addr.teredo is not None:
            return addr.teredo[1]
    return addr


def _is_blocked(addr: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Whether *addr* is one an outbound workflow request must never reach."""
    addr = _normalize_address(addr)
    if (
        addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_multicast
        or addr.is_reserved
        or addr.is_unspecified
    ):
        return True
    return any(addr in net for net in _BLOCKED_NETWORKS)


def _validate_url_safety(url: str) -> None:
    """Raise ValueError if URL resolves to a private/loopback address (SSRF protection)."""
    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme not in _ALLOWED_SCHEMES:
        raise ValueError(f"SSRF blocked: scheme '{scheme}' is not permitted (use http or https)")
    hostname = parsed.hostname
    if not hostname:
        raise ValueError(f"Cannot extract hostname from URL: {redact_userinfo(url)}")
    try:
        addr = ipaddress.ip_address(hostname)
    except ValueError:
        # Hostname is a DNS name — resolve it
        try:
            resolved = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
            addrs = {ipaddress.ip_address(r[4][0]) for r in resolved}
        except socket.gaierror as exc:
            # Fail closed: an unresolvable name must not pass the guard and be
            # resolved again, differently, by the connection itself.
            raise ValueError(f"SSRF blocked: cannot resolve host '{hostname}': {exc}") from exc
        if not addrs:
            raise ValueError(f"SSRF blocked: host '{hostname}' resolved to no addresses")
        for addr in addrs:
            if _is_blocked(addr):
                raise ValueError(
                    f"SSRF blocked: URL '{redact_userinfo(url)}' resolves to blocked address {addr}"
                )
        return

    if _is_blocked(addr):
        raise ValueError(
            f"SSRF blocked: URL '{redact_userinfo(url)}' targets blocked address {addr}"
        )


class _SafeRedirectHandler(HTTPRedirectHandler):
    """Validates each redirect target against SSRF rules."""

    def __init__(self, allow_private: bool = False) -> None:
        super().__init__()
        self._allow_private = allow_private

    def redirect_request(
        self, req: Request, fp: IO[bytes], code: int, msg: str, headers: HTTPMessage, newurl: str
    ) -> Request | None:
        if not self._allow_private:
            _validate_url_safety(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class HTTPTask(Task):
    """
    Make HTTP requests using Python's stdlib urllib.

    Supports all standard HTTP methods, file upload/download, authentication,
    retries, and SSL/TLS configuration with zero external dependencies.

    Context Parameters:
        url (str): Request URL (required)
        method (str): HTTP method - GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS
                      (default: GET)

        Request Body (mutually exclusive):
            body (str|bytes): Raw request body
            json (dict): JSON body (auto-serialized, sets Content-Type)
            form (dict): Form-encoded body (application/x-www-form-urlencoded)

        Headers & Auth:
            headers (dict): Custom request headers
            auth (tuple|list): Basic auth as [username, password]
            bearer_token (str): Bearer token for Authorization header

        File Upload (multipart/form-data):
            upload_file (str): Path to file to upload
            upload_field (str): Form field name (default: "file")
            upload_filename (str): Override filename in upload
            upload_form (dict): Additional form fields with upload

        File Download:
            download_to (str): Path to save response body (streams large files)

        Timeouts & Retries:
            timeout (int|float): Request timeout in seconds (default: 30)
            retries (int): Number of retries on transient failure (default: 0)
            retry_delay (float): Delay between retries in seconds (default: 1.0)
            retry_on_status (list[int]): Status codes to retry (default: [502, 503, 504])

        SSL/TLS:
            verify_ssl (bool): Verify SSL certificates (default: True)
            ca_cert (str): Path to CA certificate bundle
            client_cert (str): Path to client certificate for mTLS
            client_key (str): Path to client private key for mTLS

        Response Handling:
            expected_status (int|list[int]): Expected status code(s), fail if mismatch
            max_response_size (int): Max response body in bytes (default: 10MB)
            parse_json (bool): Auto-parse JSON response body (default: False)

        Error Handling:
            continue_on_failure (bool): Return failed_continue instead of terminal
            secrets (list[str]): Context keys to mask in logs

    Outputs:
        status_code (int): HTTP response status
        headers (dict): Response headers
        body (str): Response body (or file path if download_to)
        body_json (dict|list|None): Parsed JSON (if parse_json=True and valid)
        elapsed_ms (int): Request duration in milliseconds
        url (str): Final URL after redirects
        content_type (str): Response Content-Type header
        content_length (int): Response body size in bytes

    Examples:
        # Simple GET
        context = {"url": "https://api.example.com/users"}

        # POST with JSON
        context = {
            "url": "https://api.example.com/users",
            "method": "POST",
            "json": {"name": "John", "email": "john@example.com"},
            "parse_json": True,
        }

        # With authentication
        context = {
            "url": "https://api.example.com/private",
            "bearer_token": "my-api-token",
        }

        # File upload
        context = {
            "url": "https://api.example.com/upload",
            "method": "POST",
            "upload_file": "/path/to/file.pdf",
            "upload_field": "document",
        }

        # File download
        context = {
            "url": "https://example.com/large-file.zip",
            "download_to": "/tmp/downloaded.zip",
        }

        # With retries
        context = {
            "url": "https://api.example.com/flaky",
            "retries": 3,
            "retry_delay": 2.0,
        }
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        """Execute HTTP request."""
        context = stage.context
        continue_on_failure = context.get("continue_on_failure", False)
        secrets = context.get("secrets", [])

        # Extract and validate URL
        url = context.get("url")
        if not url:
            return TaskResult.terminal(error="No 'url' specified in context")

        # Substitute placeholders in URL
        url = substitute_placeholders(url, context, secrets)

        # SSRF protection: block requests to private/loopback addresses
        if not context.get("allow_private_urls", False):
            try:
                _validate_url_safety(url)
            except ValueError as e:
                return TaskResult.terminal(error=str(e))

        # Validate method
        method = context.get("method", "GET").upper()
        if method not in SUPPORTED_METHODS:
            return TaskResult.terminal(error=f"Unsupported method '{method}'. Supported: {sorted(SUPPORTED_METHODS)}")

        # Build request
        try:
            request, body_bytes = build_request(method, url, context, secrets)
        except ValueError as e:
            return TaskResult.terminal(error=str(e))
        except FileNotFoundError as e:
            return TaskResult.terminal(error=f"File not found: {e}")

        # Build SSL context
        ssl_context = build_ssl_context(context)

        # Build opener with SSRF-safe redirect handling AND connect-time peer
        # enforcement. Validating the hostname alone is a TOCTOU: urllib
        # resolves the name again when it connects, so a rebinding attacker can
        # answer the two lookups differently. The guarded handlers check the
        # address the socket actually reached.
        allow_private = context.get("allow_private_urls", False)
        opener_handlers: list[BaseHandler] = [_SafeRedirectHandler(allow_private=allow_private)]
        if allow_private:
            if ssl_context:
                opener_handlers.append(HTTPSHandler(context=ssl_context))
        else:
            opener_handlers.extend(build_guarded_handlers(_is_blocked, ssl_context))
        _opener = build_opener(*opener_handlers)

        # Retry configuration
        retries = context.get("retries", 0)
        # POST/PATCH are not idempotent: a retry after a timeout or 5xx can
        # duplicate the side effect on the server. Default keeps the existing
        # behavior (retries apply to all methods when enabled); set
        # retry_non_idempotent=False to restrict retries to idempotent methods.
        if retries and not context.get("retry_non_idempotent", True) and method in {"POST", "PATCH"}:
            logger.debug("Retries disabled for non-idempotent %s (retry_non_idempotent=False)", method)
            retries = 0
        retry_delay = context.get("retry_delay", 1.0)
        retry_on_status = context.get("retry_on_status", DEFAULT_RETRY_ON_STATUS)
        timeout = context.get("timeout", DEFAULT_TIMEOUT)

        # Logging (mask secrets)
        log_url = redact_userinfo(mask_secrets(url, context, secrets))
        logger.debug("HTTPTask %s %s", method, log_url)

        # Execute with retries using resilient-circuit
        start_time = time.time()
        last_error: Exception | None = None
        last_response: HTTPResponse | HTTPError | None = None

        # Create retry configuration with exponential backoff
        http_backoff = ExponentialDelay(
            min_delay=timedelta(seconds=retry_delay),
            max_delay=timedelta(seconds=retry_delay * 10),
            factor=2,
            jitter=0.1,
        )

        # Custom exception for retryable status codes
        class RetryableStatusError(Exception):
            """Raised when response has a retryable status code."""

            def __init__(self, response: HTTPResponse) -> None:
                self.response = response
                super().__init__(f"Retryable status: {response.status}")

        def should_retry(e: Exception) -> bool:
            """Check if error should trigger retry."""
            if isinstance(e, ValueError | BlockedPeerError):
                # An SSRF refusal is deterministic; retrying re-resolves the
                # name, which is the attacker's second chance.
                return False
            if isinstance(e, RetryableStatusError):
                return True
            if isinstance(e, HTTPError) and e.code in retry_on_status:
                return True
            if isinstance(e, (URLError, TimeoutError, OSError)):
                return True
            return False

        http_retry_policy = RetryWithBackoffPolicy(
            max_retries=retries,
            backoff=http_backoff,
            should_handle=should_retry,
        )

        @http_retry_policy
        def make_request() -> HTTPResponse | HTTPError:
            nonlocal last_error
            try:
                if not allow_private:
                    _validate_url_safety(url)
                resp: HTTPResponse = _opener.open(
                    request,
                    data=body_bytes,
                    timeout=timeout,
                )

                # Check if we should retry based on status
                if resp.status in retry_on_status:
                    logger.debug("HTTPTask got %s, will retry", resp.status)
                    raise RetryableStatusError(resp)

                return resp

            except HTTPError as e:
                last_error = e
                if e.code in retry_on_status:
                    logger.debug("HTTPTask got HTTP %s, will retry", e.code)
                    raise  # Let retry policy handle it
                # Non-retryable HTTP error - return it for processing
                return e

            except (URLError, TimeoutError, OSError) as e:
                last_error = e
                logger.debug("HTTPTask error: %s, will retry", e)
                raise

        # Execute with retry
        if retries > 0:
            try:
                last_response = make_request()
            except RetryLimitReached as e:
                # Max retries exceeded - use last error or response
                cause = e.__cause__
                if cause is not None:
                    if isinstance(cause, RetryableStatusError):
                        last_response = cause.response
                    elif isinstance(cause, HTTPError):
                        last_response = cause
                    elif isinstance(cause, Exception):
                        last_error = cause
            except RetryableStatusError as e:
                # Shouldn't happen, but handle gracefully
                last_response = e.response
            except ValueError as e:
                # An SSRF refusal from the pre-connect revalidation is not
                # retryable and must not escape as an unhandled exception.
                last_error = e
        else:
            # No retries configured - single attempt
            try:
                if not allow_private:
                    _validate_url_safety(url)
                response = _opener.open(
                    request,
                    data=body_bytes,
                    timeout=timeout,
                )
                last_response = response
            except HTTPError as e:
                last_response = e
            except ValueError as e:
                # The pre-connect revalidation and the connect-time peer guard
                # both refuse by raising. Without this the task would escape
                # execute() with an unhandled exception rather than returning a
                # terminal TaskResult, turning a blocked request into a crash.
                last_error = e
            except (URLError, TimeoutError, OSError) as e:
                last_error = e

        elapsed_ms = int((time.time() - start_time) * 1000)

        # Handle connection/timeout errors
        if last_response is None and last_error is not None:
            error_msg = format_error(last_error)
            if continue_on_failure:
                return TaskResult.failed_continue(
                    error=error_msg,
                    outputs={"elapsed_ms": elapsed_ms, "url": redact_userinfo(url)},
                )
            return TaskResult.terminal(
                error=error_msg,
                context={"elapsed_ms": elapsed_ms, "url": redact_userinfo(url)},
            )

        # Process response
        return process_response(
            response=last_response,
            context=context,
            url=url,
            elapsed_ms=elapsed_ms,
            continue_on_failure=continue_on_failure,
        )
