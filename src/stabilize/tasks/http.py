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

import base64
import json
import logging
import os
import re
import ssl
import time
import uuid
from typing import TYPE_CHECKING, Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from http.client import HTTPResponse

    from stabilize.models.stage import StageExecution

logger = logging.getLogger(__name__)

# Default settings
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RESPONSE_SIZE = 10 * 1024 * 1024  # 10MB
DEFAULT_RETRY_ON_STATUS = [502, 503, 504]
CHUNK_SIZE = 8192


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

    SUPPORTED_METHODS = frozenset({"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"})

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
        url = self._substitute_placeholders(url, context, secrets)

        # Validate method
        method = context.get("method", "GET").upper()
        if method not in self.SUPPORTED_METHODS:
            return TaskResult.terminal(
                error=f"Unsupported method '{method}'. Supported: {sorted(self.SUPPORTED_METHODS)}"
            )

        # Build request
        try:
            request, body_bytes = self._build_request(method, url, context, secrets)
        except ValueError as e:
            return TaskResult.terminal(error=str(e))
        except FileNotFoundError as e:
            return TaskResult.terminal(error=f"File not found: {e}")

        # Build SSL context
        ssl_context = self._build_ssl_context(context)

        # Retry configuration
        retries = context.get("retries", 0)
        retry_delay = context.get("retry_delay", 1.0)
        retry_on_status = context.get("retry_on_status", DEFAULT_RETRY_ON_STATUS)
        timeout = context.get("timeout", DEFAULT_TIMEOUT)

        # Logging (mask secrets)
        log_url = self._mask_secrets(url, context, secrets)
        logger.debug("HTTPTask %s %s", method, log_url)

        # Execute with retries
        start_time = time.time()
        last_error: Exception | None = None
        last_response: HTTPResponse | HTTPError | None = None

        for attempt in range(retries + 1):
            try:
                response = urlopen(
                    request,
                    data=body_bytes,
                    timeout=timeout,
                    context=ssl_context,
                )

                # Check if we should retry based on status
                if response.status in retry_on_status and attempt < retries:
                    logger.debug("HTTPTask got %s, retrying (%d/%d)", response.status, attempt + 1, retries)
                    time.sleep(retry_delay)
                    continue

                last_response = response
                break

            except HTTPError as e:
                last_error = e
                if e.code in retry_on_status and attempt < retries:
                    logger.debug("HTTPTask got HTTP %s, retrying (%d/%d)", e.code, attempt + 1, retries)
                    time.sleep(retry_delay)
                    continue
                # Store response for output extraction
                last_response = e
                break

            except URLError as e:
                last_error = e
                if attempt < retries:
                    logger.debug("HTTPTask URL error: %s, retrying (%d/%d)", e.reason, attempt + 1, retries)
                    time.sleep(retry_delay)
                    continue
                break

            except TimeoutError as e:
                last_error = e
                if attempt < retries:
                    logger.debug("HTTPTask timeout, retrying (%d/%d)", attempt + 1, retries)
                    time.sleep(retry_delay)
                    continue
                break

            except OSError as e:
                last_error = e
                if attempt < retries:
                    logger.debug("HTTPTask OS error: %s, retrying (%d/%d)", e, attempt + 1, retries)
                    time.sleep(retry_delay)
                    continue
                break

        elapsed_ms = int((time.time() - start_time) * 1000)

        # Handle connection/timeout errors
        if last_response is None and last_error is not None:
            error_msg = self._format_error(last_error)
            if continue_on_failure:
                return TaskResult.failed_continue(
                    error=error_msg,
                    outputs={"elapsed_ms": elapsed_ms, "url": url},
                )
            return TaskResult.terminal(
                error=error_msg,
                context={"elapsed_ms": elapsed_ms, "url": url},
            )

        # Process response
        return self._process_response(
            response=last_response,
            context=context,
            url=url,
            elapsed_ms=elapsed_ms,
            continue_on_failure=continue_on_failure,
        )

    def _build_request(
        self,
        method: str,
        url: str,
        context: dict[str, Any],
        secrets: list[str],
    ) -> tuple[Request, bytes | None]:
        """Build urllib Request object."""
        headers: dict[str, str] = {}
        body_bytes: bytes | None = None

        # Custom headers
        custom_headers = context.get("headers", {})
        for key, value in custom_headers.items():
            headers[key] = self._substitute_placeholders(str(value), context, secrets)

        # Authentication
        if context.get("auth"):
            auth = context["auth"]
            if isinstance(auth, (list, tuple)) and len(auth) == 2:
                credentials = f"{auth[0]}:{auth[1]}"
                encoded = base64.b64encode(credentials.encode()).decode()
                headers["Authorization"] = f"Basic {encoded}"

        if context.get("bearer_token"):
            token = self._substitute_placeholders(context["bearer_token"], context, secrets)
            headers["Authorization"] = f"Bearer {token}"

        # Request body
        if context.get("upload_file"):
            # Multipart file upload
            body_bytes, content_type = self._build_multipart(context)
            headers["Content-Type"] = content_type

        elif context.get("json") is not None:
            # JSON body
            json_body = context["json"]
            body_bytes = json.dumps(json_body).encode("utf-8")
            headers.setdefault("Content-Type", "application/json")

        elif context.get("form"):
            # Form-encoded body
            form_data = context["form"]
            body_bytes = urlencode(form_data).encode("utf-8")
            headers.setdefault("Content-Type", "application/x-www-form-urlencoded")

        elif context.get("body") is not None:
            # Raw body
            body = context["body"]
            if isinstance(body, str):
                body = self._substitute_placeholders(body, context, secrets)
                body_bytes = body.encode("utf-8")
            else:
                body_bytes = body

        # Create request
        request = Request(url, method=method, headers=headers)

        return request, body_bytes

    def _build_multipart(self, context: dict[str, Any]) -> tuple[bytes, str]:
        """Build multipart/form-data body for file upload."""
        boundary = uuid.uuid4().hex
        body_parts: list[bytes] = []

        # Add form fields
        form_fields = context.get("upload_form", {})
        for name, value in form_fields.items():
            body_parts.append(f"--{boundary}\r\n".encode())
            body_parts.append(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
            body_parts.append(f"{value}\r\n".encode())

        # Add file
        upload_file = context["upload_file"]
        field_name = context.get("upload_field", "file")
        filename = context.get("upload_filename") or os.path.basename(upload_file)

        with open(upload_file, "rb") as f:
            file_content = f.read()

        # Detect content type
        content_type = self._guess_content_type(filename)

        body_parts.append(f"--{boundary}\r\n".encode())
        body_parts.append(f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'.encode())
        body_parts.append(f"Content-Type: {content_type}\r\n\r\n".encode())
        body_parts.append(file_content)
        body_parts.append(b"\r\n")

        # Final boundary
        body_parts.append(f"--{boundary}--\r\n".encode())

        body = b"".join(body_parts)
        return body, f"multipart/form-data; boundary={boundary}"

    def _guess_content_type(self, filename: str) -> str:
        """Guess content type from filename extension."""
        ext = os.path.splitext(filename)[1].lower()
        content_types = {
            ".txt": "text/plain",
            ".html": "text/html",
            ".htm": "text/html",
            ".css": "text/css",
            ".js": "application/javascript",
            ".json": "application/json",
            ".xml": "application/xml",
            ".pdf": "application/pdf",
            ".zip": "application/zip",
            ".gz": "application/gzip",
            ".tar": "application/x-tar",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".gif": "image/gif",
            ".svg": "image/svg+xml",
            ".mp3": "audio/mpeg",
            ".mp4": "video/mp4",
            ".csv": "text/csv",
        }
        return content_types.get(ext, "application/octet-stream")

    def _build_ssl_context(self, context: dict[str, Any]) -> ssl.SSLContext | None:
        """Build SSL context for HTTPS requests."""
        verify_ssl = context.get("verify_ssl", True)
        ca_cert = context.get("ca_cert")
        client_cert = context.get("client_cert")
        client_key = context.get("client_key")

        # No SSL customization needed
        if verify_ssl and not ca_cert and not client_cert:
            return None

        # Create context
        if verify_ssl:
            ssl_context = ssl.create_default_context()
            if ca_cert:
                ssl_context.load_verify_locations(ca_cert)
        else:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        # Client certificate (mTLS)
        if client_cert:
            ssl_context.load_cert_chain(
                certfile=client_cert,
                keyfile=client_key,
            )

        return ssl_context

    def _process_response(
        self,
        response: HTTPResponse | HTTPError | None,
        context: dict[str, Any],
        url: str,
        elapsed_ms: int,
        continue_on_failure: bool,
    ) -> TaskResult:
        """Process HTTP response and build TaskResult."""
        if response is None:
            return TaskResult.terminal(error="No response received")

        # Extract status and headers
        response_obj: HTTPResponse | HTTPError
        if isinstance(response, HTTPError):
            status_code = response.code
            response_headers = dict(response.headers)
            response_obj = response
        else:
            status_code = response.status
            response_headers = dict(response.headers)
            response_obj = response

        content_type = response_headers.get("Content-Type", "")
        content_length = int(response_headers.get("Content-Length", 0))

        # Get final URL (after redirects)
        final_url = response_obj.geturl() if hasattr(response_obj, "geturl") else url

        # Download to file or read body
        download_to = context.get("download_to")
        max_size = context.get("max_response_size", DEFAULT_MAX_RESPONSE_SIZE)
        parse_json = context.get("parse_json", False)

        body: str = ""
        body_json: dict | list | None = None

        try:
            if download_to:
                # Stream to file
                bytes_written = 0
                with open(download_to, "wb") as f:
                    while chunk := response_obj.read(CHUNK_SIZE):
                        bytes_written += len(chunk)
                        if bytes_written > max_size:
                            raise ValueError(f"Response exceeds max size ({max_size} bytes)")
                        f.write(chunk)
                body = download_to
                content_length = bytes_written
            else:
                # Read body
                body_bytes = response_obj.read(max_size + 1)
                if len(body_bytes) > max_size:
                    raise ValueError(f"Response exceeds max size ({max_size} bytes)")

                # Decode body
                charset = self._get_charset(content_type)
                body = body_bytes.decode(charset, errors="replace")
                content_length = len(body_bytes)

                # Parse JSON if requested
                if parse_json:
                    try:
                        body_json = json.loads(body)
                    except json.JSONDecodeError:
                        pass  # Leave body_json as None

        except ValueError as e:
            if continue_on_failure:
                return TaskResult.failed_continue(
                    error=str(e),
                    outputs={"status_code": status_code, "url": final_url},
                )
            return TaskResult.terminal(error=str(e))

        # Build outputs
        outputs: dict[str, Any] = {
            "status_code": status_code,
            "headers": response_headers,
            "body": body,
            "elapsed_ms": elapsed_ms,
            "url": final_url,
            "content_type": content_type,
            "content_length": content_length,
        }

        if body_json is not None:
            outputs["body_json"] = body_json

        # Check expected status
        expected_status = context.get("expected_status")
        if expected_status is not None:
            if isinstance(expected_status, int):
                expected_status = [expected_status]

            if status_code not in expected_status:
                error_msg = f"Unexpected status {status_code}, expected {expected_status}"
                if continue_on_failure:
                    return TaskResult.failed_continue(error=error_msg, outputs=outputs)
                return TaskResult.terminal(error=error_msg, context=outputs)
            else:
                # Status is in expected list - this is success
                logger.debug("HTTPTask success (expected status %s)", status_code)
                return TaskResult.success(outputs=outputs)

        # Success for 2xx, failure otherwise
        if 200 <= status_code < 300:
            logger.debug("HTTPTask success: %s", status_code)
            return TaskResult.success(outputs=outputs)
        else:
            error_msg = f"HTTP {status_code}"
            if continue_on_failure:
                return TaskResult.failed_continue(error=error_msg, outputs=outputs)
            return TaskResult.terminal(error=error_msg, context=outputs)

    def _get_charset(self, content_type: str) -> str:
        """Extract charset from Content-Type header."""
        if "charset=" in content_type:
            match = re.search(r"charset=([^\s;]+)", content_type)
            if match:
                return match.group(1).strip("\"'")
        return "utf-8"

    def _substitute_placeholders(
        self,
        text: str,
        context: dict[str, Any],
        secrets: list[str],
    ) -> str:
        """Replace {key} placeholders with context values."""

        def replacer(match: re.Match[str]) -> str:
            key = match.group(1)
            if key in context:
                value: str = str(context[key])
                return value
            return match.group(0)  # Keep original if not found

        return re.sub(r"\{(\w+)\}", replacer, text)

    def _mask_secrets(
        self,
        text: str,
        context: dict[str, Any],
        secrets: list[str],
    ) -> str:
        """Mask secret values in text for logging."""
        masked = text
        for key in secrets:
            if key in context:
                value = str(context[key])
                if value:
                    masked = masked.replace(value, "***")
        return masked

    def _format_error(self, error: Exception) -> str:
        """Format exception as error message."""
        if isinstance(error, URLError):
            return f"URL error: {error.reason}"
        elif isinstance(error, TimeoutError):
            return "Request timed out"
        elif isinstance(error, OSError):
            return f"Connection error: {error}"
        else:
            return str(error)
