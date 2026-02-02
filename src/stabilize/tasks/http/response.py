"""HTTP response processing functions."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from stabilize.tasks.http.constants import CHUNK_SIZE, DEFAULT_MAX_RESPONSE_SIZE
from stabilize.tasks.http.utils import get_charset
from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from http.client import HTTPResponse
    from urllib.error import HTTPError

logger = logging.getLogger(__name__)


def process_response(
    response: HTTPResponse | HTTPError | None,
    context: dict[str, Any],
    url: str,
    elapsed_ms: int,
    continue_on_failure: bool,
) -> TaskResult:
    """Process HTTP response and build TaskResult.

    Args:
        response: HTTP response or error object
        context: Request context with response handling options
        url: Original request URL
        elapsed_ms: Request duration in milliseconds
        continue_on_failure: Whether to return failed_continue on error

    Returns:
        TaskResult with response data or error
    """
    if response is None:
        return TaskResult.terminal(error="No response received")

    # Extract status and headers
    response_obj: HTTPResponse | HTTPError
    if hasattr(response, "code"):
        # HTTPError
        status_code = response.code
        response_headers = dict(response.headers)
        response_obj = response
    else:
        # HTTPResponse
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
    body_json: dict[str, Any] | list[Any] | None = None

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
            charset = get_charset(content_type)
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
