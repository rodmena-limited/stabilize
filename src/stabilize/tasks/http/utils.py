"""HTTP task utility functions."""

from __future__ import annotations

import os
import re
from typing import Any
from urllib.error import URLError

from stabilize.tasks.http.constants import CONTENT_TYPES


def substitute_placeholders(
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


def mask_secrets(
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


def get_charset(content_type: str) -> str:
    """Extract charset from Content-Type header."""
    if "charset=" in content_type:
        match = re.search(r"charset=([^\s;]+)", content_type)
        if match:
            return match.group(1).strip("\"'")
    return "utf-8"


def format_error(error: Exception) -> str:
    """Format exception as error message."""
    if isinstance(error, URLError):
        return f"URL error: {error.reason}"
    elif isinstance(error, TimeoutError):
        return "Request timed out"
    elif isinstance(error, OSError):
        return f"Connection error: {error}"
    else:
        return str(error)


def guess_content_type(filename: str) -> str:
    """Guess content type from filename extension."""
    ext = os.path.splitext(filename)[1].lower()
    return CONTENT_TYPES.get(ext, "application/octet-stream")
