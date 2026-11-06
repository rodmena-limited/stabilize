"""HTTP task package for making HTTP requests."""

from __future__ import annotations

from stabilize.tasks.http.constants import (
    CHUNK_SIZE,
    CONTENT_TYPES,
    DEFAULT_MAX_RESPONSE_SIZE,
    DEFAULT_RETRY_ON_STATUS,
    DEFAULT_TIMEOUT,
    SUPPORTED_METHODS,
)
from stabilize.tasks.http.task import HTTPTask

__all__ = [
    "CHUNK_SIZE",
    "CONTENT_TYPES",
    "DEFAULT_MAX_RESPONSE_SIZE",
    "DEFAULT_RETRY_ON_STATUS",
    "DEFAULT_TIMEOUT",
    "SUPPORTED_METHODS",
    "HTTPTask",
]
