"""Error utility functions."""

from __future__ import annotations

import errno
import http.client
import socket
import urllib.error

from stabilize.errors.permanent import PermanentError
from stabilize.errors.transient import TransientError

_TRANSIENT_STDLIB_TYPES = (
    ConnectionError,
    TimeoutError,
    socket.timeout,
    socket.gaierror,
    http.client.RemoteDisconnected,
    urllib.error.URLError,
)

_TRANSIENT_ERRNO_VALUES = frozenset(
    {
        errno.ECONNREFUSED,
        errno.ECONNRESET,
        errno.ECONNABORTED,
        errno.ETIMEDOUT,
        errno.EHOSTUNREACH,
        errno.ENETUNREACH,
    }
)


def is_transient(error: Exception) -> bool:
    """Check if an error is transient and should be retried.

    Checks the error itself and its cause chain (__cause__) for transient errors.
    This handles cases where libraries wrap errors (e.g., BulkheadError wrapping
    a TransientError).

    IMPORTANT: Permanent classification takes precedence over transient when both
    match via class-name heuristics.  For example, ``ConnectionValidationError``
    contains both "connection" (transient pattern) and "validation" (permanent
    pattern).  Without the guard, such an error would be silently retried forever.

    Args:
        error: The exception to check

    Returns:
        True if the error is transient and should be retried
    """
    if isinstance(error, TransientError):
        return True

    # Permanent type-check takes strict precedence over transient heuristics
    if isinstance(error, PermanentError):
        return False

    if isinstance(error, _TRANSIENT_STDLIB_TYPES):
        return True

    if isinstance(error, OSError) and error.errno in _TRANSIENT_ERRNO_VALUES:
        return True

    error_name = type(error).__name__.lower()

    # Check permanent patterns FIRST — if the class name matches a permanent
    # pattern, it must NOT be classified as transient regardless of whether a
    # transient pattern also matches.
    permanent_name_patterns = (
        "validation",
        "authentication",
        "authorization",
        "forbidden",
        "notfound",
        "invalid",
    )
    if any(pattern in error_name for pattern in permanent_name_patterns):
        return False

    transient_patterns = [
        "timeout",
        "connection",
        "temporary",
        "unavailable",
        "retry",
        "throttl",
        "ratelimit",
    ]
    if any(pattern in error_name for pattern in transient_patterns):
        return True

    # Check the cause chain for wrapped transient errors
    cause = getattr(error, "__cause__", None)
    if cause is not None and cause is not error:
        return is_transient(cause)

    return False


def is_permanent(error: Exception) -> bool:
    """Check if an error is permanent and should not be retried.

    Checks the error itself and its cause chain (__cause__) for permanent errors.
    This handles cases where libraries wrap errors.

    Args:
        error: The exception to check

    Returns:
        True if the error is permanent and should not be retried
    """
    if isinstance(error, PermanentError):
        return True

    # Explicit TransientError must never be classified as permanent
    if isinstance(error, TransientError):
        return False

    if isinstance(error, (ValueError, TypeError, AttributeError, KeyError, IndexError)):
        return True

    error_name = type(error).__name__.lower()

    # Permanent patterns checked FIRST — mirrors is_transient() where permanent
    # also wins.  Ambiguous names like ConnectionValidationError must be
    # classified as permanent (same precedence in both functions).
    permanent_patterns = (
        "validation",
        "authentication",
        "authorization",
        "forbidden",
        "notfound",
        "invalid",
    )
    if any(pattern in error_name for pattern in permanent_patterns):
        return True

    transient_name_patterns = (
        "timeout",
        "connection",
        "temporary",
        "unavailable",
        "retry",
        "throttl",
        "ratelimit",
    )
    if any(pattern in error_name for pattern in transient_name_patterns):
        return False

    # Check the cause chain for wrapped permanent errors
    cause = getattr(error, "__cause__", None)
    if cause is not None and cause is not error:
        return is_permanent(cause)

    return False


def truncate_error(message: str, max_bytes: int = 102_400) -> str:
    """Truncate error message to max_bytes, appending '[TRUNCATED]' marker.

    This prevents oversized error messages from bloating the database
    or causing memory issues when persisting exception details.

    Args:
        message: The error message to truncate
        max_bytes: Maximum size in bytes (default: 100KB)

    Returns:
        Original message if within limit, otherwise truncated with marker.
    """
    if not message:
        return message

    encoded = message.encode("utf-8", errors="replace")

    if len(encoded) <= max_bytes:
        return message

    marker = " [TRUNCATED]"
    marker_bytes = len(marker.encode("utf-8"))
    target_bytes = max_bytes - marker_bytes

    if target_bytes <= 0:
        return marker.strip()

    truncated_bytes = encoded[:target_bytes]

    try:
        truncated = truncated_bytes.decode("utf-8", errors="ignore")
    except UnicodeDecodeError:
        for i in range(1, 5):
            try:
                truncated = truncated_bytes[:-i].decode("utf-8")
                break
            except UnicodeDecodeError:
                continue
        else:
            truncated = truncated_bytes.decode("utf-8", errors="replace")

    return truncated + marker
