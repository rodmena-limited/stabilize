"""Error utility functions."""

from __future__ import annotations

from stabilize.errors.permanent import PermanentError
from stabilize.errors.transient import TransientError


def is_transient(error: Exception) -> bool:
    """Check if an error is transient and should be retried.

    Checks the error itself and its cause chain (__cause__) for transient errors.
    This handles cases where libraries wrap errors (e.g., BulkheadError wrapping
    a TransientError).

    Args:
        error: The exception to check

    Returns:
        True if the error is transient and should be retried
    """
    if isinstance(error, TransientError):
        return True

    # Check for common transient errors from other libraries
    error_name = type(error).__name__.lower()
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

    # Check for common permanent errors
    error_name = type(error).__name__.lower()
    permanent_patterns = [
        "validation",
        "authentication",
        "authorization",
        "forbidden",
        "notfound",
        "invalid",
    ]
    if any(pattern in error_name for pattern in permanent_patterns):
        return True

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
