"""Error utility functions."""

from __future__ import annotations

import errno
import http.client
import socket
import urllib.error

from stabilize.errors.chain import next_link
from stabilize.errors.permanent import PermanentError
from stabilize.errors.sqlstates import is_transient_sqlstate, sqlstate
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


_PERMANENT_NAME_PATTERNS = ("validation", "authentication", "authorization", "forbidden", "notfound", "invalid")

_TRANSIENT_NAME_PATTERNS = ("timeout", "connection", "temporary", "unavailable", "retry", "throttl", "ratelimit")


def _classify(error: BaseException) -> bool | None:
    """True transient, False permanent, None undecided for this one exception."""
    if isinstance(error, TransientError):
        return True
    if isinstance(error, PermanentError):
        return False
    if isinstance(error, _TRANSIENT_STDLIB_TYPES):
        return True
    if isinstance(error, OSError) and error.errno in _TRANSIENT_ERRNO_VALUES:
        return True
    if is_transient_sqlstate(sqlstate(error)):
        return True
    name = type(error).__name__.lower()
    if any(pattern in name for pattern in _PERMANENT_NAME_PATTERNS):
        return False
    if any(pattern in name for pattern in _TRANSIENT_NAME_PATTERNS):
        return True
    return None


_PERMANENT_TYPES = (ValueError, TypeError, AttributeError, KeyError, IndexError)


def _decide(error: Exception) -> bool | None:
    """The first decision along the cause chain; None when nothing in it is classified.

    An implicit ``__context__`` is followed only past an exception that is not permanent
    by type: a ValueError raised while handling a transient failure is a bug, not a retry.
    """
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        verdict = _classify(current)
        if verdict is not None:
            return verdict
        following, implicit = next_link(current)
        if implicit and isinstance(current, _PERMANENT_TYPES):
            return False
        current = following
    return None


def is_transient(error: Exception) -> bool:
    """Whether a failure should be retried.

    The exception and its causes are examined outermost first: ``__cause__``,
    then ``StabilizeError.cause``, then ``__context__`` unless suppressed. The first
    classified exception decides, so a permanent outer error is never retried for a
    transient cause. An explicit ``TransientError`` or ``PermanentError`` wins over
    the type and name rules; PostgreSQL SQLSTATEs for contention, lock timeouts and
    connection refusals count as transient (``STABILIZE_RETRY_ON_SHUTDOWN=1`` adds
    server shutdown and query cancellation).
    """
    return _decide(error) is True


def is_permanent(error: Exception) -> bool:
    """Whether a failure must not be retried; the mirror of ``is_transient``."""
    if isinstance(error, PermanentError):
        return True
    if isinstance(error, TransientError):
        return False
    if isinstance(error, _PERMANENT_TYPES):
        return True
    return _decide(error) is False


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
