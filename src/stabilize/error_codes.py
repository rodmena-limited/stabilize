"""
Structured error codes for Stabilize.

Provides semantic error classification and exception chain traversal
following enterprise patterns from Flyte and similar workflow systems.

Usage:
    from stabilize.error_codes import ErrorCode, error_chain, classify_error

    try:
        do_something()
    except Exception as e:
        code = classify_error(e)
        chain = error_chain(e)
        if code == ErrorCode.NETWORK_ERROR:
            # Handle network errors specially
            pass
"""

from __future__ import annotations

from enum import Enum


class ErrorCode(Enum):
    """Semantic error codes for categorizing exceptions.

    These codes provide a standardized way to classify errors for:
    - Routing decisions (retry vs fail)
    - Alerting and monitoring
    - Error aggregation and reporting
    - API responses

    Each code maps to a specific category of failure that can be
    handled programmatically by downstream systems.
    """

    # General errors
    UNKNOWN = "UNKNOWN"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    USER_CODE_ERROR = "USER_CODE_ERROR"

    # Resource errors
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"

    # Task errors
    TASK_TIMEOUT = "TASK_TIMEOUT"
    TASK_NOT_FOUND = "TASK_NOT_FOUND"

    # Dependency errors
    UPSTREAM_DEPENDENCY_FAILED = "UPSTREAM_DEPENDENCY_FAILED"

    # Configuration errors
    CONFIGURATION_INVALID = "CONFIGURATION_INVALID"

    # Concurrency errors
    CONCURRENCY_CONFLICT = "CONCURRENCY_CONFLICT"

    # Auth errors
    AUTHENTICATION_FAILED = "AUTHENTICATION_FAILED"

    # Validation errors
    VALIDATION_FAILED = "VALIDATION_FAILED"

    # Network errors
    NETWORK_ERROR = "NETWORK_ERROR"

    # Resilience pattern errors
    CIRCUIT_OPEN = "CIRCUIT_OPEN"
    BULKHEAD_FULL = "BULKHEAD_FULL"

    # Verification errors
    VERIFICATION_FAILED = "VERIFICATION_FAILED"

    # Recovery errors
    RECOVERY_FAILED = "RECOVERY_FAILED"


def error_chain(error: Exception) -> list[Exception]:
    """Traverse __cause__ chain, return list from root to leaf.

    Returns all exceptions in the cause chain, starting from the
    deepest root cause up to the provided exception.

    Args:
        error: The exception to traverse

    Returns:
        List of exceptions from root cause to the provided exception.
        If no cause chain exists, returns a list with just the error.

    Example:
        try:
            raise ValueError("root") from None
        except ValueError as e1:
            try:
                raise RuntimeError("middle") from e1
            except RuntimeError as e2:
                try:
                    raise TypeError("leaf") from e2
                except TypeError as e3:
                    chain = error_chain(e3)
                    # chain = [ValueError("root"), RuntimeError("middle"), TypeError("leaf")]
    """
    chain: list[Exception] = []
    current: Exception | None = error

    # Build chain from leaf to root
    while current is not None:
        chain.append(current)
        cause = getattr(current, "__cause__", None)
        if cause is current:
            # Prevent infinite loops on self-referential causes
            break
        current = cause

    # Reverse to get root-to-leaf order
    chain.reverse()
    return chain


def find_in_chain(error: Exception, error_type: type) -> Exception | None:
    """Find first error of given type in cause chain.

    Traverses the exception's __cause__ chain looking for an instance
    of the specified type.

    Args:
        error: The exception to search from
        error_type: The type of exception to find

    Returns:
        The first exception of the given type, or None if not found.

    Example:
        try:
            raise ValueError("original")
        except ValueError as e:
            raise RuntimeError("wrapper") from e

        # Later, when catching RuntimeError:
        val_err = find_in_chain(runtime_error, ValueError)
        if val_err:
            print(f"Original error: {val_err}")
    """
    for exc in error_chain(error):
        if isinstance(exc, error_type):
            return exc
    return None


def classify_error(error: Exception) -> ErrorCode:
    """Map any exception to an ErrorCode for routing/alerting.

    Uses a combination of:
    - Explicit error_code attributes on Stabilize exceptions
    - Type-based classification for known exception types
    - Name-based heuristics for unknown exceptions

    Args:
        error: The exception to classify

    Returns:
        The most appropriate ErrorCode for the exception.

    Example:
        try:
            do_work()
        except Exception as e:
            code = classify_error(e)
            if code in {ErrorCode.NETWORK_ERROR, ErrorCode.CONCURRENCY_CONFLICT}:
                # Retry
                pass
            else:
                # Fail permanently
                pass
    """
    # Check for explicit error_code attribute (Stabilize exceptions)
    if hasattr(error, "error_code"):
        return error.error_code  # type: ignore[no-any-return]

    # Check the cause chain for Stabilize exceptions with error_code
    for exc in error_chain(error):
        if hasattr(exc, "error_code"):
            return exc.error_code  # type: ignore[no-any-return]

    # Type-based classification for common exception types
    error_type = type(error).__name__.lower()
    error_module = type(error).__module__.lower() if type(error).__module__ else ""

    # Timeout errors
    if "timeout" in error_type or "timedout" in error_type:
        return ErrorCode.TASK_TIMEOUT

    # Connection/network errors
    if any(
        pattern in error_type
        for pattern in ["connection", "network", "socket", "dns", "http", "url"]
    ):
        return ErrorCode.NETWORK_ERROR

    # Auth errors
    if any(
        pattern in error_type
        for pattern in [
            "authentication",
            "authorization",
            "forbidden",
            "unauthorized",
            "permission",
        ]
    ):
        return ErrorCode.AUTHENTICATION_FAILED

    # Validation errors
    if any(pattern in error_type for pattern in ["validation", "invalid", "parse", "schema"]):
        return ErrorCode.VALIDATION_FAILED

    # Configuration errors
    if any(pattern in error_type for pattern in ["config", "setting", "environment"]):
        return ErrorCode.CONFIGURATION_INVALID

    # Not found errors
    if "notfound" in error_type or "not_found" in error_type:
        return ErrorCode.TASK_NOT_FOUND

    # Resource exhaustion
    if any(
        pattern in error_type for pattern in ["resource", "memory", "quota", "limit", "exhausted"]
    ):
        return ErrorCode.RESOURCE_EXHAUSTED

    # Circuit breaker
    if "circuit" in error_type or "circuitopen" in error_type:
        return ErrorCode.CIRCUIT_OPEN

    # Bulkhead
    if "bulkhead" in error_type or "semaphore" in error_type:
        return ErrorCode.BULKHEAD_FULL

    # Concurrency errors
    if any(
        pattern in error_type for pattern in ["concurrency", "conflict", "optimistic", "version"]
    ):
        return ErrorCode.CONCURRENCY_CONFLICT

    # Check for resilient-circuit errors by module
    if "resilient_circuit" in error_module:
        if "bulkhead" in error_type:
            return ErrorCode.BULKHEAD_FULL
        if "circuit" in error_type:
            return ErrorCode.CIRCUIT_OPEN
        return ErrorCode.SYSTEM_ERROR

    # Default to UNKNOWN for unclassified errors
    return ErrorCode.UNKNOWN
