from __future__ import annotations
from typing import TYPE_CHECKING, Any, TypeVar
T = TypeVar("T")

class StabilizeError(Exception):
    """Base exception for Stabilize errors."""

class StabilizeFatalError(StabilizeError):
    """
    Fatal error that should terminate the pipeline.

    Fatal errors indicate unrecoverable situations like invalid configuration
    or programming errors.
    """

class StabilizeExpectedError(StabilizeError):
    """
    Expected error that may allow retry or continuation.

    Expected errors occur during normal operation and may be recoverable.
    """

class PreconditionError(StabilizeExpectedError):
    """
    Error raised when a precondition is not met.

    Precondition errors typically indicate that the stage should be
    retried or that an upstream dependency hasn't completed.
    """
    def __init__(self, message: str, key: str | None = None) -> None:
        super().__init__(message)
        self.key = key

class ContextError(StabilizeFatalError):
    """
    Error raised when required context is missing or invalid.

    Context errors are fatal because they indicate misconfiguration.
    """
    def __init__(self, message: str, key: str | None = None) -> None:
        super().__init__(message)
        self.key = key

class OutputError(StabilizeExpectedError):
    """
    Error raised when expected output is missing or invalid.

    Output errors may be recoverable if the task can be retried.
    """
    def __init__(self, message: str, key: str | None = None) -> None:
        super().__init__(message)
        self.key = key

class ConfigError(StabilizeFatalError):
    """
    Error raised when configuration is invalid.

    Config errors are fatal because they indicate invalid setup.
    """
    def __init__(self, message: str, field: str | None = None) -> None:
        super().__init__(message)
        self.field = field

class VerificationError(StabilizeExpectedError):
    """
    Error raised when verification fails.

    Verification errors may allow retry depending on the verifier.
    """
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = details or {}
