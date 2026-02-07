"""
Verification system for validating task and stage outputs.

This module provides:
- VerifyResult: Result of a verification check
- Verifier: Base class for custom verifiers
- VerifiableTask: Task interface with built-in verification

Verification runs after task completion but before downstream stages start,
ensuring outputs are valid before dependent stages consume them.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution


class VerifyStatus(Enum):
    """Status of a verification check."""

    # Verification passed
    OK = "OK"

    # Verification failed but can retry
    RETRY = "RETRY"

    # Verification failed terminally
    FAILED = "FAILED"

    # Verification was skipped
    SKIPPED = "SKIPPED"


@dataclass
class VerifyResult:
    """
    Result of a verification check.

    Attributes:
        status: The verification status
        message: Human-readable description of the result
        details: Additional details about the verification
        timestamp: When the verification was performed
    """

    status: VerifyStatus
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    # ========== Factory Methods ==========

    @classmethod
    def ok(cls, message: str = "Verification passed") -> VerifyResult:
        """
        Create a successful verification result.

        Args:
            message: Success message

        Returns:
            A VerifyResult with OK status
        """
        return cls(status=VerifyStatus.OK, message=message)

    @classmethod
    def retry(
        cls,
        message: str = "Verification pending, will retry",
        details: dict[str, Any] | None = None,
    ) -> VerifyResult:
        """
        Create a retry verification result.

        Use when verification cannot complete yet but may succeed later.

        Args:
            message: Retry message
            details: Additional context for the retry

        Returns:
            A VerifyResult with RETRY status
        """
        return cls(
            status=VerifyStatus.RETRY,
            message=message,
            details=details or {},
        )

    @classmethod
    def failed(
        cls,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> VerifyResult:
        """
        Create a failed verification result.

        Args:
            message: Failure message (required)
            details: Additional context for the failure

        Returns:
            A VerifyResult with FAILED status
        """
        return cls(
            status=VerifyStatus.FAILED,
            message=message,
            details=details or {},
        )

    @classmethod
    def skipped(cls, message: str = "Verification skipped") -> VerifyResult:
        """
        Create a skipped verification result.

        Args:
            message: Skip reason

        Returns:
            A VerifyResult with SKIPPED status
        """
        return cls(status=VerifyStatus.SKIPPED, message=message)

    # ========== Utility Properties ==========

    @property
    def is_ok(self) -> bool:
        """Check if verification passed."""
        return self.status == VerifyStatus.OK

    @property
    def is_retry(self) -> bool:
        """Check if verification should retry."""
        return self.status == VerifyStatus.RETRY

    @property
    def is_failed(self) -> bool:
        """Check if verification failed terminally."""
        return self.status == VerifyStatus.FAILED

    @property
    def is_terminal(self) -> bool:
        """Check if verification has reached a terminal state (OK, FAILED, or SKIPPED)."""
        return self.status in {VerifyStatus.OK, VerifyStatus.FAILED, VerifyStatus.SKIPPED}


class Verifier(ABC):
    """
    Base class for custom verifiers.

    Verifiers validate stage outputs after task completion.
    They can be registered in the TaskRegistry and run automatically.

    Example:
        class URLVerifier(Verifier):
            def verify(self, stage: StageExecution) -> VerifyResult:
                url = stage.outputs.get("url")
                if not url:
                    return VerifyResult.failed("No URL in outputs")

                try:
                    response = requests.head(url, timeout=5)
                    if response.ok:
                        return VerifyResult.ok(f"URL {url} is reachable")
                    return VerifyResult.retry(f"URL returned {response.status_code}")
                except Exception as e:
                    return VerifyResult.retry(f"URL check failed: {e}")
    """

    @abstractmethod
    def verify(self, stage: StageExecution) -> VerifyResult:
        """
        Verify the stage outputs.

        Args:
            stage: The stage execution with outputs to verify

        Returns:
            VerifyResult indicating the verification status
        """
        pass

    @property
    def max_retries(self) -> int:
        """
        Maximum number of verification retries.

        Override to change the default.

        Returns:
            Maximum retry count (default: 3)
        """
        return 3

    @property
    def retry_delay_seconds(self) -> float:
        """
        Delay between verification retries in seconds.

        Override to change the default.

        Returns:
            Retry delay (default: 1.0)
        """
        return 1.0


class OutputVerifier(Verifier):
    """
    Verifier that checks for required outputs.

    Example:
        verifier = OutputVerifier(required_keys=["url", "status_code"])
        result = verifier.verify(stage)
    """

    def __init__(
        self,
        required_keys: list[str] | None = None,
        type_checks: dict[str, type] | None = None,
    ) -> None:
        """
        Initialize the output verifier.

        Args:
            required_keys: List of keys that must be present in outputs
            type_checks: Dict mapping keys to expected types
        """
        self.required_keys = required_keys or []
        self.type_checks = type_checks or {}

    def verify(self, stage: StageExecution) -> VerifyResult:
        """Verify that required outputs exist with correct types."""
        missing = []
        type_errors = []

        for key in self.required_keys:
            if key not in stage.outputs:
                missing.append(key)

        for key, expected_type in self.type_checks.items():
            if key in stage.outputs:
                actual = stage.outputs[key]
                if not isinstance(actual, expected_type):
                    type_errors.append(f"{key}: expected {expected_type.__name__}, got {type(actual).__name__}")

        if missing:
            return VerifyResult.failed(
                message=f"Missing required outputs: {', '.join(missing)}",
                details={"missing_keys": missing},
            )

        if type_errors:
            return VerifyResult.failed(
                message=f"Type errors: {'; '.join(type_errors)}",
                details={"type_errors": type_errors},
            )

        return VerifyResult.ok("All required outputs present with correct types")


class CallableVerifier(Verifier):
    """
    Verifier that wraps a callable function.

    Example:
        def check_url(stage):
            url = stage.outputs.get("url")
            return VerifyResult.ok() if url else VerifyResult.failed("No URL")

        verifier = CallableVerifier(check_url)
    """

    def __init__(
        self,
        func: Callable[[StageExecution], VerifyResult],
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        """
        Initialize with a callable.

        Args:
            func: The verification function
            max_retries: Maximum retry count
            retry_delay: Delay between retries in seconds
        """

        self._func = func
        self._max_retries = max_retries
        self._retry_delay = retry_delay

    def verify(self, stage: StageExecution) -> VerifyResult:
        """Execute the wrapped function."""
        return self._func(stage)

    @property
    def max_retries(self) -> int:
        return self._max_retries

    @property
    def retry_delay_seconds(self) -> float:
        return self._retry_delay


# Type alias for verifier function
VerifierFunc = Callable[["StageExecution"], VerifyResult]
