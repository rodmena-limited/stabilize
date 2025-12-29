from __future__ import annotations
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any
VerifierFunc = Callable[["StageExecution"], VerifyResult]

class VerifyStatus(Enum):
    """Status of a verification check."""
    OK = 'OK'
    RETRY = 'RETRY'
    FAILED = 'FAILED'
    SKIPPED = 'SKIPPED'

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
    message: str = ''
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def ok(cls, message: str = "Verification passed") -> VerifyResult:
        """
        Create a successful verification result.

        Args:
            message: Success message

        Returns:
            A VerifyResult with OK status
        """
        return cls(status=VerifyStatus.OK, message=message)

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

    def skipped(cls, message: str = "Verification skipped") -> VerifyResult:
        """
        Create a skipped verification result.

        Args:
            message: Skip reason

        Returns:
            A VerifyResult with SKIPPED status
        """
        return cls(status=VerifyStatus.SKIPPED, message=message)

    def is_ok(self) -> bool:
        """Check if verification passed."""
        return self.status == VerifyStatus.OK

    def is_retry(self) -> bool:
        """Check if verification should retry."""
        return self.status == VerifyStatus.RETRY

    def is_failed(self) -> bool:
        """Check if verification failed terminally."""
        return self.status == VerifyStatus.FAILED

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
