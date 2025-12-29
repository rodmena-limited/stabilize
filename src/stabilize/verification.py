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
