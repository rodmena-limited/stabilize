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
