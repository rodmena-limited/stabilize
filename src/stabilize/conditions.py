from __future__ import annotations
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

class ConditionType(Enum):
    """Standard condition types."""
    READY = 'Ready'
    PROGRESSING = 'Progressing'
    DEGRADED = 'Degraded'
    AVAILABLE = 'Available'
    VERIFIED = 'Verified'
    FAILED = 'Failed'
    CONFIG_VALID = 'ConfigValid'

class ConditionReason(Enum):
    """Standard condition reasons."""
    TASKS_SUCCEEDED = 'TasksSucceeded'
    VERIFICATION_PASSED = 'VerificationPassed'
    CONFIG_VALID = 'ConfigValid'
    STAGE_COMPLETED = 'StageCompleted'
    WORKFLOW_COMPLETED = 'WorkflowCompleted'
    INITIALIZING = 'Initializing'
    IN_PROGRESS = 'InProgress'
    WAITING_FOR_UPSTREAM = 'WaitingForUpstream'
    VERIFYING = 'Verifying'
    TASK_FAILED = 'TaskFailed'
    VERIFICATION_FAILED = 'VerificationFailed'
    CONFIG_ERROR = 'ConfigError'
    TIMEOUT = 'Timeout'
    CANCELED = 'Canceled'
    UPSTREAM_FAILED = 'UpstreamFailed'
    UNKNOWN_ERROR = 'UnknownError'

@dataclass
class Condition:
    """
    A condition representing the state of an aspect of an entity.

    Attributes:
        type: The type of condition (e.g., Ready, Progressing)
        status: True if the condition is satisfied
        reason: Machine-readable reason for the status
        message: Human-readable message explaining the status
        last_transition_time: When the condition last changed
        observed_generation: The generation of the entity when observed
    """
    type: ConditionType | str
    status: bool
    reason: ConditionReason | str
    message: str = ''
    last_transition_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    observed_generation: int = 0

    def __post_init__(self) -> None:
        """Convert string types to enums if possible."""
        if isinstance(self.type, str):
            try:
                self.type = ConditionType(self.type)
            except ValueError:
                pass  # Keep as string for custom types

        if isinstance(self.reason, str):
            try:
                self.reason = ConditionReason(self.reason)
            except ValueError:
                pass  # Keep as string for custom reasons
