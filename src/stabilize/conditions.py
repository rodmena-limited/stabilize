"""
Structured status conditions for workflows, stages, and tasks.

This module provides a condition-based status system inspired by Kubernetes
conditions. Conditions provide:
- Detailed status information with reasons and messages
- Timestamp tracking for state transitions
- Multiple condition types per entity

Example:
    stage.add_condition(Condition.ready(
        status=True,
        reason="TasksSucceeded",
        message="All tasks completed successfully"
    ))
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


class ConditionType(Enum):
    """Standard condition types."""

    # Ready indicates the entity is ready to serve/proceed
    READY = "Ready"

    # Progressing indicates work is in progress
    PROGRESSING = "Progressing"

    # Degraded indicates reduced functionality
    DEGRADED = "Degraded"

    # Available indicates the entity is available
    AVAILABLE = "Available"

    # Verified indicates verification has passed
    VERIFIED = "Verified"

    # Failed indicates a failure condition
    FAILED = "Failed"

    # ConfigValid indicates configuration is valid
    CONFIG_VALID = "ConfigValid"


class ConditionReason(Enum):
    """Standard condition reasons."""

    # Success reasons
    TASKS_SUCCEEDED = "TasksSucceeded"
    VERIFICATION_PASSED = "VerificationPassed"
    CONFIG_VALID = "ConfigValid"
    STAGE_COMPLETED = "StageCompleted"
    WORKFLOW_COMPLETED = "WorkflowCompleted"

    # Progress reasons
    INITIALIZING = "Initializing"
    IN_PROGRESS = "InProgress"
    WAITING_FOR_UPSTREAM = "WaitingForUpstream"
    VERIFYING = "Verifying"

    # Failure reasons
    TASK_FAILED = "TaskFailed"
    VERIFICATION_FAILED = "VerificationFailed"
    CONFIG_ERROR = "ConfigError"
    TIMEOUT = "Timeout"
    CANCELED = "Canceled"
    UPSTREAM_FAILED = "UpstreamFailed"
    UNKNOWN_ERROR = "UnknownError"


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
    message: str = ""
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

    # ========== Factory Methods ==========

    @classmethod
    def ready(
        cls,
        status: bool,
        reason: ConditionReason | str,
        message: str = "",
    ) -> Condition:
        """Create a Ready condition."""
        return cls(
            type=ConditionType.READY,
            status=status,
            reason=reason,
            message=message,
        )

    @classmethod
    def progressing(
        cls,
        status: bool,
        reason: ConditionReason | str,
        message: str = "",
    ) -> Condition:
        """Create a Progressing condition."""
        return cls(
            type=ConditionType.PROGRESSING,
            status=status,
            reason=reason,
            message=message,
        )

    @classmethod
    def verified(
        cls,
        status: bool,
        reason: ConditionReason | str,
        message: str = "",
    ) -> Condition:
        """Create a Verified condition."""
        return cls(
            type=ConditionType.VERIFIED,
            status=status,
            reason=reason,
            message=message,
        )

    @classmethod
    def failed(
        cls,
        reason: ConditionReason | str,
        message: str,
    ) -> Condition:
        """Create a Failed condition (always status=True when failed)."""
        return cls(
            type=ConditionType.FAILED,
            status=True,
            reason=reason,
            message=message,
        )

    @classmethod
    def config_valid(
        cls,
        status: bool,
        reason: ConditionReason | str = ConditionReason.CONFIG_VALID,
        message: str = "",
    ) -> Condition:
        """Create a ConfigValid condition."""
        return cls(
            type=ConditionType.CONFIG_VALID,
            status=status,
            reason=reason,
            message=message,
        )

    # ========== Utility Methods ==========

    def update(
        self,
        status: bool | None = None,
        reason: ConditionReason | str | None = None,
        message: str | None = None,
    ) -> Condition:
        """
        Create an updated condition with new values.

        Only updates last_transition_time if status actually changed.

        Args:
            status: New status (or None to keep current)
            reason: New reason (or None to keep current)
            message: New message (or None to keep current)

        Returns:
            New Condition with updated values
        """
        new_status = status if status is not None else self.status
        new_reason = reason if reason is not None else self.reason
        new_message = message if message is not None else self.message

        # Only update transition time if status changed
        transition_time = (
            datetime.now(UTC) if status is not None and status != self.status else self.last_transition_time
        )

        return Condition(
            type=self.type,
            status=new_status,
            reason=new_reason,
            message=new_message,
            last_transition_time=transition_time,
            observed_generation=self.observed_generation + 1,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "type": self.type.value if isinstance(self.type, ConditionType) else self.type,
            "status": self.status,
            "reason": self.reason.value if isinstance(self.reason, ConditionReason) else self.reason,
            "message": self.message,
            "lastTransitionTime": self.last_transition_time.isoformat(),
            "observedGeneration": self.observed_generation,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Condition:
        """Create from dictionary."""
        transition_time = data.get("lastTransitionTime")
        if isinstance(transition_time, str):
            transition_time = datetime.fromisoformat(transition_time)
        elif transition_time is None:
            transition_time = datetime.now(UTC)

        return cls(
            type=data["type"],
            status=data["status"],
            reason=data["reason"],
            message=data.get("message", ""),
            last_transition_time=transition_time,
            observed_generation=data.get("observedGeneration", 0),
        )


class ConditionSet:
    """
    A collection of conditions with convenient access methods.

    Example:
        conditions = ConditionSet()
        conditions.set(Condition.ready(True, "AllGood", "Everything is fine"))
        conditions.set(Condition.progressing(False, "Complete", "Done"))

        if conditions.is_ready:
            print("Ready!")
    """

    def __init__(self, conditions: list[Condition] | None = None) -> None:
        """Initialize with optional conditions."""
        self._conditions: dict[ConditionType | str, Condition] = {}
        if conditions:
            for c in conditions:
                self._conditions[c.type] = c

    def set(self, condition: Condition) -> None:
        """
        Set or update a condition.

        If a condition of the same type exists, it's replaced.
        """
        self._conditions[condition.type] = condition

    def get(self, condition_type: ConditionType | str) -> Condition | None:
        """Get a condition by type."""
        return self._conditions.get(condition_type)

    def remove(self, condition_type: ConditionType | str) -> None:
        """Remove a condition by type."""
        self._conditions.pop(condition_type, None)

    def all(self) -> list[Condition]:
        """Get all conditions."""
        return list(self._conditions.values())

    def clear(self) -> None:
        """Remove all conditions."""
        self._conditions.clear()

    # ========== Convenience Properties ==========

    @property
    def is_ready(self) -> bool:
        """Check if Ready condition is True."""
        ready = self.get(ConditionType.READY)
        return ready.status if ready else False

    @property
    def is_progressing(self) -> bool:
        """Check if Progressing condition is True."""
        progressing = self.get(ConditionType.PROGRESSING)
        return progressing.status if progressing else False

    @property
    def is_verified(self) -> bool:
        """Check if Verified condition is True."""
        verified = self.get(ConditionType.VERIFIED)
        return verified.status if verified else False

    @property
    def has_failed(self) -> bool:
        """Check if Failed condition exists and is True."""
        failed = self.get(ConditionType.FAILED)
        return failed.status if failed else False

    @property
    def is_config_valid(self) -> bool:
        """Check if ConfigValid condition is True."""
        config = self.get(ConditionType.CONFIG_VALID)
        return config.status if config else True  # Default to valid if not set

    def to_list(self) -> list[dict[str, Any]]:
        """Convert all conditions to list of dicts for serialization."""
        return [c.to_dict() for c in self._conditions.values()]

    @classmethod
    def from_list(cls, data: list[dict[str, Any]]) -> ConditionSet:
        """Create from list of dicts."""
        conditions = [Condition.from_dict(d) for d in data]
        return cls(conditions)

    def __len__(self) -> int:
        return len(self._conditions)

    def __contains__(self, condition_type: ConditionType | str) -> bool:
        return condition_type in self._conditions

    def __iter__(self) -> Iterator[Condition]:
        return iter(self._conditions.values())
