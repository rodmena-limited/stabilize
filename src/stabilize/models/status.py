"""
WorkflowStatus enum.

This enum represents all possible states for executions, stages, and tasks.
Each status has two boolean properties:
- complete: Whether the entity has finished its work (successfully or not)
- halt: Whether downstream execution should be blocked
"""

from enum import Enum


class WorkflowStatus(Enum):
    """
    Execution status enum.

    Each value is a tuple of (name, complete, halt).
    """

    # The task has yet to start
    NOT_STARTED = ("NOT_STARTED", False, False)

    # The task is still running and may be re-executed to continue
    RUNNING = ("RUNNING", False, False)

    # The task is paused and may be resumed to continue
    PAUSED = ("PAUSED", False, False)

    # The task is complete but pipeline should stop pending a trigger
    SUSPENDED = ("SUSPENDED", False, False)

    # The task executed successfully and pipeline may proceed
    SUCCEEDED = ("SUCCEEDED", True, False)

    # The task failed but pipeline may proceed to the next task
    FAILED_CONTINUE = ("FAILED_CONTINUE", True, False)

    # The task failed terminally - pipeline will not progress further
    TERMINAL = ("TERMINAL", True, True)

    # The task was canceled - pipeline will not progress further
    CANCELED = ("CANCELED", True, True)

    # The step completed but indicates a decision path should be followed
    REDIRECT = ("REDIRECT", False, False)

    # The task was stopped - pipeline will not progress further
    STOPPED = ("STOPPED", True, True)

    # The task was skipped and pipeline will proceed to next task
    SKIPPED = ("SKIPPED", True, False)

    # The task is not started and must transition to NOT_STARTED
    BUFFERED = ("BUFFERED", False, False)

    def __init__(self, name: str, complete: bool, halt: bool) -> None:
        self._name = name
        self._complete = complete
        self._halt = halt

    @property
    def is_complete(self) -> bool:
        """
        Indicates that the task/stage/pipeline has finished its work.

        Returns True for: CANCELED, SUCCEEDED, STOPPED, SKIPPED, TERMINAL, FAILED_CONTINUE
        """
        return self._complete

    @property
    def is_halt(self) -> bool:
        """
        Indicates an abnormal completion - nothing downstream should run.

        Returns True for: TERMINAL, CANCELED, STOPPED
        """
        return self._halt

    @property
    def is_successful(self) -> bool:
        """Check if this status represents a successful completion."""
        return self in _SUCCESSFUL_STATUSES

    @property
    def is_failure(self) -> bool:
        """Check if this status represents a failure."""
        return self in _FAILURE_STATUSES

    @property
    def is_skipped(self) -> bool:
        """Check if this status is SKIPPED."""
        return self == WorkflowStatus.SKIPPED

    def __str__(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"WorkflowStatus.{self.name}"


# Status sets for quick membership testing (matching Orca's ImmutableSets)
COMPLETED_STATUSES: frozenset[WorkflowStatus] = frozenset(
    {
        WorkflowStatus.CANCELED,
        WorkflowStatus.SUCCEEDED,
        WorkflowStatus.STOPPED,
        WorkflowStatus.SKIPPED,
        WorkflowStatus.TERMINAL,
        WorkflowStatus.FAILED_CONTINUE,
    }
)

_SUCCESSFUL_STATUSES: frozenset[WorkflowStatus] = frozenset(
    {
        WorkflowStatus.SUCCEEDED,
        WorkflowStatus.STOPPED,
        WorkflowStatus.SKIPPED,
    }
)

_FAILURE_STATUSES: frozenset[WorkflowStatus] = frozenset(
    {
        WorkflowStatus.TERMINAL,
        WorkflowStatus.STOPPED,
        WorkflowStatus.FAILED_CONTINUE,
    }
)

# Statuses that allow downstream stages to continue
CONTINUABLE_STATUSES: frozenset[WorkflowStatus] = frozenset(
    {
        WorkflowStatus.SUCCEEDED,
        WorkflowStatus.FAILED_CONTINUE,
        WorkflowStatus.SKIPPED,
    }
)

# Statuses that indicate the entity is still actively processing
ACTIVE_STATUSES: frozenset[WorkflowStatus] = frozenset(
    {
        WorkflowStatus.NOT_STARTED,
        WorkflowStatus.RUNNING,
        WorkflowStatus.PAUSED,
        WorkflowStatus.SUSPENDED,
    }
)
