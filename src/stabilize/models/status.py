from enum import Enum
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
CONTINUABLE_STATUSES: frozenset[WorkflowStatus] = frozenset(
    {
        WorkflowStatus.SUCCEEDED,
        WorkflowStatus.FAILED_CONTINUE,
        WorkflowStatus.SKIPPED,
    }
)
ACTIVE_STATUSES: frozenset[WorkflowStatus] = frozenset(
    {
        WorkflowStatus.NOT_STARTED,
        WorkflowStatus.RUNNING,
        WorkflowStatus.PAUSED,
        WorkflowStatus.SUSPENDED,
    }
)

class WorkflowStatus(Enum):
    """
    Execution status enum.

    Each value is a tuple of (name, complete, halt).
    """
    NOT_STARTED = ('NOT_STARTED', False, False)
    RUNNING = ('RUNNING', False, False)
    PAUSED = ('PAUSED', False, False)
    SUSPENDED = ('SUSPENDED', False, False)
    SUCCEEDED = ('SUCCEEDED', True, False)
    FAILED_CONTINUE = ('FAILED_CONTINUE', True, False)
    TERMINAL = ('TERMINAL', True, True)
    CANCELED = ('CANCELED', True, True)
    REDIRECT = ('REDIRECT', False, False)
    STOPPED = ('STOPPED', True, True)
    SKIPPED = ('SKIPPED', True, False)
    BUFFERED = ('BUFFERED', False, False)
    def __init__(self, name: str, complete: bool, halt: bool) -> None:
        self._name = name
        self._complete = complete
        self._halt = halt

    def is_complete(self) -> bool:
        """
        Indicates that the task/stage/pipeline has finished its work.

        Returns True for: CANCELED, SUCCEEDED, STOPPED, SKIPPED, TERMINAL, FAILED_CONTINUE
        """
        return self._complete

    def is_halt(self) -> bool:
        """
        Indicates an abnormal completion - nothing downstream should run.

        Returns True for: TERMINAL, CANCELED, STOPPED
        """
        return self._halt

    def is_successful(self) -> bool:
        """Check if this status represents a successful completion."""
        return self in _SUCCESSFUL_STATUSES

    def is_failure(self) -> bool:
        """Check if this status represents a failure."""
        return self in _FAILURE_STATUSES

    def is_skipped(self) -> bool:
        """Check if this status is SKIPPED."""
        return self == WorkflowStatus.SKIPPED
