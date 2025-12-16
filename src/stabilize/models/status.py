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
