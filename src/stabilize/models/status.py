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
