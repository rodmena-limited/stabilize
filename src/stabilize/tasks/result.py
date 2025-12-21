from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any
from stabilize.models.status import WorkflowStatus

@dataclass
class TaskResult:
    """
    Result of a task execution.

    Tasks return a TaskResult to indicate their status and provide
    data to downstream stages.

    Attributes:
        status: The execution status after the task runs
        context: Data scoped to the current stage (merged into stage.context)
        outputs: Data available to downstream stages (merged into stage.outputs)
    """
    status: WorkflowStatus
    context: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, Any] = field(default_factory=dict)

    def success(
        cls,
        outputs: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
    ) -> TaskResult:
        """
        Create a successful result.

        Args:
            outputs: Values available to downstream stages
            context: Values scoped to current stage

        Returns:
            A TaskResult with SUCCEEDED status
        """
        return cls(
            status=WorkflowStatus.SUCCEEDED,
            context=context or {},
            outputs=outputs or {},
        )

    def running(
        cls,
        context: dict[str, Any] | None = None,
    ) -> TaskResult:
        """
        Create a running result (task will be re-executed).

        Use this when a task needs to poll/wait for something.
        The task will be re-queued and executed again after a backoff period.

        Args:
            context: Updated context values

        Returns:
            A TaskResult with RUNNING status
        """
        return cls(
            status=WorkflowStatus.RUNNING,
            context=context or {},
        )

    def terminal(
        cls,
        error: str,
        context: dict[str, Any] | None = None,
    ) -> TaskResult:
        """
        Create a terminal failure result.

        The stage and pipeline will fail.

        Args:
            error: Error message
            context: Additional context

        Returns:
            A TaskResult with TERMINAL status
        """
        ctx = context or {}
        ctx["error"] = error
        return cls(
            status=WorkflowStatus.TERMINAL,
            context=ctx,
        )
