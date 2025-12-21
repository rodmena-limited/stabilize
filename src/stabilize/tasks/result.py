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

    def failed_continue(
        cls,
        error: str,
        outputs: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
    ) -> TaskResult:
        """
        Create a failed result that allows pipeline to continue.

        The stage will be marked as failed but downstream stages will run.

        Args:
            error: Error message
            outputs: Values available to downstream stages
            context: Additional context

        Returns:
            A TaskResult with FAILED_CONTINUE status
        """
        ctx = context or {}
        ctx["error"] = error
        return cls(
            status=WorkflowStatus.FAILED_CONTINUE,
            context=ctx,
            outputs=outputs or {},
        )

    def skipped(cls) -> TaskResult:
        """
        Create a skipped result.

        Returns:
            A TaskResult with SKIPPED status
        """
        return cls(status=WorkflowStatus.SKIPPED)

    def canceled(
        cls,
        outputs: dict[str, Any] | None = None,
    ) -> TaskResult:
        """
        Create a canceled result.

        Args:
            outputs: Final outputs to preserve

        Returns:
            A TaskResult with CANCELED status
        """
        return cls(
            status=WorkflowStatus.CANCELED,
            outputs=outputs or {},
        )

    def stopped(
        cls,
        outputs: dict[str, Any] | None = None,
    ) -> TaskResult:
        """
        Create a stopped result.

        Args:
            outputs: Final outputs to preserve

        Returns:
            A TaskResult with STOPPED status
        """
        return cls(
            status=WorkflowStatus.STOPPED,
            outputs=outputs or {},
        )

    def redirect(
        cls,
        context: dict[str, Any] | None = None,
    ) -> TaskResult:
        """
        Create a redirect result.

        Indicates a decision branch should be followed.

        Args:
            context: Context for the redirect

        Returns:
            A TaskResult with REDIRECT status
        """
        return cls(
            status=WorkflowStatus.REDIRECT,
            context=context or {},
        )

    def builder(cls, status: WorkflowStatus) -> TaskResultBuilder:
        """
        Create a builder for more complex results.

        Args:
            status: The execution status

        Returns:
            A TaskResultBuilder
        """
        return TaskResultBuilder(status)
