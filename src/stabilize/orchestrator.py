from __future__ import annotations
from typing import TYPE_CHECKING
from stabilize.queue.messages import (
    CancelWorkflow,
    RestartStage,
    ResumeStage,
    StartWorkflow,
)

class Orchestrator:
    """
    Runner for pipeline executions.

    Provides methods to start, cancel, restart, and resume executions
    by pushing appropriate messages to the queue.
    """
    def __init__(self, queue: Queue) -> None:
        """
        Initialize the runner.

        Args:
            queue: The message queue
        """
        self.queue = queue

    def start(self, execution: Workflow) -> None:
        """
        Start a pipeline execution.

        Args:
            execution: The execution to start
        """
        self.queue.push(
            StartWorkflow(
                execution_type=execution.type.value,
                execution_id=execution.id,
            )
        )

    def cancel(
        self,
        execution: Workflow,
        user: str,
        reason: str,
    ) -> None:
        """
        Cancel a running execution.

        Args:
            execution: The execution to cancel
            user: Who is canceling
            reason: Why it's being canceled
        """
        self.queue.push(
            CancelWorkflow(
                execution_type=execution.type.value,
                execution_id=execution.id,
                user=user,
                reason=reason,
            )
        )
