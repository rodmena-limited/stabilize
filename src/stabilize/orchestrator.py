"""
Orchestrator - starts and manages pipeline executions.

This module provides the main entry point for running pipelines.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from stabilize.queue.messages import (
    CancelWorkflow,
    RestartStage,
    ResumeStage,
    StartWorkflow,
)

if TYPE_CHECKING:
    from stabilize.models.workflow import Workflow
    from stabilize.queue.queue import Queue


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

    def restart(
        self,
        execution: Workflow,
        stage_id: str,
    ) -> None:
        """
        Restart a stage in an execution.

        Args:
            execution: The execution
            stage_id: The stage to restart
        """
        self.queue.push(
            RestartStage(
                execution_type=execution.type.value,
                execution_id=execution.id,
                stage_id=stage_id,
            )
        )

    def unpause(self, execution: Workflow) -> None:
        """
        Resume a paused execution.

        Args:
            execution: The execution to resume
        """
        # Resume all paused stages
        for stage in execution.stages:
            if stage.status.name == "PAUSED":
                self.queue.push(
                    ResumeStage(
                        execution_type=execution.type.value,
                        execution_id=execution.id,
                        stage_id=stage.id,
                    )
                )
