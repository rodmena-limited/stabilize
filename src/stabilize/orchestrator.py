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
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue


class Orchestrator:
    """
    Runner for pipeline executions.

    Provides methods to start, cancel, restart, and resume executions
    by pushing appropriate messages to the queue atomically with the store.

    When a store is provided, all queue operations are wrapped in transactions
    to ensure atomicity - preventing orphaned workflow state if queue push fails.
    """

    def __init__(
        self,
        queue: Queue,
        store: WorkflowStore | None = None,
    ) -> None:
        """
        Initialize the runner.

        Args:
            queue: The message queue
            store: Optional workflow store for atomic operations. When provided,
                   queue operations are wrapped in store transactions.
        """
        self.queue = queue
        self.store = store

    def start(self, execution: Workflow) -> None:
        """
        Start a pipeline execution atomically.

        If a store is configured, wraps the queue push in a transaction
        to ensure atomicity with any prior status updates.

        Args:
            execution: The execution to start
        """
        message = StartWorkflow(
            execution_type=execution.type.value,
            execution_id=execution.id,
        )

        if self.store:
            with self.store.transaction(self.queue) as txn:
                txn.push_message(message)
        else:
            self.queue.push(message)

    def cancel(
        self,
        execution: Workflow,
        user: str,
        reason: str,
    ) -> None:
        """
        Cancel a running execution atomically.

        Args:
            execution: The execution to cancel
            user: Who is canceling
            reason: Why it's being canceled
        """
        message = CancelWorkflow(
            execution_type=execution.type.value,
            execution_id=execution.id,
            user=user,
            reason=reason,
        )

        if self.store:
            with self.store.transaction(self.queue) as txn:
                txn.push_message(message)
        else:
            self.queue.push(message)

    def restart(
        self,
        execution: Workflow,
        stage_id: str,
    ) -> None:
        """
        Restart a stage in an execution atomically.

        Args:
            execution: The execution
            stage_id: The stage to restart
        """
        message = RestartStage(
            execution_type=execution.type.value,
            execution_id=execution.id,
            stage_id=stage_id,
        )

        if self.store:
            with self.store.transaction(self.queue) as txn:
                txn.push_message(message)
        else:
            self.queue.push(message)

    def unpause(self, execution: Workflow) -> None:
        """
        Resume a paused execution atomically.

        Loads fresh execution state from store (if available) to prevent
        stale read issues where the in-memory execution doesn't reflect
        recent changes made by other processes.

        Args:
            execution: The execution to resume
        """
        # Load fresh state if store available to prevent stale reads
        if self.store:
            fresh_execution = self.store.retrieve(execution.id)
            if fresh_execution:
                execution = fresh_execution

        # Collect all resume messages
        messages = []
        for stage in execution.stages:
            if stage.status.name == "PAUSED":
                messages.append(
                    ResumeStage(
                        execution_type=execution.type.value,
                        execution_id=execution.id,
                        stage_id=stage.id,
                    )
                )

        # Push all messages atomically if store available
        if self.store and messages:
            with self.store.transaction(self.queue) as txn:
                for msg in messages:
                    txn.push_message(msg)
        else:
            for msg in messages:
                self.queue.push(msg)
