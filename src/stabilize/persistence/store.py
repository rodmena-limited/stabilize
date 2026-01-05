"""
WorkflowStore interface.

This module defines the abstract interface for execution persistence.
All storage backends must implement this interface.

Enterprise Features:
- Atomic transactions for store + queue operations (optional)
- Dead letter queue integration
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from stabilize.models.status import WorkflowStatus

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow
    from stabilize.queue.messages import Message
    from stabilize.queue.queue import Queue


class WorkflowNotFoundError(Exception):
    """Raised when an execution cannot be found."""

    def __init__(self, execution_id: str):
        self.execution_id = execution_id
        super().__init__(f"Execution not found: {execution_id}")


@dataclass
class WorkflowCriteria:
    """Criteria for querying executions."""

    page_size: int = 20
    statuses: set[WorkflowStatus] | None = None
    start_time_before: int | None = None
    start_time_after: int | None = None


class WorkflowStore(ABC):
    """Abstract interface for execution persistence."""

    # ========== Execution Operations ==========

    @abstractmethod
    def store(self, execution: Workflow) -> None:
        """
        Store a complete execution.

        Creates the execution and all its stages.

        Args:
            execution: The execution to store
        """
        pass

    @abstractmethod
    def retrieve(self, execution_id: str) -> Workflow:
        """
        Retrieve an execution by ID.

        Args:
            execution_id: The execution ID

        Returns:
            The execution

        Raises:
            WorkflowNotFoundError: If not found
        """
        pass

    @abstractmethod
    def retrieve_execution_summary(self, execution_id: str) -> Workflow:
        """
        Retrieve execution metadata without stages.

        Args:
            execution_id: The execution ID

        Returns:
            The execution with empty stages list

        Raises:
            WorkflowNotFoundError: If not found
        """
        pass

    @abstractmethod
    def update_status(self, execution: Workflow) -> None:
        """
        Update the status of an execution.

        Args:
            execution: The execution with updated status
        """
        pass

    @abstractmethod
    def delete(self, execution_id: str) -> None:
        """
        Delete an execution and all its stages.

        Args:
            execution_id: The execution ID
        """
        pass

    # ========== Stage Operations ==========

    @abstractmethod
    def store_stage(self, stage: StageExecution) -> None:
        """
        Store or update a stage.

        Args:
            stage: The stage to store
        """
        pass

    @abstractmethod
    def add_stage(self, stage: StageExecution) -> None:
        """
        Add a new stage to an execution.

        Args:
            stage: The stage to add
        """
        pass

    @abstractmethod
    def remove_stage(
        self,
        execution: Workflow,
        stage_id: str,
    ) -> None:
        """
        Remove a stage from an execution.

        Args:
            execution: The execution
            stage_id: The stage ID to remove
        """
        pass

    @abstractmethod
    def retrieve_stage(self, stage_id: str) -> StageExecution:
        """
        Retrieve a single stage by ID.

        The returned stage will have a partial parent execution attached
        (containing metadata but no other stages).

        Args:
            stage_id: The stage ID

        Returns:
            The stage execution

        Raises:
            ValueError: If stage not found
        """
        pass

    @abstractmethod
    def get_upstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """
        Get upstream stages for a given stage.

        Args:
            execution_id: The execution ID
            stage_ref_id: The reference ID of the stage

        Returns:
            List of upstream stages
        """
        pass

    @abstractmethod
    def get_downstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """
        Get downstream stages for a given stage.

        Args:
            execution_id: The execution ID
            stage_ref_id: The reference ID of the stage

        Returns:
            List of downstream stages
        """
        pass

    @abstractmethod
    def get_synthetic_stages(
        self,
        execution_id: str,
        parent_stage_id: str,
    ) -> list[StageExecution]:
        """
        Get synthetic stages for a given parent stage.

        Args:
            execution_id: The execution ID
            parent_stage_id: The parent stage ID

        Returns:
            List of synthetic stages
        """
        pass

    @abstractmethod
    def get_merged_ancestor_outputs(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> dict[str, Any]:
        """
        Get merged outputs from all ancestor stages.

        Traverses the DAG upstream, collects outputs, and merges them
        according to topological order (latest wins).

        Args:
            execution_id: The execution ID
            stage_ref_id: The reference ID of the stage

        Returns:
            Merged dictionary of outputs
        """
        pass

    # ========== Query Operations ==========

    @abstractmethod
    def retrieve_by_pipeline_config_id(
        self,
        pipeline_config_id: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """
        Retrieve executions by pipeline config ID.

        Args:
            pipeline_config_id: The pipeline config ID
            criteria: Optional query criteria

        Returns:
            Iterator of matching executions
        """
        pass

    @abstractmethod
    def retrieve_by_application(
        self,
        application: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """
        Retrieve executions by application.

        Args:
            application: The application name
            criteria: Optional query criteria

        Returns:
            Iterator of matching executions
        """
        pass

    # ========== Pause/Resume Operations ==========

    @abstractmethod
    def pause(
        self,
        execution_id: str,
        paused_by: str,
    ) -> None:
        """
        Pause an execution.

        Args:
            execution_id: The execution ID
            paused_by: Who paused it
        """
        pass

    @abstractmethod
    def resume(self, execution_id: str) -> None:
        """
        Resume a paused execution.

        Args:
            execution_id: The execution ID
        """
        pass

    # ========== Cancel Operations ==========

    @abstractmethod
    def cancel(
        self,
        execution_id: str,
        canceled_by: str,
        reason: str,
    ) -> None:
        """
        Cancel an execution.

        Args:
            execution_id: The execution ID
            canceled_by: Who canceled it
            reason: Cancellation reason
        """
        pass

    # ========== Message Deduplication ==========

    def is_message_processed(self, message_id: str) -> bool:
        """
        Check if a message has already been processed.

        Used for idempotency - prevents duplicate message processing.

        Args:
            message_id: The unique message ID

        Returns:
            True if the message has been processed before
        """
        # Default implementation: no deduplication (always returns False)
        return False

    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        """
        Mark a message as successfully processed.

        Args:
            message_id: The unique message ID
            handler_type: Optional handler type for debugging
            execution_id: Optional execution ID for debugging
        """
        # Default implementation: no-op
        pass

    def cleanup_old_processed_messages(self, max_age_hours: float = 24.0) -> int:
        """
        Clean up old processed message records.

        Args:
            max_age_hours: Delete records older than this many hours

        Returns:
            Number of records deleted
        """
        # Default implementation: no cleanup
        return 0

    # ========== Optional Methods ==========

    def is_healthy(self) -> bool:
        """
        Check if the repository is healthy.

        Returns:
            True if healthy
        """
        return True

    def count_by_application(self, application: str) -> int:
        """
        Count executions for an application.

        Args:
            application: The application name

        Returns:
            Number of executions
        """
        return sum(1 for _ in self.retrieve_by_application(application))

    @contextmanager
    def transaction(self, queue: Queue | None = None) -> Iterator[StoreTransaction]:
        """
        Create an atomic transaction for store + queue operations.

        Use this when you need to atomically update both stage state AND
        queue a message. This prevents orphaned workflows from crashes
        between separate store and queue operations.

        Default implementation provides a no-op transaction that just
        delegates to the normal store methods. SQLite and PostgreSQL
        implementations provide true atomic transactions.

        Args:
            queue: Optional queue for pushing messages. Required for
                   backends that don't have integrated queue support.

        Usage:
            with store.transaction(queue) as txn:
                txn.store_stage(stage)
                txn.push_message(message)
            # Commits on success, rolls back on exception

        Yields:
            StoreTransaction with store_stage() and push_message() methods
        """
        txn = NoOpTransaction(self, queue)
        try:
            yield txn
            # Flush pending messages on successful exit
            txn._flush_messages()
        except Exception:
            # Don't flush on exception
            raise


class StoreTransaction(ABC):
    """Abstract interface for atomic store + queue transactions.

    Implementations must ensure that store_stage() and push_message()
    are committed atomically - either both succeed or both are rolled back.
    """

    @abstractmethod
    def store_stage(self, stage: StageExecution) -> None:
        """Store or update a stage within the transaction."""
        pass

    @abstractmethod
    def push_message(self, message: Message, delay: int = 0) -> None:
        """Push a message to the queue within the transaction."""
        pass


class NoOpTransaction(StoreTransaction):
    """Default transaction that just delegates to normal store methods.

    This is used when the storage backend doesn't support atomic transactions.
    Operations are NOT atomic - a crash between store_stage and push_message
    can leave workflows in an inconsistent state.

    Only use this as a fallback. For production, use SqliteWorkflowStore
    or PostgresWorkflowStore which provide true atomic transactions.
    """

    def __init__(self, store: WorkflowStore, queue: Queue | None = None) -> None:
        self._store = store
        self._queue = queue
        self._pending_messages: list[tuple[Message, int]] = []

    def store_stage(self, stage: StageExecution) -> None:
        """Store stage (commits immediately in no-op mode)."""
        self._store.store_stage(stage)

    def push_message(self, message: Message, delay: int = 0) -> None:
        """Buffer message to be pushed when transaction completes.

        Messages are buffered and flushed when the context manager exits
        successfully. If an exception occurs, messages are not pushed.
        """
        self._pending_messages.append((message, delay))

    def _flush_messages(self) -> None:
        """Flush pending messages to the queue.

        Called by the context manager on successful exit.
        """
        if not self._queue:
            return

        from datetime import timedelta

        for message, delay in self._pending_messages:
            if delay > 0:
                self._queue.push(message, timedelta(seconds=delay))
            else:
                self._queue.push(message)
        self._pending_messages.clear()
