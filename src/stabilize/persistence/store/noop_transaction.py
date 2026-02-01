"""No-op transaction implementation for non-atomic operations."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from stabilize.persistence.store.transaction import StoreTransaction

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow
    from stabilize.queue import Queue
    from stabilize.queue.messages import Message

    from .interface import WorkflowStore

logger = logging.getLogger(__name__)


class NoOpTransaction(StoreTransaction):
    """Default transaction that buffers operations and flushes on commit.

    WARNING: This implementation is NOT truly atomic. A crash during flush
    can leave partial state in the database. This is only suitable for:
    - Testing environments
    - Non-critical workloads where eventual consistency is acceptable

    For production critical systems requiring 100% atomicity, use:
    - SqliteWorkflowStore (provides true atomic transactions)
    - PostgresWorkflowStore (provides true atomic transactions)

    Operations are buffered and flushed in careful order to minimize
    inconsistency windows, but true atomicity is impossible without
    database-level transaction support.
    """

    # Class-level flag to suppress warnings (e.g., in tests)
    _suppress_warning: bool = False

    def __init__(self, store: WorkflowStore, queue: Queue | None = None) -> None:
        # Block usage in production mode - NoOpTransaction is not atomic
        if os.getenv("STABILIZE_PRODUCTION", "false").lower() == "true":
            raise RuntimeError(
                "NoOpTransaction cannot be used in production mode "
                "(STABILIZE_PRODUCTION=true). Use SqliteWorkflowStore or "
                "PostgresWorkflowStore for atomic transactions."
            )
        if not NoOpTransaction._suppress_warning:
            logger.warning(
                "NoOpTransaction is being used. This is NOT truly atomic and "
                "should not be used in production critical systems. Use "
                "SqliteWorkflowStore or PostgresWorkflowStore for true atomicity."
            )
        self._store = store
        self._queue = queue
        self._pending_stages: list[StageExecution] = []
        self._pending_workflows: list[Workflow] = []
        self._pending_messages: list[tuple[Message, int]] = []
        self._pending_processed: list[tuple[str, str | None, str | None]] = []

    def store_stage(self, stage: StageExecution) -> None:
        """Buffer stage to be stored when transaction completes.

        Stages are buffered and flushed when the context manager exits
        successfully. If an exception occurs, stages are not stored.
        """
        self._pending_stages.append(stage)

    def update_workflow_status(self, workflow: Workflow) -> None:
        """Buffer workflow status update when transaction completes.

        Workflows are buffered and flushed when the context manager exits
        successfully. If an exception occurs, workflows are not updated.
        """
        self._pending_workflows.append(workflow)

    def push_message(self, message: Message, delay: int = 0) -> None:
        """Buffer message to be pushed when transaction completes.

        Messages are buffered and flushed when the context manager exits
        successfully. If an exception occurs, messages are not pushed.
        """
        self._pending_messages.append((message, delay))

    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        """Buffer processed message mark to be stored when transaction completes."""
        self._pending_processed.append((message_id, handler_type, execution_id))

    def _flush_messages(self) -> None:
        """Flush all pending operations.

        Called by the context manager on successful exit.

        Order is critical for crash safety:
        1. Workflows first (persist workflow status changes)
        2. Stages second (persist stage state changes)
        3. Messages third (ensure workflow can continue)
        4. Processed marks LAST (only after everything else succeeds)

        This order ensures that a crash during flush leaves the workflow in a
        recoverable state. Worst case (crash after stages, before processed marks)
        results in duplicate message handling, which handlers are designed to be
        idempotent against.

        NOTE: This implementation is NOT truly atomic. If store_stage() fails after
        update_status() succeeds, the workflow will be in an inconsistent state.
        For production with strict consistency requirements, use SqliteWorkflowStore
        or PostgresWorkflowStore which provide true atomic transactions.
        """
        # Track what we've flushed for error reporting
        workflows_flushed = 0
        stages_flushed = 0
        messages_flushed = 0

        try:
            # 1. Flush workflows first - persist workflow status changes
            for workflow in self._pending_workflows:
                self._store.update_status(workflow)
                workflows_flushed += 1
            self._pending_workflows.clear()

            # 2. Flush stages second - persist stage state changes
            for stage in self._pending_stages:
                self._store.store_stage(stage)
                stages_flushed += 1
            self._pending_stages.clear()

            # 3. Flush messages third - ensure workflow continues
            if self._queue:
                from datetime import timedelta

                for message, delay in self._pending_messages:
                    if delay > 0:
                        self._queue.push(message, timedelta(seconds=delay))
                    else:
                        self._queue.push(message)
                    messages_flushed += 1
                self._pending_messages.clear()

            # 4. Flush processed marks LAST - only after everything else succeeds
            for message_id, handler_type, execution_id in self._pending_processed:
                self._store.mark_message_processed(message_id, handler_type, execution_id)
            self._pending_processed.clear()

        except Exception as e:
            # Log what was partially flushed before the error
            logger.error(
                "NoOpTransaction partial flush failure: flushed %d/%d workflows, "
                "%d/%d stages, %d/%d messages before error: %s",
                workflows_flushed,
                workflows_flushed + len(self._pending_workflows),
                stages_flushed,
                stages_flushed + len(self._pending_stages),
                messages_flushed,
                messages_flushed + len(self._pending_messages),
                e,
            )
            # Clear remaining pending items to prevent double-flush on retry
            self._pending_workflows.clear()
            self._pending_stages.clear()
            self._pending_messages.clear()
            self._pending_processed.clear()
            raise
