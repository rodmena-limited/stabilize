"""
Transaction helper service.

Encapsulates common transaction patterns to reduce code duplication
in handlers.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue.messages import Message
    from stabilize.queue.queue import Queue


class TransactionHelper:
    """Helper for executing atomic transactions with common patterns."""

    def __init__(self, repository: WorkflowStore, queue: Queue) -> None:
        self.repository = repository
        self.queue = queue

    def execute_atomic(
        self,
        stage: StageExecution | None = None,
        source_message: Message | None = None,
        messages_to_push: Sequence[tuple[Message, int | None]] | None = None,
        handler_name: str = "UnknownHandler",
    ) -> None:
        """
        Execute an atomic transaction to update state and queue messages.

        Args:
            stage: Optional stage to store/update
            source_message: Optional message to mark as processed
            messages_to_push: List of (message, delay_seconds) tuples to push
            handler_name: Name of the handler for logging/metrics
        """
        messages_to_push = messages_to_push or []

        with self.repository.transaction(self.queue) as txn:
            if stage:
                txn.store_stage(stage)

            if source_message and source_message.message_id:
                execution_id = getattr(source_message, "execution_id", None)
                txn.mark_message_processed(
                    message_id=source_message.message_id,
                    handler_type=handler_name,
                    execution_id=execution_id,
                )

            for msg, delay in messages_to_push:
                txn.push_message(msg, delay or 0)
