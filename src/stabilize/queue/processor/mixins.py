"""
Mixin classes for QueueProcessor.

Provides handler registration, message deduplication, and DLQ management logic.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from stabilize.queue.dedup import get_deduplicator
from stabilize.queue.messages import Message, get_message_type_name
from stabilize.queue.processor.handler_base import MessageHandler

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue
    from stabilize.resilience.bulkheads import TaskBulkheadManager
    from stabilize.resilience.circuits import WorkflowCircuitFactory
    from stabilize.resilience.config import HandlerConfig
    from stabilize.tasks.registry import TaskRegistry

logger = logging.getLogger(__name__)


class QueueProcessorMixin:
    """Mixin providing handler registration, message handling, and DLQ management."""

    # These attributes are expected to be defined in the class using this mixin
    _handlers: dict[type[Message], MessageHandler[Any]]
    _store: WorkflowStore | None
    queue: Queue
    config: Any  # QueueProcessorConfig

    def _register_default_handlers(
        self,
        queue: Queue,
        store: WorkflowStore,
        task_registry: TaskRegistry,
        bulkhead_manager: TaskBulkheadManager | None = None,
        circuit_factory: WorkflowCircuitFactory | None = None,
        handler_config: HandlerConfig | None = None,
    ) -> None:
        """Register all default message handlers.

        Args:
            queue: The message queue
            store: The workflow store
            task_registry: The task registry
            bulkhead_manager: Optional bulkhead manager for RunTaskHandler
            circuit_factory: Optional circuit breaker factory
            handler_config: Optional handler configuration
        """
        from stabilize.handlers import (
            AddMultiInstanceHandler,
            CancelRegionHandler,
            CancelStageHandler,
            CompleteStageHandler,
            CompleteTaskHandler,
            CompleteWorkflowHandler,
            ContinueParentStageHandler,
            JumpToStageHandler,
            RunTaskHandler,
            SignalStageHandler,
            SkipStageHandler,
            StartStageHandler,
            StartTaskHandler,
            StartWaitingWorkflowsHandler,
            StartWorkflowHandler,
        )

        all_handlers = [
            StartWorkflowHandler(queue, store),
            StartWaitingWorkflowsHandler(queue, store),
            StartStageHandler(queue, store),
            SkipStageHandler(queue, store),
            CancelStageHandler(queue, store),
            ContinueParentStageHandler(queue, store),
            JumpToStageHandler(queue, store),
            SignalStageHandler(queue, store),
            CancelRegionHandler(queue, store),
            AddMultiInstanceHandler(queue, store),
            StartTaskHandler(queue, store, task_registry),
            RunTaskHandler(
                queue,
                store,
                task_registry,
                bulkhead_manager=bulkhead_manager,
                circuit_factory=circuit_factory,
                handler_config=handler_config,
            ),
            CompleteTaskHandler(queue, store),
            CompleteStageHandler(queue, store),
            CompleteWorkflowHandler(queue, store),
        ]

        for h in all_handlers:
            handler: MessageHandler[Any] = h  # type: ignore[assignment]
            self._handlers[handler.message_type] = handler
            logger.debug("Registered handler for %s", handler.message_type.__name__)

    def _handle_message(self, message: Message) -> None:
        """Handle a message with the appropriate handler.

        If deduplication is enabled, uses a two-tier approach:
        1. Bloom filter for fast probabilistic check (no DB query)
        2. Database check for bloom filter positives (to confirm)

        This provides O(1) fast-path for new messages while still
        guaranteeing no duplicate processing.
        """
        message_type = type(message)
        handler = self._handlers.get(message_type)

        if handler is None:
            logger.warning("No handler registered for %s", get_message_type_name(message))
            return

        # Check for duplicate message (idempotency)
        message_id = getattr(message, "message_id", None)
        execution_id = getattr(message, "execution_id", None)

        if self.config.enable_deduplication and message_id is not None:
            dedup = get_deduplicator()

            # Check bloom filter first (fast path)
            if dedup.maybe_seen(message_id):
                # Bloom filter positive - need to verify with database
                if self._store is not None and self._store.is_message_processed(message_id):
                    logger.info(
                        "Skipping duplicate message %s (%s)",
                        message_id,
                        get_message_type_name(message),
                    )
                    return
            # else: Bloom filter negative - definitely not seen, skip DB check

            # Check if bloom filter needs rotation
            if dedup.should_reset(threshold=0.7):
                logger.info(
                    "Bloom filter fill ratio %.2f exceeds threshold, resetting",
                    dedup.fill_ratio,
                )
                dedup.reset()

        logger.debug(
            "Handling %s (execution=%s)", get_message_type_name(message), execution_id or "N/A"
        )

        handler.handle(message)

        # Mark message as processed for deduplication
        if self.config.enable_deduplication and message_id is not None:
            # Mark in bloom filter (fast, in-memory)
            dedup = get_deduplicator()
            dedup.mark_seen(message_id)

            # Also mark in database for persistence
            if self._store is not None:
                self._store.mark_message_processed(
                    message_id=message_id,
                    handler_type=get_message_type_name(message),
                    execution_id=execution_id,
                )

    def _check_dlq(self) -> None:
        """Check for expired messages and move them to DLQ.

        Only works if the queue supports DLQ (has check_and_move_expired method).
        """
        if hasattr(self.queue, "check_and_move_expired"):
            try:
                moved = self.queue.check_and_move_expired()
                if moved > 0:
                    logger.info("Moved %d expired messages to DLQ", moved)
            except Exception as e:
                logger.warning("Error checking DLQ: %s", e)
