"""
Mixin classes for QueueProcessor.

Provides handler registration, message deduplication, and DLQ management logic.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast

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
            CompleteStageHandler(queue, store, task_registry=task_registry),
            CompleteWorkflowHandler(queue, store),
        ]

        for h in all_handlers:
            handler = cast("MessageHandler[Any]", h)
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

            # A bloom negative only proves "definitely new" while the filter
            # is authoritative (hydrated from the store, not reset since) AND
            # this process is the store's only writer — an empty bloom after a
            # restart/rotation, or a peer worker marking messages processed,
            # must not skip the durable check, or already-processed messages
            # re-execute on redelivery. The negative-cache fast path is
            # therefore opt-in via dedup_trust_negative_cache.
            trust_negative = getattr(self.config, "dedup_trust_negative_cache", False)
            if dedup.maybe_seen(message_id) or not (trust_negative and dedup.authoritative):
                if self._store is not None and self._store.is_message_processed(message_id):
                    logger.info(
                        "Skipping duplicate message %s (%s)",
                        message_id,
                        get_message_type_name(message),
                    )
                    return

            # Check if bloom filter needs rotation
            if dedup.should_reset(threshold=0.7):
                logger.info(
                    "Bloom filter fill ratio %.2f exceeds threshold, resetting",
                    dedup.fill_ratio,
                )
                dedup.reset()
                # Restore the fast path if the store can enumerate processed IDs
                self._hydrate_deduplicator()

        logger.debug("Handling %s (execution=%s)", get_message_type_name(message), execution_id or "N/A")

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

    def _hydrate_deduplicator(self) -> None:
        """Hydrate the bloom filter from the store's processed_messages.

        Grants the bloom authority (negatives skip the DB check) only when
        the store can enumerate ALL processed message IDs and they fit the
        filter's capacity. Otherwise the bloom stays advisory and every
        message is confirmed against the durable store — slower but safe.
        """
        if self._store is None:
            return
        dedup = get_deduplicator()
        capacity = dedup.expected_items
        try:
            # Fetch one beyond capacity so truncation is detectable.
            ids = self._store.get_processed_message_ids(limit=capacity + 1)
        except Exception as e:
            logger.warning("Dedup hydration failed; bloom stays advisory: %s", e)
            return
        if ids is None:
            return  # store cannot enumerate; bloom stays advisory
        if len(ids) > capacity:
            logger.warning(
                "processed_messages count exceeds bloom capacity (%d > %d); bloom stays advisory",
                len(ids),
                capacity,
            )
            return
        count = dedup.hydrate(ids)
        logger.debug("Hydrated dedup bloom filter with %d processed message IDs", count)

    def _check_dlq(self) -> None:
        """Check for expired messages and move them to DLQ.

        Only works if the queue supports DLQ (has check_and_move_expired method).
        """
        if hasattr(self.queue, "check_and_move_expired"):
            try:
                moved = getattr(self.queue, "check_and_move_expired")()
                if moved > 0:
                    logger.info("Moved %d expired messages to DLQ", moved)
            except Exception as e:
                logger.warning("Error checking DLQ: %s", e)
