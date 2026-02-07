"""
Queue processor for handling messages.

This module provides the QueueProcessor class that polls messages from
the queue and dispatches them to appropriate handlers.

Enterprise Features:
- Thread-safe processing with locks
- Dead Letter Queue (DLQ) cleanup for failed messages
- Configurable retry and error handling
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from stabilize.queue import Queue
from stabilize.queue.dedup import get_deduplicator
from stabilize.queue.messages import Message, get_message_type_name
from stabilize.resilience.config import HandlerConfig, get_handler_config

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.resilience.bulkheads import TaskBulkheadManager
    from stabilize.resilience.circuits import WorkflowCircuitFactory
    from stabilize.tasks.registry import TaskRegistry

logger = logging.getLogger(__name__)

M = TypeVar("M", bound=Message)


class MessageHandler(Generic[M]):
    """
    Base class for message handlers.

    Each handler processes a specific type of message.
    """

    @property
    def message_type(self) -> type[M]:
        """Return the type of message this handler processes."""
        raise NotImplementedError

    def handle(self, message: M) -> None:
        """
        Handle a message.

        Args:
            message: The message to handle
        """
        raise NotImplementedError


@dataclass
class QueueProcessorConfig:
    """Configuration for the queue processor.

    Values can be loaded from environment variables via HandlerConfig.
    See HandlerConfig documentation for environment variable names.
    """

    # How often to poll the queue (milliseconds)
    poll_frequency_ms: int = 50

    # Maximum number of concurrent message handlers
    max_workers: int = 10

    # Delay before reprocessing a failed message
    retry_delay: timedelta = timedelta(seconds=15)

    # Whether to stop on unhandled exceptions
    stop_on_error: bool = False

    # Enable message deduplication for idempotency
    enable_deduplication: bool = True

    @classmethod
    def from_handler_config(cls, handler_config: HandlerConfig | None = None) -> QueueProcessorConfig:
        """Create QueueProcessorConfig from HandlerConfig.

        Args:
            handler_config: HandlerConfig to use. If None, loads from environment.

        Returns:
            QueueProcessorConfig with values from HandlerConfig
        """
        config = handler_config or get_handler_config()
        return cls(
            poll_frequency_ms=config.poll_frequency_ms,
            max_workers=config.max_workers,
            retry_delay=timedelta(seconds=config.handler_retry_delay_seconds),
        )


class QueueProcessor:
    """
    Processes messages from a queue using registered handlers.

    The processor polls the queue at regular intervals and dispatches
    messages to appropriate handlers. Handlers run in a thread pool
    for concurrent processing.

    When ``store`` and ``task_registry`` are both provided, all 12 default
    handlers are registered automatically — no manual registration needed.

    Example:
        queue = SqliteQueue("sqlite:///workflow.db", table_name="queue_messages")
        store = SqliteWorkflowStore("sqlite:///workflow.db", create_tables=True)
        registry = TaskRegistry()
        processor = QueueProcessor(queue, store=store, task_registry=registry)
        processor.start()
    """

    def __init__(
        self,
        queue: Queue,
        config: QueueProcessorConfig | None = None,
        store: WorkflowStore | None = None,
        handler_config: HandlerConfig | None = None,
        task_registry: TaskRegistry | None = None,
        bulkhead_manager: TaskBulkheadManager | None = None,
        circuit_factory: WorkflowCircuitFactory | None = None,
    ) -> None:
        """
        Initialize the queue processor.

        Args:
            queue: The queue to process
            config: Optional configuration (takes precedence if provided)
            store: Optional store for message deduplication and handler setup
            handler_config: Optional HandlerConfig. If config is None, uses this
                           to create QueueProcessorConfig. If both are None,
                           loads from environment.
            task_registry: Optional task registry. When both store and task_registry
                          are provided, all default handlers are auto-registered.
            bulkhead_manager: Optional bulkhead manager for RunTaskHandler concurrency control.
            circuit_factory: Optional circuit breaker factory for workflow-level circuit breaking.
        """
        self.queue = queue
        # Use explicit config if provided, otherwise create from handler_config
        if config is not None:
            self.config = config
        else:
            self.config = QueueProcessorConfig.from_handler_config(handler_config)
        self._store = store

        # Warn if deduplication is enabled but no store provided
        if self.config.enable_deduplication and store is None:
            logger.warning(
                "QueueProcessor created with enable_deduplication=True but no store provided. "
                "Message deduplication will NOT work. Pass store=store to enable."
            )
        self._handlers: dict[type[Message], MessageHandler[Any]] = {}
        self._running = False
        self._stopping = False  # Flag for graceful stop (stop accepting new work)
        self._executor: ThreadPoolExecutor | None = None
        self._poll_thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._processing_lock = threading.Lock()  # Lock for process_all
        self._active_count = 0
        self._last_dlq_check = 0.0

        # Auto-register default handlers when both store and task_registry are provided
        if store is not None and task_registry is not None:
            self._register_default_handlers(
                queue,
                store,
                task_registry,
                bulkhead_manager,
                circuit_factory,
                handler_config,
            )

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

        handlers: list[MessageHandler[Any]] = [
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

        for handler in handlers:
            self._handlers[handler.message_type] = handler
            logger.debug("Registered handler for %s", handler.message_type.__name__)

    def register_handler(self, handler: MessageHandler[Any]) -> None:
        """
        Register a message handler.

        Raises ValueError if a handler for the same message type is already
        registered. Use :meth:`replace_handler` to override an existing handler.

        Args:
            handler: The handler to register

        Raises:
            ValueError: If a handler for this message type is already registered.
        """
        if handler.message_type in self._handlers:
            raise ValueError(
                f"Handler for {handler.message_type.__name__} is already registered. Use replace_handler() to override."
            )
        self._handlers[handler.message_type] = handler
        logger.debug("Registered handler for %s", handler.message_type.__name__)

    def replace_handler(self, handler: MessageHandler[Any]) -> None:
        """
        Replace an existing handler.

        Args:
            handler: The new handler to use

        Raises:
            ValueError: If no handler is registered for this message type.
        """
        if handler.message_type not in self._handlers:
            raise ValueError(f"No handler registered for {handler.message_type.__name__}")
        self._handlers[handler.message_type] = handler
        logger.debug("Replaced handler for %s", handler.message_type.__name__)

    def register_handler_func(
        self,
        message_type: type[M],
        handler_func: Callable[[M], None],
    ) -> None:
        """
        Register a handler function for a message type.

        Args:
            message_type: The type of message to handle
            handler_func: Function to call with the message
        """

        class FuncHandler(MessageHandler[M]):
            @property
            def message_type(self) -> type[M]:
                return message_type

            def handle(self, message: M) -> None:
                handler_func(message)

        self.register_handler(FuncHandler())

    def start(self) -> None:
        """Start the queue processor."""
        if self._running:
            return

        self._running = True
        self._executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        logger.info("Queue processor started")

    def stop(self, wait: bool = True) -> None:
        """
        Stop the queue processor.

        Args:
            wait: Whether to wait for pending messages to complete
        """
        self._stopping = True
        self._running = False

        if self._poll_thread:
            self._poll_thread.join(timeout=5.0)

        if self._executor:
            self._executor.shutdown(wait=wait)

        logger.info("Queue processor stopped")

    def request_stop(self) -> None:
        """
        Request graceful stop without blocking.

        Sets the stopping flag to stop accepting new messages,
        but doesn't wait for active tasks to complete.
        Use the active_count property to monitor progress.

        Example:
            processor.request_stop()
            while processor.active_count > 0:
                time.sleep(0.1)
            processor.stop()
        """
        self._stopping = True
        logger.info("Queue processor stop requested (active=%d)", self.active_count)

    @property
    def is_stopping(self) -> bool:
        """Check if stop has been requested but not yet completed."""
        return self._stopping and self._running

    def _poll_loop(self) -> None:
        """Main polling loop.

        Uses a lock to prevent race condition between stopping flag check
        and message polling. This ensures no messages are submitted after
        shutdown is requested.
        """
        poll_interval = self.config.poll_frequency_ms / 1000.0

        while self._running:
            try:
                # Hold lock while checking stopping flag AND polling to prevent race
                # condition where a message could be submitted after stop is requested
                message = None
                with self._lock:
                    # Check if stopping - don't accept new work
                    if self._stopping:
                        pass  # Will sleep outside lock
                    # Check if we have capacity
                    elif self._active_count >= self.config.max_workers:
                        pass  # Will sleep outside lock
                    else:
                        # Poll under lock to prevent race
                        message = self.queue.poll_one()
                        if message:
                            # Increment count while still holding lock
                            self._active_count += 1

                if message:
                    # Submit outside lock (submit doesn't need lock protection)
                    self._submit_message_internal(message)
                else:
                    time.sleep(poll_interval)

            except Exception as e:
                logger.error("Error in poll loop: %s", e, exc_info=True)
                if self.config.stop_on_error:
                    self._running = False
                    break
                time.sleep(poll_interval)

    def _submit_message(self, message: Message) -> None:
        """Submit a message to the thread pool for processing.

        Increments active_count before submitting. Used for external callers.
        """
        with self._lock:
            self._active_count += 1
        self._submit_message_internal(message)

    def _submit_message_internal(self, message: Message) -> None:
        """Submit a message without incrementing active_count.

        Used by _poll_loop which already incremented the count under lock.
        """

        def process_and_ack() -> None:
            try:
                self._handle_message(message)
                self.queue.ack(message)
            except Exception as e:
                logger.error(
                    "Error handling %s: %s",
                    get_message_type_name(message),
                    e,
                    exc_info=True,
                )
                # Store error context for debugging and auditing
                # This allows tracking of failure patterns across retries
                message.set_error_context(e)
                # Message will be reprocessed after lock expires or reschedule
                self.queue.reschedule(message, self.config.retry_delay)
            finally:
                with self._lock:
                    self._active_count -= 1

        if self._executor is not None:
            self._executor.submit(process_and_ack)

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

    def process_one(self) -> bool:
        """
        Process a single message synchronously.

        Useful for testing and debugging.

        Returns:
            True if a message was processed, False otherwise
        """
        message = self.queue.poll_one()
        if message:
            try:
                self._handle_message(message)
                self.queue.ack(message)
                return True
            except Exception as e:
                logger.error("Error handling message: %s", e, exc_info=True)
                # Store error context for debugging and auditing
                message.set_error_context(e)
                self.queue.reschedule(message, self.config.retry_delay)
                raise
        return False

    def process_all(self, timeout: float = 60.0) -> int:
        """
        Process all messages synchronously until queue is empty.

        Thread-safe: uses a processing lock to prevent concurrent calls.
        Also performs periodic DLQ cleanup for expired messages.

        Args:
            timeout: Maximum time to wait for processing

        Returns:
            Number of messages processed
        """
        # Use processing lock to prevent concurrent calls
        # Non-blocking acquire - if another thread is processing, return immediately
        acquired = self._processing_lock.acquire(blocking=False)
        if not acquired:
            logger.debug("Another thread is processing, skipping")
            return 0

        try:
            count = 0
            # Use monotonic time for elapsed time calculations to avoid
            # issues with clock drift, NTP adjustments, or leap seconds
            start = time.monotonic()

            # Periodic DLQ check (every 30 seconds)
            if time.monotonic() - self._last_dlq_check > 30.0:
                self._check_dlq()
                self._last_dlq_check = time.monotonic()

            while time.monotonic() - start < timeout:
                if self.queue.size() == 0:
                    break
                if self.process_one():
                    count += 1
                else:
                    # No ready messages, wait a bit
                    time.sleep(0.01)

            return count
        finally:
            self._processing_lock.release()

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

    @property
    def is_running(self) -> bool:
        """Check if the processor is running."""
        return self._running

    @property
    def active_count(self) -> int:
        """Get the number of actively processing messages."""
        with self._lock:
            return self._active_count


class SynchronousQueueProcessor(QueueProcessor):
    """
    A synchronous queue processor that processes messages immediately.

    Useful for testing where you want deterministic execution order.

    Accepts the same parameters as :class:`QueueProcessor` for auto-registration.
    """

    def __init__(
        self,
        queue: Queue,
        store: WorkflowStore | None = None,
        task_registry: TaskRegistry | None = None,
        config: QueueProcessorConfig | None = None,
        handler_config: HandlerConfig | None = None,
        bulkhead_manager: TaskBulkheadManager | None = None,
        circuit_factory: WorkflowCircuitFactory | None = None,
    ) -> None:
        super().__init__(
            queue,
            config=config,
            store=store,
            handler_config=handler_config,
            task_registry=task_registry,
            bulkhead_manager=bulkhead_manager,
            circuit_factory=circuit_factory,
        )
        self._running = True

    def start(self) -> None:
        """No-op for synchronous processor."""
        pass

    def stop(self, wait: bool = True) -> None:
        """No-op for synchronous processor."""
        pass

    def push_and_process(self, message: Message) -> None:
        """
        Push a message and process it immediately.

        Args:
            message: The message to push and process
        """
        self.queue.push(message)
        self.process_all(timeout=5.0)
