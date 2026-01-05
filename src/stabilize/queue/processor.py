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

from stabilize.queue.messages import Message, get_message_type_name
from stabilize.queue.queue import Queue

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore

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
    """Configuration for the queue processor."""

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


class QueueProcessor:
    """
    Processes messages from a queue using registered handlers.

    The processor polls the queue at regular intervals and dispatches
    messages to appropriate handlers. Handlers run in a thread pool
    for concurrent processing.

    Example:
        queue = InMemoryQueue()
        processor = QueueProcessor(queue)
        processor.register_handler(StartWorkflowHandler(queue, repository))
        processor.start()
    """

    def __init__(
        self,
        queue: Queue,
        config: QueueProcessorConfig | None = None,
        store: WorkflowStore | None = None,
    ) -> None:
        """
        Initialize the queue processor.

        Args:
            queue: The queue to process
            config: Optional configuration
            store: Optional store for message deduplication (idempotency)
        """
        self.queue = queue
        self.config = config or QueueProcessorConfig()
        self._store = store
        self._handlers: dict[type[Message], MessageHandler[Any]] = {}
        self._running = False
        self._executor: ThreadPoolExecutor | None = None
        self._poll_thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._processing_lock = threading.Lock()  # Lock for process_all
        self._active_count = 0
        self._last_dlq_check = 0.0

    def register_handler(self, handler: MessageHandler[Any]) -> None:
        """
        Register a message handler.

        Args:
            handler: The handler to register
        """
        self._handlers[handler.message_type] = handler
        logger.debug(f"Registered handler for {handler.message_type.__name__}")

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
        self._running = False

        if self._poll_thread:
            self._poll_thread.join(timeout=5.0)

        if self._executor:
            self._executor.shutdown(wait=wait)

        logger.info("Queue processor stopped")

    def _poll_loop(self) -> None:
        """Main polling loop."""
        poll_interval = self.config.poll_frequency_ms / 1000.0

        while self._running:
            try:
                # Check if we have capacity
                with self._lock:
                    if self._active_count >= self.config.max_workers:
                        time.sleep(poll_interval)
                        continue

                # Try to get a message
                message = self.queue.poll_one()

                if message:
                    self._submit_message(message)
                else:
                    time.sleep(poll_interval)

            except Exception as e:
                logger.error(f"Error in poll loop: {e}", exc_info=True)
                if self.config.stop_on_error:
                    self._running = False
                    break
                time.sleep(poll_interval)

    def _submit_message(self, message: Message) -> None:
        """Submit a message to the thread pool for processing."""
        with self._lock:
            self._active_count += 1

        def process_and_ack() -> None:
            try:
                self._handle_message(message)
                self.queue.ack(message)
            except Exception as e:
                logger.error(
                    f"Error handling {get_message_type_name(message)}: {e}",
                    exc_info=True,
                )
                # Message will be reprocessed after lock expires or reschedule
                self.queue.reschedule(message, self.config.retry_delay)
            finally:
                with self._lock:
                    self._active_count -= 1

        if self._executor is not None:
            self._executor.submit(process_and_ack)

    def _handle_message(self, message: Message) -> None:
        """Handle a message with the appropriate handler.

        If deduplication is enabled and a store is provided, checks if the
        message has already been processed and skips if so. Marks the message
        as processed after successful handling.
        """
        message_type = type(message)
        handler = self._handlers.get(message_type)

        if handler is None:
            logger.warning(f"No handler registered for {get_message_type_name(message)}")
            return

        # Check for duplicate message (idempotency)
        message_id = getattr(message, "message_id", None)
        execution_id = getattr(message, "execution_id", None)

        if self.config.enable_deduplication and self._store is not None and message_id is not None:
            if self._store.is_message_processed(message_id):
                logger.info(f"Skipping duplicate message {message_id} ({get_message_type_name(message)})")
                return

        logger.debug(f"Handling {get_message_type_name(message)} (execution={execution_id or 'N/A'})")

        handler.handle(message)

        # Mark message as processed for deduplication
        if self.config.enable_deduplication and self._store is not None and message_id is not None:
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
                logger.error(f"Error handling message: {e}", exc_info=True)
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
            start = time.time()

            # Periodic DLQ check (every 30 seconds)
            if time.time() - self._last_dlq_check > 30.0:
                self._check_dlq()
                self._last_dlq_check = time.time()

            while time.time() - start < timeout:
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
                    logger.info(f"Moved {moved} expired messages to DLQ")
            except Exception as e:
                logger.warning(f"Error checking DLQ: {e}")

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
    """

    def __init__(self, queue: Queue) -> None:
        super().__init__(queue)
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
