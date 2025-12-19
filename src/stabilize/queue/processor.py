from __future__ import annotations
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeVar
from stabilize.queue.messages import Message, get_message_type_name
from stabilize.queue.queue import Queue
logger = logging.getLogger(__name__)
M = TypeVar("M", bound=Message)

class MessageHandler(Generic[M]):
    """
    Base class for message handlers.

    Each handler processes a specific type of message.
    """

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
    poll_frequency_ms: int = 50
    max_workers: int = 10
    retry_delay: timedelta = timedelta(seconds=15)
    stop_on_error: bool = False

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
    ) -> None:
        """
        Initialize the queue processor.

        Args:
            queue: The queue to process
            config: Optional configuration
        """
        self.queue = queue
        self.config = config or QueueProcessorConfig()
        self._handlers: dict[type[Message], MessageHandler[Any]] = {}
        self._running = False
        self._executor: ThreadPoolExecutor | None = None
        self._poll_thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._active_count = 0

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
