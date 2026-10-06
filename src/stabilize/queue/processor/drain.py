"""Synchronous draining for QueueProcessor: process_one and process_all."""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from stabilize.queue import Queue
from stabilize.queue.messages import Message
from stabilize.queue.processor.lease_guard import check_sync_lease

logger = logging.getLogger(__name__)


class SynchronousDrainMixin:
    queue: Queue
    config: Any
    _sync_lease_warned: bool
    _processing_lock: threading.Lock
    _last_dlq_check: float

    if TYPE_CHECKING:

        def _handle_message(self, message: Message) -> None: ...

        def _check_dlq(self) -> None: ...

    def _warn_once_if_lease_unrenewed(self) -> None:
        """Warn at the first synchronous poll when nothing renews the lease."""
        if self._sync_lease_warned:
            return
        self._sync_lease_warned = True
        warning = check_sync_lease(self.queue)
        if warning is not None:
            logger.warning("%s", warning)

    def process_one(self) -> bool:
        """
        Process a single message synchronously.

        Useful for testing and debugging.

        Returns:
            True if a message was processed, False otherwise
        """
        self._warn_once_if_lease_unrenewed()
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
                if self.process_one():
                    count += 1
                    continue
                if self.queue.size() == 0:
                    break
                time.sleep(0.01)

            return count
        finally:
            self._processing_lock.release()
