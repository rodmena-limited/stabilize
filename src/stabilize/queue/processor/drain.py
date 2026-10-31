"""Synchronous draining for QueueProcessor: process_one and process_all."""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from stabilize.errors.delivery import DeliveryContractError, DuplicateDeliveryError
from stabilize.persistence.committed_marks import strict_delivery
from stabilize.queue import Queue
from stabilize.queue.messages import Message
from stabilize.queue.processor.lease_guard import check_sync_lease, start_lock_heartbeat

logger = logging.getLogger(__name__)


class SynchronousDrainMixin:
    queue: Queue
    config: Any
    _sync_lease_warned: bool
    delivery_defects: list[Any]
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
        if getattr(self.config, "enable_lock_heartbeat", False) and callable(getattr(self.queue, "extend_lock", None)):
            return
        warning = check_sync_lease(self.queue)
        if warning is not None:
            logger.warning("%s", warning)

    def process_one(self) -> bool:
        """
        Process a single message synchronously.

        Useful for testing and debugging.

        If the handler raises, the message is rescheduled by
        ``config.retry_delay`` and the exception is re-raised to the caller.
        The message is delivered again by a later call once that delay has
        passed. A task result held because the store was unavailable is saved
        by that redelivery without executing the task again.

        Returns:
            True if a message was processed, False otherwise

        Raises:
            Exception: whatever the handler raised, after rescheduling.
        """
        self._warn_once_if_lease_unrenewed()
        message = self.queue.poll_one()
        if message:
            heartbeat_stop = start_lock_heartbeat(self.queue, self.config, message)
            try:
                self._handle_message(message)
                self.queue.ack(message)
                return True
            except DuplicateDeliveryError as e:
                logger.info("%s; acknowledged without a second push", e)
                self.queue.ack(message)
                return True
            except Exception as e:
                logger.error("Error handling message: %s", e, exc_info=True)
                message.set_error_context(e)
                self.queue.reschedule(message, self.config.retry_delay)
                raise
            finally:
                if heartbeat_stop is not None:
                    heartbeat_stop.set()
        return False

    def process_all(self, timeout: float = 60.0) -> int:
        """
        Process all messages synchronously until queue is empty.

        Thread-safe: uses a processing lock to prevent concurrent calls.
        Also performs periodic DLQ cleanup for expired messages.

        A handler exception stops the drain: it propagates from
        ``process_one`` after the message is rescheduled by
        ``config.retry_delay``. To drain through such errors, catch the
        exception and call ``process_all`` again after that delay.

        Args:
            timeout: Maximum time to wait for processing

        Returns:
            Number of messages processed

        Raises:
            Exception: the first handler exception, after its message was rescheduled.
        """
        # Use processing lock to prevent concurrent calls
        # Non-blocking acquire - if another thread is processing, return immediately
        acquired = self._processing_lock.acquire(blocking=False)
        if not acquired:
            logger.debug("Another thread is processing, skipping")
            return 0

        try:
            count = 0
            defects_before = len(self.delivery_defects)
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
            if strict_delivery() and len(self.delivery_defects) > defects_before:
                broken = self.delivery_defects[defects_before:]
                raise DeliveryContractError(
                    f"{len(broken)} delivery(ies) committed a push outside the transaction that marked the message: "
                    + "; ".join(f"{d.message_type} {d.message_id}: {' / '.join(d.reasons)}" for d in broken)
                )
            return count
        finally:
            self._processing_lock.release()
