"""Synchronous draining for QueueProcessor: process_one and process_all."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from stabilize.errors.delivery import DeliveryContractError, DuplicateDeliveryError
from stabilize.errors.store_errors import is_store_unavailable
from stabilize.persistence.committed_marks import strict_delivery
from stabilize.queue import Queue
from stabilize.queue.messages import Message
from stabilize.queue.processor.lease_guard import check_sync_lease, start_lock_heartbeat
from stabilize.queue.processor.redelivery import redelivery_delay

logger = logging.getLogger(__name__)


@dataclass
class DrainResult:
    """What the last ``process_all`` did and why it stopped."""

    processed: int = 0
    errors: int = 0
    reason: str = "not_run"


class SynchronousDrainMixin:
    queue: Queue
    config: Any
    _sync_lease_warned: bool
    delivery_defects: list[Any]
    last_drain: DrainResult
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
                unavailable = is_store_unavailable(e)
                self.queue.reschedule(
                    message,
                    redelivery_delay(self.config.retry_delay, message, unavailable),
                    store_unavailable=unavailable,
                )
                raise
            finally:
                if heartbeat_stop is not None:
                    heartbeat_stop.set()
        return False

    def process_all(self, timeout: float = 60.0, raise_on_error: bool = True) -> int:
        """
        Process messages synchronously until none is pending or ready within the budget.

        Thread-safe: uses a processing lock to prevent concurrent calls.
        Also performs periodic DLQ cleanup for expired messages.

        When nothing is ready the drain asks the queue when the next message is due
        and sleeps until then (at most 1 s at a time); when that is beyond the
        remaining budget it returns. A handler exception is rescheduled by
        ``config.retry_delay`` and re-raised, or, with ``raise_on_error=False``,
        counted on ``last_drain`` while the drain carries on.

        Args:
            timeout: Maximum time to spend draining
            raise_on_error: Re-raise the first handler exception (the default)

        Returns:
            Number of messages processed

        Raises:
            Exception: the first handler exception, after its message was rescheduled.
        """
        acquired = self._processing_lock.acquire(blocking=False)
        if not acquired:
            logger.debug("Another thread is processing, skipping")
            return 0

        try:
            result = DrainResult()
            self.last_drain = result
            defects_before = len(self.delivery_defects)
            start = time.monotonic()

            if time.monotonic() - self._last_dlq_check > 30.0:
                self._check_dlq()
                self._last_dlq_check = time.monotonic()

            while True:
                remaining = timeout - (time.monotonic() - start)
                if remaining <= 0:
                    result.reason = "timeout"
                    break
                try:
                    processed = self.process_one()
                except Exception:
                    result.errors += 1
                    if raise_on_error:
                        result.reason = "error"
                        raise
                    continue
                if processed:
                    result.processed += 1
                    continue
                wait = self.queue.next_ready_in()
                if wait is None:
                    result.reason = "empty"
                    break
                remaining = timeout - (time.monotonic() - start)
                if wait > remaining:
                    result.reason = "next_ready_beyond_budget"
                    break
                time.sleep(min(max(wait, 0.01), max(remaining, 0.01), 1.0))
            if strict_delivery() and len(self.delivery_defects) > defects_before:
                broken = self.delivery_defects[defects_before:]
                raise DeliveryContractError(
                    f"{len(broken)} delivery(ies) committed a push outside the transaction that marked the message: "
                    + "; ".join(f"{d.message_type} {d.message_id}: {' / '.join(d.reasons)}" for d in broken)
                )
            return result.processed
        finally:
            self._processing_lock.release()
