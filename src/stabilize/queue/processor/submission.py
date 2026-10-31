"""Handing a polled message to the worker pool: lease heartbeat, acknowledgement, reschedule, latch."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from stabilize.errors.delivery import DuplicateDeliveryError
from stabilize.errors.store_errors import is_store_unavailable
from stabilize.persistence.tenancy import TenancyModeChanged
from stabilize.queue import Queue
from stabilize.queue.messages import Message, get_message_type_name
from stabilize.queue.processor.lease_guard import start_lock_heartbeat
from stabilize.queue.processor.redelivery import redelivery_delay
from stabilize.threads import in_context

logger = logging.getLogger(__name__)


class MessageSubmissionMixin:
    queue: Queue
    config: Any
    _lock: threading.Lock
    _active_count: int
    _in_flight: set[str]
    _in_flight_lock: threading.Lock
    _executor: ThreadPoolExecutor | None

    if TYPE_CHECKING:

        def _handle_message(self, message: Message) -> None: ...

        def _latch(self, exc: BaseException) -> None: ...

    def _start_lock_heartbeat(self, message: Message) -> threading.Event | None:
        """Renew this message's queue lock while its handler executes."""
        return start_lock_heartbeat(self.queue, self.config, message)

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
            msg_id = getattr(message, "message_id", None)
            if msg_id:
                with self._in_flight_lock:
                    self._in_flight.add(msg_id)
            heartbeat_stop = self._start_lock_heartbeat(message)
            try:
                self._handle_message(message)
                self.queue.ack(message)
            except DuplicateDeliveryError as e:
                logger.info("%s; acknowledged without a second push", e)
                self.queue.ack(message)
            except TenancyModeChanged as e:
                self._latch(e)
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
                unavailable = is_store_unavailable(e)
                self.queue.reschedule(
                    message,
                    redelivery_delay(self.config.retry_delay, message, unavailable),
                    store_unavailable=unavailable,
                )
            finally:
                if heartbeat_stop is not None:
                    heartbeat_stop.set()
                if msg_id:
                    with self._in_flight_lock:
                        self._in_flight.discard(msg_id)
                with self._lock:
                    self._active_count -= 1

        try:
            if self._executor is None:
                raise RuntimeError("processor is not running")
            self._executor.submit(in_context(process_and_ack))
        except RuntimeError:
            with self._lock:
                self._active_count -= 1
            self.queue.reschedule(message, timedelta(0))
            raise
