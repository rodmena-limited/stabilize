"""Detect a synchronous processor running on an unrenewed queue lease.

Lease renewal and the execution path are coupled, and only one of them is
visible at the call site. ``process_all()`` -> ``process_one()`` calls the
handler directly: no executor, no threads, and therefore no lock heartbeat,
which is only started from the asynchronous ``start()`` path. A queue whose
``lock_duration`` is shorter than a handler makes the message visible again
mid-flight, and a later poll re-runs it -- silent double execution, with no
error on either side.

``PostgresQueue(dsn)`` looks complete and is only safe if something is
renewing. This module makes that condition loud at the first synchronous poll
rather than at the first slow handler.
"""

from __future__ import annotations

import logging
import threading
from datetime import timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.queue.interface import Queue
    from stabilize.queue.messages import Message

logger = logging.getLogger(__name__)

DEFAULT_MIN_SYNC_LEASE = timedelta(minutes=5)


def lease_duration(queue: Queue | Any) -> timedelta | None:
    """The queue's message lease, when it exposes one."""
    duration = getattr(queue, "lock_duration", None)
    return duration if isinstance(duration, timedelta) else None


def check_sync_lease(
    queue: Queue | Any,
    minimum: timedelta = DEFAULT_MIN_SYNC_LEASE,
) -> str | None:
    """Return a warning when a synchronous drain has no lease renewal.

    Returns None when the queue exposes no lease, or the lease is at least
    *minimum*. The caller is responsible for warning only once.
    """
    duration = lease_duration(queue)
    if duration is None or duration >= minimum:
        return None
    return (
        f"{type(queue).__name__} has lock_duration={duration.total_seconds():.0f}s and this "
        "is the SYNCHRONOUS path (process_all/process_one), which starts no lock "
        "heartbeat. A handler running longer than the lease makes its message visible "
        "to another consumer while it is still running, and a later poll can execute it "
        "AGAIN with no error on either side. Size lock_duration above your worst-case "
        f"single message (e.g. {type(queue).__name__}(dsn, lock_duration=timedelta(minutes=30))), "
        "or drive the queue with start() so the heartbeat renews it."
    )


def start_lock_heartbeat(
    queue: Queue | Any,
    config: Any,
    message: Message,
) -> threading.Event | None:
    """Start a heartbeat renewing *message*'s queue lock while it is handled.

    Without renewal, a handler outliving the queue's lock_duration lets the
    message become visible again and a second worker re-executes
    still-running, side-effecting work. Returns the stop event, or None when
    heartbeating is disabled or the queue cannot extend a lock.
    """
    from stabilize.queue.messages import get_message_type_name

    if not getattr(config, "enable_lock_heartbeat", False):
        return None
    if getattr(message, "message_id", None) is None:
        return None
    extend = getattr(queue, "extend_lock", None)
    if extend is None or not callable(extend):
        return None

    interval = config.lock_heartbeat_interval_seconds
    if interval is None:
        duration = lease_duration(queue)
        interval = duration.total_seconds() / 2.0 if duration is not None else 30.0
    interval = max(0.05, float(interval))

    stop = threading.Event()

    def beat() -> None:
        while not stop.wait(interval):
            try:
                if not extend(message):
                    return  # message gone (acked/moved); nothing to renew
            except Exception as exc:
                logger.warning(
                    "Lock heartbeat failed for %s: %s", get_message_type_name(message), exc
                )
                return

    threading.Thread(target=beat, daemon=True, name="stabilize-lock-heartbeat").start()
    return stop
