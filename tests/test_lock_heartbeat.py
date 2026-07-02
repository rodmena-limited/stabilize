"""
Repro tests for message-lock expiry during long handler execution (audit
finding A1-6).

The queue lock_duration (default 60s) is far shorter than typical task
timeouts (hours). Once the lock expires the message becomes visible again and
another poller re-executes a task that is STILL RUNNING — duplicate execution
of side-effecting work. The processor must renew the lock (heartbeat) while
its handler is executing; the lock should only lapse when the worker dies.
"""

import threading
import time
from datetime import timedelta
from typing import Any

import pytest

from stabilize.persistence.connection import ConnectionManager, SingletonMeta
from stabilize.queue.messages import StartWorkflow
from stabilize.queue.processor import QueueProcessor
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.queue.sqlite import SqliteQueue


@pytest.fixture()
def short_lock_queue(tmp_path: Any):
    SingletonMeta.reset(ConnectionManager)
    queue = SqliteQueue(
        f"sqlite:///{tmp_path}/heartbeat.db",
        lock_duration=timedelta(seconds=0.3),
    )
    queue._create_table()
    yield queue
    SingletonMeta.reset(ConnectionManager)


class TestExtendLock:
    def test_extend_lock_keeps_message_invisible(self, short_lock_queue: Any) -> None:
        queue = short_lock_queue
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="e1"))
        message = queue.poll_one()
        assert message is not None

        # Keep extending past the original lock window.
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            assert queue.extend_lock(message) is True
            assert queue.poll_one() is None, "extended lock did not keep the message invisible"
            time.sleep(0.1)

        queue.ack(message)


class TestHeartbeatPreventsDuplicateExecution:
    def test_long_handler_does_not_get_message_redelivered(self, short_lock_queue: Any) -> None:
        """A handler that outlives lock_duration must not see its own message
        redelivered to another worker thread."""
        queue = short_lock_queue

        invocations: list[float] = []
        lock = threading.Lock()

        config = QueueProcessorConfig(poll_frequency_ms=20, max_workers=4)
        processor = QueueProcessor(queue, config=config)

        def slow_handler(message: Any) -> None:
            with lock:
                invocations.append(time.monotonic())
            time.sleep(1.2)  # 4x the lock duration

        processor.register_handler_func(StartWorkflow, slow_handler)
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="e1"))

        processor.start()
        try:
            time.sleep(2.0)
        finally:
            processor.stop()

        assert len(invocations) == 1, (
            f"message redelivered {len(invocations)} times while its handler was "
            "still executing — lock expired without heartbeat renewal"
        )
