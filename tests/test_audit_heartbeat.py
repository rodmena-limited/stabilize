from __future__ import annotations

import threading
import time
from datetime import timedelta
from typing import Any

import pytest

from stabilize.queue.messages import StartWorkflow
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.queue.processor.lease_guard import start_lock_heartbeat


class _FlakyExtend:
    def __init__(self, real: Any) -> None:
        self.real = real
        self.calls = 0
        self.lock = threading.Lock()

    def __getattr__(self, item: str) -> Any:
        return getattr(self.real, item)

    def extend_lock(self, message: Any, duration: Any = None) -> bool:
        with self.lock:
            self.calls += 1
            first = self.calls == 1
        if first:
            raise ConnectionError("server closed the connection unexpectedly")
        return bool(self.real.extend_lock(message, duration))


def test_heartbeat_keeps_renewing_after_a_failed_renewal(queue: Any) -> None:
    queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="01HZZZZZZZZZZZZZZZZZZZZZZZ"))
    message = queue.poll_one()
    assert message is not None
    flaky = _FlakyExtend(queue)
    cfg = QueueProcessorConfig()
    cfg.lock_heartbeat_interval_seconds = 0.1
    stop = start_lock_heartbeat(flaky, cfg, message)
    assert stop is not None
    time.sleep(0.8)
    stop.set()
    queue.ack(message)
    assert flaky.calls >= 3, f"C27 heartbeat stopped after its first failed renewal: {flaky.calls} call(s)"


@pytest.mark.parametrize("op", ["extend_lock", "ack"])
def test_a_worker_whose_lease_was_taken_cannot_touch_the_new_lease(queue: Any, op: str) -> None:
    short = type(queue)(queue.connection_string, table_name=queue.table_name, lock_duration=timedelta(milliseconds=300))
    try:
        short.push(StartWorkflow(execution_type="PIPELINE", execution_id="01HYYYYYYYYYYYYYYYYYYYYYYY"))
        first = short.poll_one()
        assert first is not None
        time.sleep(0.5)
        second = short.poll_one()
        assert second is not None and second.message_id == first.message_id
        if op == "extend_lock":
            extended = short.extend_lock(first)
            assert extended is False, "B79-1 a worker whose lease expired extended the new holder's lock"
        else:
            short.ack(first)
            kept = short.extend_lock(second)
            assert kept is True, "B79-1 a worker whose lease expired deleted the message under the new holder"
            short.ack(second)
    finally:
        short.clear()
        short.close()
