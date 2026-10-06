from __future__ import annotations

import time
from datetime import timedelta
from pathlib import Path

from stabilize import SqliteQueue, SqliteWorkflowStore
from stabilize.queue.messages import StartWorkflow


def _setup(tmp_path: Path) -> tuple[SqliteWorkflowStore, SqliteQueue]:
    url = f"sqlite:///{tmp_path}/clock.db"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url, lock_duration=timedelta(minutes=5))
    return store, queue


def test_sub_second_delay_is_not_delivered_early(tmp_path: Path) -> None:
    _, queue = _setup(tmp_path)
    while time.time() % 1.0 > 0.1:
        time.sleep(0.01)
    queue.push(StartWorkflow(execution_type="workflow", execution_id="soon"), delay=timedelta(milliseconds=700))
    assert queue.poll_one() is None
    time.sleep(0.9)
    assert queue.poll_one() is not None


def test_locked_message_is_not_polled_again(tmp_path: Path) -> None:
    _, queue = _setup(tmp_path)
    queue.push(StartWorkflow(execution_type="workflow", execution_id="now"))
    assert queue.poll_one() is not None
    assert queue.poll_one() is None


def test_retention_keeps_marks_younger_than_the_window(tmp_path: Path) -> None:
    store, _ = _setup(tmp_path)
    store.mark_message_processed("m1", handler_type="StartStage", execution_id="w")
    store.cleanup_old_processed_messages(max_age_hours=1.0)
    assert store.is_message_processed("m1")
    time.sleep(0.05)
    store.cleanup_old_processed_messages(max_age_hours=0.0)
    assert not store.is_message_processed("m1")
