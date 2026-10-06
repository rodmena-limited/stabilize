from __future__ import annotations

import threading
import time
from pathlib import Path

from stabilize import QueueProcessor, SqliteQueue, SqliteWorkflowStore
from stabilize.lifecycle import LifecycleManager
from stabilize.queue.messages import StartWorkflow
from stabilize.queue.processor.config import QueueProcessorConfig


def _blocked_processor(tmp_path: Path, release: threading.Event, entered: threading.Event) -> QueueProcessor:
    SqliteWorkflowStore(f"sqlite:///{tmp_path}/q.db", create_tables=True)
    queue = SqliteQueue(f"sqlite:///{tmp_path}/q.db", table_name="queue_messages")
    processor = QueueProcessor(queue, config=QueueProcessorConfig(poll_frequency_ms=10, enable_deduplication=False))

    def handler(message: StartWorkflow) -> None:
        entered.set()
        release.wait()

    processor.register_handler_func(StartWorkflow, handler)
    queue.push(StartWorkflow(execution_type="workflow", execution_id="x"))
    processor.start()
    assert entered.wait(10)
    return processor


def test_stop_returns_within_timeout_and_reports_running(tmp_path: Path) -> None:
    release, entered = threading.Event(), threading.Event()
    processor = _blocked_processor(tmp_path, release, entered)
    try:
        t0 = time.monotonic()
        remaining = processor.stop(wait=True, timeout=0.5)
        assert time.monotonic() - t0 < 2.0
        assert remaining == 1
    finally:
        release.set()


def test_lifecycle_shutdown_honours_its_timeout(tmp_path: Path) -> None:
    release, entered = threading.Event(), threading.Event()
    processor = _blocked_processor(tmp_path, release, entered)
    manager = LifecycleManager(shutdown_timeout=0.5, graceful_shutdown_delay=0.0)
    manager.register_processor(processor)
    done = threading.Event()
    threading.Thread(target=lambda: (manager._shutdown(), done.set()), daemon=True).start()
    try:
        assert done.wait(5.0)
    finally:
        release.set()
