from __future__ import annotations

from stabilize import QueueProcessor, TaskRegistry
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.resilience.config import HandlerConfig


def test_handler_config_reaches_every_default_handler(repository: WorkflowStore, queue: Queue) -> None:
    cfg = HandlerConfig(max_stage_wait_retries=7, handler_retry_delay_seconds=1.5, default_page_size=3)
    processor = QueueProcessor(queue, store=repository, task_registry=TaskRegistry(), handler_config=cfg)
    ignoring = sorted(
        type(h).__name__
        for h in processor._handlers.values()
        if hasattr(h, "handler_config") and h.handler_config is not cfg
    )
    assert ignoring == [], f"C37 handlers ignoring handler_config: {ignoring}"
