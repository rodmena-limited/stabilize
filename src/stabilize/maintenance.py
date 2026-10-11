"""Bulk maintenance through the product interface: purge completed workflows."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

PURGEABLE = ("SUCCEEDED", "FAILED_CONTINUE", "TERMINAL", "CANCELED", "STOPPED", "SKIPPED")


def purge_completed_workflows(
    store: Any,
    queue: Any,
    completed_before_ms: int,
    batch_size: int = 500,
    max_workflows: int | None = None,
    application: str | None = None,
) -> int:
    """Delete complete workflows that ended before completed_before_ms.

    Each workflow goes through store.delete(), which removes its stages, tasks,
    processed-message records, stage claims and signals; with a queue, its
    queued and dead-lettered messages are removed too. Workflows that are not
    complete, or have no end_time, are never selected. Event-store rows are kept.
    """
    deleted = 0
    while max_workflows is None or deleted < max_workflows:
        limit = batch_size if max_workflows is None else min(batch_size, max_workflows - deleted)
        ids = store.completed_workflow_ids(completed_before_ms, limit, application)
        if not ids:
            break
        for workflow_id in ids:
            store.delete(workflow_id)
            if queue is not None:
                queue.purge_workflow(workflow_id)
            deleted += 1
        if len(ids) < limit:
            break
    logger.info("Purged %d completed workflow(s) that ended before %d", deleted, completed_before_ms)
    return deleted
