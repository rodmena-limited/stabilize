"""The processed-message retention sweep, on its own thread.

Each sweep deletes processed_messages rows of this processor's queue table
older than processed_messages_max_age_hours whose message is no longer in
that table, in batches of retention_batch_size rows, one
transaction per batch, and stops after retention_max_rows_per_sweep rows, so
message polling never waits for it and no transaction holds more than one
batch.
"""

from __future__ import annotations

import inspect
import logging
import threading
import time
from typing import Any

from stabilize.threads import in_context

logger = logging.getLogger(__name__)


def _accepts(store: Any, name: str) -> bool:
    try:
        params = inspect.signature(store.cleanup_old_processed_messages).parameters
    except (TypeError, ValueError):
        return False
    return name in params or any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())


def sweep_once(processor: Any) -> int:
    store = processor._store
    cfg = processor.config
    try:
        if _accepts(store, "live"):
            queue = processor.queue
            removed = store.cleanup_old_processed_messages(
                max_age_hours=cfg.processed_messages_max_age_hours,
                batch_size=cfg.retention_batch_size,
                max_rows=cfg.retention_max_rows_per_sweep,
                table_name=getattr(queue, "table_name", None),
                live=queue.live_delivery_keys,
            )
        elif _accepts(store, "max_rows"):
            removed = store.cleanup_old_processed_messages(
                max_age_hours=cfg.processed_messages_max_age_hours,
                batch_size=cfg.retention_batch_size,
                max_rows=cfg.retention_max_rows_per_sweep,
            )
        else:
            removed = store.cleanup_old_processed_messages(max_age_hours=cfg.processed_messages_max_age_hours)
        claims = store.cleanup_completed_stage_claims()
    except Exception as e:
        processor._retention_failures += 1
        logger.error("Retention sweep failed (%d so far): %s", processor._retention_failures, e, exc_info=True)
        return 0
    if removed or claims:
        logger.info("Retention sweep removed %d processed-message record(s), %d stage claim(s)", removed, claims)
    return int(removed)


def retention_loop(processor: Any) -> None:
    interval = processor.config.retention_sweep_interval_seconds
    while processor._running:
        waited = 0.0
        while waited < interval and processor._running:
            step = min(0.5, interval - waited)
            time.sleep(step)
            waited += step
        if not processor._running or processor._stopping:
            break
        sweep_once(processor)


def start_retention_thread(processor: Any) -> threading.Thread | None:
    interval = getattr(processor.config, "retention_sweep_interval_seconds", 0.0)
    if not interval or interval <= 0 or processor._store is None:
        return None
    thread = threading.Thread(
        target=in_context(retention_loop), args=(processor,), daemon=True, name="stabilize-retention"
    )
    thread.start()
    return thread
