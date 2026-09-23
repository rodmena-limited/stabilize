"""Discarding buffered signals when a stage reaches a terminal status."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.status import WorkflowStatus

logger = logging.getLogger(__name__)


def discard_pending_signals(
    repository: Any,
    stage: StageExecution,
    new_status: WorkflowStatus,
) -> None:
    store_pending = 0
    if repository.supports_signal_storage() and stage.execution is not None:
        store_pending = repository.pending_signal_count(stage.execution.id, stage.ref_id)
        if store_pending:
            repository.discard_signals(stage.execution.id, stage.ref_id)

    discarded = stage.context.pop("_buffered_signals", None)
    count = len(discarded) if discarded else store_pending
    if count:
        logger.warning(
            "Discarding %d buffered signal(s) on stage %s (ref_id=%s) entering %s",
            count,
            stage.id,
            stage.ref_id,
            new_status,
        )
