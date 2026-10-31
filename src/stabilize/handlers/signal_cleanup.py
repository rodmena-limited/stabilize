"""Dropping the context-buffered signals of a stage that can no longer receive them."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.status import WorkflowStatus

logger = logging.getLogger(__name__)


def drop_context_signals(stage: StageExecution, new_status: WorkflowStatus) -> None:
    discarded = stage.context.pop("_buffered_signals", None)
    if discarded:
        logger.warning(
            "Discarding %d context-buffered signal(s) on stage %s (ref_id=%s) entering %s",
            len(discarded),
            stage.id,
            stage.ref_id,
            new_status,
        )
