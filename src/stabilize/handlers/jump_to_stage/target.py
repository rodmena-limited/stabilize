from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.handlers.jump_to_stage.reset import reset_stage_for_retry
from stabilize.handlers.start_stage.planner import HYDRATED_KEYS

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.queue.messages import JumpToStage


def apply_jump_to_target(
    stage: StageExecution,
    *,
    message: JumpToStage,
    jump_count: int,
    history: list[dict[str, Any]],
) -> None:
    reset_stage_for_retry(stage)
    if message.jump_context:
        stage.context.update(message.jump_context)
        hydrated = stage.context.get(HYDRATED_KEYS)
        if isinstance(hydrated, list):
            stage.context[HYDRATED_KEYS] = [key for key in hydrated if key not in message.jump_context]
    if message.jump_outputs:
        stage.context["_jump_outputs"] = message.jump_outputs
    stage.context["_jump_bypass"] = True
    stage.context["_jump_count"] = jump_count
    stage.context["_jump_history"] = list(history)
