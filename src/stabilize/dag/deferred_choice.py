"""Deferred choice (WCP-16) claim modes and the group validation Workflow.create runs."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

CLAIM_KEY = "deferredChoiceClaim"
ON_START = "on_start"
ON_COMPLETE = "on_complete"
CLAIM_MODES = frozenset({ON_START, ON_COMPLETE})


def claim_mode(stage: StageExecution) -> str:
    """How this member settles its deferred choice: when it starts (the default) or when it completes."""
    value = stage.context.get(CLAIM_KEY)
    return ON_START if value is None else str(value)


def validate_deferred_choice_groups(stages: Iterable[StageExecution]) -> None:
    """Refuse a group whose members name different claim modes, or a mode that does not exist."""
    modes: dict[str, dict[str, list[str]]] = {}
    for stage in stages:
        if not stage.deferred_choice_group:
            continue
        mode = claim_mode(stage)
        if mode not in CLAIM_MODES:
            raise ValueError(
                f"stage {stage.ref_id!r}: {CLAIM_KEY} must be {ON_START!r} or {ON_COMPLETE!r}, not {mode!r}"
            )
        modes.setdefault(stage.deferred_choice_group, {}).setdefault(mode, []).append(stage.ref_id)
    for group, by_mode in modes.items():
        if len(by_mode) > 1:
            detail = "; ".join(f"{mode}: {', '.join(refs)}" for mode, refs in sorted(by_mode.items()))
            raise ValueError(f"deferred choice group {group!r} mixes {CLAIM_KEY} values ({detail})")
