"""The context keys the engine writes, enumerated.

``StageExecution.context`` holds two things: what the caller put there, and what
the engine needs to remember about the stage. Nothing has ever distinguished
them, so an engine key added in one release appeared without warning in a task's
``INPUT`` and in every consumer's ``SELECT``.

This module is the published boundary. Consumers reading ``stage_executions``
in SQL generate their exclusion from this set rather than guessing, and
``test_engine_context_keys.py`` fails when a key is written that is not
registered here — so a new one cannot land unannounced.

An earlier design nested these under one ``_engine`` envelope. It was withdrawn:
these keys legitimately vary by execution path (a stage that took a retry jump
carries ``_jump_count``; its sibling does not), so nesting them would not have
made any consumer's key count stable, and rows written before the change would
have kept the old shape indefinitely. The enumeration delivers what the envelope
was chosen for and covers every key rather than three.
"""

from __future__ import annotations

HYDRATION_KEYS = frozenset(
    {
        "_hydrated_keys",
    }
)

CONFIGURATION_KEYS = frozenset(
    {
        "_output_reducers",
    }
)

CONTROL_FLOW_KEYS = frozenset(
    {
        "_activated_branches",
        "_blocking_failure",
        "_completed_branches",
        "_join_fired",
        "_jump_bypass",
        "_jump_count",
        "_jump_history",
        "_jump_outputs",
        "_loop_scope",
        "_max_jumps",
        "_max_recursion_depth",
        "_mi_instance_count",
        "_mi_instance_index",
        "_mi_parent_ref_id",
        "_mm_firings",
        "_on_failure_planned",
        "_parent_workflow_id",
        "_recursion_depth",
        "_sub_workflow_config",
        "_sub_workflow_id",
    }
)

SIGNAL_KEYS = frozenset(
    {
        "_buffered_signals",
        "_signal_data",
        "_signal_name",
    }
)

ENGINE_CONTEXT_KEYS = (
    HYDRATION_KEYS | CONFIGURATION_KEYS | CONTROL_FLOW_KEYS | SIGNAL_KEYS
)

TASK_VISIBLE_KEYS = frozenset(
    {
        "_signal_data",
        "_signal_name",
    }
)

HIDDEN_FROM_TASK_INPUT = ENGINE_CONTEXT_KEYS - TASK_VISIBLE_KEYS


def caller_context(context: dict[str, object]) -> dict[str, object]:
    """The caller's own context: everything the engine did not write."""
    return {k: v for k, v in context.items() if k not in ENGINE_CONTEXT_KEYS}
