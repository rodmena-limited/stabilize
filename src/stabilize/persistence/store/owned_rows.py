"""Tables holding rows owned by one workflow, in the order a delete must visit them."""

from __future__ import annotations

WORKFLOW_OWNED_ROWS: tuple[tuple[str, str], ...] = (
    ("processed_messages", "execution_id"),
    ("stage_claims", "execution_id"),
    ("workflow_signals", "execution_id"),
    ("pipeline_executions", "id"),
)
