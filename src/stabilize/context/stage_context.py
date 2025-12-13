from __future__ import annotations
from collections.abc import Iterator, MutableMapping
from typing import TYPE_CHECKING, Any

class StageContext(MutableMapping[str, Any]):
    """
    Stage context with ancestor output lookup.

    When a key is not found in the stage's own context, it automatically
    searches ancestor stages' outputs. This enables data to flow from
    earlier stages to later ones.

    Example:
        # Stage A outputs: {"deploymentId": "abc123"}
        # Stage B (downstream) context lookup:

        context = StageContext(stage_b, stage_b.context)
        deployment_id = context["deploymentId"]  # Returns "abc123" from stage A
    """
    def __init__(
        self,
        stage: StageExecution,
        delegate: dict[str, Any],
    ) -> None:
        """
        Initialize the context.

        Args:
            stage: The stage this context belongs to
            delegate: The underlying context dictionary
        """
        self._stage = stage
        self._delegate = delegate

    def stage(self) -> StageExecution:
        """Get the stage this context belongs to."""
        return self._stage
