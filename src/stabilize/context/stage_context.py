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

    def __getitem__(self, key: str) -> Any:
        """
        Get a value by key.

        First checks the stage's own context, then falls back to
        ancestor outputs.
        """
        # Check own context first
        if key in self._delegate:
            return self._delegate[key]

        # Search ancestor outputs
        for ancestor in self._stage.ancestors():
            if key in ancestor.outputs:
                return ancestor.outputs[key]

        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        """Set a value in the context."""
        self._delegate[key] = value
