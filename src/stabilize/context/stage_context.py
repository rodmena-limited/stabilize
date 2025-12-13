"""
Stage context with ancestor output lookup.

The StageContext class provides access to stage inputs with automatic
fallback to ancestor stage outputs. This enables data flow between
stages in a pipeline.
"""

from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution


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

    @property
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

    def __delitem__(self, key: str) -> None:
        """Delete a value from the context."""
        del self._delegate[key]

    def __iter__(self) -> Iterator[str]:
        """Iterate over keys."""
        return iter(self._delegate)

    def __len__(self) -> int:
        """Get number of keys in own context."""
        return len(self._delegate)

    def __contains__(self, key: object) -> bool:
        """Check if key exists in context or ancestors."""
        if key in self._delegate:
            return True
        if isinstance(key, str):
            for ancestor in self._stage.ancestors():
                if key in ancestor.outputs:
                    return True
        return False

    def get(self, key: str, default: Any = None) -> Any:
        """Get with default value."""
        try:
            return self[key]
        except KeyError:
            return default

    def get_current_only(self, key: str, default: Any = None) -> Any:
        """
        Get a value only from the current stage's context.

        Does not search ancestor outputs.
        """
        return self._delegate.get(key, default)

    def get_all(self, key: str) -> list[Any]:
        """
        Get all values of a key from ancestors.

        Returns values sorted by proximity (closest ancestor first).
        """
        values = []
        for ancestor in self._stage.ancestors():
            if key in ancestor.outputs:
                values.append(ancestor.outputs[key])
        return values

    def to_dict(self) -> dict[str, Any]:
        """Get the underlying dictionary."""
        return dict(self._delegate)

    def merge_with_ancestors(self) -> dict[str, Any]:
        """
        Get merged context including all ancestor outputs.

        Returns a new dictionary with all keys from this context
        and all ancestor outputs. Own values take precedence.
        """
        merged = {}

        # Start with ancestor outputs (reversed so closest overwrites)
        for ancestor in reversed(self._stage.ancestors()):
            merged.update(ancestor.outputs)

        # Own context takes precedence
        merged.update(self._delegate)

        return merged


def create_merged_context(stage: StageExecution) -> StageContext:
    """
    Create a StageContext with merged ancestor data.

    The returned context will have the stage's own context merged
    with ancestor outputs, with the stage's values taking precedence.

    Args:
        stage: The stage to create context for

    Returns:
        A StageContext with merged data
    """
    merged = {}

    # Collect ancestor outputs
    for ancestor in reversed(stage.ancestors()):
        merged.update(ancestor.outputs)

    # Own context takes precedence
    merged.update(stage.context)

    return StageContext(stage, merged)
