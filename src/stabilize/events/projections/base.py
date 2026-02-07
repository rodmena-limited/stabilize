"""
Base projection class.

Projections are read-only views built by applying events.
They can be used to build analytics dashboards, timelines,
or any other derived data.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from stabilize.events.base import Event


class Projection(ABC):
    """
    Abstract base class for projections.

    Projections apply events to build a specialized view of the data.
    They are typically used to:
    - Build human-readable timelines
    - Aggregate metrics
    - Create search indexes
    - Derive state for specific queries

    Projections are stateful and should be reset before replaying
    events from the beginning.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name for this projection."""
        pass

    @abstractmethod
    def apply(self, event: Event) -> None:
        """
        Apply an event to update the projection state.

        This method should be idempotent - applying the same event
        multiple times should have the same effect as applying it once.

        Args:
            event: The event to apply.
        """
        pass

    @abstractmethod
    def get_state(self) -> Any:
        """
        Get the current projection state.

        Returns:
            The current state, type depends on the projection.
        """
        pass

    def reset(self) -> None:
        """
        Reset the projection to its initial state.

        Override this method in subclasses if the projection
        needs cleanup beyond just reinitializing.
        """
        pass

    def handles_event_type(self, event: Event) -> bool:
        """
        Check if this projection handles the given event type.

        By default, all events are handled. Override to filter.

        Args:
            event: The event to check.

        Returns:
            True if this projection should process the event.
        """
        return True
