"""
Multi-instance configuration for WCP-12 through WCP-15.

Defines how a stage spawns and synchronizes multiple parallel instances.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class MultiInstanceConfig:
    """Configuration for multi-instance stage execution.

    Supports several patterns:
    - WCP-12: count > 0, sync_on_complete=False (fire and forget)
    - WCP-13: count > 0, sync_on_complete=True (design-time known count)
    - WCP-14: count_from_context set, sync_on_complete=True (runtime known count)
    - WCP-15: allow_dynamic=True (count not known until last instance)

    Attributes:
        count: Fixed number of instances (0 means use count_from_context).
        count_from_context: Context key holding the instance count at runtime.
        sync_on_complete: Whether to wait for all instances before continuing.
        allow_dynamic: Whether new instances can be added during execution.
        collection_from_context: Context key holding items to iterate over.
        join_threshold: If set, continue after this many complete (N-of-M).
        cancel_remaining: Cancel remaining instances after threshold reached.
    """

    count: int = 0
    count_from_context: str = ""
    sync_on_complete: bool = True
    allow_dynamic: bool = False
    collection_from_context: str = ""
    join_threshold: int = 0
    cancel_remaining: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "count": self.count,
            "count_from_context": self.count_from_context,
            "sync_on_complete": self.sync_on_complete,
            "allow_dynamic": self.allow_dynamic,
            "collection_from_context": self.collection_from_context,
            "join_threshold": self.join_threshold,
            "cancel_remaining": self.cancel_remaining,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MultiInstanceConfig:
        """Deserialize from dictionary."""
        return cls(
            count=data.get("count", 0),
            count_from_context=data.get("count_from_context", ""),
            sync_on_complete=data.get("sync_on_complete", True),
            allow_dynamic=data.get("allow_dynamic", False),
            collection_from_context=data.get("collection_from_context", ""),
            join_threshold=data.get("join_threshold", 0),
            cancel_remaining=data.get("cancel_remaining", False),
        )
