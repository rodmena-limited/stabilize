"""The task generation fence: a message belongs to the generation of the task it was created for."""

from __future__ import annotations

from typing import Any


def same_generation(task: Any, message: Any) -> bool:
    """Whether *message* belongs to the task's current generation; a message without one reads as generation 0."""
    return int(getattr(message, "generation", None) or 0) == int(getattr(task, "generation", 0) or 0)
