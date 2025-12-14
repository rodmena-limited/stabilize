from __future__ import annotations
from collections.abc import Callable
from typing import TYPE_CHECKING

class CircularDependencyError(Exception):
    """
    Raised when a circular dependency is detected in the stage graph.

    This indicates an invalid pipeline configuration where stages depend
    on each other in a cycle.
    """
    def __init__(self, message: str, stages: list[StageExecution] | None = None):
        super().__init__(message)
        self.stages = stages or []
