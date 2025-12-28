from __future__ import annotations
from typing import TYPE_CHECKING, Any, TypeVar
T = TypeVar("T")

class StabilizeError(Exception):
    """Base exception for Stabilize errors."""

class StabilizeFatalError(StabilizeError):
    """
    Fatal error that should terminate the pipeline.

    Fatal errors indicate unrecoverable situations like invalid configuration
    or programming errors.
    """
