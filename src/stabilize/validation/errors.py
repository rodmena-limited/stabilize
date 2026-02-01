"""Validation error types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ValidationError:
    """
    A validation error with path and message.

    Attributes:
        path: JSON path to the invalid field (e.g., "timeout" or "servers[0].host")
        message: Description of the validation error
        value: The invalid value (if available)
        constraint: The constraint that was violated (if available)
    """

    path: str
    message: str
    value: Any = None
    constraint: str | None = None

    def __str__(self) -> str:
        if self.path:
            return f"{self.path}: {self.message}"
        return self.message
