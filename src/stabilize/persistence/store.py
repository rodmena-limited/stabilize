from __future__ import annotations
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from stabilize.models.status import WorkflowStatus

class WorkflowNotFoundError(Exception):
    """Raised when an execution cannot be found."""
    def __init__(self, execution_id: str):
        self.execution_id = execution_id
        super().__init__(f"Execution not found: {execution_id}")

@dataclass
class WorkflowCriteria:
    """Criteria for querying executions."""
    page_size: int = 20
    statuses: set[WorkflowStatus] | None = None
    start_time_before: int | None = None
    start_time_after: int | None = None

class WorkflowStore(ABC):
    """Abstract interface for execution persistence."""
