"""Workflow query criteria and exceptions."""

from __future__ import annotations

from dataclasses import dataclass

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
