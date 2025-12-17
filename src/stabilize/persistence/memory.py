from __future__ import annotations
import copy
import threading
import time
from collections.abc import Iterator
from typing import Any
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.workflow import PausedDetails, Workflow
from stabilize.persistence.store import (
    WorkflowCriteria,
    WorkflowNotFoundError,
    WorkflowStore,
)

class InMemoryWorkflowStore(WorkflowStore):
    """
    In-memory implementation of WorkflowStore.

    Thread-safe storage for testing and single-process execution.
    """
    def __init__(self) -> None:
        self._executions: dict[str, Workflow] = {}
        self._lock = threading.Lock()

    def store(self, execution: Workflow) -> None:
        """Store a complete execution."""
        with self._lock:
            # Deep copy to prevent external modifications
            self._executions[execution.id] = copy.deepcopy(execution)

    def retrieve(self, execution_id: str) -> Workflow:
        """Retrieve an execution by ID."""
        with self._lock:
            if execution_id not in self._executions:
                raise WorkflowNotFoundError(execution_id)
            # Return a deep copy to prevent external modifications
            return copy.deepcopy(self._executions[execution_id])

    def retrieve_execution_summary(self, execution_id: str) -> Workflow:
        """Retrieve execution metadata without stages."""
        with self._lock:
            if execution_id not in self._executions:
                raise WorkflowNotFoundError(execution_id)

            # Deep copy but clear stages
            execution = copy.deepcopy(self._executions[execution_id])
            execution.stages = []
            return execution

    def update_status(self, execution: Workflow) -> None:
        """Update execution status."""
        with self._lock:
            if execution.id not in self._executions:
                raise WorkflowNotFoundError(execution.id)

            stored = self._executions[execution.id]
            stored.status = execution.status
            stored.start_time = execution.start_time
            stored.end_time = execution.end_time
            stored.is_canceled = execution.is_canceled
            stored.canceled_by = execution.canceled_by
            stored.cancellation_reason = execution.cancellation_reason
            stored.paused = execution.paused

    def delete(self, execution_id: str) -> None:
        """Delete an execution."""
        with self._lock:
            if execution_id in self._executions:
                del self._executions[execution_id]
