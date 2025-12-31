"""
In-memory execution repository.

Useful for testing and development.
"""

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

    def store_stage(self, stage: StageExecution) -> None:
        """Store or update a stage."""
        with self._lock:
            execution_id = stage.execution.id
            if execution_id not in self._executions:
                raise WorkflowNotFoundError(execution_id)

            execution = self._executions[execution_id]

            # Find and update or add
            for i, s in enumerate(execution.stages):
                if s.id == stage.id:
                    # Update existing stage
                    execution.stages[i] = copy.deepcopy(stage)
                    execution.stages[i]._execution = execution
                    return

            # Add new stage
            new_stage = copy.deepcopy(stage)
            new_stage._execution = execution
            execution.stages.append(new_stage)

    def add_stage(self, stage: StageExecution) -> None:
        """Add a new stage."""
        self.store_stage(stage)

    def remove_stage(
        self,
        execution: Workflow,
        stage_id: str,
    ) -> None:
        """Remove a stage."""
        with self._lock:
            if execution.id not in self._executions:
                raise WorkflowNotFoundError(execution.id)

            stored = self._executions[execution.id]
            stored.stages = [s for s in stored.stages if s.id != stage_id]

    def retrieve_stage(self, stage_id: str) -> StageExecution:
        """Retrieve a single stage by ID."""
        with self._lock:
            for execution in self._executions.values():
                for stage in execution.stages:
                    if stage.id == stage_id:
                        # Return deep copy with lightweight execution
                        stage_copy = copy.deepcopy(stage)

                        exec_copy = copy.deepcopy(execution)
                        exec_copy.stages = [stage_copy]  # Only include this stage
                        stage_copy._execution = exec_copy

                        return stage_copy

            raise ValueError(f"Stage {stage_id} not found")

    def get_upstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """Get upstream stages."""
        with self._lock:
            if execution_id not in self._executions:
                return []

            execution = self._executions[execution_id]

            # Find target stage to get requisites
            target_stage = next((s for s in execution.stages if s.ref_id == stage_ref_id), None)
            if not target_stage:
                return []

            return [copy.deepcopy(s) for s in execution.stages if s.ref_id in target_stage.requisite_stage_ref_ids]

    def get_downstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """Get downstream stages."""
        with self._lock:
            if execution_id not in self._executions:
                return []

            execution = self._executions[execution_id]

            return [copy.deepcopy(s) for s in execution.stages if stage_ref_id in s.requisite_stage_ref_ids]

    def get_synthetic_stages(
        self,
        execution_id: str,
        parent_stage_id: str,
    ) -> list[StageExecution]:
        """Get synthetic stages."""
        with self._lock:
            if execution_id not in self._executions:
                return []

            execution = self._executions[execution_id]

            return [copy.deepcopy(s) for s in execution.stages if s.parent_stage_id == parent_stage_id]

    def get_merged_ancestor_outputs(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> dict[str, Any]:
        """Get merged outputs from all ancestor stages."""
        with self._lock:
            if execution_id not in self._executions:
                return {}

            execution = self._executions[execution_id]

            # Use Workflow.get_context logic but filtered for this stage
            # Or reuse the graph logic from other stores

            # Since we have the full execution in memory here, we can use
            # topological sort on the full graph and just filter ancestors
            from stabilize.dag.topological import topological_sort

            # Build ancestor set
            target_stage = next((s for s in execution.stages if s.ref_id == stage_ref_id), None)
            if not target_stage:
                return {}

            ancestors = set()
            queue = [target_stage]
            visited = {target_stage.id}

            stage_map = {s.ref_id: s for s in execution.stages}

            while queue:
                current = queue.pop(0)
                for req_ref in current.requisite_stage_ref_ids:
                    if req_ref in stage_map:
                        req_stage = stage_map[req_ref]
                        if req_stage.id not in visited:
                            visited.add(req_stage.id)
                            ancestors.add(req_stage.id)
                            queue.append(req_stage)

            # Sort full list and filter
            sorted_stages = topological_sort(execution.stages)

            result: dict[str, Any] = {}
            for stage in sorted_stages:
                if stage.id in ancestors:
                    for key, value in stage.outputs.items():
                        if key in result and isinstance(result[key], list) and isinstance(value, list):
                            existing = result[key]
                            for item in value:
                                if item not in existing:
                                    existing.append(item)
                        else:
                            result[key] = value

            return result

    def retrieve_by_pipeline_config_id(
        self,
        pipeline_config_id: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Retrieve executions by pipeline config ID."""
        with self._lock:
            executions = [
                copy.deepcopy(e) for e in self._executions.values() if e.pipeline_config_id == pipeline_config_id
            ]

        # Apply criteria
        executions = self._apply_criteria(executions, criteria)

        yield from executions

    def retrieve_by_application(
        self,
        application: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Retrieve executions by application."""
        with self._lock:
            executions = [copy.deepcopy(e) for e in self._executions.values() if e.application == application]

        # Apply criteria
        executions = self._apply_criteria(executions, criteria)

        yield from executions

    def _apply_criteria(
        self,
        executions: list[Workflow],
        criteria: WorkflowCriteria | None,
    ) -> list[Workflow]:
        """Apply query criteria to executions."""
        if criteria is None:
            return executions

        # Filter by status
        if criteria.statuses:
            executions = [e for e in executions if e.status in criteria.statuses]

        # Filter by start time
        if criteria.start_time_before:
            executions = [e for e in executions if e.start_time and e.start_time < criteria.start_time_before]

        if criteria.start_time_after:
            executions = [e for e in executions if e.start_time and e.start_time > criteria.start_time_after]

        # Sort by start time (newest first) and limit
        executions.sort(key=lambda e: e.start_time or 0, reverse=True)
        return executions[: criteria.page_size]

    def pause(self, execution_id: str, paused_by: str) -> None:
        """Pause an execution."""
        with self._lock:
            if execution_id not in self._executions:
                raise WorkflowNotFoundError(execution_id)

            execution = self._executions[execution_id]
            execution.paused = PausedDetails(
                paused_by=paused_by,
                pause_time=int(time.time() * 1000),
            )
            execution.status = WorkflowStatus.PAUSED

    def resume(self, execution_id: str) -> None:
        """Resume a paused execution."""
        with self._lock:
            if execution_id not in self._executions:
                raise WorkflowNotFoundError(execution_id)

            execution = self._executions[execution_id]
            if execution.paused:
                current_time = int(time.time() * 1000)
                execution.paused.resume_time = current_time
                if execution.paused.pause_time:
                    execution.paused.paused_ms = current_time - execution.paused.pause_time
            execution.status = WorkflowStatus.RUNNING

    def cancel(
        self,
        execution_id: str,
        canceled_by: str,
        reason: str,
    ) -> None:
        """Cancel an execution."""
        with self._lock:
            if execution_id not in self._executions:
                raise WorkflowNotFoundError(execution_id)

            execution = self._executions[execution_id]
            execution.is_canceled = True
            execution.canceled_by = canceled_by
            execution.cancellation_reason = reason

    def clear(self) -> None:
        """Clear all executions."""
        with self._lock:
            self._executions.clear()

    def count(self) -> int:
        """Get total number of executions."""
        with self._lock:
            return len(self._executions)
