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

    def store(self, execution: Workflow) -> None:
        """
        Store a complete execution.

        Creates the execution and all its stages.

        Args:
            execution: The execution to store
        """
        pass

    def retrieve(self, execution_id: str) -> Workflow:
        """
        Retrieve an execution by ID.

        Args:
            execution_id: The execution ID

        Returns:
            The execution

        Raises:
            WorkflowNotFoundError: If not found
        """
        pass

    def retrieve_execution_summary(self, execution_id: str) -> Workflow:
        """
        Retrieve execution metadata without stages.

        Args:
            execution_id: The execution ID

        Returns:
            The execution with empty stages list

        Raises:
            WorkflowNotFoundError: If not found
        """
        pass

    def update_status(self, execution: Workflow) -> None:
        """
        Update the status of an execution.

        Args:
            execution: The execution with updated status
        """
        pass

    def delete(self, execution_id: str) -> None:
        """
        Delete an execution and all its stages.

        Args:
            execution_id: The execution ID
        """
        pass

    def store_stage(self, stage: StageExecution) -> None:
        """
        Store or update a stage.

        Args:
            stage: The stage to store
        """
        pass

    def add_stage(self, stage: StageExecution) -> None:
        """
        Add a new stage to an execution.

        Args:
            stage: The stage to add
        """
        pass

    def remove_stage(
        self,
        execution: Workflow,
        stage_id: str,
    ) -> None:
        """
        Remove a stage from an execution.

        Args:
            execution: The execution
            stage_id: The stage ID to remove
        """
        pass

    def retrieve_stage(self, stage_id: str) -> StageExecution:
        """
        Retrieve a single stage by ID.

        The returned stage will have a partial parent execution attached
        (containing metadata but no other stages).

        Args:
            stage_id: The stage ID

        Returns:
            The stage execution

        Raises:
            ValueError: If stage not found
        """
        pass

    def get_upstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """
        Get upstream stages for a given stage.

        Args:
            execution_id: The execution ID
            stage_ref_id: The reference ID of the stage

        Returns:
            List of upstream stages
        """
        pass

    def get_downstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """
        Get downstream stages for a given stage.

        Args:
            execution_id: The execution ID
            stage_ref_id: The reference ID of the stage

        Returns:
            List of downstream stages
        """
        pass

    def get_synthetic_stages(
        self,
        execution_id: str,
        parent_stage_id: str,
    ) -> list[StageExecution]:
        """
        Get synthetic stages for a given parent stage.

        Args:
            execution_id: The execution ID
            parent_stage_id: The parent stage ID

        Returns:
            List of synthetic stages
        """
        pass
