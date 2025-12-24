"""
WorkflowStore interface.

This module defines the abstract interface for execution persistence.
All storage backends must implement this interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

from stabilize.models.status import WorkflowStatus

if TYPE_CHECKING:
    from stabilize.models.workflow import Workflow
    from stabilize.models.stage import StageExecution


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

    # ========== Execution Operations ==========

    @abstractmethod
    def store(self, execution: Workflow) -> None:
        """
        Store a complete execution.

        Creates the execution and all its stages.

        Args:
            execution: The execution to store
        """
        pass

    @abstractmethod
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

    @abstractmethod
    def update_status(self, execution: Workflow) -> None:
        """
        Update the status of an execution.

        Args:
            execution: The execution with updated status
        """
        pass

    @abstractmethod
    def delete(self, execution_id: str) -> None:
        """
        Delete an execution and all its stages.

        Args:
            execution_id: The execution ID
        """
        pass

    # ========== Stage Operations ==========

    @abstractmethod
    def store_stage(self, stage: StageExecution) -> None:
        """
        Store or update a stage.

        Args:
            stage: The stage to store
        """
        pass

    @abstractmethod
    def add_stage(self, stage: StageExecution) -> None:
        """
        Add a new stage to an execution.

        Args:
            stage: The stage to add
        """
        pass

    @abstractmethod
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

    # ========== Query Operations ==========

    @abstractmethod
    def retrieve_by_pipeline_config_id(
        self,
        pipeline_config_id: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """
        Retrieve executions by pipeline config ID.

        Args:
            pipeline_config_id: The pipeline config ID
            criteria: Optional query criteria

        Returns:
            Iterator of matching executions
        """
        pass

    @abstractmethod
    def retrieve_by_application(
        self,
        application: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """
        Retrieve executions by application.

        Args:
            application: The application name
            criteria: Optional query criteria

        Returns:
            Iterator of matching executions
        """
        pass

    # ========== Pause/Resume Operations ==========

    @abstractmethod
    def pause(
        self,
        execution_id: str,
        paused_by: str,
    ) -> None:
        """
        Pause an execution.

        Args:
            execution_id: The execution ID
            paused_by: Who paused it
        """
        pass

    @abstractmethod
    def resume(self, execution_id: str) -> None:
        """
        Resume a paused execution.

        Args:
            execution_id: The execution ID
        """
        pass

    # ========== Cancel Operations ==========

    @abstractmethod
    def cancel(
        self,
        execution_id: str,
        canceled_by: str,
        reason: str,
    ) -> None:
        """
        Cancel an execution.

        Args:
            execution_id: The execution ID
            canceled_by: Who canceled it
            reason: Cancellation reason
        """
        pass

    # ========== Optional Methods ==========

    def is_healthy(self) -> bool:
        """
        Check if the repository is healthy.

        Returns:
            True if healthy
        """
        return True

    def count_by_application(self, application: str) -> int:
        """
        Count executions for an application.

        Args:
            application: The application name

        Returns:
            Number of executions
        """
        return sum(1 for _ in self.retrieve_by_application(application))
