"""
Query operations mixin for SqliteWorkflowStore.

Provides graph traversal queries, listing, filtering, and
pause/resume/cancel operations.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from stabilize.persistence.sqlite.converters import row_to_execution
from stabilize.persistence.sqlite.operations import (
    cancel_execution,
    pause_execution,
    resume_execution,
)
from stabilize.persistence.sqlite.queries import get_downstream_stages as _get_downstream_stages
from stabilize.persistence.sqlite.queries import (
    get_merged_ancestor_outputs as _get_merged_ancestor_outputs,
)
from stabilize.persistence.sqlite.queries import get_synthetic_stages as _get_synthetic_stages
from stabilize.persistence.sqlite.queries import get_upstream_stages as _get_upstream_stages
from stabilize.persistence.sqlite.queries import (
    retrieve_by_application as _retrieve_by_application,
)
from stabilize.persistence.sqlite.queries import (
    retrieve_by_pipeline_config_id as _retrieve_by_pipeline_config_id,
)
from stabilize.persistence.store import WorkflowCriteria

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow


class SqliteQueriesMixin:
    """Mixin providing query and filtering operations."""

    if TYPE_CHECKING:

        def _get_connection(self) -> sqlite3.Connection: ...

        def retrieve(self, execution_id: str) -> Workflow: ...

    def get_upstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """Get upstream stages with tasks loaded."""
        return _get_upstream_stages(self._get_connection(), execution_id, stage_ref_id)

    def get_downstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """Get downstream stages with tasks loaded."""
        return _get_downstream_stages(self._get_connection(), execution_id, stage_ref_id)

    def get_synthetic_stages(
        self,
        execution_id: str,
        parent_stage_id: str,
    ) -> list[StageExecution]:
        """Get synthetic stages with tasks loaded."""
        return _get_synthetic_stages(self._get_connection(), execution_id, parent_stage_id)

    def get_merged_ancestor_outputs(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> dict[str, Any]:
        """Get merged outputs from all ancestor stages."""
        return _get_merged_ancestor_outputs(self._get_connection(), execution_id, stage_ref_id)

    def retrieve_by_pipeline_config_id(
        self,
        pipeline_config_id: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Retrieve executions by pipeline config ID."""
        return _retrieve_by_pipeline_config_id(self._get_connection(), pipeline_config_id, criteria, self.retrieve)

    def retrieve_by_application(
        self,
        application: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Retrieve executions by application."""
        return _retrieve_by_application(self._get_connection(), application, criteria, self.retrieve)

    def list_workflows(
        self,
        application: str,
        limit: int = 100,
    ) -> list[Workflow]:
        """List workflows for an application.

        Args:
            application: Application name to filter by
            limit: Maximum number of workflows to return

        Returns:
            List of Workflow objects
        """
        conn = self._get_connection()
        result = conn.execute(
            """
            SELECT * FROM pipeline_executions
            WHERE application = :application
            ORDER BY created_at DESC
            LIMIT :limit
            """,
            {"application": application, "limit": limit},
        )

        workflows = []
        for row in result.fetchall():
            workflows.append(row_to_execution(row))
        return workflows

    def get_all_pending_workflows(
        self,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Get all workflows matching criteria across all applications.

        This is used by recovery to find workflows that need recovery
        without filtering by application.

        Args:
            criteria: Optional criteria for filtering (statuses, start_time, etc.)

        Yields:
            Workflow objects matching the criteria
        """
        conn = self._get_connection()
        query = "SELECT id FROM pipeline_executions WHERE 1=1"
        params: dict[str, Any] = {}

        if criteria and criteria.statuses:
            status_names = [s.name for s in criteria.statuses]
            placeholders = ", ".join(f":status_{i}" for i in range(len(status_names)))
            query += f" AND status IN ({placeholders})"
            for i, name in enumerate(status_names):
                params[f"status_{i}"] = name

        if criteria and criteria.start_time_after:
            # Handle both started and not-yet-started workflows
            # Workflows that haven't started yet have NULL start_time
            query += " AND (start_time >= :start_time_after OR start_time IS NULL)"
            params["start_time_after"] = criteria.start_time_after

        query += " ORDER BY start_time DESC NULLS LAST"

        if criteria and criteria.page_size:
            query += f" LIMIT {criteria.page_size}"

        result = conn.execute(query, params)
        for row in result.fetchall():
            yield self.retrieve(row["id"])

    def pause(self, execution_id: str, paused_by: str) -> None:
        """Pause an execution."""
        pause_execution(self._get_connection(), execution_id, paused_by)

    def resume(self, execution_id: str) -> None:
        """Resume a paused execution."""
        resume_execution(self._get_connection(), execution_id)

    def cancel(
        self,
        execution_id: str,
        canceled_by: str,
        reason: str,
    ) -> None:
        """Cancel an execution."""
        cancel_execution(self._get_connection(), execution_id, canceled_by, reason)
