"""Default (no capability) behaviour of WorkflowStore's query methods added in 0.34; backends override them."""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

from stabilize.persistence.store.criteria import WORKFLOW_CRITERIA_FIELDS

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow
    from stabilize.persistence.store.criteria import WorkflowCriteria
    from stabilize.persistence.store.stage_criteria import StageCriteria


class QueryDefaults:
    """Stage and workflow queries a store may not offer; the defaults say so instead of answering wrongly."""

    def supported_workflow_criteria(self) -> frozenset[str]:
        """The WorkflowCriteria fields this store honours; the 0.33 set unless the store says otherwise."""
        return WORKFLOW_CRITERIA_FIELDS - {"pipeline_config_version"}

    def supported_criteria(self) -> frozenset[str]:
        """The StageCriteria fields this store honours; empty for a store without stage queries."""
        return frozenset()

    def retrieve_stages(self, criteria: StageCriteria) -> list[StageExecution]:
        """The stages of this tenant matching *criteria*, newest id first, at most one page."""
        raise NotImplementedError(f"{type(self).__name__} does not query stages")

    def count_stages(self, criteria: StageCriteria) -> int:
        """How many stages of this tenant match *criteria*."""
        raise NotImplementedError(f"{type(self).__name__} does not query stages")

    def get_all_pending_workflows(self, criteria: WorkflowCriteria | None = None) -> Iterator[Workflow]:
        """Every workflow of this tenant matching *criteria*, whatever its application."""
        raise NotImplementedError(f"{type(self).__name__} does not list workflows without an application")

    def count_active_workflows(self) -> int:
        """How many of this tenant's workflows are RUNNING."""
        raise NotImplementedError(f"{type(self).__name__} does not count active workflows")
