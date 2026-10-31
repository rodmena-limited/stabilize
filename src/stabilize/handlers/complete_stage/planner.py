"""Planner mixin for CompleteStageHandler."""

from __future__ import annotations

from typing import TYPE_CHECKING

from stabilize.dag.graph import StageGraphBuilder
from stabilize.stages.builder import get_default_factory

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore


class CompleteStagePlannerMixin:
    """Mixin providing after-stage and on-failure-stage planning."""

    if TYPE_CHECKING:
        repository: WorkflowStore

    def _plan_after_stages(self, stage: StageExecution) -> list[StageExecution]:
        """Plan after stages; the caller stores the returned stages in its transaction."""
        builder = get_default_factory().get(stage.type)
        graph = StageGraphBuilder.after_stages(stage)
        builder.after_stages(stage, graph)

        planned = list(graph.build())
        for s in planned:
            s.execution = stage.execution
            stage.execution.stages.append(s)
        return planned

    def _plan_on_failure_stages(self, stage: StageExecution) -> list[StageExecution]:
        """Plan on-failure stages; the caller stores the returned stages in its transaction."""
        builder = get_default_factory().get(stage.type)
        graph = StageGraphBuilder.after_stages(stage)
        builder.on_failure_stages(stage, graph)

        planned = list(graph.build())
        for s in planned:
            s.execution = stage.execution
            s.context["_on_failure_child"] = True
            stage.execution.stages.append(s)
        return planned
