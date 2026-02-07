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

    def _plan_after_stages(self, stage: StageExecution) -> None:
        """Plan after stages using the stage definition builder."""
        builder = get_default_factory().get(stage.type)
        graph = StageGraphBuilder.after_stages(stage)
        builder.after_stages(stage, graph)

        for s in graph.build():
            s.execution = stage.execution
            stage.execution.stages.append(s)  # Add to in-memory list for first_after_stages()
            self.repository.add_stage(s)

    def _plan_on_failure_stages(self, stage: StageExecution) -> bool:
        """
        Plan on-failure stages using the stage definition builder.

        Returns:
            True if on-failure stages were added
        """
        builder = get_default_factory().get(stage.type)
        graph = StageGraphBuilder.after_stages(stage)
        builder.on_failure_stages(stage, graph)

        new_stages = graph.build()
        if not new_stages:
            return False

        for s in new_stages:
            s.execution = stage.execution
            stage.execution.stages.append(s)  # Add to in-memory list for first_after_stages()
            self.repository.add_stage(s)

        return True
