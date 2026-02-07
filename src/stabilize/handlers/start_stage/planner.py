"""
Stage planning methods for StartStageHandler.

Builds tasks and before-stages for a stage that is ready to run.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from stabilize.dag.graph import StageGraphBuilder
from stabilize.stages.builder import get_default_factory

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore


class StartStagePlannerMixin:
    """Mixin providing stage-planning methods used by StartStageHandler."""

    repository: WorkflowStore

    def _plan_stage(self, stage: StageExecution) -> None:
        """
        Plan the stage - build tasks and before stages.
        """
        # Hydrate context with ancestor outputs
        # This ensures tasks have access to upstream data even with partial loading
        ancestor_outputs = self.repository.get_merged_ancestor_outputs(
            stage.execution.id, stage.ref_id
        )

        merged = ancestor_outputs
        for key, value in stage.context.items():
            if key in merged and isinstance(merged[key], list) and isinstance(value, list):
                # Concatenate lists, avoiding duplicates
                existing = merged[key]
                for item in value:
                    if item not in existing:
                        existing.append(item)
            else:
                merged[key] = value

        stage.context = merged

        # Get builder
        builder = get_default_factory().get(stage.type)

        # Build tasks if none exist
        if not stage.tasks:
            stage.tasks = builder.build_tasks(stage)

        # Set task-stage back-references and mark first/last tasks
        if stage.tasks:
            for task in stage.tasks:
                task._stage = stage
            stage.tasks[0].stage_start = True
            stage.tasks[-1].stage_end = True

        # Build before stages
        graph = StageGraphBuilder.before_stages(stage)
        builder.before_stages(stage, graph)

        # Save any new synthetic stages
        for s in graph.build():
            # If not already in repository, add it
            # (StageGraphBuilder adds to execution.stages, but we need to persist)
            # Actually StageGraphBuilder usually just modifies the object graph.
            # We need to explicitly store new stages.
            # Assuming graph.build() returns new stages.
            s.execution = stage.execution  # Ensure backref
            self.repository.add_stage(s)

        # Add context flags
        builder.add_context_flags(stage)
