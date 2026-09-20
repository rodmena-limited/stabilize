"""
Stage planning methods for StartStageHandler.

Builds tasks and before-stages for a stage that is ready to run.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from stabilize.dag.graph import StageGraphBuilder
from stabilize.stages.builder import (
    get_default_factory,
    report_empty_unregistered_stage,
)

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore

HYDRATED_KEYS = "_hydrated_keys"


class StartStagePlannerMixin:
    """Mixin providing stage-planning methods used by StartStageHandler."""

    repository: WorkflowStore

    def _plan_stage(self, stage: StageExecution) -> None:
        """
        Plan the stage - build tasks and before stages.
        """
        # Hydrate context with ancestor outputs
        # This ensures tasks have access to upstream data even with partial loading
        ancestor_outputs = self.repository.get_merged_ancestor_outputs(stage.execution.id, stage.ref_id)

        # Declarative fan-in reducers: for keys with a configured reducer,
        # combine the per-branch upstream outputs (collect/sum/merge/...) so
        # parallel branches stop clobbering scalar keys at the join. Only the
        # reducer-named keys are affected; everything else keeps the existing
        # last-write-wins merge below.
        reducers = stage.output_reducers or stage.context.get("_output_reducers") or {}
        if reducers:
            from stabilize.reducers import apply_output_reducers

            upstreams = self.repository.get_upstream_stages(stage.execution.id, stage.ref_id) or []
            branch_outputs = [u.outputs for u in upstreams if u is not None and u.outputs]
            ancestor_outputs.update(apply_output_reducers(reducers, branch_outputs))

        ancestor_keys = set(ancestor_outputs)
        previously_hydrated = set(stage.context.get(HYDRATED_KEYS) or ())

        merged = ancestor_outputs
        for key, value in stage.context.items():
            if key == HYDRATED_KEYS:
                continue
            if key in reducers:
                # A reducer produced the authoritative value for this key;
                # do not let the join stage's own context override it.
                continue
            if key in previously_hydrated and key in ancestor_keys:
                # This value was copied from an ancestor on an earlier plan of
                # this stage. On a re-entry (jump, restart, loop-back) the
                # ancestor is authoritative; the copy is stale.
                continue
            if key in merged and isinstance(merged[key], list) and isinstance(value, list):
                # Concatenate lists, avoiding duplicates
                existing = merged[key]
                for item in value:
                    if item not in existing:
                        existing.append(item)
            else:
                merged[key] = value

        if ancestor_keys:
            merged[HYDRATED_KEYS] = sorted(ancestor_keys)

        stage.context = merged

        # Get builder
        builder = get_default_factory().get(stage.type)

        # Build tasks if none exist
        if not stage.tasks:
            stage.tasks = builder.build_tasks(stage)
            if not stage.tasks and not get_default_factory().has(stage.type):
                report_empty_unregistered_stage(stage)

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
