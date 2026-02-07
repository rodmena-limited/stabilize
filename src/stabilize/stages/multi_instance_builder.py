"""
Multi-instance stage builder.

Provides builder methods for creating multi-instance stage patterns:
- WCP-12: Multiple Instances without Synchronization
- WCP-13: Multiple Instances with a priori Design-Time Knowledge
- WCP-14: Multiple Instances with a priori Run-Time Knowledge
- WCP-15: Multiple Instances without a priori Run-Time Knowledge

Usage:
    from stabilize.stages.multi_instance_builder import MultiInstanceBuilder

    # WCP-13: 6 reviewers, wait for all
    stages = MultiInstanceBuilder.create_fixed(
        parent_stage=stage,
        count=6,
        instance_type="review",
        instance_name_prefix="Review",
    )

    # WCP-14: Count from context
    stages = MultiInstanceBuilder.create_from_context(
        parent_stage=stage,
        count_key="num_reviewers",
        instance_type="review",
        instance_name_prefix="Review",
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.models.multi_instance import MultiInstanceConfig
from stabilize.models.stage import JoinType, StageExecution

if TYPE_CHECKING:
    pass


class MultiInstanceBuilder:
    """Builder for multi-instance stage patterns."""

    @staticmethod
    def create_fixed(
        parent_stage: StageExecution,
        count: int,
        instance_type: str = "",
        instance_name_prefix: str = "Instance",
        sync_on_complete: bool = True,
        join_threshold: int = 0,
        cancel_remaining: bool = False,
        instance_contexts: list[dict[str, Any]] | None = None,
    ) -> list[StageExecution]:
        """Create N fixed instances with optional synchronization (WCP-12/13).

        Args:
            parent_stage: The MI parent stage
            count: Number of instances to create
            instance_type: Type for instance stages (defaults to parent type)
            instance_name_prefix: Prefix for instance stage names
            sync_on_complete: Whether to synchronize (WCP-12=False, WCP-13=True)
            join_threshold: N-of-M threshold (0 = all must complete)
            cancel_remaining: Cancel remaining after threshold reached
            instance_contexts: Per-instance context overrides

        Returns:
            List of created stages (instances + optional join stage)
        """
        if not instance_type:
            instance_type = parent_stage.type

        stages: list[StageExecution] = []

        # Set MI config on parent
        parent_stage.mi_config = MultiInstanceConfig(
            count=count,
            sync_on_complete=sync_on_complete,
            join_threshold=join_threshold,
            cancel_remaining=cancel_remaining,
        )

        # Create instance stages
        instance_ref_ids: list[str] = []
        for i in range(count):
            instance_ref_id = f"{parent_stage.ref_id}_instance_{i}"
            instance_ref_ids.append(instance_ref_id)

            ctx: dict[str, Any] = {
                "_mi_parent_ref_id": parent_stage.ref_id,
                "_mi_instance_index": i,
            }
            if instance_contexts and i < len(instance_contexts):
                ctx.update(instance_contexts[i])

            instance = StageExecution.create(
                type=instance_type,
                name=f"{instance_name_prefix} [{i}]",
                ref_id=instance_ref_id,
                context=ctx,
                requisite_stage_ref_ids={parent_stage.ref_id},
            )
            stages.append(instance)

        # Create join stage if sync_on_complete
        if sync_on_complete and instance_ref_ids:
            join_ref_id = f"{parent_stage.ref_id}_mi_join"

            # Determine join type
            if join_threshold > 0:
                join_type = JoinType.N_OF_M
            else:
                join_type = JoinType.AND

            join_stage = StageExecution.create(
                type="noop",
                name=f"{parent_stage.name} [MI Join]",
                ref_id=join_ref_id,
                requisite_stage_ref_ids=set(instance_ref_ids),
            )
            join_stage.join_type = join_type
            join_stage.join_threshold = join_threshold
            stages.append(join_stage)

        return stages

    @staticmethod
    def create_from_context(
        parent_stage: StageExecution,
        count_key: str,
        instance_type: str = "",
        instance_name_prefix: str = "Instance",
        sync_on_complete: bool = True,
        join_threshold: int = 0,
        cancel_remaining: bool = False,
    ) -> list[StageExecution]:
        """Create instances with count from context (WCP-14).

        The count is read from parent_stage.context[count_key] at build time.

        Args:
            parent_stage: The MI parent stage
            count_key: Context key holding the instance count
            instance_type: Type for instance stages
            instance_name_prefix: Prefix for instance stage names
            sync_on_complete: Whether to synchronize
            join_threshold: N-of-M threshold
            cancel_remaining: Cancel remaining after threshold

        Returns:
            List of created stages
        """
        count = int(parent_stage.context.get(count_key, 0))
        if count <= 0:
            return []

        # Set MI config with context reference
        parent_stage.mi_config = MultiInstanceConfig(
            count=count,
            count_from_context=count_key,
            sync_on_complete=sync_on_complete,
            join_threshold=join_threshold,
            cancel_remaining=cancel_remaining,
        )

        return MultiInstanceBuilder.create_fixed(
            parent_stage=parent_stage,
            count=count,
            instance_type=instance_type,
            instance_name_prefix=instance_name_prefix,
            sync_on_complete=sync_on_complete,
            join_threshold=join_threshold,
            cancel_remaining=cancel_remaining,
        )

    @staticmethod
    def create_from_collection(
        parent_stage: StageExecution,
        collection_key: str,
        instance_type: str = "",
        instance_name_prefix: str = "Instance",
        sync_on_complete: bool = True,
        item_context_key: str = "item",
    ) -> list[StageExecution]:
        """Create one instance per item in a context collection (WCP-14 variant).

        Args:
            parent_stage: The MI parent stage
            collection_key: Context key holding a list of items
            instance_type: Type for instance stages
            instance_name_prefix: Prefix for instance stage names
            sync_on_complete: Whether to synchronize
            item_context_key: Context key to set the item on each instance

        Returns:
            List of created stages
        """
        collection = parent_stage.context.get(collection_key, [])
        if not isinstance(collection, list) or not collection:
            return []

        instance_contexts = [{item_context_key: item} for item in collection]

        parent_stage.mi_config = MultiInstanceConfig(
            count=len(collection),
            collection_from_context=collection_key,
            sync_on_complete=sync_on_complete,
        )

        return MultiInstanceBuilder.create_fixed(
            parent_stage=parent_stage,
            count=len(collection),
            instance_type=instance_type,
            instance_name_prefix=instance_name_prefix,
            sync_on_complete=sync_on_complete,
            instance_contexts=instance_contexts,
        )

    @staticmethod
    def create_dynamic(
        parent_stage: StageExecution,
        instance_type: str = "",
        instance_name_prefix: str = "Instance",
        initial_count: int = 0,
    ) -> list[StageExecution]:
        """Create a dynamic MI setup (WCP-15).

        Instances can be added during execution via AddMultiInstance messages.

        Args:
            parent_stage: The MI parent stage
            instance_type: Type for instance stages
            instance_name_prefix: Prefix for instance stage names
            initial_count: Optional initial instances to create

        Returns:
            List of initially created stages (more can be added later)
        """
        parent_stage.mi_config = MultiInstanceConfig(
            count=initial_count,
            allow_dynamic=True,
            sync_on_complete=True,
        )
        parent_stage.context["_mi_instance_count"] = initial_count

        if initial_count > 0:
            return MultiInstanceBuilder.create_fixed(
                parent_stage=parent_stage,
                count=initial_count,
                instance_type=instance_type,
                instance_name_prefix=instance_name_prefix,
                sync_on_complete=True,
            )

        return []
