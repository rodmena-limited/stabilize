"""
Structured loop builder for WCP-21.

Provides builder methods for creating structured loop patterns
using the existing jump_to mechanism internally.

Usage:
    from stabilize.stages.loop_builder import LoopBuilder

    # While loop: check condition, then execute body, repeat
    stages = LoopBuilder.while_loop(
        condition="iteration_count < max_iterations",
        body_stages=[stage_a, stage_b],
        loop_ref_prefix="retry_loop",
    )

    # Repeat-until loop: execute body, then check condition
    stages = LoopBuilder.repeat_until(
        condition="tests_passed == True",
        body_stages=[stage_a, stage_b],
        loop_ref_prefix="test_loop",
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.models.stage import StageExecution
from stabilize.models.task import TaskExecution

if TYPE_CHECKING:
    pass


class LoopBuilder:
    """Builder for structured loop patterns using jump_to internally."""

    @staticmethod
    def while_loop(
        condition: str,
        body_stages: list[StageExecution],
        loop_ref_prefix: str = "loop",
        max_iterations: int = 100,
        context: dict[str, Any] | None = None,
    ) -> list[StageExecution]:
        """Create a while-loop pattern: check condition, then execute body.

        Creates stages:
        1. Condition check stage (evaluates condition, jumps back or continues)
        2. Body stages (user-provided)
        3. Loop-back stage (jumps back to condition check)

        Args:
            condition: Expression to evaluate (loop continues while True)
            body_stages: Stages to execute in each iteration
            loop_ref_prefix: Prefix for generated ref_ids
            max_iterations: Safety limit on loop iterations
            context: Initial context for the loop

        Returns:
            List of stages forming the loop structure
        """
        stages: list[StageExecution] = []

        # Condition check stage
        condition_ref = f"{loop_ref_prefix}_condition"
        condition_stage = StageExecution.create(
            type="noop",
            name=f"{loop_ref_prefix} condition check",
            ref_id=condition_ref,
            context={
                **(context or {}),
                "_loop_condition": condition,
                "_loop_max_iterations": max_iterations,
                "_loop_iteration": 0,
                "_loop_type": "while",
            },
        )
        # The condition check task evaluates the expression and either
        # proceeds (to body) or skips (to exit)
        condition_stage.tasks = [
            TaskExecution.create(
                name="Loop Condition Check",
                implementing_class="LoopConditionTask",
                stage_start=True,
                stage_end=True,
            ),
        ]
        stages.append(condition_stage)

        # Body stages - chain them after condition
        prev_ref = condition_ref
        for i, body_stage in enumerate(body_stages):
            body_stage.requisite_stage_ref_ids = {prev_ref}
            prev_ref = body_stage.ref_id
            stages.append(body_stage)

        # Loop-back stage: jumps back to condition check
        loopback_ref = f"{loop_ref_prefix}_loopback"
        loopback_stage = StageExecution.create(
            type="noop",
            name=f"{loop_ref_prefix} loop back",
            ref_id=loopback_ref,
            context={
                "_loop_target_ref_id": condition_ref,
            },
        )
        loopback_stage.requisite_stage_ref_ids = {prev_ref}
        loopback_stage.tasks = [
            TaskExecution.create(
                name="Loop Back",
                implementing_class="LoopBackTask",
                stage_start=True,
                stage_end=True,
            ),
        ]
        stages.append(loopback_stage)

        return stages

    @staticmethod
    def repeat_until(
        condition: str,
        body_stages: list[StageExecution],
        loop_ref_prefix: str = "loop",
        max_iterations: int = 100,
        context: dict[str, Any] | None = None,
    ) -> list[StageExecution]:
        """Create a repeat-until pattern: execute body, then check condition.

        Creates stages:
        1. Body stages (user-provided)
        2. Condition check stage (evaluates condition, loops back or exits)

        Args:
            condition: Expression to evaluate (loop exits when True)
            body_stages: Stages to execute in each iteration
            loop_ref_prefix: Prefix for generated ref_ids
            max_iterations: Safety limit on loop iterations
            context: Initial context for the loop

        Returns:
            List of stages forming the loop structure
        """
        stages: list[StageExecution] = []

        # Entry marker (for jump-back target)
        entry_ref = f"{loop_ref_prefix}_entry"
        entry_stage = StageExecution.create(
            type="noop",
            name=f"{loop_ref_prefix} entry",
            ref_id=entry_ref,
            context={
                **(context or {}),
                "_loop_iteration": 0,
                "_loop_type": "repeat_until",
            },
        )
        stages.append(entry_stage)

        # Body stages
        prev_ref = entry_ref
        for body_stage in body_stages:
            body_stage.requisite_stage_ref_ids = {prev_ref}
            prev_ref = body_stage.ref_id
            stages.append(body_stage)

        # Condition check / loop-back stage
        condition_ref = f"{loop_ref_prefix}_condition"
        condition_stage = StageExecution.create(
            type="noop",
            name=f"{loop_ref_prefix} condition check",
            ref_id=condition_ref,
            context={
                "_loop_condition": condition,
                "_loop_max_iterations": max_iterations,
                "_loop_target_ref_id": entry_ref,
                "_loop_type": "repeat_until",
            },
        )
        condition_stage.requisite_stage_ref_ids = {prev_ref}
        condition_stage.tasks = [
            TaskExecution.create(
                name="Loop Condition Check",
                implementing_class="LoopConditionTask",
                stage_start=True,
                stage_end=True,
            ),
        ]
        stages.append(condition_stage)

        return stages
