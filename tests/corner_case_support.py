from __future__ import annotations

import time

from stabilize import (
    StageExecution,
    Task,
    TaskResult,
)
from stabilize.errors import PermanentError, TransientError


# =============================================================================
# Test Task Implementations for Corner Cases
# =============================================================================


class SlowTask(Task):
    """A task that takes time to execute."""

    def execute(self, stage: StageExecution) -> TaskResult:
        sleep_time = stage.context.get("sleep_time", 0.1)
        time.sleep(sleep_time)
        return TaskResult.success(outputs={"completed": True})


class ConditionalFailTask(Task):
    """A task that fails based on context."""

    def execute(self, stage: StageExecution) -> TaskResult:
        if stage.context.get("should_fail", False):
            return TaskResult.terminal("Conditional failure triggered")
        return TaskResult.success(outputs={"passed": True})


class JumpingTask(Task):
    """A task that performs a jump based on context."""

    def execute(self, stage: StageExecution) -> TaskResult:
        target = stage.context.get("jump_target")
        if target:
            return TaskResult.jump_to(
                target_stage_ref_id=target,
                context={"jumped_from": stage.ref_id},
                outputs={"jump_occurred": True},
            )
        return TaskResult.success(outputs={"no_jump": True})


class TransientFailTask(Task):
    """A task that fails transiently a configurable number of times."""

    failure_counts: dict[str, int] = {}

    def execute(self, stage: StageExecution) -> TaskResult:
        key = f"{stage.execution.id}:{stage.id}"
        current_count = self.failure_counts.get(key, 0)
        max_failures = stage.context.get("max_transient_failures", 2)

        if current_count < max_failures:
            self.failure_counts[key] = current_count + 1
            raise TransientError(
                f"Transient failure {current_count + 1}/{max_failures}",
                retry_after=0.01,
                context_update={"retry_count": current_count + 1},
            )
        return TaskResult.success(outputs={"succeeded_after_retries": current_count})


class OutputMergingTask(Task):
    """A task that tests output merging behavior."""

    def execute(self, stage: StageExecution) -> TaskResult:
        # Return various types of outputs to test merging
        return TaskResult.success(
            outputs={
                "list_output": stage.context.get("list_items", ["default"]),
                "scalar_output": stage.context.get("scalar_value", "value"),
                "nested_output": {
                    "inner": stage.context.get("nested_value", "nested"),
                },
            }
        )


class RunningTask(Task):
    """A task that returns RUNNING status multiple times."""

    run_counts: dict[str, int] = {}

    def execute(self, stage: StageExecution) -> TaskResult:
        key = f"{stage.execution.id}:{stage.id}"
        current_count = self.run_counts.get(key, 0)
        required_runs = stage.context.get("required_runs", 3)

        self.run_counts[key] = current_count + 1

        if current_count < required_runs - 1:
            return TaskResult.running(context={"run_count": current_count + 1})
        return TaskResult.success(outputs={"total_runs": current_count + 1})


class PermanentFailTask(Task):
    """A task that always fails permanently."""

    def execute(self, stage: StageExecution) -> TaskResult:
        raise PermanentError("This task always fails permanently")
