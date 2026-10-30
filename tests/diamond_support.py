from __future__ import annotations

import random
import threading
import time
from typing import Any

from stabilize import (
    StageExecution,
    Task,
    TaskResult,
)
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow

# =============================================================================
# Test Task Implementations
# =============================================================================


class OutputTask(Task):
    """Task that produces configurable outputs."""

    def execute(self, stage: StageExecution) -> TaskResult:
        outputs = stage.context.get("outputs_to_produce", {})
        return TaskResult.success(outputs=outputs)


class SlowOutputTask(Task):
    """Task that waits before producing outputs - for race condition testing."""

    def execute(self, stage: StageExecution) -> TaskResult:
        delay = stage.context.get("delay", 0.1)
        time.sleep(delay)
        outputs = stage.context.get("outputs_to_produce", {})
        return TaskResult.success(outputs=outputs)


class CountingOutputTask(Task):
    """Thread-safe task that counts executions and produces outputs."""

    _lock = threading.Lock()
    execution_count: int = 0
    execution_log: list[tuple[str, str, float]] = []  # (workflow_id, stage_ref, timestamp)

    @classmethod
    def reset(cls) -> None:
        with cls._lock:
            cls.execution_count = 0
            cls.execution_log = []

    def execute(self, stage: StageExecution) -> TaskResult:
        with CountingOutputTask._lock:
            CountingOutputTask.execution_count += 1
            CountingOutputTask.execution_log.append((stage.execution.id, stage.ref_id, time.time()))
        outputs = stage.context.get("outputs_to_produce", {})
        return TaskResult.success(outputs=outputs)


class VerifyInputsTask(Task):
    """Task that verifies expected inputs are present in context."""

    verified_contexts: dict[str, dict[str, Any]] = {}
    verification_errors: list[str] = []
    _lock = threading.Lock()

    @classmethod
    def reset(cls) -> None:
        with cls._lock:
            cls.verified_contexts = {}
            cls.verification_errors = []

    def execute(self, stage: StageExecution) -> TaskResult:
        expected_keys = stage.context.get("expected_keys", [])
        stage_key = f"{stage.execution.id}:{stage.ref_id}"

        with VerifyInputsTask._lock:
            VerifyInputsTask.verified_contexts[stage_key] = dict(stage.context)

            missing = []
            for key in expected_keys:
                if key not in stage.context:
                    missing.append(key)

            if missing:
                error = f"Stage {stage.ref_id} missing expected keys: {missing}"
                VerifyInputsTask.verification_errors.append(error)
                return TaskResult.terminal(error)

        return TaskResult.success(outputs={"verified": True, "received_keys": list(stage.context.keys())})


class ConditionalFailTask(Task):
    """Task that fails if context has 'should_fail' set."""

    def execute(self, stage: StageExecution) -> TaskResult:
        if stage.context.get("should_fail", False):
            return TaskResult.terminal("Intentional failure")
        outputs = stage.context.get("outputs_to_produce", {})
        return TaskResult.success(outputs=outputs)


class RandomDelayTask(Task):
    """Task with random delay to create non-deterministic timing."""

    def execute(self, stage: StageExecution) -> TaskResult:
        min_delay = stage.context.get("min_delay", 0.01)
        max_delay = stage.context.get("max_delay", 0.05)
        time.sleep(random.uniform(min_delay, max_delay))
        outputs = stage.context.get("outputs_to_produce", {})
        return TaskResult.success(outputs=outputs)


# =============================================================================
# Helper Functions
# =============================================================================


def create_diamond_workflow(
    name: str,
    stage_a_outputs: dict[str, Any] | None = None,
    stage_b_outputs: dict[str, Any] | None = None,
    stage_c_outputs: dict[str, Any] | None = None,
    stage_d_expected_keys: list[str] | None = None,
    task_class: str = "output",
    extra_context: dict[str, Any] | None = None,
) -> Workflow:
    """
    Create a basic diamond workflow:

        A
       / \\
      B   C
       \\ /
        D
    """
    stage_a_ctx = {"outputs_to_produce": stage_a_outputs or {"from_a": "value_a"}}
    stage_b_ctx = {"outputs_to_produce": stage_b_outputs or {"from_b": "value_b"}}
    stage_c_ctx = {"outputs_to_produce": stage_c_outputs or {"from_c": "value_c"}}
    stage_d_ctx = {"expected_keys": stage_d_expected_keys or ["from_a", "from_b", "from_c"]}

    if extra_context:
        for ctx in [stage_a_ctx, stage_b_ctx, stage_c_ctx, stage_d_ctx]:
            ctx.update(extra_context)

    return Workflow.create(
        application="diamond-test",
        name=name,
        stages=[
            StageExecution(
                ref_id="stage_a",
                name="Stage A (Root)",
                context=stage_a_ctx,
                tasks=[
                    TaskExecution.create(
                        name="Task A",
                        implementing_class=task_class,
                        stage_start=True,
                        stage_end=True,
                    )
                ],
            ),
            StageExecution(
                ref_id="stage_b",
                name="Stage B (Left Branch)",
                requisite_stage_ref_ids={"stage_a"},
                context=stage_b_ctx,
                tasks=[
                    TaskExecution.create(
                        name="Task B",
                        implementing_class=task_class,
                        stage_start=True,
                        stage_end=True,
                    )
                ],
            ),
            StageExecution(
                ref_id="stage_c",
                name="Stage C (Right Branch)",
                requisite_stage_ref_ids={"stage_a"},
                context=stage_c_ctx,
                tasks=[
                    TaskExecution.create(
                        name="Task C",
                        implementing_class=task_class,
                        stage_start=True,
                        stage_end=True,
                    )
                ],
            ),
            StageExecution(
                ref_id="stage_d",
                name="Stage D (Join)",
                requisite_stage_ref_ids={"stage_b", "stage_c"},
                context=stage_d_ctx,
                tasks=[
                    TaskExecution.create(
                        name="Task D",
                        implementing_class="verify_inputs",
                        stage_start=True,
                        stage_end=True,
                    )
                ],
            ),
        ],
    )


def create_deep_diamond_workflow(name: str) -> Workflow:
    """
    Create a deep diamond with multiple join points:

        A
       / \\
      B   C
       \\ /
        D
       / \\
      E   F
       \\ /
        G
    """
    return Workflow.create(
        application="diamond-test",
        name=name,
        stages=[
            StageExecution(
                ref_id="stage_a",
                name="Stage A",
                context={"outputs_to_produce": {"depth": 0, "from_a": True}},
                tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="stage_b",
                name="Stage B",
                requisite_stage_ref_ids={"stage_a"},
                context={"outputs_to_produce": {"from_b": True, "branch": "left1"}},
                tasks=[TaskExecution.create("Task B", "output", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="stage_c",
                name="Stage C",
                requisite_stage_ref_ids={"stage_a"},
                context={"outputs_to_produce": {"from_c": True, "branch": "right1"}},
                tasks=[TaskExecution.create("Task C", "output", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="stage_d",
                name="Stage D (Join 1)",
                requisite_stage_ref_ids={"stage_b", "stage_c"},
                context={"outputs_to_produce": {"from_d": True, "depth": 1}},
                tasks=[TaskExecution.create("Task D", "output", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="stage_e",
                name="Stage E",
                requisite_stage_ref_ids={"stage_d"},
                context={"outputs_to_produce": {"from_e": True, "branch": "left2"}},
                tasks=[TaskExecution.create("Task E", "output", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="stage_f",
                name="Stage F",
                requisite_stage_ref_ids={"stage_d"},
                context={"outputs_to_produce": {"from_f": True, "branch": "right2"}},
                tasks=[TaskExecution.create("Task F", "output", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="stage_g",
                name="Stage G (Join 2)",
                requisite_stage_ref_ids={"stage_e", "stage_f"},
                context={"expected_keys": ["from_a", "from_b", "from_c", "from_d", "from_e", "from_f"]},
                tasks=[TaskExecution.create("Task G", "verify_inputs", stage_start=True, stage_end=True)],
            ),
        ],
    )


def create_wide_diamond_workflow(name: str, width: int = 5) -> Workflow:
    """
    Create a wide diamond with many parallel branches:

              A
         / | | | \\
        B  C D E  F
         \\ | | | /
              G
    """
    stages = [
        StageExecution(
            ref_id="stage_a",
            name="Stage A (Root)",
            context={"outputs_to_produce": {"from_a": True}},
            tasks=[TaskExecution.create("Task A", "random_delay", stage_start=True, stage_end=True)],
        )
    ]

    branch_refs = []
    expected_keys = ["from_a"]

    for i in range(width):
        ref_id = f"stage_branch_{i}"
        branch_refs.append(ref_id)
        key = f"from_branch_{i}"
        expected_keys.append(key)

        stages.append(
            StageExecution(
                ref_id=ref_id,
                name=f"Branch {i}",
                requisite_stage_ref_ids={"stage_a"},
                context={
                    "outputs_to_produce": {key: True, "branch_index": i},
                    "min_delay": 0.01,
                    "max_delay": 0.1,
                },
                tasks=[TaskExecution.create(f"Task Branch {i}", "random_delay", stage_start=True, stage_end=True)],
            )
        )

    stages.append(
        StageExecution(
            ref_id="stage_join",
            name="Stage Join",
            requisite_stage_ref_ids=set(branch_refs),
            context={"expected_keys": expected_keys},
            tasks=[TaskExecution.create("Task Join", "verify_inputs", stage_start=True, stage_end=True)],
        )
    )

    return Workflow.create(application="diamond-test", name=name, stages=stages)
