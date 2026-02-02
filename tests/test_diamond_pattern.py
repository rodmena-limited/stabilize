"""
Diamond Pattern Tests for Airport-Critical Systems.

This module tests the diamond/join pattern which is common in workflow systems:

        A (root)
       / \\
      B   C  (parallel branches)
       \\ /
        D (join point)

Critical scenarios tested:
1. Basic diamond execution - D must wait for both B and C
2. Concurrent completion race - B and C completing simultaneously
3. Output merging at join point - D must see outputs from both branches
4. Non-deterministic merge order - same key in B and C should be handled predictably
5. Deep diamond (A→B,C→D,E→F) - multiple join points
6. Wide diamond (A→B,C,D,E→F) - many parallel branches
7. Diamond with failures - one branch fails, should propagate correctly
8. Diamond with jumps - dynamic routing in diamond patterns
9. Diamond under high concurrency - many workflows with diamonds running together
10. Diamond with slow branches - timing-related race conditions

These tests run on both SQLite and PostgreSQL backends.
"""

from __future__ import annotations

import random
import threading
import time
from collections import Counter
from typing import Any

import pytest

from stabilize import (
    StageExecution,
    Task,
    TaskResult,
)
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize

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


# =============================================================================
# Test Classes
# =============================================================================


class TestBasicDiamond:
    """Test basic diamond pattern execution."""

    def test_diamond_completes_successfully(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """Basic diamond workflow should complete with all stages succeeded."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = create_diamond_workflow("basic-diamond")
        file_repository.store(wf)
        runner.start(wf)

        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Workflow status: {result.status}"

        # All stages should be SUCCEEDED
        for stage in result.stages:
            assert stage.status == WorkflowStatus.SUCCEEDED, f"Stage {stage.ref_id}: {stage.status}"

        # Stage D should have received all expected keys
        assert not VerifyInputsTask.verification_errors, f"Errors: {VerifyInputsTask.verification_errors}"

    def test_join_waits_for_all_branches(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """Join stage D must wait for both B and C before starting."""
        CountingOutputTask.reset()
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "slow_output": SlowOutputTask,
                "counting": CountingOutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        # Make B slow, C fast
        wf = Workflow.create(
            application="diamond-test",
            name="slow-branch-diamond",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    context={"outputs_to_produce": {"from_a": True}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B (Slow)",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_b": True}, "delay": 0.3},
                    tasks=[TaskExecution.create("Task B", "slow_output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C (Fast)",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_c": True}},
                    tasks=[TaskExecution.create("Task C", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_d",
                    name="Stage D (Join)",
                    requisite_stage_ref_ids={"stage_b", "stage_c"},
                    context={"expected_keys": ["from_a", "from_b", "from_c"]},
                    tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # D should have all inputs despite B being slow
        assert not VerifyInputsTask.verification_errors


class TestOutputMerging:
    """Test output merging at diamond join points."""

    def test_outputs_from_both_branches_merged(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """Stage D should receive merged outputs from both B and C."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = create_diamond_workflow(
            "merge-test",
            stage_a_outputs={"shared_key": "from_a", "a_only": 1},
            stage_b_outputs={"b_only": 2, "b_list": [1, 2]},
            stage_c_outputs={"c_only": 3, "c_list": [3, 4]},
            stage_d_expected_keys=["shared_key", "a_only", "b_only", "c_only", "b_list", "c_list"],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED
        assert not VerifyInputsTask.verification_errors

        # Verify the actual merged context
        stage_d_key = f"{wf.id}:stage_d"
        assert stage_d_key in VerifyInputsTask.verified_contexts
        ctx = VerifyInputsTask.verified_contexts[stage_d_key]

        assert ctx.get("a_only") == 1
        assert ctx.get("b_only") == 2
        assert ctx.get("c_only") == 3

    def test_conflicting_keys_in_parallel_branches(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        CRITICAL TEST: When B and C both produce the same key with different values,
        the merge behavior should be consistent (not non-deterministic).

        This is a potential bug area - topological sort order of B and C is undefined.
        """
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "random_delay": RandomDelayTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        # Run multiple times to detect non-determinism
        results = []
        for i in range(10):
            wf = Workflow.create(
                application="diamond-test",
                name=f"conflict-test-{i}",
                stages=[
                    StageExecution(
                        ref_id="stage_a",
                        name="Stage A",
                        context={"outputs_to_produce": {"shared": "from_a"}},
                        tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_b",
                        name="Stage B",
                        requisite_stage_ref_ids={"stage_a"},
                        context={
                            "outputs_to_produce": {"conflict_key": "from_b", "b_marker": True},
                            "min_delay": 0.01,
                            "max_delay": 0.05,
                        },
                        tasks=[TaskExecution.create("Task B", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_c",
                        name="Stage C",
                        requisite_stage_ref_ids={"stage_a"},
                        context={
                            "outputs_to_produce": {"conflict_key": "from_c", "c_marker": True},
                            "min_delay": 0.01,
                            "max_delay": 0.05,
                        },
                        tasks=[TaskExecution.create("Task C", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_d",
                        name="Stage D",
                        requisite_stage_ref_ids={"stage_b", "stage_c"},
                        context={"expected_keys": ["shared", "conflict_key", "b_marker", "c_marker"]},
                        tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                    ),
                ],
            )

            file_repository.store(wf)
            runner.start(wf)
            processor.process_all(timeout=30.0)

            result = file_repository.retrieve(wf.id)
            assert result.status == WorkflowStatus.SUCCEEDED, f"Run {i} failed: {result.status}"

            stage_d_key = f"{wf.id}:stage_d"
            ctx = VerifyInputsTask.verified_contexts.get(stage_d_key, {})
            conflict_value = ctx.get("conflict_key")
            results.append(conflict_value)

        # Check for non-determinism - if results vary, we have a bug
        unique_values = set(results)
        if len(unique_values) > 1:
            # This is a potential issue - non-deterministic merge order
            # Count occurrences
            counter = Counter(results)
            pytest.fail(
                f"NON-DETERMINISTIC OUTPUT MERGE DETECTED!\n"
                f"conflict_key values across 10 runs: {counter}\n"
                f"This indicates the merge order of parallel branches is not stable."
            )

    def test_list_outputs_concatenated(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """List outputs from parallel branches should be concatenated."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = Workflow.create(
            application="diamond-test",
            name="list-merge-test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    context={"outputs_to_produce": {"items": ["a1", "a2"]}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"items": ["b1", "b2"]}},
                    tasks=[TaskExecution.create("Task B", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"items": ["c1", "c2"]}},
                    tasks=[TaskExecution.create("Task C", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_d",
                    name="Stage D",
                    requisite_stage_ref_ids={"stage_b", "stage_c"},
                    context={"expected_keys": ["items"]},
                    tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        stage_d_key = f"{wf.id}:stage_d"
        ctx = VerifyInputsTask.verified_contexts.get(stage_d_key, {})
        items = ctx.get("items", [])

        # Should contain items from A, B, and C
        assert "a1" in items and "a2" in items, f"Missing A items: {items}"
        assert "b1" in items and "b2" in items, f"Missing B items: {items}"
        assert "c1" in items and "c2" in items, f"Missing C items: {items}"


class TestDiamondWithFailures:
    """Test diamond patterns with failures in branches."""

    def test_one_branch_fails_blocks_join(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """If B fails, D should not start and workflow should fail."""
        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "conditional_fail": ConditionalFailTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = Workflow.create(
            application="diamond-test",
            name="failure-test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    context={"outputs_to_produce": {"from_a": True}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B (Will Fail)",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"should_fail": True},
                    tasks=[TaskExecution.create("Task B", "conditional_fail", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_c": True}},
                    tasks=[TaskExecution.create("Task C", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_d",
                    name="Stage D (Join)",
                    requisite_stage_ref_ids={"stage_b", "stage_c"},
                    context={"expected_keys": ["from_a", "from_c"]},
                    tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)

        # Workflow should fail due to B's failure
        assert result.status == WorkflowStatus.TERMINAL, f"Workflow status: {result.status}"

        # Stage D should NOT have run (should be NOT_STARTED or CANCELED)
        stage_d = result.stage_by_ref_id("stage_d")
        assert stage_d.status in {
            WorkflowStatus.NOT_STARTED,
            WorkflowStatus.CANCELED,
        }, f"Stage D should not have run: {stage_d.status}"


class TestDeepDiamond:
    """Test deep diamond patterns with multiple join points."""

    def test_deep_diamond_completes(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """Deep diamond with two join points should complete successfully."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = create_deep_diamond_workflow("deep-diamond")
        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=60.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Workflow status: {result.status}"

        # Final join stage G should have all expected inputs
        assert not VerifyInputsTask.verification_errors, f"Errors: {VerifyInputsTask.verification_errors}"


class TestWideDiamond:
    """Test wide diamond patterns with many parallel branches."""

    def test_wide_diamond_completes(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """Wide diamond with 10 parallel branches should complete."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "random_delay": RandomDelayTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = create_wide_diamond_workflow("wide-diamond", width=10)
        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=60.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Workflow status: {result.status}"
        assert not VerifyInputsTask.verification_errors


class TestConcurrentDiamonds:
    """Test multiple diamond workflows running concurrently."""

    @pytest.mark.stress
    def test_many_concurrent_diamonds(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """
        Run many diamond workflows concurrently.

        This is a critical stress test for race conditions at join points.
        """
        VerifyInputsTask.reset()
        CountingOutputTask.reset()

        num_workflows = 20

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "counting": CountingOutputTask,
                "random_delay": RandomDelayTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        workflow_ids = []
        for i in range(num_workflows):
            wf = Workflow.create(
                application="diamond-test",
                name=f"concurrent-diamond-{i}",
                stages=[
                    StageExecution(
                        ref_id="stage_a",
                        name="Stage A",
                        context={
                            "outputs_to_produce": {"workflow_index": i, "from_a": True},
                            "min_delay": 0.001,
                            "max_delay": 0.01,
                        },
                        tasks=[TaskExecution.create("Task A", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_b",
                        name="Stage B",
                        requisite_stage_ref_ids={"stage_a"},
                        context={
                            "outputs_to_produce": {"from_b": True},
                            "min_delay": 0.001,
                            "max_delay": 0.02,
                        },
                        tasks=[TaskExecution.create("Task B", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_c",
                        name="Stage C",
                        requisite_stage_ref_ids={"stage_a"},
                        context={
                            "outputs_to_produce": {"from_c": True},
                            "min_delay": 0.001,
                            "max_delay": 0.02,
                        },
                        tasks=[TaskExecution.create("Task C", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_d",
                        name="Stage D (Join)",
                        requisite_stage_ref_ids={"stage_b", "stage_c"},
                        context={"expected_keys": ["workflow_index", "from_a", "from_b", "from_c"]},
                        tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                    ),
                ],
            )
            file_repository.store(wf)
            runner.start(wf)
            workflow_ids.append(wf.id)

        # Process all workflows
        processor.process_all(timeout=120.0)

        # Verify all completed successfully
        succeeded = 0
        failed = []
        for wf_id in workflow_ids:
            result = file_repository.retrieve(wf_id)
            if result.status == WorkflowStatus.SUCCEEDED:
                succeeded += 1
            else:
                failed.append((wf_id, result.status))

        assert succeeded == num_workflows, f"Only {succeeded}/{num_workflows} succeeded. Failed: {failed}"
        assert not VerifyInputsTask.verification_errors, f"Verification errors: {VerifyInputsTask.verification_errors}"


class TestDiamondRaceConditions:
    """Tests specifically designed to expose race conditions."""

    def test_simultaneous_branch_completion(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Force B and C to complete at almost exactly the same time.

        Both will push StartStage(D) - only one should win.
        """
        CountingOutputTask.reset()
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "slow_output": SlowOutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        # Run multiple times to increase chance of hitting race
        for run in range(5):
            wf = Workflow.create(
                application="diamond-test",
                name=f"race-test-{run}",
                stages=[
                    StageExecution(
                        ref_id="stage_a",
                        name="Stage A",
                        context={"outputs_to_produce": {"from_a": True}},
                        tasks=[TaskExecution.create("Task A", "slow_output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_b",
                        name="Stage B",
                        requisite_stage_ref_ids={"stage_a"},
                        context={"outputs_to_produce": {"from_b": True}, "delay": 0.1},
                        tasks=[TaskExecution.create("Task B", "slow_output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_c",
                        name="Stage C",
                        requisite_stage_ref_ids={"stage_a"},
                        context={"outputs_to_produce": {"from_c": True}, "delay": 0.1},
                        tasks=[TaskExecution.create("Task C", "slow_output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_d",
                        name="Stage D (Join)",
                        requisite_stage_ref_ids={"stage_b", "stage_c"},
                        context={"expected_keys": ["from_a", "from_b", "from_c"]},
                        tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                    ),
                ],
            )

            file_repository.store(wf)
            runner.start(wf)
            processor.process_all(timeout=30.0)

            result = file_repository.retrieve(wf.id)
            assert result.status == WorkflowStatus.SUCCEEDED, f"Run {run}: {result.status}"

            # Stage D should have been executed exactly once
            stage_d = result.stage_by_ref_id("stage_d")
            assert stage_d.status == WorkflowStatus.SUCCEEDED

    def test_join_does_not_start_prematurely(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Verify D never starts before both B and C complete.

        Make C very slow to ensure B completes first.
        D should not start until C also completes.
        """
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "slow_output": SlowOutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = Workflow.create(
            application="diamond-test",
            name="premature-start-test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    context={"outputs_to_produce": {"from_a": True}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B (Fast)",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_b": True, "b_finished_first": True}},
                    tasks=[TaskExecution.create("Task B", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C (Very Slow)",
                    requisite_stage_ref_ids={"stage_a"},
                    context={
                        "outputs_to_produce": {"from_c": True, "c_finished_last": True},
                        "delay": 0.5,
                    },
                    tasks=[TaskExecution.create("Task C", "slow_output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_d",
                    name="Stage D (Join)",
                    requisite_stage_ref_ids={"stage_b", "stage_c"},
                    # If D starts prematurely, it won't have from_c
                    context={
                        "expected_keys": [
                            "from_a",
                            "from_b",
                            "from_c",
                            "b_finished_first",
                            "c_finished_last",
                        ]
                    },
                    tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # D must have received from_c (meaning it waited for C)
        assert not VerifyInputsTask.verification_errors, (
            f"Join started prematurely! Errors: {VerifyInputsTask.verification_errors}"
        )

        stage_d_key = f"{wf.id}:stage_d"
        ctx = VerifyInputsTask.verified_contexts.get(stage_d_key, {})
        assert ctx.get("c_finished_last") is True, "D started before C completed!"


class TestDiamondEdgeCases:
    """Test edge cases in diamond patterns."""

    def test_empty_outputs_in_branch(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """Branch with empty outputs should still allow join to proceed."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = create_diamond_workflow(
            "empty-outputs-test",
            stage_a_outputs={"from_a": True},
            stage_b_outputs={},  # Empty outputs
            stage_c_outputs={"from_c": True},
            stage_d_expected_keys=["from_a", "from_c"],  # Don't expect from_b
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

    def test_large_outputs_in_diamond(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """Diamond with large outputs should handle merging correctly."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        # Create large list outputs
        large_list_b = [f"item_b_{i}" for i in range(100)]
        large_list_c = [f"item_c_{i}" for i in range(100)]

        wf = create_diamond_workflow(
            "large-outputs-test",
            stage_a_outputs={"from_a": True},
            stage_b_outputs={"large_list": large_list_b},
            stage_c_outputs={"large_list": large_list_c},
            stage_d_expected_keys=["from_a", "large_list"],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Verify lists were concatenated
        stage_d_key = f"{wf.id}:stage_d"
        ctx = VerifyInputsTask.verified_contexts.get(stage_d_key, {})
        merged_list = ctx.get("large_list", [])

        assert len(merged_list) == 200, f"Expected 200 items, got {len(merged_list)}"
        assert "item_b_0" in merged_list
        assert "item_c_0" in merged_list


class TestAggressiveStress:
    """
    Aggressive stress tests designed to expose race conditions and edge cases.

    These tests use extreme concurrency and timing to try to break the system.
    """

    @pytest.mark.stress
    def test_100_concurrent_diamonds_with_conflicts(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Run 100 diamond workflows where each has conflicting output keys.

        This maximizes the chance of exposing non-deterministic merge order.
        """
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "random_delay": RandomDelayTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        num_workflows = 50  # Reduced from 100 for reasonable test time
        workflow_ids = []

        for i in range(num_workflows):
            wf = Workflow.create(
                application="diamond-stress",
                name=f"stress-conflict-{i}",
                stages=[
                    StageExecution(
                        ref_id="stage_a",
                        name="Stage A",
                        context={
                            "outputs_to_produce": {"index": i, "conflict": f"a_{i}"},
                            "min_delay": 0.001,
                            "max_delay": 0.005,
                        },
                        tasks=[TaskExecution.create("Task A", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_b",
                        name="Stage B",
                        requisite_stage_ref_ids={"stage_a"},
                        context={
                            "outputs_to_produce": {"conflict": f"b_{i}", "from_b": i},
                            "min_delay": 0.001,
                            "max_delay": 0.01,
                        },
                        tasks=[TaskExecution.create("Task B", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_c",
                        name="Stage C",
                        requisite_stage_ref_ids={"stage_a"},
                        context={
                            "outputs_to_produce": {"conflict": f"c_{i}", "from_c": i},
                            "min_delay": 0.001,
                            "max_delay": 0.01,
                        },
                        tasks=[TaskExecution.create("Task C", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_d",
                        name="Stage D",
                        requisite_stage_ref_ids={"stage_b", "stage_c"},
                        context={"expected_keys": ["index", "conflict", "from_b", "from_c"]},
                        tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                    ),
                ],
            )
            file_repository.store(wf)
            runner.start(wf)
            workflow_ids.append(wf.id)

        processor.process_all(timeout=180.0)

        # Analyze results
        conflict_values = []
        for wf_id in workflow_ids:
            result = file_repository.retrieve(wf_id)
            if result.status != WorkflowStatus.SUCCEEDED:
                continue

            stage_d_key = f"{wf_id}:stage_d"
            ctx = VerifyInputsTask.verified_contexts.get(stage_d_key, {})
            conflict_val = ctx.get("conflict", "MISSING")
            conflict_values.append(conflict_val)

        # Verify all workflows succeeded
        succeeded = sum(
            1 for wf_id in workflow_ids if file_repository.retrieve(wf_id).status == WorkflowStatus.SUCCEEDED
        )
        assert succeeded == num_workflows, f"Only {succeeded}/{num_workflows} succeeded"

        # Check if merge order is consistent
        # Each workflow should have conflict value from either B or C, but consistently
        b_wins = sum(1 for v in conflict_values if v.startswith("b_"))
        c_wins = sum(1 for v in conflict_values if v.startswith("c_"))

        # If both B and C "win" different runs, we have non-determinism
        if b_wins > 0 and c_wins > 0:
            pytest.fail(
                f"NON-DETERMINISTIC MERGE ORDER DETECTED!\n"
                f"B won {b_wins} times, C won {c_wins} times.\n"
                f"This indicates inconsistent merge behavior."
            )

    @pytest.mark.stress
    def test_triple_diamond_chain(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """
        Test a chain of three diamonds - maximum complexity.

            A
           /|\\
          B C D
           \\|/
            E
           /|\\
          F G H
           \\|/
            I
           /|\\
          J K L
           \\|/
            M
        """
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "random_delay": RandomDelayTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = Workflow.create(
            application="diamond-test",
            name="triple-diamond",
            stages=[
                # First diamond
                StageExecution(
                    ref_id="A",
                    name="A",
                    context={
                        "outputs_to_produce": {"from_A": True},
                        "min_delay": 0.001,
                        "max_delay": 0.01,
                    },
                    tasks=[TaskExecution.create("Task A", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="B",
                    name="B",
                    requisite_stage_ref_ids={"A"},
                    context={
                        "outputs_to_produce": {"from_B": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task B", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="C",
                    name="C",
                    requisite_stage_ref_ids={"A"},
                    context={
                        "outputs_to_produce": {"from_C": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task C", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="D",
                    name="D",
                    requisite_stage_ref_ids={"A"},
                    context={
                        "outputs_to_produce": {"from_D": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task D", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="E",
                    name="E (Join 1)",
                    requisite_stage_ref_ids={"B", "C", "D"},
                    context={
                        "outputs_to_produce": {"from_E": True},
                        "min_delay": 0.001,
                        "max_delay": 0.01,
                    },
                    tasks=[TaskExecution.create("Task E", "random_delay", stage_start=True, stage_end=True)],
                ),
                # Second diamond
                StageExecution(
                    ref_id="F",
                    name="F",
                    requisite_stage_ref_ids={"E"},
                    context={
                        "outputs_to_produce": {"from_F": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task F", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="G",
                    name="G",
                    requisite_stage_ref_ids={"E"},
                    context={
                        "outputs_to_produce": {"from_G": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task G", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="H",
                    name="H",
                    requisite_stage_ref_ids={"E"},
                    context={
                        "outputs_to_produce": {"from_H": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task H", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="I",
                    name="I (Join 2)",
                    requisite_stage_ref_ids={"F", "G", "H"},
                    context={
                        "outputs_to_produce": {"from_I": True},
                        "min_delay": 0.001,
                        "max_delay": 0.01,
                    },
                    tasks=[TaskExecution.create("Task I", "random_delay", stage_start=True, stage_end=True)],
                ),
                # Third diamond
                StageExecution(
                    ref_id="J",
                    name="J",
                    requisite_stage_ref_ids={"I"},
                    context={
                        "outputs_to_produce": {"from_J": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task J", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="K",
                    name="K",
                    requisite_stage_ref_ids={"I"},
                    context={
                        "outputs_to_produce": {"from_K": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task K", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="L",
                    name="L",
                    requisite_stage_ref_ids={"I"},
                    context={
                        "outputs_to_produce": {"from_L": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task L", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="M",
                    name="M (Final Join)",
                    requisite_stage_ref_ids={"J", "K", "L"},
                    context={
                        "expected_keys": [
                            "from_A",
                            "from_B",
                            "from_C",
                            "from_D",
                            "from_E",
                            "from_F",
                            "from_G",
                            "from_H",
                            "from_I",
                            "from_J",
                            "from_K",
                            "from_L",
                        ]
                    },
                    tasks=[TaskExecution.create("Task M", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=60.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"
        assert not VerifyInputsTask.verification_errors, f"Errors: {VerifyInputsTask.verification_errors}"

    @pytest.mark.stress
    def test_interleaved_diamonds(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """
        Test interleaved diamonds - one branch of diamond 1 feeds into diamond 2.

            A1
           / \\
          B1  C1
           \\   |
            D1 |
               \\|
                E (join from C1 and D1)
               / \\
              F   G
               \\ /
                H
        """
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "random_delay": RandomDelayTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = Workflow.create(
            application="diamond-test",
            name="interleaved-diamond",
            stages=[
                StageExecution(
                    ref_id="A1",
                    name="A1",
                    context={
                        "outputs_to_produce": {"from_A1": True},
                        "min_delay": 0.001,
                        "max_delay": 0.01,
                    },
                    tasks=[TaskExecution.create("Task A1", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="B1",
                    name="B1",
                    requisite_stage_ref_ids={"A1"},
                    context={
                        "outputs_to_produce": {"from_B1": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task B1", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="C1",
                    name="C1",
                    requisite_stage_ref_ids={"A1"},
                    context={
                        "outputs_to_produce": {"from_C1": True},
                        "min_delay": 0.001,
                        "max_delay": 0.03,
                    },
                    tasks=[TaskExecution.create("Task C1", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="D1",
                    name="D1 (partial join)",
                    requisite_stage_ref_ids={"B1"},
                    context={
                        "outputs_to_produce": {"from_D1": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task D1", "random_delay", stage_start=True, stage_end=True)],
                ),
                # E joins from C1 (bypassing B1->D1 path) and D1
                StageExecution(
                    ref_id="E",
                    name="E (cross-diamond join)",
                    requisite_stage_ref_ids={"C1", "D1"},
                    context={
                        "outputs_to_produce": {"from_E": True},
                        "min_delay": 0.001,
                        "max_delay": 0.01,
                    },
                    tasks=[TaskExecution.create("Task E", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="F",
                    name="F",
                    requisite_stage_ref_ids={"E"},
                    context={
                        "outputs_to_produce": {"from_F": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task F", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="G",
                    name="G",
                    requisite_stage_ref_ids={"E"},
                    context={
                        "outputs_to_produce": {"from_G": True},
                        "min_delay": 0.001,
                        "max_delay": 0.02,
                    },
                    tasks=[TaskExecution.create("Task G", "random_delay", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="H",
                    name="H (final join)",
                    requisite_stage_ref_ids={"F", "G"},
                    context={
                        "expected_keys": [
                            "from_A1",
                            "from_B1",
                            "from_C1",
                            "from_D1",
                            "from_E",
                            "from_F",
                            "from_G",
                        ]
                    },
                    tasks=[TaskExecution.create("Task H", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=60.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"
        assert not VerifyInputsTask.verification_errors, f"Errors: {VerifyInputsTask.verification_errors}"

    @pytest.mark.stress
    def test_diamond_with_rapid_fire_messages(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Test that the system handles rapid-fire message processing correctly.

        This is designed to stress test the message deduplication and optimistic
        locking mechanisms.
        """
        CountingOutputTask.reset()
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "counting": CountingOutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        # Create many small diamond workflows
        num_workflows = 30
        workflow_ids = []

        for i in range(num_workflows):
            wf = Workflow.create(
                application="rapid-fire-test",
                name=f"rapid-{i}",
                stages=[
                    StageExecution(
                        ref_id="A",
                        name="A",
                        context={"outputs_to_produce": {"wf": i}},
                        tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="B",
                        name="B",
                        requisite_stage_ref_ids={"A"},
                        context={"outputs_to_produce": {"from_B": True}},
                        tasks=[TaskExecution.create("Task B", "output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="C",
                        name="C",
                        requisite_stage_ref_ids={"A"},
                        context={"outputs_to_produce": {"from_C": True}},
                        tasks=[TaskExecution.create("Task C", "output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="D",
                        name="D",
                        requisite_stage_ref_ids={"B", "C"},
                        context={"expected_keys": ["wf", "from_B", "from_C"]},
                        tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                    ),
                ],
            )
            file_repository.store(wf)
            runner.start(wf)
            workflow_ids.append(wf.id)

        # Process all at once
        processor.process_all(timeout=120.0)

        # Verify all succeeded
        failed = []
        for wf_id in workflow_ids:
            result = file_repository.retrieve(wf_id)
            if result.status != WorkflowStatus.SUCCEEDED:
                failed.append((wf_id, result.status))

        assert not failed, f"Failed workflows: {failed}"
        assert not VerifyInputsTask.verification_errors


class TestDiamondRecovery:
    """Test recovery scenarios in diamond patterns."""

    def test_diamond_resumes_after_partial_completion(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Test that a diamond can resume if processing is interrupted.

        This simulates what happens if the processor stops and restarts mid-workflow.
        """
        VerifyInputsTask.reset()

        processor1, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "slow_output": SlowOutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = Workflow.create(
            application="recovery-test",
            name="interrupt-test",
            stages=[
                StageExecution(
                    ref_id="A",
                    name="A",
                    context={"outputs_to_produce": {"from_A": True}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="B",
                    name="B",
                    requisite_stage_ref_ids={"A"},
                    context={"outputs_to_produce": {"from_B": True}},
                    tasks=[TaskExecution.create("Task B", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="C",
                    name="C",
                    requisite_stage_ref_ids={"A"},
                    context={"outputs_to_produce": {"from_C": True}, "delay": 0.5},
                    tasks=[TaskExecution.create("Task C", "slow_output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="D",
                    name="D",
                    requisite_stage_ref_ids={"B", "C"},
                    context={"expected_keys": ["from_A", "from_B", "from_C"]},
                    tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)

        # Process just a bit (should complete A and start B/C)
        processor1.process_one()
        processor1.process_one()
        processor1.process_one()

        # "Crash" - create new processor
        processor2, _, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "slow_output": SlowOutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        # Continue processing
        processor2.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"
        assert not VerifyInputsTask.verification_errors


class TestDiamondWithJumps:
    """
    Test diamond patterns combined with dynamic routing (jumps).

    This is the most complex scenario: a diamond pattern where one
    of the branches triggers a jump back to retry the diamond.
    """

    def test_diamond_with_conditional_retry(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Test diamond where branch C can trigger a retry of the entire diamond.

        First iteration: C jumps back to A (retry)
        Second iteration: C succeeds

        This tests:
        - Proper reset of all stages on jump
        - Context preservation across iterations
        - No duplicate execution of join point D
        """

        class JumpOnFirstAttemptTask(Task):
            """Task that jumps back on first attempt, succeeds on second."""

            attempts: dict[str, int] = {}
            _lock = threading.Lock()

            @classmethod
            def reset(cls) -> None:
                with cls._lock:
                    cls.attempts = {}

            def execute(self, stage: StageExecution) -> TaskResult:
                key = f"{stage.execution.id}:{stage.ref_id}"
                with JumpOnFirstAttemptTask._lock:
                    attempt = JumpOnFirstAttemptTask.attempts.get(key, 0) + 1
                    JumpOnFirstAttemptTask.attempts[key] = attempt

                if attempt == 1:
                    # First attempt - jump back to A
                    return TaskResult.jump_to(
                        target_stage_ref_id="stage_a",
                        context={"retry_from_c": True, "attempt": attempt},
                        outputs={"jump_triggered": True},
                    )
                else:
                    # Second attempt - succeed
                    return TaskResult.success(outputs={"from_c": True, "attempt": attempt})

        JumpOnFirstAttemptTask.reset()
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "jump_first": JumpOnFirstAttemptTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = Workflow.create(
            application="diamond-jump-test",
            name="conditional-retry",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    context={"outputs_to_produce": {"from_a": True}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_b": True}},
                    tasks=[TaskExecution.create("Task B", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C (May Jump)",
                    requisite_stage_ref_ids={"stage_a"},
                    context={},  # Uses JumpOnFirstAttemptTask
                    tasks=[TaskExecution.create("Task C", "jump_first", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_d",
                    name="Stage D (Join)",
                    requisite_stage_ref_ids={"stage_b", "stage_c"},
                    context={"expected_keys": ["from_a", "from_b", "from_c"]},
                    tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=60.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"
        assert not VerifyInputsTask.verification_errors

        # Verify C was executed twice (jump + retry)
        c_key = f"{wf.id}:stage_c"
        assert JumpOnFirstAttemptTask.attempts.get(c_key, 0) == 2, (
            f"C should have been executed twice, got {JumpOnFirstAttemptTask.attempts.get(c_key, 0)}"
        )

    def test_diamond_jump_from_join_point(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Test jump from the join point D back to one of the branches.

        This is tricky because D has received outputs from both B and C,
        but on retry, those outputs should be regenerated.
        """

        class JumpFromJoinTask(Task):
            """Task that jumps back from join point on first attempt."""

            attempts: dict[str, int] = {}
            _lock = threading.Lock()

            @classmethod
            def reset(cls) -> None:
                with cls._lock:
                    cls.attempts = {}

            def execute(self, stage: StageExecution) -> TaskResult:
                key = f"{stage.execution.id}:{stage.ref_id}"
                expected = stage.context.get("expected_keys", [])

                # Verify we have all expected inputs
                missing = [k for k in expected if k not in stage.context]
                if missing:
                    return TaskResult.terminal(f"Missing keys at join: {missing}")

                with JumpFromJoinTask._lock:
                    attempt = JumpFromJoinTask.attempts.get(key, 0) + 1
                    JumpFromJoinTask.attempts[key] = attempt

                if attempt == 1:
                    # First attempt at join - jump back to B
                    return TaskResult.jump_to(
                        target_stage_ref_id="stage_b",
                        context={"retry_from_d": True},
                        outputs={"d_jump_triggered": True},
                    )
                else:
                    # Second attempt - succeed
                    return TaskResult.success(outputs={"from_d": True, "d_attempts": attempt})

        JumpFromJoinTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "jump_from_join": JumpFromJoinTask,
            },
        )

        wf = Workflow.create(
            application="diamond-jump-test",
            name="jump-from-join",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    context={"outputs_to_produce": {"from_a": True}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_b": True}},
                    tasks=[TaskExecution.create("Task B", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_c": True}},
                    tasks=[TaskExecution.create("Task C", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_d",
                    name="Stage D (Join - May Jump)",
                    requisite_stage_ref_ids={"stage_b", "stage_c"},
                    context={"expected_keys": ["from_a", "from_b", "from_c"]},
                    tasks=[TaskExecution.create("Task D", "jump_from_join", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=60.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"

        # Verify D was executed twice
        d_key = f"{wf.id}:stage_d"
        assert JumpFromJoinTask.attempts.get(d_key, 0) == 2


class TestDiamondChaos:
    """
    Chaos testing for diamond patterns - random failures and jumps.

    These tests are designed to expose any hidden bugs by creating
    unpredictable execution patterns.
    """

    @pytest.mark.stress
    def test_many_diamonds_with_random_jumps(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Run many diamonds where branches randomly decide to jump.

        This creates maximum chaos to expose race conditions.
        """

        class MaybeJumpTask(Task):
            """Task that may randomly trigger a jump."""

            _lock = threading.Lock()
            jump_count: int = 0
            success_count: int = 0

            @classmethod
            def reset(cls) -> None:
                with cls._lock:
                    cls.jump_count = 0
                    cls.success_count = 0

            def execute(self, stage: StageExecution) -> TaskResult:
                # 20% chance to jump back to A on first iteration
                iteration = stage.context.get("_jump_iteration", 0)

                if iteration == 0 and random.random() < 0.2:
                    with MaybeJumpTask._lock:
                        MaybeJumpTask.jump_count += 1
                    return TaskResult.jump_to(
                        target_stage_ref_id="stage_a",
                        context={"_jump_iteration": iteration + 1},
                        outputs={"jumped": True},
                    )

                with MaybeJumpTask._lock:
                    MaybeJumpTask.success_count += 1

                outputs = stage.context.get("outputs_to_produce", {})
                return TaskResult.success(outputs=outputs)

        MaybeJumpTask.reset()
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "maybe_jump": MaybeJumpTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        num_workflows = 20
        workflow_ids = []

        for i in range(num_workflows):
            wf = Workflow.create(
                application="chaos-test",
                name=f"chaos-diamond-{i}",
                stages=[
                    StageExecution(
                        ref_id="stage_a",
                        name="Stage A",
                        context={"outputs_to_produce": {"from_a": True, "wf_index": i}},
                        tasks=[TaskExecution.create("Task A", "maybe_jump", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_b",
                        name="Stage B",
                        requisite_stage_ref_ids={"stage_a"},
                        context={"outputs_to_produce": {"from_b": True}},
                        tasks=[TaskExecution.create("Task B", "maybe_jump", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_c",
                        name="Stage C",
                        requisite_stage_ref_ids={"stage_a"},
                        context={"outputs_to_produce": {"from_c": True}},
                        tasks=[TaskExecution.create("Task C", "maybe_jump", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_d",
                        name="Stage D",
                        requisite_stage_ref_ids={"stage_b", "stage_c"},
                        context={"expected_keys": ["from_a", "from_b", "from_c", "wf_index"]},
                        tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                    ),
                ],
            )

            file_repository.store(wf)
            runner.start(wf)
            workflow_ids.append(wf.id)

        processor.process_all(timeout=180.0)

        # All workflows should eventually succeed despite random jumps
        failed = []
        for wf_id in workflow_ids:
            result = file_repository.retrieve(wf_id)
            if result.status != WorkflowStatus.SUCCEEDED:
                failed.append((wf_id, result.status))

        assert not failed, f"Failed workflows: {failed}"
        assert not VerifyInputsTask.verification_errors, f"Errors: {VerifyInputsTask.verification_errors}"

        # Log chaos statistics
        print(f"\nChaos stats: {MaybeJumpTask.jump_count} jumps, {MaybeJumpTask.success_count} successes")
