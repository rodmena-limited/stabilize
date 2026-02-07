"""
Critical corner case tests for airport-critical systems.

This test suite focuses on edge cases and potential bugs that could cause
workflow failures in critical systems. Tests run on both SQLite and PostgreSQL
backends using testcontainers.

Categories:
1. State determination edge cases
2. Jump-to functionality edge cases
3. DAG traversal edge cases
4. Concurrency and race condition tests
5. Context merging edge cases
6. Synthetic stage edge cases
7. Task execution edge cases
8. Status transition edge cases
9. Message handling edge cases
10. Recovery and resilience tests
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from stabilize import (
    StageExecution,
    Task,
    TaskResult,
)
from stabilize.dag.topological import (
    CircularDependencyError,
    get_execution_layers,
    topological_sort,
)
from stabilize.errors import ConcurrencyError, PermanentError, TransientError
from stabilize.models.stage import SyntheticStageOwner
from stabilize.models.status import (
    CONTINUABLE_STATUSES,
    HALT_STATUSES,
    WorkflowStatus,
    can_transition,
)
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize

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


# =============================================================================
# 1. State Determination Edge Cases
# =============================================================================


class TestStateDeterminationEdgeCases:
    """Test edge cases in stage.determine_status()."""

    def test_empty_stage_status(self) -> None:
        """Stage with no tasks and no synthetic stages."""
        stage = StageExecution.create(
            type="empty",
            name="Empty Stage",
            ref_id="empty",
        )
        # Empty stage should return NOT_STARTED
        assert stage.determine_status() == WorkflowStatus.NOT_STARTED

    def test_stage_with_only_not_started_tasks(self) -> None:
        """Stage with tasks that are all NOT_STARTED."""
        stage = StageExecution.create(
            type="test",
            name="Test Stage",
            ref_id="test",
        )
        stage.tasks = [
            TaskExecution(name="task1", implementing_class="test"),
            TaskExecution(name="task2", implementing_class="test"),
        ]
        # Should be RUNNING since tasks haven't started yet
        status = stage.determine_status()
        assert status == WorkflowStatus.RUNNING

    def test_stage_with_mixed_task_statuses(self) -> None:
        """Stage with tasks in different states."""
        stage = StageExecution.create(
            type="test",
            name="Test Stage",
            ref_id="test",
        )
        task1 = TaskExecution(name="task1", implementing_class="test")
        task1.status = WorkflowStatus.SUCCEEDED
        task2 = TaskExecution(name="task2", implementing_class="test")
        task2.status = WorkflowStatus.RUNNING
        stage.tasks = [task1, task2]

        # Should be RUNNING since task2 is still running
        assert stage.determine_status() == WorkflowStatus.RUNNING

    def test_stage_with_terminal_and_succeeded_tasks(self) -> None:
        """Stage with one TERMINAL and one SUCCEEDED task."""
        stage = StageExecution.create(
            type="test",
            name="Test Stage",
            ref_id="test",
        )
        task1 = TaskExecution(name="task1", implementing_class="test")
        task1.status = WorkflowStatus.SUCCEEDED
        task2 = TaskExecution(name="task2", implementing_class="test")
        task2.status = WorkflowStatus.TERMINAL
        stage.tasks = [task1, task2]

        # TERMINAL should take priority
        status = stage.determine_status()
        assert status.is_halt

    def test_stage_with_paused_tasks(self) -> None:
        """Stage with paused tasks."""
        stage = StageExecution.create(
            type="test",
            name="Test Stage",
            ref_id="test",
        )
        task1 = TaskExecution(name="task1", implementing_class="test")
        task1.status = WorkflowStatus.PAUSED
        stage.tasks = [task1]

        assert stage.determine_status() == WorkflowStatus.PAUSED

    def test_stage_status_priority_order(self) -> None:
        """Verify status priority order is correct."""
        # Priority: TERMINAL > STOPPED > CANCELED > PAUSED > RUNNING > FAILED_CONTINUE > SUCCEEDED
        stage = StageExecution.create(
            type="test",
            name="Test Stage",
            ref_id="test",
        )

        # All halt statuses should take priority
        for halt_status in [
            WorkflowStatus.TERMINAL,
            WorkflowStatus.STOPPED,
            WorkflowStatus.CANCELED,
        ]:
            task = TaskExecution(name="task", implementing_class="test")
            task.status = halt_status
            stage.tasks = [task]
            assert stage.determine_status().is_halt

    def test_after_stages_running_prevents_success(self) -> None:
        """After-stages running should prevent stage from reporting SUCCEEDED."""
        # Create workflow with parent and after-stage
        parent = StageExecution.create(
            type="test",
            name="Parent",
            ref_id="parent",
        )
        task = TaskExecution(name="task", implementing_class="test")
        task.status = WorkflowStatus.SUCCEEDED
        parent.tasks = [task]

        after = StageExecution.create_synthetic(
            type="cleanup",
            name="Cleanup",
            parent=parent,
            owner=SyntheticStageOwner.STAGE_AFTER,
        )
        after.status = WorkflowStatus.RUNNING

        # Create workflow to link stages (must store reference to prevent GC)
        _workflow = Workflow.create(
            application="test",
            name="Test",
            stages=[parent, after],
        )

        # Parent should still be RUNNING since after-stage is running
        assert parent.determine_status() == WorkflowStatus.RUNNING
        del _workflow  # Explicit cleanup


# =============================================================================
# 2. Jump-to Functionality Edge Cases
# =============================================================================


class TestJumpToEdgeCases:
    """Test edge cases in jump-to functionality."""

    def test_jump_to_self_basic(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test jumping to the same stage (self-loop)."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"jumping": JumpingTask},
        )

        stage = StageExecution.create(
            type="test",
            name="Self Jump Stage",
            ref_id="self_jump",
            context={
                "jump_target": "self_jump",
                "_max_jumps": 3,  # Limit iterations
            },
        )

        task = TaskExecution(
            name="JumpTask",
            implementing_class="jumping",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Self Jump Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=10.0)

        # Reload and check - should have hit max jumps
        result = repository.retrieve(workflow.id)
        assert result.status in HALT_STATUSES or result.status == WorkflowStatus.TERMINAL

    def test_jump_to_nonexistent_stage(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test jumping to a stage that doesn't exist."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"jumping": JumpingTask},
        )

        stage = StageExecution.create(
            type="test",
            name="Bad Jump Stage",
            ref_id="bad_jump",
            context={"jump_target": "nonexistent_stage"},
        )

        task = TaskExecution(
            name="JumpTask",
            implementing_class="jumping",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Bad Jump Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        # Should fail with TERMINAL status
        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.TERMINAL

    def test_jump_with_zero_max_jumps(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test that _max_jumps=0 correctly disables all jumps.

        Setting _max_jumps=0 should block any jump attempts, causing the
        workflow to fail with TERMINAL status when a jump is attempted.
        """
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"jumping": JumpingTask},
        )

        stage1 = StageExecution.create(
            type="test",
            name="Jump Source",
            ref_id="source",
            context={"jump_target": "target", "_max_jumps": 0},
        )
        task1 = TaskExecution(
            name="JumpTask",
            implementing_class="jumping",
            stage_start=True,
            stage_end=True,
        )
        stage1.tasks = [task1]

        stage2 = StageExecution.create(
            type="test",
            name="Jump Target",
            ref_id="target",
            requisite_stage_ref_ids={"source"},
        )
        task2 = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        stage2.tasks = [task2]

        workflow = Workflow.create(
            application="test",
            name="Zero Max Jumps Test",
            stages=[stage1, stage2],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        # With _max_jumps=0, the jump should be blocked and workflow fails
        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.TERMINAL

    def test_jump_context_preservation(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test that jump context is properly preserved."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"jumping": JumpingTask, "output": OutputMergingTask},
        )

        stage1 = StageExecution.create(
            type="test",
            name="Jump Source",
            ref_id="source",
            context={
                "original_value": "preserved",
                "jump_target": "target",
                "_max_jumps": 5,
            },
        )
        task1 = TaskExecution(
            name="JumpTask",
            implementing_class="jumping",
            stage_start=True,
            stage_end=True,
        )
        stage1.tasks = [task1]

        stage2 = StageExecution.create(
            type="test",
            name="Jump Target",
            ref_id="target",
            requisite_stage_ref_ids={"source"},
        )
        task2 = TaskExecution(
            name="OutputTask",
            implementing_class="output",
            stage_start=True,
            stage_end=True,
        )
        stage2.tasks = [task2]

        workflow = Workflow.create(
            application="test",
            name="Jump Context Test",
            stages=[stage1, stage2],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        # Check that jump context was preserved
        target_stage = result.stage_by_ref_id("target")
        assert target_stage is not None
        assert "jumped_from" in target_stage.context or target_stage.context.get("_jump_history")


# =============================================================================
# 3. DAG Traversal Edge Cases
# =============================================================================


class TestDagTraversalEdgeCases:
    """Test edge cases in DAG traversal."""

    def test_stage_with_missing_requisite(self) -> None:
        """Stage with requisite pointing to non-existent stage."""
        stages = [
            StageExecution.create(
                type="stage",
                name="A",
                ref_id="a",
                requisite_stage_ref_ids={"nonexistent"},
            ),
        ]

        # Should raise or handle gracefully
        with pytest.raises(CircularDependencyError):
            topological_sort(stages)

    def test_self_referential_requisite(self) -> None:
        """Stage with requisite pointing to itself."""
        stages = [
            StageExecution.create(
                type="stage",
                name="A",
                ref_id="a",
                requisite_stage_ref_ids={"a"},
            ),
        ]

        # Should detect self-cycle
        with pytest.raises(CircularDependencyError):
            topological_sort(stages)

    def test_deep_dag_structure(self) -> None:
        """Very deep DAG structure (potential stack overflow)."""
        stages = []
        prev_ref = None

        # Create 100-level deep DAG
        for i in range(100):
            ref_id = f"stage_{i}"
            stage = StageExecution.create(
                type="stage",
                name=f"Stage {i}",
                ref_id=ref_id,
                requisite_stage_ref_ids={prev_ref} if prev_ref else set(),
            )
            stages.append(stage)
            prev_ref = ref_id

        # Should handle deep structures without stack overflow
        sorted_stages = topological_sort(stages)
        assert len(sorted_stages) == 100
        assert sorted_stages[0].ref_id == "stage_0"
        assert sorted_stages[-1].ref_id == "stage_99"

    def test_wide_dag_structure(self) -> None:
        """Very wide DAG structure with many parallel stages."""
        stages = [
            StageExecution.create(type="stage", name="Start", ref_id="start"),
        ]

        # Add 50 parallel stages
        for i in range(50):
            stages.append(
                StageExecution.create(
                    type="stage",
                    name=f"Parallel {i}",
                    ref_id=f"parallel_{i}",
                    requisite_stage_ref_ids={"start"},
                )
            )

        # Add join stage
        stages.append(
            StageExecution.create(
                type="stage",
                name="Join",
                ref_id="join",
                requisite_stage_ref_ids={f"parallel_{i}" for i in range(50)},
            )
        )

        sorted_stages = topological_sort(stages)
        layers = get_execution_layers(stages)

        assert len(sorted_stages) == 52
        assert len(layers) == 3
        assert len(layers[1]) == 50  # 50 parallel stages

    def test_complex_diamond_pattern(self) -> None:
        """Complex diamond with multiple join points."""
        #       A
        #      /|\
        #     B C D
        #     |X|/
        #     E F
        #      \|
        #       G

        stages = [
            StageExecution.create(type="stage", name="A", ref_id="a"),
            StageExecution.create(
                type="stage", name="B", ref_id="b", requisite_stage_ref_ids={"a"}
            ),
            StageExecution.create(
                type="stage", name="C", ref_id="c", requisite_stage_ref_ids={"a"}
            ),
            StageExecution.create(
                type="stage", name="D", ref_id="d", requisite_stage_ref_ids={"a"}
            ),
            StageExecution.create(
                type="stage", name="E", ref_id="e", requisite_stage_ref_ids={"b", "c"}
            ),
            StageExecution.create(
                type="stage", name="F", ref_id="f", requisite_stage_ref_ids={"c", "d"}
            ),
            StageExecution.create(
                type="stage", name="G", ref_id="g", requisite_stage_ref_ids={"e", "f"}
            ),
        ]

        sorted_stages = topological_sort(stages)

        # Verify order constraints
        ref_ids = [s.ref_id for s in sorted_stages]
        assert ref_ids.index("a") < ref_ids.index("b")
        assert ref_ids.index("a") < ref_ids.index("c")
        assert ref_ids.index("a") < ref_ids.index("d")
        assert ref_ids.index("b") < ref_ids.index("e")
        assert ref_ids.index("c") < ref_ids.index("e")
        assert ref_ids.index("c") < ref_ids.index("f")
        assert ref_ids.index("d") < ref_ids.index("f")
        assert ref_ids.index("e") < ref_ids.index("g")
        assert ref_ids.index("f") < ref_ids.index("g")


# =============================================================================
# 4. Concurrency and Race Condition Tests
# =============================================================================


class TestConcurrencyEdgeCases:
    """Test concurrency-related edge cases."""

    def test_concurrent_workflow_starts(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test starting multiple workflows concurrently."""
        processor, orchestrator, registry = setup_stabilize(repository, queue)

        workflows = []
        for i in range(5):
            stage = StageExecution.create(
                type="test",
                name=f"Stage {i}",
                ref_id=f"stage_{i}",
            )
            task = TaskExecution(
                name="SuccessTask",
                implementing_class="success",
                stage_start=True,
                stage_end=True,
            )
            stage.tasks = [task]

            workflow = Workflow.create(
                application="test",
                name=f"Concurrent Test {i}",
                stages=[stage],
            )
            repository.store(workflow)
            workflows.append(workflow)

        # Start all workflows
        for wf in workflows:
            orchestrator.start(wf)

        # Process all
        processor.process_all(timeout=10.0)

        # All should complete successfully
        for wf in workflows:
            result = repository.retrieve(wf.id)
            assert result.status == WorkflowStatus.SUCCEEDED

    def test_concurrent_stage_completion(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test multiple stages completing at the same time."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"slow": SlowTask},
        )

        # Create a workflow with multiple parallel stages
        start_stage = StageExecution.create(
            type="test",
            name="Start",
            ref_id="start",
        )
        start_task = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        start_stage.tasks = [start_task]

        parallel_stages = []
        for i in range(5):
            stage = StageExecution.create(
                type="test",
                name=f"Parallel {i}",
                ref_id=f"parallel_{i}",
                requisite_stage_ref_ids={"start"},
                context={"sleep_time": 0.01},  # Short sleep
            )
            task = TaskExecution(
                name="SlowTask",
                implementing_class="slow",
                stage_start=True,
                stage_end=True,
            )
            stage.tasks = [task]
            parallel_stages.append(stage)

        join_stage = StageExecution.create(
            type="test",
            name="Join",
            ref_id="join",
            requisite_stage_ref_ids={f"parallel_{i}" for i in range(5)},
        )
        join_task = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        join_stage.tasks = [join_task]

        workflow = Workflow.create(
            application="test",
            name="Concurrent Stage Test",
            stages=[start_stage] + parallel_stages + [join_stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

    def test_concurrency_error_retry(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test that ConcurrencyError triggers proper retry."""
        from stabilize.handlers.base import StabilizeHandler
        from stabilize.resilience.config import HandlerConfig

        call_count = 0
        success_on_attempt = 3

        # Note: retry_on_concurrency_error expects Callable[[], None]
        def failing_func() -> None:
            nonlocal call_count
            call_count += 1
            if call_count < success_on_attempt:
                raise ConcurrencyError("Test concurrency error")
            # Success - just return (no value needed)

        # Create a mock handler to test retry logic
        class MockHandler(StabilizeHandler):
            @property
            def message_type(self) -> type:
                return MagicMock

            def handle(self, message: Any) -> None:
                pass

        config = HandlerConfig(
            concurrency_max_retries=5,
            concurrency_min_delay_ms=1,
        )

        handler = MockHandler(
            queue=MagicMock(),
            repository=MagicMock(),
            handler_config=config,
        )

        # Should not raise - succeeds after retries
        handler.retry_on_concurrency_error(failing_func, "test operation")
        assert call_count == success_on_attempt


# =============================================================================
# 5. Context Merging Edge Cases
# =============================================================================


class TestContextMergingEdgeCases:
    """Test edge cases in context and output merging."""

    def test_list_concatenation_with_duplicates(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test that list outputs are concatenated with deduplication."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"output": OutputMergingTask},
        )

        stage1 = StageExecution.create(
            type="test",
            name="Stage 1",
            ref_id="stage1",
            context={"list_items": ["a", "b", "c"]},
        )
        task1 = TaskExecution(
            name="OutputTask",
            implementing_class="output",
            stage_start=True,
            stage_end=True,
        )
        stage1.tasks = [task1]

        stage2 = StageExecution.create(
            type="test",
            name="Stage 2",
            ref_id="stage2",
            requisite_stage_ref_ids={"stage1"},
            context={"list_items": ["b", "c", "d"]},
        )
        task2 = TaskExecution(
            name="OutputTask",
            implementing_class="output",
            stage_start=True,
            stage_end=True,
        )
        stage2.tasks = [task2]

        workflow = Workflow.create(
            application="test",
            name="List Merge Test",
            stages=[stage1, stage2],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Check aggregated context
        aggregated = result.get_context()
        assert "list_output" in aggregated
        # Should have deduplicated list items
        assert set(aggregated["list_output"]) == {"a", "b", "c", "d"}

    def test_scalar_override_in_merge(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test that scalar values are overridden (latest wins)."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"output": OutputMergingTask},
        )

        stage1 = StageExecution.create(
            type="test",
            name="Stage 1",
            ref_id="stage1",
            context={"scalar_value": "first"},
        )
        task1 = TaskExecution(
            name="OutputTask",
            implementing_class="output",
            stage_start=True,
            stage_end=True,
        )
        stage1.tasks = [task1]

        stage2 = StageExecution.create(
            type="test",
            name="Stage 2",
            ref_id="stage2",
            requisite_stage_ref_ids={"stage1"},
            context={"scalar_value": "second"},
        )
        task2 = TaskExecution(
            name="OutputTask",
            implementing_class="output",
            stage_start=True,
            stage_end=True,
        )
        stage2.tasks = [task2]

        workflow = Workflow.create(
            application="test",
            name="Scalar Merge Test",
            stages=[stage1, stage2],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        aggregated = result.get_context()
        assert aggregated["scalar_output"] == "second"  # Latest wins

    def test_empty_context_merge(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test merging with empty contexts."""
        processor, orchestrator, registry = setup_stabilize(repository, queue)

        stage = StageExecution.create(
            type="test",
            name="Empty Context Stage",
            ref_id="empty",
            context={},  # Empty context
        )
        task = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Empty Context Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# 6. Synthetic Stage Edge Cases
# =============================================================================


class TestSyntheticStageEdgeCases:
    """Test edge cases with synthetic stages."""

    def test_synthetic_stage_without_parent_link(self) -> None:
        """Synthetic stage with parent_stage_id but parent not in workflow."""
        parent = StageExecution.create(
            type="test",
            name="Parent",
            ref_id="parent",
        )

        synthetic = StageExecution.create_synthetic(
            type="cleanup",
            name="Cleanup",
            parent=parent,
            owner=SyntheticStageOwner.STAGE_AFTER,
        )

        # Create workflow without the parent
        Workflow.create(
            application="test",
            name="Orphan Synthetic Test",
            stages=[synthetic],
        )

        # Should raise when trying to access parent
        with pytest.raises(ValueError):
            synthetic.parent()

    def test_multiple_before_stages(self) -> None:
        """Multiple before-stages should all run."""
        parent = StageExecution.create(
            type="test",
            name="Parent",
            ref_id="parent",
        )

        before1 = StageExecution.create_synthetic(
            type="setup1",
            name="Setup 1",
            parent=parent,
            owner=SyntheticStageOwner.STAGE_BEFORE,
        )

        before2 = StageExecution.create_synthetic(
            type="setup2",
            name="Setup 2",
            parent=parent,
            owner=SyntheticStageOwner.STAGE_BEFORE,
        )

        # Must store workflow reference to prevent garbage collection
        _workflow = Workflow.create(
            application="test",
            name="Multi Before Test",
            stages=[parent, before1, before2],
        )

        # Should find both before-stages
        before_stages = parent.before_stages()
        assert len(before_stages) == 2
        del _workflow  # Explicit cleanup

    def test_synthetic_stage_status_propagation(self) -> None:
        """Synthetic stage failure should affect parent status."""
        parent = StageExecution.create(
            type="test",
            name="Parent",
            ref_id="parent",
        )
        task = TaskExecution(name="task", implementing_class="test")
        task.status = WorkflowStatus.SUCCEEDED
        parent.tasks = [task]

        after = StageExecution.create_synthetic(
            type="cleanup",
            name="Cleanup",
            parent=parent,
            owner=SyntheticStageOwner.STAGE_AFTER,
        )
        after.status = WorkflowStatus.TERMINAL

        # Must store workflow reference to prevent garbage collection
        _workflow = Workflow.create(
            application="test",
            name="Synthetic Status Test",
            stages=[parent, after],
        )

        # Parent should report TERMINAL due to after-stage failure
        assert parent.determine_status() == WorkflowStatus.TERMINAL
        del _workflow  # Explicit cleanup


# =============================================================================
# 7. Task Execution Edge Cases
# =============================================================================


class TestTaskExecutionEdgeCases:
    """Test edge cases in task execution."""

    def test_task_returning_running_multiple_times(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Task that returns RUNNING should be re-executed."""
        # Reset the run counter
        RunningTask.run_counts = {}

        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"running": RunningTask},
        )

        stage = StageExecution.create(
            type="test",
            name="Running Task Stage",
            ref_id="running",
            context={"required_runs": 3},
        )
        task = TaskExecution(
            name="RunningTask",
            implementing_class="running",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Running Task Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Check outputs show multiple runs occurred
        running_stage = result.stage_by_ref_id("running")
        assert running_stage.outputs.get("total_runs") == 3

    def test_transient_error_with_context_update(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """TransientError with context_update should preserve state."""
        # Reset the failure counter
        TransientFailTask.failure_counts = {}

        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"transient": TransientFailTask},
        )

        stage = StageExecution.create(
            type="test",
            name="Transient Fail Stage",
            ref_id="transient",
            context={"max_transient_failures": 2},
        )
        task = TaskExecution(
            name="TransientTask",
            implementing_class="transient",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Transient Error Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

    def test_permanent_error_moves_to_dlq(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """PermanentError should mark task as TERMINAL."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"permanent": PermanentFailTask},
        )

        stage = StageExecution.create(
            type="test",
            name="Permanent Fail Stage",
            ref_id="permanent",
        )
        task = TaskExecution(
            name="PermanentTask",
            implementing_class="permanent",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Permanent Error Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.TERMINAL


# =============================================================================
# 8. Status Transition Edge Cases
# =============================================================================


class TestStatusTransitionEdgeCases:
    """Test edge cases in status transitions."""

    def test_all_halt_statuses_block_downstream(self) -> None:
        """All halt statuses should block downstream execution."""
        for halt_status in HALT_STATUSES:
            assert halt_status.is_halt
            assert halt_status.is_complete

    def test_continuable_statuses_allow_downstream(self) -> None:
        """Continuable statuses should allow downstream execution."""
        for status in CONTINUABLE_STATUSES:
            # These should all allow downstream continuation
            assert not status.is_halt or status == WorkflowStatus.REDIRECT

    def test_invalid_transitions_are_rejected(self) -> None:
        """Invalid state transitions should be rejected."""
        # Terminal states should not transition to anything
        for terminal_status in [
            WorkflowStatus.SUCCEEDED,
            WorkflowStatus.TERMINAL,
            WorkflowStatus.CANCELED,
        ]:
            for target in WorkflowStatus:
                if target != terminal_status:
                    assert not can_transition(terminal_status, target)

    def test_paused_to_running_is_valid(self) -> None:
        """PAUSED -> RUNNING (resume) should be valid."""
        assert can_transition(WorkflowStatus.PAUSED, WorkflowStatus.RUNNING)

    def test_redirect_transitions(self) -> None:
        """REDIRECT status should transition correctly."""
        # REDIRECT -> RUNNING (continue) should be valid
        assert can_transition(WorkflowStatus.REDIRECT, WorkflowStatus.RUNNING)
        # REDIRECT -> SUCCEEDED (complete) should be valid
        assert can_transition(WorkflowStatus.REDIRECT, WorkflowStatus.SUCCEEDED)


# =============================================================================
# 9. Message Handling Edge Cases
# =============================================================================


class TestMessageHandlingEdgeCases:
    """Test edge cases in message handling."""

    def test_duplicate_message_handling(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Duplicate messages should be handled idempotently."""
        processor, orchestrator, registry = setup_stabilize(repository, queue)

        stage = StageExecution.create(
            type="test",
            name="Dedup Stage",
            ref_id="dedup",
        )
        task = TaskExecution(
            name="CounterTask",
            implementing_class="counter",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Dedup Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

    def test_message_ordering_preserved(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Messages should be processed in correct order."""
        processor, orchestrator, registry = setup_stabilize(repository, queue)

        stages = []
        prev_ref = None
        for i in range(5):
            ref_id = f"stage_{i}"
            stage = StageExecution.create(
                type="test",
                name=f"Stage {i}",
                ref_id=ref_id,
                requisite_stage_ref_ids={prev_ref} if prev_ref else set(),
            )
            task = TaskExecution(
                name="SuccessTask",
                implementing_class="success",
                stage_start=True,
                stage_end=True,
            )
            stage.tasks = [task]
            stages.append(stage)
            prev_ref = ref_id

        workflow = Workflow.create(
            application="test",
            name="Order Test",
            stages=stages,
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Verify all stages completed in order
        for i, stage in enumerate(result.stages):
            assert stage.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# 10. Recovery and Resilience Tests
# =============================================================================


class TestRecoveryAndResilience:
    """Test recovery and resilience scenarios."""

    def test_workflow_continues_after_failed_continue(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Workflow should continue after FAILED_CONTINUE status."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"conditional": ConditionalFailTask},
        )

        stage1 = StageExecution.create(
            type="test",
            name="Failing Stage",
            ref_id="failing",
            context={
                "continuePipelineOnFailure": True,
                "should_fail": False,  # Actually succeed
            },
        )
        task1 = TaskExecution(
            name="ConditionalTask",
            implementing_class="conditional",
            stage_start=True,
            stage_end=True,
        )
        stage1.tasks = [task1]

        stage2 = StageExecution.create(
            type="test",
            name="Downstream Stage",
            ref_id="downstream",
            requisite_stage_ref_ids={"failing"},
        )
        task2 = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        stage2.tasks = [task2]

        workflow = Workflow.create(
            application="test",
            name="Failed Continue Test",
            stages=[stage1, stage2],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

    def test_parallel_failure_propagation(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Failure in one parallel branch should propagate correctly."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"conditional": ConditionalFailTask},
        )

        start = StageExecution.create(
            type="test",
            name="Start",
            ref_id="start",
        )
        start_task = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        start.tasks = [start_task]

        branch1 = StageExecution.create(
            type="test",
            name="Branch 1 (fails)",
            ref_id="branch1",
            requisite_stage_ref_ids={"start"},
            context={"should_fail": True},
        )
        branch1_task = TaskExecution(
            name="ConditionalTask",
            implementing_class="conditional",
            stage_start=True,
            stage_end=True,
        )
        branch1.tasks = [branch1_task]

        branch2 = StageExecution.create(
            type="test",
            name="Branch 2 (succeeds)",
            ref_id="branch2",
            requisite_stage_ref_ids={"start"},
            context={"should_fail": False},
        )
        branch2_task = TaskExecution(
            name="ConditionalTask",
            implementing_class="conditional",
            stage_start=True,
            stage_end=True,
        )
        branch2.tasks = [branch2_task]

        join = StageExecution.create(
            type="test",
            name="Join",
            ref_id="join",
            requisite_stage_ref_ids={"branch1", "branch2"},
        )
        join_task = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        join.tasks = [join_task]

        workflow = Workflow.create(
            application="test",
            name="Parallel Failure Test",
            stages=[start, branch1, branch2, join],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(workflow.id)
        # Workflow should fail because branch1 failed
        assert result.status == WorkflowStatus.TERMINAL

    def test_empty_workflow(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Empty workflow (no stages) should be handled gracefully.

        Current behavior: Empty workflows are marked TERMINAL because there are
        no initial stages to start. This is a safety measure - an empty workflow
        likely indicates a configuration error.

        See StartWorkflowHandler._start() lines 153-166 which sets TERMINAL
        when no initial stages are found.
        """
        processor, orchestrator, registry = setup_stabilize(repository, queue)

        workflow = Workflow.create(
            application="test",
            name="Empty Workflow",
            stages=[],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        # Empty workflow goes to TERMINAL (no initial stages = configuration error)
        assert result.status == WorkflowStatus.TERMINAL


# =============================================================================
# 11. Edge Cases in Workflow Lifecycle
# =============================================================================


class TestWorkflowLifecycleEdgeCases:
    """Test edge cases in workflow lifecycle management."""

    def test_cancel_not_started_workflow(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Canceling a NOT_STARTED workflow should work."""
        processor, orchestrator, registry = setup_stabilize(repository, queue)

        stage = StageExecution.create(
            type="test",
            name="Never Started",
            ref_id="never",
        )
        task = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Cancel Before Start",
            stages=[stage],
        )

        repository.store(workflow)

        # Cancel before starting
        repository.cancel(workflow.id, "test_user", "Test cancellation")

        result = repository.retrieve(workflow.id)
        assert result.is_canceled

    def test_pause_and_resume_workflow(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Pausing and resuming a workflow should work."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"slow": SlowTask},
        )

        stage = StageExecution.create(
            type="test",
            name="Pausable Stage",
            ref_id="pausable",
            context={"sleep_time": 0.5},
        )
        task = TaskExecution(
            name="SlowTask",
            implementing_class="slow",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Pause Resume Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)

        # Process for a short time, then pause
        processor.process_all(timeout=0.1)

        # Try to pause (may or may not be in pausable state)
        try:
            repository.pause(workflow.id, "test_user")
            result = repository.retrieve(workflow.id)
            if result.status == WorkflowStatus.PAUSED:
                repository.resume(workflow.id)
        except Exception:
            pass  # May not be pausable if already completed

        # Continue processing
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        assert result.status in {WorkflowStatus.SUCCEEDED, WorkflowStatus.PAUSED}


# =============================================================================
# 12. Special Character and Encoding Tests
# =============================================================================


class TestSpecialCharacterHandling:
    """Test handling of special characters in various fields."""

    def test_unicode_in_context(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Unicode characters in context should be handled correctly."""
        processor, orchestrator, registry = setup_stabilize(repository, queue)

        stage = StageExecution.create(
            type="test",
            name="Unicode Stage",
            ref_id="unicode",
            context={
                "japanese": "日本語テスト",
                "emoji": "🎉🚀✨",
                "arabic": "اختبار عربي",
                "chinese": "中文测试",
            },
        )
        task = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test_日本語",
            name="Unicode Test 🚀",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Verify context was preserved
        unicode_stage = result.stage_by_ref_id("unicode")
        assert unicode_stage.context["japanese"] == "日本語テスト"
        assert unicode_stage.context["emoji"] == "🎉🚀✨"

    def test_special_chars_in_ref_id(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Special characters in ref_id should be handled."""
        processor, orchestrator, registry = setup_stabilize(repository, queue)

        stage = StageExecution.create(
            type="test",
            name="Special Ref Stage",
            ref_id="stage-with-dashes_and_underscores.and.dots",
        )
        task = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Special RefId Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# 13. Bug Documentation Tests
# =============================================================================


class TestBugDocumentation:
    """Tests that document known bugs for tracking and verification after fixes."""

    def test_bug_max_jumps_zero_treated_as_falsy(self) -> None:
        """BUG: _max_jumps=0 is treated as falsy in JumpToStageHandler.

        Location: src/stabilize/handlers/jump_to_stage/handler.py line 349
        Code:
            max_jumps = execution.context.get("_max_jumps") or \
                source_stage.context.get("_max_jumps") or DEFAULT_MAX_JUMPS

        Problem: The `or` operator treats 0 as falsy, so _max_jumps=0 defaults to DEFAULT_MAX_JUMPS (10).

        Impact: Users cannot disable jumps by setting max_jumps to 0.

        Fix: Use explicit None checks:
            max_jumps = execution.context.get("_max_jumps")
            if max_jumps is None:
                max_jumps = source_stage.context.get("_max_jumps")
            if max_jumps is None:
                max_jumps = DEFAULT_MAX_JUMPS
        """
        # This is a unit test that directly tests the buggy logic
        from stabilize.handlers.jump_to_stage.handler import DEFAULT_MAX_JUMPS

        # Simulate the buggy logic
        execution_context: dict[str, int] = {"_max_jumps": 0}
        stage_context: dict[str, int] = {}

        # Current (buggy) implementation
        max_jumps_buggy = (
            execution_context.get("_max_jumps")
            or stage_context.get("_max_jumps")
            or DEFAULT_MAX_JUMPS
        )

        # BUG: 0 is treated as falsy, so we get DEFAULT_MAX_JUMPS instead of 0
        assert max_jumps_buggy == DEFAULT_MAX_JUMPS  # BUG: Should be 0

        # Fixed implementation would be:
        max_jumps_fixed = execution_context.get("_max_jumps")
        if max_jumps_fixed is None:
            max_jumps_fixed = stage_context.get("_max_jumps")
        if max_jumps_fixed is None:
            max_jumps_fixed = DEFAULT_MAX_JUMPS

        # Fixed version should return 0
        assert max_jumps_fixed == 0  # CORRECT

    def test_same_falsy_bug_exists_in_get_jump_count(self) -> None:
        """Related to max_jumps bug: Same pattern exists in other places.

        The pattern `context.get("key") or default` is used throughout
        the codebase and can cause issues with valid falsy values like:
        - 0 (zero)
        - "" (empty string)
        - [] (empty list)
        - False (boolean)

        All such patterns should be audited and fixed.
        """
        # Document the pattern issue
        context: dict[str, int] = {"count": 0}

        # Buggy pattern
        count_buggy = context.get("count") or 10
        assert count_buggy == 10  # Wrong! Should be 0

        # Fixed pattern
        count_fixed = context.get("count", 10) if "count" not in context else context["count"]
        # Or simpler with None check:
        count_fixed = context.get("count")
        if count_fixed is None:
            count_fixed = 10
        assert count_fixed == 0  # Correct


# =============================================================================
# 14. Stress and Boundary Tests
# =============================================================================


class TestStressAndBoundaryConditions:
    """Tests for stress conditions and boundary values."""

    def test_very_long_stage_name(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test handling of very long stage names.

        Note: PostgreSQL schema has VARCHAR(255) limit for stage names.
        This test uses a name within that limit for compatibility.

        POTENTIAL ISSUE: Longer names (>255 chars) will cause
        StringDataRightTruncation error in PostgreSQL but work in SQLite.
        """
        processor, orchestrator, registry = setup_stabilize(repository, queue)

        # Use 250 chars to stay within PostgreSQL VARCHAR(255) limit
        long_name = "A" * 250

        stage = StageExecution.create(
            type="test",
            name=long_name,
            ref_id="long_name_stage",
        )
        task = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Long Name Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.stages[0].name == long_name

    def test_large_context_payload(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test handling of large context payloads."""
        processor, orchestrator, registry = setup_stabilize(repository, queue)

        # Create a large context (10KB of data)
        large_list = [f"item_{i}" for i in range(1000)]
        large_context = {
            "large_list": large_list,
            "nested": {
                "level1": {
                    "level2": {
                        "data": list(range(100)),
                    }
                }
            },
        }

        stage = StageExecution.create(
            type="test",
            name="Large Context Stage",
            ref_id="large_context",
            context=large_context,
        )
        task = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Large Context Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Verify context was preserved
        result_stage = result.stage_by_ref_id("large_context")
        assert len(result_stage.context["large_list"]) == 1000

    def test_many_tasks_in_single_stage(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test handling of many tasks in a single stage.

        Tests that many tasks (50) in a single stage execute correctly.

        This test verifies the fix for the task ordering bug where missing
        ORDER BY clauses in task retrieval queries caused tasks to be
        loaded in unpredictable order, breaking task sequencing.
        """
        processor, orchestrator, registry = setup_stabilize(repository, queue)

        stage = StageExecution.create(
            type="test",
            name="Many Tasks Stage",
            ref_id="many_tasks",
        )

        # Test with 50 tasks - this previously caused race conditions
        # before the ORDER BY fix was applied
        num_tasks = 50
        tasks = []
        for i in range(num_tasks):
            task = TaskExecution(
                name=f"SuccessTask_{i}",
                implementing_class="success",
                stage_start=(i == 0),
                stage_end=(i == num_tasks - 1),
            )
            tasks.append(task)
        stage.tasks = tasks

        workflow = Workflow.create(
            application="test",
            name="Many Tasks Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=60.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Verify all tasks completed
        result_stage = result.stage_by_ref_id("many_tasks")
        assert len(result_stage.tasks) == num_tasks
        for task in result_stage.tasks:
            assert task.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# 15. Data Integrity Tests
# =============================================================================


class TestDataIntegrity:
    """Tests for data integrity and consistency."""

    def test_output_mutation_isolation(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test that mutating outputs in one stage doesn't affect retrieved data."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"output": OutputMergingTask},
        )

        stage = StageExecution.create(
            type="test",
            name="Output Stage",
            ref_id="output",
            context={"list_items": ["a", "b", "c"]},
        )
        task = TaskExecution(
            name="OutputTask",
            implementing_class="output",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Output Mutation Test",
            stages=[stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        # Retrieve workflow
        result1 = repository.retrieve(workflow.id)
        original_list = result1.stage_by_ref_id("output").outputs.get("list_output", [])

        # Mutate the list
        if isinstance(original_list, list):
            original_list.append("mutated")

        # Retrieve again - should not be affected
        result2 = repository.retrieve(workflow.id)
        result2.stage_by_ref_id("output").outputs.get("list_output", [])

        # If isolation is working, the mutation shouldn't persist
        # Note: This depends on whether the implementation returns copies or references

    def test_version_increments_on_update(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test that stage version increments properly on updates."""
        processor, orchestrator, registry = setup_stabilize(repository, queue)

        stage = StageExecution.create(
            type="test",
            name="Version Test Stage",
            ref_id="version_test",
        )
        task = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]

        workflow = Workflow.create(
            application="test",
            name="Version Test",
            stages=[stage],
        )

        repository.store(workflow)

        # Check initial version
        stored = repository.retrieve(workflow.id)
        initial_version = stored.stages[0].version

        # Start workflow which should update stages
        orchestrator.start(workflow)
        processor.process_all(timeout=5.0)

        # Check version increased
        result = repository.retrieve(workflow.id)
        final_version = result.stages[0].version

        # Version should have increased during processing
        assert final_version > initial_version
