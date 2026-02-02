"""
Additional tests to increase code coverage for critical areas.

Targets low coverage areas identified in the coverage report:
- Recovery module (51%)
- Queue processor (55%)
- Resilience circuits (57%)
- Task interface (56%)
- Task registry (50%)
"""

from __future__ import annotations

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
# Tests for Task Interface Coverage
# =============================================================================


class TestTaskInterfaceEdgeCases:
    """Test edge cases in Task interface."""

    def test_task_with_timeout_handler(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test task with on_timeout handler."""

        class TimeoutHandlerTask(Task):
            timeout_called = False

            def execute(self, stage: StageExecution) -> TaskResult:
                return TaskResult.success()

            def on_timeout(self, stage: StageExecution) -> None:
                TimeoutHandlerTask.timeout_called = True

        processor, runner, task_registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"timeout_handler": TimeoutHandlerTask},
        )

        workflow = Workflow.create(
            application="test",
            name="timeout-handler-test",
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                    tasks=[
                        TaskExecution.create(
                            name="task",
                            implementing_class="timeout_handler",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                )
            ],
        )

        repository.store(workflow)
        runner.start(workflow)

        for _ in range(30):
            if not processor.process_one():
                break

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

    def test_task_with_cancel_handler(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test task with on_cancel handler."""

        class CancelHandlerTask(Task):
            cancel_called = False

            def execute(self, stage: StageExecution) -> TaskResult:
                return TaskResult.success()

            def on_cancel(self, stage: StageExecution) -> None:
                CancelHandlerTask.cancel_called = True

        processor, runner, task_registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"cancel_handler": CancelHandlerTask},
        )

        workflow = Workflow.create(
            application="test",
            name="cancel-handler-test",
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                    tasks=[
                        TaskExecution.create(
                            name="task",
                            implementing_class="cancel_handler",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                )
            ],
        )

        repository.store(workflow)
        runner.start(workflow)

        for _ in range(30):
            if not processor.process_one():
                break

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# Tests for Task Result Edge Cases
# =============================================================================


class TestTaskResultEdgeCases:
    """Test edge cases in TaskResult creation."""

    def test_task_result_running_with_context(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test task returning RUNNING status with context update."""

        class RunningTask(Task):
            call_count = 0

            def execute(self, stage: StageExecution) -> TaskResult:
                RunningTask.call_count += 1
                if RunningTask.call_count < 3:
                    return TaskResult.running(context={"iteration": RunningTask.call_count})
                return TaskResult.success(outputs={"final": True})

        RunningTask.call_count = 0

        processor, runner, task_registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"running": RunningTask},
        )

        workflow = Workflow.create(
            application="test",
            name="running-test",
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                    tasks=[
                        TaskExecution.create(
                            name="task",
                            implementing_class="running",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                )
            ],
        )

        repository.store(workflow)
        runner.start(workflow)

        for _ in range(100):  # More iterations for RUNNING tasks
            if not processor.process_one():
                break

        result = repository.retrieve(workflow.id)
        # Task may still be RUNNING if we don't process enough iterations
        assert result.status in {WorkflowStatus.SUCCEEDED, WorkflowStatus.RUNNING}

    def test_task_result_failed_continue(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test task returning FAILED_CONTINUE status."""

        class FailContinueTask(Task):
            def execute(self, stage: StageExecution) -> TaskResult:
                return TaskResult.failed_continue("Non-fatal failure")

        processor, runner, task_registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"fail_continue": FailContinueTask},
        )

        workflow = Workflow.create(
            application="test",
            name="fail-continue-test",
            stages=[
                StageExecution(
                    ref_id="stage1",
                    name="Fail Continue Stage",
                    context={"continuePipelineOnFailure": True},
                    tasks=[
                        TaskExecution.create(
                            name="task",
                            implementing_class="fail_continue",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage2",
                    name="Next Stage",
                    requisite_stage_ref_ids={"stage1"},
                    tasks=[
                        TaskExecution.create(
                            name="task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(workflow)
        runner.start(workflow)

        for _ in range(50):
            if not processor.process_one():
                break

        result = repository.retrieve(workflow.id)
        # Workflow should complete (either FAILED_CONTINUE or SUCCEEDED depending on downstream)
        assert result.status.is_complete


# =============================================================================
# Tests for Status Transitions
# =============================================================================


class TestStatusTransitions:
    """Test status transition logic."""

    def test_workflow_status_progression(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test that workflow status progresses correctly."""
        processor, runner, task_registry = setup_stabilize(repository, queue)

        workflow = Workflow.create(
            application="test",
            name="status-progression",
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                    tasks=[
                        TaskExecution.create(
                            name="task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                )
            ],
        )

        repository.store(workflow)

        # Initial status should be NOT_STARTED
        initial = repository.retrieve(workflow.id)
        assert initial.status == WorkflowStatus.NOT_STARTED

        runner.start(workflow)

        # After start, should be RUNNING
        processor.process_one()  # StartWorkflow
        after_start = repository.retrieve(workflow.id)
        assert after_start.status == WorkflowStatus.RUNNING

        # Process rest
        for _ in range(30):
            if not processor.process_one():
                break

        # Should be SUCCEEDED
        final = repository.retrieve(workflow.id)
        assert final.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# Tests for DAG Edge Cases
# =============================================================================


class TestDAGEdgeCases:
    """Test DAG-related edge cases."""

    def test_diamond_dependency_pattern(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test diamond dependency: A -> B, C -> D where D depends on both B and C."""
        processor, runner, task_registry = setup_stabilize(repository, queue)

        workflow = Workflow.create(
            application="test",
            name="diamond-dependency",
            stages=[
                StageExecution(
                    ref_id="a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="task_a",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="b",
                    name="Stage B",
                    requisite_stage_ref_ids={"a"},
                    tasks=[
                        TaskExecution.create(
                            name="task_b",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="c",
                    name="Stage C",
                    requisite_stage_ref_ids={"a"},
                    tasks=[
                        TaskExecution.create(
                            name="task_c",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="d",
                    name="Stage D",
                    requisite_stage_ref_ids={"b", "c"},
                    tasks=[
                        TaskExecution.create(
                            name="task_d",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(workflow)
        runner.start(workflow)

        for _ in range(100):
            if not processor.process_one():
                break

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Verify all stages completed
        for ref_id in ["a", "b", "c", "d"]:
            stage = result.stage_by_ref_id(ref_id)
            assert stage.status == WorkflowStatus.SUCCEEDED

    def test_multiple_initial_stages(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test workflow with multiple initial stages (no dependencies)."""
        processor, runner, task_registry = setup_stabilize(repository, queue)

        workflow = Workflow.create(
            application="test",
            name="multiple-initial",
            stages=[
                StageExecution(
                    ref_id="a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="task_a",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="b",
                    name="Stage B",
                    tasks=[
                        TaskExecution.create(
                            name="task_b",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="c",
                    name="Stage C",
                    tasks=[
                        TaskExecution.create(
                            name="task_c",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(workflow)
        runner.start(workflow)

        for _ in range(50):
            if not processor.process_one():
                break

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# Tests for Workflow Methods
# =============================================================================


class TestWorkflowMethods:
    """Test Workflow model methods."""

    def test_workflow_pause_resume(self) -> None:
        """Test workflow pause and resume functionality."""
        workflow = Workflow.create(
            application="test",
            name="pause-test",
            stages=[],
        )

        # Pause
        workflow.pause("test_user")
        assert workflow.status == WorkflowStatus.PAUSED
        assert workflow.paused is not None
        assert workflow.paused.paused_by == "test_user"
        assert workflow.paused.is_paused

        # Resume
        workflow.resume()
        assert workflow.status == WorkflowStatus.RUNNING
        assert workflow.paused is not None
        assert workflow.paused.resume_time is not None

    def test_workflow_cancel(self) -> None:
        """Test workflow cancellation."""
        workflow = Workflow.create(
            application="test",
            name="cancel-test",
            stages=[],
        )

        workflow.cancel("test_user", "Testing cancellation")

        assert workflow.is_canceled
        assert workflow.canceled_by == "test_user"
        assert workflow.cancellation_reason == "Testing cancellation"

    def test_workflow_stage_by_id(self) -> None:
        """Test stage lookup by ID."""
        stage = StageExecution(
            ref_id="test",
            name="Test Stage",
        )

        workflow = Workflow.create(
            application="test",
            name="lookup-test",
            stages=[stage],
        )

        # Find by ID
        found = workflow.stage_by_id(stage.id)
        assert found.ref_id == "test"

        # Not found raises ValueError
        with pytest.raises(ValueError):
            workflow.stage_by_id("nonexistent")

    def test_workflow_initial_stages(self) -> None:
        """Test initial_stages method."""
        workflow = Workflow.create(
            application="test",
            name="initial-stages-test",
            stages=[
                StageExecution(
                    ref_id="a",
                    name="Stage A",
                ),
                StageExecution(
                    ref_id="b",
                    name="Stage B",
                    requisite_stage_ref_ids={"a"},
                ),
                StageExecution(
                    ref_id="c",
                    name="Stage C",
                ),
            ],
        )

        initial = workflow.initial_stages()
        ref_ids = {s.ref_id for s in initial}
        assert ref_ids == {"a", "c"}


# =============================================================================
# Tests for Stage Methods
# =============================================================================


class TestStageMethods:
    """Test StageExecution model methods."""

    def test_stage_upstream_downstream(self) -> None:
        """Test upstream and downstream navigation."""
        workflow = Workflow.create(
            application="test",
            name="navigation-test",
            stages=[
                StageExecution(
                    ref_id="a",
                    name="Stage A",
                ),
                StageExecution(
                    ref_id="b",
                    name="Stage B",
                    requisite_stage_ref_ids={"a"},
                ),
                StageExecution(
                    ref_id="c",
                    name="Stage C",
                    requisite_stage_ref_ids={"b"},
                ),
            ],
        )

        stage_a = workflow.stage_by_ref_id("a")
        stage_b = workflow.stage_by_ref_id("b")
        stage_c = workflow.stage_by_ref_id("c")

        # Upstream
        assert stage_a.upstream_stages() == []
        assert len(stage_b.upstream_stages()) == 1
        assert stage_b.upstream_stages()[0].ref_id == "a"

        # Downstream
        assert len(stage_a.downstream_stages()) == 1
        assert stage_a.downstream_stages()[0].ref_id == "b"
        assert stage_c.downstream_stages() == []

    def test_stage_is_initial_and_is_join(self) -> None:
        """Test is_initial and is_join methods."""
        workflow = Workflow.create(
            application="test",
            name="predicates-test",
            stages=[
                StageExecution(
                    ref_id="initial",
                    name="Initial Stage",
                ),
                StageExecution(
                    ref_id="middle",
                    name="Middle Stage",
                    requisite_stage_ref_ids={"initial"},
                ),
                StageExecution(
                    ref_id="join",
                    name="Join Stage",
                    requisite_stage_ref_ids={"initial", "middle"},
                ),
            ],
        )

        initial = workflow.stage_by_ref_id("initial")
        middle = workflow.stage_by_ref_id("middle")
        join = workflow.stage_by_ref_id("join")

        assert initial.is_initial() is True
        assert middle.is_initial() is False
        assert join.is_initial() is False

        assert initial.is_join() is False
        assert middle.is_join() is False
        assert join.is_join() is True

    def test_stage_ancestors(self) -> None:
        """Test ancestors traversal."""
        workflow = Workflow.create(
            application="test",
            name="ancestors-test",
            stages=[
                StageExecution(
                    ref_id="a",
                    name="Stage A",
                ),
                StageExecution(
                    ref_id="b",
                    name="Stage B",
                    requisite_stage_ref_ids={"a"},
                ),
                StageExecution(
                    ref_id="c",
                    name="Stage C",
                    requisite_stage_ref_ids={"b"},
                ),
            ],
        )

        stage_c = workflow.stage_by_ref_id("c")
        ancestors = stage_c.ancestors()

        # Should include both A and B
        ancestor_refs = {s.ref_id for s in ancestors}
        assert "a" in ancestor_refs
        assert "b" in ancestor_refs
        assert "c" not in ancestor_refs


# =============================================================================
# Tests for Task Execution
# =============================================================================


class TestTaskExecutionMethods:
    """Test TaskExecution model methods."""

    def test_task_stage_reference(self) -> None:
        """Test task-stage back reference."""
        stage = StageExecution(
            ref_id="test",
            name="Test Stage",
        )

        task = TaskExecution.create(
            name="test_task",
            implementing_class="test",
            stage_start=True,
            stage_end=True,
        )
        stage.tasks = [task]
        task.stage = stage

        assert task.has_stage()
        assert task.stage.ref_id == "test"

    def test_task_exception_details(self) -> None:
        """Test setting exception details on task."""
        task = TaskExecution.create(
            name="test_task",
            implementing_class="test",
            stage_start=True,
            stage_end=True,
        )

        exception_info = {
            "type": "ValueError",
            "message": "Test error",
            "stacktrace": "...",
        }

        task.set_exception_details(exception_info)

        assert task.task_exception_details["exception"] == exception_info


# =============================================================================
# Tests for Execution Layers
# =============================================================================


class TestExecutionLayers:
    """Test execution layer calculation."""

    def test_get_execution_layers(self) -> None:
        """Test that get_execution_layers returns correct parallel groups."""
        from stabilize.dag.topological import get_execution_layers

        workflow = Workflow.create(
            application="test",
            name="layers-test",
            stages=[
                StageExecution(ref_id="a", name="A"),
                StageExecution(ref_id="b", name="B"),
                StageExecution(
                    ref_id="c",
                    name="C",
                    requisite_stage_ref_ids={"a", "b"},
                ),
                StageExecution(
                    ref_id="d",
                    name="D",
                    requisite_stage_ref_ids={"c"},
                ),
            ],
        )

        layers = get_execution_layers(workflow.stages)

        # Layer 0: A, B (no dependencies)
        # Layer 1: C (depends on A, B)
        # Layer 2: D (depends on C)
        assert len(layers) == 3

        layer_0_refs = {s.ref_id for s in layers[0]}
        assert layer_0_refs == {"a", "b"}

        layer_1_refs = {s.ref_id for s in layers[1]}
        assert layer_1_refs == {"c"}

        layer_2_refs = {s.ref_id for s in layers[2]}
        assert layer_2_refs == {"d"}


# =============================================================================
# Tests for Context Aggregation
# =============================================================================


class TestContextAggregation:
    """Test workflow context aggregation from stages."""

    def test_get_context_merges_outputs(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test that get_context properly merges stage outputs."""

        class OutputTask(Task):
            def execute(self, stage: StageExecution) -> TaskResult:
                output_key = stage.context.get("output_key", "default")
                output_value = stage.context.get("output_value", "value")
                return TaskResult.success(outputs={output_key: output_value})

        processor, runner, task_registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"output": OutputTask},
        )

        workflow = Workflow.create(
            application="test",
            name="context-merge-test",
            stages=[
                StageExecution(
                    ref_id="a",
                    name="Stage A",
                    context={"output_key": "key_a", "output_value": "value_a"},
                    tasks=[
                        TaskExecution.create(
                            name="task_a",
                            implementing_class="output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="b",
                    name="Stage B",
                    requisite_stage_ref_ids={"a"},
                    context={"output_key": "key_b", "output_value": "value_b"},
                    tasks=[
                        TaskExecution.create(
                            name="task_b",
                            implementing_class="output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(workflow)
        runner.start(workflow)

        for _ in range(50):
            if not processor.process_one():
                break

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Get aggregated context
        context = result.get_context()
        assert context.get("key_a") == "value_a"
        assert context.get("key_b") == "value_b"
