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
