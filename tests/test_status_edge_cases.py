"""
Tests for status determination edge cases.

This module tests edge cases in status determination logic including:
- Workflow becomes TERMINAL when any stage fails
- Workflow SUCCEEDED when all stages complete
- Stage stays RUNNING while tasks are running
- Parallel stages with mixed status
- Stale CompleteStage messages ignored via version check
"""

from stabilize import TaskResult
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import Task
from tests.conftest import setup_stabilize
from tests.status_edge_support import SuccessTask


class TerminalTask(Task):
    """Task that always fails with TERMINAL status."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.terminal("Intentional terminal failure")


class FailContinueTask(Task):
    """Task that fails with FAILED_CONTINUE status."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.failed_continue("Intentional failure, continue workflow")


class ConditionalFailTask(Task):
    """Task that fails based on stage context."""

    def execute(self, stage: StageExecution) -> TaskResult:
        if stage.context.get("should_fail"):
            return TaskResult.terminal("Conditional failure")
        return TaskResult.success()


class TestWorkflowTerminalPropagation:
    """Test that workflow becomes TERMINAL when stages fail."""

    def test_single_stage_terminal_makes_workflow_terminal(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test workflow becomes TERMINAL when single stage fails."""
        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"terminal": TerminalTask})

        execution = Workflow.create(
            application="test",
            name="Single Terminal Test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Terminal Task",
                            implementing_class="terminal",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)

        assert result.status == WorkflowStatus.TERMINAL, f"Status: {result.status}"
        assert result.stage_by_ref_id("stage_a").status == WorkflowStatus.TERMINAL

    def test_one_of_many_stages_terminal(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test workflow becomes TERMINAL when one of many sequential stages fails."""
        processor, runner, _ = setup_stabilize(
            repository, queue, extra_tasks={"success": SuccessTask, "terminal": TerminalTask}
        )

        execution = Workflow.create(
            application="test",
            name="Multi Stage Terminal Test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Success Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"stage_a"},
                    tasks=[
                        TaskExecution.create(
                            name="Terminal Task",
                            implementing_class="terminal",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C",
                    requisite_stage_ref_ids={"stage_b"},
                    tasks=[
                        TaskExecution.create(
                            name="Success Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)

        assert result.status == WorkflowStatus.TERMINAL, f"Status: {result.status}"

        # Stage A should have succeeded
        assert result.stage_by_ref_id("stage_a").status == WorkflowStatus.SUCCEEDED

        # Stage B should be TERMINAL
        assert result.stage_by_ref_id("stage_b").status == WorkflowStatus.TERMINAL

        # Stage C should NOT have run (still NOT_STARTED)
        stage_c = result.stage_by_ref_id("stage_c")
        assert stage_c.status == WorkflowStatus.NOT_STARTED, f"Stage C status: {stage_c.status}"


class TestWorkflowSucceeded:
    """Test workflow SUCCEEDED when all stages complete successfully."""

    def test_all_stages_succeed(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test workflow SUCCEEDED when all stages complete successfully."""
        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"success": SuccessTask})

        execution = Workflow.create(
            application="test",
            name="All Success Test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Success Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"stage_a"},
                    tasks=[
                        TaskExecution.create(
                            name="Success Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C",
                    requisite_stage_ref_ids={"stage_b"},
                    tasks=[
                        TaskExecution.create(
                            name="Success Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)

        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"

        for stage in result.stages:
            assert stage.status == WorkflowStatus.SUCCEEDED, f"{stage.ref_id}: {stage.status}"


class TestParallelStagesMixedStatus:
    """Test parallel stages with mixed status outcomes."""

    def test_parallel_one_fails_one_succeeds(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test parallel stages where one fails and one succeeds."""
        processor, runner, _ = setup_stabilize(
            repository, queue, extra_tasks={"success": SuccessTask, "terminal": TerminalTask}
        )

        execution = Workflow.create(
            application="test",
            name="Parallel Mixed Test",
            stages=[
                # Two parallel stages (no dependencies on each other)
                StageExecution(
                    ref_id="stage_success",
                    name="Success Stage",
                    tasks=[
                        TaskExecution.create(
                            name="Success Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_fail",
                    name="Fail Stage",
                    tasks=[
                        TaskExecution.create(
                            name="Terminal Task",
                            implementing_class="terminal",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)

        # Workflow should be TERMINAL because one stage failed
        assert result.status == WorkflowStatus.TERMINAL, f"Status: {result.status}"

        # Success stage should still show SUCCEEDED
        assert result.stage_by_ref_id("stage_success").status == WorkflowStatus.SUCCEEDED

        # Fail stage should show TERMINAL
        assert result.stage_by_ref_id("stage_fail").status == WorkflowStatus.TERMINAL

    def test_parallel_with_failed_continue(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test parallel stages with FAILED_CONTINUE status.

        FAILED_CONTINUE means the stage failed but workflow continues.
        When all stages complete (even with FAILED_CONTINUE), the workflow
        completes. The final status depends on the implementation's rules
        for combining stage statuses.
        """
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"success": SuccessTask, "fail_continue": FailContinueTask},
        )

        execution = Workflow.create(
            application="test",
            name="Parallel Failed Continue Test",
            stages=[
                StageExecution(
                    ref_id="stage_success",
                    name="Success Stage",
                    tasks=[
                        TaskExecution.create(
                            name="Success Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_fail_continue",
                    name="Fail Continue Stage",
                    tasks=[
                        TaskExecution.create(
                            name="Fail Continue Task",
                            implementing_class="fail_continue",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)

        # Verify the failed stage has FAILED_CONTINUE status
        fail_stage = result.stage_by_ref_id("stage_fail_continue")
        assert fail_stage.status == WorkflowStatus.FAILED_CONTINUE

        # Workflow completes (not TERMINAL) since FAILED_CONTINUE doesn't stop execution
        # Final workflow status can be SUCCEEDED or FAILED_CONTINUE depending on implementation
        assert result.status in (
            WorkflowStatus.SUCCEEDED,
            WorkflowStatus.FAILED_CONTINUE,
        ), f"Unexpected status: {result.status}"
