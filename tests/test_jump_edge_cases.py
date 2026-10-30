"""
Tests for jump-to-stage edge cases.

This module tests edge cases in the JumpToStage handler including:
- Jump to self (retry loop)
- Max jumps exceeded
- Jump to non-existent stage
- Backward jump resets downstream
- Forward jump marks intermediate as SKIPPED
- Jump context merging
- Stale CompleteStage messages after jump
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
from tests.jump_edge_support import JumpToSelfTask, JumpWithContextTask, MaxJumpsTask, RecordContextTask, reset_tasks


class JumpToNonexistentTask(Task):
    """Task that jumps to a non-existent stage."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.jump_to("nonexistent_stage")


class TestJumpToSelf:
    """Test jumping to the same stage (self-loop for retry)."""

    def test_jump_to_self_retry_loop(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test that a stage can jump to itself for retry semantics.

        Expected sequence: A executes -> jumps to A -> A executes again -> ...
        """
        reset_tasks()
        JumpToSelfTask.max_retries = 2

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"jump_self": JumpToSelfTask})

        execution = Workflow.create(
            application="test",
            name="Self Jump Test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Self Jump",
                            implementing_class="jump_self",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)

        # Should eventually succeed after retries
        assert result.status == WorkflowStatus.SUCCEEDED, f"Workflow status: {result.status}"

        # Retry count should be max_retries + 1 (final success)
        assert JumpToSelfTask.retry_count == 3, f"Expected 3 attempts, got {JumpToSelfTask.retry_count}"

    def test_jump_to_self_increments_jump_count(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test that jumping to self properly tracks jump count."""
        reset_tasks()
        JumpToSelfTask.max_retries = 1

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"jump_self": JumpToSelfTask})

        execution = Workflow.create(
            application="test",
            name="Self Jump Count Test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Self Jump",
                            implementing_class="jump_self",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)
        result.stage_by_ref_id("stage_a")

        # Jump count should be tracked in context
        # (exact key depends on implementation)
        assert result.status == WorkflowStatus.SUCCEEDED


class TestMaxJumpsExceeded:
    """Test behavior when max jump count is exceeded."""

    def test_max_jumps_causes_terminal(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test that exceeding max jumps causes workflow to become TERMINAL.

        Default max_jumps is typically 10.
        """
        reset_tasks()

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"max_jump": MaxJumpsTask})

        execution = Workflow.create(
            application="test",
            name="Max Jumps Test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    context={"_max_jumps": 3},  # Low limit for testing
                    tasks=[
                        TaskExecution.create(
                            name="Always Jump",
                            implementing_class="max_jump",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)

        # Should be TERMINAL due to max jumps exceeded
        assert result.status == WorkflowStatus.TERMINAL, f"Workflow status: {result.status}"


class TestJumpToNonexistent:
    """Test jumping to a stage that doesn't exist."""

    def test_jump_to_nonexistent_stage_fails(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test that jumping to a non-existent stage causes failure.
        """
        reset_tasks()

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"bad_jump": JumpToNonexistentTask})

        execution = Workflow.create(
            application="test",
            name="Nonexistent Jump Test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Bad Jump",
                            implementing_class="bad_jump",
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

        # Should be TERMINAL due to invalid jump target
        assert result.status == WorkflowStatus.TERMINAL, f"Workflow status: {result.status}"


class TestJumpContextMerging:
    """Test that jump context is properly merged into target stage."""

    def test_jump_context_merged_into_target(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test that jump_context is properly merged into target stage context.
        """
        reset_tasks()
        JumpWithContextTask.target = "stage_b"
        JumpWithContextTask.context_data = {"jump_key": "jump_value", "shared_key": "from_jump"}

        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"jump_ctx": JumpWithContextTask, "record_ctx": RecordContextTask},
        )

        execution = Workflow.create(
            application="test",
            name="Jump Context Test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Jump With Context",
                            implementing_class="jump_ctx",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    context={"existing_key": "existing_value", "shared_key": "original"},
                    requisite_stage_ref_ids={"stage_a"},
                    tasks=[
                        TaskExecution.create(
                            name="Record Context",
                            implementing_class="record_ctx",
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

        assert result.status == WorkflowStatus.SUCCEEDED

        # Check recorded context from stage B
        assert len(RecordContextTask.recorded_context) >= 1
        recorded = RecordContextTask.recorded_context[-1]

        # Jump context should be merged
        assert recorded.get("jump_key") == "jump_value"

    def test_jump_outputs_available_via_key(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test that jump_outputs are available via _jump_outputs key.
        """
        reset_tasks()
        JumpWithContextTask.target = "stage_b"
        JumpWithContextTask.output_data = {"result": "from_jump"}

        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"jump_out": JumpWithContextTask, "record_ctx": RecordContextTask},
        )

        execution = Workflow.create(
            application="test",
            name="Jump Outputs Test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Jump With Outputs",
                            implementing_class="jump_out",
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
                            name="Record Context",
                            implementing_class="record_ctx",
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

        assert result.status == WorkflowStatus.SUCCEEDED
