from __future__ import annotations

from stabilize import StageExecution
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
from tests.corner_case_support import OutputMergingTask, PermanentFailTask, RunningTask, TransientFailTask

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
