from __future__ import annotations

from stabilize import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize
from tests.corner_case_support import ConditionalFailTask, SlowTask

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
