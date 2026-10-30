from __future__ import annotations

from stabilize import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize

# =============================================================================
# Bug Hunt Test: Empty Workflow Handling
# =============================================================================


class TestEmptyWorkflowHandling:
    """
    BUG HYPOTHESIS: Empty workflows may not complete correctly.
    """

    def test_workflow_with_no_stages_completes(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test that a workflow with no stages completes immediately."""
        processor, runner, task_registry = setup_stabilize(repository, queue)

        workflow = Workflow.create(
            application="airport",
            name="empty-workflow",
            stages=[],
        )

        repository.store(workflow)
        runner.start(workflow)

        for _ in range(20):
            if not processor.process_one():
                break

        result = repository.retrieve(workflow.id)
        assert result.status.is_complete


# =============================================================================
# Bug Hunt Test: Unicode and Special Characters
# =============================================================================


class TestUnicodeAndSpecialCharacters:
    """
    BUG HYPOTHESIS: Unicode characters may cause serialization issues.
    """

    def test_unicode_in_stage_context_preserved(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test that unicode characters are preserved in stage context."""
        workflow = Workflow.create(
            application="airport",
            name="unicode-test",
            stages=[
                StageExecution(
                    ref_id="unicode",
                    name="Unicode Stage",
                    context={
                        "japanese": "こんにちは",
                        "emoji": "👋🌍",
                        "greek": "αβγδ",
                    },
                )
            ],
        )

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        unicode_stage = result.stage_by_ref_id("unicode")
        assert unicode_stage.context.get("japanese") == "こんにちは"
        assert unicode_stage.context.get("emoji") == "👋🌍"
        assert unicode_stage.context.get("greek") == "αβγδ"


# =============================================================================
# Bug Hunt Test: Timestamp Edge Cases
# =============================================================================


class TestTimestampEdgeCases:
    """
    BUG HYPOTHESIS: Edge values of timestamps may cause issues.
    """

    def test_zero_timestamp_handled(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test that zero timestamps are handled correctly."""
        stage = StageExecution(
            ref_id="test",
            name="Test Stage",
            start_time=0,
            end_time=0,
        )

        workflow = Workflow.create(
            application="airport",
            name="zero-timestamp",
            stages=[stage],
        )
        workflow.start_time = 0

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        assert result.start_time == 0
        stage_result = result.stage_by_ref_id("test")
        assert stage_result.start_time == 0

    def test_very_large_timestamp_handled(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test that very large timestamps are handled."""
        future_time = 32503680000000  # Year 3000

        workflow = Workflow.create(
            application="airport",
            name="future-timestamp",
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                    scheduled_time=future_time,
                )
            ],
        )

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        stage_result = result.stage_by_ref_id("test")
        assert stage_result.scheduled_time == future_time


# =============================================================================
# Bug Hunt Test: Zombie Stage Detection
# =============================================================================


class TestZombieStageDetection:
    """
    BUG HYPOTHESIS: Stages may get stuck in RUNNING state indefinitely.
    """

    def test_running_stage_with_completed_tasks(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test detection of stages stuck in RUNNING without active processing."""
        task = TaskExecution.create(
            name="completed_task",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        task.status = WorkflowStatus.SUCCEEDED

        stage = StageExecution(
            ref_id="zombie",
            name="Zombie Stage",
            status=WorkflowStatus.RUNNING,
            tasks=[task],
        )

        workflow = Workflow.create(
            application="airport",
            name="zombie-test",
            stages=[stage],
        )

        repository.store(workflow)

        result = repository.retrieve(workflow.id)
        zombie_stage = result.stage_by_ref_id("zombie")
        determined_status = zombie_stage.determine_status()
        assert determined_status == WorkflowStatus.SUCCEEDED
