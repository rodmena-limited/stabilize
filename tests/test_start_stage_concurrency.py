"""Tests for StartStageHandler concurrency handling.

These tests verify that the StartStageHandler correctly handles race conditions
when multiple StartStage messages arrive for the same stage simultaneously
(e.g., when multiple upstream stages complete around the same time).
"""

from unittest.mock import MagicMock, patch

from stabilize import (
    StageExecution,
    StartStageHandler,
    TaskExecution,
    Workflow,
    WorkflowStatus,
)
from stabilize.errors import ConcurrencyError
from stabilize.persistence.memory import InMemoryWorkflowStore
from stabilize.queue import InMemoryQueue
from stabilize.queue.messages import StartStage


class TestStartStageConcurrencyHandling:
    """Test that StartStageHandler handles ConcurrencyError gracefully."""

    def test_concurrent_start_stage_handles_optimistic_lock_failure(self) -> None:
        """Test that ConcurrencyError is caught and handled gracefully.

        This simulates the race condition where multiple StartStage messages
        arrive for the same stage (due to multiple upstream stages completing),
        and one handler wins while others fail with ConcurrencyError.
        """
        queue = InMemoryQueue()
        repository = InMemoryWorkflowStore()

        # Create a diamond pipeline: A -> [B, C] -> D
        # Stage D has two upstream stages, so when both B and C complete,
        # two StartStage messages for D may be processed concurrently
        execution = Workflow.create(
            application="test",
            name="Diamond Pipeline",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="test",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="b",
                    type="test",
                    name="Stage B",
                    requisite_stage_ref_ids={"a"},
                    tasks=[
                        TaskExecution.create(
                            name="Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="c",
                    type="test",
                    name="Stage C",
                    requisite_stage_ref_ids={"a"},
                    tasks=[
                        TaskExecution.create(
                            name="Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="d",
                    type="test",
                    name="Stage D",
                    requisite_stage_ref_ids={"b", "c"},
                    tasks=[
                        TaskExecution.create(
                            name="Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
            ],
        )

        repository.store(execution)

        # Mark upstream stages as complete so stage D is ready to start
        for stage in execution.stages:
            if stage.ref_id in {"a", "b", "c"}:
                stage.status = WorkflowStatus.SUCCEEDED
                repository.store_stage(stage)

        handler = StartStageHandler(queue, repository)

        # Get stage D
        stage_d = next(s for s in execution.stages if s.ref_id == "d")

        # Create a StartStage message for stage D
        message = StartStage(
            execution_type="Workflow",
            execution_id=execution.id,
            stage_id=stage_d.id,
        )

        # Mock the transaction to raise ConcurrencyError (simulating another
        # handler already updated the stage)
        def mock_transaction_raises_concurrency_error(queue: InMemoryQueue) -> MagicMock:
            """Create a mock transaction that raises ConcurrencyError on store_stage."""
            mock_txn = MagicMock()
            mock_txn.__enter__ = MagicMock(return_value=mock_txn)
            mock_txn.__exit__ = MagicMock(return_value=False)
            mock_txn.store_stage = MagicMock(
                side_effect=ConcurrencyError("Optimistic lock failed for stage (version 0)")
            )
            return mock_txn

        with patch.object(repository, "transaction", mock_transaction_raises_concurrency_error):
            # This should NOT raise an exception - it should handle gracefully
            handler.handle(message)

        # Verify no exception was raised and handler completed
        # The stage should still be NOT_STARTED since the mock prevented the update
        # (in real scenario, another handler would have updated it)

    def test_start_stage_skips_already_running_stage(self) -> None:
        """Test that StartStage is skipped if stage is already running.

        This is the fast-path check before the transaction, which catches
        most duplicate messages.
        """
        queue = InMemoryQueue()
        repository = InMemoryWorkflowStore()

        execution = Workflow.create(
            application="test",
            name="Simple Pipeline",
            stages=[
                StageExecution(
                    ref_id="1",
                    type="test",
                    name="Test Stage",
                    tasks=[
                        TaskExecution.create(
                            name="Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
            ],
        )

        repository.store(execution)

        # Mark stage as already RUNNING
        stage = execution.stages[0]
        stage.status = WorkflowStatus.RUNNING
        repository.store_stage(stage)

        handler = StartStageHandler(queue, repository)

        message = StartStage(
            execution_type="Workflow",
            execution_id=execution.id,
            stage_id=stage.id,
        )

        # Should not raise and should skip processing
        handler.handle(message)

        # Queue should be empty (no new messages pushed)
        assert queue.size() == 0

    def test_concurrent_diamond_pipeline_completes_successfully(self) -> None:
        """Integration test: diamond pipeline completes even with concurrent messages.

        This tests the full flow where stage D might receive multiple StartStage
        messages when B and C complete around the same time.
        """
        from stabilize import (
            CompleteStageHandler,
            CompleteTaskHandler,
            CompleteWorkflowHandler,
            Orchestrator,
            QueueProcessor,
            RunTaskHandler,
            StartTaskHandler,
            StartWorkflowHandler,
            Task,
            TaskRegistry,
            TaskResult,
        )

        class SuccessTask(Task):
            def execute(self, stage: StageExecution) -> TaskResult:
                return TaskResult.success(outputs={"done": True})

        queue = InMemoryQueue()
        repository = InMemoryWorkflowStore()
        task_registry = TaskRegistry()
        task_registry.register("success", SuccessTask)

        processor = QueueProcessor(queue)
        for h in [
            StartWorkflowHandler(queue, repository),
            StartStageHandler(queue, repository),
            StartTaskHandler(queue, repository, task_registry),
            RunTaskHandler(queue, repository, task_registry),
            CompleteTaskHandler(queue, repository),
            CompleteStageHandler(queue, repository),
            CompleteWorkflowHandler(queue, repository),
        ]:
            processor.register_handler(h)

        runner = Orchestrator(queue)

        # Diamond: A -> [B, C] -> D
        execution = Workflow.create(
            application="test",
            name="Diamond Pipeline",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="test",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="b",
                    type="test",
                    name="Stage B",
                    requisite_stage_ref_ids={"a"},
                    tasks=[
                        TaskExecution.create(
                            name="Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="c",
                    type="test",
                    name="Stage C",
                    requisite_stage_ref_ids={"a"},
                    tasks=[
                        TaskExecution.create(
                            name="Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="d",
                    type="test",
                    name="Stage D",
                    requisite_stage_ref_ids={"b", "c"},
                    tasks=[
                        TaskExecution.create(
                            name="Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # All stages should succeed
        for stage in result.stages:
            assert stage.status == WorkflowStatus.SUCCEEDED
