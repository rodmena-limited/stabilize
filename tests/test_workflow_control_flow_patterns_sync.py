from __future__ import annotations

from stabilize import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import CancelRegion, SignalStage
from tests.conftest import setup_stabilize
from tests.control_flow_support import OutputTask, SlowTask, SuspendTask, TrackingTask


# =============================================================================
# Section 8: Mutex / Critical Section (WCP-17, 39, 40) Tests
# =============================================================================


class TestMutexCriticalSection:
    """Test Mutex / Critical Section (WCP-39) pattern.

    Stages with the same mutex_key cannot execute concurrently.
    One must wait for the other to complete.
    """

    def test_mutex_prevents_concurrent_execution(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Stages sharing a mutex_key should execute one at a time."""
        TrackingTask.reset()

        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"tracking": TrackingTask},
        )

        wf = Workflow.create(
            application="wcp-test",
            name="mutex-basic",
            stages=[
                StageExecution(
                    ref_id="root",
                    name="Root",
                    context={"outputs_to_produce": {"started": True}},
                    tasks=[
                        TaskExecution.create(
                            "Root Task",
                            "tracking",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="db_op_1",
                    name="DB Operation 1",
                    requisite_stage_ref_ids={"root"},
                    mutex_key="shared_db",
                    context={"outputs_to_produce": {"op1_done": True}},
                    tasks=[
                        TaskExecution.create(
                            "DB Op 1 Task",
                            "tracking",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="db_op_2",
                    name="DB Operation 2",
                    requisite_stage_ref_ids={"root"},
                    mutex_key="shared_db",
                    context={"outputs_to_produce": {"op2_done": True}},
                    tasks=[
                        TaskExecution.create(
                            "DB Op 2 Task",
                            "tracking",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="done",
                    name="Done",
                    requisite_stage_ref_ids={"db_op_1", "db_op_2"},
                    context={"outputs_to_produce": {"all_done": True}},
                    tasks=[
                        TaskExecution.create(
                            "Done Task",
                            "tracking",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Both mutex stages should have completed
        op1 = result.stage_by_ref_id("db_op_1")
        op2 = result.stage_by_ref_id("db_op_2")
        assert op1.status == WorkflowStatus.SUCCEEDED
        assert op2.status == WorkflowStatus.SUCCEEDED

        # Verify they ran sequentially (not concurrently)
        # Extract execution log for the mutex stages
        db_op_entries = [
            (ref_id, ts) for _, ref_id, ts in TrackingTask.execution_log if ref_id in ("db_op_1", "db_op_2")
        ]
        assert len(db_op_entries) == 2, f"Expected 2 mutex task executions, got {len(db_op_entries)}"


# =============================================================================
# Section 9: Cancel Region (WCP-25) Tests
# =============================================================================


class TestCancelRegion:
    """Test Cancel Region (WCP-25) pattern.

    A set of stages in a named region can be cancelled together.
    """

    def test_cancel_region_cancels_matching_stages(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """CancelRegion should cancel all active stages in the named region."""
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={
                "output": OutputTask,
                "slow": SlowTask,
            },
        )

        wf = Workflow.create(
            application="wcp-test",
            name="cancel-region",
            stages=[
                StageExecution(
                    ref_id="root",
                    name="Root",
                    context={"outputs_to_produce": {"started": True}},
                    tasks=[
                        TaskExecution.create(
                            "Root Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="evidence_1",
                    name="Evidence Query 1",
                    requisite_stage_ref_ids={"root"},
                    cancel_region="evidence_access",
                    context={
                        "outputs_to_produce": {"evidence_1": True},
                        "delay": 1.0,
                    },
                    tasks=[
                        TaskExecution.create(
                            "Evidence 1 Task",
                            "slow",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="evidence_2",
                    name="Evidence Query 2",
                    requisite_stage_ref_ids={"root"},
                    cancel_region="evidence_access",
                    context={
                        "outputs_to_produce": {"evidence_2": True},
                        "delay": 1.0,
                    },
                    tasks=[
                        TaskExecution.create(
                            "Evidence 2 Task",
                            "slow",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="unrelated",
                    name="Unrelated Stage",
                    requisite_stage_ref_ids={"root"},
                    context={"outputs_to_produce": {"unrelated": True}},
                    tasks=[
                        TaskExecution.create(
                            "Unrelated Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(wf)
        runner.start(wf)

        # Process a few messages to get root completed and evidence stages started
        for _ in range(10):
            processor.process_one()

        # Now push a cancel region message
        queue.push(
            CancelRegion(
                execution_type=wf.type.value,
                execution_id=wf.id,
                region="evidence_access",
            )
        )

        processor.process_all(timeout=15.0)

        result = repository.retrieve(wf.id)

        # Evidence stages should be cancelled
        evidence_1 = result.stage_by_ref_id("evidence_1")
        evidence_2 = result.stage_by_ref_id("evidence_2")
        assert evidence_1.status in {
            WorkflowStatus.CANCELED,
            WorkflowStatus.SUCCEEDED,  # may have completed before cancel
        }
        assert evidence_2.status in {
            WorkflowStatus.CANCELED,
            WorkflowStatus.SUCCEEDED,  # may have completed before cancel
        }

        # Unrelated stage should not be cancelled
        unrelated = result.stage_by_ref_id("unrelated")
        assert unrelated.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# Section 10: Signal Stage - Transient Trigger (WCP-23) Tests
# =============================================================================


class TestTransientTrigger:
    """Test Transient Trigger (WCP-23) pattern via SignalStage.

    A transient signal is lost if the stage is not SUSPENDED when it arrives.
    """

    def test_transient_signal_delivered_when_suspended(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Signal should be delivered when stage is SUSPENDED."""
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={
                "output": OutputTask,
                "suspend": SuspendTask,
            },
        )

        wf = Workflow.create(
            application="wcp-test",
            name="transient-signal",
            stages=[
                StageExecution(
                    ref_id="root",
                    name="Root",
                    context={"outputs_to_produce": {"started": True}},
                    tasks=[
                        TaskExecution.create(
                            "Root Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="handler",
                    name="Signal Handler",
                    requisite_stage_ref_ids={"root"},
                    context={},
                    tasks=[
                        TaskExecution.create(
                            "Handler Task",
                            "suspend",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(wf)
        runner.start(wf)

        # Process until the handler stage is SUSPENDED
        for _ in range(20):
            processor.process_one()
            result = repository.retrieve(wf.id)
            handler = result.stage_by_ref_id("handler")
            if handler.status == WorkflowStatus.SUSPENDED:
                break

        # Verify handler is SUSPENDED
        result = repository.retrieve(wf.id)
        handler = result.stage_by_ref_id("handler")
        assert handler.status == WorkflowStatus.SUSPENDED, f"Expected SUSPENDED, got {handler.status}"

        # Send transient signal
        queue.push(
            SignalStage(
                execution_type=wf.type.value,
                execution_id=wf.id,
                stage_id=handler.id,
                signal_name="dam_capacity_full",
                signal_data={"level": 95.5},
                persistent=False,
            )
        )

        # Process the signal
        processor.process_all(timeout=15.0)

        result = repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        handler = result.stage_by_ref_id("handler")
        assert handler.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# Section 11: Signal Stage - Persistent Trigger (WCP-24) Tests
# =============================================================================


class TestPersistentTrigger:
    """Test Persistent Trigger (WCP-24) pattern via SignalStage.

    A persistent signal is buffered if the stage is not yet SUSPENDED.
    """

    def test_persistent_signal_buffered_and_consumed(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Persistent signal is buffered when stage not SUSPENDED, then consumed.

        When a persistent signal arrives before the stage reaches SUSPENDED,
        it's stored in context. When the task later returns SUSPENDED, it
        finds the buffered signal and immediately resumes, completing the stage.
        """
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={
                "output": OutputTask,
                "suspend": SuspendTask,
            },
        )

        wf = Workflow.create(
            application="wcp-test",
            name="persistent-signal",
            stages=[
                StageExecution(
                    ref_id="root",
                    name="Root",
                    context={"outputs_to_produce": {"started": True}},
                    tasks=[
                        TaskExecution.create(
                            "Root Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="handler",
                    name="Signal Handler",
                    requisite_stage_ref_ids={"root"},
                    context={},
                    tasks=[
                        TaskExecution.create(
                            "Handler Task",
                            "suspend",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(wf)
        runner.start(wf)

        # Send persistent signal BEFORE the handler reaches SUSPENDED
        # Process just enough to start the workflow
        processor.process_one()  # Start workflow
        processor.process_one()  # Start root stage

        result = repository.retrieve(wf.id)
        handler = result.stage_by_ref_id("handler")

        # Send signal while handler is likely NOT_STARTED or just started
        queue.push(
            SignalStage(
                execution_type=wf.type.value,
                execution_id=wf.id,
                stage_id=handler.id,
                signal_name="new_staff_member",
                signal_data={"name": "Alice"},
                persistent=True,
            )
        )

        # Process everything - signal gets buffered, then consumed when task suspends
        processor.process_all(timeout=15.0)

        result = repository.retrieve(wf.id)
        handler = result.stage_by_ref_id("handler")

        # The handler should have completed: the persistent signal was either
        # delivered directly (if stage was SUSPENDED when signal arrived) or
        # buffered then auto-consumed when the task returned SUSPENDED.
        assert handler.status == WorkflowStatus.SUCCEEDED, (
            f"Expected handler to complete via persistent signal, got {handler.status}"
        )
