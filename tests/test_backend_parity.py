"""
Tests for PostgreSQL vs SQLite backend parity.

This module verifies that PostgreSQL and SQLite behave identically, catching
discrepancies like those fixed in commit 129f739 "fixed decrepency between pg and sq".

Key differences to test:
- JSON functions: json_set() vs jsonb_set()
- Datetime handling
- NULL handling
- Type casting
"""

import time
from datetime import timedelta

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import StartWorkflow


class TestBackendParity:
    """Test that PostgreSQL and SQLite behave identically."""

    def test_workflow_storage_and_retrieval(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test basic workflow storage and retrieval is identical across backends."""
        # Create workflow
        wf = Workflow.create("parity-app", "parity-test", [])
        stage = StageExecution.create("stage-1", "Test Stage", "1")
        stage.execution = wf
        task = TaskExecution.create("Task A", "shell", stage_start=True, stage_end=True)
        stage.tasks = [task]
        wf.stages = [stage]

        # Store
        repository.store(wf)

        # Retrieve
        retrieved = repository.retrieve(wf.id)
        assert retrieved is not None
        assert retrieved.id == wf.id
        assert retrieved.application == "parity-app"
        assert retrieved.name == "parity-test"
        assert len(retrieved.stages) == 1

    def test_json_context_storage_parity(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """
        Test JSON context storage is identical across backends.

        SQLite uses json_set(), PostgreSQL uses jsonb_set().
        """
        wf = Workflow.create("json-test-app", "json-test", [])
        stage = StageExecution.create("stage-1", "JSON Test Stage", "1")
        stage.execution = wf
        task = TaskExecution.create("Task", "shell", stage_start=True, stage_end=True)
        stage.tasks = [task]
        wf.stages = [stage]

        # Set complex JSON context
        stage.context = {
            "string_key": "value",
            "int_key": 42,
            "float_key": 3.14159,
            "bool_key": True,
            "null_key": None,
            "nested": {
                "inner": "nested value",
                "list": [1, 2, 3],
            },
            "array": ["a", "b", "c"],
        }

        repository.store(wf)

        # Retrieve and verify
        retrieved_stage = repository.retrieve_stage(stage.id)
        assert retrieved_stage.context["string_key"] == "value"
        assert retrieved_stage.context["int_key"] == 42
        assert abs(retrieved_stage.context["float_key"] - 3.14159) < 0.0001
        assert retrieved_stage.context["bool_key"] is True
        assert retrieved_stage.context["null_key"] is None
        assert retrieved_stage.context["nested"]["inner"] == "nested value"
        assert retrieved_stage.context["nested"]["list"] == [1, 2, 3]
        assert retrieved_stage.context["array"] == ["a", "b", "c"]

    def test_json_outputs_storage_parity(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test JSON outputs storage is identical across backends."""
        wf = Workflow.create("outputs-test-app", "outputs-test", [])
        stage = StageExecution.create("stage-1", "Outputs Test Stage", "1")
        stage.execution = wf
        task = TaskExecution.create("Task", "shell", stage_start=True, stage_end=True)
        stage.tasks = [task]
        wf.stages = [stage]

        # Set outputs
        stage.outputs = {
            "result": "success",
            "data": {"key1": "val1", "key2": 123},
            "items": [{"id": 1}, {"id": 2}],
        }

        repository.store(wf)

        # Retrieve and verify
        retrieved_stage = repository.retrieve_stage(stage.id)
        assert retrieved_stage.outputs["result"] == "success"
        assert retrieved_stage.outputs["data"]["key1"] == "val1"
        assert retrieved_stage.outputs["data"]["key2"] == 123
        assert retrieved_stage.outputs["items"][0]["id"] == 1

    def test_status_update_parity(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test status updates work identically across backends."""
        wf = Workflow.create("status-test-app", "status-test", [])
        stage = StageExecution.create("stage-1", "Status Test Stage", "1")
        stage.execution = wf
        task = TaskExecution.create("Task", "shell", stage_start=True, stage_end=True)
        stage.tasks = [task]
        wf.stages = [stage]
        repository.store(wf)

        # Update status through various states
        statuses = [
            WorkflowStatus.RUNNING,
            WorkflowStatus.PAUSED,
            WorkflowStatus.RUNNING,
            WorkflowStatus.SUCCEEDED,
        ]

        for status in statuses:
            # Retrieve, update, and store
            retrieved = repository.retrieve(wf.id)
            retrieved.update_status(status)
            repository.update_status(retrieved)
            # Verify
            verified = repository.retrieve(wf.id)
            assert verified.status == status

    def test_pause_resume_json_operations_parity(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """
        Test pause/resume operations with JSON manipulation.

        SQLite uses: json_set(paused, '$.resume_time', :resume_time)
        PostgreSQL uses: jsonb_set(paused, '{resume_time}', to_jsonb(...))
        """
        wf = Workflow.create("pause-test-app", "pause-test", [])
        stage = StageExecution.create("stage-1", "Pause Test Stage", "1")
        stage.execution = wf
        task = TaskExecution.create("Task", "shell", stage_start=True, stage_end=True)
        stage.tasks = [task]
        wf.stages = [stage]
        repository.store(wf)

        # Pause
        repository.pause(wf.id, "test-user")

        # Verify paused state
        paused_wf = repository.retrieve(wf.id)
        assert paused_wf.status == WorkflowStatus.PAUSED
        assert paused_wf.paused is not None
        assert paused_wf.paused.paused_by == "test-user"
        assert paused_wf.paused.pause_time > 0

        # Resume
        repository.resume(wf.id)

        # Verify resumed state
        resumed_wf = repository.retrieve(wf.id)
        assert resumed_wf.status == WorkflowStatus.RUNNING
        assert resumed_wf.paused is not None
        assert resumed_wf.paused.resume_time is not None
        assert resumed_wf.paused.resume_time > 0
        assert resumed_wf.paused.paused_ms is not None
        assert resumed_wf.paused.paused_ms >= 0

    def test_null_handling_parity(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test NULL handling is consistent across backends."""
        wf = Workflow.create("null-test-app", "null-test", [])
        stage = StageExecution.create("stage-1", "Null Test Stage", "1")
        stage.execution = wf
        task = TaskExecution.create("Task", "shell", stage_start=True, stage_end=True)
        stage.tasks = [task]
        wf.stages = [stage]

        # Leave various fields as None/null
        stage.context = {}
        stage.outputs = {}

        repository.store(wf)

        # Retrieve and verify NULLs
        retrieved = repository.retrieve(wf.id)
        assert retrieved is not None
        retrieved_stage = repository.retrieve_stage(stage.id)
        assert (
            retrieved_stage.context == {}
            or retrieved_stage.context is None
            or retrieved_stage.context == {}
        )

    def test_version_increment_parity(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test version increment for optimistic locking is identical."""
        wf = Workflow.create("version-test-app", "version-test", [])
        stage = StageExecution.create("stage-1", "Version Test Stage", "1")
        stage.execution = wf
        task = TaskExecution.create("Task", "shell", stage_start=True, stage_end=True)
        stage.tasks = [task]
        wf.stages = [stage]
        repository.store(wf)

        # Initial version should be 0
        initial_stage = repository.retrieve_stage(stage.id)
        assert initial_stage.version == 0

        # Update stage
        initial_stage.context["key"] = "value1"
        repository.store_stage(initial_stage)

        # Version should be 1
        updated_stage = repository.retrieve_stage(stage.id)
        assert updated_stage.version == 1

        # Update again
        updated_stage.context["key"] = "value2"
        repository.store_stage(updated_stage)

        # Version should be 2
        final_stage = repository.retrieve_stage(stage.id)
        assert final_stage.version == 2

    def test_requisite_stage_ref_ids_parity(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test requisite_stage_ref_ids (JSON array) storage parity."""
        wf = Workflow.create("dag-test-app", "dag-test", [])

        # Create stages with dependencies
        stage1 = StageExecution.create("stage-1", "First Stage", "1")
        stage1.execution = wf
        stage1.tasks = [TaskExecution.create("T1", "shell", stage_start=True, stage_end=True)]

        stage2 = StageExecution.create("stage-2", "Second Stage", "2")
        stage2.execution = wf
        stage2.requisite_stage_ref_ids = ["1"]  # Depends on stage-1
        stage2.tasks = [TaskExecution.create("T2", "shell", stage_start=True, stage_end=True)]

        stage3 = StageExecution.create("stage-3", "Third Stage", "3")
        stage3.execution = wf
        stage3.requisite_stage_ref_ids = ["1", "2"]  # Depends on both
        stage3.tasks = [TaskExecution.create("T3", "shell", stage_start=True, stage_end=True)]

        wf.stages = [stage1, stage2, stage3]
        repository.store(wf)

        # Retrieve and verify (requisite_stage_ref_ids is a set)
        retrieved_stage2 = repository.retrieve_stage(stage2.id)
        assert retrieved_stage2.requisite_stage_ref_ids == {"1"}

        retrieved_stage3 = repository.retrieve_stage(stage3.id)
        assert retrieved_stage3.requisite_stage_ref_ids == {"1", "2"}

    def test_task_exception_details_json_parity(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test task_exception_details JSON storage parity."""
        wf = Workflow.create("exception-test-app", "exception-test", [])
        stage = StageExecution.create("stage-1", "Exception Test Stage", "1")
        stage.execution = wf
        task = TaskExecution.create("Task", "shell", stage_start=True, stage_end=True)
        stage.tasks = [task]
        wf.stages = [stage]
        repository.store(wf)

        # Update task with exception details
        fresh_stage = repository.retrieve_stage(stage.id)
        fresh_task = fresh_stage.tasks[0]
        fresh_task.status = WorkflowStatus.TERMINAL
        fresh_task.task_exception_details = {
            "error_type": "RuntimeError",
            "message": "Something went wrong",
            "traceback": "line 1\nline 2\nline 3",
            "context": {"input": "test", "output": None},
        }
        repository.store_stage(fresh_stage)

        # Retrieve and verify
        final_stage = repository.retrieve_stage(stage.id)
        final_task = final_stage.tasks[0]
        assert final_task.task_exception_details["error_type"] == "RuntimeError"
        assert final_task.task_exception_details["message"] == "Something went wrong"
        assert "line 2" in final_task.task_exception_details["traceback"]
        assert final_task.task_exception_details["context"]["output"] is None


class TestQueueBackendParity:
    """Test queue operations are identical across backends."""

    def test_message_serialization_parity(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test message serialization and deserialization is identical."""
        # Clear queue to ensure clean state
        queue.clear()

        msg = StartWorkflow(
            execution_type="PIPELINE",
            execution_id="serial-test",
        )
        queue.push(msg)

        polled = queue.poll_one()
        assert polled is not None
        assert isinstance(polled, StartWorkflow)
        assert polled.execution_id == "serial-test"
        assert polled.execution_type == "PIPELINE"

        queue.ack(polled)

    def test_delay_handling_parity(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test message delay handling is identical across backends."""
        msg = StartWorkflow(
            execution_type="PIPELINE",
            execution_id="delay-test",
        )

        # Push with short delay
        queue.push(msg, delay=timedelta(seconds=1))

        # Should not be available immediately
        immediate = queue.poll_one()
        assert immediate is None

        # Wait for delay
        time.sleep(1.5)

        # Should be available now
        delayed = queue.poll_one()
        assert delayed is not None
        assert delayed.execution_id == "delay-test"

        queue.ack(delayed)

    def test_reschedule_parity(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test rescheduling behavior is identical across backends."""
        # Clear queue to ensure clean state
        queue.clear()

        msg = StartWorkflow(
            execution_type="PIPELINE",
            execution_id="reschedule-parity-test",
        )
        queue.push(msg)

        # Poll and reschedule
        polled = queue.poll_one()
        assert polled is not None

        queue.reschedule(polled, delay=timedelta(seconds=1))

        # Should not be immediately available
        immediate = queue.poll_one()
        assert immediate is None

        # But queue size should still be 1
        assert queue.size() == 1


class TestConcurrentUpdateParity:
    """Test concurrent update behavior is identical across backends."""

    def test_optimistic_locking_error_parity(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test ConcurrencyError behavior is identical."""
        from stabilize.errors import ConcurrencyError

        wf = Workflow.create("concurrent-test-app", "concurrent-test", [])
        stage = StageExecution.create("stage-1", "Concurrent Test Stage", "1")
        stage.execution = wf
        task = TaskExecution.create("Task", "shell", stage_start=True, stage_end=True)
        stage.tasks = [task]
        wf.stages = [stage]
        repository.store(wf)

        # Get two copies of the stage (simulating concurrent handlers)
        stage_a = repository.retrieve_stage(stage.id)
        stage_b = repository.retrieve_stage(stage.id)

        # Both have version 0
        assert stage_a.version == 0
        assert stage_b.version == 0

        # Update A first
        stage_a.context["from"] = "A"
        repository.store_stage(stage_a)

        # Try to update B with stale version
        stage_b.context["from"] = "B"
        with pytest.raises(ConcurrencyError):
            repository.store_stage(stage_b)

        # Verify A's update persisted
        final_stage = repository.retrieve_stage(stage.id)
        assert final_stage.context["from"] == "A"
        assert final_stage.version == 1
