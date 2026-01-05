"""Golden Standard Test - V2 Workflow.

Tests the V2 workflow which validates:
- Sequential execution
- Parallel branches
- Retry simulation
- Timeout + Compensation
- Event coordination
- Join gate
- Loop expansion
- Conditional logic
"""

from __future__ import annotations

from pathlib import Path

from golden_standard_tests.workflows.v2_workflow import (
    EventEmitterTask,
    RetryTask,
    create_v2_workflow,
    register_v2_tasks,
)
from stabilize import TaskRegistry, WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue
from tests.conftest import setup_stabilize


class TestGoldenV2:
    """V2 Golden Standard Workflow Test - runs on both SQLite and PostgreSQL."""

    def test_v2_workflow(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test the complete V2 golden standard workflow."""
        # Reset task state for clean test run
        RetryTask.reset()
        EventEmitterTask.reset_emitter()

        # Setup with custom tasks
        processor, runner = setup_stabilize(repository, queue)

        # Get the task registry and register V2 tasks
        for handler in processor._handlers.values():
            if hasattr(handler, "task_registry"):
                task_registry: TaskRegistry = handler.task_registry
                register_v2_tasks(task_registry)
                break

        # Create and store workflow
        workflow = create_v2_workflow()
        repository.store(workflow)

        # Run workflow
        runner.start(workflow)
        processor.process_all(timeout=30.0)

        # Retrieve result
        result = repository.retrieve(workflow.id)

        # Verify execution succeeded
        assert result.status == WorkflowStatus.SUCCEEDED, f"[{backend}] Workflow failed with status {result.status}"

        # Find finalize stage and get result
        finalize_stage = next(
            (s for s in result.stages if s.ref_id == "6"),
            None,
        )
        assert finalize_stage is not None, f"[{backend}] Finalize stage not found"

        final_result = finalize_stage.outputs.get("final_result", "")

        # Load expected result
        expected_path = Path(__file__).parent / "expected" / "v2_expected_result.txt"
        expected = expected_path.read_text().strip()

        # Verify output matches expected
        assert final_result == expected, f"[{backend}] Output mismatch!\nExpected: {expected}\nGot:      {final_result}"

        # Cleanup
        repository.delete(workflow.id)
