"""Golden Standard Test - Disaster Recovery Workflow.

Tests the Disaster Recovery workflow which validates:
- Parallel branches with retry/timeout/compensation
- Event coordination (emit/wait)
- Foreach loop (unrolled to explicit stages)
- Switch/case conditional logic (unrolled)
- While loop with counter (unrolled)
- Multi-line output collection
"""

from __future__ import annotations

from pathlib import Path

from golden_standard_tests.workflows.disaster_recovery_workflow import (
    DREventEmitterTask,
    DRRetryTask,
    create_disaster_recovery_workflow,
    register_dr_tasks,
)
from stabilize import TaskRegistry, WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue
from tests.conftest import setup_stabilize


class TestGoldenDisasterRecovery:
    """Disaster Recovery Golden Standard Workflow Test - runs on both SQLite and PostgreSQL."""

    def test_disaster_recovery_workflow(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test the complete Disaster Recovery golden standard workflow."""
        # Reset task state for clean test run
        DRRetryTask.reset()
        DREventEmitterTask.reset_emitter()

        # Setup with custom tasks
        processor, runner = setup_stabilize(repository, queue)

        # Get the task registry and register DR tasks
        for handler in processor._handlers.values():
            if hasattr(handler, "task_registry"):
                task_registry: TaskRegistry = handler.task_registry
                register_dr_tasks(task_registry)
                break

        # Create and store workflow
        workflow = create_disaster_recovery_workflow()
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
            (s for s in result.stages if s.ref_id == "8"),
            None,
        )
        assert finalize_stage is not None, f"[{backend}] Finalize stage not found"

        final_result = finalize_stage.outputs.get("final_result", "")

        # Load expected result
        expected_path = Path(__file__).parent / "expected" / "disaster_recovery_expected_result.txt"
        expected = expected_path.read_text().strip()

        # Verify output matches expected
        assert final_result == expected, f"[{backend}] Output mismatch!\nExpected:\n{expected}\nGot:\n{final_result}"

        # Cleanup
        repository.delete(workflow.id)
