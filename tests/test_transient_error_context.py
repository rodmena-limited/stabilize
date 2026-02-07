"""Tests for TransientError context preservation across retries."""

from stabilize import TaskResult, TransientError
from stabilize.errors import is_transient
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import Task
from tests.conftest import setup_stabilize


class TestTransientErrorContextUpdate:
    """Tests for TransientError context_update parameter."""

    def test_context_update_attribute_exists(self) -> None:
        """Verify TransientError has context_update attribute."""
        error = TransientError("test error")
        assert hasattr(error, "context_update")
        assert error.context_update == {}

    def test_context_update_empty_by_default(self) -> None:
        """Verify backward compatibility when context_update not provided."""
        error = TransientError("test error", retry_after=5)
        assert error.context_update == {}
        assert error.retry_after == 5

    def test_context_update_stores_dict(self) -> None:
        """Verify context_update stores provided dict."""
        update = {"progress": 50, "items_processed": 100}
        error = TransientError("test error", context_update=update)
        assert error.context_update == update

    def test_context_update_with_all_params(self) -> None:
        """Verify all TransientError parameters work together."""
        error = TransientError(
            "Rate limited",
            code=429,
            retry_after=30,
            context_update={"batch": 5},
        )
        assert str(error) == "Rate limited (code=429)"
        assert error.retry_after == 30
        assert error.context_update == {"batch": 5}

    def test_is_transient_still_works(self) -> None:
        """Verify is_transient() still identifies TransientError."""
        error = TransientError("test", context_update={"x": 1})
        assert is_transient(error)


class ProgressTask(Task):
    """Task that tracks progress and raises TransientError with context_update."""

    max_retries_before_success = 3
    attempts = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        ProgressTask.attempts += 1
        progress = stage.context.get("progress", 0)

        if ProgressTask.attempts < ProgressTask.max_retries_before_success:
            # Simulate transient failure with progress update
            raise TransientError(
                f"Attempt {ProgressTask.attempts} failed",
                retry_after=0.1,
                context_update={"progress": progress + 33},
            )

        # Final success
        return TaskResult.success(outputs={"final_progress": stage.context.get("progress", 0) + 34})


class TestStatefulRetriesIntegration:
    """Integration tests for stateful retries with context preservation."""

    def test_context_preserved_across_retries(
        self,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Verify context_update is merged into stage context on retry."""
        # Reset task state
        ProgressTask.attempts = 0
        ProgressTask.max_retries_before_success = 3

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"progress": ProgressTask})

        execution = Workflow.create(
            application="test",
            name="Progress Pipeline",
            stages=[
                StageExecution(
                    ref_id="progress_stage",
                    type="test",
                    name="Progress Stage",
                    tasks=[
                        TaskExecution.create(
                            name="Progress Task",
                            implementing_class="progress",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)

        # Process all messages (with retries)
        processor.process_all(timeout=30.0)

        # Verify
        result = repository.retrieve(execution.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # The progress should have accumulated across retries
        # Each retry adds 33, then final adds 34
        # Attempt 1: progress 0 -> raises with context_update=33
        # Attempt 2: progress 33 -> raises with context_update=66
        # Attempt 3: progress 66 -> succeeds with output 66+34=100
        stage = result.stages[0]
        assert stage.outputs.get("final_progress") == 100

        # Cleanup
        repository.delete(execution.id)


class TestTransientErrorBackwardCompatibility:
    """Tests to ensure backward compatibility."""

    def test_existing_transient_error_usage_unchanged(self) -> None:
        """Verify existing TransientError usage patterns still work."""
        # Pattern 1: Just message (includes default error code)
        e1 = TransientError("simple error")
        assert "simple error" in str(e1)

        # Pattern 2: Message with retry_after
        e2 = TransientError("rate limited", retry_after=30)
        assert e2.retry_after == 30

        # Pattern 3: Message with code
        e3 = TransientError("server error", code=503)
        assert e3.code == 503

        # Pattern 4: Message with cause
        original = ValueError("original")
        e4 = TransientError("wrapped", cause=original)
        assert e4.cause == original

    def test_subclass_concurrency_error_unchanged(self) -> None:
        """Verify ConcurrencyError subclass still works."""
        from stabilize.errors import ConcurrencyError

        error = ConcurrencyError("optimistic lock failed")
        assert is_transient(error)
        assert error.context_update == {}

    def test_transient_verification_error_unchanged(self) -> None:
        """Verify TransientVerificationError still works."""
        from stabilize.errors import TransientVerificationError

        error = TransientVerificationError(
            "verification pending",
            retry_after=5,
            details={"resource": "deployment"},
        )
        assert is_transient(error)
        assert error.retry_after == 5
        assert error.details == {"resource": "deployment"}
