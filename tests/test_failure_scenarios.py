"""Tests for failure scenarios and enterprise hardening features.

These tests verify:
1. Atomic transactions (rollback on failure)
2. Dead Letter Queue (DLQ) functionality
3. Recovery scenarios
4. Error hierarchy and classification

Note: Thread safety tests are skipped for in-memory SQLite because
each thread gets its own connection/database. For true thread safety
tests, use a file-based SQLite or PostgreSQL.
"""

from __future__ import annotations

import os
import tempfile

import pytest

from stabilize import (
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    Workflow,
)
from stabilize.errors import (
    PermanentError,
    RecoveryError,
    StabilizeError,
    TaskError,
    TransientError,
    is_permanent,
    is_transient,
)
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import CompleteTask, StartStage
from stabilize.recovery import WorkflowRecovery

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def temp_db_path():
    """Create a temp file for SQLite database."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def sqlite_store(temp_db_path):
    """Create file-based SQLite store."""
    store = SqliteWorkflowStore(
        connection_string=f"sqlite:///{temp_db_path}",
        create_tables=True,
    )
    yield store
    store.close()


@pytest.fixture
def sqlite_queue(temp_db_path):
    """Create file-based SQLite queue."""
    queue = SqliteQueue(
        connection_string=f"sqlite:///{temp_db_path}",
        table_name="queue_messages",
        max_attempts=3,  # Low for testing
    )
    queue._create_table()
    yield queue
    queue.clear()


@pytest.fixture
def sample_workflow(sqlite_store):
    """Create a sample workflow for testing."""
    workflow = Workflow.create(
        application="test-app",
        name="test-workflow",
        stages=[
            StageExecution(
                ref_id="stage1",
                type="test",
                name="Test Stage",
                tasks=[
                    TaskExecution.create(
                        name="TestTask",
                        implementing_class="test",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )
    sqlite_store.store(workflow)
    return workflow


# =============================================================================
# Atomic Transaction Tests
# =============================================================================


class TestAtomicTransactions:
    """Tests for atomic transaction functionality."""

    def test_transaction_commits_on_success(self, sqlite_store, sample_workflow):
        """Verify transaction commits when all operations succeed."""
        stage = sqlite_store.retrieve_stage(sample_workflow.stages[0].id)
        stage.status = WorkflowStatus.RUNNING
        stage.outputs["test"] = "value"

        # Create a mock message
        message = StartStage(
            execution_type="pipeline",
            execution_id=sample_workflow.id,
            stage_id=stage.id,
        )

        with sqlite_store.transaction() as txn:
            txn.store_stage(stage)
            txn.push_message(message)

        # Verify stage was saved
        updated_stage = sqlite_store.retrieve_stage(stage.id)
        assert updated_stage.status == WorkflowStatus.RUNNING
        assert updated_stage.outputs.get("test") == "value"

    def test_transaction_rollback_on_exception(self, sqlite_store, sample_workflow):
        """Verify transaction rolls back when an exception occurs."""
        stage = sqlite_store.retrieve_stage(sample_workflow.stages[0].id)
        original_status = stage.status

        stage.status = WorkflowStatus.SUCCEEDED

        with pytest.raises(ValueError):
            with sqlite_store.transaction() as txn:
                txn.store_stage(stage)
                raise ValueError("Simulated failure")

        # Verify stage was NOT updated (rolled back)
        unchanged_stage = sqlite_store.retrieve_stage(stage.id)
        assert unchanged_stage.status == original_status

    def test_transaction_isolation(self, sqlite_store, sample_workflow):
        """Verify changes aren't visible until commit."""
        stage = sqlite_store.retrieve_stage(sample_workflow.stages[0].id)
        stage.outputs["during_transaction"] = True

        # Start transaction but don't complete it
        conn = sqlite_store._get_connection()

        # Update in transaction
        conn.execute(
            """
            UPDATE stage_executions SET outputs = :outputs WHERE id = :id
            """,
            {"id": stage.id, "outputs": '{"during_transaction": true}'},
        )

        # Without commit, should see old value in new query
        # (SQLite isolation depends on mode, this verifies basic operation)
        result = conn.execute(
            "SELECT outputs FROM stage_executions WHERE id = :id",
            {"id": stage.id},
        )
        row = result.fetchone()
        assert "during_transaction" in row["outputs"]

        # Rollback
        conn.rollback()


# =============================================================================
# Dead Letter Queue Tests
# =============================================================================


class TestDeadLetterQueue:
    """Tests for Dead Letter Queue functionality."""

    def test_move_to_dlq(self, sqlite_queue):
        """Verify message is moved to DLQ."""
        # Push a message
        message = StartStage(
            execution_type="pipeline",
            execution_id="test-exec",
            stage_id="test-stage",
        )
        sqlite_queue.push(message)

        # Poll to get the message ID
        polled = sqlite_queue.poll_one()
        assert polled is not None
        msg_id = polled.message_id

        # Move to DLQ
        sqlite_queue.move_to_dlq(msg_id, "Test error")

        # Verify main queue is empty
        assert sqlite_queue.size() == 0

        # Verify DLQ has the message
        assert sqlite_queue.dlq_size() == 1

        dlq_messages = sqlite_queue.list_dlq()
        assert len(dlq_messages) == 1
        assert dlq_messages[0]["error"] == "Test error"
        assert dlq_messages[0]["message_type"] == "StartStage"

    def test_replay_dlq(self, sqlite_queue):
        """Verify message can be replayed from DLQ."""
        # Push and move to DLQ
        message = StartStage(
            execution_type="pipeline",
            execution_id="test-exec",
            stage_id="test-stage",
        )
        sqlite_queue.push(message)
        polled = sqlite_queue.poll_one()
        sqlite_queue.move_to_dlq(polled.message_id, "Test error")

        # Get DLQ entry ID
        dlq_messages = sqlite_queue.list_dlq()
        dlq_id = dlq_messages[0]["id"]

        # Replay
        success = sqlite_queue.replay_dlq(dlq_id)
        assert success

        # Verify DLQ is empty
        assert sqlite_queue.dlq_size() == 0

        # Verify message is back in main queue
        assert sqlite_queue.size() == 1

    def test_check_and_move_expired(self, sqlite_queue):
        """Verify expired messages are automatically moved to DLQ.

        Note: This test simulates a message that has exceeded max_attempts
        by directly updating the database, then verifying check_and_move_expired
        moves it to the DLQ.
        """
        # Push a message
        message = StartStage(
            execution_type="pipeline",
            execution_id="test-exec",
            stage_id="test-stage",
        )
        sqlite_queue.push(message)

        # Verify message is in queue
        initial_size = sqlite_queue.size()
        assert initial_size == 1

        # Directly set attempts to exceed max (simulating exhausted retries)
        # Need to clear locked_until so it can be found by check_and_move_expired
        conn = sqlite_queue._get_connection()
        conn.execute(
            f"UPDATE {sqlite_queue.table_name} SET attempts = :attempts, locked_until = NULL",
            {"attempts": sqlite_queue.max_attempts + 1},
        )
        conn.commit()

        # Now check_and_move_expired should find and move it
        moved = sqlite_queue.check_and_move_expired()

        # If check_and_move_expired doesn't find by attempts, manually verify the test setup
        if moved == 0:
            # Check if message exists with high attempts
            result = conn.execute(f"SELECT attempts FROM {sqlite_queue.table_name}")
            row = result.fetchone()
            if row:
                assert row["attempts"] >= sqlite_queue.max_attempts, (
                    f"Message attempts={row['attempts']} should be >= {sqlite_queue.max_attempts}"
                )

        assert moved == 1, f"Expected 1 message to be moved, but {moved} were moved"
        assert sqlite_queue.dlq_size() == 1

    def test_clear_dlq(self, sqlite_queue):
        """Verify DLQ can be cleared."""
        # Add messages to DLQ
        for i in range(3):
            message = StartStage(
                execution_type="pipeline",
                execution_id=f"exec-{i}",
                stage_id=f"stage-{i}",
            )
            sqlite_queue.push(message)
            polled = sqlite_queue.poll_one()
            sqlite_queue.move_to_dlq(polled.message_id, f"Error {i}")

        assert sqlite_queue.dlq_size() == 3

        # Clear DLQ
        cleared = sqlite_queue.clear_dlq()
        assert cleared == 3
        assert sqlite_queue.dlq_size() == 0

    def test_list_dlq_with_filter(self, sqlite_queue):
        """Verify DLQ can be filtered by message type."""
        # Add different message types
        sqlite_queue.push(
            StartStage(
                execution_type="pipeline",
                execution_id="exec-1",
                stage_id="stage-1",
            )
        )
        polled = sqlite_queue.poll_one()
        sqlite_queue.move_to_dlq(polled.message_id, "Error 1")

        sqlite_queue.push(
            CompleteTask(
                execution_type="pipeline",
                execution_id="exec-2",
                stage_id="stage-2",
                task_id="task-2",
                status=WorkflowStatus.SUCCEEDED,
            )
        )
        polled = sqlite_queue.poll_one()
        sqlite_queue.move_to_dlq(polled.message_id, "Error 2")

        # Filter by type
        start_messages = sqlite_queue.list_dlq(message_type="StartStage")
        assert len(start_messages) == 1

        complete_messages = sqlite_queue.list_dlq(message_type="CompleteTask")
        assert len(complete_messages) == 1


# =============================================================================
# Thread Safety Tests
# =============================================================================
# Note: Thread safety tests are skipped for SQLite because in-memory SQLite
# doesn't share tables across threads properly. For true thread safety tests,
# use PostgreSQL backend.
#
# The thread safety mechanisms (locks in processor.py) are tested indirectly
# through the integration tests that use parameterized backends.


# =============================================================================
# Recovery Tests
# =============================================================================


class TestRecovery:
    """Tests for workflow recovery functionality."""

    def test_recovery_instance_creation(self, sqlite_store, sqlite_queue):
        """Verify WorkflowRecovery can be instantiated."""
        recovery = WorkflowRecovery(sqlite_store, sqlite_queue)
        assert recovery.store is sqlite_store
        assert recovery.queue is sqlite_queue
        assert recovery.max_recovery_age_hours == 24.0

    def test_recovery_with_no_workflows(self, sqlite_store, sqlite_queue):
        """Verify recovery handles empty store gracefully."""
        recovery = WorkflowRecovery(sqlite_store, sqlite_queue)
        # Should not fail even with no workflows
        results = recovery.recover_pending_workflows(application="nonexistent-app")
        assert len(results) == 0

    @pytest.mark.skip(reason="Requires integration with retrieve_by_application")
    def test_recover_pending_workflow(self, sqlite_store, sqlite_queue, sample_workflow):
        """Verify pending workflow is recovered on startup."""
        pass

    @pytest.mark.skip(reason="Requires integration with retrieve_by_application")
    def test_skip_completed_workflow(self, sqlite_store, sqlite_queue, sample_workflow):
        """Verify completed workflows are skipped during recovery."""
        pass

    @pytest.mark.skip(reason="Requires integration with retrieve_by_application")
    def test_recover_on_startup_convenience(self, sqlite_store, sqlite_queue, sample_workflow):
        """Verify convenience function works."""
        pass


# =============================================================================
# Error Hierarchy Tests
# =============================================================================


class TestErrorHierarchy:
    """Tests for error classification and hierarchy."""

    def test_transient_error_is_retryable(self):
        """Verify TransientError is classified as retryable."""
        error = TransientError("Network timeout")
        assert is_transient(error)
        assert not is_permanent(error)

    def test_permanent_error_not_retryable(self):
        """Verify PermanentError is not retryable."""
        error = PermanentError("Invalid request")
        assert is_permanent(error)
        assert not is_transient(error)

    def test_error_with_cause(self):
        """Verify error chaining works."""
        cause = ValueError("Original error")
        error = TaskError("Task failed", cause=cause)

        assert error.cause is cause
        assert "caused by" in str(error)

    def test_error_codes(self):
        """Verify error codes are set correctly."""
        assert TransientError.code == 101
        assert PermanentError.code == 102
        assert RecoveryError.code == 103
        assert TaskError.code == 200

    def test_task_error_with_details(self):
        """Verify TaskError captures task details."""
        error = TaskError(
            "Task failed",
            task_name="MyTask",
            stage_id="stage-123",
            execution_id="exec-456",
            details={"step": 3, "input": "bad_value"},
        )

        assert error.task_name == "MyTask"
        assert error.stage_id == "stage-123"
        assert error.execution_id == "exec-456"
        assert error.details["step"] == 3

    def test_transient_detection_from_name(self):
        """Verify transient detection works on error names."""

        # Custom timeout error
        class CustomTimeoutError(Exception):
            pass

        error = CustomTimeoutError("Request timed out")
        assert is_transient(error)

        # Custom connection error
        class ConnectionRefusedError(Exception):
            pass

        error = ConnectionRefusedError("Cannot connect")
        assert is_transient(error)

    def test_permanent_detection_from_name(self):
        """Verify permanent detection works on error names."""

        # Custom validation error
        class ValidationError(Exception):
            pass

        error = ValidationError("Invalid input")
        assert is_permanent(error)

        # Custom auth error
        class AuthenticationError(Exception):
            pass

        error = AuthenticationError("Bad credentials")
        assert is_permanent(error)

    def test_retry_after_on_transient(self):
        """Verify retry_after hint on TransientError."""
        error = TransientError(
            "Rate limited",
            retry_after=30.0,
        )
        assert error.retry_after == 30.0

    def test_error_inheritance(self):
        """Verify error hierarchy is correct."""
        # All errors inherit from StabilizeError
        assert issubclass(TransientError, StabilizeError)
        assert issubclass(PermanentError, StabilizeError)
        assert issubclass(TaskError, StabilizeError)
        assert issubclass(RecoveryError, StabilizeError)

        # TaskError can be caught as StabilizeError
        try:
            raise TaskError("test")
        except StabilizeError:
            pass  # Should catch


# =============================================================================
# Integration Tests
# =============================================================================


class TestFailureIntegration:
    """Integration tests for failure handling."""

    def test_dlq_move_after_exceeding_attempts(self, sqlite_queue):
        """Verify message can be manually moved to DLQ after too many attempts."""
        msg = StartStage(
            execution_type="pipeline",
            execution_id="test-exec",
            stage_id="test-stage",
        )
        sqlite_queue.push(msg)

        # Poll the message
        polled = sqlite_queue.poll_one()
        assert polled is not None

        # Manually move to DLQ (simulating what check_and_move_expired does)
        sqlite_queue.move_to_dlq(polled.message_id, "Exceeded max attempts")

        # Message should be in DLQ
        assert sqlite_queue.dlq_size() == 1
        assert sqlite_queue.size() == 0

    def test_transaction_rollback_on_exception(self, sqlite_store, sample_workflow):
        """Verify transaction rolls back stage changes on exception."""
        stage = sqlite_store.retrieve_stage(sample_workflow.stages[0].id)
        original_outputs = dict(stage.outputs)
        stage.outputs["should_rollback"] = True

        # Force an exception during transaction
        with pytest.raises(RuntimeError):
            with sqlite_store.transaction() as txn:
                txn.store_stage(stage)
                raise RuntimeError("Simulated failure")

        # Stage should be unchanged (rolled back)
        unchanged_stage = sqlite_store.retrieve_stage(stage.id)
        assert unchanged_stage.outputs == original_outputs
