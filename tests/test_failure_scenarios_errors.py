from __future__ import annotations

from stabilize.errors import (
    PermanentError,
    RecoveryError,
    StabilizeError,
    TaskError,
    TransientError,
    is_permanent,
    is_transient,
)

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
