import pytest
from stabilize.assertions import (
    ConfigError,
    ContextError,
    OutputError,
    PreconditionError,
    StabilizeError,
    StabilizeExpectedError,
    StabilizeFatalError,
    StageNotReadyError,
    VerificationError,
    assert_config,
    assert_context,
    assert_context_in,
    assert_context_type,
    assert_no_upstream_failures,
    assert_non_empty,
    assert_not_none,
    assert_output,
    assert_output_type,
    assert_stage_ready,
    assert_true,
    assert_verified,
)
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.workflow import Workflow

class TestExceptionHierarchy:
    """Tests for the exception hierarchy."""

    def test_stabilize_error_is_base(self) -> None:
        """Test StabilizeError is the base exception."""
        error = StabilizeError("test")
        assert isinstance(error, Exception)

    def test_fatal_error_is_stabilize_error(self) -> None:
        """Test StabilizeFatalError inherits from StabilizeError."""
        error = StabilizeFatalError("fatal")
        assert isinstance(error, StabilizeError)

    def test_expected_error_is_stabilize_error(self) -> None:
        """Test StabilizeExpectedError inherits from StabilizeError."""
        error = StabilizeExpectedError("expected")
        assert isinstance(error, StabilizeError)

    def test_precondition_error_is_expected(self) -> None:
        """Test PreconditionError is an expected error."""
        error = PreconditionError("precondition", key="test_key")
        assert isinstance(error, StabilizeExpectedError)
        assert error.key == "test_key"

    def test_context_error_is_fatal(self) -> None:
        """Test ContextError is a fatal error."""
        error = ContextError("context", key="test_key")
        assert isinstance(error, StabilizeFatalError)
        assert error.key == "test_key"

    def test_output_error_is_expected(self) -> None:
        """Test OutputError is an expected error."""
        error = OutputError("output", key="test_key")
        assert isinstance(error, StabilizeExpectedError)
        assert error.key == "test_key"

    def test_config_error_is_fatal(self) -> None:
        """Test ConfigError is a fatal error."""
        error = ConfigError("config", field="test_field")
        assert isinstance(error, StabilizeFatalError)
        assert error.field == "test_field"

    def test_verification_error_is_expected(self) -> None:
        """Test VerificationError is an expected error."""
        error = VerificationError("verification", details={"code": 500})
        assert isinstance(error, StabilizeExpectedError)
        assert error.details == {"code": 500}
