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

    def test_stage_not_ready_error_is_expected(self) -> None:
        """Test StageNotReadyError is an expected error."""
        error = StageNotReadyError("not ready", stage_ref_id="stage-1")
        assert isinstance(error, StabilizeExpectedError)
        assert error.stage_ref_id == "stage-1"

class TestAssertTrue:
    """Tests for assert_true function."""

    def test_passes_on_true(self) -> None:
        """Test assert_true passes when condition is True."""
        assert_true(True, "Should not fail")  # Should not raise

    def test_fails_on_false(self) -> None:
        """Test assert_true fails when condition is False."""
        with pytest.raises(PreconditionError) as exc_info:
            assert_true(False, "Condition failed")
        assert "Condition failed" in str(exc_info.value)

class TestAssertContext:
    """Tests for context assertion functions."""

    def test_assert_context_returns_value(self) -> None:
        """Test assert_context returns the value when present."""
        stage = StageExecution(ref_id="test", context={"api_key": "secret123"})
        value = assert_context(stage, "api_key")
        assert value == "secret123"

    def test_assert_context_fails_on_missing(self) -> None:
        """Test assert_context fails when key is missing."""
        stage = StageExecution(ref_id="test", context={})
        with pytest.raises(ContextError) as exc_info:
            assert_context(stage, "api_key")
        assert "api_key" in str(exc_info.value)
        assert exc_info.value.key == "api_key"

    def test_assert_context_custom_message(self) -> None:
        """Test assert_context with custom message."""
        stage = StageExecution(ref_id="test", context={})
        with pytest.raises(ContextError) as exc_info:
            assert_context(stage, "api_key", "API key is required for authentication")
        assert "API key is required" in str(exc_info.value)

    def test_assert_context_type_correct_type(self) -> None:
        """Test assert_context_type with correct type."""
        stage = StageExecution(ref_id="test", context={"timeout": 30})
        value = assert_context_type(stage, "timeout", int)
        assert value == 30
        assert isinstance(value, int)

    def test_assert_context_type_wrong_type(self) -> None:
        """Test assert_context_type with wrong type."""
        stage = StageExecution(ref_id="test", context={"timeout": "thirty"})
        with pytest.raises(ContextError) as exc_info:
            assert_context_type(stage, "timeout", int)
        assert "int" in str(exc_info.value)

    def test_assert_context_in_valid(self) -> None:
        """Test assert_context_in with valid value."""
        stage = StageExecution(ref_id="test", context={"env": "prod"})
        value = assert_context_in(stage, "env", ["dev", "staging", "prod"])
        assert value == "prod"

    def test_assert_context_in_invalid(self) -> None:
        """Test assert_context_in with invalid value."""
        stage = StageExecution(ref_id="test", context={"env": "test"})
        with pytest.raises(ContextError) as exc_info:
            assert_context_in(stage, "env", ["dev", "staging", "prod"])
        assert "test" in str(exc_info.value)

class TestAssertOutput:
    """Tests for output assertion functions."""

    def test_assert_output_returns_value(self) -> None:
        """Test assert_output returns the value when present."""
        stage = StageExecution(ref_id="test", outputs={"result": "success"})
        value = assert_output(stage, "result")
        assert value == "success"

    def test_assert_output_fails_on_missing(self) -> None:
        """Test assert_output fails when key is missing."""
        stage = StageExecution(ref_id="test", outputs={})
        with pytest.raises(OutputError) as exc_info:
            assert_output(stage, "result")
        assert exc_info.value.key == "result"

    def test_assert_output_type_correct(self) -> None:
        """Test assert_output_type with correct type."""
        stage = StageExecution(ref_id="test", outputs={"count": 42})
        value = assert_output_type(stage, "count", int)
        assert value == 42

    def test_assert_output_type_wrong(self) -> None:
        """Test assert_output_type with wrong type."""
        stage = StageExecution(ref_id="test", outputs={"count": "forty-two"})
        with pytest.raises(OutputError) as exc_info:
            assert_output_type(stage, "count", int)
        assert "int" in str(exc_info.value)

class TestAssertStageReady:
    """Tests for stage readiness assertions."""
