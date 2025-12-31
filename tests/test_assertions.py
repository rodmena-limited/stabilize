"""Tests for the assertion helpers module."""

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

    def test_assert_stage_ready_passes(self) -> None:
        """Test assert_stage_ready passes when all upstream complete."""
        workflow = Workflow.create(
            application="test",
            name="Test",
            stages=[
                StageExecution(
                    ref_id="upstream",
                    type="test",
                    name="Upstream",
                    status=WorkflowStatus.SUCCEEDED,
                ),
                StageExecution(
                    ref_id="downstream",
                    type="test",
                    name="Downstream",
                    requisite_stage_ref_ids={"upstream"},
                ),
            ],
        )
        downstream = workflow.stage_by_ref_id("downstream")
        assert_stage_ready(downstream)  # Should not raise

    def test_assert_stage_ready_fails(self) -> None:
        """Test assert_stage_ready fails when upstream not complete."""
        workflow = Workflow.create(
            application="test",
            name="Test",
            stages=[
                StageExecution(
                    ref_id="upstream",
                    type="test",
                    name="Upstream",
                    status=WorkflowStatus.RUNNING,
                ),
                StageExecution(
                    ref_id="downstream",
                    type="test",
                    name="Downstream",
                    requisite_stage_ref_ids={"upstream"},
                ),
            ],
        )
        downstream = workflow.stage_by_ref_id("downstream")
        with pytest.raises(StageNotReadyError):
            assert_stage_ready(downstream)

    def test_assert_no_upstream_failures_passes(self) -> None:
        """Test assert_no_upstream_failures passes when no failures."""
        workflow = Workflow.create(
            application="test",
            name="Test",
            stages=[
                StageExecution(
                    ref_id="upstream",
                    type="test",
                    name="Upstream",
                    status=WorkflowStatus.SUCCEEDED,
                ),
                StageExecution(
                    ref_id="downstream",
                    type="test",
                    name="Downstream",
                    requisite_stage_ref_ids={"upstream"},
                ),
            ],
        )
        downstream = workflow.stage_by_ref_id("downstream")
        assert_no_upstream_failures(downstream)  # Should not raise

    def test_assert_no_upstream_failures_fails(self) -> None:
        """Test assert_no_upstream_failures fails when upstream failed."""
        workflow = Workflow.create(
            application="test",
            name="Test",
            stages=[
                StageExecution(
                    ref_id="upstream",
                    type="test",
                    name="Upstream",
                    status=WorkflowStatus.TERMINAL,
                ),
                StageExecution(
                    ref_id="downstream",
                    type="test",
                    name="Downstream",
                    requisite_stage_ref_ids={"upstream"},
                ),
            ],
        )
        downstream = workflow.stage_by_ref_id("downstream")
        with pytest.raises(StageNotReadyError):
            assert_no_upstream_failures(downstream)


class TestAssertConfig:
    """Tests for configuration assertion."""

    def test_assert_config_passes(self) -> None:
        """Test assert_config passes when condition is True."""
        assert_config(True, "Should pass")  # Should not raise

    def test_assert_config_fails(self) -> None:
        """Test assert_config fails when condition is False."""
        with pytest.raises(ConfigError) as exc_info:
            assert_config(False, "Invalid config", field="timeout")
        assert exc_info.value.field == "timeout"


class TestAssertVerified:
    """Tests for verification assertion."""

    def test_assert_verified_passes(self) -> None:
        """Test assert_verified passes when condition is True."""
        assert_verified(True, "Should pass")  # Should not raise

    def test_assert_verified_fails(self) -> None:
        """Test assert_verified fails when condition is False."""
        with pytest.raises(VerificationError) as exc_info:
            assert_verified(False, "Check failed", details={"code": 500})
        assert exc_info.value.details == {"code": 500}


class TestAssertNotNone:
    """Tests for assert_not_none function."""

    def test_returns_value_when_not_none(self) -> None:
        """Test returns the value when it's not None."""
        result = assert_not_none("hello", "Value required")
        assert result == "hello"

    def test_fails_when_none(self) -> None:
        """Test fails when value is None."""
        with pytest.raises(PreconditionError) as exc_info:
            assert_not_none(None, "Value required")
        assert "Value required" in str(exc_info.value)

    def test_allows_falsy_values(self) -> None:
        """Test allows falsy values that aren't None."""
        assert assert_not_none(0, "msg") == 0
        assert assert_not_none("", "msg") == ""
        assert assert_not_none([], "msg") == []
        assert assert_not_none(False, "msg") is False


class TestAssertNonEmpty:
    """Tests for assert_non_empty function."""

    def test_passes_non_empty_string(self) -> None:
        """Test passes with non-empty string."""
        result = assert_non_empty("hello", "String required")
        assert result == "hello"

    def test_fails_empty_string(self) -> None:
        """Test fails with empty string."""
        with pytest.raises(PreconditionError):
            assert_non_empty("", "String required")

    def test_passes_non_empty_list(self) -> None:
        """Test passes with non-empty list."""
        result = assert_non_empty([1, 2, 3], "List required")
        assert result == [1, 2, 3]

    def test_fails_empty_list(self) -> None:
        """Test fails with empty list."""
        with pytest.raises(PreconditionError):
            assert_non_empty([], "List required")

    def test_passes_non_empty_dict(self) -> None:
        """Test passes with non-empty dict."""
        result = assert_non_empty({"key": "value"}, "Dict required")
        assert result == {"key": "value"}

    def test_fails_empty_dict(self) -> None:
        """Test fails with empty dict."""
        with pytest.raises(PreconditionError):
            assert_non_empty({}, "Dict required")
