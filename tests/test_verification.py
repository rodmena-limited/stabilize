"""Tests for the verification system."""

from datetime import UTC, datetime

from stabilize.models.stage import StageExecution
from stabilize.verification import (
    CallableVerifier,
    OutputVerifier,
    Verifier,
    VerifyResult,
    VerifyStatus,
)


class TestVerifyResult:
    """Tests for VerifyResult class."""

    def test_ok_result(self) -> None:
        """Test creating a successful result."""
        result = VerifyResult.ok("All good")
        assert result.status == VerifyStatus.OK
        assert result.message == "All good"
        assert result.is_ok
        assert not result.is_retry
        assert not result.is_failed
        assert result.is_terminal

    def test_ok_default_message(self) -> None:
        """Test OK result with default message."""
        result = VerifyResult.ok()
        assert result.message == "Verification passed"

    def test_retry_result(self) -> None:
        """Test creating a retry result."""
        result = VerifyResult.retry("Still waiting", {"attempt": 1})
        assert result.status == VerifyStatus.RETRY
        assert result.message == "Still waiting"
        assert result.details == {"attempt": 1}
        assert result.is_retry
        assert not result.is_ok
        assert not result.is_failed
        assert not result.is_terminal

    def test_failed_result(self) -> None:
        """Test creating a failed result."""
        result = VerifyResult.failed("Something went wrong", {"error_code": 500})
        assert result.status == VerifyStatus.FAILED
        assert result.message == "Something went wrong"
        assert result.details == {"error_code": 500}
        assert result.is_failed
        assert not result.is_ok
        assert not result.is_retry
        assert result.is_terminal

    def test_skipped_result(self) -> None:
        """Test creating a skipped result."""
        result = VerifyResult.skipped("Not applicable")
        assert result.status == VerifyStatus.SKIPPED
        assert result.message == "Not applicable"
        assert result.is_terminal
        assert not result.is_ok
        assert not result.is_retry
        assert not result.is_failed

    def test_timestamp_set(self) -> None:
        """Test that timestamp is set on creation."""
        before = datetime.now(UTC)
        result = VerifyResult.ok()
        after = datetime.now(UTC)
        assert before <= result.timestamp <= after


class TestOutputVerifier:
    """Tests for OutputVerifier class."""

    def test_required_keys_present(self) -> None:
        """Test verification passes when required keys are present."""
        verifier = OutputVerifier(required_keys=["url", "status"])
        stage = StageExecution(
            ref_id="test",
            outputs={"url": "http://example.com", "status": "ok"},
        )
        result = verifier.verify(stage)
        assert result.is_ok

    def test_required_keys_missing(self) -> None:
        """Test verification fails when required keys are missing."""
        verifier = OutputVerifier(required_keys=["url", "status"])
        stage = StageExecution(
            ref_id="test",
            outputs={"url": "http://example.com"},
        )
        result = verifier.verify(stage)
        assert result.is_failed
        assert "status" in result.message
        assert result.details["missing_keys"] == ["status"]

    def test_type_checks_pass(self) -> None:
        """Test type checking passes with correct types."""
        verifier = OutputVerifier(type_checks={"count": int, "name": str})
        stage = StageExecution(
            ref_id="test",
            outputs={"count": 5, "name": "test"},
        )
        result = verifier.verify(stage)
        assert result.is_ok

    def test_type_checks_fail(self) -> None:
        """Test type checking fails with incorrect types."""
        verifier = OutputVerifier(type_checks={"count": int})
        stage = StageExecution(
            ref_id="test",
            outputs={"count": "not an int"},
        )
        result = verifier.verify(stage)
        assert result.is_failed
        assert "Type errors" in result.message

    def test_combined_checks(self) -> None:
        """Test combined required keys and type checks."""
        verifier = OutputVerifier(
            required_keys=["url"],
            type_checks={"count": int},
        )
        stage = StageExecution(
            ref_id="test",
            outputs={"url": "http://example.com", "count": 10},
        )
        result = verifier.verify(stage)
        assert result.is_ok


class TestCallableVerifier:
    """Tests for CallableVerifier class."""

    def test_callable_verifier_ok(self) -> None:
        """Test callable verifier with passing function."""

        def check(stage: StageExecution) -> VerifyResult:
            if stage.outputs.get("ready"):
                return VerifyResult.ok()
            return VerifyResult.retry("Not ready")

        verifier = CallableVerifier(check)
        stage = StageExecution(ref_id="test", outputs={"ready": True})
        result = verifier.verify(stage)
        assert result.is_ok

    def test_callable_verifier_retry(self) -> None:
        """Test callable verifier with retry result."""

        def check(stage: StageExecution) -> VerifyResult:
            return VerifyResult.retry("Still waiting")

        verifier = CallableVerifier(check)
        stage = StageExecution(ref_id="test")
        result = verifier.verify(stage)
        assert result.is_retry

    def test_custom_retry_settings(self) -> None:
        """Test custom retry settings."""

        def check(stage: StageExecution) -> VerifyResult:
            return VerifyResult.ok()

        verifier = CallableVerifier(check, max_retries=5, retry_delay=2.0)
        assert verifier.max_retries == 5
        assert verifier.retry_delay_seconds == 2.0


class TestVerifierBaseClass:
    """Tests for Verifier base class defaults."""

    def test_default_max_retries(self) -> None:
        """Test default max retries value."""

        class SimpleVerifier(Verifier):
            def verify(self, stage: StageExecution) -> VerifyResult:
                return VerifyResult.ok()

        verifier = SimpleVerifier()
        assert verifier.max_retries == 3

    def test_default_retry_delay(self) -> None:
        """Test default retry delay value."""

        class SimpleVerifier(Verifier):
            def verify(self, stage: StageExecution) -> VerifyResult:
                return VerifyResult.ok()

        verifier = SimpleVerifier()
        assert verifier.retry_delay_seconds == 1.0
