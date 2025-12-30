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
