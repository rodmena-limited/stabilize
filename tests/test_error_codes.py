"""Tests for error codes and error chain traversal."""

from stabilize.error_codes import (
    ErrorCode,
    classify_error,
    error_chain,
    find_in_chain,
)
from stabilize.errors import (
    ConcurrencyError,
    ConfigurationError,
    PermanentError,
    RecoveryError,
    TaskError,
    TaskNotFoundError,
    TaskTimeoutError,
    TransientError,
    VerificationError,
)


class TestErrorChain:
    """Tests for error_chain() function."""

    def test_single_error_returns_list_with_one_item(self) -> None:
        """Single error with no cause returns list with just that error."""
        error = ValueError("test")
        chain = error_chain(error)
        assert len(chain) == 1
        assert chain[0] is error

    def test_chained_errors_returns_root_to_leaf(self) -> None:
        """Chained errors return list from root cause to leaf."""
        root = ValueError("root")
        middle = RuntimeError("middle")
        middle.__cause__ = root
        leaf = TypeError("leaf")
        leaf.__cause__ = middle

        chain = error_chain(leaf)
        assert len(chain) == 3
        assert chain[0] is root
        assert chain[1] is middle
        assert chain[2] is leaf

    def test_none_cause_terminates_chain(self) -> None:
        """Explicit None cause terminates chain traversal."""
        try:
            raise ValueError("root") from None
        except ValueError as e:
            chain = error_chain(e)
            assert len(chain) == 1


class TestFindInChain:
    """Tests for find_in_chain() function."""

    def test_finds_matching_type_in_chain(self) -> None:
        """Returns first error of matching type in chain."""
        root = ValueError("root")
        middle = RuntimeError("middle")
        middle.__cause__ = root
        leaf = TypeError("leaf")
        leaf.__cause__ = middle

        found = find_in_chain(leaf, ValueError)
        assert found is root

    def test_returns_none_when_not_found(self) -> None:
        """Returns None when type not in chain."""
        error = ValueError("test")
        found = find_in_chain(error, TypeError)
        assert found is None

    def test_finds_exact_type_not_subclass(self) -> None:
        """Can find subclass when looking for parent type."""
        error = TaskTimeoutError("timeout")
        found = find_in_chain(error, TaskError)
        # isinstance matches parent type
        assert found is error


class TestClassifyError:
    """Tests for classify_error() function."""

    def test_stabilize_errors_use_error_code_property(self) -> None:
        """Stabilize errors return their error_code property."""
        assert classify_error(TransientError("test")) == ErrorCode.NETWORK_ERROR
        assert classify_error(PermanentError("test")) == ErrorCode.USER_CODE_ERROR
        assert classify_error(RecoveryError("test")) == ErrorCode.RECOVERY_FAILED
        assert classify_error(ConfigurationError("test")) == ErrorCode.CONFIGURATION_INVALID
        assert classify_error(ConcurrencyError("test")) == ErrorCode.CONCURRENCY_CONFLICT
        assert classify_error(TaskTimeoutError("test")) == ErrorCode.TASK_TIMEOUT
        assert classify_error(TaskNotFoundError("test")) == ErrorCode.TASK_NOT_FOUND
        assert classify_error(VerificationError("test")) == ErrorCode.VERIFICATION_FAILED

    def test_timeout_errors_classified_correctly(self) -> None:
        """Timeout-related errors classified as TASK_TIMEOUT."""
        assert classify_error(TimeoutError("test")) == ErrorCode.TASK_TIMEOUT

    def test_connection_errors_classified_as_network(self) -> None:
        """Connection errors classified as NETWORK_ERROR."""
        assert classify_error(ConnectionError("test")) == ErrorCode.NETWORK_ERROR

    def test_unknown_errors_return_unknown(self) -> None:
        """Unknown exception types return UNKNOWN."""
        assert classify_error(Exception("generic")) == ErrorCode.UNKNOWN

    def test_cause_chain_checked_for_error_code(self) -> None:
        """Checks cause chain for Stabilize error codes."""
        inner = TaskTimeoutError("timeout")
        outer = RuntimeError("wrapper")
        outer.__cause__ = inner

        assert classify_error(outer) == ErrorCode.TASK_TIMEOUT

    def test_explicit_error_code_respected(self) -> None:
        """Explicit error_code parameter is used."""
        error = TaskError("test", error_code=ErrorCode.CIRCUIT_OPEN)
        assert classify_error(error) == ErrorCode.CIRCUIT_OPEN


class TestErrorCodeEnum:
    """Tests for ErrorCode enum values."""

    def test_all_codes_have_string_values(self) -> None:
        """All error codes have string values."""
        for code in ErrorCode:
            assert isinstance(code.value, str)
            assert len(code.value) > 0

    def test_codes_are_unique(self) -> None:
        """All error code values are unique."""
        values = [code.value for code in ErrorCode]
        assert len(values) == len(set(values))
