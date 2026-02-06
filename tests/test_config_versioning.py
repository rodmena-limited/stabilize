"""Tests for configuration versioning."""

import pytest

from stabilize.resilience.config import (
    BackoffConfig,
    BulkheadConfig,
    HandlerConfig,
    ResilienceConfig,
    reset_handler_config,
)


@pytest.fixture(autouse=True)
def reset_config() -> None:
    """Reset handler config singleton before each test."""
    reset_handler_config()
    yield
    reset_handler_config()


class TestBackoffConfig:
    """Tests for BackoffConfig frozen dataclass."""

    def test_frozen_instance(self) -> None:
        """BackoffConfig is immutable."""
        config = BackoffConfig()

        with pytest.raises(Exception):
            config.min_delay_ms = 200  # type: ignore

    def test_config_fingerprint_deterministic(self) -> None:
        """Same config produces same fingerprint."""
        config1 = BackoffConfig(min_delay_ms=100, max_delay_ms=1000)
        config2 = BackoffConfig(min_delay_ms=100, max_delay_ms=1000)

        assert config1.config_fingerprint() == config2.config_fingerprint()

    def test_config_fingerprint_changes_with_values(self) -> None:
        """Different configs produce different fingerprints."""
        config1 = BackoffConfig(min_delay_ms=100)
        config2 = BackoffConfig(min_delay_ms=200)

        assert config1.config_fingerprint() != config2.config_fingerprint()


class TestBulkheadConfig:
    """Tests for BulkheadConfig frozen dataclass."""

    def test_frozen_instance(self) -> None:
        """BulkheadConfig is immutable."""
        config = BulkheadConfig()

        with pytest.raises(Exception):
            config.max_concurrent = 10  # type: ignore

    def test_config_fingerprint_deterministic(self) -> None:
        """Same config produces same fingerprint."""
        config1 = BulkheadConfig(max_concurrent=5)
        config2 = BulkheadConfig(max_concurrent=5)

        assert config1.config_fingerprint() == config2.config_fingerprint()


class TestHandlerConfig:
    """Tests for HandlerConfig frozen dataclass."""

    def test_frozen_instance(self) -> None:
        """HandlerConfig is immutable."""
        config = HandlerConfig()

        with pytest.raises(Exception):
            config.concurrency_max_retries = 10  # type: ignore

    def test_config_fingerprint_deterministic(self) -> None:
        """Same config produces same fingerprint."""
        config1 = HandlerConfig()
        config2 = HandlerConfig()

        assert config1.config_fingerprint() == config2.config_fingerprint()

    def test_config_fingerprint_changes_with_values(self) -> None:
        """Different configs produce different fingerprints."""
        config1 = HandlerConfig(concurrency_max_retries=3)
        config2 = HandlerConfig(concurrency_max_retries=5)

        assert config1.config_fingerprint() != config2.config_fingerprint()

    def test_fingerprint_is_16_chars(self) -> None:
        """Fingerprint is 16 character hex string."""
        config = HandlerConfig()
        fingerprint = config.config_fingerprint()

        assert len(fingerprint) == 16
        # Should be valid hex
        int(fingerprint, 16)

    def test_from_env_returns_frozen_instance(self) -> None:
        """from_env() returns frozen instance."""
        config = HandlerConfig.from_env()

        with pytest.raises(Exception):
            config.max_workers = 20  # type: ignore

    def test_get_backoff_config_returns_frozen(self) -> None:
        """get_backoff_config() returns frozen BackoffConfig."""
        handler_config = HandlerConfig()
        backoff = handler_config.get_backoff_config()

        assert isinstance(backoff, BackoffConfig)
        with pytest.raises(Exception):
            backoff.factor = 3.0  # type: ignore


class TestResilienceConfig:
    """Tests for ResilienceConfig frozen dataclass."""

    def test_frozen_instance(self) -> None:
        """ResilienceConfig is immutable."""
        config = ResilienceConfig()

        with pytest.raises(Exception):
            config.circuit_cooldown_seconds = 60.0  # type: ignore

    def test_bulkheads_is_tuple(self) -> None:
        """Bulkheads is a tuple, not dict (for frozen compatibility)."""
        config = ResilienceConfig()

        assert isinstance(config.bulkheads, tuple)

    def test_get_bulkhead(self) -> None:
        """get_bulkhead() retrieves config by task type."""
        config = ResilienceConfig()

        shell = config.get_bulkhead("shell")
        assert shell is not None
        assert shell.max_concurrent == 5

        python = config.get_bulkhead("python")
        assert python is not None
        assert python.max_concurrent == 3

        unknown = config.get_bulkhead("unknown")
        assert unknown is None

    def test_bulkheads_dict(self) -> None:
        """bulkheads_dict() returns mutable dict copy."""
        config = ResilienceConfig()

        result = config.bulkheads_dict()

        assert isinstance(result, dict)
        assert "shell" in result
        assert "python" in result

        # Modifying dict doesn't affect config
        result["custom"] = BulkheadConfig()
        assert config.get_bulkhead("custom") is None

    def test_config_fingerprint_deterministic(self) -> None:
        """Same config produces same fingerprint."""
        config1 = ResilienceConfig()
        config2 = ResilienceConfig()

        assert config1.config_fingerprint() == config2.config_fingerprint()

    def test_config_fingerprint_changes_with_values(self) -> None:
        """Different configs produce different fingerprints."""
        config1 = ResilienceConfig(circuit_cooldown_seconds=30.0)
        config2 = ResilienceConfig(circuit_cooldown_seconds=60.0)

        assert config1.config_fingerprint() != config2.config_fingerprint()

    def test_from_env_returns_frozen(self) -> None:
        """from_env() returns frozen instance."""
        config = ResilienceConfig.from_env()

        with pytest.raises(Exception):
            config.circuit_cache_size = 2000  # type: ignore


class TestWorkflowConfigVersion:
    """Tests for config_version field on Workflow."""

    def test_workflow_has_config_version_field(self) -> None:
        """Workflow model has config_version field."""
        from stabilize.models.workflow import Workflow

        workflow = Workflow(
            application="test",
            name="test-workflow",
        )

        assert hasattr(workflow, "config_version")
        assert workflow.config_version is None

    def test_config_version_can_be_set(self) -> None:
        """config_version can be set on workflow."""
        from stabilize.models.workflow import Workflow

        config = HandlerConfig()
        fingerprint = config.config_fingerprint()

        workflow = Workflow(
            application="test",
            name="test-workflow",
            config_version=fingerprint,
        )

        assert workflow.config_version == fingerprint
