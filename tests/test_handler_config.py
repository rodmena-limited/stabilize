"""
Tests for HandlerConfig and configurable retry/backoff behavior.

Tests that:
1. HandlerConfig loads defaults correctly
2. HandlerConfig loads from environment variables
3. Setting max_retries to 0 disables retries
4. QueueProcessorConfig can be created from HandlerConfig
"""

from __future__ import annotations

import os
from datetime import timedelta
from unittest.mock import patch

from stabilize.resilience.config import (
    BackoffConfig,
    HandlerConfig,
    get_handler_config,
    reset_handler_config,
)


class TestBackoffConfig:
    """Tests for BackoffConfig dataclass."""

    def test_default_values(self) -> None:
        """Test that default values are correct."""
        config = BackoffConfig()

        assert config.min_delay_ms == 100
        assert config.max_delay_ms == 1000
        assert config.factor == 2.0
        assert config.jitter == 0.25


class TestHandlerConfig:
    """Tests for HandlerConfig dataclass."""

    def setup_method(self) -> None:
        """Reset singleton before each test."""
        reset_handler_config()

    def teardown_method(self) -> None:
        """Reset singleton after each test."""
        reset_handler_config()

    def test_default_values(self) -> None:
        """Test that default values match previous hardcoded values."""
        config = HandlerConfig()

        # Concurrency retry defaults
        assert config.concurrency_max_retries == 3
        assert config.concurrency_min_delay_ms == 100
        assert config.concurrency_max_delay_ms == 1000
        assert config.concurrency_backoff_factor == 2.0
        assert config.concurrency_jitter == 0.25

        # Long-running retry defaults
        assert config.max_stage_wait_retries == 240

        # Task execution defaults
        assert config.default_task_timeout_seconds == 300.0
        assert config.task_backoff_min_delay_ms == 1000
        assert config.task_backoff_max_delay_ms == 60000

        # Handler retry delay default
        assert config.handler_retry_delay_seconds == 15.0

        # Processor settings defaults
        assert config.poll_frequency_ms == 50
        assert config.max_workers == 10
        assert config.default_page_size == 100

    def test_from_env_with_defaults(self) -> None:
        """Test from_env() returns defaults when no env vars set."""
        # Ensure env vars are not set
        env_vars = [
            "STABILIZE_HANDLER_MAX_RETRIES",
            "STABILIZE_HANDLER_MIN_DELAY_MS",
            "STABILIZE_HANDLER_MAX_DELAY_MS",
            "STABILIZE_MAX_STAGE_WAIT_RETRIES",
            "STABILIZE_DEFAULT_TASK_TIMEOUT_S",
            "STABILIZE_HANDLER_RETRY_DELAY_S",
            "STABILIZE_PROCESSOR_POLL_MS",
            "STABILIZE_PROCESSOR_MAX_WORKERS",
        ]

        # Clear env vars
        with patch.dict(os.environ, {}, clear=True):
            for var in env_vars:
                os.environ.pop(var, None)

            config = HandlerConfig.from_env()

            assert config.concurrency_max_retries == 3
            assert config.handler_retry_delay_seconds == 15.0

    def test_from_env_custom_values(self) -> None:
        """Test from_env() loads custom values from environment."""
        custom_env = {
            "STABILIZE_HANDLER_MAX_RETRIES": "0",  # Disable retries
            "STABILIZE_HANDLER_MIN_DELAY_MS": "50",
            "STABILIZE_HANDLER_MAX_DELAY_MS": "500",
            "STABILIZE_MAX_STAGE_WAIT_RETRIES": "100",
            "STABILIZE_DEFAULT_TASK_TIMEOUT_S": "600",
            "STABILIZE_HANDLER_RETRY_DELAY_S": "30",
            "STABILIZE_PROCESSOR_POLL_MS": "100",
            "STABILIZE_PROCESSOR_MAX_WORKERS": "20",
            "STABILIZE_DEFAULT_PAGE_SIZE": "50",
        }

        with patch.dict(os.environ, custom_env):
            config = HandlerConfig.from_env()

            assert config.concurrency_max_retries == 0
            assert config.concurrency_min_delay_ms == 50
            assert config.concurrency_max_delay_ms == 500
            assert config.max_stage_wait_retries == 100
            assert config.default_task_timeout_seconds == 600.0
            assert config.handler_retry_delay_seconds == 30.0
            assert config.poll_frequency_ms == 100
            assert config.max_workers == 20
            assert config.default_page_size == 50

    def test_get_backoff_config(self) -> None:
        """Test get_backoff_config() creates BackoffConfig from settings."""
        config = HandlerConfig(
            concurrency_min_delay_ms=200,
            concurrency_max_delay_ms=2000,
            concurrency_backoff_factor=3.0,
            concurrency_jitter=0.5,
        )

        backoff = config.get_backoff_config()

        assert backoff.min_delay_ms == 200
        assert backoff.max_delay_ms == 2000
        assert backoff.factor == 3.0
        assert backoff.jitter == 0.5

    def test_singleton_behavior(self) -> None:
        """Test get_handler_config() returns singleton."""
        config1 = get_handler_config()
        config2 = get_handler_config()

        assert config1 is config2

    def test_reset_singleton(self) -> None:
        """Test reset_handler_config() clears singleton."""
        config1 = get_handler_config()
        reset_handler_config()
        config2 = get_handler_config()

        # Should be new instance after reset
        assert config1 is not config2


class TestQueueProcessorConfigIntegration:
    """Tests for QueueProcessorConfig.from_handler_config()."""

    def setup_method(self) -> None:
        """Reset singleton before each test."""
        reset_handler_config()

    def teardown_method(self) -> None:
        """Reset singleton after each test."""
        reset_handler_config()

    def test_from_handler_config_defaults(self) -> None:
        """Test QueueProcessorConfig created from default HandlerConfig."""
        from stabilize.queue.processor import QueueProcessorConfig

        config = QueueProcessorConfig.from_handler_config()

        assert config.poll_frequency_ms == 50
        assert config.max_workers == 10
        assert config.retry_delay == timedelta(seconds=15)

    def test_from_handler_config_custom(self) -> None:
        """Test QueueProcessorConfig created from custom HandlerConfig."""
        from stabilize.queue.processor import QueueProcessorConfig

        handler_config = HandlerConfig(
            poll_frequency_ms=100,
            max_workers=20,
            handler_retry_delay_seconds=30.0,
        )

        config = QueueProcessorConfig.from_handler_config(handler_config)

        assert config.poll_frequency_ms == 100
        assert config.max_workers == 20
        assert config.retry_delay == timedelta(seconds=30)


class TestRetryDisabling:
    """Tests that setting max_retries=0 disables retries."""

    def test_retry_on_concurrency_error_disabled(self) -> None:
        """Test that retry_on_concurrency_error runs once when retries disabled."""
        from unittest.mock import MagicMock

        from stabilize.handlers.base import StabilizeHandler

        # Create a mock handler with retries disabled
        config = HandlerConfig(concurrency_max_retries=0)
        handler = MagicMock(spec=StabilizeHandler)
        handler.handler_config = config

        # Call the actual method
        call_count = 0

        def func_that_counts() -> None:
            nonlocal call_count
            call_count += 1

        # Use the actual implementation
        StabilizeHandler.retry_on_concurrency_error(handler, func_that_counts, "test")

        # Should have been called exactly once
        assert call_count == 1
