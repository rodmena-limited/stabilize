"""
Resilience configuration for Stabilize.

Provides configuration for bulkheads, circuit breakers, and handler behavior,
with support for loading from environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from fractions import Fraction


@dataclass
class BackoffConfig:
    """Configuration for exponential backoff.

    Attributes:
        min_delay_ms: Minimum delay between retries in milliseconds
        max_delay_ms: Maximum delay between retries in milliseconds
        factor: Multiplication factor for exponential backoff
        jitter: Random jitter factor (0.0 to 1.0) to prevent thundering herd
    """

    min_delay_ms: int = 100
    max_delay_ms: int = 1000
    factor: float = 2.0
    jitter: float = 0.25


@dataclass
class HandlerConfig:
    """Handler retry and backoff settings.

    This configuration controls how handlers retry on ConcurrencyError and
    other transient failures. All values have sensible defaults that match
    the previous hardcoded behavior for backward compatibility.

    Environment Variables:
        STABILIZE_HANDLER_MAX_RETRIES: Max ConcurrencyError retries (default: 3, 0 to disable)
        STABILIZE_HANDLER_MIN_DELAY_MS: Min backoff delay in ms (default: 100)
        STABILIZE_HANDLER_MAX_DELAY_MS: Max backoff delay in ms (default: 1000)
        STABILIZE_HANDLER_BACKOFF_FACTOR: Backoff multiplication factor (default: 2.0)
        STABILIZE_HANDLER_JITTER: Backoff jitter factor (default: 0.25)

        STABILIZE_ERROR_MAX_RETRIES: Max retries for error handling paths (default: 10)
        STABILIZE_ERROR_MIN_DELAY_MS: Min backoff for error handling (default: 25)
        STABILIZE_ERROR_MAX_DELAY_MS: Max backoff for error handling (default: 10000)
        STABILIZE_ERROR_BACKOFF_FACTOR: Backoff factor for error handling (default: 2.0)
        STABILIZE_ERROR_JITTER: Jitter for error handling (default: 0.3)

        STABILIZE_MAX_STAGE_WAIT_RETRIES: Max retries waiting for stages (default: 240)
        STABILIZE_DEFAULT_TASK_TIMEOUT_S: Default task timeout in seconds (default: 14400 = 4 hours)
        STABILIZE_TASK_BACKOFF_MIN_MS: Task retry min delay in ms (default: 1000)
        STABILIZE_TASK_BACKOFF_MAX_MS: Task retry max delay in ms (default: 60000)
        STABILIZE_HANDLER_RETRY_DELAY_S: Re-queue delay in seconds (default: 15)

        STABILIZE_PROCESSOR_POLL_MS: Queue poll frequency in ms (default: 50)
        STABILIZE_PROCESSOR_MAX_WORKERS: Max worker threads (default: 10)
        STABILIZE_DEFAULT_PAGE_SIZE: Default page size for queries (default: 100)

    Attributes:
        concurrency_max_retries: Max retries for ConcurrencyError. Set to 0 to disable.
        concurrency_min_delay_ms: Min backoff delay for concurrency retries
        concurrency_max_delay_ms: Max backoff delay for concurrency retries
        concurrency_backoff_factor: Exponential backoff factor
        concurrency_jitter: Jitter factor to prevent thundering herd

        error_handling_max_retries: Max retries for error handling paths (more aggressive)
        error_handling_min_delay_ms: Min backoff for error handling (starts faster)
        error_handling_max_delay_ms: Max backoff for error handling (allows longer)
        error_handling_backoff_factor: Backoff factor for error handling
        error_handling_jitter: Higher jitter to reduce thundering herd

        max_stage_wait_retries: Max retries waiting for upstream stages
            (with 15s delay, 240 retries = 1 hour)
        default_task_timeout_seconds: Default timeout for tasks that don't specify one
        task_backoff_min_delay_ms: Min backoff for task retries
        task_backoff_max_delay_ms: Max backoff for task retries
        handler_retry_delay_seconds: Delay before re-queuing messages

        poll_frequency_ms: How often to poll the queue
        max_workers: Maximum concurrent handler threads
        default_page_size: Default page size for repository queries
    """

    # Concurrency retry (for ConcurrencyError)
    concurrency_max_retries: int = 3  # 0 disables retries
    concurrency_min_delay_ms: int = 100
    concurrency_max_delay_ms: int = 1000
    concurrency_backoff_factor: float = 2.0
    concurrency_jitter: float = 0.25

    # Error handling retry (more aggressive for critical paths)
    # Used when recording errors - we should try harder to persist error info
    error_handling_max_retries: int = 10  # 2x normal for critical paths
    error_handling_min_delay_ms: int = 25  # Start faster
    error_handling_max_delay_ms: int = 10000  # Allow longer backoff
    error_handling_backoff_factor: float = 2.0
    error_handling_jitter: float = 0.3  # More jitter to reduce thundering herd

    # Long-running retry limits
    max_stage_wait_retries: int = 240  # With 15s delay = 1 hour

    # Task execution
    # Default timeout for task execution (4 hours for long-running workflows)
    # Override with STABILIZE_DEFAULT_TASK_TIMEOUT_S environment variable
    default_task_timeout_seconds: float = 14400.0  # 4 hours
    task_backoff_min_delay_ms: int = 1000
    task_backoff_max_delay_ms: int = 60000

    # Handler retry delay
    handler_retry_delay_seconds: float = 15.0

    # Processor settings
    poll_frequency_ms: int = 50
    max_workers: int = 10
    default_page_size: int = 100

    @classmethod
    def from_env(cls) -> HandlerConfig:
        """Load configuration from environment variables with defaults.

        All settings have sensible defaults that match previous hardcoded values,
        ensuring backward compatibility for existing deployments.

        Returns:
            HandlerConfig with values loaded from environment or defaults
        """
        return cls(
            # Concurrency retry settings
            concurrency_max_retries=int(os.getenv("STABILIZE_HANDLER_MAX_RETRIES", "3")),
            concurrency_min_delay_ms=int(os.getenv("STABILIZE_HANDLER_MIN_DELAY_MS", "100")),
            concurrency_max_delay_ms=int(os.getenv("STABILIZE_HANDLER_MAX_DELAY_MS", "1000")),
            concurrency_backoff_factor=float(os.getenv("STABILIZE_HANDLER_BACKOFF_FACTOR", "2.0")),
            concurrency_jitter=float(os.getenv("STABILIZE_HANDLER_JITTER", "0.25")),
            # Error handling retry settings (more aggressive for critical paths)
            error_handling_max_retries=int(os.getenv("STABILIZE_ERROR_MAX_RETRIES", "10")),
            error_handling_min_delay_ms=int(os.getenv("STABILIZE_ERROR_MIN_DELAY_MS", "25")),
            error_handling_max_delay_ms=int(os.getenv("STABILIZE_ERROR_MAX_DELAY_MS", "10000")),
            error_handling_backoff_factor=float(os.getenv("STABILIZE_ERROR_BACKOFF_FACTOR", "2.0")),
            error_handling_jitter=float(os.getenv("STABILIZE_ERROR_JITTER", "0.3")),
            # Stage wait retry settings
            max_stage_wait_retries=int(os.getenv("STABILIZE_MAX_STAGE_WAIT_RETRIES", "240")),
            # Task execution settings (4 hours default for long-running workflows)
            default_task_timeout_seconds=float(os.getenv("STABILIZE_DEFAULT_TASK_TIMEOUT_S", "14400")),
            task_backoff_min_delay_ms=int(os.getenv("STABILIZE_TASK_BACKOFF_MIN_MS", "1000")),
            task_backoff_max_delay_ms=int(os.getenv("STABILIZE_TASK_BACKOFF_MAX_MS", "60000")),
            # Handler retry delay
            handler_retry_delay_seconds=float(os.getenv("STABILIZE_HANDLER_RETRY_DELAY_S", "15")),
            # Processor settings
            poll_frequency_ms=int(os.getenv("STABILIZE_PROCESSOR_POLL_MS", "50")),
            max_workers=int(os.getenv("STABILIZE_PROCESSOR_MAX_WORKERS", "10")),
            default_page_size=int(os.getenv("STABILIZE_DEFAULT_PAGE_SIZE", "100")),
        )

    def get_backoff_config(self) -> BackoffConfig:
        """Get BackoffConfig for concurrency retries."""
        return BackoffConfig(
            min_delay_ms=self.concurrency_min_delay_ms,
            max_delay_ms=self.concurrency_max_delay_ms,
            factor=self.concurrency_backoff_factor,
            jitter=self.concurrency_jitter,
        )

    def get_error_handling_backoff_config(self) -> BackoffConfig:
        """Get BackoffConfig for error handling retries (more aggressive)."""
        return BackoffConfig(
            min_delay_ms=self.error_handling_min_delay_ms,
            max_delay_ms=self.error_handling_max_delay_ms,
            factor=self.error_handling_backoff_factor,
            jitter=self.error_handling_jitter,
        )


# Singleton for default handler config (loaded lazily)
_default_handler_config: HandlerConfig | None = None


def get_handler_config() -> HandlerConfig:
    """Get the default HandlerConfig, loading from environment on first call.

    Returns:
        The singleton HandlerConfig instance
    """
    global _default_handler_config
    if _default_handler_config is None:
        _default_handler_config = HandlerConfig.from_env()
    return _default_handler_config


def reset_handler_config() -> None:
    """Reset the handler config singleton. Useful for testing."""
    global _default_handler_config
    _default_handler_config = None


@dataclass
class BulkheadConfig:
    """Configuration for a single bulkhead.

    Attributes:
        max_concurrent: Maximum concurrent executions in this bulkhead
        max_queue_size: Maximum queued tasks waiting for execution
        timeout_seconds: Default timeout for bulkhead execution (4 hours)
    """

    max_concurrent: int = 5
    max_queue_size: int = 20
    timeout_seconds: float = 14400.0  # 4 hours for long-running workflows


@dataclass
class ResilienceConfig:
    """
    Configuration for resilience patterns.

    Attributes:
        bulkheads: Per-task-type bulkhead configurations
        circuit_failure_threshold: Fraction of failures to trip circuit (e.g., 5/10 = 50%)
        circuit_cooldown_seconds: Seconds to wait before testing recovery
        database_url: Database connection string (from MG_DATABASE_URL)
    """

    bulkheads: dict[str, BulkheadConfig] = field(
        default_factory=lambda: {
            "shell": BulkheadConfig(max_concurrent=5),
            "python": BulkheadConfig(max_concurrent=3),
            "http": BulkheadConfig(max_concurrent=10),
            "docker": BulkheadConfig(max_concurrent=3),
            "ssh": BulkheadConfig(max_concurrent=5),
        }
    )
    circuit_failure_threshold: Fraction = field(default_factory=lambda: Fraction(5, 10))
    circuit_cooldown_seconds: float = 30.0
    circuit_cache_size: int = 1000
    database_url: str | None = None

    @classmethod
    def from_env(cls) -> ResilienceConfig:
        """
        Load configuration from environment variables.

        Environment variables:
            MG_DATABASE_URL: Database connection string
            STABILIZE_BULKHEAD_{TYPE}_MAX_CONCURRENT: Max concurrent for task type
            STABILIZE_CIRCUIT_FAILURE_THRESHOLD: Failure rate to trip (0.0-1.0)
            STABILIZE_CIRCUIT_COOLDOWN_SECONDS: Cooldown duration
        """
        # Load bulkhead configs from environment
        bulkheads = {
            "shell": BulkheadConfig(max_concurrent=int(os.environ.get("STABILIZE_BULKHEAD_SHELL_MAX_CONCURRENT", "5"))),
            "python": BulkheadConfig(
                max_concurrent=int(os.environ.get("STABILIZE_BULKHEAD_PYTHON_MAX_CONCURRENT", "3"))
            ),
            "http": BulkheadConfig(max_concurrent=int(os.environ.get("STABILIZE_BULKHEAD_HTTP_MAX_CONCURRENT", "10"))),
            "docker": BulkheadConfig(
                max_concurrent=int(os.environ.get("STABILIZE_BULKHEAD_DOCKER_MAX_CONCURRENT", "3"))
            ),
            "ssh": BulkheadConfig(max_concurrent=int(os.environ.get("STABILIZE_BULKHEAD_SSH_MAX_CONCURRENT", "5"))),
        }

        # Parse failure threshold as fraction
        threshold_float = float(os.environ.get("STABILIZE_CIRCUIT_FAILURE_THRESHOLD", "0.5"))
        # Convert to fraction (e.g., 0.5 -> 5/10)
        threshold_int = int(threshold_float * 10)
        circuit_failure_threshold = Fraction(threshold_int, 10)

        return cls(
            bulkheads=bulkheads,
            circuit_failure_threshold=circuit_failure_threshold,
            circuit_cooldown_seconds=float(os.environ.get("STABILIZE_CIRCUIT_COOLDOWN_SECONDS", "30")),
            circuit_cache_size=int(os.environ.get("STABILIZE_CIRCUIT_CACHE_SIZE", "1000")),
            database_url=os.environ.get("MG_DATABASE_URL"),
        )
