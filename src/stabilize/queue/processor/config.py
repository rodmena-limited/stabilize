"""
Configuration for the queue processor.

This module provides the QueueProcessorConfig dataclass with
values that can be loaded from environment variables via HandlerConfig.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from stabilize.resilience.config import HandlerConfig, get_handler_config


@dataclass
class QueueProcessorConfig:
    """Configuration for the queue processor.

    Values can be loaded from environment variables via HandlerConfig.
    See HandlerConfig documentation for environment variable names.
    """

    # How often to poll the queue (milliseconds)
    poll_frequency_ms: int = 50

    # Maximum number of concurrent message handlers
    max_workers: int = 10

    # Delay before reprocessing a failed message
    retry_delay: timedelta = timedelta(seconds=15)

    # Whether to stop on unhandled exceptions
    stop_on_error: bool = False

    # Enable message deduplication for idempotency
    enable_deduplication: bool = True

    # --- Automatic crash recovery (opt-in; all default to disabled) ---
    # Run a one-shot recovery sweep when start() is called. This re-queues
    # workflows that were interrupted by a crash/restart. Requires a store.
    recover_on_start: bool = False
    # If > 0, run periodic recovery sweeps on a background thread every N
    # seconds (for long-running / distributed deployments where a peer worker
    # may have died). 0 disables periodic recovery. Relies on the same
    # idempotent recovery path as recover_on_start.
    recovery_interval_seconds: float = 0.0
    # Optional application filter for recovery sweeps.
    recovery_application: str | None = None
    # Only recover workflows started within this many hours.
    recovery_max_age_hours: float = 24.0

    @classmethod
    def from_handler_config(cls, handler_config: HandlerConfig | None = None) -> QueueProcessorConfig:
        """Create QueueProcessorConfig from HandlerConfig.

        Args:
            handler_config: HandlerConfig to use. If None, loads from environment.

        Returns:
            QueueProcessorConfig with values from HandlerConfig
        """
        config = handler_config or get_handler_config()
        return cls(
            poll_frequency_ms=config.poll_frequency_ms,
            max_workers=config.max_workers,
            retry_delay=timedelta(seconds=config.handler_retry_delay_seconds),
        )
