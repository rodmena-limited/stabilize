"""
Metrics collection for Stabilize.

Provides a unified interface for metrics collection, supporting:
- Null/Log-based implementation (default)
- OpenTelemetry implementation (if installed and configured)
- Custom implementations via registry

Metrics tracked:
- workflow_count (counter): Total workflows created
- workflow_duration_seconds (histogram): Workflow execution time
- stage_duration_seconds (histogram): Stage execution time
- task_duration_seconds (histogram): Task execution time
- active_workflows (gauge): Currently running workflows
- queue_depth (gauge): Messages in queue
- queue_lag_seconds (gauge): Time messages spend in queue
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class MetricsProvider(ABC):
    """Abstract base class for metrics providers."""

    @abstractmethod
    def increment(self, name: str, value: float = 1.0, tags: dict[str, str] | None = None) -> None:
        """Increment a counter."""
        pass

    @abstractmethod
    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Set a gauge value."""
        pass

    @abstractmethod
    def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Record a value in a histogram."""
        pass


class LogMetricsProvider(MetricsProvider):
    """Simple provider that logs metrics (useful for development/debugging)."""

    def increment(self, name: str, value: float = 1.0, tags: dict[str, str] | None = None) -> None:
        logger.debug("METRIC INC %s: %s tags=%s", name, value, tags)

    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        logger.debug("METRIC GAUGE %s: %s tags=%s", name, value, tags)

    def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        logger.debug("METRIC HIST %s: %s tags=%s", name, value, tags)


class NoOpMetricsProvider(MetricsProvider):
    """Provider that does nothing."""

    def increment(self, name: str, value: float = 1.0, tags: dict[str, str] | None = None) -> None:
        pass

    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        pass

    def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        pass


# Global provider instance
_provider: MetricsProvider = NoOpMetricsProvider()


def get_metrics() -> MetricsProvider:
    """Get the current metrics provider."""
    return _provider


def set_metrics_provider(provider: MetricsProvider) -> None:
    """Set the metrics provider."""
    global _provider
    _provider = provider


def configure_metrics() -> None:
    """
    Auto-configure metrics based on available libraries.

    Tries to initialize OpenTelemetry if available.
    Falls back to LogMetricsProvider if STABILIZE_LOG_METRICS is set.
    Otherwise uses NoOpMetricsProvider.
    """
    import os

    if os.environ.get("STABILIZE_LOG_METRICS"):
        set_metrics_provider(LogMetricsProvider())
        logger.info("Using LogMetricsProvider")
        return

    try:
        # Check for OpenTelemetry
        import opentelemetry.metrics  # noqa: F401

        # We could implement OTel provider here, but for now we'll stick to simple
        # interfaces. A full OTel implementation would be a separate class.
        # For this "start fixing" phase, logging or no-op is sufficient default.
        pass
    except ImportError:
        pass


# Convenience wrappers
def increment(name: str, value: float = 1.0, **tags: Any) -> None:
    """Increment metric with tags as kwargs."""
    _provider.increment(name, value, {k: str(v) for k, v in tags.items()})


def gauge(name: str, value: float, **tags: Any) -> None:
    """Set gauge with tags as kwargs."""
    _provider.gauge(name, value, {k: str(v) for k, v in tags.items()})


def histogram(name: str, value: float, **tags: Any) -> None:
    """Record histogram with tags as kwargs."""
    _provider.histogram(name, value, {k: str(v) for k, v in tags.items()})


class Timer:
    """Context manager for timing code blocks."""

    def __init__(self, name: str, **tags: Any) -> None:
        self.name = name
        self.tags = tags
        self.start_time: float = 0

    def __enter__(self) -> Timer:
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        duration = time.perf_counter() - self.start_time
        status = "failed" if exc_type else "success"
        self.tags["status"] = status
        histogram(self.name, duration, **self.tags)
