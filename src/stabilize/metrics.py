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
from collections.abc import Callable, Iterable
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


class OpenTelemetryMetricsProvider(MetricsProvider):
    """Counters, histograms and gauges on an OpenTelemetry meter named ``stabilize``.

    Instruments are created on first use, one per metric name. Values go to whatever
    MeterProvider the application configured (the global one unless *meter_provider*
    is given), so stabilize's metrics leave through the application's exporter.
    """

    def __init__(self, meter_provider: Any | None = None) -> None:
        from opentelemetry import metrics as otel

        source = meter_provider if meter_provider is not None else otel.get_meter_provider()
        self._meter = source.get_meter("stabilize")
        self._counters: dict[str, Any] = {}
        self._histograms: dict[str, Any] = {}
        self._gauges: dict[str, Any] = {}
        self._gauge_values: dict[str, dict[tuple[tuple[str, str], ...], float]] = {}

    def increment(self, name: str, value: float = 1.0, tags: dict[str, str] | None = None) -> None:
        counter = self._counters.get(name)
        if counter is None:
            counter = self._counters[name] = self._meter.create_counter(name)
        counter.add(value, attributes=dict(tags or {}))

    def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        histogram = self._histograms.get(name)
        if histogram is None:
            histogram = self._histograms[name] = self._meter.create_histogram(name)
        histogram.record(value, attributes=dict(tags or {}))

    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        attributes = dict(tags or {})
        gauge = self._gauges.get(name)
        if gauge is None:
            create = getattr(self._meter, "create_gauge", None)
            if create is not None:
                gauge = self._gauges[name] = create(name)
            else:
                self._gauge_values[name] = {}
                gauge = self._gauges[name] = self._meter.create_observable_gauge(name, callbacks=[self._observe(name)])
        if hasattr(gauge, "set"):
            gauge.set(value, attributes=attributes)
        else:
            self._gauge_values[name][tuple(sorted(attributes.items()))] = value

    def _observe(self, name: str) -> Callable[[Any], Iterable[Any]]:
        from opentelemetry.metrics import Observation

        def callback(options: Any) -> Iterable[Any]:
            for key, value in self._gauge_values.get(name, {}).items():
                yield Observation(value, dict(key))

        return callback


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
        import opentelemetry.metrics  # noqa: F401
    except ImportError:
        set_metrics_provider(NoOpMetricsProvider())
        return
    set_metrics_provider(OpenTelemetryMetricsProvider())
    logger.info("Using OpenTelemetryMetricsProvider")


# Convenience wrappers
def _tags(tags: dict[str, Any]) -> dict[str, str]:
    """String tags, plus the bound tenant when there is one."""
    from stabilize.persistence.tenancy import current_tenant

    labelled = {k: str(v) for k, v in tags.items()}
    tenant = current_tenant()
    if tenant:
        labelled.setdefault("tenant", tenant)
    return labelled


def increment(name: str, value: float = 1.0, **tags: Any) -> None:
    """Increment metric with tags as kwargs."""
    _provider.increment(name, value, _tags(tags))


def gauge(name: str, value: float, **tags: Any) -> None:
    """Set gauge with tags as kwargs."""
    _provider.gauge(name, value, _tags(tags))


def histogram(name: str, value: float, **tags: Any) -> None:
    """Record histogram with tags as kwargs."""
    _provider.histogram(name, value, _tags(tags))


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
