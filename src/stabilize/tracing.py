"""OpenTelemetry tracing for Stabilize.

This module provides distributed tracing using OpenTelemetry, enabling:
- End-to-end visibility across workflow execution
- Performance monitoring and bottleneck identification
- Integration with observability platforms (Jaeger, Zipkin, etc.)

Usage:
    from stabilize.tracing import configure_tracing, trace_operation

    # Configure once at application startup
    configure_tracing(service_name="my-service")

    # Trace operations
    with trace_operation("process_task", task_id="task-123") as span:
        result = do_work()
        span.set_attribute("result.size", len(result))

The tracing is optional - if OpenTelemetry is not installed,
all tracing operations become no-ops.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

# OpenTelemetry is optional - gracefully degrade to no-ops
try:
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.trace import Span, Status, StatusCode

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    trace = None  # type: ignore
    Span = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer


_tracer: Tracer | None = None
_configured = False


class NoOpSpan:
    """A no-op span for when OpenTelemetry is not available."""

    def set_attribute(self, key: str, value: Any) -> None:
        """No-op."""
        pass

    def set_status(self, status: Any) -> None:
        """No-op."""
        pass

    def record_exception(self, exception: BaseException) -> None:
        """No-op."""
        pass

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        """No-op."""
        pass

    def __enter__(self) -> NoOpSpan:
        return self

    def __exit__(self, *args: Any) -> None:
        pass


def configure_tracing(
    service_name: str = "stabilize",
    service_version: str | None = None,
    environment: str | None = None,
) -> None:
    """Configure OpenTelemetry tracing for Stabilize.

    Call this once at application startup. The TracerProvider should
    be configured with exporters separately (e.g., OTLP, Jaeger, Zipkin).

    Args:
        service_name: Name of the service (appears in traces)
        service_version: Optional version string
        environment: Optional environment (dev, staging, prod)

    Example:
        # Basic configuration
        configure_tracing(service_name="my-workflow-app")

        # With exporters (requires additional setup)
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        configure_tracing(service_name="my-app")
        tracer_provider = trace.get_tracer_provider()
        tracer_provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter())
        )
    """
    global _tracer, _configured

    if not OTEL_AVAILABLE:
        _configured = True
        return

    # Build resource attributes
    resource_attrs = {"service.name": service_name}
    if service_version:
        resource_attrs["service.version"] = service_version
    if environment:
        resource_attrs["deployment.environment"] = environment

    resource = Resource.create(resource_attrs)

    # Create and set tracer provider
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    # Get tracer for stabilize
    _tracer = trace.get_tracer(
        instrumenting_module_name="stabilize",
        instrumenting_library_version=service_version,
    )

    _configured = True


def get_tracer() -> Any:
    """Get the configured tracer.

    Returns:
        The OpenTelemetry tracer, or None if not configured/available.
    """
    global _tracer

    if not _configured:
        configure_tracing()

    if not OTEL_AVAILABLE:
        return None

    if _tracer is None:
        _tracer = trace.get_tracer("stabilize")

    return _tracer


@contextmanager
def trace_operation(
    name: str,
    **attributes: Any,
) -> Iterator[Any]:
    """Context manager for tracing an operation.

    Creates a span for the operation and automatically:
    - Sets provided attributes
    - Records exceptions if raised
    - Sets error status on failure

    Args:
        name: Name of the operation (e.g., "task.execute", "stage.start")
        **attributes: Attributes to set on the span

    Yields:
        The span (or NoOpSpan if tracing not available)

    Example:
        with trace_operation("process_message", message_type="StartTask") as span:
            result = process()
            span.set_attribute("result.success", True)
    """
    tracer = get_tracer()

    if tracer is None:
        yield NoOpSpan()
        return

    with tracer.start_as_current_span(name) as span:
        # Set initial attributes
        for key, value in attributes.items():
            if value is not None:
                span.set_attribute(key, str(value))

        try:
            yield span
        except Exception as e:
            # Record exception and set error status
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            raise


@contextmanager
def trace_workflow(workflow_id: str, workflow_name: str) -> Iterator[Any]:
    """Trace a workflow execution.

    Args:
        workflow_id: Unique workflow execution ID
        workflow_name: Name of the workflow

    Yields:
        The span
    """
    with trace_operation(
        "workflow.execute",
        workflow_id=workflow_id,
        workflow_name=workflow_name,
    ) as span:
        yield span


@contextmanager
def trace_stage(
    workflow_id: str,
    stage_id: str,
    stage_name: str,
) -> Iterator[Any]:
    """Trace a stage execution.

    Args:
        workflow_id: Parent workflow execution ID
        stage_id: Stage execution ID
        stage_name: Name of the stage

    Yields:
        The span
    """
    with trace_operation(
        "stage.execute",
        workflow_id=workflow_id,
        stage_id=stage_id,
        stage_name=stage_name,
    ) as span:
        yield span


@contextmanager
def trace_task(
    workflow_id: str,
    stage_id: str,
    task_id: str,
    task_name: str,
    task_type: str,
) -> Iterator[Any]:
    """Trace a task execution.

    Args:
        workflow_id: Parent workflow execution ID
        stage_id: Parent stage execution ID
        task_id: Task execution ID
        task_name: Name of the task
        task_type: Type of task (shell, python, http, etc.)

    Yields:
        The span
    """
    with trace_operation(
        "task.execute",
        workflow_id=workflow_id,
        stage_id=stage_id,
        task_id=task_id,
        task_name=task_name,
        task_type=task_type,
    ) as span:
        yield span


@contextmanager
def trace_message_processing(
    message_type: str,
    execution_id: str | None = None,
    stage_id: str | None = None,
) -> Iterator[Any]:
    """Trace message processing in the queue processor.

    Args:
        message_type: Type of message being processed
        execution_id: Optional workflow execution ID
        stage_id: Optional stage ID

    Yields:
        The span
    """
    with trace_operation(
        "message.process",
        message_type=message_type,
        execution_id=execution_id,
        stage_id=stage_id,
    ) as span:
        yield span


def add_event(name: str, attributes: dict[str, Any] | None = None) -> None:
    """Add an event to the current span.

    Args:
        name: Event name
        attributes: Optional event attributes
    """
    if not OTEL_AVAILABLE:
        return

    span = trace.get_current_span()
    if span:
        span.add_event(name, attributes=attributes or {})


def set_attribute(key: str, value: Any) -> None:
    """Set an attribute on the current span.

    Args:
        key: Attribute key
        value: Attribute value
    """
    if not OTEL_AVAILABLE:
        return

    span = trace.get_current_span()
    if span and value is not None:
        span.set_attribute(key, str(value))
