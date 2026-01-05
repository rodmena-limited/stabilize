"""Structured logging for Stabilize.

This module provides structured logging using structlog, enabling:
- JSON-formatted logs for production (machine-readable)
- Pretty console logs for development (human-readable)
- Automatic context binding (workflow_id, stage_id, task_id)
- Integration with standard library logging

Usage:
    from stabilize.logging import configure_logging, get_logger

    # Configure once at application startup
    configure_logging(json_format=True)  # For production

    # Get a logger
    logger = get_logger("my.module")
    logger.info("task_started", workflow_id="wf-123", task_id="task-456")

Context binding:
    logger = get_logger("handler").bind(workflow_id="wf-123")
    logger.info("starting")  # workflow_id automatically included
    logger.info("completed", result="success")  # workflow_id still included
"""

from __future__ import annotations

import logging
import sys
from typing import Any

# structlog is optional - gracefully degrade to stdlib logging
try:
    import structlog
    from structlog.types import Processor

    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False
    structlog = None  # type: ignore


_configured = False


def configure_logging(
    json_format: bool = False,
    level: int = logging.INFO,
    logger_factory: Any = None,
) -> None:
    """Configure structured logging for Stabilize.

    Call this once at application startup before any logging occurs.

    Args:
        json_format: If True, output JSON logs (for production).
                    If False, output pretty console logs (for development).
        level: Minimum log level (default: INFO)
        logger_factory: Custom logger factory (for testing)

    Example:
        # Development (pretty console output)
        configure_logging(json_format=False, level=logging.DEBUG)

        # Production (JSON for log aggregation)
        configure_logging(json_format=True)
    """
    global _configured

    if not STRUCTLOG_AVAILABLE:
        # Fall back to basic stdlib logging
        logging.basicConfig(
            level=level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            stream=sys.stdout,
        )
        _configured = True
        return

    # Configure stdlib logging (structlog wraps it)
    logging.basicConfig(
        level=level,
        format="%(message)s",
        stream=sys.stdout,
    )

    # Build processor chain
    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if json_format:
        # Production: JSON output
        processors.append(structlog.processors.JSONRenderer())
    else:
        # Development: Pretty console output
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.plain_traceback,
            )
        )

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=logger_factory or structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    _configured = True


def get_logger(name: str) -> Any:
    """Get a structured logger for the given name.

    Args:
        name: Logger name (typically __name__ or module path)

    Returns:
        A bound logger that supports structured logging.
        If structlog is not available, returns a stdlib logger.

    Example:
        logger = get_logger(__name__)
        logger.info("processing", item_id=123, status="started")
    """
    if not _configured:
        # Auto-configure with defaults if not explicitly configured
        configure_logging()

    if STRUCTLOG_AVAILABLE:
        return structlog.get_logger(name)
    else:
        return logging.getLogger(name)


def bind_context(**kwargs: Any) -> None:
    """Bind context variables that will be included in all subsequent logs.

    This is useful for adding request-scoped context like workflow_id
    that should appear in all log messages within a scope.

    Args:
        **kwargs: Context key-value pairs to bind

    Example:
        bind_context(workflow_id="wf-123", request_id="req-456")
        logger.info("processing")  # Includes workflow_id and request_id
    """
    if STRUCTLOG_AVAILABLE:
        structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    """Clear all bound context variables.

    Call this at the end of a request/workflow to prevent context leakage.
    """
    if STRUCTLOG_AVAILABLE:
        structlog.contextvars.clear_contextvars()


def unbind_context(*keys: str) -> None:
    """Remove specific context variables.

    Args:
        *keys: Context keys to remove
    """
    if STRUCTLOG_AVAILABLE:
        structlog.contextvars.unbind_contextvars(*keys)


# Convenience loggers for common Stabilize components
def workflow_logger(workflow_id: str) -> Any:
    """Get a logger pre-bound with workflow context.

    Args:
        workflow_id: The workflow execution ID

    Returns:
        Logger with workflow_id bound
    """
    logger = get_logger("stabilize.workflow")
    if STRUCTLOG_AVAILABLE:
        return logger.bind(workflow_id=workflow_id)
    return logger


def stage_logger(workflow_id: str, stage_id: str) -> Any:
    """Get a logger pre-bound with stage context.

    Args:
        workflow_id: The workflow execution ID
        stage_id: The stage execution ID

    Returns:
        Logger with workflow_id and stage_id bound
    """
    logger = get_logger("stabilize.stage")
    if STRUCTLOG_AVAILABLE:
        return logger.bind(workflow_id=workflow_id, stage_id=stage_id)
    return logger


def task_logger(workflow_id: str, stage_id: str, task_id: str) -> Any:
    """Get a logger pre-bound with task context.

    Args:
        workflow_id: The workflow execution ID
        stage_id: The stage execution ID
        task_id: The task execution ID

    Returns:
        Logger with full task context bound
    """
    logger = get_logger("stabilize.task")
    if STRUCTLOG_AVAILABLE:
        return logger.bind(
            workflow_id=workflow_id,
            stage_id=stage_id,
            task_id=task_id,
        )
    return logger
