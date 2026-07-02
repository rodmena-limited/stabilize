"""
Audit logging for Stabilize.

Provides an immutable, structured audit trail for compliance-critical operations.
Audits are distinct from operational logs:
- Operational logs: Debugging, performance, errors (for engineers)
- Audit logs: "Who did what and when" (for compliance/security officers)

Events recorded:
- Workflow creation/start
- Workflow cancellation/pause/resume
- Critical configuration changes
- Manual overrides (skip stage, force success)
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

logger = logging.getLogger("stabilize.audit")


@dataclass
class AuditEvent:
    """Represents a single audit event."""

    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    event_type: str = ""
    user: str = "system"
    resource_type: str = ""  # workflow, stage, system
    resource_id: str = ""
    action: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    ip_address: str | None = None

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(self.__dict__)


class AuditLogger(Protocol):
    """Protocol for audit loggers."""

    def log(self, event: AuditEvent) -> None: ...


class LogAuditLogger:
    """Bridge audit events to the process log with an AUDIT prefix."""

    def log(self, event: AuditEvent) -> None:
        """Log the event to the standard logger."""
        logger.info("AUDIT: %s", event.to_json())


class FileAuditLogger:
    """Append audit events as JSON lines to a file (created with mode 600)."""

    def __init__(self, filepath: str = "audit.log") -> None:
        self.filepath = filepath

    def log(self, event: AuditEvent) -> None:
        """Append the event to the audit log file and the process log."""
        entry = event.to_json()
        try:
            fd = os.open(self.filepath, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            with os.fdopen(fd, "a") as f:
                f.write(entry + "\n")
        except OSError as e:
            logger.warning("Failed to write audit log %s: %s", self.filepath, e)
        logger.info("AUDIT: %s", entry)


# Global audit logger instance. Log-only by default: merely importing/using
# stabilize must not start writing audit files into the working directory.
# Opt in to file auditing with set_audit_logger(FileAuditLogger(path)).
_audit_logger: AuditLogger = LogAuditLogger()


def set_audit_logger(logger: AuditLogger) -> None:
    """Set the global audit logger."""
    global _audit_logger
    _audit_logger = logger


def audit(
    event_type: str,
    action: str,
    user: str,
    resource_type: str,
    resource_id: str,
    details: dict[str, Any] | None = None,
    ip_address: str | None = None,
) -> None:
    """
    Record an audit event.

    Args:
        event_type: Category (e.g., "WORKFLOW_LIFECYCLE", "SECURITY")
        action: Specific action (e.g., "CREATE", "CANCEL", "OVERRIDE")
        user: User identifier (or "system")
        resource_type: Type of resource affected
        resource_id: ID of the resource
        details: Additional context
        ip_address: Origin IP (if web context)
    """
    event = AuditEvent(
        event_type=event_type,
        action=action,
        user=user,
        resource_type=resource_type,
        resource_id=resource_id,
        details=details or {},
        ip_address=ip_address,
    )
    _audit_logger.log(event)
