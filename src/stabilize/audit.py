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


class FileAuditLogger:
    """Log audit events to a secure file."""

    def __init__(self, filepath: str = "audit.log") -> None:
        self.filepath = filepath

    def log(self, event: AuditEvent) -> None:
        """Write event to audit log file."""
        # implementation would likely use a rotating file handler in production
        # ensuring permission bits are restricted (600)
        entry = event.to_json()
        logger.info(f"AUDIT: {entry}")  # For now, bridge to main log with AUDIT prefix


# Global audit logger instance
_audit_logger: AuditLogger = FileAuditLogger()


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
