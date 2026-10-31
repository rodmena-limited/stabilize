"""Durable subscription names are per tenant: the stored id is the tenant, a unit separator and the name."""

from __future__ import annotations

SEPARATOR = "\x1f"
MAX_NAME_LENGTH = 100


def validate_name(name: str) -> str:
    if not isinstance(name, str) or not name:
        raise ValueError("a subscription name must be a non-empty string")
    if len(name) > MAX_NAME_LENGTH:
        raise ValueError(f"subscription name {name[:20]!r}... is longer than {MAX_NAME_LENGTH} characters")
    if any(ord(c) < 32 or ord(c) == 127 for c in name):
        raise ValueError("a subscription name must not contain control characters")
    return name


def qualify(tenant: str, name: str) -> str:
    """The stored id for *name* under *tenant*; the '' tenant keeps bare names for 0.33 compatibility."""
    validate_name(name)
    return f"{tenant}{SEPARATOR}{name}" if tenant else name


def unqualify(tenant: str, stored_id: str) -> str:
    prefix = f"{tenant}{SEPARATOR}" if tenant else ""
    return stored_id[len(prefix) :] if prefix and stored_id.startswith(prefix) else stored_id
