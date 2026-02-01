"""Migration file utilities for Stabilize CLI."""

from __future__ import annotations

import hashlib
import re
from importlib.resources import files


def get_migrations() -> list[tuple[str, str]]:
    """Get all migration files from the package."""
    migrations_pkg = files("stabilize.migrations")
    migrations = []

    for item in migrations_pkg.iterdir():
        if item.name.endswith(".sql"):
            content = item.read_text()
            migrations.append((item.name, content))

    # Sort by filename (ULID prefix ensures chronological order)
    migrations.sort(key=lambda x: x[0])
    return migrations


def extract_up_migration(content: str) -> str:
    """Extract the UP migration from SQL content."""
    # Find content between "-- migrate: up" and "-- migrate: down"
    up_match = re.search(
        r"--\s*migrate:\s*up\s*\n(.*?)(?:--\s*migrate:\s*down|$)",
        content,
        re.DOTALL | re.IGNORECASE,
    )
    if up_match:
        return up_match.group(1).strip()
    return content


def compute_checksum(content: str) -> str:
    """Compute MD5 checksum of migration content."""
    return hashlib.md5(content.encode()).hexdigest()
