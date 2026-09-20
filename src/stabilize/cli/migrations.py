"""Migration file utilities for Stabilize CLI."""

from __future__ import annotations

import hashlib
import re
from importlib.resources import files
from pathlib import Path

PACKAGE_LOCATION = "stabilize.migrations"
CHECKOUT_LOCATION = "<repository root>/migrations"


class MigrationsNotFoundError(RuntimeError):
    """No migration files could be located in any known location."""


def _package_migrations() -> list[tuple[str, str]]:
    try:
        pkg = files(PACKAGE_LOCATION)
    except (ModuleNotFoundError, FileNotFoundError):
        return []
    try:
        return [
            (item.name, item.read_text())
            for item in pkg.iterdir()
            if item.name.endswith(".sql")
        ]
    except (FileNotFoundError, NotADirectoryError):
        return []


def _checkout_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent.parent


def _checkout_migrations() -> list[tuple[str, str]]:
    root = _checkout_root() / "migrations"
    if not root.is_dir():
        return []
    return [(p.name, p.read_text()) for p in root.glob("*.sql")]


def get_migrations() -> list[tuple[str, str]]:
    """Return every migration as (filename, sql), in ULID order.

    The .sql files live at the repository root and are force-included into the
    wheel under stabilize/migrations at build time, so an installed package and
    a source checkout keep them in different places.
    """
    migrations = _package_migrations() or _checkout_migrations()

    if not migrations:
        raise MigrationsNotFoundError(
            "No migration files found. Searched the installed package "
            f"({PACKAGE_LOCATION}) and the source checkout "
            f"({_checkout_root() / 'migrations'}). An installed wheel carries "
            "them in the package; a checkout carries them at the repository "
            "root."
        )

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
