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


def _package_migrations() -> list[tuple[str, bytes]]:
    try:
        pkg = files(PACKAGE_LOCATION)
    except (ModuleNotFoundError, FileNotFoundError):
        return []
    try:
        return [(item.name, item.read_bytes()) for item in pkg.iterdir() if item.name.endswith(".sql")]
    except (FileNotFoundError, NotADirectoryError):
        return []


def _checkout_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent.parent


def _checkout_migrations() -> list[tuple[str, bytes]]:
    root = _checkout_root() / "migrations"
    if not root.is_dir():
        return []
    return [(p.name, p.read_bytes()) for p in root.glob("*.sql")]


def get_migration_files() -> list[tuple[str, bytes]]:
    """Return every shipped migration as (filename, raw bytes), in ULID order.

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


def migration_text(raw: bytes) -> str:
    """Decode a migration file the way a text-mode read does (UTF-8, universal newlines)."""
    return raw.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")


def get_migrations() -> list[tuple[str, str]]:
    """Return every shipped migration as (filename, text), in ULID order."""
    return [(name, migration_text(raw)) for name, raw in get_migration_files()]


def migration_id(filename: str) -> str:
    """Return the migretti id of a migration filename (the part before the first underscore)."""
    return filename.split("_", 1)[0]


def extract_up_migration(content: str) -> str:
    """Extract the UP migration from SQL content."""
    up_match = re.search(
        r"--\s*migrate:\s*up\s*\n(.*?)(?:--\s*migrate:\s*down|$)",
        content,
        re.DOTALL | re.IGNORECASE,
    )
    if up_match:
        return up_match.group(1).strip()
    return content


def migretti_checksum(content: str) -> str:
    """Checksum migretti records in _migrations: SHA-256 of the text."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()
