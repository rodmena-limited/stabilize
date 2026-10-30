"""psycopg import and driver disclosure for the database CLI commands."""

from __future__ import annotations

import sys
from typing import Any

PSYCOPG_DISTRIBUTIONS = frozenset({"psycopg", "psycopg_pool"})


def print_install_hint() -> None:
    print("Error: psycopg not installed")
    print("Install with: pip install stabilize[postgres]")


def import_psycopg() -> Any:
    """Import psycopg, printing the install hint only when psycopg itself is absent.

    Any other ImportError, including a psycopg_c that does not match the
    installed psycopg, propagates unchanged with its own message.
    """
    try:
        import psycopg
    except ModuleNotFoundError as exc:
        if exc.name not in PSYCOPG_DISTRIBUTIONS:
            raise
        print_install_hint()
        sys.exit(1)
    return psycopg


def describe_driver(psycopg: Any) -> str:
    """Describe the psycopg implementation and the libpq that will verify the connection."""
    impl = psycopg.pq.__impl__
    libpq = psycopg.pq.version()
    source = "bundled inside the psycopg-binary wheel" if impl == "binary" else "system libpq"
    return f"psycopg {psycopg.__version__} impl={impl}, libpq {libpq} ({source})"


def announce_driver(psycopg: Any) -> None:
    """Print which driver and libpq will open the connection, refusing when that cannot be determined."""
    try:
        description = describe_driver(psycopg)
    except Exception as exc:
        print(f"Error: cannot determine the psycopg implementation or libpq version: {type(exc).__name__}")
        print("Refusing to connect without knowing which libpq verifies TLS")
        sys.exit(1)
    print(f"Driver: {description}")
