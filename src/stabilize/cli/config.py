"""Configuration loading utilities for Stabilize CLI."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any

# Migration tracking table
MIGRATION_TABLE = "stabilize_migrations"


def load_config() -> dict[str, Any]:
    """Load database config from mg.yaml or environment."""
    db_url = os.environ.get("MG_DATABASE_URL")
    if db_url:
        return parse_db_url(db_url)

    # Try to load mg.yaml
    mg_yaml = Path("mg.yaml")
    if mg_yaml.exists():
        try:
            import yaml

            with open(mg_yaml) as f:
                config = yaml.safe_load(f)
                db_config: dict[str, Any] = config.get("database", {}) if config else {}
                return db_config
        except ImportError:
            print("Warning: PyYAML not installed, cannot read mg.yaml")
            print("Set MG_DATABASE_URL environment variable instead")
            sys.exit(1)

    print("Error: No database configuration found")
    print("Either create mg.yaml or set MG_DATABASE_URL environment variable")
    sys.exit(1)


def parse_db_url(url: str) -> dict[str, Any]:
    """Parse a database URL into connection parameters."""
    # postgres://user:pass@host:port/dbname
    pattern = r"postgres(?:ql)?://(?:(?P<user>[^:]+)(?::(?P<password>[^@]+))?@)?(?P<host>[^:/]+)(?::(?P<port>\d+))?/(?P<dbname>[^?]+)"
    match = re.match(pattern, url)
    if not match:
        print(f"Error: Invalid database URL: {url}")
        sys.exit(1)

    return {
        "host": match.group("host"),
        "port": int(match.group("port") or 5432),
        "user": match.group("user") or "postgres",
        "password": match.group("password") or "",
        "dbname": match.group("dbname"),
    }
