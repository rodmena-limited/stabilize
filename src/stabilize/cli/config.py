"""Configuration loading utilities for Stabilize CLI."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import parse_qs

if TYPE_CHECKING:
    from typing import Any

# Migration tracking table
MIGRATION_TABLE = "stabilize_migrations"

# Plain PostgreSQL identifier: interpolated into DDL, so nothing else is legal.
_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_schema_name(schema: str) -> str:
    """Validate a target schema name as a plain PostgreSQL identifier."""
    if not _SCHEMA_NAME_RE.match(schema) or len(schema) > 63:
        print(f"Error: Invalid schema name: {schema!r}")
        print("Expected a plain PostgreSQL identifier (letters, digits, underscore; max 63 chars)")
        sys.exit(1)
    return schema


def load_config() -> dict[str, Any]:
    """Load database config from mg.yaml or environment."""
    config: dict[str, Any] | None = None

    db_url = os.environ.get("MG_DATABASE_URL")
    if db_url:
        config = parse_db_url(db_url)
    else:
        # Try to load mg.yaml
        mg_yaml = Path("mg.yaml")
        if mg_yaml.exists():
            try:
                import yaml

                with open(mg_yaml) as f:
                    raw = yaml.safe_load(f)
                    config = raw.get("database", {}) if raw else {}
            except ImportError:
                print("Warning: PyYAML not installed, cannot read mg.yaml")
                print("Set MG_DATABASE_URL environment variable instead")
                sys.exit(1)

    if config is None:
        print("Error: No database configuration found")
        print("Either create mg.yaml or set MG_DATABASE_URL environment variable")
        sys.exit(1)

    schema = os.environ.get("MG_SCHEMA")
    if schema:
        config["schema"] = schema
    return config


def parse_db_url(url: str) -> dict[str, Any]:
    """Parse a database URL into connection parameters."""
    # postgres://user:pass@host:port/dbname?schema=name
    pattern = (
        r"postgres(?:ql)?://(?:(?P<user>[^:]+)(?::(?P<password>[^@]+))?@)?"
        r"(?P<host>[^:/]+)(?::(?P<port>\d+))?/(?P<dbname>[^?]+)(?:\?(?P<query>.*))?$"
    )
    match = re.match(pattern, url)
    if not match:
        print(f"Error: Invalid database URL: {url}")
        sys.exit(1)

    config: dict[str, Any] = {
        "host": match.group("host"),
        "port": int(match.group("port") or 5432),
        "user": match.group("user") or "postgres",
        "password": match.group("password") or "",
        "dbname": match.group("dbname"),
    }

    query = match.group("query")
    if query:
        schema_values = parse_qs(query).get("schema")
        if schema_values:
            config["schema"] = schema_values[-1]
    return config
