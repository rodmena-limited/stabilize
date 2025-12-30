"""Stabilize CLI for database migrations."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from importlib.resources import files
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
                return config.get("database", {})
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


def mg_up(db_url: str | None = None) -> None:
    """Apply pending migrations to PostgreSQL database."""
    try:
        import psycopg
    except ImportError:
        print("Error: psycopg not installed")
        print("Install with: pip install stabilize[postgres]")
        sys.exit(1)

    # Load config
    if db_url:
        config = parse_db_url(db_url)
    else:
        config = load_config()

    # Connect to database
    conninfo = f"host={config['host']} port={config.get('port', 5432)} user={config.get('user', 'postgres')} password={config.get('password', '')} dbname={config['dbname']}"

    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                # Ensure migration tracking table exists
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MIGRATION_TABLE} (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL UNIQUE,
                        checksum VARCHAR(32) NOT NULL,
                        applied_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                conn.commit()

                # Get applied migrations
                cur.execute(f"SELECT name, checksum FROM {MIGRATION_TABLE}")
                applied = {row[0]: row[1] for row in cur.fetchall()}

                # Get available migrations
                migrations = get_migrations()

                if not migrations:
                    print("No migrations found in package")
                    return

                # Apply pending migrations
                pending = 0
                for name, content in migrations:
                    if name in applied:
                        # Verify checksum
                        expected = compute_checksum(content)
                        if applied[name] != expected:
                            print(f"Warning: Checksum mismatch for {name}")
                        continue

                    pending += 1
                    print(f"Applying: {name}")

                    up_sql = extract_up_migration(content)
                    cur.execute(up_sql)

                    checksum = compute_checksum(content)
                    cur.execute(
                        f"INSERT INTO {MIGRATION_TABLE} (name, checksum) VALUES (%s, %s)",
                        (name, checksum),
                    )
                    conn.commit()

                if pending == 0:
                    print("All migrations already applied")
                else:
                    print(f"Applied {pending} migration(s)")

    except psycopg.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)


def mg_status(db_url: str | None = None) -> None:
    """Show migration status."""
    try:
        import psycopg
    except ImportError:
        print("Error: psycopg not installed")
        print("Install with: pip install stabilize[postgres]")
        sys.exit(1)

    # Load config
    if db_url:
        config = parse_db_url(db_url)
    else:
        config = load_config()

    conninfo = f"host={config['host']} port={config.get('port', 5432)} user={config.get('user', 'postgres')} password={config.get('password', '')} dbname={config['dbname']}"

    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                # Check if tracking table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = %s
                    )
                """, (MIGRATION_TABLE,))
                table_exists = cur.fetchone()[0]

                applied = {}
                if table_exists:
                    cur.execute(f"SELECT name, checksum, applied_at FROM {MIGRATION_TABLE} ORDER BY applied_at")
                    applied = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

                migrations = get_migrations()

                print(f"{'Status':<10} {'Migration':<50} {'Applied At'}")
                print("-" * 80)

                for name, content in migrations:
                    if name in applied:
                        checksum, applied_at = applied[name]
                        expected = compute_checksum(content)
                        status = "applied" if checksum == expected else "MISMATCH"
                        print(f"{status:<10} {name:<50} {applied_at}")
                    else:
                        print(f"{'pending':<10} {name:<50} -")

    except psycopg.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="stabilize",
        description="Stabilize - Workflow Engine CLI",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # mg-up command
    up_parser = subparsers.add_parser("mg-up", help="Apply pending PostgreSQL migrations")
    up_parser.add_argument(
        "--db-url",
        help="Database URL (postgres://user:pass@host:port/dbname)",
    )

    # mg-status command
    status_parser = subparsers.add_parser("mg-status", help="Show migration status")
    status_parser.add_argument(
        "--db-url",
        help="Database URL (postgres://user:pass@host:port/dbname)",
    )

    args = parser.parse_args()

    if args.command == "mg-up":
        mg_up(args.db_url)
    elif args.command == "mg-status":
        mg_status(args.db_url)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
