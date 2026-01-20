"""SQLite configuration for performance optimization.

This module provides configurable PRAGMA settings for SQLite connections
to optimize performance for workflows with massive parallelism while
maintaining correctness and atomicity guarantees.

Three optimization tiers are available:
- MINIMAL: Only essential pragmas, backward compatible
- SAFE: Production-safe optimizations (default)
- AGGRESSIVE: Maximum performance, for ephemeral/test data only

Environment Variables:
    STABILIZE_SQLITE_TIER: Optimization tier (minimal/safe/aggressive)
    STABILIZE_SQLITE_SYNCHRONOUS: Override synchronous setting (OFF/NORMAL/FULL)
    STABILIZE_SQLITE_CACHE_SIZE_KB: Cache size in KB
    STABILIZE_SQLITE_MMAP_SIZE_MB: Memory-mapped I/O size in MB
    STABILIZE_SQLITE_TEMP_STORE: Temp storage (file/memory)
    STABILIZE_SQLITE_WAL_AUTOCHECKPOINT: Pages between checkpoints
    STABILIZE_SQLITE_BUSY_TIMEOUT_MS: Busy timeout in milliseconds
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Any


class SqliteOptimizationTier(Enum):
    """Optimization tier for SQLite pragmas."""

    MINIMAL = "minimal"  # Only essential pragmas (backward compatible)
    SAFE = "safe"  # Production-safe optimizations (default)
    AGGRESSIVE = "aggressive"  # Maximum performance, crash risk


@dataclass
class SqliteConfig:
    """Configuration for SQLite connection optimization.

    Controls PRAGMA settings for performance tuning while maintaining
    correctness guarantees. All settings have sensible defaults that
    prioritize safety.

    Attributes:
        tier: Optimization tier (determines defaults if individual settings not specified)
        synchronous: Sync mode (OFF, NORMAL, or FULL)
        cache_size_kb: Page cache size in KB
        mmap_size_mb: Memory-mapped I/O size in MB (0 to disable)
        temp_store: Where to store temp tables ("file" or "memory")
        wal_autocheckpoint: Pages between automatic WAL checkpoints
        busy_timeout_ms: How long to wait on locked database
    """

    tier: SqliteOptimizationTier = SqliteOptimizationTier.SAFE

    # Individual PRAGMA overrides (None = use tier default)
    synchronous: str | None = None  # "OFF", "NORMAL", or "FULL"
    cache_size_kb: int | None = None  # e.g., 32000 for 32MB
    mmap_size_mb: int | None = None  # e.g., 256 for 256MB
    temp_store: str | None = None  # "file" or "memory"
    wal_autocheckpoint: int | None = None
    busy_timeout_ms: int = 30000  # Keep existing default

    @staticmethod
    def _get_tier_defaults() -> dict[SqliteOptimizationTier, dict[str, Any]]:
        """Get tier default values."""
        return {
            SqliteOptimizationTier.MINIMAL: {
                "synchronous": "FULL",
                "cache_size_kb": 2000,  # ~2MB
                "mmap_size_mb": 0,  # Disabled
                "temp_store": "file",
                "wal_autocheckpoint": 1000,
            },
            SqliteOptimizationTier.SAFE: {
                "synchronous": "NORMAL",
                "cache_size_kb": 32000,  # 32MB
                "mmap_size_mb": 256,  # 256MB
                "temp_store": "memory",
                "wal_autocheckpoint": 2000,
            },
            SqliteOptimizationTier.AGGRESSIVE: {
                "synchronous": "OFF",
                "cache_size_kb": 128000,  # 128MB
                "mmap_size_mb": 1024,  # 1GB
                "temp_store": "memory",
                "wal_autocheckpoint": 10000,
            },
        }

    @classmethod
    def from_env(cls) -> SqliteConfig:
        """Load configuration from environment variables.

        Returns:
            SqliteConfig with values from environment or defaults
        """
        tier_str = os.getenv("STABILIZE_SQLITE_TIER", "safe").lower()
        try:
            tier = SqliteOptimizationTier(tier_str)
        except ValueError:
            tier = SqliteOptimizationTier.SAFE

        return cls(
            tier=tier,
            synchronous=os.getenv("STABILIZE_SQLITE_SYNCHRONOUS"),
            cache_size_kb=_parse_int_env("STABILIZE_SQLITE_CACHE_SIZE_KB"),
            mmap_size_mb=_parse_int_env("STABILIZE_SQLITE_MMAP_SIZE_MB"),
            temp_store=os.getenv("STABILIZE_SQLITE_TEMP_STORE"),
            wal_autocheckpoint=_parse_int_env("STABILIZE_SQLITE_WAL_AUTOCHECKPOINT"),
            busy_timeout_ms=int(os.getenv("STABILIZE_SQLITE_BUSY_TIMEOUT_MS", "30000")),
        )

    def get_effective_value(self, setting: str) -> Any:
        """Get effective value for a setting (override or tier default).

        Args:
            setting: The setting name (e.g., "synchronous", "cache_size_kb")

        Returns:
            The effective value for the setting
        """
        override = getattr(self, setting, None)
        if override is not None:
            return override
        tier_defaults = self._get_tier_defaults()
        return tier_defaults[self.tier].get(setting)

    def get_pragma_statements(self) -> list[str]:
        """Generate PRAGMA statements for this configuration.

        Returns:
            List of PRAGMA SQL statements to execute
        """
        statements = []

        # Always set these (existing behavior)
        statements.append("PRAGMA foreign_keys = ON")
        statements.append("PRAGMA journal_mode = WAL")
        statements.append(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")

        # Optimization pragmas
        sync = self.get_effective_value("synchronous")
        statements.append(f"PRAGMA synchronous = {sync}")

        cache_kb = self.get_effective_value("cache_size_kb")
        # Negative value means KB units (more portable than page counts)
        statements.append(f"PRAGMA cache_size = -{cache_kb}")

        mmap_mb = self.get_effective_value("mmap_size_mb")
        if mmap_mb and mmap_mb > 0:
            mmap_bytes = mmap_mb * 1024 * 1024
            statements.append(f"PRAGMA mmap_size = {mmap_bytes}")

        temp = self.get_effective_value("temp_store")
        temp_value = 2 if temp == "memory" else 0
        statements.append(f"PRAGMA temp_store = {temp_value}")

        wal_cp = self.get_effective_value("wal_autocheckpoint")
        statements.append(f"PRAGMA wal_autocheckpoint = {wal_cp}")

        return statements


def _parse_int_env(name: str) -> int | None:
    """Parse integer from environment variable.

    Args:
        name: Environment variable name

    Returns:
        Parsed integer or None if not set or invalid
    """
    value = os.getenv(name)
    if value is not None:
        try:
            return int(value)
        except ValueError:
            pass
    return None


# Singleton pattern (matching resilience/config.py)
_default_sqlite_config: SqliteConfig | None = None


def get_sqlite_config() -> SqliteConfig:
    """Get the default SqliteConfig, loading from environment on first call.

    Returns:
        The singleton SqliteConfig instance
    """
    global _default_sqlite_config
    if _default_sqlite_config is None:
        _default_sqlite_config = SqliteConfig.from_env()
    return _default_sqlite_config


def reset_sqlite_config() -> None:
    """Reset the sqlite config singleton. Useful for testing."""
    global _default_sqlite_config
    _default_sqlite_config = None
