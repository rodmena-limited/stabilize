"""Tests for SQLite configuration optimization."""

from __future__ import annotations

from pathlib import Path

import pytest

from stabilize.persistence.sqlite_config import (
    SqliteConfig,
    SqliteOptimizationTier,
    get_sqlite_config,
    reset_sqlite_config,
)


class TestSqliteOptimizationTier:
    """Tests for SqliteOptimizationTier enum."""

    def test_tier_values(self) -> None:
        """All tiers should have expected values."""
        assert SqliteOptimizationTier.MINIMAL.value == "minimal"
        assert SqliteOptimizationTier.SAFE.value == "safe"
        assert SqliteOptimizationTier.AGGRESSIVE.value == "aggressive"


class TestSqliteConfig:
    """Tests for SqliteConfig dataclass."""

    def test_default_tier_is_safe(self) -> None:
        """Default tier should be SAFE for production."""
        config = SqliteConfig()
        assert config.tier == SqliteOptimizationTier.SAFE

    def test_default_busy_timeout(self) -> None:
        """Default busy timeout should be 30000ms."""
        config = SqliteConfig()
        assert config.busy_timeout_ms == 30000

    def test_safe_tier_defaults(self) -> None:
        """Safe tier should use production-safe values."""
        config = SqliteConfig(tier=SqliteOptimizationTier.SAFE)
        assert config.get_effective_value("synchronous") == "NORMAL"
        assert config.get_effective_value("cache_size_kb") == 32000
        assert config.get_effective_value("mmap_size_mb") == 256
        assert config.get_effective_value("temp_store") == "memory"
        assert config.get_effective_value("wal_autocheckpoint") == 2000

    def test_minimal_tier_defaults(self) -> None:
        """Minimal tier should use conservative values."""
        config = SqliteConfig(tier=SqliteOptimizationTier.MINIMAL)
        assert config.get_effective_value("synchronous") == "FULL"
        assert config.get_effective_value("cache_size_kb") == 2000
        assert config.get_effective_value("mmap_size_mb") == 0
        assert config.get_effective_value("temp_store") == "file"
        assert config.get_effective_value("wal_autocheckpoint") == 1000

    def test_aggressive_tier_defaults(self) -> None:
        """Aggressive tier should use maximum performance values."""
        config = SqliteConfig(tier=SqliteOptimizationTier.AGGRESSIVE)
        assert config.get_effective_value("synchronous") == "OFF"
        assert config.get_effective_value("cache_size_kb") == 128000
        assert config.get_effective_value("mmap_size_mb") == 1024
        assert config.get_effective_value("temp_store") == "memory"
        assert config.get_effective_value("wal_autocheckpoint") == 10000

    def test_override_takes_precedence(self) -> None:
        """Individual overrides should take precedence over tier defaults."""
        config = SqliteConfig(
            tier=SqliteOptimizationTier.SAFE,
            synchronous="FULL",
            cache_size_kb=64000,
        )
        # Overridden values
        assert config.get_effective_value("synchronous") == "FULL"
        assert config.get_effective_value("cache_size_kb") == 64000
        # Non-overridden still use tier default
        assert config.get_effective_value("mmap_size_mb") == 256


class TestSqliteConfigPragmaStatements:
    """Tests for PRAGMA statement generation."""

    def test_pragma_statements_include_essentials(self) -> None:
        """Generated pragmas should include essential settings."""
        config = SqliteConfig(tier=SqliteOptimizationTier.SAFE)
        statements = config.get_pragma_statements()

        assert "PRAGMA foreign_keys = ON" in statements
        assert "PRAGMA journal_mode = WAL" in statements
        assert "PRAGMA busy_timeout = 30000" in statements

    def test_pragma_statements_include_optimizations(self) -> None:
        """Generated pragmas should include optimization settings."""
        config = SqliteConfig(tier=SqliteOptimizationTier.SAFE)
        statements = config.get_pragma_statements()

        assert "PRAGMA synchronous = NORMAL" in statements
        assert "PRAGMA cache_size = -32000" in statements
        assert "PRAGMA mmap_size = 268435456" in statements  # 256MB
        assert "PRAGMA temp_store = 2" in statements  # MEMORY
        assert "PRAGMA wal_autocheckpoint = 2000" in statements

    def test_pragma_mmap_zero_not_included(self) -> None:
        """mmap_size should not be included when set to 0."""
        config = SqliteConfig(tier=SqliteOptimizationTier.MINIMAL)
        statements = config.get_pragma_statements()

        mmap_statements = [s for s in statements if "mmap_size" in s]
        assert len(mmap_statements) == 0

    def test_custom_busy_timeout(self) -> None:
        """Custom busy timeout should be reflected in pragmas."""
        config = SqliteConfig(busy_timeout_ms=60000)
        statements = config.get_pragma_statements()

        assert "PRAGMA busy_timeout = 60000" in statements


class TestSqliteConfigFromEnv:
    """Tests for environment variable loading."""

    def setup_method(self) -> None:
        """Reset config before each test."""
        reset_sqlite_config()

    def teardown_method(self) -> None:
        """Clean up after each test."""
        reset_sqlite_config()

    def test_default_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default should be SAFE when no env vars set."""
        # Clear any existing env vars
        for var in [
            "STABILIZE_SQLITE_TIER",
            "STABILIZE_SQLITE_SYNCHRONOUS",
            "STABILIZE_SQLITE_CACHE_SIZE_KB",
        ]:
            monkeypatch.delenv(var, raising=False)

        config = SqliteConfig.from_env()
        assert config.tier == SqliteOptimizationTier.SAFE

    def test_tier_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Tier should be loaded from environment variable."""
        monkeypatch.setenv("STABILIZE_SQLITE_TIER", "aggressive")
        config = SqliteConfig.from_env()
        assert config.tier == SqliteOptimizationTier.AGGRESSIVE

    def test_invalid_tier_falls_back_to_safe(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Invalid tier should fall back to SAFE."""
        monkeypatch.setenv("STABILIZE_SQLITE_TIER", "invalid_tier")
        config = SqliteConfig.from_env()
        assert config.tier == SqliteOptimizationTier.SAFE

    def test_override_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Individual overrides should be loaded from environment."""
        monkeypatch.setenv("STABILIZE_SQLITE_TIER", "safe")
        monkeypatch.setenv("STABILIZE_SQLITE_SYNCHRONOUS", "FULL")
        monkeypatch.setenv("STABILIZE_SQLITE_CACHE_SIZE_KB", "64000")

        config = SqliteConfig.from_env()
        assert config.tier == SqliteOptimizationTier.SAFE
        assert config.synchronous == "FULL"
        assert config.cache_size_kb == 64000
        # Effective value uses override
        assert config.get_effective_value("synchronous") == "FULL"
        assert config.get_effective_value("cache_size_kb") == 64000

    def test_busy_timeout_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Busy timeout should be loaded from environment."""
        monkeypatch.setenv("STABILIZE_SQLITE_BUSY_TIMEOUT_MS", "60000")
        config = SqliteConfig.from_env()
        assert config.busy_timeout_ms == 60000


class TestSqliteConfigSingleton:
    """Tests for singleton behavior."""

    def setup_method(self) -> None:
        """Reset config before each test."""
        reset_sqlite_config()

    def teardown_method(self) -> None:
        """Clean up after each test."""
        reset_sqlite_config()

    def test_singleton_returns_same_instance(self) -> None:
        """get_sqlite_config should return same instance."""
        config1 = get_sqlite_config()
        config2 = get_sqlite_config()
        assert config1 is config2

    def test_reset_clears_singleton(self) -> None:
        """reset_sqlite_config should clear the singleton."""
        config1 = get_sqlite_config()
        reset_sqlite_config()
        config2 = get_sqlite_config()
        # New instance after reset
        assert config1 is not config2


class TestSqliteConfigIntegration:
    """Integration tests for SQLite configuration."""

    def test_pragmas_applied_to_connection(self, tmp_path: Path) -> None:
        """Verify PRAGMA settings are actually applied to connections."""
        from stabilize.persistence.connection import (
            ConnectionManager,
            SingletonMeta,
        )

        # Reset singletons
        SingletonMeta.reset(ConnectionManager)
        reset_sqlite_config()

        db_path = tmp_path / "test.db"
        conn_str = f"sqlite:///{db_path}"

        manager = ConnectionManager()
        conn = manager.get_sqlite_connection(conn_str)

        # Verify essential pragmas
        result = conn.execute("PRAGMA journal_mode").fetchone()
        assert result[0].upper() == "WAL"

        result = conn.execute("PRAGMA foreign_keys").fetchone()
        assert result[0] == 1  # ON

        # Verify optimization pragmas (SAFE tier defaults)
        result = conn.execute("PRAGMA synchronous").fetchone()
        assert result[0] == 1  # NORMAL

        result = conn.execute("PRAGMA cache_size").fetchone()
        assert result[0] == -32000  # 32MB

        result = conn.execute("PRAGMA temp_store").fetchone()
        assert result[0] == 2  # MEMORY

        manager.close_all()
        SingletonMeta.reset(ConnectionManager)

    def test_memory_db_only_foreign_keys(self) -> None:
        """In-memory databases should only have foreign_keys pragma."""
        from stabilize.persistence.connection import (
            ConnectionManager,
            SingletonMeta,
        )

        # Reset singletons
        SingletonMeta.reset(ConnectionManager)
        reset_sqlite_config()

        manager = ConnectionManager()
        conn = manager.get_sqlite_connection("sqlite:///:memory:")

        # Verify foreign keys enabled
        result = conn.execute("PRAGMA foreign_keys").fetchone()
        assert result[0] == 1  # ON

        # Memory DBs don't use WAL
        result = conn.execute("PRAGMA journal_mode").fetchone()
        assert result[0].upper() == "MEMORY"

        manager.close_all()
        SingletonMeta.reset(ConnectionManager)

    def test_custom_tier_from_env(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify custom tier is applied from environment."""
        from stabilize.persistence.connection import (
            ConnectionManager,
            SingletonMeta,
        )

        # Reset singletons
        SingletonMeta.reset(ConnectionManager)
        reset_sqlite_config()

        # Set aggressive tier
        monkeypatch.setenv("STABILIZE_SQLITE_TIER", "aggressive")

        db_path = tmp_path / "test_aggressive.db"
        conn_str = f"sqlite:///{db_path}"

        manager = ConnectionManager()
        conn = manager.get_sqlite_connection(conn_str)

        # Verify aggressive tier settings
        result = conn.execute("PRAGMA synchronous").fetchone()
        assert result[0] == 0  # OFF

        result = conn.execute("PRAGMA cache_size").fetchone()
        assert result[0] == -128000  # 128MB

        manager.close_all()
        SingletonMeta.reset(ConnectionManager)
        reset_sqlite_config()
