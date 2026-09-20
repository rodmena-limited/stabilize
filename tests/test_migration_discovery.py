"""get_migrations must find the .sql files from a source checkout, not only a wheel."""

from __future__ import annotations

from pathlib import Path

import pytest

from stabilize.cli.migrations import MigrationsNotFoundError, get_migrations

REPO_ROOT = Path(__file__).resolve().parent.parent
ROOT_MIGRATIONS = REPO_ROOT / "migrations"


def _root_sql_names() -> set[str]:
    return {p.name for p in ROOT_MIGRATIONS.glob("*.sql")}


def test_the_repository_actually_carries_migrations() -> None:
    names = _root_sql_names()
    assert names, (
        f"no .sql under {ROOT_MIGRATIONS}; every other assertion here would pass "
        "vacuously against an empty directory"
    )


def test_get_migrations_finds_them_from_a_source_checkout() -> None:
    found = {name for name, _ in get_migrations()}
    assert found == _root_sql_names()


def test_every_migration_has_content() -> None:
    for name, content in get_migrations():
        assert content.strip(), f"{name} resolved to empty content"


def test_migrations_are_returned_in_ulid_order() -> None:
    names = [name for name, _ in get_migrations()]
    assert names == sorted(names)


def test_absence_names_both_locations_searched(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from stabilize.cli import migrations as mod

    monkeypatch.setattr(mod, "_package_migrations", lambda: [])
    monkeypatch.setattr(mod, "_checkout_migrations", lambda: [])

    with pytest.raises(MigrationsNotFoundError) as exc:
        get_migrations()

    message = str(exc.value)
    assert "stabilize.migrations" in message
    assert "migrations" in message
