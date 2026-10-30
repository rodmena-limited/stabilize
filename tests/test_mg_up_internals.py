"""Units of the mg-up hand-off to migretti (#95): history table, takeover plan, staging, environment, TLS, redaction."""

from __future__ import annotations

import hashlib
import stat
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from stabilize.cli import migretti_runner as runner
from stabilize.cli.config import connection_params, parse_db_url
from stabilize.cli.legacy_history import (
    KNOWN_LEGACY_CHECKSUMS,
    PLACEHOLDER_CHECKSUM,
    RETIRED_MIGRATIONS,
    MigrettiState,
    TakeoverRefusedError,
    plan_takeover,
)
from stabilize.cli.migrations import get_migration_files, get_migrations, migration_id
from tests.mg_support import REPO, scratch_database

SHIPPED = dict(get_migrations())
IDS = {migration_id(name): name for name in SHIPPED}
SENTINEL = "Zq7SentinelPw9xK"


def _md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _legacy_now() -> dict[str, str]:
    return {name: _md5(text) for name, text in SHIPPED.items()}


def _state(applied: dict[str, str] | None = None, failed: tuple[str, ...] = ()) -> MigrettiState:
    return MigrettiState(tracking=bool(applied or failed), applied=applied or {}, failed=failed)


def test_every_shipped_file_is_in_the_history_table_with_its_current_checksum() -> None:
    assert len(SHIPPED) >= 14
    for name, text in SHIPPED.items():
        assert _md5(text) in KNOWN_LEGACY_CHECKSUMS.get(name, frozenset()), f"{name} current checksum not recorded"


def test_the_history_table_names_only_shipped_or_retired_files() -> None:
    assert set(KNOWN_LEGACY_CHECKSUMS) == set(SHIPPED) | RETIRED_MIGRATIONS
    assert not (RETIRED_MIGRATIONS & set(SHIPPED))


def test_the_history_table_covers_every_committed_version_of_every_migration() -> None:
    import subprocess

    def git(*args: str) -> str:
        return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, text=True, check=True).stdout

    try:
        if git("rev-parse", "--is-shallow-repository").strip() == "true":
            pytest.skip("shallow clone: migration file history is incomplete")
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("not a git checkout")
    seen = 0
    for path in sorted(set(git("log", "--all", "--format=", "--name-only", "--", "migrations/*.sql").split())):
        name = Path(path).name
        for commit in set(git("log", "--all", "--format=%H", "--", path).split()):
            show = subprocess.run(["git", "-C", str(REPO), "show", f"{commit}:{path}"], capture_output=True)
            if show.returncode != 0:
                continue
            seen += 1
            text = show.stdout.decode().replace("\r\n", "\n")
            assert _md5(text) in KNOWN_LEGACY_CHECKSUMS.get(name, frozenset()), f"{name}@{commit[:10]} not recorded"
    assert seen >= len(SHIPPED)


def test_an_untouched_history_becomes_placeholders_then_fixes() -> None:
    plan = plan_takeover(_legacy_now(), SHIPPED, _state())
    assert plan.placeholders == sorted(IDS) and plan.fixes == sorted(IDS) and plan.adopted == sorted(IDS)


def test_placeholders_already_recorded_only_need_fixes() -> None:
    applied = {mig_id: PLACEHOLDER_CHECKSUM for mig_id in IDS}
    plan = plan_takeover(_legacy_now(), SHIPPED, _state(applied))
    assert plan.placeholders == [] and plan.fixes == sorted(IDS)


def test_a_completed_takeover_needs_nothing() -> None:
    applied = {mig_id: _sha256(SHIPPED[name]) for mig_id, name in IDS.items()}
    assert not plan_takeover(_legacy_now(), SHIPPED, _state(applied)).needed


def test_a_retired_migration_is_reported_and_not_adopted() -> None:
    legacy = {**_legacy_now(), "01KDRK3TXW4R2GERC1WBCQYJGG_rag_embeddings.sql": "0c05b1306b39c39841bf4a82d229d7f5"}
    plan = plan_takeover(legacy, SHIPPED, _state())
    assert plan.retired == ["01KDRK3TXW4R2GERC1WBCQYJGG_rag_embeddings.sql"]
    assert "01KDRK3TXW4R2GERC1WBCQYJGG" not in plan.adopted


@pytest.mark.parametrize(
    ("legacy_edit", "state", "message"),
    [
        ({"01KE4YJT2W8A6NBX5R7CQMJK84_add_dlq_table.sql": "0" * 32}, None, "matches no committed version"),
        ({"01M60ZZZZZZZZZZZZZZZZZZZZZ_hand_written.sql": "f" * 32}, None, "never shipped"),
        ({}, _state(failed=("01KDQ4N9QPJ6Q4MCV3V9GHWPV4",)), "failed migrations"),
        ({}, _state({"01M64AAAAAAAAAAAAAAAAAAAAA": "x"}), "does not ship"),
        ({}, _state({"01KDQ4N9QPJ6Q4MCV3V9GHWPV4": "x" * 64}), "neither the shipped file's"),
    ],
)
def test_history_the_takeover_cannot_vouch_for_is_refused(
    legacy_edit: dict[str, str], state: MigrettiState | None, message: str
) -> None:
    with pytest.raises(TakeoverRefusedError, match=message):
        plan_takeover({**_legacy_now(), **legacy_edit}, SHIPPED, state or _state())


def test_a_placeholder_the_old_runner_never_applied_is_refused() -> None:
    legacy = _legacy_now()
    del legacy[IDS["01M4SVVY5XE3S18Y0CPQ8TATDP"]]
    with pytest.raises(TakeoverRefusedError, match="no record"):
        plan_takeover(legacy, SHIPPED, _state({"01M4SVVY5XE3S18Y0CPQ8TATDP": PLACEHOLDER_CHECKSUM}))


def test_the_staging_directory_holds_exactly_the_shipped_bytes_and_no_config() -> None:
    files = get_migration_files()
    with runner.staged(files) as root:
        assert stat.S_IMODE(root.stat().st_mode) == 0o700
        assert sorted(p.name for p in root.iterdir()) == ["migrations"]
        staged = {p.name: p.read_bytes() for p in (root / "migrations").iterdir()}
    assert staged == dict(files)
    assert not root.exists()


def test_migretti_connects_with_every_parameter_given_and_the_target_schema() -> None:
    from psycopg.conninfo import conninfo_to_dict

    query = "sslmode=verify-full&sslrootcert=/r.crt&options=-cstatement_timeout%3D5s"
    params = connection_params(parse_db_url(f"postgres://u:{SENTINEL}@h:6543/d?{query}"))
    parsed = conninfo_to_dict(runner.migretti_conninfo(params, "orch"))
    assert parsed["host"] == "h" and parsed["port"] == "6543" and parsed["dbname"] == "d" and parsed["user"] == "u"
    assert parsed["password"] == SENTINEL and parsed["sslmode"] == "verify-full" and parsed["sslrootcert"] == "/r.crt"
    assert parsed["options"] == "-cstatement_timeout=5s -c search_path=orch"


def test_mg_variables_are_removed_from_migrettis_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in runner.SCRUBBED_ENV:
        monkeypatch.setenv(name, "from-another-project")
    monkeypatch.setenv("PGPASSFILE", "/kept")
    env = runner.migretti_env("host=h dbname=d")
    assert env["MG_DATABASE_URL"] == "host=h dbname=d"
    assert all(name not in env for name in runner.SCRUBBED_ENV if name != "MG_DATABASE_URL")
    assert env["PGPASSFILE"] == "/kept"


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgunit") as url:
        yield url


def test_tls_parameters_reach_migretti_in_both_directions(db: str, capsys: pytest.CaptureFixture[str]) -> None:
    pytest.importorskip("psycopg")
    files = get_migration_files()
    required = runner.migretti_conninfo(
        connection_params(parse_db_url(f"{db}?sslmode=verify-full&sslrootcert=/nonexistent/stabilize-root.crt")), None
    )
    disabled = runner.migretti_conninfo(connection_params(parse_db_url(f"{db}?sslmode=disable")), None)
    with runner.staged(files) as root:
        with pytest.raises(runner.MigrettiFailedError):
            runner.snapshot(root, required)
        printed = capsys.readouterr().out
        assert "stabilize-root.crt" in printed or "SSL" in printed, printed
        assert runner.snapshot(root, disabled) == MigrettiState(tracking=False, applied={}, failed=())


def test_migretti_output_is_printed_without_the_password(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    script = f"print('connection failed: host=h password={SENTINEL} dbname=d'); raise SystemExit(1)"
    monkeypatch.setattr(runner, "_command", lambda args: [sys.executable, "-c", script])
    with pytest.raises(runner.MigrettiFailedError):
        runner.run(["status"], tmp_path, f"host=h password={SENTINEL} dbname=d")
    printed = capsys.readouterr().out
    assert "connection failed" in printed
    assert SENTINEL not in printed


def test_the_redaction_check_can_see_the_password(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = f"print('connection failed: host=h password={SENTINEL} dbname=d')"
    monkeypatch.setattr(runner, "_command", lambda args: [sys.executable, "-c", script])
    lines = runner.run(["status"], tmp_path, f"host=h password={SENTINEL} dbname=d", echo=False)
    assert any(SENTINEL in line for line in lines)


def test_mg_up_without_migretti_names_the_install(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("psycopg")
    from stabilize.cli import migrate

    monkeypatch.setattr(runner, "migretti_installed", lambda: False)
    with pytest.raises(SystemExit) as raised:
        migrate.mg_up("postgresql://u:p@127.0.0.1:1/db")
    assert raised.value.code == 1
    assert "pip install stabilize[postgres]" in capsys.readouterr().out
