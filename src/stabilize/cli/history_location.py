"""Where stabilize's migretti history lives: in the target schema, or in a history schema of its own."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from stabilize.cli.legacy_history import PLACEHOLDER_SQL, MigrettiState, TakeoverRefusedError
from stabilize.cli.migrations import migretti_checksum

DEFAULT_HISTORY_SCHEMA = "stabilize_history"


@dataclass(frozen=True)
class Layout:
    target: str
    history: str | None = None

    @property
    def tracking_schema(self) -> str:
        return self.history or self.target

    def stage(self, text: str) -> str:
        """The migration file migretti runs: unchanged, or redirected to the target schema."""
        if self.history is None:
            return text
        lines = text.split("\n")
        ups = [i for i, line in enumerate(lines) if line.strip().startswith("-- migrate: up")]
        downs = [i for i, line in enumerate(lines) if line.strip().startswith("-- migrate: down")]
        if any(line.strip().startswith("-- migrate: no-transaction") for line in lines):
            raise TakeoverRefusedError("a no-transaction migration cannot run with a separate history schema")
        if len(ups) != 1 or len(downs) > 1 or (downs and downs[0] < ups[0]):
            raise TakeoverRefusedError("a migration file without exactly one up section cannot be redirected")
        end = downs[0] if downs else len(lines)
        lines.insert(end, f'SET LOCAL search_path TO "{self.history}";')
        lines.insert(ups[0] + 1, f'SET LOCAL search_path TO "{self.target}";')
        return "\n".join(lines)

    def placeholder(self, statements: list[str] | None = None) -> str:
        body = "".join(f"{statement};\n" for statement in statements) if statements else "SELECT 1;\n"
        return self.stage(f"-- migrate: up\n{body}-- migrate: down\nSELECT 1;\n")

    def placeholder_checksums(self, rerun: list[list[str]]) -> frozenset[str]:
        sums = {migretti_checksum(self.placeholder()), migretti_checksum(self.stage(PLACEHOLDER_SQL))}
        sums.update(migretti_checksum(self.placeholder(statements)) for statements in rerun if statements)
        return frozenset(sums)


def current_schema(conn: Any) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT current_schema()")
        row = cur.fetchone()
    if not row or not row[0]:
        raise TakeoverRefusedError("the connection has no current schema; pass ?schema= in the database URL")
    return str(row[0])


def history_candidates(conn: Any, target: str) -> list[str]:
    """Schemas other than *target* that hold a migretti tracking table."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relname = '_migrations' AND c.relkind IN ('r', 'p') AND n.nspname <> %s ORDER BY 1",
            (target,),
        )
        return [str(row[0]) for row in cur.fetchall()]


@dataclass(frozen=True)
class Elsewhere:
    """migretti histories outside the target schema, as seen from one target."""

    owners: tuple[str, ...] = ()
    empty: tuple[str, ...] = ()
    taken: Mapping[str, str] = field(default_factory=dict)


def survey(
    target: str,
    states: Mapping[str, MigrettiState],
    texts: Iterable[str],
    rerun: list[list[str]],
    shipped_ids: set[str],
) -> Elsewhere:
    """Sort other schemas' histories into this target's, empty ones, and ones it cannot use."""
    texts = list(texts)
    owners: list[str] = []
    empty: list[str] = []
    taken: dict[str, str] = {}
    for schema, state in sorted(states.items()):
        recorded = set(state.applied) | set(state.failed)
        if recorded - shipped_ids:
            taken[schema] = "another project's migretti history"
            continue
        if not state.applied:
            empty.append(schema)
            continue
        layout = Layout(target, schema)
        sums = {migretti_checksum(layout.stage(text)) for text in texts} | layout.placeholder_checksums(rerun)
        if set(state.applied.values()) & sums:
            owners.append(schema)
        else:
            taken[schema] = "the migretti history of stabilize's tables in another schema"
    return Elsewhere(owners=tuple(owners), empty=tuple(empty), taken=taken)


def resolve(
    target: str,
    explicit_history: str | None,
    target_state: MigrettiState,
    shipped_ids: set[str],
    legacy_present: bool,
    elsewhere: Elsewhere,
) -> Layout:
    """Decide where stabilize's history lives for this run (#99 R1-R6, R11-R13)."""
    recorded = set(target_state.applied) | set(target_state.failed)
    ours, foreign = recorded & shipped_ids, recorded - shipped_ids
    if explicit_history:
        if explicit_history == target:
            raise TakeoverRefusedError(f"the history schema must differ from the target schema ({target})")
        if ours:
            raise TakeoverRefusedError(
                f"stabilize's migretti history is already in {target}._migrations, so it cannot also be kept in "
                f"{explicit_history}; run without a history schema"
            )
        other = [schema for schema in elsewhere.owners if schema != explicit_history]
        if other:
            raise TakeoverRefusedError(
                f"stabilize's migretti history for {target} is in {other[0]}; run with --history-schema {other[0]}"
            )
        if explicit_history in elsewhere.taken:
            raise TakeoverRefusedError(
                f"{explicit_history} already holds {elsewhere.taken[explicit_history]}; choose another history schema"
            )
        return Layout(target, explicit_history)
    if len(elsewhere.owners) > 1:
        raise TakeoverRefusedError(
            f"stabilize's migretti history for {target} is in several schemas ({', '.join(elsewhere.owners)}); "
            "nothing was changed"
        )
    if elsewhere.owners:
        if ours:
            raise TakeoverRefusedError(
                f"stabilize's migretti history is in both {target}._migrations and {elsewhere.owners[0]}; "
                "nothing was changed"
            )
        return Layout(target, elsewhere.owners[0])
    if ours:
        return Layout(target)
    default_free = DEFAULT_HISTORY_SCHEMA not in elsewhere.taken and target != DEFAULT_HISTORY_SCHEMA
    if DEFAULT_HISTORY_SCHEMA in elsewhere.empty and default_free:
        return Layout(target, DEFAULT_HISTORY_SCHEMA)
    if foreign and legacy_present and default_free:
        return Layout(target, DEFAULT_HISTORY_SCHEMA)
    if foreign:
        remedy = DEFAULT_HISTORY_SCHEMA if default_free else "SCHEMA"
        raise TakeoverRefusedError(
            f"migretti's history in {target} holds migrations this stabilize does not ship: "
            + ", ".join(sorted(foreign))
            + f". Keep stabilize's history apart with --history-schema {remedy} (its tables stay in "
            f"{target}), or give stabilize a schema of its own with ?schema="
        )
    return Layout(target)
