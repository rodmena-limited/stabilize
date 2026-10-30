"""Find which shipped migrations a schema without migration history already reflects, from its catalog."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from stabilize.cli.schema_model import MigrationModel, model_migration

ENGINE_MARKER_TABLE = "pipeline_executions"


class AdoptionRefusedError(RuntimeError):
    """The catalog does not match exactly one level of the shipped migrations."""


@dataclass
class Catalog:
    tables: dict[str, dict[str, str]] = field(default_factory=dict)
    indexes: set[str] = field(default_factory=set)
    sequence_types: dict[str, str] = field(default_factory=dict)
    server_version: int = 0


@dataclass
class _State:
    tables: dict[str, dict[str, str]] = field(default_factory=dict)
    indexes: set[str] = field(default_factory=set)
    sequence_types: dict[str, str] = field(default_factory=dict)


def read_catalog(conn: Any, schema: str) -> Catalog:
    catalog = Catalog()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped "
            "WHERE n.nspname = %s AND c.relkind IN ('r', 'p')",
            (schema,),
        )
        for table, column, type_name in cur.fetchall():
            catalog.tables.setdefault(str(table), {})[str(column)] = str(type_name)
        cur.execute(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relkind IN ('i', 'I')",
            (schema,),
        )
        catalog.indexes = {str(row[0]) for row in cur.fetchall()}
        cur.execute(
            "SELECT c.relname, format_type(s.seqtypid, NULL) FROM pg_sequence s "
            "JOIN pg_class c ON c.oid = s.seqrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s",
            (schema,),
        )
        catalog.sequence_types = {str(name): str(type_name) for name, type_name in cur.fetchall()}
        cur.execute("SHOW server_version_num")
        row = cur.fetchone()
        catalog.server_version = int(row[0]) if row else 0
    return catalog


def models_for(shipped: dict[str, str]) -> list[MigrationModel]:
    return [model_migration(name, text) for name, text in sorted(shipped.items())]


def _expected(models: list[MigrationModel], level: int, server: int) -> _State:
    state = _State()
    for model in models[:level]:
        for e in model.expectations:
            if e.min_server > server:
                continue
            if e.kind == "table":
                state.tables.setdefault(e.relation, {})
            elif e.kind in ("column", "column_type"):
                state.tables.setdefault(e.relation, {})[e.name] = e.type
            elif e.kind == "index":
                state.indexes.add(e.name)
            elif e.kind == "sequence_type":
                state.sequence_types[e.relation] = e.type
    return state


def problems_at(models: list[MigrationModel], level: int, catalog: Catalog) -> list[str]:
    expected = _expected(models, level, catalog.server_version)
    problems: list[str] = []
    for table, columns in sorted(expected.tables.items()):
        actual = catalog.tables.get(table)
        if actual is None:
            problems.append(f"table {table} is missing")
            continue
        for column, type_name in sorted(columns.items()):
            found = actual.get(column)
            if found is None:
                problems.append(f"column {table}.{column} is missing")
            elif found != type_name:
                problems.append(f"column {table}.{column} is {found}, expected {type_name}")
    problems.extend(f"index {name} is missing" for name in sorted(expected.indexes - catalog.indexes))
    for sequence, type_name in sorted(expected.sequence_types.items()):
        if catalog.sequence_types.get(sequence) != type_name:
            problems.append(f"sequence {sequence} is {catalog.sequence_types.get(sequence)}, expected {type_name}")
    for model in models[level:]:
        for e in model.expectations:
            if e.min_server > catalog.server_version or e.may_preexist:
                continue
            present = catalog.tables.get(e.relation, {})
            if e.kind == "table" and e.relation in catalog.tables and e.relation not in expected.tables:
                problems.append(f"table {e.relation} exists although {model.filename} is not applied")
            elif e.kind == "column" and e.name in present and e.name not in expected.tables.get(e.relation, {}):
                problems.append(f"column {e.relation}.{e.name} exists although {model.filename} is not applied")
            elif e.kind == "index" and e.name in catalog.indexes and e.name not in expected.indexes:
                problems.append(f"index {e.name} exists although {model.filename} is not applied")
            elif e.kind == "column_type":
                was = expected.tables.get(e.relation, {}).get(e.name)
                if was is not None and was != e.type and present.get(e.name) == e.type:
                    problems.append(f"column {e.relation}.{e.name} is already {e.type} ({model.filename})")
            elif e.kind == "sequence_type":
                was_seq = expected.sequence_types.get(e.relation)
                if catalog.sequence_types.get(e.relation) == e.type and was_seq != e.type:
                    problems.append(f"sequence {e.relation} is already {e.type} ({model.filename})")
    return problems


def adoption_level(models: list[MigrationModel], catalog: Catalog) -> int:
    """The lowest level (count of shipped migrations, in order) the catalog matches.

    Objects of later migrations may exist only where those migrations create
    them with IF NOT EXISTS, so applying the remaining migrations afterwards
    leaves them as they are.
    """
    results = {level: problems_at(models, level, catalog) for level in range(1, len(models) + 1)}
    matches = [level for level, problems in results.items() if not problems]
    if not matches:
        best = min(results, key=lambda level: (len(results[level]), -level))
        detail = "; ".join(results[best][:12])
        raise AdoptionRefusedError(
            "the schema has stabilize's tables but no migration history, and no level of the shipped "
            f"migrations matches it. Closest is level {best} ({models[best - 1].filename}): {detail}"
        )
    level = min(matches)
    for model in models[:level]:
        if model.unmodelled:
            raise AdoptionRefusedError(
                f"{model.filename} contains statements whose effect the catalog cannot show: "
                + "; ".join(statement[:80] for statement in model.unmodelled)
            )
    return level


def legacy_rows_for(shipped_plain: dict[str, str], level: int) -> dict[str, str]:
    """The stabilize_migrations rows the old runner would have recorded for the first *level* migrations."""
    names = sorted(shipped_plain)[:level]
    return {name: hashlib.md5(shipped_plain[name].encode()).hexdigest() for name in names}
