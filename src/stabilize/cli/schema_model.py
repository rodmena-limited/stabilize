"""What each shipped migration leaves in the PostgreSQL catalog, derived from its own SQL."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from stabilize.cli.migrations import extract_up_migration

_TYPE_NAMES = {
    "BIGINT": "bigint",
    "BIGSERIAL": "bigint",
    "INTEGER": "integer",
    "INT": "integer",
    "SERIAL": "integer",
    "TEXT": "text",
    "TEXT[]": "text[]",
    "JSONB": "jsonb",
    "BOOLEAN": "boolean",
    "TIMESTAMP": "timestamp without time zone",
    "TIMESTAMPTZ": "timestamp with time zone",
    "TIMESTAMP WITH TIME ZONE": "timestamp with time zone",
    "XID8": "xid8",
}
_VARCHAR_RE = re.compile(r"^VARCHAR\((\d+)\)$")
_TYPE_RE = re.compile(r"^(TIMESTAMP WITH TIME ZONE|VARCHAR\(\d+\)|[A-Z0-9_]+(?:\[\])?)", re.IGNORECASE)
_CREATE_TABLE_RE = re.compile(r"^CREATE TABLE (IF NOT EXISTS )?([a-z_][a-z0-9_]*) \((.*)\)$", re.IGNORECASE | re.DOTALL)
_CREATE_INDEX_RE = re.compile(
    r"^CREATE (UNIQUE )?INDEX (IF NOT EXISTS )?([a-z_][a-z0-9_]*) ON ([a-z_][a-z0-9_]*)", re.IGNORECASE
)
_ALTER_TABLE_RE = re.compile(r"^ALTER TABLE ([a-z_][a-z0-9_]*) (.*)$", re.IGNORECASE | re.DOTALL)
_ADD_COLUMN_RE = re.compile(r"^ADD COLUMN (IF NOT EXISTS )?([a-z_][a-z0-9_]*) (.*)$", re.IGNORECASE | re.DOTALL)
_ALTER_TYPE_RE = re.compile(r"^ALTER COLUMN ([a-z_][a-z0-9_]*) TYPE (.*)$", re.IGNORECASE | re.DOTALL)
_SET_DEFAULT_RE = re.compile(r"^ALTER COLUMN ([a-z_][a-z0-9_]*) SET DEFAULT ", re.IGNORECASE)
_ALTER_SEQUENCE_RE = re.compile(r"^ALTER SEQUENCE ([a-z_][a-z0-9_]*) AS ([A-Z]+)$", re.IGNORECASE)
_VERSION_GUARD_RE = re.compile(
    r"^DO \$\$ BEGIN IF current_setting\('server_version_num'\)::int >= (\d+) THEN (.*) END IF; END \$\$$",
    re.IGNORECASE | re.DOTALL,
)
_SETVAL_RE = re.compile(r"^SELECT setval\(", re.IGNORECASE)
_CONSTRAINT_RE = re.compile(r"^(PRIMARY|UNIQUE|FOREIGN|CONSTRAINT|CHECK|EXCLUDE)\b", re.IGNORECASE)


@dataclass(frozen=True)
class Expectation:
    kind: str
    relation: str
    name: str = ""
    type: str = ""
    may_preexist: bool = False
    min_server: int = 0


@dataclass
class MigrationModel:
    filename: str
    expectations: list[Expectation] = field(default_factory=list)
    rerun_on_adoption: list[str] = field(default_factory=list)
    unmodelled: list[str] = field(default_factory=list)


def normalize_type(raw: str) -> str:
    text = " ".join(raw.split()).upper()
    match = _VARCHAR_RE.match(text)
    if match:
        return f"character varying({match.group(1)})"
    if text not in _TYPE_NAMES:
        raise ValueError(f"unknown column type {raw!r}")
    return _TYPE_NAMES[text]


def split_statements(sql: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_quote = in_dollar = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if not in_quote and not in_dollar and sql.startswith("--", i):
            end = sql.find("\n", i)
            i = len(sql) if end == -1 else end
            continue
        if not in_quote and sql.startswith("$$", i):
            in_dollar = not in_dollar
            current.append("$$")
            i += 2
            continue
        if not in_dollar and ch == "'":
            in_quote = not in_quote
        if ch == ";" and not in_quote and not in_dollar:
            statements.append(" ".join("".join(current).split()))
            current = []
        else:
            current.append(ch)
        i += 1
    tail = " ".join("".join(current).split())
    if tail:
        statements.append(tail)
    return [s for s in statements if s]


def _split_top_level(text: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    in_quote = False
    current: list[str] = []
    for ch in text:
        if ch == "'":
            in_quote = not in_quote
        elif not in_quote and ch == "(":
            depth += 1
        elif not in_quote and ch == ")":
            depth -= 1
        if ch == "," and depth == 0 and not in_quote:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if "".join(current).strip():
        parts.append("".join(current).strip())
    return parts


def _column_type(definition: str) -> str:
    match = _TYPE_RE.match(definition.strip())
    if not match:
        raise ValueError(f"no column type in {definition!r}")
    return normalize_type(match.group(1))


def _model_statement(statement: str, model: MigrationModel, min_server: int = 0) -> None:
    created = _CREATE_TABLE_RE.match(statement)
    if created:
        optional, table = bool(created.group(1)), created.group(2).lower()
        model.expectations.append(Expectation("table", table, may_preexist=optional, min_server=min_server))
        for item in _split_top_level(created.group(3)):
            if _CONSTRAINT_RE.match(item):
                continue
            column, _, rest = item.partition(" ")
            model.expectations.append(
                Expectation("column", table, column.lower(), _column_type(rest), optional, min_server)
            )
        return
    index = _CREATE_INDEX_RE.match(statement)
    if index:
        model.expectations.append(
            Expectation("index", index.group(4).lower(), index.group(3).lower(), "", bool(index.group(2)), min_server)
        )
        return
    altered = _ALTER_TABLE_RE.match(statement)
    if altered:
        table = altered.group(1).lower()
        for action in _split_top_level(altered.group(2)):
            add = _ADD_COLUMN_RE.match(action)
            change = _ALTER_TYPE_RE.match(action)
            if add:
                column, optional = add.group(2).lower(), bool(add.group(1))
                model.expectations.append(
                    Expectation("column", table, column, _column_type(add.group(3)), optional, min_server)
                )
            elif change:
                new_type = _column_type(change.group(2))
                model.expectations.append(Expectation("column_type", table, change.group(1).lower(), new_type))
            elif not _SET_DEFAULT_RE.match(action):
                model.unmodelled.append(statement)
        return
    sequence = _ALTER_SEQUENCE_RE.match(statement)
    if sequence:
        model.expectations.append(
            Expectation("sequence_type", sequence.group(1).lower(), "", normalize_type(sequence.group(2)))
        )
        return
    guarded = _VERSION_GUARD_RE.match(statement)
    if guarded and min_server == 0:
        for inner in split_statements(guarded.group(2)):
            _model_statement(inner, model, int(guarded.group(1)))
        return
    if _SETVAL_RE.match(statement) and min_server == 0:
        model.rerun_on_adoption.append(statement)
        return
    model.unmodelled.append(statement)


def model_migration(filename: str, content: str) -> MigrationModel:
    model = MigrationModel(filename)
    for statement in split_statements(extract_up_migration(content)):
        _model_statement(statement, model)
    return model
