"""The declared dependency set and the imported module set must agree.

An undeclared direct import breaks the day its transitive carrier drops it.
A declared-but-unimported runtime dependency is forced on every consumer for
nothing. Both have shipped; neither was mechanically checked.
"""

from __future__ import annotations

import ast
import sys
import tomllib
from importlib.metadata import packages_distributions
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src" / "stabilize"
PYPROJECT = REPO_ROOT / "pyproject.toml"

GUARDED_IMPORT_EXTRAS = {
    "psutil": "process",
    "opentelemetry": "observability",
    "structlog": "observability",
    "yaml": "cli",
    "psycopg": "postgres",
    "psycopg_pool": "postgres",
}

DYNAMIC_IMPORTS = {"structlog"}

DECLARED_PENDING_USE = {
    "pydantic": "issue 41 - contract validation at the deserialization boundaries",
}


def _canonical(name: str) -> str:
    return name.lower().replace("_", "-")


def _requirement_name(requirement: str) -> str:
    head = requirement.split(";", 1)[0].strip()
    for delimiter in ("==", ">=", "<=", "~=", "!=", ">", "<", "["):
        head = head.split(delimiter, 1)[0]
    return _canonical(head.strip())


def _imported_top_level_modules() -> dict[str, set[str]]:
    modules: dict[str, set[str]] = {}
    for path in sorted(SRC_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level or not node.module:
                    continue
                names = [node.module]
            else:
                continue
            for name in names:
                top = name.split(".", 1)[0]
                if top in sys.stdlib_module_names or top == "stabilize":
                    continue
                modules.setdefault(top, set()).add(str(path.relative_to(REPO_ROOT)))
    for module in DYNAMIC_IMPORTS:
        modules.setdefault(module, {"<dynamic importlib.import_module>"})
    return modules


def _manifest() -> dict[str, object]:
    with PYPROJECT.open("rb") as handle:
        return tomllib.load(handle)["project"]


def _runtime_requirements() -> set[str]:
    return {_requirement_name(req) for req in _manifest().get("dependencies", [])}


def _optional_requirements() -> dict[str, set[str]]:
    groups = _manifest().get("optional-dependencies", {})
    return {
        group: {_requirement_name(req) for req in reqs} for group, reqs in groups.items()
    }


def _distributions_for(module: str) -> set[str]:
    provided = packages_distributions().get(module)
    if provided:
        return {_canonical(dist) for dist in provided}
    return {_canonical(module)}


@pytest.fixture(scope="module")
def imported_modules() -> dict[str, set[str]]:
    return _imported_top_level_modules()


def test_every_third_party_import_is_declared(imported_modules: dict[str, set[str]]) -> None:
    runtime = _runtime_requirements()
    optional = _optional_requirements()
    declared = set(runtime)
    for names in optional.values():
        declared |= names

    undeclared: list[str] = []
    for module, importers in sorted(imported_modules.items()):
        if _distributions_for(module) & declared:
            continue
        where = ", ".join(sorted(importers)[:3])
        undeclared.append(f"{module} (imported by {where})")

    assert not undeclared, (
        "third-party modules imported by src/stabilize/ with no matching "
        "declaration in pyproject.toml: " + "; ".join(undeclared)
    )


def test_no_runtime_dependency_is_unimported(imported_modules: dict[str, set[str]]) -> None:
    provided_by_imports: set[str] = set()
    for module in imported_modules:
        provided_by_imports |= _distributions_for(module)

    unimported = sorted(
        _runtime_requirements() - provided_by_imports - set(DECLARED_PENDING_USE)
    )

    assert not unimported, (
        "declared in [project].dependencies but imported by no module under "
        "src/stabilize/, so every consumer installs it for nothing: "
        + ", ".join(unimported)
    )


def test_the_pending_use_exemptions_are_still_pending() -> None:
    """An exemption that has been taken up must be removed, or it hides the next one."""
    imported = _imported_top_level_modules()
    provided: set[str] = set()
    for module in imported:
        provided |= _distributions_for(module)

    now_used = sorted(set(DECLARED_PENDING_USE) & provided)

    assert not now_used, (
        "these are now imported, so their DECLARED_PENDING_USE entry is stale "
        "and must be deleted: " + ", ".join(now_used)
    )


def test_guarded_imports_are_optional_not_runtime(imported_modules: dict[str, set[str]]) -> None:
    runtime = _runtime_requirements()
    optional = _optional_requirements()

    misplaced: list[str] = []
    for module, group in sorted(GUARDED_IMPORT_EXTRAS.items()):
        if module not in imported_modules:
            continue
        distributions = _distributions_for(module)
        if distributions & runtime:
            misplaced.append(f"{module} is a hard dependency but is imported under a guard")
        elif not (distributions & optional.get(group, set())):
            misplaced.append(f"{module} is not declared in the '{group}' extra")

    assert not misplaced, "; ".join(misplaced)


def test_the_check_can_see_a_known_positive() -> None:
    """The scanner must find an import it is pointed at, or its silence means nothing."""
    modules = _imported_top_level_modules()

    assert "resilient_circuit" in modules, (
        "the AST scan found no resilient_circuit import, so it cannot be trusted "
        "to report the absence of any other import"
    )
    assert any(
        importer.endswith("persistence/transaction.py")
        for importer in modules["resilient_circuit"]
    )
    assert _distributions_for("resilient_circuit") == {"resilient-circuit"}
