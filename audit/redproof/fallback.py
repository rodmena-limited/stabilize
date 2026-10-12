from __future__ import annotations

import ast
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from mutants import Mutant, all_mutants, mutants_for_file

PER_FILE_MUTANTS = 60
HELPER_MUTANTS = 20
OPERATOR_ORDER = {"base-drop": 0, "str-const": 1, "bool-const": 2, "int-const": 3, "return-none": 4}


def imported_sources(test_file: Path, src_root: Path) -> list[str]:
    tree = ast.parse(test_file.read_text())
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("stabilize"):
            modules.add(node.module)
            for alias in node.names:
                modules.add(f"{node.module}.{alias.name}")
        elif isinstance(node, ast.Import):
            modules.update(a.name for a in node.names if a.name.startswith("stabilize"))
    found: list[str] = []
    for mod in sorted(modules):
        parts = mod.split(".")[1:]
        rel = Path(*parts) if parts else Path()
        options = (
            [src_root / "__init__.py"]
            if not parts
            else [src_root / rel.with_suffix(".py"), src_root / rel / "__init__.py"]
        )
        for candidate in options:
            if candidate.exists():
                found.append(candidate.relative_to(src_root.parent.parent).as_posix())
    return sorted(set(found))


def defining_sources(test_file: Path, ws: Path) -> list[str]:
    import json
    import subprocess

    tree = ast.parse(test_file.read_text())
    names: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("stabilize"):
            names.extend((node.module, a.name) for a in node.names)
    script = (
        "import importlib, inspect, json, sys\n"
        "out = set()\n"
        "for mod, name in json.loads(sys.argv[1]):\n"
        "    try:\n"
        "        m = importlib.import_module(mod)\n"
        "        obj = getattr(m, name, None)\n"
        "        plain = isinstance(obj, (int, str, float, tuple, frozenset, set, dict, list))\n"
        "        f = inspect.getsourcefile(obj) if obj is not None and not plain else None\n"
        "        out.add(f or inspect.getsourcefile(m))\n"
        "    except Exception:\n"
        "        pass\n"
        "print(json.dumps(sorted(x for x in out if x)))\n"
    )
    proc = subprocess.run(
        [str(ws / ".venv/bin/python"), "-c", script, json.dumps(names)],
        cwd=ws,
        capture_output=True,
        text=True,
    )
    try:
        files = json.loads(proc.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return []
    marker = str(ws / "src") + "/"
    return sorted({"src/" + f[len(marker) :] for f in files if f.startswith(marker)})


def helper_mutants(test_file: Path, rel: str) -> list[Mutant]:
    tree = ast.parse(test_file.read_text())
    spans = [
        (n.lineno, n.end_lineno or n.lineno)
        for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name.startswith("test")
    ]
    return [m for m in mutants_for_file(test_file, rel) if not any(a <= m.line <= b for a, b in spans)]


def plan(state: Any, ws0: Path) -> list[tuple[Mutant, list[str]]]:
    remaining = sorted(t for t in state.passing if t not in state.proven)
    by_file: dict[str, list[str]] = defaultdict(list)
    for t in remaining:
        by_file[t.split("::", 1)[0]].append(t)
    src_root = ws0 / "src" / "stabilize"
    by_path: dict[str, list[Mutant]] = defaultdict(list)
    for m in all_mutants(src_root):
        by_path[m.path].append(m)
    covered_lines = {k for k, v in state.line_tests.items() if v}
    jobs: list[tuple[Mutant, list[str]]] = []
    for test_file, tests in sorted(by_file.items()):
        sources = sorted(
            set(imported_sources(ws0 / test_file, src_root)) | set(defining_sources(ws0 / test_file, ws0))
        )
        pool = [m for p in sources for m in by_path.get(p, []) if m.key not in state.tried]
        pool.sort(
            key=lambda m: (f"{m.path}:{m.line}" in covered_lines, OPERATOR_ORDER.get(m.operator, 9), m.key)
        )
        jobs.extend((m, tests) for m in pool[:PER_FILE_MUTANTS])
        helpers = [m for m in helper_mutants(ws0 / test_file, test_file) if m.key not in state.tried]
        helpers.sort(key=lambda m: (OPERATOR_ORDER.get(m.operator, 9), m.key))
        jobs.extend((m, tests) for m in helpers[:HELPER_MUTANTS])
    return jobs


def run(
    state: Any, out: Path, workers: int, budget_s: float, try_mutant: Any, wait_host: Any, log: Any
) -> None:
    jobs = plan(state, out / "ws-0")
    log(f"fallback: {len(jobs)} mutant runs for {len(state.passing) - len(state.proven)} unproven tests")
    lock = threading.Lock()
    deadline = time.monotonic() + budget_s

    def worker(i: int) -> None:
        ws = out / f"ws-{i}"
        while time.monotonic() < deadline:
            with lock:
                if not jobs:
                    return
                m, tests = jobs.pop(0)
                ids = [t for t in tests if t not in state.proven]
            if not ids:
                continue
            wait_host()
            confirmed, discarded = try_mutant(ws, i, m, ids)
            state.record(m, ids, confirmed, discarded)
            log(f"fb w{i} {m.key} ran={len(ids)} proven+={len(confirmed)} total_proven={len(state.proven)}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
