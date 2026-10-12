from __future__ import annotations

import ast
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from mutants import Mutant, all_mutants

PER_FILE_MUTANTS = 40
OPERATOR_ORDER = {"str-const": 0, "bool-const": 1, "int-const": 2, "return-none": 3}


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
        sources = imported_sources(ws0 / test_file, src_root)
        pool = [m for p in sources for m in by_path.get(p, []) if m.key not in state.tried]
        pool.sort(
            key=lambda m: (f"{m.path}:{m.line}" in covered_lines, OPERATOR_ORDER.get(m.operator, 9), m.key)
        )
        jobs.extend((m, tests) for m in pool[:PER_FILE_MUTANTS])
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
