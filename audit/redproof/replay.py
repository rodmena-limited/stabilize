from __future__ import annotations

import threading
from collections import defaultdict
from pathlib import Path
from typing import Any

from fallback import helper_mutants
from mutants import Mutant, all_mutants

KNOWN = Path(__file__).resolve().parent / "known_proofs.tsv"


def load_known(path: Path = KNOWN) -> dict[str, str]:
    if not path.exists():
        return {}
    known: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if line.strip() and "\t" in line:
            test, key = line.split("\t", 1)
            known[test] = key.strip()
    return known


def write_known(proof_tsv: Path, path: Path = KNOWN) -> int:
    rows: dict[str, str] = load_known(path)
    for line in proof_tsv.read_text().splitlines()[1:]:
        test, key = line.split("\t")[:2]
        rows[test] = key
    path.write_text("".join(f"{t}\t{k}\n" for t, k in sorted(rows.items())))
    return len(rows)


def _lookup(ws0: Path, keys: set[str]) -> dict[str, Mutant]:
    found = {m.key: m for m in all_mutants(ws0 / "src" / "stabilize") if m.key in keys}
    for test_file in sorted({k.split(":", 1)[0] for k in keys if k.startswith("tests/")}):
        if (ws0 / test_file).exists():
            found.update({m.key: m for m in helper_mutants(ws0 / test_file, test_file) if m.key in keys})
    return found


def run(state: Any, out: Path, workers: int, try_mutant: Any, wait_host: Any, log: Any) -> None:
    known = {t: k for t, k in load_known().items() if t in state.passing and t not in state.proven}
    by_key: dict[str, list[str]] = defaultdict(list)
    for test, key in known.items():
        by_key[key].append(test)
    mutants = _lookup(out / "ws-0", set(by_key))
    jobs = [(mutants[k], tests) for k, tests in sorted(by_key.items()) if k in mutants]
    log(
        f"replay: {len(known)} known proofs, {len(jobs)} mutants still apply, {len(by_key) - len(jobs)} no longer do"
    )
    lock = threading.Lock()

    def worker(i: int) -> None:
        ws = out / f"ws-{i}"
        while True:
            with lock:
                if not jobs:
                    return
                m, tests = jobs.pop(0)
            wait_host()
            confirmed, discarded = try_mutant(ws, i, m, tests)
            state.record(m, tests, confirmed, discarded)
            log(
                f"replay w{i} {m.key} ran={len(tests)} proven+={len(confirmed)} total_proven={len(state.proven)}"
            )

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
