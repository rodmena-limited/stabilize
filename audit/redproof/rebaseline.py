"""Rewrite known_proofs.tsv from line-based mutant keys to stable keys, reading the sources at the base commit."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from mutants import mutants_for_file  # noqa: E402
from replay import KNOWN, load_known  # noqa: E402

REPO = Path(__file__).resolve().parents[2]


def _source_at(commit: str, rel: str) -> str | None:
    proc = subprocess.run(["git", "-C", str(REPO), "show", f"{commit}:{rel}"], capture_output=True, text=True)
    return proc.stdout if proc.returncode == 0 else None


def rebaseline(commit: str, known_path: Path = KNOWN) -> tuple[int, int, int]:
    known = load_known(known_path)
    by_file: dict[str, set[str]] = defaultdict(set)
    for key in known.values():
        if len(key.split(":")) == 4:
            by_file[key.split(":", 1)[0]].add(key)
    mapping: dict[str, str] = {}
    with tempfile.TemporaryDirectory() as tmp:
        for rel, keys in sorted(by_file.items()):
            if not rel.startswith("src/"):
                continue
            source = _source_at(commit, rel)
            if source is None:
                continue
            path = Path(tmp) / Path(rel).name
            path.write_text(source)
            try:
                for m in mutants_for_file(path, rel):
                    if m.key in keys:
                        mapping[m.key] = m.stable_key
            except SyntaxError:
                continue
    rows: dict[str, str] = {}
    dropped = 0
    for test, key in known.items():
        if key.startswith("tests/") or key.startswith("golden_standard_tests/"):
            dropped += 1
            continue
        if len(key.split(":")) == 5:
            rows[test] = key
        elif key in mapping:
            rows[test] = mapping[key]
        else:
            dropped += 1
    known_path.write_text("".join(f"{t}\t{k}\n" for t, k in sorted(rows.items())))
    return len(known), len(rows), dropped


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="the commit the existing proofs were made on")
    ap.add_argument("--known", type=Path, default=KNOWN)
    a = ap.parse_args()
    before, after, dropped = rebaseline(a.base, a.known)
    print(f"known proofs: {before} before, {after} after, {dropped} dropped (test-file mutants or unmapped)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
