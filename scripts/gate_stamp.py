"""Record which tree the gate was green on, so a redundant re-run can be skipped.

Running the full gate, then running it again inside `release.py`, costs ~10
minutes and tells you nothing the first run did not. But skipping tests on an
assertion ("--skip-tests") is worse than the waste: it cannot tell a tree that
was verified from one that was never touched.

So the gate writes a stamp keyed to a fingerprint of the source tree, and the
release reads it. A skip then rests on evidence -- these exact bytes passed --
rather than on someone's word. Change any source file and the fingerprint moves,
the stamp stops matching, and the tests run again.

    python scripts/gate_stamp.py write   --results "tests=1670 lint=ok"
    python scripts/gate_stamp.py check
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STAMP_PATH = PROJECT_ROOT / ".gate-stamp.json"

# What the gate actually exercises. A change to any of it invalidates the stamp.
TRACKED_GLOBS = ("src/**/*.py", "tests/**/*.py", "golden_standard_tests/**/*.py")
TRACKED_FILES = ("pyproject.toml", "Makefile")


def fingerprint() -> str:
    """A stable digest of the working tree's source content."""
    h = hashlib.sha256()
    paths: list[Path] = []
    for pattern in TRACKED_GLOBS:
        paths.extend(PROJECT_ROOT.glob(pattern))
    for name in TRACKED_FILES:
        p = PROJECT_ROOT / name
        if p.is_file():
            paths.append(p)
    for path in sorted(set(paths), key=lambda p: str(p)):
        if "__pycache__" in path.parts:
            continue
        rel = path.relative_to(PROJECT_ROOT).as_posix()
        h.update(rel.encode())
        h.update(b"\0")
        h.update(hashlib.sha256(path.read_bytes()).digest())
    return h.hexdigest()


def _head() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=10,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def write(results: str, expect: str = "") -> None:
    """Record the stamp, refusing if the tree moved while the gate ran.

    A 14-minute gate can have source edited underneath it. Fingerprinting only
    at the end would bless bytes the run never tested -- the stale-gate problem
    the stamp exists to prevent, rebuilt inside the stamp. So the caller passes
    the fingerprint taken BEFORE the run and this refuses on a mismatch.
    """
    current = fingerprint()
    if expect and expect != current:
        print(
            "REFUSING to write a gate stamp: the source tree changed while the "
            f"gate ran (started {expect[:12]}..., now {current[:12]}...). "
            "The run did not test the bytes that are here now."
        )
        raise SystemExit(1)
    _write(results)


def _write(results: str) -> None:
    STAMP_PATH.write_text(
        json.dumps(
            {
                "fingerprint": fingerprint(),
                "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "head": _head(),
                "results": results,
            },
            indent=2,
        )
        + "\n"
    )
    print(f"gate stamp written: {STAMP_PATH.name} ({results})")


def read_valid() -> dict | None:
    """The stamp, if it was recorded against the tree as it is right now."""
    if not STAMP_PATH.is_file():
        return None
    try:
        data = json.loads(STAMP_PATH.read_text())
    except Exception:
        return None
    if data.get("fingerprint") != fingerprint():
        return None
    return data


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("action", choices=["write", "check", "fingerprint"])
    ap.add_argument("--results", default="")
    ap.add_argument("--expect", default="")
    args = ap.parse_args()

    if args.action == "fingerprint":
        print(fingerprint())
        return 0
    if args.action == "write":
        write(args.results, args.expect)
        return 0

    stamp = read_valid()
    if stamp is None:
        print("no valid gate stamp for this tree (it changed, or was never recorded)")
        return 1
    print(f"gate was green on these exact bytes at {stamp['recorded_at']} "
          f"(HEAD {stamp['head']}): {stamp['results']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
