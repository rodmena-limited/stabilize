"""The baseline as one pytest process per worker over balanced file shards, combined into one coverage map."""

from __future__ import annotations

import json
import os
import subprocess
import threading
from collections import defaultdict
from pathlib import Path
from typing import Any

from proofs import assert_baseline_usable

COVERAGE_ARGS = ["--cov=stabilize", "--cov-context=test", "--cov-report="]


def suite_files(ws: Path, suites: list[str]) -> list[str]:
    files: list[str] = []
    for suite in suites:
        path = ws / suite
        if path.is_file():
            files.append(suite)
        elif path.is_dir():
            files.extend(str(p.relative_to(ws)) for p in sorted(path.rglob("test_*.py")))
        else:
            raise SystemExit(f"suite {suite!r} is neither a file nor a directory under {ws}")
    return sorted(dict.fromkeys(files))


def shard_files(files: list[str], weight: dict[str, int], count: int) -> list[list[str]]:
    """Split *files* into *count* shards balanced by *weight*, heaviest first, each shard in sorted order."""
    shards: list[list[str]] = [[] for _ in range(max(count, 1))]
    loads = [0] * len(shards)
    for name in sorted(files, key=lambda f: (-weight.get(f, 0), f)):
        lightest = loads.index(min(loads))
        shards[lightest].append(name)
        loads[lightest] += weight.get(name, 0)
    return [sorted(s) for s in shards]


def combine(out: Path, rc: Path, python: str, parts: list[Path]) -> None:
    for stale in out.glob(".coverage*"):
        stale.unlink()
    missing = [str(p) for p in parts if not p.exists()]
    if missing:
        raise SystemExit(f"baseline shards wrote no coverage data: {missing}")
    subprocess.run(
        [python, "-m", "coverage", "combine", "--keep", "--rcfile", str(rc), *map(str, parts)],
        cwd=out,
        env={**os.environ, "COVERAGE_FILE": str(out / ".coverage")},
        check=True,
    )


def run(out: Path, workers: int, runner: Any) -> None:
    """Run the suites split across *workers* processes with per-test coverage, then build the map."""
    ws0 = out / "ws-0"
    rc = ws0 / "audit" / "redproof" / "coveragerc"
    files = suite_files(ws0, runner.SUITES)
    weight = {f: (ws0 / f).stat().st_size for f in files}
    shards = [s for s in shard_files(files, weight, workers) if s]
    runner.log(f"baseline: {len(files)} test files in {len(shards)} shards with per-test coverage contexts")
    results: dict[str, dict[str, Any]] = {}
    outcomes: dict[int, dict[str, Any]] = {}
    lock = threading.Lock()

    def one(i: int, shard: list[str]) -> None:
        part_dir = out / f"baseline-{i}"
        part_dir.mkdir(exist_ok=True)
        for stale in part_dir.glob(".coverage*"):
            stale.unlink()
        env_extra = {"COVERAGE_FILE": str(part_dir / ".coverage"), "COVERAGE_CORE": "ctrace"}
        res = runner.run_pytest(out / f"ws-{i}", i, shard, [*COVERAGE_ARGS, f"--cov-config={rc}"], 7200, env_extra)
        with lock:
            outcomes[i] = res
            runner.log(f"baseline shard {i}: exit {res.get('exitstatus')}, {len(res.get('results') or {})} results")

    threads = [threading.Thread(target=one, args=(i, shard)) for i, shard in enumerate(shards)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for i in sorted(outcomes):
        assert_baseline_usable(outcomes[i])
        results.update(outcomes[i]["results"])
    failed = sorted(k for k, v in results.items() if v["outcome"] in ("failed", "error"))
    if failed:
        runner.log(f"{len(failed)} baseline failures; rerunning them once without coverage")
        again = runner.run_pytest(ws0, 0, failed, [], 3600)["results"]
        for k in failed:
            if again.get(k, {}).get("outcome") == "passed":
                results[k] = {"outcome": "passed", "kind": "flaky-baseline"}
    combine(out, rc, runner.worker_python(ws0), [out / f"baseline-{i}" / ".coverage" for i in range(len(shards))])
    (out / "baseline.json").write_text(json.dumps(results))
    cmap = runner.coverage_map(out, ws0)
    (out / "coverage_map.json").write_text(json.dumps(cmap))
    passing = {k for k, v in results.items() if v["outcome"] == "passed"}
    covered = set().union(*cmap.values()) & passing if cmap else set()
    runner.log(f"coverage map: {len(cmap)} lines, {len(covered)}/{len(passing)} passing tests with at least one line")
    if len(covered) < 0.9 * len(passing):
        raise SystemExit(f"coverage map implausible: only {len(covered)} of {len(passing)} passing tests covered")
    counts: dict[str, int] = defaultdict(int)
    for v in results.values():
        counts[v["outcome"]] += 1
    runner.log(f"baseline: {dict(counts)}")
