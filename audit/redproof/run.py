from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from mutants import Mutant, all_mutants, apply  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
PER_LINE_CAP = 12
PER_TEST_TRIES = 30
BATCH_CAP = 400
SPECIFIC_BATCH = 50
PG = os.environ.get("REDPROOF_PG", "postgresql://postgres:gate@127.0.0.1:55432")
PG_CONTAINER = os.environ.get("REDPROOF_PG_CONTAINER", "stabilize-gate-pg")
PYTHON = os.environ.get(
    "REDPROOF_PYTHON", "/home/farshid/.local/share/uv/python/cpython-3.14-linux-x86_64-gnu/bin/python3.14"
)
SUITES = os.environ.get("REDPROOF_SUITES", "tests golden_standard_tests").split()
EXTRA_TESTS = [p for p in os.environ.get("REDPROOF_EXTRA_TESTS", "").split() if p]


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def host_ok() -> bool:
    load = float(Path("/proc/loadavg").read_text().split()[0])
    avail = 0
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            avail = int(line.split()[1]) // 1024
    return load < 8 and avail >= 6144


def wait_host() -> None:
    while not host_ok():
        time.sleep(20)


def fresh_db(name: str) -> str:
    if not PG_CONTAINER:
        import psycopg

        with psycopg.connect(f"{PG}/postgres", autocommit=True) as conn:
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
            conn.execute(f'CREATE DATABASE "{name}"')
        return f"{PG}/{name}"
    subprocess.run(
        [
            "docker",
            "exec",
            PG_CONTAINER,
            "psql",
            "-U",
            "postgres",
            "-qc",
            f"DROP DATABASE IF EXISTS {name} WITH (FORCE)",
            "-c",
            f"CREATE DATABASE {name}",
        ],
        check=True,
        capture_output=True,
    )
    return f"{PG}/{name}"


def prepare(out: Path, workers: int, sha: str) -> None:
    for i in range(workers):
        ws = out / f"ws-{i}"
        if (ws / ".venv").exists():
            continue
        ws.mkdir(parents=True, exist_ok=True)
        if (REPO / ".git").exists():
            archive = subprocess.run(
                ["git", "-C", str(REPO), "archive", sha], check=True, capture_output=True
            ).stdout
            subprocess.run(["tar", "-x", "-C", str(ws)], input=archive, check=True)
        else:
            skip = shutil.ignore_patterns(".venv*", ".old", ".redproof", "__pycache__", "*.log", ".coverage*")
            shutil.copytree(REPO, ws, ignore=skip, dirs_exist_ok=True)
        shutil.copytree(REPO / "audit" / "redproof", ws / "audit" / "redproof", dirs_exist_ok=True)
        for extra in EXTRA_TESTS:
            shutil.copy(extra, ws / "tests" / Path(extra).name)
        subprocess.run(["uv", "venv", "-q", "-p", PYTHON, ".venv"], cwd=ws, check=True)
        subprocess.run(
            ["uv", "pip", "install", "-q", "-p", ".venv/bin/python", "-e", ".[dev,observability,cli]"],
            cwd=ws,
            check=True,
        )
        where = subprocess.run(
            [str(ws / ".venv/bin/python"), "-c", "import stabilize; print(stabilize.__file__)"],
            cwd="/",
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if not where.startswith(str(ws / "src")):
            raise SystemExit(f"ws-{i} imports stabilize from {where}, not its own src")
        log(f"ws-{i} ready, stabilize from {where}")


def env_for(ws: Path, worker: int, result_file: Path) -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONPATH=str(ws / "audit" / "redproof"),
        REDPROOF_OUT=str(result_file),
        STABILIZE_TEST_DSN=fresh_db(f"rp_w{worker}"),
        PATH=f"{ws / '.venv' / 'bin'}:{env.get('PATH', '')}",
    )
    env.pop("VIRTUAL_ENV", None)
    return env


def run_pytest(ws: Path, worker: int, ids: list[str], extra: list[str], timeout: int) -> dict:
    result_file = ws / f".redproof-{worker}.json"
    result_file.unlink(missing_ok=True)
    args_file = ws / f".redproof-{worker}.args"
    args_file.write_text("\n".join(ids))
    cmd = [
        str(ws / ".venv/bin/python"),
        "-m",
        "pytest",
        f"@{args_file}",
        "-q",
        "-p",
        "no:cacheprovider",
        "-p",
        "redproof_plugin",
        "--timeout",
        "120",
        *extra,
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=ws,
        env=env_for(ws, worker, result_file),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()
        return {"exitstatus": -9, "results": {}}
    if not result_file.exists():
        return {"exitstatus": proc.returncode, "results": {}}
    return json.loads(result_file.read_text())


def coverage_map(out: Path, ws: Path) -> dict[str, list[str]]:
    from coverage import CoverageData

    data = CoverageData(basename=str(out / ".coverage"))
    data.read()
    lines: dict[str, set[str]] = defaultdict(set)
    for measured in data.measured_files():
        marker = "/src/stabilize/"
        if marker not in measured:
            continue
        rel = "src/stabilize/" + measured.split(marker, 1)[1]
        for lineno, contexts in (data.contexts_by_lineno(measured) or {}).items():
            for ctx in contexts:
                nodeid = ctx.rsplit("|", 1)[0]
                if nodeid:
                    lines[f"{rel}:{lineno}"].add(nodeid)
    return {k: sorted(v) for k, v in lines.items()}


def baseline(out: Path) -> None:
    ws = out / "ws-0"
    wait_host()
    log("baseline with per-test coverage contexts")
    cov_env_file = out / ".coverage"
    os.environ["COVERAGE_FILE"] = str(cov_env_file)
    os.environ["COVERAGE_CORE"] = "ctrace"
    res = run_pytest(ws, 0, SUITES, ["--cov=stabilize", "--cov-context=test", "--cov-report="], 7200)
    os.environ.pop("COVERAGE_FILE")
    os.environ.pop("COVERAGE_CORE")
    results = res["results"]
    failed = sorted(k for k, v in results.items() if v["outcome"] in ("failed", "error"))
    if failed:
        log(f"{len(failed)} baseline failures; rerunning them once without coverage")
        again = run_pytest(ws, 0, failed, [], 3600)["results"]
        for k in failed:
            if again.get(k, {}).get("outcome") == "passed":
                results[k] = {"outcome": "passed", "kind": "flaky-baseline"}
    (out / "baseline.json").write_text(json.dumps(results))
    cmap = coverage_map(out, ws)
    (out / "coverage_map.json").write_text(json.dumps(cmap))
    passing = {k for k, v in results.items() if v["outcome"] == "passed"}
    covered = set().union(*cmap.values()) & passing if cmap else set()
    log(
        f"coverage map: {len(cmap)} lines, {len(covered)}/{len(passing)} passing tests with at least one line"
    )
    if len(covered) < 0.9 * len(passing):
        raise SystemExit(
            f"coverage map implausible: only {len(covered)} of {len(passing)} passing tests covered"
        )
    counts = defaultdict(int)
    for v in results.values():
        counts[v["outcome"]] += 1
    log(f"baseline: {dict(counts)}")


class State:
    def __init__(self, out: Path) -> None:
        self.out = out
        self.lock = threading.RLock()
        self.baseline = json.loads((out / "baseline.json").read_text())
        self.passing = {k for k, v in self.baseline.items() if v["outcome"] == "passed"}
        cmap = json.loads((out / "coverage_map.json").read_text())
        self.line_tests = {k: set(v) & self.passing for k, v in cmap.items()}
        src_root = out / "ws-0" / "src" / "stabilize"
        by_line: dict[str, list[Mutant]] = defaultdict(list)
        for m in all_mutants(src_root):
            by_line[f"{m.path}:{m.line}"].append(m)
        rng = random.Random(1978)
        self.mutants: dict[str, Mutant] = {}
        self.cover: dict[str, set[str]] = {}
        for line_key, ms in by_line.items():
            rng.shuffle(ms)
            for m in ms[:PER_LINE_CAP]:
                self.mutants[m.key] = m
                self.cover[m.key] = self.line_tests.get(line_key, set())
        self.proven: dict[str, str] = {}
        self.tries: dict[str, int] = defaultdict(int)
        self.tried: set[str] = set()
        self.in_flight: set[str] = set()
        self.no_candidates: set[str] = set()
        self.file_tries: dict[str, int] = defaultdict(int)
        self.file_proofs: dict[str, int] = defaultdict(int)
        self.broad_results: list[int] = []
        self.file_recent: dict[str, list[int]] = {}
        self._resume()

    def _resume(self) -> None:
        proof = self.out / "proof.tsv"
        if proof.exists():
            for line in proof.read_text().splitlines()[1:]:
                nodeid, mkey = line.split("\t")[:2]
                self.proven[nodeid] = mkey
                self.file_proofs[mkey.split(":", 1)[0]] += 1
        tried = self.out / "tried.tsv"
        if tried.exists():
            for line in tried.read_text().splitlines():
                mkey, *ids = line.split("\t")
                self.tried.add(mkey)
                self.file_tries[mkey.split(":", 1)[0]] += 1
                if len(ids) <= SPECIFIC_BATCH:
                    for t in ids:
                        self.tries[t] += 1

    def active(self) -> set[str]:
        return {t for t in self.passing if t not in self.proven and self.tries[t] < PER_TEST_TRIES}

    def pick(self, broad: bool) -> tuple[Mutant, list[str]] | None:
        with self.lock:
            act = self.active()
            if not act:
                return None
            best_key, best_score, best_size = None, 0, 0
            if broad:
                for k, cov in self.cover.items():
                    if k in self.tried or k in self.in_flight:
                        continue
                    recent = self.file_recent.get(self.mutants[k].path)
                    weight = 1 + (sum(recent) / len(recent) if recent else 5)
                    score = len(cov & act) * weight
                    if score > best_score:
                        best_key, best_score = k, score
            else:
                target = min(act, key=lambda t: (self.tries[t], t))
                for k, cov in self.cover.items():
                    if k in self.tried or k in self.in_flight or target not in cov:
                        continue
                    if best_key is None or len(cov) < best_size:
                        best_key, best_size = k, len(cov)
                if best_key is None:
                    self.no_candidates.add(target)
                    self.tries[target] = PER_TEST_TRIES
                    return self.pick(True) if self.active() else None
            if best_key is None:
                return None
            ids = sorted(self.cover[best_key] & act)
            if len(ids) > BATCH_CAP:
                ids = sorted(random.Random(best_key).sample(ids, BATCH_CAP))
            self.in_flight.add(best_key)
            return self.mutants[best_key], ids

    def record(self, m: Mutant, ran: list[str], confirmed: dict[str, str], discarded: bool) -> None:
        with self.lock:
            self.in_flight.discard(m.key)
            self.tried.add(m.key)
            self.file_tries[m.path] += 1
            with open(self.out / "tried.tsv", "a") as fh:
                fh.write("\t".join([m.key, *([] if discarded else ran)]) + "\n")
            if discarded:
                return
            if len(ran) <= SPECIFIC_BATCH:
                for t in ran:
                    self.tries[t] += 1
            proof = self.out / "proof.tsv"
            new = not proof.exists()
            with open(proof, "a") as fh:
                if new:
                    fh.write("test\tmutant\toperator\treplacement\tfailure\n")
                for t, kind in confirmed.items():
                    if t not in self.proven:
                        self.proven[t] = m.key
                        self.file_proofs[m.path] += 1
                        fh.write(f"{t}\t{m.key}\t{m.operator}\t{m.replacement[:80]}\t{kind}\n")


def try_mutant(ws: Path, worker: int, m: Mutant, ids: list[str]) -> tuple[dict[str, str], bool]:
    target = ws / m.path
    original = target.read_text()
    try:
        mutated = apply(original, m)
    except SyntaxError:
        return {}, True
    target.write_text(mutated)
    try:
        first = run_pytest(ws, worker, ids, [], 1800)
        res = first["results"]
        if first["exitstatus"] not in (0, 1) or any(t not in res for t in ids):
            return {}, True
        red = [t for t in ids if res[t]["outcome"] == "failed"]
        if not red:
            return {}, False
        second = run_pytest(ws, worker, red, [], 1800)["results"]
        return {t: res[t]["kind"] for t in red if second.get(t, {}).get("outcome") == "failed"}, False
    finally:
        target.write_text(original)


def prove(out: Path, workers: int, budget_s: float) -> None:
    state = State(out)
    log(
        f"mutants {len(state.mutants)}, passing tests {len(state.passing)}, proven so far {len(state.proven)}"
    )
    import replay

    replay.run(state, out, workers, try_mutant, wait_host, log)
    deadline = time.monotonic() + budget_s
    counter = {"n": 0}

    def worker_loop(i: int) -> None:
        ws = out / f"ws-{i}"
        while time.monotonic() < deadline:
            wait_host()
            with state.lock:
                counter["n"] += 1
                recent = state.broad_results[-5:]
                paying = len(recent) < 5 or sorted(recent)[2] >= 10
                broad = counter["n"] % 2 == 1 and paying
            picked = state.pick(broad)
            if picked is None:
                return
            m, ids = picked
            confirmed, discarded = try_mutant(ws, i, m, ids)
            state.record(m, ids, confirmed, discarded)
            with state.lock:
                if broad:
                    state.broad_results.append(len(confirmed))
                if not discarded:
                    state.file_recent[m.path] = (state.file_recent.get(m.path, []) + [len(confirmed)])[-3:]
            log(
                f"w{i} {m.key} ran={len(ids)} proven+={len(confirmed)} discarded={discarded} "
                f"total_proven={len(state.proven)}/{len(state.passing)} active={len(state.active())}"
            )

    threads = [threading.Thread(target=worker_loop, args=(i,)) for i in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    report(out, state)


def report(out: Path, state: State) -> None:
    rows = []
    for t in sorted(state.baseline):
        outcome = state.baseline[t]["outcome"]
        if t in state.proven:
            continue
        status = "UNPROVEN" if outcome == "passed" else f"NOT-RUN-IN-BASELINE({outcome})"
        if outcome == "passed" and t in state.no_candidates:
            status = "UNPROVEN(no untried mutant on any line it executes)"
        rows.append(f"{t}\t{status}\t{state.tries[t]}")
    (out / "unproven.tsv").write_text("test\tstatus\tmutants_tried\n" + "\n".join(rows) + "\n")
    log(f"proven {len(state.proven)}; unproven or not run {len(rows)}; see {out / 'unproven.tsv'}")


def collected_now() -> dict[str, dict[str, str]]:
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider", *SUITES],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    ids = [line.strip() for line in proc.stdout.splitlines() if "::" in line and not line.startswith(" ")]
    return {i: {"outcome": "collected"} for i in ids}


def manual_proven() -> set[str]:
    found: set[str] = set()
    for diff in sorted((REPO / "audit" / "redproof" / "manual").glob("*.diff")):
        first = diff.read_text().splitlines()[:1]
        if first and first[0].startswith("test-id: "):
            found.add(first[0][len("test-id: ") :].strip())
    return found


def check(out: Path, allowlist: Path, collect: bool = False) -> int:
    baseline = collected_now() if collect else json.loads((out / "baseline.json").read_text())
    proven = {line.split("\t")[0] for line in (out / "proof.tsv").read_text().splitlines()[1:]}
    proven |= manual_proven()
    allowed: dict[str, str] = {}
    bad = 0
    if allowlist.exists():
        for line in allowlist.read_text().splitlines():
            if not line.strip():
                continue
            nodeid, _, reason = line.partition("\t")
            if not reason.strip():
                print(f"ALLOWLIST ENTRY WITHOUT REASON: {nodeid}")
                bad += 1
            elif nodeid not in baseline:
                print(f"ALLOWLIST ENTRY FOR A TEST THAT DOES NOT EXIST: {nodeid}")
                bad += 1
            elif nodeid in proven:
                print(f"ALLOWLIST ENTRY FOR A TEST THAT IS NOW PROVEN: {nodeid}")
                bad += 1
            allowed[nodeid] = reason
    for t in sorted(baseline):
        if t not in proven and t not in allowed:
            print(f"NOT PROVEN RED: {t} ({baseline[t]['outcome']} in baseline)")
            bad += 1
    print(f"proven {len(proven)} / {len(baseline)} collected; allowlisted {len(allowed)}; violations {bad}")
    return 1 if bad else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["prepare", "baseline", "prove", "fallback", "check", "all", "export-known"])
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--sha", default="HEAD")
    ap.add_argument("--budget-hours", type=float, default=3.0)
    ap.add_argument("--allowlist", type=Path, default=REPO / "audit" / "redproof" / "unproven_allowed.txt")
    ap.add_argument("--collect", action="store_true", help="check the tests collected now instead of the baseline's")
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    if a.command in ("prepare", "all"):
        prepare(a.out, a.workers, a.sha)
    if a.command in ("baseline", "all"):
        baseline(a.out)
    if a.command in ("prove", "all"):
        prove(a.out, a.workers, a.budget_hours * 3600)
    if a.command in ("fallback", "all"):
        import fallback

        state = State(a.out)
        fallback.run(state, a.out, a.workers, a.budget_hours * 3600, try_mutant, wait_host, log)
        report(a.out, state)
    if a.command == "export-known":
        import replay

        log(f"known proofs: {replay.write_known(a.out / 'proof.tsv')}")
        return 0
    if a.command in ("check", "all"):
        return check(a.out, a.allowlist, a.collect)
    return 0


if __name__ == "__main__":
    sys.exit(main())
