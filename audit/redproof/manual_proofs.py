from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import host

REPO = Path(__file__).resolve().parents[2]
MANUAL = REPO / "audit" / "redproof" / "manual"
PYTHON = os.environ.get("MANUAL_PROOFS_PYTHON", str(REPO / ".venv" / "bin" / "python"))
EXPORT = ["src", "tests", "migrations", "pyproject.toml"]


def export(dest: Path) -> None:
    if (REPO / ".git").exists():
        archive = subprocess.run(["git", "-C", str(REPO), "archive", "HEAD", *EXPORT], check=True, capture_output=True)
        subprocess.run(["tar", "-x", "-C", str(dest)], input=archive.stdout, check=True)
        return
    skip = shutil.ignore_patterns("__pycache__", "*.pyc")
    for name in EXPORT:
        source = REPO / name
        if source.is_dir():
            shutil.copytree(source, dest / name, ignore=skip)
        else:
            shutil.copy2(source, dest / name)


def env_for(tree: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(tree / "src")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.pop("VIRTUAL_ENV", None)
    return env


def imports_tree(tree: Path) -> bool:
    out = subprocess.run(
        [PYTHON, "-c", "import stabilize; print(stabilize.__file__)"],
        cwd=tree,
        env=env_for(tree),
        capture_output=True,
        text=True,
    )
    return out.stdout.strip().startswith(str(tree / "src") + "/")


def run_test(tree: Path, test_id: str) -> tuple[int, str]:
    host.wait_host()
    proc = subprocess.run(
        [PYTHON, "-m", "pytest", test_id, "-q", "-p", "no:cacheprovider", "-p", "no:logging", "-rf"]
        + ["--timeout", "300"],
        cwd=tree,
        env=env_for(tree),
        capture_output=True,
        text=True,
        timeout=900,
    )
    return proc.returncode, proc.stdout + proc.stderr


def prove(diff: Path) -> tuple[bool, str]:
    lines = diff.read_text().splitlines()
    if not lines or not lines[0].startswith("test-id: "):
        return False, "diff has no test-id header"
    test_id = lines[0][len("test-id: ") :].strip()
    work = Path(tempfile.mkdtemp(prefix="manual-proof-"))
    try:
        clean = work / "clean"
        mutant = work / "mutant"
        for tree in (clean, mutant):
            tree.mkdir()
            export(tree)
            if not imports_tree(tree):
                return False, f"stabilize is not imported from {tree / 'src'}"
        rc, out = run_test(clean, test_id)
        if rc != 0:
            return False, f"not green on the unmutated tree (pytest exit {rc}): {out.strip().splitlines()[-1:]}"
        applied = subprocess.run(["git", "apply", "-p1", str(diff)], cwd=mutant, capture_output=True, text=True)
        if applied.returncode != 0:
            return False, f"patch failed: {applied.stdout.strip()} {applied.stderr.strip()}"
        rc, out = run_test(mutant, test_id)
        failed = [line for line in out.splitlines() if line.startswith(f"FAILED {test_id}")]
        if rc != 1 or not failed:
            return False, f"mutant did not fail in the call phase (pytest exit {rc}): {out.strip().splitlines()[-1:]}"
        return True, failed[0]
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main() -> int:
    diffs = sorted(MANUAL.glob("*.diff"))
    if not diffs:
        print("[FAIL] no manual proofs found; an empty check proves nothing")
        return 1
    bad = 0
    for diff in diffs:
        ok, detail = prove(diff)
        test_id = diff.read_text().splitlines()[0].removeprefix("test-id: ")
        print(f"[{'PASS' if ok else 'FAIL'}] {test_id} {detail}", flush=True)
        if not ok:
            bad += 1
    print(f"manual proofs: {len(diffs) - bad} of {len(diffs)} red on their mutant and green without it")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
