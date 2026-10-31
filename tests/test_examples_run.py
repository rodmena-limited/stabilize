"""Every example that needs no external service runs to completion on SQLite; the rest are named with their reason."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"

HEADLESS = [
    "shell-example.py",
    "python-example.py",
    "dynamic-routing-example.py",
    "event-sourcing-example.py",
    "golden-standard.py",
]

NEEDS = {
    "http-example.py": "a public HTTP endpoint",
    "docker-example.py": "the docker daemon",
    "llama-example.py": "an Ollama server",
    "lats-coding-agent.py": "an LLM endpoint",
    "highway-integration-example.py": "the Highway service",
    "ssh-example.py": "an SSH server",
    "benchmark-overhead.py": "no entry point; a module of timing helpers",
}


def run_example(script: Path, cwd: Path, timeout: float = 180.0) -> subprocess.CompletedProcess[str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("STABILIZE_")}
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [sys.executable, str(script)],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_every_example_is_accounted_for() -> None:
    on_disk = sorted(
        p.name for p in EXAMPLES.glob("*.py") if p.name != "__init__.py" and not p.name.endswith("_pipelines.py")
    )
    helpers = {"llama_example_support.py", "python_servers.py"}
    unaccounted = [n for n in on_disk if n not in HEADLESS and n not in NEEDS and n not in helpers]
    assert unaccounted == [], f"examples neither run nor named with a reason: {unaccounted}"
    for name in HEADLESS + list(NEEDS):
        assert (EXAMPLES / name).exists(), f"{name} is listed but not on disk"


def test_the_runner_sees_a_failing_script(tmp_path: Path) -> None:
    failing = tmp_path / "failing.py"
    failing.write_text("import sys\nprint('boom')\nsys.exit(3)\n")
    result = run_example(failing, tmp_path)
    assert result.returncode == 3 and "boom" in result.stdout


@pytest.mark.parametrize("name", HEADLESS)
def test_headless_example_runs(name: str, tmp_path: Path) -> None:
    result = run_example(EXAMPLES / name, tmp_path)
    assert result.returncode == 0, (
        f"{name} exited {result.returncode}\n--- stdout ---\n{result.stdout[-3000:]}\n"
        f"--- stderr ---\n{result.stderr[-3000:]}"
    )
    assert "Traceback" not in result.stderr, f"{name} printed a traceback:\n{result.stderr[-2500:]}"
    if name == "golden-standard.py":
        assert "[PASS] Output matches expected result!" in result.stdout, result.stdout[-2000:]
