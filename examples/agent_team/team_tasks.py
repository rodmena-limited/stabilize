"""
Custom tasks for the multi-agent software-team example.

These tasks drive a real LLM (ollama.com cloud glm-5.2 by default) through
stabilize to design, implement, test, and review a small Python library.
Each task is small and deterministic in shape so the workflow engine — not
the model — owns control flow, durability, and recovery.

Model access is via OLLAMA_API_KEY (never hard-coded). If the key is unset,
the tasks fall back to a small offline stub so the example still runs and
exercises the engine end-to-end.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from stabilize.llm import ChatMessage, LLMClient
from stabilize.models.stage import StageExecution
from stabilize.streaming import emit_progress
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult

MODEL = os.getenv("AGENT_TEAM_MODEL", "glm-5.2")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "https://ollama.com")
PROJECT_DIR = Path(os.getenv("AGENT_TEAM_DIR", "/tmp/stabilize-agent-team"))


def _client() -> LLMClient | None:
    """Build a cloud LLM client, or None when no key is configured (offline)."""
    api_key = os.getenv("OLLAMA_API_KEY")
    if not api_key:
        return None
    # ollama.com exposes the native Ollama chat API at /api/chat.
    return LLMClient(model=MODEL, base_url=OLLAMA_BASE_URL, api_key=api_key, api="ollama", timeout=180.0)


def _extract_code(text: str) -> str:
    """Pull the first fenced code block out of an LLM response (or return all)."""
    match = re.search(r"```(?:python)?\s*(.*?)```", text, re.S)
    return (match.group(1) if match else text).strip()


def _extract_json(text: str) -> Any:
    """Pull the first JSON object/array out of an LLM response."""
    match = re.search(r"(\{.*\}|\[.*\])", text, re.S)
    if not match:
        raise ValueError("no JSON found in model output")
    return json.loads(match.group(1))


class ArchitectTask(Task):
    """Design the library: produce a list of modules to implement.

    Outputs: ``{"modules": [{"name","spec"}...], "module_count": N,
    "module_names": [...]}`` — module_count feeds a MultiInstance fan-out.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        goal = stage.context["goal"]
        emit_progress(stage, "Architect: designing modules", agent="architect")

        client = _client()
        if client is None:
            modules = [
                {"name": "core", "spec": f"Implement the core logic for: {goal}"},
                {"name": "utils", "spec": "Helper utilities supporting the core module."},
            ]
        else:
            prompt = (
                f"You are a software architect. Design a small Python library for this goal:\n{goal}\n\n"
                "Return ONLY a JSON array of 2-3 modules, each an object with 'name' "
                "(a python-identifier module name) and 'spec' (a one-paragraph implementation brief). "
                "Keep it minimal and self-contained (standard library only)."
            )
            response = client.chat([ChatMessage(role="user", content=prompt)], temperature=0.2)
            try:
                modules = _extract_json(response.content)
                modules = [{"name": re.sub(r"\W+", "_", m["name"]), "spec": m["spec"]} for m in modules][:3]
            except Exception as e:
                emit_progress(stage, f"Architect parse fallback: {e}", agent="architect")
                modules = [{"name": "core", "spec": f"Implement: {goal}"}]

        emit_progress(stage, f"Architect: {len(modules)} modules planned", agent="architect")
        return TaskResult.success(
            outputs={
                "modules": modules,
                "module_count": len(modules),
                "module_names": [m["name"] for m in modules],
            }
        )


class CoderTask(Task):
    """Implement one module (selected by the multi-instance index).

    Reads ``modules`` and the instance index from context, writes the module
    file into the project dir, and returns its path + name.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        modules = stage.context["modules"]
        index = int(stage.context.get("_instance_index", stage.context.get("instance_index", 0)))
        index = max(0, min(index, len(modules) - 1))
        module = modules[index]
        name = module["name"]
        feedback = stage.context.get("review_feedback", "")

        emit_progress(stage, f"Coder: implementing {name}.py", agent=f"coder:{name}")
        client = _client()
        if client is None:
            code = f'"""Module {name} (offline stub)."""\n\n\ndef run():\n    return "{name}"\n'
        else:
            extra = f"\n\nA previous attempt was rejected in review. Address this feedback:\n{feedback}" if feedback else ""
            prompt = (
                f"Write a single self-contained Python module named {name}.py.\n"
                f"Spec: {module['spec']}{extra}\n\n"
                "Requirements: standard library only, include docstrings and type hints, "
                "no side effects on import. Return ONLY the code in a ```python code block."
            )
            response = client.chat([ChatMessage(role="user", content=prompt)], temperature=0.2)
            code = _extract_code(response.content)

        pkg_dir = PROJECT_DIR / "mylib"
        pkg_dir.mkdir(parents=True, exist_ok=True)
        (pkg_dir / "__init__.py").touch()
        module_path = pkg_dir / f"{name}.py"
        module_path.write_text(code)

        emit_progress(stage, f"Coder: wrote {module_path}", agent=f"coder:{name}")
        return TaskResult.success(
            outputs={"module_name": name, "module_path": str(module_path), "written": name}
        )


class TestWriterTask(Task):
    """Write and run a smoke test importing every generated module.

    Uses the reducer-gathered ``written`` list (all coder branches) to build
    an import test, runs pytest, and returns pass/fail.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        written = stage.context.get("written") or stage.context.get("module_names") or []
        if isinstance(written, str):
            written = [written]
        emit_progress(stage, f"Tester: smoke-testing modules {written}", agent="tester")

        tests_dir = PROJECT_DIR / "tests"
        tests_dir.mkdir(parents=True, exist_ok=True)
        (tests_dir / "__init__.py").touch()
        imports = "\n".join(f"import mylib.{m}" for m in written)
        test_code = (
            f'"""Auto-generated smoke test."""\n\n\n'
            f"def test_modules_import():\n"
            f"    {imports.replace(chr(10), chr(10) + '    ') if imports else 'pass'}\n"
            f"    assert True\n"
        )
        (tests_dir / "test_smoke.py").write_text(test_code)

        proc = subprocess.run(
            [sys.executable, "-m", "pytest", str(tests_dir), "-q"],
            cwd=str(PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=120,
        )
        passed = proc.returncode == 0
        emit_progress(stage, f"Tester: {'PASS' if passed else 'FAIL'}", agent="tester", passed=passed)
        outputs = {"tests_passed": passed, "test_output": (proc.stdout + proc.stderr)[-2000:]}
        if passed:
            return TaskResult.success(outputs=outputs)
        # Failing tests are recoverable — the reviewer loop can send it back.
        return TaskResult.failed_continue(error="smoke tests failed", outputs=outputs)


class ReviewerTask(Task):
    """Review the build; loop back to the coders on failure (bounded).

    On success: succeed. On failure and under the retry budget: jump back to
    the first coder stage with feedback (exercising jump_to retry loops). On
    exhausted budget: fail terminally.
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        tests_passed = stage.context.get("tests_passed", False)
        attempt = int(stage.context.get("_review_attempt", 0))
        max_attempts = int(stage.context.get("max_review_attempts", 2))

        emit_progress(stage, f"Reviewer: attempt {attempt + 1}, tests_passed={tests_passed}", agent="reviewer")

        if tests_passed:
            return TaskResult.success(outputs={"review": "approved", "attempts": attempt + 1})

        if attempt >= max_attempts:
            return TaskResult.terminal(
                error=f"Reviewer rejected after {attempt + 1} attempts",
                context={"review": "rejected", "attempts": attempt + 1},
            )

        feedback = "The smoke test failed. Fix imports/syntax so `import mylib.<module>` works."
        client = _client()
        if client is not None:
            prompt = (
                "A generated Python module failed a smoke import test with this output:\n"
                f"{stage.context.get('test_output', '')[-1000:]}\n\n"
                "Give one short, concrete instruction to fix it."
            )
            try:
                feedback = client.chat([ChatMessage(role="user", content=prompt)], temperature=0.2).content
            except Exception:
                pass

        target = stage.context.get("coder_stage_ref", "code_0")
        emit_progress(stage, f"Reviewer: sending back to {target}", agent="reviewer")
        return TaskResult.jump_to(
            target,
            context={
                "review_feedback": feedback,
                "_review_attempt": attempt + 1,
                "max_review_attempts": max_attempts,
            },
        )


class PackagerTask(Task):
    """Finalize: write a README describing the shipped library."""

    def execute(self, stage: StageExecution) -> TaskResult:
        module_names = stage.context.get("module_names", [])
        goal = stage.context.get("goal", "")
        emit_progress(stage, "Packager: writing README", agent="packager")
        readme = (
            f"# mylib\n\nGoal: {goal}\n\nModules: {', '.join(module_names)}\n\n"
            "Generated by the stabilize multi-agent software-team example.\n"
        )
        (PROJECT_DIR / "README.md").write_text(readme)
        return TaskResult.success(outputs={"packaged": True, "readme": str(PROJECT_DIR / "README.md")})
