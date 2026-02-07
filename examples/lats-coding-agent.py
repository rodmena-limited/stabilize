#!/usr/bin/env python3
"""
LATS-based Coding Agent Example using Stabilize and Local Ollama.

This script implements a simplified Language Agent Tree Search (LATS) loop:
1. Generate: Create candidate solution (bulkhead.py)
2. Test: Verify solution with pytest
3. Evaluate: Check results
   - If success: Finish
   - If failure: Analyze error, update context, and loop back (Refinement)

Target: Implement a bulkhead pattern in Python.
Location: /tmp/stabilize-LATS-project/

Run with:
    python examples/lats-coding-agent.py
"""

import logging
import os
import shutil
import subprocess
from typing import Any

logging.basicConfig(level=logging.ERROR)

from stabilize import (
    HTTPTask,
    Orchestrator,
    QueueProcessor,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.events import SqliteEventStore, configure_event_sourcing

# =============================================================================
# Custom Tasks
# =============================================================================


class OllamaTask(Task):
    """Call Ollama API and optionally write to file."""

    DEFAULT_HOST = "http://localhost:11434"
    DEFAULT_MODEL = "qwen3-coder-next:cloud"

    def __init__(self) -> None:
        self._http_task = HTTPTask()

    def _make_http_request(self, stage: StageExecution, context: dict[str, Any]) -> TaskResult:
        temp_stage = StageExecution(
            ref_id=stage.ref_id,
            type="http",
            name=f"{stage.name} (HTTP)",
            context=context,
            tasks=[],
        )
        return self._http_task.execute(temp_stage)

    def _write_file(self, content: str, path: str) -> str:
        """Write content to file, cleaning markdown if present."""
        full_path = os.path.join("/tmp/stabilize-LATS-project", path)
        os.makedirs(
            (
                os.path.dirname(full_path)
                if os.path.dirname(full_path)
                else "/tmp/stabilize-LATS-project"
            ),
            exist_ok=True,
        )

        # Clean markdown code blocks if present
        if "```python" in content:
            content = content.split("```python")[1]
        if "```" in content:
            content = content.split("```")[0]

        with open(full_path, "w") as f:
            f.write(content.strip())

        return full_path

    def execute(self, stage: StageExecution) -> TaskResult:
        prompt = stage.context.get("prompt")
        system = stage.context.get("system")
        model = stage.context.get("model", self.DEFAULT_MODEL)
        output_path = stage.context.get("output_path")  # Optional file output

        # Check health
        health = self._make_http_request(
            stage,
            {
                "url": f"{self.DEFAULT_HOST}/api/tags",
                "method": "GET",
                "timeout": 2,
                "continue_on_failure": True,
            },
        )
        if health.status.is_halt:
            # Fallback for mocked environment or if Ollama is missing
            print("  [OllamaTask] Warning: Ollama not reachable. Using mock response.")
            mock_response = "Mock code for bulkhead pattern.\nclass Bulkhead: pass"
            outputs = {"response": mock_response, "model": "mock"}
            if output_path:
                outputs["path"] = self._write_file(mock_response, output_path)
            return TaskResult.success(outputs=outputs)

        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.2},
        }
        if system:
            payload["system"] = system

        result = self._make_http_request(
            stage,
            {
                "url": f"{self.DEFAULT_HOST}/api/generate",
                "method": "POST",
                "json": payload,
                "timeout": 120,
                "parse_json": True,
            },
        )

        if result.status.is_halt or result.status.is_failure:
            return TaskResult.terminal(error="Ollama API failed")

        body = result.outputs.get("body_json", {})
        response = body.get("response", "")

        outputs = {"response": response, "model": model}

        # Write to file if path specified
        if output_path:
            outputs["path"] = self._write_file(response, output_path)
            print(f"  [OllamaTask] Wrote {len(response)} chars to {outputs['path']}")

        return TaskResult.success(outputs=outputs)


class WriteFileTask(Task):
    """Write content to a file."""

    def execute(self, stage: StageExecution) -> TaskResult:
        path = stage.context.get("path", "bulkhead.py")  # Default path
        content = stage.context.get("content")

        # If content not in context, look for it in previous task outputs
        if not content:
            # Check previous tasks in this stage for 'response' output
            for task in stage.tasks:
                if task.outputs and "response" in task.outputs:
                    content = task.outputs["response"]
                    break

        if not content:
            return TaskResult.terminal("Missing content (not in context or previous task outputs)")

        full_path = os.path.join("/tmp/stabilize-LATS-project", path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        # Clean markdown code blocks if present
        if content.startswith("```python"):
            content = content.split("```python")[1]
        if content.endswith("```"):
            content = content.rsplit("```", 1)[0]

        with open(full_path, "w") as f:
            f.write(content)

        return TaskResult.success(outputs={"path": full_path})


class PytestTask(Task):
    """Run pytest."""

    def execute(self, stage: StageExecution) -> TaskResult:
        import sys

        test_file = stage.context.get("test_file", "tests/")
        cwd = "/tmp/stabilize-LATS-project"

        # Use the same Python environment's pytest
        pytest_path = os.path.join(os.path.dirname(sys.executable), "pytest")
        cmd = [pytest_path, test_file, "-v"]
        try:
            result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=30)
            print(f"  [PytestTask] Exit code: {result.returncode}")
            if result.stdout:
                # Show summary line
                for line in result.stdout.split("\n"):
                    if "passed" in line or "failed" in line or "error" in line:
                        print(f"  [PytestTask] {line.strip()}")
            return TaskResult.success(
                outputs={
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }
            )
        except subprocess.TimeoutExpired:
            return TaskResult.failed_continue("Test timeout")
        except Exception as e:
            return TaskResult.failed_continue(f"Test error: {e}")


class DecisionTask(Task):
    """Decide next step based on test results."""

    def execute(self, stage: StageExecution) -> TaskResult:
        # Find the 'test' stage results
        # We look for the stage by ref_id="test" in the execution
        test_stage = None
        for s in stage.execution.stages:
            if s.ref_id == "test":
                test_stage = s
                break

        if not test_stage:
            return TaskResult.terminal("Could not find 'test' stage")

        # Check stage outputs
        test_rc = test_stage.outputs.get("returncode", -1)
        test_stdout = test_stage.outputs.get("stdout", "No output")

        if test_rc == 0:
            print("\n>>> SUCCESS: Tests passed!")
            return TaskResult.success(outputs={"status": "solved"})

        print(f"\n>>> FAILURE: Tests failed (rc={test_rc})")

        # Check retries
        jump_count = stage.context.get("_jump_count", 0)
        if jump_count >= 3:
            print(">>> Max retries reached. Giving up.")
            return TaskResult.terminal("Max retries exceeded")

        # Construct new prompt
        old_prompt = stage.context.get("prompt_history", "")
        new_prompt = f"{old_prompt}\n\nThe previous attempt failed with:\n{test_stdout}\n\nPlease fix the code."

        print(f">>> Retrying (Attempt {jump_count + 1})...")
        return TaskResult.jump_to(
            target_stage_ref_id="generate",
            context={"prompt": new_prompt, "prompt_history": new_prompt},
        )


# =============================================================================
# Pipeline Setup
# =============================================================================


def setup_project():
    project_dir = "/tmp/stabilize-LATS-project"
    if os.path.exists(project_dir):
        shutil.rmtree(project_dir)
    os.makedirs(project_dir)

    # Create the test file (Golden Standard)
    test_code = """
import pytest
import time
import threading
from bulkhead import Bulkhead, BulkheadError

def test_bulkhead_concurrency():
    bh = Bulkhead(max_concurrent=2)

    active = 0
    lock = threading.Lock()

    def task():
        nonlocal active
        with bh:
            with lock:
                active += 1
                current = active
            assert current <= 2
            time.sleep(0.1)
            with lock:
                active -= 1

    threads = [threading.Thread(target=task) for _ in range(5)]
    for t in threads: t.start()
    for t in threads: t.join()

def test_bulkhead_limit():
    bh = Bulkhead(max_concurrent=1)

    def task():
        with bh:
            time.sleep(0.1)

    # Hold the slot
    t = threading.Thread(target=task)
    t.start()
    time.sleep(0.01)

    # Try to acquire
    try:
        with bh:
            assert False, "Should have raised BulkheadError"
    except BulkheadError:
        pass
    except Exception as e:
        assert False, f"Wrong exception: {e}"

    t.join()
"""
    with open(os.path.join(project_dir, "test_bulkhead.py"), "w") as f:
        f.write(test_code)


def main():
    setup_project()

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()

    registry = TaskRegistry()
    registry.register("ollama", OllamaTask)
    registry.register("pytest", PytestTask)
    registry.register("decision", DecisionTask)

    # Enable event sourcing — all handler events are recorded automatically
    event_store = SqliteEventStore("sqlite:///:memory:", create_tables=True)
    configure_event_sourcing(event_store)

    processor = QueueProcessor(queue, store=store, task_registry=registry)

    workflow = Workflow.create(
        application="lats-agent",
        name="Bulkhead Generator",
        stages=[
            # Stage 1: Generate Code
            StageExecution(
                ref_id="generate",
                type="gen",
                name="Generate Solution",
                context={
                    "system": "You are a python expert. Output ONLY valid Python code, no explanations. Implement a 'Bulkhead' class context manager. It should use threading.Semaphore with non-blocking acquire. Raise 'BulkheadError' (define it) if the semaphore cannot be acquired immediately.",
                    "prompt": "Implement a thread-safe Bulkhead pattern in Python. The class must: 1) Accept max_concurrent in __init__, 2) Use threading.Semaphore, 3) Implement __enter__ with non-blocking acquire that raises BulkheadError if full, 4) Implement __exit__ to release. Define BulkheadError as a custom exception.",
                    "prompt_history": "Implement Bulkhead pattern with threading.Semaphore.",
                    "model": "qwen3-coder-next:cloud",
                    "output_path": "bulkhead.py",
                },
                tasks=[
                    TaskExecution.create(
                        "Generate & Save", "ollama", stage_start=True, stage_end=True
                    ),
                ],
            ),
            # Stage 2: Run Tests
            StageExecution(
                ref_id="test",
                type="test",
                name="Run Tests",
                requisite_stage_ref_ids={"generate"},
                context={"test_file": "test_bulkhead.py"},
                tasks=[TaskExecution.create("Pytest", "pytest", stage_start=True, stage_end=True)],
            ),
            # Stage 3: Evaluate & Decide
            StageExecution(
                ref_id="evaluate",
                type="decision",
                name="Evaluate Results",
                requisite_stage_ref_ids={"test"},
                context={},
                tasks=[
                    TaskExecution.create("Decide", "decision", stage_start=True, stage_end=True)
                ],
            ),
        ],
    )

    # Data Flow: Pass outputs
    # Generate -> Save File (needs 'response' from LLM)
    # But tasks in same stage share context? No, they share STAGE context.
    # Stabilize doesn't auto-pipe task outputs to next task inputs within stage unless coded.
    # My WriteFileTask expects 'content' in context.
    # OllamaTask puts 'response' in outputs.
    # We need a way to move output to input.
    # In `stabilize`, stage context is immutable during execution? No.
    # But Task outputs don't auto-update stage context.

    # HACK: Custom Task that does both? Or use `JumpToStage` to pipe?
    # Or just subclass WriteFileTask to read from `execution.stages`?
    # Or simpler: Update `OllamaTask` to write to file? No, separation of concerns.

    # Correct way in Stabilize: Tasks return outputs.
    # Subsequent tasks can access previous task outputs via `stage.tasks`.

    # Let's modify WriteFileTask to look at previous task output if 'content' missing.

    store.store(workflow)
    orchestrator = Orchestrator(queue, store)
    orchestrator.start(workflow)

    print("Starting Agentic Workflow...")
    processor.process_all(timeout=300.0)

    final = store.retrieve(workflow.id)
    print(f"Workflow finished with status: {final.status}")


if __name__ == "__main__":
    main()
