#!/usr/bin/env python3
"""
PyPI Release Pipeline - Automates the release process for stabilize.

This script uses stabilize itself to orchestrate the release pipeline:
1. Validate version sync (pyproject.toml ↔ __init__.py)
2. Run tests (100% must pass)
3. Clean dist/ directory
4. Build wheel and sdist
5. Validate packages with twine check
6. Upload to PyPI or TestPyPI
7. Create and push git tag

Usage:
    # Dry run - test, build, validate but don't upload
    .venv/bin/python scripts/release.py --dry-run

    # Upload to TestPyPI first
    .venv/bin/python scripts/release.py --test-pypi

    # Full release to PyPI
    .venv/bin/python scripts/release.py

    # Skip tests (for re-uploading after failed upload)
    .venv/bin/python scripts/release.py --skip-tests

Requirements:
    - twine: pip install twine
    - build: pip install build
    - TWINE_USERNAME and TWINE_PASSWORD env vars (or use __token__ for API tokens)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

# Ensure we're running from the project's virtualenv
_PROJECT_ROOT = Path(__file__).parent.parent
_VENV_PATH = _PROJECT_ROOT / ".venv"
if _VENV_PATH.exists():
    _expected_prefix = str(_VENV_PATH.resolve())
    _current_prefix = getattr(sys, "prefix", "")
    if not _current_prefix.startswith(_expected_prefix):
        print("Error: This script must be run using the project's virtualenv.")
        print(f"  Expected: {_VENV_PATH}/bin/python")
        print(f"  Current:  {sys.executable}")
        print("\nRun with: .venv/bin/python scripts/release.py")
        sys.exit(1)

# Add src to path for development
sys.path.insert(0, str(_PROJECT_ROOT / "src"))

from stabilize import (  # noqa: E402
    Orchestrator,
    QueueProcessor,
    ShellTask,
    SqliteQueue,
    SqliteWorkflowStore,
    StabilizeHandler,
    StageExecution,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
    WorkflowStatus,
)
from stabilize.queue.messages import CancelStage  # noqa: E402

# =============================================================================
# Project paths
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.parent
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
INIT_PATH = PROJECT_ROOT / "src" / "stabilize" / "__init__.py"


# =============================================================================
# Custom Tasks
# =============================================================================


class VersionValidatorTask(Task):
    """
    Validate that pyproject.toml and __init__.py have matching versions.

    This task reads both files and compares the version strings.
    Fails terminally if versions don't match.

    Outputs:
        version: The validated version string
        pyproject_version: Version from pyproject.toml
        init_version: Version from __init__.py
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        pyproject_path = stage.context.get("pyproject_path", str(PYPROJECT_PATH))
        init_path = stage.context.get("init_path", str(INIT_PATH))

        # Read pyproject.toml version
        try:
            pyproject_content = Path(pyproject_path).read_text()
            pyproject_match = re.search(r'^version\s*=\s*"([^"]+)"', pyproject_content, re.MULTILINE)
            if not pyproject_match:
                return TaskResult.terminal(error=f"Could not find version in {pyproject_path}")
            pyproject_version = pyproject_match.group(1)
        except FileNotFoundError:
            return TaskResult.terminal(error=f"pyproject.toml not found at {pyproject_path}")
        except Exception as e:
            return TaskResult.terminal(error=f"Error reading pyproject.toml: {e}")

        # Read __init__.py version
        try:
            init_content = Path(init_path).read_text()
            init_match = re.search(r'^__version__\s*=\s*"([^"]+)"', init_content, re.MULTILINE)
            if not init_match:
                return TaskResult.terminal(error=f"Could not find __version__ in {init_path}")
            init_version = init_match.group(1)
        except FileNotFoundError:
            return TaskResult.terminal(error=f"__init__.py not found at {init_path}")
        except Exception as e:
            return TaskResult.terminal(error=f"Error reading __init__.py: {e}")

        # Compare versions
        if pyproject_version != init_version:
            return TaskResult.terminal(
                error=(
                    f"Version mismatch!\n"
                    f"  pyproject.toml: {pyproject_version}\n"
                    f"  __init__.py:    {init_version}\n"
                    f"Please sync versions before releasing."
                )
            )

        print(f"  [VersionValidator] Version validated: {pyproject_version}")
        return TaskResult.success(
            outputs={
                "version": pyproject_version,
                "pyproject_version": pyproject_version,
                "init_version": init_version,
            }
        )


class GitTagTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        import subprocess as sp

        version = stage.context.get("version")
        if not version:
            return TaskResult.terminal(error="No 'version' found in context (from upstream outputs)")

        cwd = stage.context.get("cwd", ".")
        tag = f"v{version}"
        cmd = f'git tag -a "{tag}" -m "Release {tag}" && git push origin "{tag}"'

        print(f"  [Create Git Tag] Running: {cmd}")
        sys.stdout.flush()

        try:
            result = sp.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                print(f"  [Create Git Tag] Tagged {tag}")
                return TaskResult.success(outputs={"tag": tag})

            error = result.stderr.strip() or result.stdout.strip()
            if stage.context.get("continue_on_failure"):
                return TaskResult.failed_continue(error=f"Git tag failed: {error}", outputs={"tag": tag})
            return TaskResult.terminal(error=f"Git tag failed: {error}")
        except sp.TimeoutExpired:
            return TaskResult.terminal(error="Git tag timed out after 60s")
        except Exception as e:
            return TaskResult.terminal(error=f"Git tag error: {e}")


class SkipTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        reason = stage.context.get("reason", "Skipped by user request")
        print(f"  [Skip] {reason}")
        return TaskResult.success(outputs={"skipped": True, "reason": reason})


class StreamingShellTask(ShellTask):
    """ShellTask that streams stdout in real-time for long-running commands."""

    def execute(self, stage: StageExecution) -> TaskResult:
        command = stage.context.get("command", "")
        stage_name = stage.name

        print(f"  [{stage_name}] Running: {command[:80]}{'...' if len(command) > 80 else ''}")
        sys.stdout.flush()

        result = self._execute_streaming(stage)

        failed_statuses = {WorkflowStatus.TERMINAL, WorkflowStatus.FAILED_CONTINUE}
        if result.status in failed_statuses:
            print(f"  [{stage_name}] FAILED")
            if result.context and "error" in result.context:
                print(f"  [{stage_name}] Error: {str(result.context['error'])[:200]}")
        else:
            print(f"  [{stage_name}] Done")

        sys.stdout.flush()
        return result

    def _execute_streaming(self, stage: StageExecution) -> TaskResult:
        import subprocess as sp

        command = stage.context.get("command", "")
        timeout: int = stage.context.get("timeout", 60)
        cwd: str | None = stage.context.get("cwd")
        env_extra: dict[str, str] = stage.context.get("env", {})
        continue_on_failure: bool = stage.context.get("continue_on_failure", False)

        full_env = os.environ.copy()
        for k, v in env_extra.items():
            full_env[k] = str(v) if not isinstance(v, str) else v

        proc = sp.Popen(
            command,
            shell=True,
            stdout=sp.PIPE,
            stderr=sp.STDOUT,
            cwd=cwd,
            env=full_env,
        )

        output_lines: list[str] = []
        try:
            assert proc.stdout is not None
            import time

            start = time.monotonic()
            for raw_line in proc.stdout:
                if time.monotonic() - start > timeout:
                    self._kill_process_tree(proc)
                    return TaskResult.terminal(
                        error=f"Command timed out after {timeout}s",
                        context={"stdout": "\n".join(output_lines)},
                    )
                line = raw_line.decode("utf-8", errors="replace").rstrip()
                output_lines.append(line)
                print(f"       {line}")
                sys.stdout.flush()
            proc.wait()
        except Exception as e:
            self._kill_process_tree(proc)
            return TaskResult.terminal(error=f"Streaming error: {e}")

        stdout_text = "\n".join(output_lines)
        outputs = {"stdout": stdout_text, "stderr": "", "returncode": proc.returncode, "truncated": False}

        if proc.returncode == 0:
            return TaskResult.success(outputs=outputs)

        error_msg = f"Command exited with code {proc.returncode}"
        if continue_on_failure:
            return TaskResult.failed_continue(error=error_msg, outputs=outputs)
        return TaskResult.terminal(error=error_msg, context=outputs)


class CancelStageHandler(StabilizeHandler):
    """Handler for CancelStage messages - marks stages as canceled."""

    @property
    def message_type(self) -> type[CancelStage]:
        return CancelStage  # type: ignore[no-any-return]

    def handle(self, message: CancelStage) -> None:
        """Mark the stage as canceled."""

        def on_stage(stage: StageExecution) -> None:
            stage.status = WorkflowStatus.CANCELED
            self.repository.store_stage(stage)

        self.with_stage(message, on_stage)


# =============================================================================
# Pipeline Builder
# =============================================================================


def create_release_workflow(
    dry_run: bool = False,
    test_pypi: bool = False,
    skip_tests: bool = False,
    verbose: bool = False,
) -> Workflow:
    """
    Create the release workflow with all stages.

    Args:
        dry_run: If True, skip upload and git tag stages
        test_pypi: If True, upload to TestPyPI instead of PyPI
        skip_tests: If True, skip the test stage
        verbose: If True, show verbose output

    Returns:
        Configured Workflow ready to execute
    """
    project_root = str(PROJECT_ROOT)
    pytest_args = "-v" if verbose else "-v --tb=short"

    stages: list[StageExecution] = []

    # Stage 1: Validate version
    stages.append(
        StageExecution(
            ref_id="validate_version",
            type="version_validator",
            name="Validate Version",
            context={
                "pyproject_path": str(PYPROJECT_PATH),
                "init_path": str(INIT_PATH),
            },
            tasks=[
                TaskExecution.create(
                    name="Check Version Sync",
                    implementing_class="version_validator",
                    stage_start=True,
                    stage_end=True,
                ),
            ],
        )
    )

    # Stage 2: Run tests (or skip)
    if skip_tests:
        stages.append(
            StageExecution(
                ref_id="test",
                type="skip",
                name="Run Tests (SKIPPED)",
                requisite_stage_ref_ids={"validate_version"},
                context={"reason": "Tests skipped via --skip-tests flag"},
                tasks=[
                    TaskExecution.create(
                        name="Skip Tests",
                        implementing_class="skip",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            )
        )
    else:
        test_paths = " ".join(
            [
                "golden_standard_tests/",
                "tests/test_workflow_control_flow_patterns.py",
                "tests/test_diamond_pattern.py",
                "tests/test_status_edge_cases.py",
                "tests/test_jump_edge_cases.py",
                "tests/test_synthetic_stage_edge_cases.py",
                "tests/test_failure_scenarios.py",
                "tests/test_dlq_atomicity.py",
                "tests/test_concurrency.py",
                "tests/test_stage_data_flow.py",
                "tests/test_dynamic_routing.py",
                "tests/test_critical_corner_cases.py",
                "tests/test_mission_critical_certification.py",
            ]
        )
        # Only run SQLite backend to avoid Docker dependency and cut runtime from ~9min to ~2min
        test_cmd = f".venv/bin/python -m pytest {test_paths} {pytest_args} -k sqlite -p no:cacheprovider --no-header"
        stages.append(
            StageExecution(
                ref_id="test",
                type="shell",
                name="Run Tests",
                requisite_stage_ref_ids={"validate_version"},
                context={
                    "command": test_cmd,
                    "cwd": project_root,
                    "timeout": 600,  # 10 minutes (golden+patterns+edges on sqlite+postgres)
                    "env": {
                        "PYTHONUNBUFFERED": "1",
                        "CI": "true",
                    },
                },
                tasks=[
                    TaskExecution.create(
                        name="pytest",
                        implementing_class="shell",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            )
        )

    # Stage 3: Clean dist/
    stages.append(
        StageExecution(
            ref_id="clean",
            type="shell",
            name="Clean Dist",
            requisite_stage_ref_ids={"test"},
            context={
                "command": "rm -rf dist/",
                "cwd": project_root,
                "timeout": 30,
            },
            tasks=[
                TaskExecution.create(
                    name="rm dist/",
                    implementing_class="shell",
                    stage_start=True,
                    stage_end=True,
                ),
            ],
        )
    )

    # Stage 4: Build
    stages.append(
        StageExecution(
            ref_id="build",
            type="shell",
            name="Build Package",
            requisite_stage_ref_ids={"clean"},
            context={
                "command": ".venv/bin/python -m build",
                "cwd": project_root,
                "timeout": 120,  # 2 minutes
            },
            tasks=[
                TaskExecution.create(
                    name="build package",
                    implementing_class="shell",
                    stage_start=True,
                    stage_end=True,
                ),
            ],
        )
    )

    # Stage 5: Dry run (twine check)
    stages.append(
        StageExecution(
            ref_id="dry_run",
            type="shell",
            name="Validate Package",
            requisite_stage_ref_ids={"build"},
            context={
                "command": ".venv/bin/twine check dist/*",
                "cwd": project_root,
                "timeout": 60,
            },
            tasks=[
                TaskExecution.create(
                    name="validate package",
                    implementing_class="shell",
                    stage_start=True,
                    stage_end=True,
                ),
            ],
        )
    )

    # Stage 6: Upload (or skip if dry_run)
    if dry_run:
        stages.append(
            StageExecution(
                ref_id="upload",
                type="skip",
                name="Upload to PyPI (SKIPPED)",
                requisite_stage_ref_ids={"dry_run"},
                context={"reason": "Upload skipped - dry run mode"},
                tasks=[
                    TaskExecution.create(
                        name="Skip Upload",
                        implementing_class="skip",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            )
        )
    else:
        upload_cmd = ".venv/bin/twine upload"
        if test_pypi:
            upload_cmd += " --repository testpypi"
        upload_cmd += " dist/*"

        stages.append(
            StageExecution(
                ref_id="upload",
                type="shell",
                name=f"Upload to {'TestPyPI' if test_pypi else 'PyPI'}",
                requisite_stage_ref_ids={"dry_run"},
                context={
                    "command": upload_cmd,
                    "cwd": project_root,
                    "timeout": 300,  # 5 minutes
                },
                tasks=[
                    TaskExecution.create(
                        name="upload package",
                        implementing_class="shell",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            )
        )

    # Stage 7: Git tag (or skip if dry_run)
    if dry_run:
        stages.append(
            StageExecution(
                ref_id="git_tag",
                type="skip",
                name="Create Git Tag (SKIPPED)",
                requisite_stage_ref_ids={"upload"},
                context={"reason": "Git tag skipped - dry run mode"},
                tasks=[
                    TaskExecution.create(
                        name="Skip Git Tag",
                        implementing_class="skip",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            )
        )
    else:
        stages.append(
            StageExecution(
                ref_id="git_tag",
                type="git_tag",
                name="Create Git Tag",
                requisite_stage_ref_ids={"upload"},
                context={
                    "cwd": project_root,
                    "continue_on_failure": True,
                },
                tasks=[
                    TaskExecution.create(
                        name="git tag",
                        implementing_class="git_tag",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            )
        )

    return Workflow.create(
        application="stabilize-release",
        name=f"Release Pipeline ({'dry-run' if dry_run else 'testpypi' if test_pypi else 'pypi'})",
        stages=stages,
    )


# =============================================================================
# Runner
# =============================================================================


def run_release(
    dry_run: bool = False,
    test_pypi: bool = False,
    skip_tests: bool = False,
    verbose: bool = False,
) -> int:
    """
    Execute the release pipeline.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Create in-memory persistence
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()

    # Register tasks
    registry = TaskRegistry()
    registry.register("shell", StreamingShellTask)
    registry.register("version_validator", VersionValidatorTask)
    registry.register("skip", SkipTask)
    registry.register("git_tag", GitTagTask)

    # Create processor (auto-registers all default handlers)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    # Replace the default CancelStageHandler with our custom one
    processor.replace_handler(CancelStageHandler(queue, store))

    orchestrator = Orchestrator(queue)

    # Create and run workflow
    workflow = create_release_workflow(
        dry_run=dry_run,
        test_pypi=test_pypi,
        skip_tests=skip_tests,
        verbose=verbose,
    )

    print("=" * 60)
    print(f"Release Pipeline: {workflow.name}")
    print("=" * 60)
    print()
    print("Starting pipeline... (tests may take 1-2 minutes)")
    print()

    store.store(workflow)
    orchestrator.start(workflow)

    # Process with generous timeout
    timeout = 600.0 if not skip_tests else 300.0  # 10 min with tests, 5 min without
    processor.process_all(timeout=timeout)

    # Get result
    result = store.retrieve(workflow.id)

    print()
    print("=" * 60)
    print("Pipeline Results")
    print("=" * 60)

    # Print stage results
    for stage in result.stages:
        status_icon = {
            WorkflowStatus.SUCCEEDED: "[OK]",
            WorkflowStatus.TERMINAL: "[FAIL]",
            WorkflowStatus.FAILED_CONTINUE: "[WARN]",
            WorkflowStatus.RUNNING: "[...]",
            WorkflowStatus.CANCELED: "[SKIP]",
            WorkflowStatus.SKIPPED: "[SKIP]",
            WorkflowStatus.NOT_STARTED: "[--]",
        }.get(stage.status, "[??]")

        print(f"  {status_icon} {stage.name}")

        # Show error if failed
        if stage.status in (WorkflowStatus.TERMINAL, WorkflowStatus.FAILED_CONTINUE):
            # Error is stored in stage.context["error"] by TaskResult.terminal()
            error_msg = stage.context.get("error")
            if error_msg:
                # Handle multi-line errors
                for line in str(error_msg).split("\n"):
                    print(f"       {line}")

        # Show version if validated
        if stage.ref_id == "validate_version" and stage.status == WorkflowStatus.SUCCEEDED:
            version = stage.outputs.get("version", "unknown")
            print(f"       Version: {version}")

    print()

    if result.status == WorkflowStatus.SUCCEEDED:
        version = result.stages[0].outputs.get("version", "unknown")
        if dry_run:
            print(f"Dry run completed successfully for version {version}")
            print("Run without --dry-run to upload to PyPI")
        else:
            target = "TestPyPI" if test_pypi else "PyPI"
            print(f"Successfully released version {version} to {target}!")
        return 0
    else:
        print("Release pipeline FAILED")
        return 1


# =============================================================================
# CLI
# =============================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Release stabilize to PyPI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Dry run - test, build, validate without uploading
    .venv/bin/python scripts/release.py --dry-run

    # Upload to TestPyPI first
    .venv/bin/python scripts/release.py --test-pypi

    # Full release to PyPI
    .venv/bin/python scripts/release.py

    # Skip tests (for re-uploading)
    .venv/bin/python scripts/release.py --skip-tests

Environment Variables:
    TWINE_USERNAME  - PyPI username (or __token__ for API tokens)
    TWINE_PASSWORD  - PyPI password or API token
        """,
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run tests, build, validate but skip upload and git tag",
    )

    parser.add_argument(
        "--test-pypi",
        action="store_true",
        help="Upload to TestPyPI instead of PyPI",
    )

    parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="Skip running tests (use for re-uploading)",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show verbose output",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.test_pypi and args.dry_run:
        parser.error("Cannot use --test-pypi with --dry-run")

    exit_code = run_release(
        dry_run=args.dry_run,
        test_pypi=args.test_pypi,
        skip_tests=args.skip_tests,
        verbose=args.verbose,
    )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
