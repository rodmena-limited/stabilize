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
    python scripts/release.py --dry-run

    # Upload to TestPyPI first
    python scripts/release.py --test-pypi

    # Full release to PyPI
    python scripts/release.py

    # Skip tests (for re-uploading after failed upload)
    python scripts/release.py --skip-tests

Requirements:
    - twine: pip install twine
    - build: pip install build
    - TWINE_USERNAME and TWINE_PASSWORD env vars (or use __token__ for API tokens)
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    Orchestrator,
    QueueProcessor,
    RunTaskHandler,
    ShellTask,
    SqliteQueue,
    SqliteWorkflowStore,
    StabilizeHandler,
    StageExecution,
    StartStageHandler,
    StartTaskHandler,
    StartWorkflowHandler,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
    WorkflowStatus,
)
from stabilize.queue.messages import CancelStage

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


class SkipTask(Task):
    """A task that immediately succeeds - used for skipped stages."""

    def execute(self, stage: StageExecution) -> TaskResult:
        reason = stage.context.get("reason", "Skipped by user request")
        print(f"  [Skip] {reason}")
        return TaskResult.success(outputs={"skipped": True, "reason": reason})


class VerboseShellTask(ShellTask):
    """ShellTask that prints progress before and after execution."""

    def execute(self, stage: StageExecution) -> TaskResult:
        command = stage.context.get("command", "")
        stage_name = stage.name

        # Print what we're about to do
        print(f"  [{stage_name}] Running: {command[:60]}{'...' if len(command) > 60 else ''}")
        sys.stdout.flush()

        # Run the actual command
        result = super().execute(stage)

        # Print result based on status
        failed_statuses = {WorkflowStatus.TERMINAL, WorkflowStatus.FAILED_CONTINUE}
        if result.status in failed_statuses:
            print(f"  [{stage_name}] FAILED")
            if result.context and "error" in result.context:
                error_preview = str(result.context["error"])[:200]
                print(f"  [{stage_name}] Error: {error_preview}")
        else:
            print(f"  [{stage_name}] Done")

        sys.stdout.flush()
        return result


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
        # Use -p no:cacheprovider to avoid cache-related hangs
        # Use --no-header to reduce terminal output
        test_cmd = f"python -m pytest tests/ {pytest_args} -p no:cacheprovider --no-header"
        stages.append(
            StageExecution(
                ref_id="test",
                type="shell",
                name="Run Tests",
                requisite_stage_ref_ids={"validate_version"},
                context={
                    "command": test_cmd,
                    "cwd": project_root,
                    "timeout": 300,  # 5 minutes
                    "env": {
                        "PYTHONUNBUFFERED": "1",  # Disable output buffering
                        "CI": "true",  # Signal we're in CI mode
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
                "command": "python -m build",
                "cwd": project_root,
                "timeout": 120,  # 2 minutes
            },
            tasks=[
                TaskExecution.create(
                    name="python -m build",
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
                "command": "twine check dist/*",
                "cwd": project_root,
                "timeout": 60,
            },
            tasks=[
                TaskExecution.create(
                    name="twine check",
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
        upload_cmd = "twine upload"
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
                        name="twine upload",
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
        # Use placeholder for version - will be substituted from validate_version outputs
        stages.append(
            StageExecution(
                ref_id="git_tag",
                type="shell",
                name="Create Git Tag",
                requisite_stage_ref_ids={"upload"},
                context={
                    "command": 'git tag -a "v{version}" -m "Release v{version}" && git push origin "v{version}"',
                    "cwd": project_root,
                    "timeout": 60,
                    "continue_on_failure": True,  # Warning only - upload already succeeded
                },
                tasks=[
                    TaskExecution.create(
                        name="git tag",
                        implementing_class="shell",
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
    registry.register("shell", VerboseShellTask)  # Use verbose version for progress output
    registry.register("version_validator", VersionValidatorTask)
    registry.register("skip", SkipTask)

    # Create processor with handlers
    processor = QueueProcessor(queue)
    handlers: list[Any] = [
        StartWorkflowHandler(queue, store),
        StartStageHandler(queue, store),
        StartTaskHandler(queue, store),
        RunTaskHandler(queue, store, registry),
        CompleteTaskHandler(queue, store),
        CompleteStageHandler(queue, store),
        CompleteWorkflowHandler(queue, store),
        CancelStageHandler(queue, store),  # Handle stage cancellation
    ]
    for handler in handlers:
        processor.register_handler(handler)

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
    python scripts/release.py --dry-run

    # Upload to TestPyPI first
    python scripts/release.py --test-pypi

    # Full release to PyPI
    python scripts/release.py

    # Skip tests (for re-uploading)
    python scripts/release.py --skip-tests

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
