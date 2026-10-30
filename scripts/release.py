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

from release_tasks import (  # noqa: E402
    CancelStageHandler,
    GitTagTask,
    SkipTask,
    StreamingShellTask,
    VersionValidatorTask,
)
from release_workflow import create_release_workflow  # noqa: E402

from stabilize import (  # noqa: E402
    Orchestrator,
    QueueProcessor,
    SqliteQueue,
    SqliteWorkflowStore,
    TaskRegistry,
    WorkflowStatus,
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
