"""The release pipeline's workflow, built for scripts/release.py."""

from __future__ import annotations

import sys

from release_tasks import INIT_PATH, PROJECT_ROOT, PYPROJECT_PATH

from stabilize import StageExecution, TaskExecution, Workflow

# =============================================================================
# Pipeline Builder
# =============================================================================


def _gate_already_green() -> dict | None:
    """The gate stamp, if it was recorded against this exact source tree.

    Running the gate and then re-running a subset of it inside the release
    costs minutes and proves nothing new. Skipping on an assertion
    (--skip-tests) cannot tell a verified tree from an untouched one. The stamp
    is the middle: a skip backed by evidence that these bytes passed.
    """
    try:
        sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
        from gate_stamp import read_valid

        return read_valid()
    except Exception:
        return None


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
    stamp = None if skip_tests else _gate_already_green()
    if stamp is not None:
        skip_tests = True
        print(
            f"  Gate stamp matches this tree (recorded {stamp['recorded_at']}, "
            f"HEAD {stamp['head']}): {stamp['results']}"
        )
        print("  Tests skipped because they are provably redundant, not by assertion.")

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
                "tests/test_workflow_control_flow_patterns*.py",
                "tests/test_diamond_pattern*.py",
                "tests/test_status_edge_cases*.py",
                "tests/test_jump_edge_cases*.py",
                "tests/test_synthetic_stage_edge_cases.py",
                "tests/test_failure_scenarios*.py",
                "tests/test_dlq_atomicity.py",
                "tests/test_concurrency.py",
                "tests/stage_data_flow/",
                "tests/test_dynamic_routing.py",
                "tests/test_critical_corner_cases*.py",
                "tests/test_mission_critical_certification*.py",
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
