"""
Diamond Pattern Tests for Airport-Critical Systems.

This module tests the diamond/join pattern which is common in workflow systems:

        A (root)
       / \\
      B   C  (parallel branches)
       \\ /
        D (join point)

Critical scenarios tested:
1. Basic diamond execution - D must wait for both B and C
2. Concurrent completion race - B and C completing simultaneously
3. Output merging at join point - D must see outputs from both branches
4. Non-deterministic merge order - same key in B and C should be handled predictably
5. Deep diamond (A→B,C→D,E→F) - multiple join points
6. Wide diamond (A→B,C,D,E→F) - many parallel branches
7. Diamond with failures - one branch fails, should propagate correctly
8. Diamond with jumps - dynamic routing in diamond patterns
9. Diamond under high concurrency - many workflows with diamonds running together
10. Diamond with slow branches - timing-related race conditions

These tests run on both SQLite and PostgreSQL backends.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest

from stabilize import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize
from tests.diamond_support import (
    ConditionalFailTask,
    CountingOutputTask,
    OutputTask,
    RandomDelayTask,
    SlowOutputTask,
    VerifyInputsTask,
    create_diamond_workflow,
)

# =============================================================================
# Test Classes
# =============================================================================


class TestBasicDiamond:
    """Test basic diamond pattern execution."""

    def test_diamond_completes_successfully(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """Basic diamond workflow should complete with all stages succeeded."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = create_diamond_workflow("basic-diamond")
        file_repository.store(wf)
        runner.start(wf)

        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Workflow status: {result.status}"

        # All stages should be SUCCEEDED
        for stage in result.stages:
            assert stage.status == WorkflowStatus.SUCCEEDED, f"Stage {stage.ref_id}: {stage.status}"

        # Stage D should have received all expected keys
        assert not VerifyInputsTask.verification_errors, f"Errors: {VerifyInputsTask.verification_errors}"

    def test_join_waits_for_all_branches(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """Join stage D must wait for both B and C before starting."""
        CountingOutputTask.reset()
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "slow_output": SlowOutputTask,
                "counting": CountingOutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        # Make B slow, C fast
        wf = Workflow.create(
            application="diamond-test",
            name="slow-branch-diamond",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    context={"outputs_to_produce": {"from_a": True}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B (Slow)",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_b": True}, "delay": 0.3},
                    tasks=[TaskExecution.create("Task B", "slow_output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C (Fast)",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_c": True}},
                    tasks=[TaskExecution.create("Task C", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_d",
                    name="Stage D (Join)",
                    requisite_stage_ref_ids={"stage_b", "stage_c"},
                    context={"expected_keys": ["from_a", "from_b", "from_c"]},
                    tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # D should have all inputs despite B being slow
        assert not VerifyInputsTask.verification_errors


class TestOutputMerging:
    """Test output merging at diamond join points."""

    def test_outputs_from_both_branches_merged(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """Stage D should receive merged outputs from both B and C."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = create_diamond_workflow(
            "merge-test",
            stage_a_outputs={"shared_key": "from_a", "a_only": 1},
            stage_b_outputs={"b_only": 2, "b_list": [1, 2]},
            stage_c_outputs={"c_only": 3, "c_list": [3, 4]},
            stage_d_expected_keys=["shared_key", "a_only", "b_only", "c_only", "b_list", "c_list"],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED
        assert not VerifyInputsTask.verification_errors

        # Verify the actual merged context
        stage_d_key = f"{wf.id}:stage_d"
        assert stage_d_key in VerifyInputsTask.verified_contexts
        ctx = VerifyInputsTask.verified_contexts[stage_d_key]

        assert ctx.get("a_only") == 1
        assert ctx.get("b_only") == 2
        assert ctx.get("c_only") == 3

    def test_conflicting_keys_in_parallel_branches(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        CRITICAL TEST: When B and C both produce the same key with different values,
        the merge behavior should be consistent (not non-deterministic).

        This is a potential bug area - topological sort order of B and C is undefined.
        """
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "random_delay": RandomDelayTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        # Run multiple times to detect non-determinism
        results = []
        for i in range(10):
            wf = Workflow.create(
                application="diamond-test",
                name=f"conflict-test-{i}",
                stages=[
                    StageExecution(
                        ref_id="stage_a",
                        name="Stage A",
                        context={"outputs_to_produce": {"shared": "from_a"}},
                        tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_b",
                        name="Stage B",
                        requisite_stage_ref_ids={"stage_a"},
                        context={
                            "outputs_to_produce": {"conflict_key": "from_b", "b_marker": True},
                            "min_delay": 0.01,
                            "max_delay": 0.05,
                        },
                        tasks=[TaskExecution.create("Task B", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_c",
                        name="Stage C",
                        requisite_stage_ref_ids={"stage_a"},
                        context={
                            "outputs_to_produce": {"conflict_key": "from_c", "c_marker": True},
                            "min_delay": 0.01,
                            "max_delay": 0.05,
                        },
                        tasks=[TaskExecution.create("Task C", "random_delay", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_d",
                        name="Stage D",
                        requisite_stage_ref_ids={"stage_b", "stage_c"},
                        context={"expected_keys": ["shared", "conflict_key", "b_marker", "c_marker"]},
                        tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                    ),
                ],
            )

            file_repository.store(wf)
            runner.start(wf)
            processor.process_all(timeout=30.0)

            result = file_repository.retrieve(wf.id)
            assert result.status == WorkflowStatus.SUCCEEDED, f"Run {i} failed: {result.status}"

            stage_d_key = f"{wf.id}:stage_d"
            ctx = VerifyInputsTask.verified_contexts.get(stage_d_key, {})
            conflict_value = ctx.get("conflict_key")
            results.append(conflict_value)

        # Check for non-determinism - if results vary, we have a bug
        unique_values = set(results)
        if len(unique_values) > 1:
            # This is a potential issue - non-deterministic merge order
            # Count occurrences
            counter = Counter(results)
            pytest.fail(
                f"NON-DETERMINISTIC OUTPUT MERGE DETECTED!\n"
                f"conflict_key values across 10 runs: {counter}\n"
                f"This indicates the merge order of parallel branches is not stable."
            )

    def test_merge_winner_is_stable_across_processes(self, tmp_path: Path) -> None:
        """The sibling that wins a key collision must not depend on hash seed.

        test_conflicting_keys_in_parallel_branches runs its iterations inside one
        interpreter, where str hashing is fixed for the process lifetime, so it
        cannot observe this. Each iteration here is a fresh interpreter.
        """
        worker = tmp_path / "merge_worker.py"
        worker.write_text(
            "import tempfile\n"
            "from stabilize import SqliteWorkflowStore, StageExecution, Workflow\n"
            "tmp = tempfile.mkdtemp()\n"
            "store = SqliteWorkflowStore(f'sqlite:///{tmp}/m.db', create_tables=True)\n"
            "stages = [\n"
            "    StageExecution(ref_id='stage_a', type='t', name='A'),\n"
            "    StageExecution(ref_id='stage_b', type='t', name='B',"
            " requisite_stage_ref_ids={'stage_a'}),\n"
            "    StageExecution(ref_id='stage_c', type='t', name='C',"
            " requisite_stage_ref_ids={'stage_a'}),\n"
            "    StageExecution(ref_id='stage_d', type='t', name='D',"
            " requisite_stage_ref_ids={'stage_b', 'stage_c'}),\n"
            "]\n"
            "wf = Workflow.create(application='merge', name='diamond', stages=stages)\n"
            "store.store(wf)\n"
            "for s in store.retrieve(wf.id).stages:\n"
            "    if s.ref_id == 'stage_b':\n"
            "        s.outputs = {'conflict_key': 'from_b'}\n"
            "        store.store_stage(s)\n"
            "    elif s.ref_id == 'stage_c':\n"
            "        s.outputs = {'conflict_key': 'from_c'}\n"
            "        store.store_stage(s)\n"
            "print(store.get_merged_ancestor_outputs(wf.id, 'stage_d').get('conflict_key'))\n"
        )

        winners = []
        for seed in range(8):
            env = {**os.environ, "PYTHONHASHSEED": str(seed)}
            proc = subprocess.run(
                [sys.executable, str(worker)],
                capture_output=True,
                text=True,
                env=env,
                timeout=120,
            )
            assert proc.returncode == 0, f"worker failed (seed={seed}): {proc.stderr}"
            winners.append(proc.stdout.strip())

        assert all(w in {"from_b", "from_c"} for w in winners), winners
        assert len(set(winners)) == 1, (
            f"merge winner varies with PYTHONHASHSEED: {Counter(winners)}"
        )

    def test_list_outputs_concatenated(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """List outputs from parallel branches should be concatenated."""
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = Workflow.create(
            application="diamond-test",
            name="list-merge-test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    context={"outputs_to_produce": {"items": ["a1", "a2"]}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"items": ["b1", "b2"]}},
                    tasks=[TaskExecution.create("Task B", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"items": ["c1", "c2"]}},
                    tasks=[TaskExecution.create("Task C", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_d",
                    name="Stage D",
                    requisite_stage_ref_ids={"stage_b", "stage_c"},
                    context={"expected_keys": ["items"]},
                    tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        stage_d_key = f"{wf.id}:stage_d"
        ctx = VerifyInputsTask.verified_contexts.get(stage_d_key, {})
        items = ctx.get("items", [])

        # Should contain items from A, B, and C
        assert "a1" in items and "a2" in items, f"Missing A items: {items}"
        assert "b1" in items and "b2" in items, f"Missing B items: {items}"
        assert "c1" in items and "c2" in items, f"Missing C items: {items}"


class TestDiamondWithFailures:
    """Test diamond patterns with failures in branches."""

    def test_one_branch_fails_blocks_join(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """If B fails, D should not start and workflow should fail."""
        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "conditional_fail": ConditionalFailTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = Workflow.create(
            application="diamond-test",
            name="failure-test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    context={"outputs_to_produce": {"from_a": True}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B (Will Fail)",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"should_fail": True},
                    tasks=[TaskExecution.create("Task B", "conditional_fail", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_c": True}},
                    tasks=[TaskExecution.create("Task C", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_d",
                    name="Stage D (Join)",
                    requisite_stage_ref_ids={"stage_b", "stage_c"},
                    context={"expected_keys": ["from_a", "from_c"]},
                    tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        result = file_repository.retrieve(wf.id)

        # Workflow should fail due to B's failure
        assert result.status == WorkflowStatus.TERMINAL, f"Workflow status: {result.status}"

        # Stage D should NOT have run (should be NOT_STARTED or CANCELED)
        stage_d = result.stage_by_ref_id("stage_d")
        assert stage_d.status in {
            WorkflowStatus.NOT_STARTED,
            WorkflowStatus.CANCELED,
        }, f"Stage D should not have run: {stage_d.status}"
