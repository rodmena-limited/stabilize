from __future__ import annotations

import pytest

from stabilize import StageExecution
from stabilize.models.stage import SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import StartStage
from tests.conftest import setup_stabilize


# =============================================================================
# Bug Hunt Test: Workflow Context Persistence (BUG #1 - CRITICAL)
# =============================================================================


class TestWorkflowContextPersistence:
    """
    CRITICAL BUG FOUND: Workflow context was not persisted to the database.
    This caused _max_jumps and other workflow-level settings to be lost after
    retrieving a workflow from the database.

    FIX: Added 'context' column to pipeline_executions table and updated
    converters in both SQLite and PostgreSQL backends.
    """

    def test_workflow_context_persisted(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Verify workflow context is persisted and retrieved correctly."""
        workflow = Workflow.create(
            application="airport",
            name="context-persistence-test",
            context={
                "custom_setting": "test_value",
                "nested": {"key": "value"},
                "list_data": [1, 2, 3],
            },
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                )
            ],
        )

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        assert result.context == {
            "custom_setting": "test_value",
            "nested": {"key": "value"},
            "list_data": [1, 2, 3],
        }

    def test_max_jumps_setting_persisted(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Verify _max_jumps workflow context setting is preserved."""
        workflow = Workflow.create(
            application="airport",
            name="max-jumps-persistence-test",
            context={"_max_jumps": 5},
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                )
            ],
        )

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        assert result.context.get("_max_jumps") == 5

    def test_max_jumps_zero_disables_jumps(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Verify _max_jumps=0 persists correctly and could disable jumps."""
        workflow = Workflow.create(
            application="airport",
            name="zero-max-jumps-test",
            context={"_max_jumps": 0},
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                )
            ],
        )

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        # _max_jumps=0 should persist as 0, not be lost
        assert "_max_jumps" in result.context
        assert result.context.get("_max_jumps") == 0

    def test_empty_workflow_context_persisted(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Verify empty workflow context is handled correctly."""
        workflow = Workflow.create(
            application="airport",
            name="empty-context-test",
            context={},
            stages=[
                StageExecution(
                    ref_id="test",
                    name="Test Stage",
                )
            ],
        )

        repository.store(workflow)
        result = repository.retrieve(workflow.id)

        assert result.context == {}


# =============================================================================
# Bug Hunt Test: Status Transition Consistency
# =============================================================================


class TestStatusTransitionConsistency:
    """
    BUG HYPOTHESIS: Invalid status transitions may not be properly prevented.
    """

    def test_completed_stage_cannot_restart(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Verify a completed stage cannot be restarted."""
        processor, runner, task_registry = setup_stabilize(repository, queue)

        stage = StageExecution(
            ref_id="test",
            name="Test Stage",
            status=WorkflowStatus.SUCCEEDED,
            tasks=[
                TaskExecution.create(
                    name="task",
                    implementing_class="success",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        workflow = Workflow.create(
            application="airport",
            name="restart-prevention",
            stages=[stage],
        )

        repository.store(workflow)

        queue.push(
            StartStage(
                execution_type="PIPELINE",
                execution_id=workflow.id,
                stage_id=stage.id,
            )
        )

        processor.process_one()

        result = repository.retrieve(workflow.id)
        stage_result = result.stage_by_ref_id("test")
        assert stage_result.status == WorkflowStatus.SUCCEEDED


# =============================================================================
# Bug Hunt Test: Orphaned Synthetic Stages
# =============================================================================


class TestOrphanedSyntheticStages:
    """
    BUG HYPOTHESIS: Synthetic stages with invalid parent_stage_id may cause issues.
    """

    def test_synthetic_stage_without_valid_parent(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test handling of synthetic stage with invalid parent_stage_id."""
        synthetic = StageExecution(
            ref_id="orphan",
            name="Orphaned Synthetic",
            parent_stage_id="non_existent_parent_id",
            synthetic_stage_owner=SyntheticStageOwner.STAGE_AFTER,
        )

        workflow = Workflow.create(
            application="airport",
            name="orphaned-synthetic",
            stages=[synthetic],
        )

        # Store and retrieve to ensure stage is attached to execution
        repository.store(workflow)
        result = repository.retrieve(workflow.id)
        orphan_stage = result.stage_by_ref_id("orphan")

        # Now calling parent() should raise ValueError about parent not found
        try:
            orphan_stage.parent()
            assert False, "Should have raised ValueError"
        except ValueError as e:
            # Should fail because parent_stage_id doesn't exist in the workflow
            assert "not found" in str(e).lower() or "parent" in str(e).lower()


# =============================================================================
# Bug Hunt Test: DAG Cycle Detection
# =============================================================================


class TestDAGCycleDetection:
    """
    BUG HYPOTHESIS: Circular dependencies may cause infinite loops.
    """

    def test_direct_cycle_detected(self) -> None:
        """Direct cycles (A -> B -> A) are rejected at Workflow.create."""
        from stabilize.dag.topological import CircularDependencyError

        with pytest.raises(CircularDependencyError):
            Workflow.create(
                application="airport",
                name="direct-cycle",
                stages=[
                    StageExecution(
                        ref_id="a",
                        name="Stage A",
                        requisite_stage_ref_ids={"b"},
                    ),
                    StageExecution(
                        ref_id="b",
                        name="Stage B",
                        requisite_stage_ref_ids={"a"},
                    ),
                ],
            )

    def test_indirect_cycle_detected(self) -> None:
        """Indirect cycles (A -> B -> C -> A) are rejected at Workflow.create."""
        from stabilize.dag.topological import CircularDependencyError

        with pytest.raises(CircularDependencyError):
            Workflow.create(
                application="airport",
                name="indirect-cycle",
                stages=[
                    StageExecution(
                        ref_id="a",
                        name="Stage A",
                        requisite_stage_ref_ids={"c"},
                    ),
                    StageExecution(
                        ref_id="b",
                        name="Stage B",
                        requisite_stage_ref_ids={"a"},
                    ),
                    StageExecution(
                        ref_id="c",
                        name="Stage C",
                        requisite_stage_ref_ids={"b"},
                    ),
                ],
            )


# =============================================================================
# Bug Hunt Test: Duplicate ref_id Detection (BUG #3)
# =============================================================================


class TestDuplicateRefIdDetection:
    """
    BUG FOUND: Duplicate ref_ids in stages were silently accepted by topological
    sort but caused undefined behavior when stage_by_ref_id() was called.

    This is a configuration error that should be detected and reported.
    """

    def test_duplicate_ref_id_in_topological_sort(self) -> None:
        """Test that duplicate ref_ids should be detected during validation."""
        from stabilize.dag.topological import topological_sort

        # Create two stages with the same ref_id
        stages = [
            StageExecution(
                ref_id="duplicate",
                name="Stage 1",
            ),
            StageExecution(
                ref_id="duplicate",
                name="Stage 2",
            ),
        ]

        # Currently topological_sort silently accepts this, but both stages
        # will be sorted (tracking by id, not ref_id). This is problematic
        # because stage_by_ref_id will only return one of them.
        sorted_stages = topological_sort(stages)

        # Document current behavior: both stages are returned
        # This test documents the bug - ideally this should raise an error
        assert len(sorted_stages) == 2

    def test_database_rejects_duplicate_ref_ids(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
    ) -> None:
        """Test that database correctly rejects duplicate ref_ids per execution.

        Workflow.create() now rejects duplicate ref_ids at submit time (the
        fix this test's docstring used to wish for). The DB's UNIQUE
        constraint on (execution_id, ref_id) remains the defense-in-depth
        layer, so build the workflow via the raw constructor to reach it.
        """
        import sqlite3

        from stabilize.dag.topological import InvalidStageGraphError
        from stabilize.models.workflow import Trigger

        duplicate_stages = [
            StageExecution(
                ref_id="duplicate",
                name="Stage 1",
                context={"order": 1},
            ),
            StageExecution(
                ref_id="duplicate",
                name="Stage 2",
                context={"order": 2},
            ),
        ]

        # Layer 1: submit-time validation refuses the graph outright.
        with pytest.raises(InvalidStageGraphError):
            Workflow.create(application="airport", name="duplicate-ref-test", stages=duplicate_stages)

        # Layer 2: the DB constraint still rejects a workflow that skipped
        # validation (raw constructor).
        workflow = Workflow(
            application="airport",
            name="duplicate-ref-test",
            stages=duplicate_stages,
            trigger=Trigger(),
        )
        with pytest.raises((sqlite3.IntegrityError, Exception)):
            repository.store(workflow)

    def test_workflow_validation_should_detect_duplicates(self) -> None:
        """Test that workflows should validate for duplicate ref_ids."""
        from stabilize.dag.topological import validate_dag

        workflow = Workflow.create(
            application="airport",
            name="duplicate-ref-validation",
            stages=[
                StageExecution(
                    ref_id="unique_a",
                    name="Stage A",
                ),
                StageExecution(
                    ref_id="unique_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"unique_a"},
                ),
            ],
        )

        # Valid DAG with unique ref_ids should pass
        assert validate_dag(workflow.stages) is True
