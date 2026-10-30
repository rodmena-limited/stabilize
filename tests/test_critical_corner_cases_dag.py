from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from stabilize import StageExecution
from stabilize.dag.topological import (
    CircularDependencyError,
    get_execution_layers,
    topological_sort,
)
from stabilize.errors import ConcurrencyError
from stabilize.models.stage import SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize
from tests.corner_case_support import SlowTask

# =============================================================================
# 3. DAG Traversal Edge Cases
# =============================================================================


class TestDagTraversalEdgeCases:
    """Test edge cases in DAG traversal."""

    def test_stage_with_missing_requisite(self) -> None:
        """Stage with requisite pointing to non-existent stage."""
        stages = [
            StageExecution.create(
                type="stage",
                name="A",
                ref_id="a",
                requisite_stage_ref_ids={"nonexistent"},
            ),
        ]

        # Should raise or handle gracefully
        with pytest.raises(CircularDependencyError):
            topological_sort(stages)

    def test_self_referential_requisite(self) -> None:
        """Stage with requisite pointing to itself."""
        stages = [
            StageExecution.create(
                type="stage",
                name="A",
                ref_id="a",
                requisite_stage_ref_ids={"a"},
            ),
        ]

        # Should detect self-cycle
        with pytest.raises(CircularDependencyError):
            topological_sort(stages)

    def test_deep_dag_structure(self) -> None:
        """Very deep DAG structure (potential stack overflow)."""
        stages = []
        prev_ref = None

        # Create 100-level deep DAG
        for i in range(100):
            ref_id = f"stage_{i}"
            stage = StageExecution.create(
                type="stage",
                name=f"Stage {i}",
                ref_id=ref_id,
                requisite_stage_ref_ids={prev_ref} if prev_ref else set(),
            )
            stages.append(stage)
            prev_ref = ref_id

        # Should handle deep structures without stack overflow
        sorted_stages = topological_sort(stages)
        assert len(sorted_stages) == 100
        assert sorted_stages[0].ref_id == "stage_0"
        assert sorted_stages[-1].ref_id == "stage_99"

    def test_wide_dag_structure(self) -> None:
        """Very wide DAG structure with many parallel stages."""
        stages = [
            StageExecution.create(type="stage", name="Start", ref_id="start"),
        ]

        # Add 50 parallel stages
        for i in range(50):
            stages.append(
                StageExecution.create(
                    type="stage",
                    name=f"Parallel {i}",
                    ref_id=f"parallel_{i}",
                    requisite_stage_ref_ids={"start"},
                )
            )

        # Add join stage
        stages.append(
            StageExecution.create(
                type="stage",
                name="Join",
                ref_id="join",
                requisite_stage_ref_ids={f"parallel_{i}" for i in range(50)},
            )
        )

        sorted_stages = topological_sort(stages)
        layers = get_execution_layers(stages)

        assert len(sorted_stages) == 52
        assert len(layers) == 3
        assert len(layers[1]) == 50  # 50 parallel stages

    def test_complex_diamond_pattern(self) -> None:
        """Complex diamond with multiple join points."""
        #       A
        #      /|\
        #     B C D
        #     |X|/
        #     E F
        #      \|
        #       G

        stages = [
            StageExecution.create(type="stage", name="A", ref_id="a"),
            StageExecution.create(type="stage", name="B", ref_id="b", requisite_stage_ref_ids={"a"}),
            StageExecution.create(type="stage", name="C", ref_id="c", requisite_stage_ref_ids={"a"}),
            StageExecution.create(type="stage", name="D", ref_id="d", requisite_stage_ref_ids={"a"}),
            StageExecution.create(type="stage", name="E", ref_id="e", requisite_stage_ref_ids={"b", "c"}),
            StageExecution.create(type="stage", name="F", ref_id="f", requisite_stage_ref_ids={"c", "d"}),
            StageExecution.create(type="stage", name="G", ref_id="g", requisite_stage_ref_ids={"e", "f"}),
        ]

        sorted_stages = topological_sort(stages)

        # Verify order constraints
        ref_ids = [s.ref_id for s in sorted_stages]
        assert ref_ids.index("a") < ref_ids.index("b")
        assert ref_ids.index("a") < ref_ids.index("c")
        assert ref_ids.index("a") < ref_ids.index("d")
        assert ref_ids.index("b") < ref_ids.index("e")
        assert ref_ids.index("c") < ref_ids.index("e")
        assert ref_ids.index("c") < ref_ids.index("f")
        assert ref_ids.index("d") < ref_ids.index("f")
        assert ref_ids.index("e") < ref_ids.index("g")
        assert ref_ids.index("f") < ref_ids.index("g")


# =============================================================================
# 4. Concurrency and Race Condition Tests
# =============================================================================


class TestConcurrencyEdgeCases:
    """Test concurrency-related edge cases."""

    def test_concurrent_workflow_starts(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test starting multiple workflows concurrently."""
        processor, orchestrator, registry = setup_stabilize(repository, queue)

        workflows = []
        for i in range(5):
            stage = StageExecution.create(
                type="test",
                name=f"Stage {i}",
                ref_id=f"stage_{i}",
            )
            task = TaskExecution(
                name="SuccessTask",
                implementing_class="success",
                stage_start=True,
                stage_end=True,
            )
            stage.tasks = [task]

            workflow = Workflow.create(
                application="test",
                name=f"Concurrent Test {i}",
                stages=[stage],
            )
            repository.store(workflow)
            workflows.append(workflow)

        # Start all workflows
        for wf in workflows:
            orchestrator.start(wf)

        # Process all
        processor.process_all(timeout=10.0)

        # All should complete successfully
        for wf in workflows:
            result = repository.retrieve(wf.id)
            assert result.status == WorkflowStatus.SUCCEEDED

    def test_concurrent_stage_completion(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test multiple stages completing at the same time."""
        processor, orchestrator, registry = setup_stabilize(
            repository,
            queue,
            extra_tasks={"slow": SlowTask},
        )

        # Create a workflow with multiple parallel stages
        start_stage = StageExecution.create(
            type="test",
            name="Start",
            ref_id="start",
        )
        start_task = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        start_stage.tasks = [start_task]

        parallel_stages = []
        for i in range(5):
            stage = StageExecution.create(
                type="test",
                name=f"Parallel {i}",
                ref_id=f"parallel_{i}",
                requisite_stage_ref_ids={"start"},
                context={"sleep_time": 0.01},  # Short sleep
            )
            task = TaskExecution(
                name="SlowTask",
                implementing_class="slow",
                stage_start=True,
                stage_end=True,
            )
            stage.tasks = [task]
            parallel_stages.append(stage)

        join_stage = StageExecution.create(
            type="test",
            name="Join",
            ref_id="join",
            requisite_stage_ref_ids={f"parallel_{i}" for i in range(5)},
        )
        join_task = TaskExecution(
            name="SuccessTask",
            implementing_class="success",
            stage_start=True,
            stage_end=True,
        )
        join_stage.tasks = [join_task]

        workflow = Workflow.create(
            application="test",
            name="Concurrent Stage Test",
            stages=[start_stage] + parallel_stages + [join_stage],
        )

        repository.store(workflow)
        orchestrator.start(workflow)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

    def test_concurrency_error_retry(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test that ConcurrencyError triggers proper retry."""
        from stabilize.handlers.base import StabilizeHandler
        from stabilize.resilience.config import HandlerConfig

        call_count = 0
        success_on_attempt = 3

        # Note: retry_on_concurrency_error expects Callable[[], None]
        def failing_func() -> None:
            nonlocal call_count
            call_count += 1
            if call_count < success_on_attempt:
                raise ConcurrencyError("Test concurrency error")
            # Success - just return (no value needed)

        # Create a mock handler to test retry logic
        class MockHandler(StabilizeHandler):
            @property
            def message_type(self) -> type:
                return MagicMock

            def handle(self, message: Any) -> None:
                pass

        config = HandlerConfig(
            concurrency_max_retries=5,
            concurrency_min_delay_ms=1,
        )

        handler = MockHandler(
            queue=MagicMock(),
            repository=MagicMock(),
            handler_config=config,
        )

        # Should not raise - succeeds after retries
        handler.retry_on_concurrency_error(failing_func, "test operation")
        assert call_count == success_on_attempt


# =============================================================================
# 6. Synthetic Stage Edge Cases
# =============================================================================


class TestSyntheticStageEdgeCases:
    """Test edge cases with synthetic stages."""

    def test_synthetic_stage_without_parent_link(self) -> None:
        """Synthetic stage with parent_stage_id but parent not in workflow."""
        parent = StageExecution.create(
            type="test",
            name="Parent",
            ref_id="parent",
        )

        synthetic = StageExecution.create_synthetic(
            type="cleanup",
            name="Cleanup",
            parent=parent,
            owner=SyntheticStageOwner.STAGE_AFTER,
        )

        # Create workflow without the parent
        Workflow.create(
            application="test",
            name="Orphan Synthetic Test",
            stages=[synthetic],
        )

        # Should raise when trying to access parent
        with pytest.raises(ValueError):
            synthetic.parent()

    def test_multiple_before_stages(self) -> None:
        """Multiple before-stages should all run."""
        parent = StageExecution.create(
            type="test",
            name="Parent",
            ref_id="parent",
        )

        before1 = StageExecution.create_synthetic(
            type="setup1",
            name="Setup 1",
            parent=parent,
            owner=SyntheticStageOwner.STAGE_BEFORE,
        )

        before2 = StageExecution.create_synthetic(
            type="setup2",
            name="Setup 2",
            parent=parent,
            owner=SyntheticStageOwner.STAGE_BEFORE,
        )

        # Must store workflow reference to prevent garbage collection
        _workflow = Workflow.create(
            application="test",
            name="Multi Before Test",
            stages=[parent, before1, before2],
        )

        # Should find both before-stages
        before_stages = parent.before_stages()
        assert len(before_stages) == 2
        del _workflow  # Explicit cleanup

    def test_synthetic_stage_status_propagation(self) -> None:
        """Synthetic stage failure should affect parent status."""
        parent = StageExecution.create(
            type="test",
            name="Parent",
            ref_id="parent",
        )
        task = TaskExecution(name="task", implementing_class="test")
        task.status = WorkflowStatus.SUCCEEDED
        parent.tasks = [task]

        after = StageExecution.create_synthetic(
            type="cleanup",
            name="Cleanup",
            parent=parent,
            owner=SyntheticStageOwner.STAGE_AFTER,
        )
        after.status = WorkflowStatus.TERMINAL

        # Must store workflow reference to prevent garbage collection
        _workflow = Workflow.create(
            application="test",
            name="Synthetic Status Test",
            stages=[parent, after],
        )

        # Parent should report TERMINAL due to after-stage failure
        assert parent.determine_status() == WorkflowStatus.TERMINAL
        del _workflow  # Explicit cleanup
