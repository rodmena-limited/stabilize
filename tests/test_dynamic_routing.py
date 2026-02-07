"""Tests for dynamic routing (jump_to) functionality."""

from stabilize import TaskResult
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import JumpToStage
from stabilize.tasks.interface import Task
from tests.conftest import setup_stabilize


class TestTaskResultJumpTo:
    """Tests for TaskResult.jump_to() factory method."""

    def test_jump_to_creates_redirect_status(self) -> None:
        """Verify jump_to creates a result with REDIRECT status."""
        result = TaskResult.jump_to("target_stage")
        assert result.status == WorkflowStatus.REDIRECT
        assert result.target_stage_ref_id == "target_stage"

    def test_jump_to_with_context(self) -> None:
        """Verify jump_to accepts context parameter."""
        result = TaskResult.jump_to(
            "target_stage",
            context={"retry_reason": "tests failed"},
        )
        assert result.context == {"retry_reason": "tests failed"}
        assert result.target_stage_ref_id == "target_stage"

    def test_jump_to_with_outputs(self) -> None:
        """Verify jump_to accepts outputs parameter."""
        result = TaskResult.jump_to(
            "target_stage",
            outputs={"preserved_data": [1, 2, 3]},
        )
        assert result.outputs == {"preserved_data": [1, 2, 3]}
        assert result.target_stage_ref_id == "target_stage"

    def test_jump_to_with_all_params(self) -> None:
        """Verify jump_to accepts all parameters."""
        result = TaskResult.jump_to(
            "implement_stage",
            context={"attempt": 2},
            outputs={"error_log": "test failed"},
        )
        assert result.status == WorkflowStatus.REDIRECT
        assert result.target_stage_ref_id == "implement_stage"
        assert result.context == {"attempt": 2}
        assert result.outputs == {"error_log": "test failed"}

    def test_regular_redirect_has_no_target(self) -> None:
        """Verify regular redirect() has no target_stage_ref_id."""
        result = TaskResult.redirect(context={"branch": "A"})
        assert result.status == WorkflowStatus.REDIRECT
        assert result.target_stage_ref_id is None


class TestJumpToStageMessage:
    """Tests for JumpToStage message."""

    def test_message_attributes(self) -> None:
        """Verify JumpToStage message has correct attributes."""
        msg = JumpToStage(
            execution_type="PIPELINE",
            execution_id="exec-123",
            stage_id="stage-456",
            target_stage_ref_id="target_ref",
            jump_context={"key": "value"},
            jump_outputs={"out": "data"},
        )
        assert msg.target_stage_ref_id == "target_ref"
        assert msg.jump_context == {"key": "value"}
        assert msg.jump_outputs == {"out": "data"}

    def test_message_defaults(self) -> None:
        """Verify JumpToStage message default values."""
        msg = JumpToStage()
        assert msg.target_stage_ref_id == ""
        assert msg.jump_context == {}
        assert msg.jump_outputs == {}


# Test task that jumps to another stage
class RouterTask(Task):
    """Task that conditionally jumps to another stage."""

    should_jump = True
    target_stage = "implement_stage"
    jump_context: dict = {}

    def execute(self, stage: StageExecution) -> TaskResult:
        if RouterTask.should_jump:
            return TaskResult.jump_to(
                RouterTask.target_stage,
                context=RouterTask.jump_context,
            )
        return TaskResult.success(outputs={"routed": False})


class ImplementTask(Task):
    """Task in the target stage that captures context."""

    received_context: dict = {}
    execution_count = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        ImplementTask.execution_count += 1
        ImplementTask.received_context = dict(stage.context)
        return TaskResult.success(
            outputs={
                "implemented": True,
                "execution_count": ImplementTask.execution_count,
            }
        )


class TestDynamicRoutingIntegration:
    """Integration tests for dynamic routing."""

    def test_jump_to_existing_stage(
        self,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Verify jump to existing stage works correctly."""
        # Reset task state
        RouterTask.should_jump = True
        RouterTask.target_stage = "implement_stage"
        RouterTask.jump_context = {"retry_reason": "tests failed"}
        ImplementTask.execution_count = 0
        ImplementTask.received_context = {}

        processor, runner, _ = setup_stabilize(
            repository, queue, extra_tasks={"router": RouterTask, "implement": ImplementTask}
        )

        execution = Workflow.create(
            application="test",
            name="Routing Pipeline",
            stages=[
                StageExecution(
                    ref_id="router_stage",
                    type="test",
                    name="Router Stage",
                    tasks=[
                        TaskExecution.create(
                            name="Router Task",
                            implementing_class="router",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="implement_stage",
                    type="test",
                    name="Implement Stage",
                    requisite_stage_ref_ids={"router_stage"},
                    tasks=[
                        TaskExecution.create(
                            name="Implement Task",
                            implementing_class="implement",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)

        # Process all messages
        processor.process_all(timeout=30.0)

        # Verify
        result = repository.retrieve(execution.id)

        # The implement_stage should have run (jumped to by router)
        implement_stage = result.stage_by_ref_id("implement_stage")
        assert implement_stage is not None
        assert implement_stage.status == WorkflowStatus.SUCCEEDED
        assert implement_stage.outputs.get("implemented") is True

        # Verify context was passed
        assert "retry_reason" in ImplementTask.received_context

        # Cleanup
        repository.delete(execution.id)

    def test_jump_count_tracked(
        self,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Verify jump count is tracked in execution context."""
        RouterTask.should_jump = True
        RouterTask.target_stage = "implement_stage"
        RouterTask.jump_context = {}
        ImplementTask.execution_count = 0

        processor, runner, _ = setup_stabilize(
            repository, queue, extra_tasks={"router": RouterTask, "implement": ImplementTask}
        )

        execution = Workflow.create(
            application="test",
            name="Jump Count Pipeline",
            stages=[
                StageExecution(
                    ref_id="router_stage",
                    type="test",
                    name="Router Stage",
                    tasks=[
                        TaskExecution.create(
                            name="Router Task",
                            implementing_class="router",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="implement_stage",
                    type="test",
                    name="Implement Stage",
                    requisite_stage_ref_ids={"router_stage"},
                    tasks=[
                        TaskExecution.create(
                            name="Implement Task",
                            implementing_class="implement",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=30.0)

        result = repository.retrieve(execution.id)

        # Verify jump count was tracked (stored in source stage context)
        router_stage = result.stage_by_ref_id("router_stage")
        assert router_stage is not None
        assert router_stage.context.get("_jump_count", 0) >= 1

        # Verify jump history was recorded
        jump_history = router_stage.context.get("_jump_history", [])
        assert len(jump_history) >= 1
        assert jump_history[0]["from_stage"] == "router_stage"
        assert jump_history[0]["to_stage"] == "implement_stage"

        repository.delete(execution.id)


class InfiniteLoopTask(Task):
    """Task that always jumps back, creating potential infinite loop."""

    jump_count = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        InfiniteLoopTask.jump_count += 1
        # Always try to jump back to stage_a
        return TaskResult.jump_to(
            "stage_a",
            context={"loop_iteration": InfiniteLoopTask.jump_count},
        )


class TestJumpCountLimiting:
    """Tests for jump count limiting to prevent infinite loops."""

    def test_max_jump_count_prevents_infinite_loop(
        self,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Verify max jump count is enforced to prevent infinite loops."""
        InfiniteLoopTask.jump_count = 0

        processor, runner, _ = setup_stabilize(
            repository, queue, extra_tasks={"infinite_loop": InfiniteLoopTask}
        )

        execution = Workflow.create(
            application="test",
            name="Infinite Loop Pipeline",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    type="test",
                    name="Stage A",
                    context={"_max_jumps": 3},  # Set low limit in stage context
                    tasks=[
                        TaskExecution.create(
                            name="Infinite Loop Task",
                            implementing_class="infinite_loop",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=30.0)

        result = repository.retrieve(execution.id)

        # Should have hit max jump count and stopped (check stage context)
        stage_a = result.stage_by_ref_id("stage_a")
        assert stage_a is not None
        assert stage_a.context.get("_jump_count", 0) <= 3

        # Stage should be marked as TERMINAL with jump error
        assert stage_a.status == WorkflowStatus.TERMINAL
        assert "jump_error" in stage_a.context
        assert "Max jump count exceeded" in stage_a.context["jump_error"]

        repository.delete(execution.id)


class TestJumpToNonexistentStage:
    """Tests for error handling when jumping to nonexistent stage."""

    def test_jump_to_nonexistent_stage_handled(
        self,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Verify jumping to nonexistent stage is handled gracefully."""
        RouterTask.should_jump = True
        RouterTask.target_stage = "nonexistent_stage"  # This doesn't exist
        RouterTask.jump_context = {}

        processor, runner, _ = setup_stabilize(
            repository, queue, extra_tasks={"router": RouterTask}
        )

        execution = Workflow.create(
            application="test",
            name="Bad Jump Pipeline",
            stages=[
                StageExecution(
                    ref_id="router_stage",
                    type="test",
                    name="Router Stage",
                    tasks=[
                        TaskExecution.create(
                            name="Router Task",
                            implementing_class="router",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=30.0)

        result = repository.retrieve(execution.id)

        # Workflow should be completed (not hanging)
        assert result.status.is_complete

        # Should have recorded jump error
        router_stage = result.stage_by_ref_id("router_stage")
        assert router_stage is not None
        # The jump_error should be in context
        assert "jump_error" in router_stage.context

        repository.delete(execution.id)


class TestBackwardCompatibility:
    """Tests to ensure regular redirect still works."""

    def test_regular_redirect_unchanged(
        self,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Verify regular redirect (without target) works as before."""

        class RegularRedirectTask(Task):
            def execute(self, stage: StageExecution) -> TaskResult:
                # Regular redirect without target
                return TaskResult.redirect(context={"redirected": True})

        processor, runner, _ = setup_stabilize(
            repository, queue, extra_tasks={"regular_redirect": RegularRedirectTask}
        )

        execution = Workflow.create(
            application="test",
            name="Redirect Pipeline",
            stages=[
                StageExecution(
                    ref_id="redirect_stage",
                    type="test",
                    name="Redirect Stage",
                    tasks=[
                        TaskExecution.create(
                            name="Regular Redirect Task",
                            implementing_class="regular_redirect",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=30.0)

        result = repository.retrieve(execution.id)

        # Regular redirect should complete the stage
        redirect_stage = result.stage_by_ref_id("redirect_stage")
        assert redirect_stage is not None
        assert redirect_stage.context.get("redirected") is True

        repository.delete(execution.id)
