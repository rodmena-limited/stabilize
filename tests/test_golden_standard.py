"""Golden Standard Pipeline Test - Parameterized for both backends."""

from stabilize import (
    StageExecution,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
    WorkflowStatus,
)
from stabilize.context.stage_context import StageContext
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue
from tests.conftest import setup_stabilize

# =============================================================================
# Custom Task Implementations
# =============================================================================


class SetupTask(Task):
    """Phase 1: Setup task that outputs PHASE1_SETUP."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"phase1_token": "PHASE1_SETUP"})


class RetryTask(Task):
    """Branch A: Simulates retry - first call returns RUNNING, second returns SUCCESS."""

    _attempts: dict[str, int] = {}

    def execute(self, stage: StageExecution) -> TaskResult:
        stage_id = stage.id
        attempts = RetryTask._attempts.get(stage_id, 0)
        RetryTask._attempts[stage_id] = attempts + 1

        if attempts < 1:
            return TaskResult.running(context={"retry_attempt": attempts + 1})

        return TaskResult.success(outputs={"phase2a_token": "PHASE2A_RETRY_SUCCESS"})


class TimeoutTask(Task):
    """Branch B: Simulates timeout with FAILED_CONTINUE."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.failed_continue(
            error="Task timed out",
            outputs={"phase2b_timed_out": True},
        )


class CompensationTask(Task):
    """Branch B Compensation: Runs after TimeoutTask fails."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"phase2b_token": "PHASE2B_TIMEOUT_COMPENSATED"})


class EventEmitterTask(Task):
    """Branch C: Event emitter - just succeeds."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"event_emitted": True})


class EventReceiverTask(Task):
    """Branch D: Receives event from Branch C."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"phase3_token": "PHASE3_EVENT_RECEIVED"})


class JoinGateTask(Task):
    """Phase 4: Join gate that waits for all branches."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"phase4_token": "PHASE4_SYNC_GATE_PASSED"})


class LoopIterationTask(Task):
    """Phase 5: Loop iteration with conditional logic."""

    def execute(self, stage: StageExecution) -> TaskResult:
        counter = stage.context.get("counter", 1)

        if counter == 2:
            token = "PHASE5_LOOP_IF_2"
        else:
            token = f"PHASE5_LOOP_ELSE_{counter}"

        output_key = f"loop{counter}_token"
        return TaskResult.success(outputs={output_key: token})


class FinalizeTask(Task):
    """Phase 6: Finalize - collect all tokens and assemble result."""

    def execute(self, stage: StageExecution) -> TaskResult:
        context = StageContext(stage, stage.context)

        tokens = [
            context.get("phase1_token", "MISSING_PHASE1"),
            context.get("phase2a_token", "MISSING_PHASE2A"),
            context.get("phase2b_token", "MISSING_PHASE2B"),
            context.get("phase3_token", "MISSING_PHASE3"),
            context.get("phase4_token", "MISSING_PHASE4"),
            context.get("loop1_token", "MISSING_LOOP1"),
            context.get("loop2_token", "MISSING_LOOP2"),
            context.get("loop3_token", "MISSING_LOOP3"),
            "PHASE6_END",
        ]

        result = "::".join(tokens)
        return TaskResult.success(outputs={"final_result": result})


# =============================================================================
# Pipeline Definition
# =============================================================================


def create_golden_pipeline() -> Workflow:
    """Create the golden standard pipeline."""
    return Workflow.create(
        application="golden-standard-test",
        name="Golden Standard Pipeline",
        stages=[
            # Phase 1: Setup
            StageExecution(
                ref_id="1",
                type="setup",
                name="Phase 1: Setup",
                tasks=[
                    TaskExecution.create(
                        name="Setup Task",
                        implementing_class="gs_setup",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch A: Retry
            StageExecution(
                ref_id="2a",
                type="retry",
                name="Branch A: Retry",
                requisite_stage_ref_ids={"1"},
                tasks=[
                    TaskExecution.create(
                        name="Retry Task",
                        implementing_class="gs_retry",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch B: Timeout
            StageExecution(
                ref_id="2b",
                type="timeout",
                name="Branch B: Timeout",
                requisite_stage_ref_ids={"1"},
                context={"continuePipelineOnFailure": True},
                tasks=[
                    TaskExecution.create(
                        name="Timeout Task",
                        implementing_class="gs_timeout",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch B Compensation
            StageExecution(
                ref_id="2b_comp",
                type="compensation",
                name="Branch B: Compensation",
                requisite_stage_ref_ids={"2b"},
                tasks=[
                    TaskExecution.create(
                        name="Compensation Task",
                        implementing_class="gs_compensation",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch C: Event Emitter
            StageExecution(
                ref_id="2c",
                type="event_emitter",
                name="Branch C: Event Emitter",
                requisite_stage_ref_ids={"1"},
                tasks=[
                    TaskExecution.create(
                        name="Event Emitter Task",
                        implementing_class="gs_event_emitter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Branch D: Event Receiver
            StageExecution(
                ref_id="2d",
                type="event_receiver",
                name="Branch D: Event Receiver",
                requisite_stage_ref_ids={"2c"},
                tasks=[
                    TaskExecution.create(
                        name="Event Receiver Task",
                        implementing_class="gs_event_receiver",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 4: Join Gate
            StageExecution(
                ref_id="4",
                type="join",
                name="Phase 4: Join Gate",
                requisite_stage_ref_ids={"2a", "2b_comp", "2d"},
                tasks=[
                    TaskExecution.create(
                        name="Join Gate Task",
                        implementing_class="gs_join_gate",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 5: Loop Iterations
            StageExecution(
                ref_id="5a",
                type="loop",
                name="Phase 5: Loop Iteration 1",
                requisite_stage_ref_ids={"4"},
                context={"counter": 1},
                tasks=[
                    TaskExecution.create(
                        name="Loop Iteration Task",
                        implementing_class="gs_loop_iteration",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="5b",
                type="loop",
                name="Phase 5: Loop Iteration 2",
                requisite_stage_ref_ids={"5a"},
                context={"counter": 2},
                tasks=[
                    TaskExecution.create(
                        name="Loop Iteration Task",
                        implementing_class="gs_loop_iteration",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="5c",
                type="loop",
                name="Phase 5: Loop Iteration 3",
                requisite_stage_ref_ids={"5b"},
                context={"counter": 3},
                tasks=[
                    TaskExecution.create(
                        name="Loop Iteration Task",
                        implementing_class="gs_loop_iteration",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Phase 6: Finalize
            StageExecution(
                ref_id="6",
                type="finalize",
                name="Phase 6: Finalize",
                requisite_stage_ref_ids={"5c"},
                tasks=[
                    TaskExecution.create(
                        name="Finalize Task",
                        implementing_class="gs_finalize",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )


# =============================================================================
# Parameterized Test
# =============================================================================


class TestGoldenStandard:
    """Golden Standard Pipeline Test - runs on both SQLite and PostgreSQL."""

    EXPECTED_RESULT = (
        "PHASE1_SETUP::PHASE2A_RETRY_SUCCESS::PHASE2B_TIMEOUT_COMPENSATED::"
        "PHASE3_EVENT_RECEIVED::PHASE4_SYNC_GATE_PASSED::PHASE5_LOOP_ELSE_1::"
        "PHASE5_LOOP_IF_2::PHASE5_LOOP_ELSE_3::PHASE6_END"
    )

    def test_golden_standard_pipeline(
        self,
        backend: str,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test the complete golden standard pipeline on both backends."""
        # Reset retry task state
        RetryTask._attempts = {}

        # Setup with custom tasks
        processor, runner = setup_stabilize(repository, queue)

        # Get the task registry from the RunTaskHandler
        for handler in processor._handlers.values():
            if hasattr(handler, "task_registry"):
                task_registry: TaskRegistry = handler.task_registry
                task_registry.register("gs_setup", SetupTask)
                task_registry.register("gs_retry", RetryTask)
                task_registry.register("gs_timeout", TimeoutTask)
                task_registry.register("gs_compensation", CompensationTask)
                task_registry.register("gs_event_emitter", EventEmitterTask)
                task_registry.register("gs_event_receiver", EventReceiverTask)
                task_registry.register("gs_join_gate", JoinGateTask)
                task_registry.register("gs_loop_iteration", LoopIterationTask)
                task_registry.register("gs_finalize", FinalizeTask)
                break

        # Create and store pipeline
        execution = create_golden_pipeline()
        repository.store(execution)

        # Run pipeline
        runner.start(execution)
        processor.process_all(timeout=30.0)

        # Retrieve result
        result = repository.retrieve(execution.id)

        # Verify execution succeeded
        assert result.status == WorkflowStatus.SUCCEEDED, f"[{backend}] Execution failed with status {result.status}"

        # Find finalize stage and get result
        finalize_stage = next(
            (s for s in result.stages if s.ref_id == "6"),
            None,
        )
        assert finalize_stage is not None, f"[{backend}] Finalize stage not found"

        final_result = finalize_stage.outputs.get("final_result", "")

        # Verify output matches expected
        assert final_result == self.EXPECTED_RESULT, (
            f"[{backend}] Output mismatch!\nExpected: {self.EXPECTED_RESULT}\nGot:      {final_result}"
        )

        # Cleanup
        repository.delete(execution.id)
