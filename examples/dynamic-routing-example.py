#!/usr/bin/env python3
"""
Dynamic Routing Example - Demonstrates jump_to and stateful retries.

This example shows two key Stabilize features:
1. Dynamic routing with TaskResult.jump_to() - redirect flow to different stages
2. Stateful retries with TransientError.context_update - preserve progress across retries

Run with:
    python examples/dynamic-routing-example.py
"""

import logging
from typing import Any

# Configure logging before importing stabilize modules
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    JumpToStageHandler,
    Orchestrator,
    Queue,
    QueueProcessor,
    RunTaskHandler,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    StartStageHandler,
    StartTaskHandler,
    StartWorkflowHandler,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
    WorkflowStore,
)
from stabilize.errors import TransientError
from stabilize.models.stage import StageExecution as StageModel
from stabilize.tasks.interface import Task

# =============================================================================
# Custom Tasks demonstrating the features
# =============================================================================


class QualityLoopTask(Task):
    """
    Quality improvement loop using self-jump pattern.

    This demonstrates TaskResult.jump_to() for dynamic routing.
    The task checks quality and either succeeds or jumps back to itself
    with an incremented attempt counter.
    """

    QUALITY_THRESHOLD = 80

    def execute(self, stage: StageModel) -> TaskResult:
        attempt = stage.context.get("attempt", 1)
        # Quality improves with each attempt: 65, 80, 95...
        quality = 50 + (attempt * 15)

        logger.info(f"[QualityLoop] Attempt #{attempt}, quality={quality} (threshold={self.QUALITY_THRESHOLD})")

        if quality >= self.QUALITY_THRESHOLD:
            logger.info(f"[QualityLoop] PASSED! Quality {quality} meets threshold.")
            return TaskResult.success(
                outputs={
                    "final_quality": quality,
                    "attempts_needed": attempt,
                    "status": "PASSED",
                }
            )
        else:
            logger.info(f"[QualityLoop] Quality {quality} < {self.QUALITY_THRESHOLD}, jumping back to retry...")
            # Jump back to same stage with incremented attempt (self-loop)
            return TaskResult.jump_to(
                "quality_loop",  # Jump to self
                context={
                    "attempt": attempt + 1,
                    "previous_quality": quality,
                },
            )


class BatchProcessTask(Task):
    """
    Simulates batch processing with transient failures and checkpointing.

    Demonstrates TransientError.context_update for stateful retries.
    Progress is preserved across retries via context_update.
    """

    TOTAL_ITEMS = 100
    ITEMS_PER_RUN = 33

    def execute(self, stage: StageModel) -> TaskResult:
        processed = stage.context.get("processed_count", 0)
        attempt = stage.context.get("_batch_attempt", 0) + 1

        logger.info(f"[Batch] Run #{attempt}, starting from {processed} items")

        # Simulate transient failure on first attempt when we have some progress
        if attempt == 1 and processed == 0:
            # First run - process some items then fail
            new_processed = self.ITEMS_PER_RUN
            logger.warning(f"[Batch] Transient error after processing {new_processed} items! Saving checkpoint...")
            raise TransientError(
                "Simulated transient error - network timeout",
                retry_after=0.1,
                context_update={
                    "processed_count": new_processed,
                    "_batch_attempt": attempt,
                    "_checkpoint_saved": True,
                },
            )

        # Continue from checkpoint
        new_processed = processed + self.ITEMS_PER_RUN

        if new_processed >= self.TOTAL_ITEMS:
            new_processed = self.TOTAL_ITEMS
            logger.info(f"[Batch] Complete! Processed all {self.TOTAL_ITEMS} items.")
            return TaskResult.success(
                outputs={
                    "total_processed": new_processed,
                    "status": "complete",
                    "checkpoint_used": stage.context.get("_checkpoint_saved", False),
                }
            )
        else:
            # More to process - fail again to show multiple retries
            logger.warning(f"[Batch] Another transient error at {new_processed} items! Checkpointing...")
            raise TransientError(
                "Simulated transient error - rate limit",
                retry_after=0.1,
                context_update={
                    "processed_count": new_processed,
                    "_batch_attempt": attempt,
                    "_checkpoint_saved": True,
                },
            )


# =============================================================================
# Helper: Setup pipeline infrastructure
# =============================================================================


def setup_pipeline_runner(store: WorkflowStore, queue: Queue) -> tuple[QueueProcessor, Orchestrator]:
    """Create processor and orchestrator with custom tasks registered."""
    task_registry = TaskRegistry()
    task_registry.register("quality_loop", QualityLoopTask)
    task_registry.register("batch", BatchProcessTask)

    processor = QueueProcessor(queue)

    handlers: list[Any] = [
        StartWorkflowHandler(queue, store),
        StartStageHandler(queue, store),
        StartTaskHandler(queue, store, task_registry),
        RunTaskHandler(queue, store, task_registry),
        CompleteTaskHandler(queue, store),
        CompleteStageHandler(queue, store),
        CompleteWorkflowHandler(queue, store),
        JumpToStageHandler(queue, store),  # Required for jump_to support
    ]

    for handler in handlers:
        processor.register_handler(handler)

    orchestrator = Orchestrator(queue)
    return processor, orchestrator


# =============================================================================
# Example 1: Dynamic Routing with jump_to
# =============================================================================


def example_jump_to() -> None:
    """Demonstrate TaskResult.jump_to() for quality improvement loop."""
    print("\n" + "=" * 70)
    print("Example 1: Dynamic Routing with jump_to()")
    print("=" * 70)
    print("""
This example shows a self-loop quality improvement pattern:
1. Task checks quality score (improves each attempt: 65, 80, 95...)
2. If quality < 80, jump_to() same stage with incremented attempt
3. Stage resets and runs again with updated context
4. Loop continues until quality >= 80

Flow: quality_loop (65) -> quality_loop (80, pass!)
""")

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="dynamic-routing-example",
        name="Quality Improvement Loop",
        context={"_max_jumps": 10},  # Allow up to 10 jumps
        stages=[
            StageExecution(
                ref_id="quality_loop",
                type="custom",
                name="Quality Check & Improve",
                context={"attempt": 1},
                tasks=[
                    TaskExecution.create(
                        name="Check and Improve Quality",
                        implementing_class="quality_loop",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=60.0)

    result = store.retrieve(workflow.id)
    print(f"\n{'=' * 60}")
    print(f"RESULT: Workflow Status = {result.status}")
    print(f"{'=' * 60}")

    stage = result.stage_by_ref_id("quality_loop")
    if stage:
        print("\nQuality Loop Stage Final State:")
        print(f"  Status: {stage.status}")
        print(f"  Outputs: {stage.outputs}")

        jump_count = stage.context.get("_jump_count", 0)
        if jump_count > 0:
            print("\n  Jump tracking (self-loops):")
            print(f"    Total jumps: {jump_count}")
            for i, jump in enumerate(stage.context.get("_jump_history", []), 1):
                print(f"    {i}. {jump['from_stage']} -> {jump['to_stage']} (context: {jump['context_keys']})")


# =============================================================================
# Example 2: Stateful Retries with context_update
# =============================================================================


def example_stateful_retries() -> None:
    """Demonstrate TransientError.context_update for preserving progress."""
    print("\n" + "=" * 70)
    print("Example 2: Stateful Retries with context_update")
    print("=" * 70)
    print("""
This example shows batch processing with checkpoint recovery:
1. Task processes ~33 items per run (total 100 items)
2. Transient errors occur, simulating network issues
3. Each error carries context_update with progress checkpoint
4. On retry, processing resumes from saved checkpoint
5. Context is durable - progress is never lost!

Expected: 3 runs (33 + 33 + 34 = 100 items)
""")

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="dynamic-routing-example",
        name="Batch Processing with Checkpoints",
        stages=[
            StageExecution(
                ref_id="batch",
                type="custom",
                name="Process Items",
                context={"processed_count": 0},
                tasks=[
                    TaskExecution.create(
                        name="Batch Processor",
                        implementing_class="batch",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=60.0)

    result = store.retrieve(workflow.id)
    print(f"\n{'=' * 60}")
    print(f"RESULT: Workflow Status = {result.status}")
    print(f"{'=' * 60}")

    stage = result.stages[0]
    print("\nBatch Stage Final State:")
    print(f"  Status: {stage.status}")
    print(f"  Outputs: {stage.outputs}")

    print("\n  Context (checkpoint data preserved across retries):")
    print(f"    processed_count: {stage.context.get('processed_count', 'N/A')}")
    print(f"    _batch_attempt: {stage.context.get('_batch_attempt', 'N/A')}")
    print(f"    _checkpoint_saved: {stage.context.get('_checkpoint_saved', False)}")

    if stage.outputs.get("checkpoint_used"):
        print("\n  [VERIFIED] Checkpoint was used to resume processing!")
        print("  Context survived transient errors and retries.")


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    print("=" * 70)
    print("Stabilize Dynamic Routing & Stateful Retries Examples")
    print("=" * 70)

    example_jump_to()
    example_stateful_retries()

    print("\n" + "=" * 70)
    print("All examples completed!")
    print("=" * 70)
