"""The default handler set a QueueProcessor registers, built from one configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue
    from stabilize.resilience.bulkheads import TaskBulkheadManager
    from stabilize.resilience.circuits import WorkflowCircuitFactory
    from stabilize.resilience.config import HandlerConfig
    from stabilize.tasks.registry import TaskRegistry


def default_handlers(
    queue: Queue,
    store: WorkflowStore,
    task_registry: TaskRegistry,
    bulkhead_manager: TaskBulkheadManager | None = None,
    circuit_factory: WorkflowCircuitFactory | None = None,
    handler_config: HandlerConfig | None = None,
) -> list[Any]:
    from stabilize.handlers import (
        AddMultiInstanceHandler,
        CancelRegionHandler,
        CancelStageHandler,
        CancelWorkflowHandler,
        CompleteStageHandler,
        CompleteTaskHandler,
        CompleteWorkflowHandler,
        ContinueParentStageHandler,
        JumpToStageHandler,
        PauseTaskHandler,
        RestartStageHandler,
        ResumeStageHandler,
        RunTaskHandler,
        SignalStageHandler,
        SkipStageHandler,
        StartStageHandler,
        StartTaskHandler,
        StartWaitingWorkflowsHandler,
        StartWorkflowHandler,
    )

    cfg = {"handler_config": handler_config}
    return [
        StartWorkflowHandler(queue, store, **cfg),
        StartWaitingWorkflowsHandler(queue, store, **cfg),
        StartStageHandler(queue, store, **cfg),
        SkipStageHandler(queue, store, **cfg),
        CancelStageHandler(queue, store, task_registry=task_registry, **cfg),
        ContinueParentStageHandler(queue, store, **cfg),
        JumpToStageHandler(queue, store, **cfg),
        SignalStageHandler(queue, store, **cfg),
        CancelRegionHandler(queue, store, **cfg),
        AddMultiInstanceHandler(queue, store, **cfg),
        StartTaskHandler(queue, store, task_registry, **cfg),
        RunTaskHandler(
            queue,
            store,
            task_registry,
            bulkhead_manager=bulkhead_manager,
            circuit_factory=circuit_factory,
            **cfg,
        ),
        CompleteTaskHandler(queue, store, **cfg),
        CompleteStageHandler(queue, store, task_registry=task_registry, **cfg),
        CompleteWorkflowHandler(queue, store, **cfg),
        CancelWorkflowHandler(queue, store, **cfg),
        RestartStageHandler(queue, store, **cfg),
        ResumeStageHandler(queue, store, **cfg),
        PauseTaskHandler(queue, store, **cfg),
    ]
