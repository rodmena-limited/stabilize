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

    return [
        StartWorkflowHandler(queue, store, handler_config=handler_config),
        StartWaitingWorkflowsHandler(queue, store, handler_config=handler_config),
        StartStageHandler(queue, store, handler_config=handler_config),
        SkipStageHandler(queue, store, handler_config=handler_config),
        CancelStageHandler(queue, store, task_registry=task_registry, handler_config=handler_config),
        ContinueParentStageHandler(queue, store, handler_config=handler_config),
        JumpToStageHandler(queue, store, handler_config=handler_config),
        SignalStageHandler(queue, store, handler_config=handler_config),
        CancelRegionHandler(queue, store, handler_config=handler_config),
        AddMultiInstanceHandler(queue, store, handler_config=handler_config),
        StartTaskHandler(queue, store, task_registry, handler_config=handler_config),
        RunTaskHandler(
            queue,
            store,
            task_registry,
            bulkhead_manager=bulkhead_manager,
            circuit_factory=circuit_factory,
            handler_config=handler_config,
        ),
        CompleteTaskHandler(queue, store, handler_config=handler_config),
        CompleteStageHandler(queue, store, task_registry=task_registry, handler_config=handler_config),
        CompleteWorkflowHandler(queue, store, handler_config=handler_config),
        CancelWorkflowHandler(queue, store, handler_config=handler_config),
        RestartStageHandler(queue, store, handler_config=handler_config),
        ResumeStageHandler(queue, store, handler_config=handler_config),
        PauseTaskHandler(queue, store, handler_config=handler_config),
    ]
