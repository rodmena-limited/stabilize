"""
Synchronous queue processor for testing.

This module provides the SynchronousQueueProcessor class that
processes messages immediately without threading.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from stabilize.queue import Queue
from stabilize.queue.messages import Message
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.queue.processor.processor import QueueProcessor
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.resilience.bulkheads import TaskBulkheadManager
    from stabilize.resilience.circuits import WorkflowCircuitFactory
    from stabilize.tasks.registry import TaskRegistry


class SynchronousQueueProcessor(QueueProcessor):
    """
    A synchronous queue processor that processes messages immediately.

    Useful for testing where you want deterministic execution order.

    Accepts the same parameters as :class:`QueueProcessor` for auto-registration.
    """

    def __init__(
        self,
        queue: Queue,
        store: WorkflowStore | None = None,
        task_registry: TaskRegistry | None = None,
        config: QueueProcessorConfig | None = None,
        handler_config: HandlerConfig | None = None,
        bulkhead_manager: TaskBulkheadManager | None = None,
        circuit_factory: WorkflowCircuitFactory | None = None,
    ) -> None:
        super().__init__(
            queue,
            config=config,
            store=store,
            handler_config=handler_config,
            task_registry=task_registry,
            bulkhead_manager=bulkhead_manager,
            circuit_factory=circuit_factory,
        )
        self._running = True

    def start(self) -> None:
        """No-op for synchronous processor."""
        pass

    def stop(self, wait: bool = True) -> None:
        """No-op for synchronous processor."""
        pass

    def push_and_process(self, message: Message) -> None:
        """
        Push a message and process it immediately.

        Args:
            message: The message to push and process
        """
        self.queue.push(message)
        self.process_all(timeout=5.0)
