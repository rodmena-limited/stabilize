from __future__ import annotations
from typing import TYPE_CHECKING
from stabilize.queue.messages import (
    CancelWorkflow,
    RestartStage,
    ResumeStage,
    StartWorkflow,
)

class Orchestrator:
    """
    Runner for pipeline executions.

    Provides methods to start, cancel, restart, and resume executions
    by pushing appropriate messages to the queue.
    """
    def __init__(self, queue: Queue) -> None:
        """
        Initialize the runner.

        Args:
            queue: The message queue
        """
        self.queue = queue
