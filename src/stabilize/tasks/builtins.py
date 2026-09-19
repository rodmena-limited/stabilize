"""Tasks the engine's own builders emit, seeded into every TaskRegistry.

``LoopBuilder`` and ``WaitStageBuilder`` construct stages naming these classes.
Without them registered, a workflow built by those documented APIs dies on
``TaskNotFoundError`` at its first step.

Seeded from ``TaskRegistry.__init__`` rather than the process-global default
registry: ``QueueProcessor`` takes an injected registry, and callers construct a
bare ``TaskRegistry()``, so seeding the global would reach nobody.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from stabilize.tasks.interface import NoOpTask, WaitTask
from stabilize.tasks.loop import LoopBackTask, LoopConditionTask, LoopEntryTask

if TYPE_CHECKING:
    from stabilize.tasks.interface import Task

BUILTIN_TASKS: dict[str, type[Task]] = {
    "NoOpTask": NoOpTask,
    "WaitTask": WaitTask,
    "LoopConditionTask": LoopConditionTask,
    "LoopBackTask": LoopBackTask,
    "LoopEntryTask": LoopEntryTask,
}

BUILTIN_ALIASES: dict[str, str] = {
    "noop": "NoOpTask",
    "wait": "WaitTask",
}
