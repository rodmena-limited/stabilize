from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class PredicatePhase(Enum):
    """Result of readiness evaluation.

    Determines what action to take for a stage based on upstream state.
    """

    READY = "READY"
    """All upstreams complete, proceed with execution."""

    NOT_READY = "NOT_READY"
    """Upstreams still running, re-queue for later."""

    SKIP = "SKIP"
    """Conditions not met (upstream halted), skip this stage."""

    PRUNE = "PRUNE"
    """Every incoming edge is dead; no token can reach this stage."""

    UNDEFINED = "UNDEFINED"
    """Error during evaluation or inconsistent state."""


@dataclass(frozen=True)
class ReadinessResult:
    """Result of readiness evaluation.

    Contains the phase decision and supporting information for
    logging, debugging, and downstream handling.

    Attributes:
        phase: The readiness decision
        reason: Human-readable explanation of the decision
        failed_upstream_ids: IDs of upstream stages that caused SKIP
        active_upstream_ids: IDs of upstream stages still running (NOT_READY)
    """

    phase: PredicatePhase
    reason: str = ""
    failed_upstream_ids: list[str] = field(default_factory=list)
    active_upstream_ids: list[str] = field(default_factory=list)
