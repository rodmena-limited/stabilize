"""Settling a deferred choice when its members complete (claim mode on_complete)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from stabilize.dag.deferred_choice import ON_COMPLETE, claim_mode
from stabilize.models.status import CONTINUABLE_STATUSES, WorkflowStatus

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow
    from stabilize.persistence.store.transaction import StoreTransaction

FAILED_OWNER = "<failed>"
Outcome = Literal["winner", "loser", "failed_loser", "last_failed"]


def settle_key(group: str) -> str:
    return f"choice:{group}"


def settle_row_key(group: str, stage_id: str) -> str:
    return f"{settle_key(group)}:{stage_id}"


def settle_lock(execution_id: str, group: str) -> str:
    return f"choice\x1f{execution_id}\x1f{group}"


def settles_on_complete(stage: StageExecution) -> bool:
    return bool(stage.deferred_choice_group) and stage.parent_stage_id is None and claim_mode(stage) == ON_COMPLETE


def group_members(execution: Workflow, stage: StageExecution) -> list[StageExecution]:
    return [
        s
        for s in execution.stages
        if s.deferred_choice_group == stage.deferred_choice_group and s.parent_stage_id is None
    ]


def record_settle_row(txn: StoreTransaction, execution_id: str, stage: StageExecution) -> None:
    """Record that *stage* has settled (completed, skipped or cancelled) its deferred choice."""
    if stage.deferred_choice_group:
        txn.acquire_claim(execution_id, settle_row_key(stage.deferred_choice_group, stage.id), stage.id)


@dataclass
class Settlement:
    outcome: Outcome
    live_siblings: list[StageExecution] = field(default_factory=list)


def settle(txn: StoreTransaction, stage: StageExecution, status: WorkflowStatus, execution: Workflow) -> Settlement:
    """Decide this member's fate under the group's lock; the lock must be the transaction's first statement."""
    group = stage.deferred_choice_group or ""
    key = settle_key(group)
    txn.lock_admission(settle_lock(execution.id, group))
    members = group_members(execution, stage)
    txn.acquire_claim(execution.id, settle_row_key(group, stage.id), stage.id)
    owner = txn.claim_owner(execution.id, key)
    if owner == stage.id:
        return Settlement("winner", [m for m in members if m.id != stage.id and not m.status.is_complete])
    if owner is not None:
        return Settlement("loser")
    if status in CONTINUABLE_STATUSES:
        txn.acquire_claim(execution.id, key, stage.id)
        return Settlement("winner", [m for m in members if m.id != stage.id and not m.status.is_complete])
    if txn.count_claims(execution.id, f"{key}:") < len(members):
        return Settlement("failed_loser")
    txn.acquire_claim(execution.id, key, FAILED_OWNER)
    return Settlement("last_failed")
