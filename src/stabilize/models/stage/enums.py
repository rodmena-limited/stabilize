"""
Stage enums and helpers.

Provides the enum types used by StageExecution and a helper
for generating unique stage IDs.
"""

from enum import Enum


def _generate_stage_id() -> str:
    """Generate a unique stage ID using ULID."""
    from ulid import ULID

    return str(ULID())


class SyntheticStageOwner(Enum):
    """
    Indicates the relationship of a synthetic stage to its parent.

    STAGE_BEFORE: Runs before the parent's tasks
    STAGE_AFTER: Runs after the parent completes
    """

    STAGE_BEFORE = "STAGE_BEFORE"
    STAGE_AFTER = "STAGE_AFTER"


class JoinType(Enum):
    """Join semantics for stages with multiple upstream dependencies.

    AND: Wait for ALL upstreams (default, WCP-3).
    OR: Wait only for activated branches from a paired OR-split (WCP-7).
    MULTI_MERGE: Fire once per upstream completion, no sync (WCP-8).
    DISCRIMINATOR: Fire on first upstream completion, ignore rest (WCP-9).
    N_OF_M: Fire when N of M upstreams complete (WCP-30).
    """

    AND = "AND"
    OR = "OR"
    MULTI_MERGE = "MULTI_MERGE"
    DISCRIMINATOR = "DISCRIMINATOR"
    N_OF_M = "N_OF_M"


class SplitType(Enum):
    """Split semantics for stages with multiple downstream branches.

    AND: Activate ALL downstream stages (default, WCP-2).
    OR: Evaluate conditions per downstream, activate matching ones (WCP-6).
    """

    AND = "AND"
    OR = "OR"
