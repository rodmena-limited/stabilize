"""Identifier limits of the persisted schema."""

from __future__ import annotations

import re
from typing import Any

MAX_ID_LENGTH = 26


ULID_RE = re.compile(r"^[0-7][0-9A-HJKMNP-TV-Z]{25}$")


def check_ids(execution: Any, tenant: str = "") -> None:
    """Refuse ids longer than the 26-character columns and, under a tenant, ids that are not ULIDs."""
    if tenant:
        ids = [("workflow", execution.id)]
        for stage in execution.stages:
            ids.append(("stage", stage.id))
            ids.extend(("task", t.id) for t in stage.tasks)
        bad = [(kind, value) for kind, value in ids if not ULID_RE.match(str(value))]
        if bad:
            kind, value = bad[0]
            raise ValueError(
                f"{kind} id {value!r} is not a ULID; under tenant {tenant!r} every id must be a 26-character ULID "
                f"so ids cannot collide across tenants ({len(bad)} id(s))"
            )
    too_long = [("workflow", execution.id)] if len(str(execution.id)) > MAX_ID_LENGTH else []
    for stage in execution.stages:
        if len(str(stage.id)) > MAX_ID_LENGTH:
            too_long.append(("stage", stage.id))
        too_long.extend(("task", t.id) for t in stage.tasks if len(str(t.id)) > MAX_ID_LENGTH)
    if too_long:
        kind, value = too_long[0]
        raise ValueError(
            f"{kind} id {value!r} is longer than {MAX_ID_LENGTH} characters ({len(too_long)} id(s) too long)"
        )
