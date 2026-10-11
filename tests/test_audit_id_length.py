from __future__ import annotations

from stabilize import Workflow
from stabilize.persistence.store import WorkflowStore
from tests.audit_support import stage


def test_over_long_workflow_id_is_refused_before_any_write(repository: WorkflowStore) -> None:
    wf = Workflow.create(application="test", name="ids", stages=[stage("s", ["x"])])
    wf.id = "W" * 27
    try:
        repository.store(wf)
    except ValueError as exc:
        assert "longer than 26" in str(exc)
    except Exception as exc:
        raise AssertionError(f"C26 over-long id refused by the database, not validated: {type(exc).__name__}") from exc
    else:
        raise AssertionError("C26 over-long id stored without error")
    assert not repository.exists(wf.id)


def test_control_ulid_ids_are_accepted(repository: WorkflowStore) -> None:
    wf = Workflow.create(application="test", name="ids", stages=[stage("s", ["x"])])
    repository.store(wf)
    assert repository.exists(wf.id)
