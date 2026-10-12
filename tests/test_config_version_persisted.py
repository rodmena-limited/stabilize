from __future__ import annotations

from stabilize import Orchestrator, Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.resilience.config import HandlerConfig
from tests.audit_support import stage


def test_config_version_set_before_store_is_read_back(repository: WorkflowStore) -> None:
    wf = Workflow.create(application="cv", name="stored", stages=[stage("s", ["ok"])])
    wf.config_version = "fingerprint-0123"
    repository.store(wf)
    assert repository.retrieve(wf.id).config_version == "fingerprint-0123", "config_version lost by store()"


def test_orchestrator_start_persists_the_config_fingerprint(repository: WorkflowStore, queue: Queue) -> None:
    config = HandlerConfig(max_stage_wait_retries=7)
    wf = Workflow.create(application="cv", name="started", stages=[stage("s", ["ok"])])
    repository.store(wf)
    Orchestrator(queue, store=repository).start(wf, handler_config=config)
    stored = repository.retrieve(wf.id).config_version
    assert stored == config.config_fingerprint(), f"config_version lost after Orchestrator.start: {stored!r}"
