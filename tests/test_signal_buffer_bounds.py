"""Bounds on the WCP-24 persistent-signal buffer (issuedb #15).

Reported by ci-conductor: 73,608 buffered signals in a single 3.5 MB
stage_executions.context row, 99.997% of them on CANCELED or TERMINAL stages
that can never consume them.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from stabilize import (
    Orchestrator,
    QueueProcessor,
    StageExecution,
    Task,
    TaskRegistry,
    TaskResult,
)
from stabilize.handlers.signal_refusal import (
    MAX_EMISSION_GAP,
    RefusalTracker,
    _is_emission_point,
)
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.signal_scope import (
    UnknownStatusError,
    complete_status_names,
    signal_status_filter,
)
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import SignalStage
from stabilize.resilience.config import HandlerConfig

SIBLING_CONTEXT = {
    "env": {"CI": "true"},
    "head_sha": "0" * 40,
    "job_id": "job-42",
    "script": "make test",
    "slug": "rodmena-limited/example",
    "secrets": ["NPM_TOKEN", "DEPLOY_KEY", "SENTRY_DSN"],
    "exception": None,
}


class NoopTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"ok": True})


class SuspendTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        if stage.context.get("_signal_data") is not None:
            return TaskResult.success(outputs={"resumed": True})
        return TaskResult.suspend(context={"waiting_for": "signal"})


def _setup(repository: WorkflowStore, queue: Queue, task_name: str, task_cls: type):
    registry = TaskRegistry()
    registry.register(task_name, task_cls)
    return QueueProcessor(queue, store=repository, task_registry=registry), Orchestrator(queue)


def _workflow(name: str, task_name: str) -> Workflow:
    return Workflow.create(
        application="signal-bounds",
        name=name,
        stages=[
            StageExecution(
                ref_id="only",
                name="Only",
                context=dict(SIBLING_CONTEXT),
                tasks=[TaskExecution.create("T", task_name, stage_start=True, stage_end=True)],
            )
        ],
    )


def _blast(queue: Queue, wf: Workflow, stage: StageExecution, n: int) -> None:
    for _ in range(n):
        queue.push(
            SignalStage(
                execution_type=wf.type.value,
                execution_id=wf.id,
                stage_id=stage.id,
                signal_name="ci.resume",
                signal_data={},
                persistent=True,
            )
        )


class TestCompleteStageRefusesSignals:
    def test_completed_stage_buffers_nothing(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        processor, runner = _setup(repository, queue, "noop", NoopTask)
        wf = _workflow("terminal-refusal", "noop")
        repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        stage = repository.retrieve(wf.id).stage_by_ref_id("only")
        assert stage.status == WorkflowStatus.SUCCEEDED

        _blast(queue, wf, stage, 200)
        processor.process_all(timeout=120.0)

        stage = repository.retrieve(wf.id).stage_by_ref_id("only")
        assert stage.context.get("_buffered_signals", []) == []
        for key, value in SIBLING_CONTEXT.items():
            assert stage.context[key] == value


class TestBufferCap:
    def test_cap_is_enforced_and_overflow_dead_lettered(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        cap = 20
        sent = 100
        config = replace(HandlerConfig.from_env(), signal_buffer_max=cap)
        assert config.signal_buffer_max == cap

        registry = TaskRegistry()
        registry.register("suspend", SuspendTask)
        processor = QueueProcessor(
            queue, store=repository, task_registry=registry, handler_config=config
        )
        runner = Orchestrator(queue)

        wf = _workflow("cap", "suspend")
        repository.store(wf)
        runner.start(wf)
        processor.process_one()

        dlq_before = queue.dlq_size()

        stage = repository.retrieve(wf.id).stage_by_ref_id("only")
        _blast(queue, wf, stage, sent)
        processor.process_all(timeout=180.0)

        signal_handler = processor._handlers[SignalStage]
        assert signal_handler.handler_config.signal_buffer_max == cap, (
            "injected HandlerConfig never reached SignalStageHandler"
        )

        stage = repository.retrieve(wf.id).stage_by_ref_id("only")
        assert len(stage.context.get("_buffered_signals", [])) <= cap
        assert queue.dlq_size() - dlq_before == sent - cap

    def test_buffered_signal_still_resumes_a_suspended_stage(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        processor, runner = _setup(repository, queue, "suspend", SuspendTask)
        wf = _workflow("drain", "suspend")
        repository.store(wf)
        runner.start(wf)
        processor.process_one()

        stage = repository.retrieve(wf.id).stage_by_ref_id("only")
        _blast(queue, wf, stage, 1)
        processor.process_all(timeout=60.0)

        stage = repository.retrieve(wf.id).stage_by_ref_id("only")
        assert stage.status == WorkflowStatus.SUCCEEDED


class TestBufferReclamation:
    def test_cleanup_strips_stranded_buffers_and_keeps_siblings(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        processor, runner = _setup(repository, queue, "noop", NoopTask)
        wf = _workflow("reclaim", "noop")
        repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        stage = repository.retrieve(wf.id).stage_by_ref_id("only")
        stage.context["_buffered_signals"] = [
            {"signal_name": "ci.resume", "signal_data": {}} for _ in range(500)
        ]
        repository.store_stage(stage)

        seeded = repository.retrieve(wf.id).stage_by_ref_id("only")
        assert len(seeded.context["_buffered_signals"]) == 500

        rows = repository.cleanup_buffered_signals(only_complete=True)
        assert rows == 1

        after = repository.retrieve(wf.id).stage_by_ref_id("only")
        assert "_buffered_signals" not in after.context
        for key, value in SIBLING_CONTEXT.items():
            assert after.context[key] == value

    def test_cleanup_spares_live_stages_by_default(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        processor, runner = _setup(repository, queue, "suspend", SuspendTask)
        wf = _workflow("spare-live", "suspend")
        repository.store(wf)
        runner.start(wf)
        processor.process_one()

        stage = repository.retrieve(wf.id).stage_by_ref_id("only")
        assert not stage.status.is_complete
        stage.context["_buffered_signals"] = [{"signal_name": "x", "signal_data": {}}]
        repository.store_stage(stage)

        rows = repository.cleanup_buffered_signals(only_complete=True)
        assert rows == 0

        after = repository.retrieve(wf.id).stage_by_ref_id("only")
        assert len(after.context["_buffered_signals"]) == 1

        assert repository.cleanup_buffered_signals(only_complete=False) == 1
        assert "_buffered_signals" not in repository.retrieve(wf.id).stage_by_ref_id("only").context


class TestStatusScoping:
    def test_status_filter_restricts_to_named_statuses(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        processor, runner = _setup(repository, queue, "noop", NoopTask)
        wf = _workflow("status-scope", "noop")
        repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=30.0)

        stage = repository.retrieve(wf.id).stage_by_ref_id("only")
        assert stage.status == WorkflowStatus.SUCCEEDED
        stage.context["_buffered_signals"] = [{"signal_name": "x", "signal_data": {}}]
        repository.store_stage(stage)

        assert repository.count_buffered_signal_stages(statuses=["SUCCEEDED"]) == 1
        assert repository.count_buffered_signal_stages(statuses=["CANCELED"]) == 0

        assert repository.cleanup_buffered_signals(statuses=["CANCELED"]) == 0
        assert "_buffered_signals" in repository.retrieve(wf.id).stage_by_ref_id("only").context

        assert repository.cleanup_buffered_signals(statuses=["SUCCEEDED"]) == 1
        after = repository.retrieve(wf.id).stage_by_ref_id("only")
        assert "_buffered_signals" not in after.context
        for key, value in SIBLING_CONTEXT.items():
            assert after.context[key] == value

    def test_unknown_status_raises_rather_than_cleaning_nothing(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        with pytest.raises(UnknownStatusError, match="SUCEEDED"):
            repository.cleanup_buffered_signals(statuses=["SUCEEDED"])
        with pytest.raises(UnknownStatusError):
            repository.count_buffered_signal_stages(statuses=["nonsense"])

    def test_filter_resolution(self) -> None:
        assert signal_status_filter(only_complete=False) is None
        assert signal_status_filter(only_complete=True) == complete_status_names()
        assert signal_status_filter(only_complete=True, statuses=["RUNNING"]) == ["RUNNING"]
        assert "RUNNING" not in complete_status_names()
        assert "SUCCEEDED" in complete_status_names()


class TestRefusalEmissionRate:
    def test_first_refusal_emits_then_decade_boundaries(self) -> None:
        tracker = RefusalTracker()
        emitted = [
            count
            for _ in range(100000)
            for count, emit in [tracker.record("exec-1", "stage-a")]
            if emit
        ]
        assert emitted == [1, 10, 100, 1000, 10000, 100000]

    def test_ci_incident_volume_costs_six_lines(self) -> None:
        assert sum(1 for n in range(1, 112902) if _is_emission_point(n)) == 6

    def test_emission_gap_is_bounded_so_a_live_producer_stays_visible(self) -> None:
        points = [n for n in range(1, 3_000_001) if _is_emission_point(n)]
        gaps = [b - a for a, b in zip(points, points[1:], strict=False)]
        assert max(gaps) == MAX_EMISSION_GAP
        assert points[:7] == [1, 10, 100, 1000, 10000, 100000, 200000]

    def test_emission_count_stays_bounded_at_high_volume(self) -> None:
        assert sum(1 for n in range(1, 10_000_001) if _is_emission_point(n)) == 105

    def test_stages_are_counted_independently(self) -> None:
        tracker = RefusalTracker()
        assert tracker.record("exec-1", "stage-a") == (1, True)
        assert tracker.record("exec-1", "stage-b") == (1, True)
        assert tracker.record("exec-2", "stage-a") == (1, True)
        assert tracker.record("exec-1", "stage-a") == (2, False)

    def test_tracker_is_bounded(self) -> None:
        tracker = RefusalTracker(max_tracked=8)
        for i in range(100):
            tracker.record(f"exec-{i}", "stage")
        assert len(tracker._counts) == 8


class TestConfig:
    def test_buffer_max_defaults_to_1000(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("STABILIZE_SIGNAL_BUFFER_MAX", raising=False)
        assert HandlerConfig.from_env().signal_buffer_max == 1000

    def test_buffer_max_reads_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("STABILIZE_SIGNAL_BUFFER_MAX", "25")
        assert HandlerConfig.from_env().signal_buffer_max == 25
