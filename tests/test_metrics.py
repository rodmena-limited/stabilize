"""WI-9 slice 9d (A22, B45): the seven named metrics are emitted, OpenTelemetry carries them, tenants label them."""

from __future__ import annotations

from typing import Any

import pytest

from stabilize import StageExecution, TaskExecution, TaskResult, metrics
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import Task
from tests.audit_support import drive, make_processor, start

NAMED = {
    "workflow_count",
    "workflow_duration_seconds",
    "stage_duration_seconds",
    "task_duration_seconds",
    "active_workflows",
    "queue_depth",
    "queue_lag_seconds",
}


class Recorder(metrics.MetricsProvider):
    def __init__(self) -> None:
        self.seen: list[tuple[str, str, float, dict[str, str]]] = []

    def increment(self, name: str, value: float = 1.0, tags: dict[str, str] | None = None) -> None:
        self.seen.append(("counter", name, value, dict(tags or {})))

    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        self.seen.append(("gauge", name, value, dict(tags or {})))

    def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        self.seen.append(("histogram", name, value, dict(tags or {})))

    def by_name(self, name: str) -> list[tuple[str, str, float, dict[str, str]]]:
        return [entry for entry in self.seen if entry[1] == name]


class Quick(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"ok": True})


@pytest.fixture()
def recorder() -> Any:
    previous = metrics.get_metrics()
    recording = Recorder()
    metrics.set_metrics_provider(recording)
    yield recording
    metrics.set_metrics_provider(previous)


def test_configure_metrics_installs_the_opentelemetry_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    previous = metrics.get_metrics()
    try:
        monkeypatch.delenv("STABILIZE_LOG_METRICS", raising=False)
        metrics.configure_metrics()
        assert isinstance(metrics.get_metrics(), metrics.OpenTelemetryMetricsProvider), type(metrics.get_metrics())
        monkeypatch.setenv("STABILIZE_LOG_METRICS", "1")
        metrics.configure_metrics()
        assert isinstance(metrics.get_metrics(), metrics.LogMetricsProvider)
    finally:
        metrics.set_metrics_provider(previous)


def test_the_engine_emits_the_seven_named_metrics(repository: WorkflowStore, queue: Queue, recorder: Recorder) -> None:
    processor = make_processor(repository, queue, {"quick": Quick})
    stage = StageExecution(
        ref_id="s0",
        type="quicktype",
        name="S0",
        tasks=[TaskExecution.create("q", "quick", stage_start=True, stage_end=True)],
    )
    wf = start(repository, queue, [stage], application="metered")
    drive(processor, repository, wf.id, timeout=10, idle=1.0)
    assert repository.retrieve(wf.id).status == WorkflowStatus.SUCCEEDED

    names = {entry[1] for entry in recorder.seen}
    missing = {"workflow_count", "workflow_duration_seconds", "stage_duration_seconds", "task_duration_seconds"} - names
    assert not missing, f"never emitted: {sorted(missing)}; saw {sorted(names)}"
    [(kind, _, value, tags)] = recorder.by_name("workflow_count")
    assert (kind, value, tags.get("application")) == ("counter", 1.0, "metered")
    kind, _, value, tags = recorder.by_name("workflow_duration_seconds")[-1]
    assert kind == "histogram" and value >= 0 and tags.get("status") == "SUCCEEDED"
    kind, _, value, tags = recorder.by_name("stage_duration_seconds")[-1]
    assert (
        kind == "histogram"
        and value >= 0
        and tags.get("stage_type") == "quicktype"
        and tags.get("status") == "SUCCEEDED"
    )
    kind, _, value, tags = recorder.by_name("task_duration_seconds")[-1]
    assert kind == "histogram" and value >= 0 and tags.get("task_type") == "quick"

    processor.sample_metrics()
    gauges = {entry[1]: entry for entry in recorder.seen if entry[0] == "gauge"}
    assert {"active_workflows", "queue_depth", "queue_lag_seconds"} <= set(gauges), sorted(gauges)
    assert gauges["active_workflows"][2] == 0
    assert gauges["queue_depth"][2] == 0
    assert gauges["queue_lag_seconds"][2] == 0
    assert set(NAMED) <= {entry[1] for entry in recorder.seen}


def test_metrics_carry_the_tenant_when_one_is_bound(recorder: Recorder) -> None:
    from stabilize.persistence.tenancy import bind_tenant

    metrics.increment("workflow_count", application="a")
    with bind_tenant("acme"):
        metrics.increment("workflow_count", application="a")
        metrics.gauge("queue_depth", 3)
    tags = [entry[3] for entry in recorder.seen]
    assert "tenant" not in tags[0], tags[0]
    assert tags[1].get("tenant") == "acme" and tags[1].get("application") == "a"
    assert tags[2].get("tenant") == "acme"


def test_the_opentelemetry_provider_reaches_the_sdk() -> None:
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    reader = InMemoryMetricReader()
    provider = metrics.OpenTelemetryMetricsProvider(meter_provider=MeterProvider(metric_readers=[reader]))
    provider.increment("workflow_count", 2, {"application": "a"})
    provider.histogram("stage_duration_seconds", 1.5, {"stage_type": "t"})
    provider.gauge("queue_depth", 7, {})
    data = reader.get_metrics_data()
    assert data is not None
    points: dict[str, list[Any]] = {}
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                points.setdefault(metric.name, []).extend(metric.data.data_points)
    assert set(points) == {"workflow_count", "stage_duration_seconds", "queue_depth"}, sorted(points)
    [count] = points["workflow_count"]
    assert count.value == 2 and dict(count.attributes) == {"application": "a"}
    [duration] = points["stage_duration_seconds"]
    assert duration.count == 1 and duration.sum == 1.5
    [depth] = points["queue_depth"]
    assert depth.value == 7
