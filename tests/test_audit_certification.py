"""
Audit certification suite — executable proof of the AUDIT.md claims.

Each test asserts one invariant that a confirmed audit finding restored, so
the certificate's "no known unresolved defects" claim is backed by running
code rather than prose. These are deliberately high-level cross-checks; the
per-fix regression tests live in their own files (referenced in AUDIT.md).
"""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from typing import Any

from stabilize.persistence.connection import ConnectionManager, SingletonMeta
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.queue.messages import StartWorkflow
from stabilize.queue.processor import QueueProcessor
from stabilize.queue.sqlite import SqliteQueue


def _env(tmp_path: Any, name: str):
    SingletonMeta.reset(ConnectionManager)
    url = f"sqlite:///{tmp_path}/{name}.db"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url)
    queue._create_table()
    return store, queue


class TestCertifiedInvariants:
    """One assertion per audit-finding class; details in the dedicated tests."""

    def test_dedup_survives_restart(self, tmp_path: Any) -> None:
        from stabilize.queue.dedup import reset_deduplicator

        store, queue = _env(tmp_path, "cert_dedup")
        try:
            store.mark_message_processed("m1", handler_type="X", execution_id="e1")
            reset_deduplicator()  # restart
            processor = QueueProcessor(queue, store=store)
            calls: list[Any] = []
            processor.register_handler_func(StartWorkflow, lambda m: calls.append(m))
            processor._handle_message(
                StartWorkflow(execution_type="PIPELINE", execution_id="e1", message_id="m1")
            )
            assert calls == []  # already-processed message not re-executed
        finally:
            reset_deduplicator()
            SingletonMeta.reset(ConnectionManager)

    def test_lock_heartbeat_prevents_double_delivery(self, tmp_path: Any) -> None:
        SingletonMeta.reset(ConnectionManager)
        try:
            queue = SqliteQueue(f"sqlite:///{tmp_path}/cert_hb.db", lock_duration=timedelta(seconds=0.3))
            queue._create_table()
            processor = QueueProcessor(queue)
            invocations: list[float] = []

            def slow(m: Any) -> None:
                invocations.append(time.monotonic())
                time.sleep(1.0)

            processor.register_handler_func(StartWorkflow, slow)
            queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="e1"))
            processor.start()
            time.sleep(1.6)
            processor.stop()
            assert len(invocations) == 1
        finally:
            SingletonMeta.reset(ConnectionManager)

    def test_mutex_serializes_siblings(self, tmp_path: Any) -> None:
        # The atomic claim primitive underpinning mutex/deferred-choice.
        store, queue = _env(tmp_path, "cert_mutex")
        try:
            with store.transaction(queue) as txn:
                first = txn.acquire_claim("e1", "mutex:db", "stage_a")
                second = txn.acquire_claim("e1", "mutex:db", "stage_b")
            assert first is True and second is False
        finally:
            SingletonMeta.reset(ConnectionManager)

    def test_finalizer_timeout_enforced(self) -> None:
        from stabilize.finalizers import FinalizerRegistry

        registry = FinalizerRegistry()
        hang = threading.Event()
        registry.register("s", "hung", lambda: hang.wait(10.0))
        start = time.monotonic()
        results = registry.execute("s", timeout=0.2)
        try:
            assert time.monotonic() - start < 2.0
            assert results[0].success is False
        finally:
            hang.set()

    def test_engine_errors_caught_by_public_base(self) -> None:
        import stabilize
        from stabilize.errors import ConcurrencyError, TransientError

        assert isinstance(TransientError("t"), stabilize.StabilizeError)
        assert isinstance(ConcurrencyError("c"), stabilize.StabilizeError)

    def test_non_primitive_outputs_persist(self, tmp_path: Any) -> None:
        from datetime import UTC, datetime

        from stabilize.models.stage import StageExecution
        from stabilize.models.workflow import Workflow

        store, _ = _env(tmp_path, "cert_ser")
        try:
            stage = StageExecution(ref_id="s", name="S", tasks=[])
            wf = Workflow.create(application="t", name="w", stages=[stage])
            store.store(wf)
            stage.outputs = {"when": datetime(2026, 7, 1, tzinfo=UTC)}
            store.store_stage(stage)  # must not raise
            assert isinstance(store.retrieve(wf.id).stages[0].outputs["when"], str)
        finally:
            SingletonMeta.reset(ConnectionManager)

    def test_agentic_features_importable_from_facade(self) -> None:
        import stabilize

        for name in (
            "WorkflowStream",
            "emit_progress",
            "ApprovalTask",
            "approve",
            "reject",
            "register_reducer",
            "apply_output_reducers",
        ):
            assert hasattr(stabilize, name), f"missing agentic export: {name}"

        from stabilize.llm import AgentLoopTask, LLMClient, ToolRegistry, tool  # noqa: F401
