"""
Tests for the Phase-B agentic ergonomics features (all additive/opt-in):
streaming, HITL approvals, declarative fan-in reducers, and the LLM toolkit.
"""

from typing import Any

import pytest

from stabilize.models.stage import JoinType, StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue

# ============ B1: Streaming ============


class TestStreaming:
    def test_emit_progress_reaches_a_stream_subscriber(self) -> None:
        from stabilize.events import reset_event_bus
        from stabilize.streaming import WorkflowStream, emit_progress

        reset_event_bus()
        try:
            stage = StageExecution(ref_id="s", name="S", tasks=[])
            execution = Workflow.create(application="t", name="w", stages=[stage])

            seen: list[Any] = []
            stream = WorkflowStream(execution.id)
            stream.on_event(lambda item: seen.append(item))

            emit_progress(stage, "step 1", percent=10)
            emit_progress(stage, "step 2", percent=90)
            stream.close()

            assert [i.data["message"] for i in seen] == ["step 1", "step 2"]
            assert seen[0].data["percent"] == 10
            assert all(i.source == "live" for i in seen)
        finally:
            reset_event_bus()

    def test_stream_filters_by_workflow(self) -> None:
        from stabilize.events import reset_event_bus
        from stabilize.streaming import WorkflowStream, emit_progress

        reset_event_bus()
        try:
            stage_a = StageExecution(ref_id="a", name="A", tasks=[])
            stage_b = StageExecution(ref_id="b", name="B", tasks=[])
            wf_a = Workflow.create(application="t", name="a", stages=[stage_a])
            # Binds stage_b to a different workflow id than the stream watches.
            # The reference must stay alive: stage.execution is a weakref.
            _wf_b = Workflow.create(application="t", name="b", stages=[stage_b])

            seen: list[Any] = []
            stream = WorkflowStream(wf_a.id)
            stream.on_event(lambda item: seen.append(item))

            emit_progress(stage_b, "other workflow")
            emit_progress(stage_a, "mine")
            stream.close()

            assert [i.data["message"] for i in seen] == ["mine"]
        finally:
            reset_event_bus()


# ============ B2: HITL approvals ============


class TestApprovalTask:
    def _run(self, repository: WorkflowStore, queue: Queue, decision: str, extra_ctx: dict | None = None):
        from stabilize.hitl import ApprovalTask, approve, reject
        from tests.conftest import setup_stabilize

        processor, runner, _ = setup_stabilize(
            repository, queue, extra_tasks={"approval": ApprovalTask}
        )
        ctx = {"approval_reject_continues": False}
        ctx.update(extra_ctx or {})
        stage = StageExecution(
            ref_id="gate",
            type="approval",
            name="Approval Gate",
            context=ctx,
            tasks=[
                TaskExecution.create(
                    name="wait", implementing_class="approval", stage_start=True, stage_end=True
                )
            ],
        )
        execution = Workflow.create(application="t", name="approval flow", stages=[stage])
        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        # Stage should now be suspended, awaiting a decision.
        mid = repository.retrieve(execution.id)
        gate = next(s for s in mid.stages if s.ref_id == "gate")
        assert gate.status == WorkflowStatus.SUSPENDED, f"expected SUSPENDED, got {gate.status}"

        if decision == "approve":
            approve(queue, execution.id, gate.id, {"user": "alice"})
        else:
            reject(queue, execution.id, gate.id, {"user": "bob", "reason": "no"})
        processor.process_all(timeout=10.0)
        return repository.retrieve(execution.id)

    def test_approval_flow_succeeds_after_approve(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        result = self._run(repository, queue, "approve")
        gate = next(s for s in result.stages if s.ref_id == "gate")
        assert gate.status == WorkflowStatus.SUCCEEDED
        assert gate.outputs.get("approved") is True
        assert gate.outputs["approval"]["user"] == "alice"

    def test_approval_flow_fails_after_reject(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        result = self._run(repository, queue, "reject")
        gate = next(s for s in result.stages if s.ref_id == "gate")
        assert gate.status.is_failure or gate.status == WorkflowStatus.TERMINAL


# ============ B3: Reducers ============


class TestReducers:
    def test_apply_output_reducers_builtins(self) -> None:
        from stabilize.reducers import apply_output_reducers

        branches = [
            {"candidate": "a", "score": 3},
            {"candidate": "b", "score": 5},
            {"candidate": "c", "score": 2},
        ]
        result = apply_output_reducers(
            {"candidate": "collect", "score": "sum"}, branches
        )
        assert result["candidate"] == ["a", "b", "c"]
        assert result["score"] == 10

    def test_unknown_reducer_raises(self) -> None:
        from stabilize.reducers import apply_output_reducers

        with pytest.raises(ValueError):
            apply_output_reducers({"k": "no_such_reducer"}, [{"k": 1}])

    def test_custom_reducer(self) -> None:
        from stabilize.reducers import apply_output_reducers, register_reducer

        register_reducer("joinstr", lambda values: ",".join(str(v) for v in values))
        result = apply_output_reducers({"tag": "joinstr"}, [{"tag": "x"}, {"tag": "y"}])
        assert result["tag"] == "x,y"

    def test_parallel_branches_gather_via_reducer_end_to_end(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Two parallel branches writing the same scalar key are gathered into
        a list at the join instead of clobbering."""
        from stabilize.tasks.interface import Task
        from stabilize.tasks.result import TaskResult
        from tests.conftest import setup_stabilize

        class EmitTask(Task):
            def execute(self, stage: StageExecution) -> TaskResult:
                return TaskResult.success(outputs={"result": stage.context["value"]})

        class GatherTask(Task):
            def execute(self, stage: StageExecution) -> TaskResult:
                # 'result' has been reduced into a list by the engine.
                return TaskResult.success(outputs={"gathered": stage.context.get("result")})

        processor, runner, _ = setup_stabilize(
            repository, queue, extra_tasks={"emit": EmitTask, "gather": GatherTask}
        )

        def emit_stage(ref: str, value: str) -> StageExecution:
            return StageExecution(
                ref_id=ref,
                type="emit",
                name=ref,
                context={"value": value},
                tasks=[TaskExecution.create(name="e", implementing_class="emit", stage_start=True, stage_end=True)],
            )

        branch_a = emit_stage("a", "alpha")
        branch_b = emit_stage("b", "beta")
        join = StageExecution(
            ref_id="join",
            type="gather",
            name="join",
            requisite_stage_ref_ids={"a", "b"},
            join_type=JoinType.AND,
            output_reducers={"result": "collect"},
            tasks=[TaskExecution.create(name="g", implementing_class="gather", stage_start=True, stage_end=True)],
        )
        execution = Workflow.create(
            application="t", name="reduce flow", stages=[branch_a, branch_b, join]
        )
        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"status={result.status}"
        join_stage = next(s for s in result.stages if s.ref_id == "join")
        gathered = join_stage.outputs.get("gathered")
        assert sorted(gathered) == ["alpha", "beta"], (
            f"reducer did not gather both branches: {gathered}"
        )


# ============ B4: LLM toolkit ============


class TestLLMToolkit:
    def test_tool_schema_generation(self) -> None:
        from stabilize.llm import ToolRegistry, tool

        @tool
        def add(a: int, b: int) -> int:
            """Add two integers."""
            return a + b

        registry = ToolRegistry().add(add)
        schemas = registry.schemas()
        assert len(schemas) == 1
        fn = schemas[0]["function"]
        assert fn["name"] == "add"
        assert fn["description"] == "Add two integers."
        assert fn["parameters"]["properties"]["a"]["type"] == "integer"
        assert set(fn["parameters"]["required"]) == {"a", "b"}

    def test_tool_dispatch_openai_and_ollama_shapes(self) -> None:
        from stabilize.llm import ToolRegistry, tool

        @tool
        def echo(text: str) -> str:
            """Echo text."""
            return text.upper()

        registry = ToolRegistry().add(echo)
        openai_call = {"id": "1", "function": {"name": "echo", "arguments": '{"text": "hi"}'}}
        ollama_call = {"function": {"name": "echo", "arguments": {"text": "yo"}}}
        assert registry.dispatch(openai_call) == "HI"
        assert registry.dispatch(ollama_call) == "YO"

    def test_llm_task_with_fake_client(self) -> None:
        from stabilize.llm import ChatResponse, LLMTask

        class FakeClient:
            model = "fake"

            def chat(self, messages, tools=None, temperature=None, **opts):
                return ChatResponse(content="hello world", raw={"ok": True})

        stage = StageExecution(
            ref_id="s", type="llm", name="LLM", context={"prompt": "hi"}, tasks=[]
        )
        result = LLMTask(client=FakeClient()).execute(stage)
        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["completion"] == "hello world"

    def test_agent_loop_runs_tools_until_final_answer(self) -> None:
        from stabilize.llm import AgentLoopTask, ChatResponse, ToolRegistry, tool

        @tool
        def get_number() -> int:
            """Return the magic number."""
            return 42

        registry = ToolRegistry().add(get_number)

        class ScriptedClient:
            model = "fake"

            def __init__(self) -> None:
                self.turn = 0

            def chat(self, messages, tools=None, temperature=None, **opts):
                self.turn += 1
                if self.turn == 1:
                    return ChatResponse(
                        content="",
                        tool_calls=[{"id": "c1", "function": {"name": "get_number", "arguments": "{}"}}],
                    )
                return ChatResponse(content="The number is 42.")

        stage = StageExecution(
            ref_id="s", type="agent", name="Agent", context={"prompt": "what is the number?"}, tasks=[]
        )
        result = AgentLoopTask(client=ScriptedClient(), tools=registry).execute(stage)
        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["answer"] == "The number is 42."
        assert result.outputs["iterations"] == 2
        assert result.outputs["tool_invocations"][0]["tool"] == "get_number"
        assert result.outputs["tool_invocations"][0]["result"] == "42"

    def test_agent_loop_respects_iteration_cap(self) -> None:
        from stabilize.llm import AgentLoopTask, ChatResponse, ToolRegistry, tool

        @tool
        def loop_tool() -> str:
            """Always asks to be called again."""
            return "again"

        class AlwaysToolsClient:
            model = "fake"

            def chat(self, messages, tools=None, temperature=None, **opts):
                return ChatResponse(
                    content="",
                    tool_calls=[{"id": "x", "function": {"name": "loop_tool", "arguments": "{}"}}],
                )

        stage = StageExecution(
            ref_id="s",
            type="agent",
            name="Agent",
            context={"prompt": "go", "max_iterations": 3},
            tasks=[],
        )
        result = AgentLoopTask(client=AlwaysToolsClient(), tools=ToolRegistry().add(loop_tool)).execute(stage)
        assert result.outputs["iterations"] == 3
        assert result.status in (WorkflowStatus.FAILED_CONTINUE, WorkflowStatus.TERMINAL)
