Agentic Workflows
=================

Stabilize ships a small set of **opt-in** building blocks for LLM-agent
systems. They are additive — existing workflows are unaffected — and layer on
top of the engine's durable, event-sourced execution so agent runs inherit
crash recovery, replay, and the full control-flow pattern set.

Four capabilities are covered here:

*  **Streaming** — consume a workflow's progress (and LLM tokens) live.
*  **Human-in-the-loop (HITL)** — pause for an approval and resume with a value.
*  **Fan-in reducers** — combine parallel branches instead of clobbering them.
*  **LLM toolkit** — a chat client, tool-calling, and a ReAct agent loop.

A complete, runnable program that uses all four (a multi-agent "software team"
that designs, writes, tests, reviews, and packages a small library, with a
chaos-recovery mode) lives in ``examples/agent_team/``.

Streaming Progress
------------------

Tasks can emit custom progress events, and a caller can consume them live via
:class:`~stabilize.WorkflowStream`. This is built on the event bus, so it works
whether or not durable event sourcing is configured.

.. code-block:: python

   from stabilize import WorkflowStream, emit_progress

   # Inside a task's execute():
   def execute(self, stage):
       emit_progress(stage, "generating candidate", percent=10)
       ...
       emit_progress(stage, "done", percent=100)
       return TaskResult.success(outputs={...})

   # In the caller — callback form (non-blocking):
   stream = WorkflowStream(execution_id)
   stream.on_event(lambda item: print(item.data.get("message", "")))
   # ... run the workflow ...
   stream.close()

   # Or iterate live until the workflow ends:
   stream = WorkflowStream(execution_id, event_store=event_store)
   for item in stream.follow(include_history=True, timeout=30):
       print(item.source, item.event_type, item.data)

Each item is a :class:`~stabilize.StreamItem` with ``event_type``,
``entity_id``, ``workflow_id``, ``data``, ``sequence``, and ``source``
(``"replay"`` or ``"live"``). Progress events carry ``event_type ==
"custom.progress"``; lifecycle events (``stage.completed``,
``workflow.completed``, …) flow through the same stream.

Human-in-the-Loop Approvals
---------------------------

The :class:`~stabilize.ApprovalTask` suspends a stage until an approval signal
arrives, then routes on it. It wraps the engine's durable
``TaskResult.suspend()`` plus persistent signal buffering, so an approval sent
*before* the stage suspends is not lost, and the whole gate survives a crash.

.. code-block:: python

   from stabilize import ApprovalTask, approve, reject

   registry.register("approval", ApprovalTask)

   gate = StageExecution(
       ref_id="gate",
       type="approval",
       requisite_stage_ref_ids={"prepare"},
       tasks=[TaskExecution.create(
           name="wait", implementing_class="approval",
           stage_start=True, stage_end=True,
       )],
   )

   # ... the stage runs, suspends, and waits ...

   # A human (or another system) approves — the payload lands in the stage's
   # outputs under "approval":
   approve(queue, execution_id, gate.id, {"user": "alice"})
   # or: reject(queue, execution_id, gate.id, {"reason": "needs work"})

By default a rejection is terminal; set
``context["approval_reject_continues"] = True`` to continue the pipeline
(``FAILED_CONTINUE``) instead. For custom suspend/resume tasks, use
:func:`~stabilize.send_signal` to deliver an arbitrary signal and
:func:`~stabilize.get_signal` inside ``execute`` to read which signal woke the
stage.

Declarative Fan-in Reducers
---------------------------

When several parallel branches write the **same** output key, the default
ancestor-output merge is last-write-wins for scalar values (list values
already concatenate). Set ``output_reducers`` on a join stage to combine the
per-branch values declaratively instead of losing all but one.

.. code-block:: python

   from stabilize import StageExecution, JoinType

   gather = StageExecution(
       ref_id="gather",
       join_type=JoinType.AND,
       requisite_stage_ref_ids={"gen_1", "gen_2", "gen_3"},
       output_reducers={"candidate": "collect", "score": "sum"},
       tasks=[...],
   )

Each branch that produced ``candidate`` contributes to a list under that key;
``score`` values are summed. Built-in reducers: ``collect`` / ``append``,
``extend``, ``sum``, ``max``, ``min``, ``merge``, ``first``, ``last``. Register
your own with :func:`~stabilize.register_reducer`. Keys without a reducer keep
the existing last-write-wins behavior, so this is fully backward compatible.

LLM Toolkit
-----------

``stabilize.llm`` is a dependency-light (standard-library only) toolkit for
LLM agents. It is **not** imported by the core engine, so it adds no cost to
workflows that do not use it. The model client is injectable, targeting
OpenAI-compatible endpoints or Ollama (local or cloud).

.. code-block:: python

   from stabilize.llm import LLMClient, LLMTask, AgentLoopTask, tool, ToolRegistry

   # A chat client (credentials from OLLAMA_API_KEY / OPENAI_API_KEY by default)
   client = LLMClient(model="glm-5.2", base_url="https://ollama.com", api="ollama")

   # A one-shot completion as a workflow task:
   registry.register("llm", LLMTask(client=client))

   # A tool-calling ReAct agent as a single durable task:
   @tool
   def get_weather(city: str) -> str:
       "Return the weather for a city."
       return lookup(city)

   tools = ToolRegistry().add(get_weather)
   registry.register("agent", AgentLoopTask(client=client, tools=tools))

:class:`~stabilize.llm.AgentLoopTask` runs a bounded model → tool-calls →
results → model loop inside one task execution (capped by
``max_iterations``), so the whole agent turn is one durable, retryable unit of
work. The ``@tool`` decorator generates an OpenAI-style JSON schema from the
function signature and docstring; :class:`~stabilize.llm.ToolRegistry`
dispatches the model's tool calls back to your functions.

.. note::

   Provide API keys via environment variables (``OLLAMA_API_KEY`` /
   ``OPENAI_API_KEY``) — never hard-code them. The example in
   ``examples/agent_team/`` reads ``OLLAMA_API_KEY`` and falls back to offline
   stubs when it is unset, so it always runs end to end.

See Also
--------

*  :doc:`event_sourcing` — the durable log the streaming surface consumes.
*  :doc:`flow_control` — joins, suspend/resume signals, and dynamic routing
   that these features build on.
