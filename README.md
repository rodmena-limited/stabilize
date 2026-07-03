# Stabilize

Stabilize is a durable workflow engine for Python. You describe work as a
directed graph of stages, and the engine runs it — in parallel where the graph
allows, resuming exactly where it left off after a crash, and recording every
state transition for audit and replay. It runs embedded in your process on
SQLite, or against PostgreSQL for multi-node deployments; there is no separate
server or scheduler to operate.

The engine was built for reliable orchestration in general, and it turns out to
be a particularly good foundation for **agentic systems**: LLM agents are
long-running, make expensive external calls, loop, wait on humans, and must not
lose progress or repeat side effects when something fails. Those are exactly the
guarantees Stabilize provides. This document is organised around building
agentic workflows, from a single model call to a multi-agent system, but the
same primitives apply to any pipeline.

## Build with your coding agent

Stabilize ships its own complete reference for AI coding agents. If you work with
Claude Code, Cursor, Copilot, or any LLM coding assistant, you don't need to
teach it the API — point it at the built-in reference and describe what you want:

```bash
pip install stabilize
stabilize prompt > stabilize.md      # a self-contained guide, written for coding agents
```

Then ask your agent, for example:

> Read `stabilize.md`, then build a workflow where three agents research a
> question in parallel, their findings are gathered, a reviewer scores the
> result and loops back if confidence is low, and a human approves before it is
> saved. Stream progress as it runs.

The reference documents the whole API — DAG construction, the built-in tasks, the
control-flow patterns, and the agentic toolkit (tool-calling LLM agents,
human-in-the-loop approvals, live streaming, and fan-in reducers) — with runnable
templates. It is validated end to end: given only `stabilize prompt` and a
multi-agent task, a model generated a complete, correct workflow on the first
attempt and ran it — parallel tool-using agents, a quorum join, a durable
approval gate, and streaming, all from the reference alone.

The rest of this README walks through the same concepts by hand, so you
understand what your agent is building.

## Why Stabilize

**Durability is built in, not bolted on.** Every step commits the new workflow
state and the message that continues the workflow in a single database
transaction. A process that is killed mid-run leaves no half-written state and
no lost work: on restart, recovery re-queues precisely the work that was in
flight. For an agent that has already spent real time and money on model calls,
this is the difference between resuming and starting over.

**Control flow is expressive.** Beyond fan-out and fan-in, Stabilize implements
a large subset of the van der Aalst workflow patterns: proceed when *k of n*
branches finish, proceed on the *first* branch to finish, mutual exclusion,
milestones, deferred choice, and dynamic routing that lets a task jump back to
an earlier stage to form a bounded loop. Agent behaviours that are awkward to
express as a static graph — retry-until-good, race several strategies, gather a
quorum of results — map directly onto these primitives.

**Agents are first-class.** A small, dependency-light toolkit ships with the
engine: a provider-agnostic chat client, a tool-calling ReAct agent that runs as
a single durable task, human-in-the-loop approvals that survive restarts, live
progress streaming, and declarative reducers for combining parallel results.

**It stays out of your way.** Stabilize is a library. Import it, point it at a
SQLite file, and run. There is nothing to deploy.

## Installation

```bash
pip install stabilize            # SQLite backend
pip install stabilize[postgres]  # add PostgreSQL support
pip install stabilize[all]       # all optional features
```

Requires Python 3.11 or newer.

## Core concepts

A **workflow** is a set of **stages** connected by dependencies. Each stage runs
one or more **tasks** — the unit where your code executes. A task returns a
`TaskResult` (success, failure, suspend, or a jump to another stage) and may
read from and write to the stage's `context` and `outputs`; a stage's outputs
flow to its descendants.

Three objects run a workflow:

- a **store** (`SqliteWorkflowStore` / `PostgresWorkflowStore`) holds durable
  state;
- a **queue** (`SqliteQueue` / `PostgresQueue`) holds pending work; and
- a **`QueueProcessor`** polls the queue and dispatches messages to handlers,
  while an **`Orchestrator`** submits workflows to it.

Tasks are looked up by name through a `TaskRegistry`. You register the tasks your
workflow uses; the processor registers the built-in handlers for you.

## Building agentic workflows

The examples below use the built-in LLM toolkit with a cloud model served over
an Ollama-compatible endpoint. The client is provider-agnostic — set
`api="openai"` with a `base_url` for any OpenAI-compatible gateway. API keys are
read from the environment (`OLLAMA_API_KEY` or `OPENAI_API_KEY`); never hard-code
them.

### Simple: a single model call

The smallest useful agent is one stage that calls a model. `LLMTask` reads a
prompt from the stage context and writes the completion to the stage outputs.

```python
from stabilize import (
    Workflow, StageExecution, TaskExecution, TaskRegistry,
    SqliteWorkflowStore, SqliteQueue, QueueProcessor, Orchestrator,
)
from stabilize.llm import LLMClient, LLMTask

client = LLMClient(model="glm-5.2", base_url="https://ollama.com", api="ollama")

store = SqliteWorkflowStore("sqlite:///agent.db", create_tables=True)
queue = SqliteQueue("sqlite:///agent.db")
queue._create_table()

registry = TaskRegistry()
registry.register("llm", LLMTask(client=client))

workflow = Workflow.create(
    application="demo",
    name="One-shot answer",
    stages=[
        StageExecution(
            ref_id="answer",
            type="llm",
            name="Answer",
            context={"prompt": "In one sentence, what is a token bucket rate limiter?"},
            tasks=[TaskExecution.create(
                name="answer", implementing_class="llm",
                stage_start=True, stage_end=True,
            )],
        ),
    ],
)

processor = QueueProcessor(queue, store=store, task_registry=registry)
orchestrator = Orchestrator(queue, store=store)

store.store(workflow)
orchestrator.start(workflow)
processor.process_all(timeout=60)

result = store.retrieve(workflow.id)
print(result.stages[0].outputs["completion"])
```

`process_all` runs the workflow to completion synchronously, which is convenient
for scripts and tests. In a service you would call `processor.start()` and let
it run in the background.

### Mid-level: a tool-using agent with human approval

Real agents use tools and often need a human to sign off before an action takes
effect. `AgentLoopTask` runs a full ReAct loop — the model calls tools, reads the
results, and calls more tools until it produces an answer — inside a single
durable task. `ApprovalTask` suspends the workflow until an approval arrives; the
suspension is persisted, so the workflow can wait for minutes or days, and
survives a restart.

Tools are ordinary functions. The `@tool` decorator derives a JSON schema from
the signature and docstring so the model knows how to call them.

```python
from stabilize import (
    Workflow, StageExecution, TaskExecution, TaskRegistry,
    SqliteWorkflowStore, SqliteQueue, QueueProcessor, Orchestrator,
    ApprovalTask, approve, WorkflowStatus,
)
from stabilize.llm import LLMClient, AgentLoopTask, ToolRegistry, tool


@tool
def account_balance(user_id: str) -> str:
    """Return the current balance for a user id."""
    return lookup_balance(user_id)


client = LLMClient(model="glm-5.2", base_url="https://ollama.com", api="ollama")
tools = ToolRegistry().add(account_balance)

store = SqliteWorkflowStore("sqlite:///agent.db", create_tables=True)
queue = SqliteQueue("sqlite:///agent.db")
queue._create_table()

registry = TaskRegistry()
registry.register("agent", AgentLoopTask(client=client, tools=tools))
registry.register("approval", ApprovalTask)

workflow = Workflow.create(
    application="demo",
    name="Agent with approval",
    stages=[
        StageExecution(
            ref_id="agent", type="agent", name="Look up balance",
            context={"prompt": "What is user u_42's balance? Use the tool, then state it."},
            tasks=[TaskExecution.create(
                name="a", implementing_class="agent",
                stage_start=True, stage_end=True)],
        ),
        StageExecution(
            ref_id="approve", type="approval", name="Human sign-off",
            requisite_stage_ref_ids={"agent"},
            tasks=[TaskExecution.create(
                name="ap", implementing_class="approval",
                stage_start=True, stage_end=True)],
        ),
    ],
)

processor = QueueProcessor(queue, store=store, task_registry=registry)
orchestrator = Orchestrator(queue, store=store)

store.store(workflow)
orchestrator.start(workflow)

# Run until the approval gate suspends.
processor.process_all(timeout=90)

gate = next(s for s in store.retrieve(workflow.id).stages if s.ref_id == "approve")
assert gate.status == WorkflowStatus.SUSPENDED

# ... later, when a human decides ...
approve(queue, workflow.id, gate.id, {"user": "alice"})
processor.process_all(timeout=30)   # resumes and finishes
```

To watch the agent work as it runs, subscribe to the workflow's event stream.
Tasks emit progress with `emit_progress`, and lifecycle events are published
automatically:

```python
from stabilize import WorkflowStream

stream = WorkflowStream(workflow.id)
stream.on_event(lambda item: print(item.event_type, item.data.get("message", "")))
```

### Complex: a multi-agent research analyst

Larger agentic systems combine parallelism, quorums, races, loops, human gates,
and sub-workflows. The `examples/research_analyst/` project is a complete,
runnable example that does all of this: a planner decomposes a question; three
tool-using agents research the parts in parallel; the workflow proceeds once a
quorum of them finishes; two reviewers race and the first verdict wins; a router
loops back to refine when confidence is low; a human approves; and a child
workflow writes the report.

```
                 ┌── researcher 0 (ReAct + tools) ─┐
   plan ─────────┼── researcher 1 (ReAct + tools) ─┤  proceed on 2 of 3,
 (planner)  ▲    └── researcher 2 (ReAct + tools) ─┘  gather findings
            │                                             │
            │                                             ▼
            │                                        synthesize
            │                          ┌── review A ─┐        │
            │                          └── review B ─┘ first  ▼
            │                                    verdict → router
            └──────── loop back to refine ───────────┘  │ accept
                                                         ▼
                                          human approval → report (sub-workflow)
```

Each advanced behaviour is a property on a stage or a return value from a task.
A join that fires when a quorum finishes, gathering each branch's output into a
list:

```python
StageExecution(
    ref_id="synthesize",
    join_type=JoinType.N_OF_M,
    join_threshold=2,
    requisite_stage_ref_ids={"researcher_0", "researcher_1", "researcher_2"},
    output_reducers={"finding": "collect"},   # combine, don't overwrite
    tasks=[...],
)
```

A join that fires on the first upstream to complete:

```python
StageExecution(
    ref_id="router",
    join_type=JoinType.DISCRIMINATOR,
    requisite_stage_ref_ids={"review_a", "review_b"},
    tasks=[...],
)
```

A bounded loop — the router jumps back to an earlier stage to refine, carrying
feedback with it:

```python
class Router(Task):
    def execute(self, stage):
        if stage.context["confidence"] < 0.85 and passes_remaining(stage):
            return TaskResult.jump_to("plan", context={"feedback": "tighten the numbers"})
        return TaskResult.success()
```

A child workflow, run and awaited as a single stage, via `SubWorkflowTask`.

The example is verified end to end against a real model, including a `--chaos`
mode that kills the worker mid-run and lets recovery finish the job. See
`examples/research_analyst/README.md` for the full walkthrough and a feature map.
`examples/agent_team/` is a second complete example: a team of coding agents that
build and test a small library.

## Durability, and why it matters for agents

Each handler commits three things in one transaction: the updated stage or task
state, the message that advances the workflow, and a deduplication record. There
is no window in which state is saved but the next step is lost, or a step runs
twice. When a worker dies, `WorkflowRecovery` inspects durable state on startup
and re-queues exactly the work that was interrupted.

```python
from stabilize.queue.processor.config import QueueProcessorConfig

config = QueueProcessorConfig(recover_on_start=True)
processor = QueueProcessor(queue, config=config, store=store, task_registry=registry)
processor.start()   # interrupted workflows resume automatically
```

For an agent, "resume rather than restart" is not a nicety. A research run that
made forty tool calls and three model calls before the machine rebooted picks up
where it stopped, not from the beginning. Combined with the event log, you can
also replay a completed run to reconstruct its state at any point — useful for
debugging non-deterministic agent behaviour after the fact.

## Agentic building blocks

All of the following are additive and opt-in.

**LLM toolkit** (`stabilize.llm`). `LLMClient` speaks the OpenAI and Ollama chat
APIs. `LLMTask` is a one-shot call; `AgentLoopTask` is a bounded ReAct
tool-calling loop. `@tool` and `ToolRegistry` turn functions into model tools and
dispatch the model's calls back to them. The toolkit is standard-library only and
is never imported by the core engine.

**Human-in-the-loop** (`stabilize.hitl`). `ApprovalTask` plus `approve`, `reject`,
`send_signal`, and `get_signal` wrap the engine's durable suspend-and-signal
machinery. A gate can wait indefinitely and survives restarts.

**Streaming** (`stabilize.streaming`). `WorkflowStream` consumes a workflow's
events live or replays them from the durable log; `emit_progress` lets a task
push progress or token updates to any listener.

**Fan-in reducers** (`stabilize.reducers`). Set `output_reducers` on a join stage
to combine parallel branches — `collect`, `sum`, `merge`, or a function you
register — instead of the default last-writer-wins.

## Built-in tasks

Stabilize is not only for LLM work; it ships tasks for the operations pipelines
usually need, and you can add your own by subclassing `Task`.

- `ShellTask` — run a shell command with a timeout, environment, and working
  directory.
- `HTTPTask` — HTTP requests with JSON handling, authentication, file
  upload/download, retries, and SSRF guards.
- `PythonTask` — run a Python callable.
- `DockerTask`, `SSHTask` — containers and remote hosts.
- `SubWorkflowTask` — run a child workflow as a stage.

```python
from stabilize import ShellTask, HTTPTask

registry.register("shell", ShellTask)
registry.register("http", HTTPTask)

# Stage context configures the task:
{"command": "pytest -q", "cwd": "/app", "timeout": 300}
{"url": "https://api.example.com/data", "parse_json": True}
```

## General workflows

The same graph model expresses ordinary pipelines. Stages with a shared
dependency run in parallel; a stage waits for all of its requisites unless you
choose a different join.

```python
#     setup
#    /     \
#  test    lint
#    \     /
#    deploy

Workflow.create(application="ci", name="pipeline", stages=[
    StageExecution(ref_id="setup",  type="shell", name="Setup",  ...),
    StageExecution(ref_id="test",   type="shell", name="Test",   requisite_stage_ref_ids={"setup"}, ...),
    StageExecution(ref_id="lint",   type="shell", name="Lint",   requisite_stage_ref_ids={"setup"}, ...),
    StageExecution(ref_id="deploy", type="shell", name="Deploy", requisite_stage_ref_ids={"test", "lint"}, ...),
])
```

Tasks can preserve progress across transient-error retries by attaching a
`context_update`, which is merged into the stage context before the next attempt:

```python
raise TransientError("rate limited", retry_after=30,
                     context_update={"processed_items": done + 10})
```

## Persistence and operations

**SQLite** needs no setup; the schema is created and migrated in place. For
higher single-node concurrency, opt into WAL:

```bash
export STABILIZE_SQLITE_JOURNAL_MODE=WAL
```

**PostgreSQL** is applied with the CLI:

```bash
stabilize mg-up --db-url postgres://user:pass@host:5432/dbname
stabilize mg-status --db-url postgres://user:pass@host:5432/dbname
```

**Event sourcing** is one call, after which every transition is recorded:

```python
from stabilize.events import configure_event_sourcing, SqliteEventStore

configure_event_sourcing(SqliteEventStore("sqlite:///events.db", create_tables=True))
```

**Monitoring.** `stabilize monitor` opens a live terminal dashboard of running
workflows and queue depth.

## Documentation, examples, tests

- Guides: `docs/guide/` (getting started, tasks, data flow, flow control,
  persistence, resilience, event sourcing, agentic workflows).
- Runnable examples: `examples/` — including `research_analyst/` and
  `agent_team/` for complex agentic workflows, and smaller examples for shell,
  HTTP, SSH, Docker, event sourcing, and dynamic routing.

```bash
pytest tests/ -v            # full suite (PostgreSQL tests require Docker)
pytest tests/ -v -k sqlite  # SQLite only
```

## License

Apache 2.0
