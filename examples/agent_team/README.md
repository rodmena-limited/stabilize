# Multi-Agent Software Team

A complex agentic workflow that stresses nearly every stabilize capability. An
**architect** agent designs a small Python library; parallel **coder** agents
each implement a module; their outputs are gathered with a declarative fan-in
**reducer**; a smoke **test** runs; a **reviewer** agent loops back to the
coders on failure (a bounded `jump_to` retry loop); a human-in-the-loop
**approval** gate suspends until approved; then a **packager** writes the
README. Progress **streams** live to the terminal, every step is
**event-sourced**, and a `--chaos` run proves **durable recovery** by
`SIGKILL`-ing the worker mid-run and resuming to completion.

## What it exercises

| Capability | Where |
|---|---|
| Parallel fan-out (AND-join) | `architect → [code_0, code_1] → gather` |
| Declarative fan-in reducers (B3) | `gather` uses `output_reducers={"written": "collect"}` |
| Dynamic routing / retry loop | reviewer `TaskResult.jump_to("code_0", ...)` on test failure |
| Durable suspend + HITL approval (B2) | `ApprovalTask` gate + `approve()` |
| Live streaming (B1) | `WorkflowStream` + `emit_progress()` |
| LLM toolkit (B4) | `LLMClient` against ollama.com cloud `glm-5.2` |
| Event sourcing + replay equivalence | post-run `EventReplayer` audit |
| Atomic durability + crash recovery | `--chaos`: kill worker, `recover_on_startup` |

## Run it

```bash
# Optional: real cloud model (glm-5.2 on ollama.com). Without it, the tasks
# fall back to offline stubs so the ENGINE still runs end to end.
export OLLAMA_API_KEY=...

# Normal run
python examples/agent_team/main.py

# Chaos run: SIGKILL the worker mid-flight, then recover to completion
python examples/agent_team/main.py --chaos
```

The generated library lands in `/tmp/stabilize-agent-team/` (override with
`AGENT_TEAM_DIR`). Change the target with `AGENT_TEAM_GOAL`, the model with
`AGENT_TEAM_MODEL`.

## Expected output (abridged)

```
=== Multi-Agent Software Team on stabilize (normal) ===
    · [architect] Architect: 2 modules planned
    · [coder:core] Coder: wrote /tmp/stabilize-agent-team/mylib/core.py
    · [coder:helpers] Coder: wrote /tmp/stabilize-agent-team/mylib/helpers.py
    · [tester] Tester: PASS
    · [reviewer] Reviewer: attempt 1, tests_passed=True
    ✋ approval gate suspended — auto-approving
    ▸ workflow.completed
=== Post-run audit ===
  workflow status: SUCCEEDED
  replayed status: SUCCEEDED       # event-sourced rebuild == live state
  generated library tests: PASS
  RESULT: PASS ✅
```

## Security

The ollama.com API key is read only from the `OLLAMA_API_KEY` environment
variable — it is never written to disk or committed. The example runs fully
on a laptop.

## Files

- `main.py` — workflow assembly, streaming, auto-approver, chaos harness, audit.
- `team_tasks.py` — the agent tasks (architect / coder / tester / reviewer /
  packager), each a small `Task` that calls the model via the LLM toolkit and
  emits progress.
