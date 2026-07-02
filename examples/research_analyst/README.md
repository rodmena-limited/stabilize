# Autonomous Research Analyst

A deliberately **complex** agentic workflow built on stabilize, designed to
exercise a *superset* of the features LangGraph is known for — and several
stabilize has that LangGraph does not. It answers a research question by
decomposing it, researching in parallel with real tool-using agents,
cross-verifying, refining, and publishing — all durable and event-sourced.

Driven by a real LLM: **ollama.com cloud `glm-5.2`** via `OLLAMA_API_KEY`
(genuine tool-calling). Without a key it falls back to deterministic offline
stubs so the engine still runs end to end.

## The DAG

```
                 ┌── research_0 (ReAct + tools) ─┐
   plan ─────────┼── research_1 (ReAct + tools) ─┤  N-of-M (≥2 of 3)
 (planner)  ▲    └── research_2 (ReAct + tools) ─┘  + fan-in reducer
            │                                          │
            │                                          ▼
            │                                     synthesize (LLM)
            │                                          │
            │                          ┌── verify_a (correctness) ─┐
            │                          └── verify_b (cost) ────────┤ DISCRIMINATOR
            │                                                      │ (first verdict wins)
            │                                                      ▼
            └──────────── jump_to (refine, bounded) ──────────── route
                                                                   │ accept
                                                                   ▼
                                                approve (human-in-the-loop)
                                                                   │
                                                                   ▼
                                        report  ── SUB-WORKFLOW ──▶ draft ─▶ polish
```

## Feature parity with LangGraph (and beyond)

| LangGraph feature | This workflow | stabilize mechanism |
|---|---|---|
| `StateGraph` + typed state | the DAG + stage context/outputs | `StageExecution`, ancestor-output flow |
| Channel **reducers** (`operator.add`) | gather findings across branches | `output_reducers={"finding": "collect"}` |
| **Conditional edges** / routing | accept vs. refine decision | router task → `jump_to` or success |
| **Cycles** (agent loops) | refine loop back to the planner | `TaskResult.jump_to("plan", ...)`, bounded |
| `create_react_agent` (**tool-calling**) | 3 research agents call real tools | `AgentLoopTask` + `@tool`/`ToolRegistry` |
| **Send API** map-reduce | parallel researchers, gathered at join | parallel stages + reducer |
| **Human-in-the-loop** interrupt | approval gate before publishing | `ApprovalTask` (durable suspend/resume) |
| **Subgraphs** | report generation as a child DAG | `SubWorkflowTask` (draft → polish) |
| **Streaming** | live agent narration to the terminal | `WorkflowStream` + `emit_progress` |
| Checkpointing / persistence | every step commits atomically | store+queue transaction per step |
| Time-travel / replay | post-run replay equivalence check | event sourcing + `EventReplayer` |
| **Beyond LangGraph** ↓ | | |
| Proceed on **K of N** (not all) | synthesize starts at 2 of 3 researchers | `JoinType.N_OF_M`, `join_threshold=2` |
| Proceed on **first of many** | route fires on the first verifier | `JoinType.DISCRIMINATOR` |
| **Crash recovery** | `--chaos`: SIGKILL mid-run, resume | atomic durability + `recover_on_startup` |
| Partial-failure tolerance | a failed researcher doesn't sink the run | `continuePipelineOnFailure` + N-of-M |

LangGraph's persistence is checkpoint-based (state snapshots); stabilize commits
**state + the continuation message in one transaction per step**, so a killed
process resumes exactly where it stopped — demonstrated by `--chaos`.

## Run it

```bash
export OLLAMA_API_KEY=...          # optional; offline stubs otherwise
python examples/research_analyst/main.py            # normal run
python examples/research_analyst/main.py --chaos    # kill worker mid-run, recover
```

Tunable via env: `ANALYST_QUESTION`, `ANALYST_MODEL` (default `glm-5.2`),
`ANALYST_MIN_PASSES` (forced refine passes, default 1), `ANALYST_MAX_PASSES`,
`ANALYST_CONF_THRESHOLD`, `ANALYST_DIR`.

## What the post-run audit checks

- workflow reached `SUCCEEDED`
- how many planning passes ran (the cycle fired)
- how many researchers completed vs. the N-of-M quorum
- the discriminator winner's confidence
- **event-sourced replay reconstructs the same terminal state** (time-travel)
- the report file was written

## Files

- `tools.py` — deterministic ReAct tools (`calculate`, `knowledge_base`, `list_catalog`).
- `analyst_tasks.py` — planner, ReAct researchers, synthesizer, dual verifiers,
  cycle router, and the report sub-workflow tasks.
- `main.py` — DAG assembly, streaming, auto-approver, chaos harness, audit.
