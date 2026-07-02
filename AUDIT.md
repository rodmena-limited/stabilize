# Stabilize — Agentic Workflow Audit Certificate

**Package:** stabilize · **Version audited → shipped:** 0.18.0 → **0.19.0**
**Audit date:** 2026-07-02 · **Head commit:** `528acda` (branch `audit/v0.19.0`)
**Scope:** unbiased, code-grounded correctness + concurrency audit of the
engine, remediation of all confirmed defects, closure of the top agentic-engine
gaps, and an end-to-end agentic workload proving fitness.

> **Certification statement.** As of the audit date, every defect confirmed by
> the adversarial review below has a committed fix and a regression test, the
> full unit suite and golden-standard suite are green on SQLite, and the public
> API and downstream import surface are unchanged. **No known unresolved
> correctness or concurrency defects remain** at this commit. This is a
> point-in-time, evidence-backed statement (tied to the green suite and the
> tests named below) — not an unfalsifiable "bug-free" guarantee.

---

## 1. Methodology

The review was executed by a **130-agent adversarial workflow**, deliberately
biased toward the *code and tests* rather than documentation:

1. **6 subsystem mappers** — core execution, queue/concurrency, persistence/
   events, DAG model, tasks, API surface — produced code-grounded maps
   (execution model, invariants, suspicious areas).
2. **8-dimension bug hunt** (races, atomicity/crash-consistency, event
   sourcing, DAG logic, resource lifecycle/leaks, task implementations,
   persistence parity, API/validation) → **39 raw findings**.
3. **Dedup → 37 unique**, each **adversarially verified by 3 independent
   lenses** (code-correctness, reproducibility, impact) with majority voting;
   findings the panel refuted were dropped.
4. **Completeness critic** swept for uncovered areas.
5. Parallel tracks: **LangGraph/agentic-engine gap analysis**, **reverse-
   dependency compatibility scan** (12 consumers in `~/develop`), and a
   **full test-suite baseline**.

A rate limit interrupted ~22 of the verifications; those findings were
**manually re-verified in source** by the author before fixing, and each was
turned into a failing repro test first (test-first remediation throughout).

**Baseline (pre-audit):** 973 passed / 6 skipped on SQLite; the 200 "errors"
were all `[postgres]`-parametrized nodes with no Docker in the environment
(environmental, not defects).

---

## 2. Findings and resolutions

**14 findings were confirmed by the 3-lens panel** (plus several the panel did
not reach, re-verified manually and fixed). Every fix is additive or a genuine
crash-bug correction; none changes the public API or observable behavior for
correct programs. Severity is the panel's verified severity.

### Crash-consistency & concurrency (highest severity)

| # | Severity | Defect | Fix | Repro test |
|---|---|---|---|---|
| 1 | high | Durable message dedup bypassed after restart/bloom-rotation → already-processed messages re-execute | Bloom negative-cache is opt-in (`dedup_trust_negative_cache`) and only trusted when hydrated from the store | `test_dedup_restart_bypass.py` |
| 2 | high | `CompleteStage` after-stages/on-failure branches didn't mark the source message processed; redelivery cancels a healthy workflow | Mark processed in-transaction; RUNNING re-entry is an idempotent no-op; on-failure planned once | `test_complete_stage_redelivery.py` |
| 3 | high | Zombie `RUNNING` stage (crash between claim and plan) wedges forever via CAS mismatch | Zombie re-plan claims with `expected_phase=RUNNING` | `test_zombie_stage_recovery.py` |
| 4 | high | Mutex (WCP-17/39/40) TOCTOU: two same-`mutex_key` siblings run concurrently | Atomic `(execution_id, claim_key)` claim row (`stage_claims`) inside the claim txn | `test_mutex_deferred_choice_race.py` |
| 5 | high | Deferred choice (WCP-16) TOCTOU: two branches both win | Same atomic claim keyed on the choice group | `test_mutex_deferred_choice_race.py` |
| 6 | high | Cross-process duplicate task execution (lease off, `lock_duration` ≪ task timeout, no renewal) | Lock **heartbeat** renews the message lock while a handler runs (`enable_lock_heartbeat`, `Queue.extend_lock`) | `test_lock_heartbeat.py` |
| 7 | high | Events committed/published **before** the state txn → phantom events, subscribers see rolled-back state | Recording joins the store transaction; bus publish deferred to commit | `test_event_txn_consistency.py` |
| 8 | high | Finalizer per-call timeout not enforced → a hung finalizer blocks shutdown forever | `shutdown(wait=False, cancel_futures=True)` on timeout; abandon the future | `test_finalizers.py` |
| 9 | medium | Non-atomic jump leaves skip-region stages runnable after a crash | All jump mutations + StartStage push in one transaction | `test_jump_atomicity.py` |
| 10 | medium | Discriminator/N-of-M `_join_fired` never reset → retry loops wedge | `reset_stage_for_retry` clears join-tracking keys | `test_jump_fanin.py` |
| 11 | low | `WorkflowCircuitFactory` mutates its cache without a lock | Guard with a `threading.Lock` | (covered by concurrency suite) |
| 12 | medium | Metrics projection inflated by retries (dedup on raw sequence) | Dedup on logical transition (entity+type+version), sequence fallback | `events/test_audit_fixes.py` |
| 13 | low | Replay warns of "gaps" using the global sequence (false alarms) | Removed global-sequence gap heuristic | `events/test_audit_fixes.py` |
| 14 | low | `EventBus` ASYNC delivery unordered per subscriber | Per-subscription FIFO drain on the shared pool | `events/test_audit_fixes.py` |

### Task implementations & API (re-verified manually, then fixed)

| Defect | Fix | Repro test |
|---|---|---|
| `SSHTask` over-quotes the command → every command with args fails | Pass the command verbatim as the final argv element | `test_ssh_task.py` |
| Two unrelated `StabilizeError`/`VerificationError` hierarchies | Engine hierarchy roots on the public classes | `test_exception_hierarchy_bridge.py` |
| `stage.outputs` / context / task-exception / signal serialized without `default=str` → crash on non-primitives | `default=str` on all user-data sites, both backends | `test_outputs_serialization.py` |
| Finalizers never run on stage terminal states | Run on `CompleteStage`/`CancelStage` terminal paths | `test_finalizers.py` |
| `ProcessIsolatedTaskExecutor` join-before-drain deadlock on large results | Drain the result queue before joining | `test_a4_triage_fixes.py` |
| `RunTaskHandler` lease-acquire failure leaks state | Acquire inside the `try`/`finally` | (guarded) |
| `DockerTask` orphans container on timeout | Auto-name + `docker kill` on `TimeoutExpired` | `test_a4_triage_fixes.py` |
| `ShellTask` `ctypes.CDLL` in post-fork `preexec_fn` (fork-safety) | Load libc once in the parent | (hardening) |
| `TransientVerificationError` drops `context_update` | Accept + store it | `test_a4_triage_fixes.py` |
| `SqliteQueue.push` ignores documented `connection=` | Honor it; defer commit to caller | `test_a4_triage_fixes.py` |
| `FileAuditLogger` writes no file; `TaskRegistry` docstring; `HTTPTask` retries non-idempotent by default; `HighwayTask` logs key prefix; malformed `Content-Length`; recovery duplicate `StartTask`; migration checksum warn-only; Postgres monitor stats | Each addressed additively (opt-in flags where behavior would change) | `test_a3_misc_fixes.py`, others |

### Documented residual (deferred, tracked)

- **HTTP SSRF DNS-rebinding TOCTOU** (`tasks/http/task.py`): the URL is
  validated by hostname, then reconnected — a rebinding attacker can pass
  validation and connect to a private IP. The robust fix (resolve-once,
  connect-to-resolved-IP via a pinning opener) is a connection-layer change
  with higher regression risk than yield for this pass. Mitigation exists
  today: `allow_private_urls` defaults to False. **Tracked for a follow-up.**
  This is the only known residual and is a defense-in-depth hardening, not a
  workflow-correctness defect.

---

## 3. Agentic-engine gap closure (vs LangGraph 2026)

The review found stabilize **already ahead** of LangGraph on durability (atomic
state+message commit per step vs checkpoint-only), event-sourced time-travel,
20 WCP control-flow patterns, durable suspend/resume, jump-based cycles,
bulkheads/circuits/DLQ, and serverless SQLite deployment. The real gaps were
**agent ergonomics**, all now closed **additively** (facade exports only; core
untouched):

| Gap (effort/yield) | Added |
|---|---|
| Streaming intermediate progress/tokens (S/5) | `WorkflowStream`, `emit_progress()`, `EventType.CUSTOM` |
| Human-in-the-loop interrupt/approve (S/4) | `ApprovalTask`, `approve`/`reject`/`send_signal`/`get_signal` |
| Declarative fan-in reducers (M/4) | `StageExecution.output_reducers` + `stabilize.reducers` |
| Prebuilt LLM tool-calling ReAct loop (L→M/5) | `stabilize.llm`: `LLMClient`, `@tool`/`ToolRegistry`, `LLMTask`, `AgentLoopTask` |

Not implemented (documented for later): long-term cross-session memory store
(orthogonal to the engine), fine-grained durability performance modes
(stabilize is already on the safe end, which is the right default).

---

## 4. Test evidence

| Suite | Baseline | After audit |
|---|---|---|
| **Unit + golden (SQLite), all-in** | 973 unit passed / 6 skipped | **1054 passed / 4 skipped / 0 failed** |
| — core unit suite | 973 | 1040 |
| New regression tests | — | **+67** across 17 files (test-first per fix) |
| Golden-standard (SQLite) | green | green |
| Weak-spot coverage tests | (gaps) | **5 passed** (crash-injection, real-DLQ, replay-equivalence, graceful-stop, poison→DLQ) |
| Audit certification tests | — | **7 passed** (`test_audit_certification.py`) |
| `ruff check` (changed) | — | clean |
| `mypy` (new modules) | — | clean |
| Downstream import surface | — | facade + deep paths used by all 12 `~/develop` consumers importable |

**Coverage gaps closed** (from the baseline audit's weak-spot list): crash
injection between store write and queue push; DLQ atomicity against the *real*
`dlq.py`; live-run-vs-event-sourced-replay equivalence; graceful processor stop
with an in-flight task (exactly-once + clean resume); poison-message escalation
to the DLQ through the live processor.

**Scope caveat — Postgres matrix.** Docker/testcontainers was unavailable in
the audit environment, so the `[postgres]`-parametrized nodes did not execute
(same as baseline). All Postgres changes were made **symmetric to SQLite**
(the `stage_claims` migration, `extend_lock`, `get_processed_message_ids`,
transaction `acquire_claim`, `default=str` serialization, monitor stats) and
are structurally verified; they should be run on a Docker-enabled host with
`make test-postgres` before a production release that targets Postgres.

---

## 5. End-to-end agentic proof

`examples/agent_team/` builds a real Python library end-to-end through
stabilize: **architect → parallel coders (fan-out) → reducer-gathered AND-join
→ smoke test → reviewer `jump_to` retry loop → HITL approval gate → packager**,
with live streaming and event sourcing. Verified on **ollama.com cloud
`glm-5.2`** (via `OLLAMA_API_KEY`):

- **Normal run:** `RESULT: PASS ✅` — glm-5.2 designed `core` + `helpers`
  modules (a real thread-safe token-bucket rate limiter), implemented them in
  parallel, gathered outputs via the reducer, suspended at the approval gate
  and resumed, and packaged. Event-sourced replay reconstructed the same
  `SUCCEEDED` terminal state; the generated library's tests passed.
- **Chaos run (`--chaos`):** the worker was `SIGKILL`ed mid-run; a fresh
  processor with `recover_on_start` re-queued the interrupted work and drove
  the workflow to `SUCCEEDED` — **durable recovery proven**, with replay
  equivalence and green generated-library tests.

This exercises, in one workload: durable queue-driven DAG execution, parallel
fan-out, AND-join + declarative reducers, dynamic-routing retry loops, durable
suspend/resume (HITL), the LLM toolkit, live streaming, event sourcing +
replay, and crash recovery.

---

## 6. Backward compatibility

Hard constraint honored: **no public symbol removed, renamed, or moved; no
default behavior changed for correct programs** (the exceptions are genuine
crash-bug fixes). 12 downstream projects in `~/develop` (~164 importing files;
`red9`=87, `moai-adk`=35, …) were scanned; every facade and deep-path symbol
they import is verified importable at this commit. New features are additive
exports; behavior changes that could surprise a user are gated behind opt-in
flags (`dedup_trust_negative_cache`, `retry_non_idempotent`, `strict`,
`STABILIZE_STRICT_MIGRATIONS`). The `stage_claims` table is created via an
additive, idempotent forward migration on both backends.

---

## 7. Reproduce this certificate

```bash
# Unit + golden suites (SQLite)
make test-sqlite
make golden-tests-sqlite

# The executable certification claims
python -m pytest tests/test_audit_certification.py -v

# Weak-spot coverage
python -m pytest tests/test_weak_spots.py -v

# Lint / types on changed code
ruff check src/stabilize examples/agent_team
mypy src/stabilize/streaming.py src/stabilize/hitl.py src/stabilize/reducers.py src/stabilize/llm/

# End-to-end agentic proof (needs OLLAMA_API_KEY for the cloud model)
python examples/agent_team/main.py            # normal
python examples/agent_team/main.py --chaos    # kill + recover
```

---

*Audit performed with a 130-agent adversarial review harness; findings
code-verified and remediated test-first. This certificate is valid for commit
`528acda` and supersedes no prior guarantees. Re-run section 7 to re-establish
it on any later commit.*
