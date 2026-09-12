# Changelog

## [0.22.0]

### Fixed
- **Persistent signals (WCP-24) no longer accumulate without bound in
  `stage_executions.context`.** `SignalStageHandler` buffered a persistent signal
  whenever the target stage was not `SUSPENDED`, and the only drain
  (`handlers/run_task/result.py`) pops one entry on a suspend *edge*. A stage in a
  complete status can never reach another suspend edge, so every signal sent to it
  was appended to a JSON list that nothing could ever read. Reported by
  ci.rodmena.co.uk against 0.21.1 on PostgreSQL: 1,259,015 buffered entries across
  105 stage rows, of which 1,258,972 (99.997%) sat on 39 CANCELED or TERMINAL rows,
  the largest holding 112,901 copies of the same 46-byte object. The row was 93 kB
  on disk and 5,405 kB uncompressed — a 58x TOAST ratio, because the entries are
  identical — and PostgreSQL sent the uncompressed value as a bind parameter on
  every state transition, logging it each time.

  Measured by infra-manager on the affected host (pg-nano-03, shared by six
  databases): ~5.1 GiB of free space consumed in about 36 hours to 2026-09-06,
  peaking at 1.66 GiB per 6-hour window (~6.6 GiB/day at the filesystem, against
  a ~0.25 GiB/day baseline), and 3.76 GiB reclaimed on remediation. Free space
  bottomed at 14.46 GiB of 38 GB. No other database on the host was degraded.
  A separate outage on that host on 2026-09-12 was an unrelated memory
  misconfiguration and is not attributable to this defect.

  Three bounds, none of which changes behaviour for a stage that is genuinely
  waiting:
  - A persistent signal for a stage whose status `is_complete` is **refused**, not
    buffered. It is marked processed so it is neither redelivered nor retried.
  - The buffer holds at most `STABILIZE_SIGNAL_BUFFER_MAX` entries per stage
    (default 1000). Overflow is refused and moved to the dead-letter queue with
    reason `signal_buffer_full`, so it stays inspectable and replayable rather than
    silently dropped.
  - A stage's buffer is cleared when the stage reaches a complete status.

- **`SignalStageHandler` ignored an injected `HandlerConfig`.** It was constructed
  without one in `queue/processor/mixins.py`, so programmatic configuration was
  silently discarded and only the environment variable took effect.

### Added
- **`stabilize prune-signals`** reclaims buffers already stranded in a database by
  an earlier version — the new bounds stop the growth but cannot reach rows whose
  stage has already completed. `--dry-run` reports the count without writing,
  `--status` narrows to named statuses for a staged rollout, and `--include-active`
  extends to stages that could still consume their buffer. An unrecognised status
  name is rejected rather than matching no rows, so a typo cannot report a
  successful cleanup of zero. Backed by `cleanup_buffered_signals()` and
  `count_buffered_signal_stages()` on `WorkflowStore`.
- **`STABILIZE_SIGNAL_BUFFER_MAX`** (default 1000) caps buffered persistent signals
  per stage. The cap is on **array length**, not serialised size: identical entries
  TOAST-compress ~58x, so a byte-based cap sized from `pg_column_size` would admit
  far more than intended, while the real cost — bind-parameter size, parse time and
  WAL per transition — tracks the uncompressed form.

### Changed
- **Refusal logging is rate-bounded.** A log line per refused signal would fire at
  the signal rate and reproduce, at the logging layer, the unbounded growth the
  refusal prevents. Refusals are counted per stage and emitted at WARNING on counts
  1, 10, 100, 1000, ... and on every multiple of 100,000, carrying the running
  total. Decade spacing alone leaves a live producer effectively silent as the gap
  grows geometrically (104 days, then 1,040, at 6 signals/min); the floor bounds it
  so an ongoing problem never looks like a stopped one. Measured: 112,901 refusals
  cost 6 lines, 10,000,000 cost 105. The tracker is LRU-bounded to 1024 stages.

### Fixed (PostgreSQL)
- **`count_buffered_signal_stages` raised `KeyError: 0` on PostgreSQL.** It indexed
  a row positionally against psycopg's `dict_row` factory, so
  `prune-signals --dry-run` — the read-only command — would have crashed on the
  backend it is most needed on. It passed mypy and ruff; no test exercised the
  counting path under the postgres fixture until one was added.

### Known
- Dead-lettering on cap overflow is not itself bounded: a producer hammering a
  stage that is live but never suspends writes a DLQ row per overflow. This is
  row-per-message in a dedicated table with an existing `clear_dlq()`, is the
  storage shape this area is moving toward, and is not reachable by the reported
  incident, whose stages were complete and therefore refused without storing.
- `workflow_signals` — migrated in `01KGHWCP1M2QSDE3TN4GUBLV8A`, indexed, with a
  complete SQLite implementation at `persistence/sqlite/signals.py` — is still
  unwired; confirmed empty in production. Moving buffering onto it is tracked
  separately and deliberately not shipped alongside an incident remediation,
  because it touches the hot path of every suspended stage.

## [0.21.0]

### Changed (dependencies)
- **Adopted `bulkman>=2.0.1,<3`** (was `>=0.1.0`), so the 2.0.x major is taken
  deliberately rather than picked up incidentally by a lock refresh. The direct
  `resilient-circuit` requirement moves to `>=0.4.6,<0.5` to match the floor
  bulkman itself requires, so a resolver cannot select a version bulkman
  rejects. Verified against the wheels PyPI actually serves.
- **Shutdown races are now classified on exception type.** bulkman 2.0.1 adds
  `BulkheadShutdownError`, raised on post-shutdown execute by both the
  deterministic and the submit-race paths (confirmed against the published
  wheel). The interim workaround — a bare `except RuntimeError` gated on our own
  terminal-state flag — is removed. The pre-dispatch `is_shutdown` check is
  deliberately **kept**: it refuses before the call enters the circuit breaker,
  and a shutdown raised from inside a protected call is recorded by
  `resilient-circuit` as a failure (measured: the circuit opens after 2), which
  would trip circuits for workflows that never failed during a rolling restart.

### Fixed (test integrity)
- **Two tests that could not report a failure now can.** Neither was flaky; both were
  green regardless of the state of the code they covered.
  - `test_race_between_check_and_mark_demonstrates_bug` recorded the known message-
    deduplication race with an imperative `pytest.xfail()` inside an `if`, so it
    xfailed when the race reproduced and *passed silently* when it did not — and would
    have stayed green if the race were fixed, or if it got worse. It now asserts the
    desired contract (deduplication admits exactly one worker) under
    `@pytest.mark.xfail(strict=True)`, so closing the race makes it XPASS and fail the
    suite, forcing the marker to be retired deliberately. **The underlying race is
    unchanged and still open** — handlers must remain idempotent.
  - The SQLite thread-local connection test called `pytest.skip("Too many SQLite lock
    failures ... test inconclusive")` at runtime when too few threads obtained a
    connection — disarming itself under exactly the condition it exists to detect. A
    regression in thread-local connection creation reported *skipped*, not *failed*.
    It now asserts, and reports the observed lock errors.

### Fixed
- **Graceful shutdown now bounds bulkhead shutdown.** `LifecycleManager` called
  `TaskBulkheadManager.shutdown(wait=True)` without passing any timeout, so a
  single stuck task held shutdown open indefinitely despite the advertised
  `shutdown_timeout` (measured: still blocked after 6s with a 30s task). The
  remaining budget is now passed down, and an exhausted budget shuts the manager
  down without waiting. This was latent under bulkman 1.x, which ignored the
  timeout argument entirely; bulkman 2.0.0 honors it.
- **A shutdown race no longer permanently fails a task.** bulkman 2.0.0 makes
  shutdown terminal and surfaces a later execute as a bare `RuntimeError` from
  `concurrent.futures`, which callers cannot distinguish from a task's own
  error. `TaskBulkheadManager` now exposes `is_shutdown`, and
  `execute_with_resilience` refuses dispatch to a terminal manager with a
  retryable `TransientError` instead of burning the task's attempts.

## [0.20.0]

Peer-review remediation release. An external 10-finding evaluation of the
engine (by the RunFlow team) was verified finding-by-finding — all 10
confirmed — and fixed, alongside a partner-reported migration defect and a
re-verification of the v0.19.0 audit certificate on this tree (see the
addendum in `AUDIT.md`).

### Fixed (correctness)
- **`Orchestrator.cancel()` / `.restart()` / `.unpause()` now work.** The
  `CancelWorkflow`, `RestartStage`, `ResumeStage` and `PauseTask` messages had
  no registered handler and were consumed and discarded: cancel returned
  success while the workflow kept running, and pausing a workflow *lost* its
  in-flight task. Four new handlers (`stabilize.handlers.workflow_control`)
  implement the documented contracts; pausing parks the task as `PAUSED` and
  resume re-arms and re-dispatches it. The engine's own start-time-expiry
  cancellation path was silently broken by the same gap and now works.
- **A message with no registered handler is no longer silently acked** — it
  raises, retries, and escalates to the DLQ, so a lost instruction is loud.
  The `Invalid*` diagnostic markers get an explicit consuming handler.
- **Poison messages now reach the DLQ in daemon mode.** The poll loop sweeps
  attempts-exhausted messages every `dlq_check_interval_seconds` (default
  30s); previously only `process_all()` swept, so a service using `start()`
  stalled poisoned work invisibly and `size()`-based drain checks never hit
  zero.
- **Invalid stage graphs are rejected at `Workflow.create()`** with named
  errors — `duplicate_ref`, `self_edge`, `unknown_ref` (naming the typo'd
  stage and the missing ref), and cycles (naming the members) — instead of
  surfacing at runtime as "Exceeded max retries waiting for upstream stages".
- **Recovery's duplicate-guard no longer full-scans the queue.** The
  leading-wildcard `LIKE` over `payload::text` became an exact, indexed
  top-level `task_id` lookup on both backends (additive migration
  `add_queue_task_id_index`), which also removes a nested-substring
  false-positive.

### Added
- **`stabilize mg-up`/`mg-status` target schema support** (reported by
  SponsorSignal): `?schema=` in the db URL, `MG_SCHEMA`, or a `schema:` key in
  `mg.yaml`. `mg-up` validates the identifier, creates the schema if missing,
  and applies everything into it; docs cover pointing the runtime at the same
  schema (`options=-csearch_path`) and moving an existing `public` install.
- **Opt-in retention sweep** (`retention_sweep_interval_seconds`, default off)
  cleans `processed_messages` by age and `stage_claims` for **terminal
  executions only** — a live execution's claim is never deleted, since that
  would resurrect the mutex/deferred-choice race it prevents.
- **Startup warning when crash recovery is disabled** on a store-backed
  processor (`from_handler_config()` does not carry recovery settings, so
  env-configured embedders were getting recovery silently off).
- Soft-timeout visibility: thread-mode timeout logs now state the worker
  thread keeps running, and the resilience guide gained a "Timeout Semantics
  by Isolation Mode" section.

### Changed
- `resilient-circuit>=0.4` is declared as a direct dependency (previously
  imported but only present transitively via `bulkman`).
- CI runs the full non-postgres suite (`-k "not postgres"`) instead of
  `-k "sqlite"`, which had been deselecting ~half the tests — including the
  audit certification suite.
- Docs corrected: there is no In-Memory store backend (use
  `sqlite:///:memory:`, per-connection); SQLite journal mode defaults to
  DELETE with WAL opt-in.

## [0.19.1]

Documentation and developer-experience release. No engine behavior changes.

### Added
- **`stabilize prompt` now covers the agentic toolkit.** The built-in reference
  consumed by AI coding agents gained a full "Agentic Workflows" section:
  `LLMClient`, one-shot `LLMTask`, tool-calling `AgentLoopTask`, `@tool`/
  `ToolRegistry`, durable human-in-the-loop approvals (`ApprovalTask` +
  `approve`/`reject`), live streaming (`WorkflowStream` + `emit_progress`),
  fan-in reducers (`output_reducers`), agentic control-flow (`jump_to` loops,
  N-of-M quorum, discriminator race), and a complete runnable template.
  Validated against glm-5.2: given only `stabilize prompt`, a model generated
  and ran a complete multi-agent workflow on the first attempt.
- **New example `examples/research_analyst/`** — a complex multi-agent workflow
  (parallel ReAct researchers, N-of-M join with reducer, discriminator race,
  refine loop, human approval, report sub-workflow, and crash recovery),
  verified end to end on glm-5.2.
- **Docs:** an "Agentic Workflows" guide and API reference page.

### Changed
- **README rewritten** around building simple, mid, and complex agentic
  workflows, and it now leads with pointing a coding agent at `stabilize
  prompt`. Removed the tool-comparison table.

## [0.19.0]

Correctness audit (130-agent adversarial review) + agentic ergonomics. All
changes are additive or crash-bug fixes; the public API and existing tests are
unchanged. See `AUDIT.md` for the full audit record.

### Fixed (crash-consistency & concurrency)
- Durable message dedup is no longer bypassed after a process restart or bloom
  rotation — the negative-cache fast path is opt-in
  (`dedup_trust_negative_cache`) and only trusted when the bloom is hydrated
  from the durable store.
- `CompleteStage` after-stages/on-failure branches mark the source message
  processed in-transaction; a redelivered/stale `CompleteStage` while work is
  in flight is now an idempotent no-op instead of cancelling a healthy
  workflow, and on-failure stages are planned exactly once.
- Zombie `RUNNING` stages (claimed then crashed before planning) re-claim with
  `expected_phase=RUNNING`, so recovery re-plans them instead of wedging.
- Mutex (`mutex_key`, WCP-17/39/40) and deferred choice (`deferred_choice_group`,
  WCP-16) are enforced with an atomic claim row (new `stage_claims` table),
  fixing a TOCTOU race under concurrent workers.
- `QueueProcessor` heartbeats the queue message lock while a handler runs
  (`enable_lock_heartbeat`, default on; `Queue.extend_lock`), so a task
  outliving `lock_duration` is not redelivered and executed twice.
- Event recording joins the enclosing store transaction and defers bus
  publication until commit — no phantom events, no rolled-back state observed
  by subscribers.
- Jumps apply all stage mutations + follow-on messages in one transaction
  (atomic), and `reset_stage_for_retry` re-arms discriminator/N-of-M joins in
  retry loops.
- Finalizers: per-call timeout is actually enforced (a hung finalizer no
  longer blocks shutdown), and finalizers run on stage terminal states
  (completion/cancellation), not only at process shutdown.
- `WorkflowCircuitFactory` locks its circuit cache; metrics projection dedups
  on logical transition; replay drops spurious cross-workflow gap warnings;
  `EventBus` ASYNC delivery preserves per-subscriber order.

### Fixed (tasks & misc)
- `SSHTask` passes the remote command verbatim (over-quoting broke every
  command with arguments).
- Engine exception hierarchy roots on public `stabilize.StabilizeError` /
  `VerificationError` (`except stabilize.StabilizeError` now catches engine
  errors).
- `stage.outputs` / workflow context / `task_exception_details` / signal data
  serialize with `default=str` (non-primitive values no longer crash
  persistence).
- `ProcessIsolatedTaskExecutor` drains the result before joining (large
  results no longer misreported as timeouts); `RunTaskHandler` lease acquire
  no longer leaks state on failure; `DockerTask` names run containers and
  kills them on timeout; `ShellTask` loads libc in the parent (fork-safety);
  `TransientVerificationError` keeps `context_update`; `SqliteQueue.push`
  honors the `connection=` param; `HighwayTask` stops logging the API key;
  malformed `Content-Length` is tolerated; recovery guards duplicate
  `StartTask`.
- `FileAuditLogger` writes its file (global default is log-only);
  `TaskRegistry.register(strict=True)` opt-in; `HTTPTask` `retry_non_idempotent`
  opt-out; `STABILIZE_STRICT_MIGRATIONS` opt-in; monitor shows Postgres queue
  stats.

### Added (agentic ergonomics — additive/opt-in)
- **Streaming**: `WorkflowStream` (replay and/or follow live) + `emit_progress()`
  and a new `EventType.CUSTOM` for task-emitted progress/token events.
- **Human-in-the-loop**: `ApprovalTask` + `approve()` / `reject()` /
  `send_signal()` / `get_signal()` over the durable suspend/signal machinery.
- **Declarative fan-in reducers**: `StageExecution.output_reducers` +
  `stabilize.reducers` (`collect`/`sum`/`merge`/custom) so parallel branches
  stop clobbering scalar keys at a join.
- **LLM toolkit** (`stabilize.llm`, stdlib-only, not imported by the core):
  `LLMClient` (OpenAI-compatible/Ollama), `@tool` + `ToolRegistry`, `LLMTask`,
  and `AgentLoopTask` (bounded ReAct loop as one durable task).
- New example `examples/agent_team/` — a multi-agent software team that
  exercises the full engine end-to-end (verified on ollama.com cloud glm-5.2).

## [0.18.0]

### Added
- **Workflow Control-Flow Patterns (20 of 43)** — based on van der Aalst et al.
- Join types: `JoinType.OR` (WCP-7), `MULTI_MERGE` (WCP-8), `DISCRIMINATOR` (WCP-9), `N_OF_M` (WCP-30)
- Split types: `SplitType.OR` with per-downstream condition expressions (WCP-6)
- Safe expression evaluator (`stabilize.expressions`) for condition evaluation
- Deferred choice pattern: `deferred_choice_group` on `StageExecution` (WCP-16)
- Milestone gating: `milestone_ref_id` / `milestone_status` on `StageExecution` (WCP-18)
- Mutual exclusion / critical sections: `mutex_key` on `StageExecution` (WCP-17, 39, 40)
- Cancel region pattern: `cancel_region` on `StageExecution` (WCP-25)
- Signal-based suspend/resume: `TaskResult.suspend()` and `SignalStage` message (WCP-23, 24)
- Persistent trigger buffering for stages not yet suspended (WCP-24)
- Multi-instance builders: `MultiInstanceBuilder` for WCP-12 through WCP-15
- `MultiInstanceConfig` dataclass for multi-instance stage configuration
- Structured loop builders: `LoopBuilder.while_loop()` and `LoopBuilder.repeat_until()` (WCP-21)
- Sub-workflow task: `SubWorkflowTask` for recursive workflow patterns (WCP-22)
- New handlers: `SignalStageHandler`, `CancelRegionHandler`, `AddMultiInstanceHandler`
- New messages: `SignalStage`, `CancelRegion`, `AddMultiInstance`
- PostgreSQL migration for new stage columns
- Comprehensive test suite for all WCP patterns (62 new tests)

#### Production hardening (all opt-in; defaults preserve existing behavior)
- **Automatic crash recovery**: `QueueProcessorConfig.recover_on_start` and
  `recovery_interval_seconds` run `WorkflowRecovery` from the processor;
  `QueueProcessor.run_recovery()` for manual sweeps
- **PostgreSQL `get_all_pending_workflows`** parity with SQLite (cross-application
  recovery now works on both backends)
- **Cooperative task cancellation**: `is_cancellation_requested()` /
  `raise_if_cancellation_requested()` / `CancellationToken`; `CancelStage` signals
  running tasks (process-isolation mode remains the hard-kill path)
- **Distributed task lease** (`STABILIZE_TASK_LEASE=1`): cross-process
  single-execution guard for a task; window-narrowing (still requires idempotent
  handlers)
- **SQLite WAL opt-in** via `STABILIZE_SQLITE_JOURNAL_MODE=WAL` /
  `SqliteConfig(journal_mode=...)` (default remains `DELETE`)
- **SQLite schema migrations**: version-stamped, forward-only runner; existing
  databases upgrade in place without re-running baseline DDL
- **Event-sourcing upcasting**: global `EventMigrator` applied (leniently) on
  replay so historical events can be migrated to the current schema version
- 74 new tests covering the above on both SQLite and PostgreSQL

### Changed
- `StageExecution` now has 10 new fields for advanced control-flow patterns
- `evaluate_readiness()` dispatches on `join_type` for OR, MULTI_MERGE, DISCRIMINATOR, N_OF_M joins
- `CompleteStageHandler` supports conditional OR-split logic
- `StartStageHandler` supports milestone gating, mutex blocking, and deferred choice
- `QueueProcessor` auto-registers the 3 new handlers (15 total)

## [0.17.0]

### Added
- Event sourcing system (`stabilize.events`) with full audit trail
- Event store backends: SQLite, PostgreSQL, in-memory
- Event bus for in-process pub/sub with sync/async subscriptions
- Event recorder with automatic handler integration
- Projections: `StageMetricsProjection`, `WorkflowTimelineProjection`
- Event replay for state reconstruction and time-travel queries
- Snapshot support for faster replay of long-running workflows
- Durable subscriptions with at-least-once delivery
- Bloom filter deduplication (`stabilize.queue.dedup`)
- Structured error codes (`stabilize.error_codes`)
- Finalizer registry for resource cleanup
- DAG readiness evaluation (`stabilize.dag.readiness`)
- Event sourcing example (`examples/event-sourcing-example.py`)

### Changed
- All examples updated to use event sourcing by default
- Handler integration tests now verify event recording

## [0.16.2]

### Changed
- `QueueProcessor` now auto-registers all 12 default handlers when `store` and `task_registry` are provided
- `SynchronousQueueProcessor` accepts the same parameters as `QueueProcessor`
- `register_handler()` now raises `ValueError` on duplicate registration
- Added `replace_handler()` for explicit handler overrides
- Added `bulkhead_manager` and `circuit_factory` optional parameters to `QueueProcessor`

### Deprecated
- `register_all_handlers()` standalone function (no longer needed)
