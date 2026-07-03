# Changelog

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
