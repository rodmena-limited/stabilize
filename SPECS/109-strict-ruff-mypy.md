# #109 — Every ruff rule and a stricter mypy over `src/`; 0.34.4

Ticket: issuedb #109. Asked by the director on 2026-11-06: run a full ruff lint with the fewest exceptions and the highest standard, and a full `mypy --strict` with the fewest ignores, over the product code (not `tests/`), report, fix what they find and release.

## EARS SPEC
- The stabilize repository shall lint `src/` with ruff's complete rule set (`select = ["ALL"]`, google docstring convention) and report zero findings; every disabled rule is listed once in `pyproject.toml` and its reason is recorded here.
- The stabilize repository shall type-check `src/` with `mypy --strict` plus `warn_unreachable` and the error codes `ignore-without-code`, `truthy-bool` and `possibly-undefined`, without a global `ignore_missing_imports`, and report zero errors.
- Inline suppressions in `src/` shall be limited to those that protect a public name; each one that remains is listed here.
- If a finding is a defect, then the fix shall change behaviour only in the direction the rule names, and a test shall cover it where behaviour changes.
- The full suite on both backends with the published 0.33.1 and 0.32.1 runners shall pass before release.
- When `src/` changes, stabilize 0.34.4 shall be built from the tagged commit, uploaded to PyPI with digests verified, tagged `v0.34.4`, fast-forwarded to `main` and announced to the adopters.
- Non-product directories (`tests/`, `golden_standard_tests/`, `audit/`, `examples/`, `scripts/`, `docs/`) shall keep the previous rule set (E, F, I, N, W, UP).

## Baseline (2026-11-06, main 11ac69d)
- ruff with the previous six families: clean. ruff `--select ALL`: 4849 findings in `src/`, 1574 autofixable.
- `mypy --strict`: clean only with `ignore_missing_imports = true`, which hid two untyped imports (`psutil`, `yaml`). With `warn_unreachable` and the extra codes: 87 errors in 34 files.
- Inline: 24 `type: ignore`, 7 `noqa`.

## TECHNICAL PROBLEMS
1. A rule set that is auditable: every exception named once with a reason, rather than a maximal set narrowed silently by inline suppressions.
2. Mechanical rewrites (524 docstring summaries, 175 typing-only imports moved behind `TYPE_CHECKING`) that must not change runtime. Pydantic's `TypeAdapter` resolves the message dataclasses' annotations at runtime (`queue/message_contract.py`), so `queue/messages.py` and `queue/messages/*` keep their imports at runtime (per-file ignore of TC001-TC003).
3. Dead-branch findings in handlers. `WorkflowStore.retrieve`, `retrieve_stage`, `get_upstream_stages` and `get_synthetic_stages` raise or return a list; they never return `None`. `StageExecution.execution` raises when the stage is unattached. The `None` guards were therefore unreachable and are removed; the contract is the type.
4. Public API frozen in a patch release: positional booleans, the `type` parameter name on `StageExecution` factories, exception class names and exception types raised by validators stay.

## SOLUTION DOMAINS
- ruff's documented formatter conflicts (COM812, ISC001), the google pydocstyle convention, negated per-file-ignores for the non-product directories, `required-imports` for `from __future__ import annotations` so the TC fixes become available.
- mypy optional error codes; typeshed stubs `types-psutil` and `types-PyYAML` as dev dependencies.
- House rules: no code comments, the 500-line cap, red before green (a lint rule's red is its baseline run), public API unchanged in a patch.

## ALTERNATIVES
- Rule set: `select = ["ALL"]` with an explicit ignore list [CHOSEN] vs extending the six families by a hand-picked dozen [REJECTED: silent on every family not picked].
- Scope: one negated per-file-ignore (`!src/**`) that keeps the old set outside `src/` [CHOSEN] vs a `ruff.toml` per directory [REJECTED: configuration in six places].
- Missing stubs: install the stubs [CHOSEN] vs per-module `ignore_missing_imports` overrides [REJECTED: still an ignore].
- Mixin attribute access: declare what the mixin needs in its class body (`tenant: str`, `delivery_defects`, `_borrow()` and `_latch()` stubs under `TYPE_CHECKING`) [CHOSEN] vs keeping 17 `attr-defined` ignores [REJECTED].
- `TaskLeaseManager` store access: `isinstance` on the two concrete stores with a `TypeError` otherwise [CHOSEN] vs `cast` [REJECTED: a cast asserts without checking].
- Complexity rules (C901, PLR091x): disabled with reason [CHOSEN] vs refactoring 65 functions in a lint pass [REJECTED: regression risk in the release the estate runs; the 500-line cap is the house size rule].
- `redundant-expr`: not enabled [CHOSEN]. It flags `isinstance` checks on untyped input in public validators (`StageCriteria`, `PoolOptions`, subscription names), which the plain dataclasses rely on, and it flags `self._running` re-reads inside loops, which another thread changes.
- `TRY004` (ValueError for a type check): disabled [CHOSEN] vs raising `TypeError` from `StageDefinition.from_dict` and `StageCriteria` [REJECTED: callers catch `ValueError`; an API change].
- `TRY301` (raise inside try): disabled [CHOSEN]. Raising `_ClaimBlockedError` inside the claim transaction is how the transaction is rolled back; moving the raise to a helper hides the design.

## The disabled rules and why
| Rule | Reason |
|---|---|
| COM812, ISC001 | Conflict with `ruff format`. |
| CPY001 | No copyright-header policy. |
| D100-D107 | Docstring presence is not mandated; the house rule avoids judgment-laden docstrings. |
| ANN401 | `dict[str, Any]` contexts are the engine's data model. |
| PLC0415 | Local imports break cycles and defer optional dependencies. |
| TRY003, EM101, EM102 | Exception messages are strings; a class per message is not the house style. |
| FBT001-FBT003 | Positional booleans are public API. |
| ARG001, ARG002, ARG005 | Interface signatures keep their parameters. |
| C901, PLR0911, PLR0912, PLR0913, PLR0915, PLR0917 | Complexity thresholds; the 500-line cap is the house size rule. |
| PLR2004 | Comparing against literal status codes and counts. |
| BLE001 | `except Exception` in handlers is the classification point. |
| SLF001 | In-package access to private members between cooperating modules. |
| PLW0603 | Module singletons (`global` statements) in the connection manager, recorder, bus. |
| S603 | Running subprocesses is the function of ShellTask, DockerTask, SSHTask, PythonTask and the migretti runner. |
| S608 | All 142 sites interpolate table or schema identifiers and SQL fragments built from constants; values are bound parameters. |
| B027 | Optional hooks on base classes have empty bodies on purpose. |
| TRY400 | `logger.error` without a traceback is deliberate on expected, recovered errors. |
| TRY301 | See alternatives. |
| TRY004 | See alternatives. |

Per-file: `cli/*` T201 (the CLI prints); `models/stage/stage.py` A002 (`type` is a public keyword argument); `tasks/http/request.py`, `tasks/highway/task.py`, `llm/client.py` S310 (the URL scheme is checked before `urlopen`; the SSRF guard in `tasks/http/task.py` covers HTTPTask); `tasks/docker.py`, `tasks/ssh.py` S607 (the operator's PATH decides which `docker` and `ssh` run, as it does for ShellTask); `queue/messages.py` TC001-TC003 (see problem 2).

## Remaining inline suppressions
- `errors/base.py`: `noqa: N818` on `StabilizeBaseException` (public name).
- `persistence/tenancy.py`: `noqa: N818` on `TenancyModeChanged` (public name since 0.34.0).

## Behaviour that changed
- Monitor timestamps are timezone-aware (`datetime.fromtimestamp(..., tz=UTC).astimezone()`); the JSON detail view's ISO strings carry the local offset.
- `assert` statements in `logging.configure_logging`, `tracing`, `Orchestrator._resume_without_paused_stages`, `PythonTask._generate_script`, `ShellTask` and `_handle_redirect` are explicit checks or raises that survive `python -O`.
- `EventStore.supports_commit_cursor()` and `get_events_since_committed()` are on the interface; the default is False and `NotImplementedError`. `SubscriptionManager._uses_commit_cursor()` calls the method. A custom store is polled by sequence; before, a store without `supports_commit_cursor` but with `get_events_since_committed` was assumed to support it. `tests/test_event_store_contract.py`, red on 0.34.3 (`AttributeError`).
- `TaskLeaseManager` reaches its store through `_sqlite_connection()` / `_pool()`, which refuse a store of the other class with `TypeError`.
- `from None` on the SSRF guard's raises inside its DNS `except`, `from exc` on the SQLite task writer's `ConcurrencyError`, `from timeout_error` on ShellTask's timeout `TransientError`.
- `logger.exception` replaces `logger.error(..., exc_info=True)` in ten places; the message loses the `%s` of the exception text, which the traceback carries.
- `lifecycle.install_handlers` / `uninstall_handlers` take the lock without an unlocked pre-check.
- `continue_parent_stage` has no branch for a third synthetic phase (the enum has two).
- `evaluate_readiness` filters None entries once at its public entry (its documented behaviour) and is typed `Sequence[StageExecution | None]`; dead `None` guards removed from its private evaluators, `handlers/start_stage/*`, `handlers/run_task/error.py`, `handlers/complete_stage/split_logic.py`, `handlers/jump_to_stage/handler.py`, `handlers/workflow_control.py`, the event recorders and `streaming.py`.
- `sqlite/transaction.py` compares the queue and store file locations with `Path.resolve()` (symlinks resolved) instead of `os.path.abspath`.
- `queue/dedup.py` and `cli/adoption.py` pass `usedforsecurity=False` to md5/sha1 (digests unchanged).
- `LOOP_TOKEN` constant renamed `LOOP_NONCE` in `tasks/loop.py`; the context key `_loop_token` is unchanged.
- `StageDefinitionBuilder` no longer inherits `ABC` (it had no abstract methods).

## Verification
- ruff clean on `src/`, `tests/` and `golden_standard_tests/`; `ruff format --check src/` clean; `mypy src/`: "Success: no issues found in 309 source files".
- First full suite (2026-11-06, `-n 2`): 4 failed, 2973 passed, 1 skipped. Two failures came from ruff's unsafe rewrite of `row["state_hash"] if "state_hash" in row.keys() else None` into `row.get(...)` on a `sqlite3.Row`, which has no `.get`; fixed as `dict(row).get("state_hash")`. One was `tests/test_readiness.py::test_none_upstreams_ignored`: `evaluate_readiness` is documented to ignore None entries, so the public entry filters them once and is typed `Sequence[StageExecution | None]`, while the private evaluators keep `list[StageExecution]`. One was a timing race in `tests/test_audit_control_matrix.py::test_pause_a_buffered_workflow_then_resume`: the test did not hold its first workflow, so under lighter load it completed before the second was observed BUFFERED; the test now holds it until then.
- Final gate (2026-11-06): full suite on SQLite and PostgreSQL 16 with the published 0.33.1 and 0.32.1 runners, `-n 2`: 2977 passed, 1 skipped (the by-design SQLite demonstration), 0 failed, 12 min 48 s. Log `/tmp/claude-1000/fullsuite_9.txt`.
- Lesson: review every `.get(` an unsafe ruff fix introduces; `RUF019` assumes a dict and the SQLite rows are `sqlite3.Row`.
