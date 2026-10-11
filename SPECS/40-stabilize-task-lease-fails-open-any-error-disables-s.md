# 40 — STABILIZE_TASK_LEASE fails open: any error disables single-execution leasing and leaves only a WARNING

Ticket: issuedb #40 · status in-progress · priority high

EARS SPEC:
- Where an operator enables STABILIZE_TASK_LEASE, the engine shall either provide distributed task leasing or refuse to start, and shall not run with leasing silently disabled.
- If the lease manager cannot be initialised, then the engine shall raise, naming the cause and the fact that single-execution across processes is unavailable.
- The engine shall not catch a bare Exception around a correctness control's initialisation.
- When leasing is requested and available, the engine shall log at INFO only after the lease table has been confirmed usable, not merely after the object is constructed.
- The probe shall demonstrate both directions: leasing active with the flag set and the privilege present, and a refusal with the flag set and the privilege absent.

SOURCE, handlers/run_task/handler.py:106-116 on 0.27.0, verbatim:

    self.task_lease = None
    if os.environ.get("STABILIZE_TASK_LEASE", "").lower() in ("1", "true", "yes"):
        from stabilize.persistence.task_lease import TaskLeaseManager
        try:
            ttl = float(os.environ.get("STABILIZE_TASK_LEASE_TTL_SECONDS", "3600"))
            self.task_lease = TaskLeaseManager(repository, ttl_seconds=ttl)
            logger.info("Distributed task lease enabled (owner=%s)", self.task_lease.owner)
        except Exception as e:
            logger.warning("Failed to initialize task lease manager, continuing without it: %s", e)
            self.task_lease = None

TaskLeaseManager.__init__ (persistence/task_lease.py:36, :57, :61-89) calls _ensure_table(), which executes CREATE TABLE IF NOT EXISTS task_leases on both backends. So a runtime role without CREATE raises InsufficientPrivilege, which this catches.

WHY THIS IS SEVERE. The lease exists so that only ONE worker across processes executes a given task at a time — the code's own comment says so. Catching the failure and continuing turns off a CONCURRENCY-CORRECTNESS CONTROL and leaves a WARNING as the only trace. An operator who deliberately set STABILIZE_TASK_LEASE=1 believes they have single-execution, does not have it, and will have stopped defending against double execution some other way. That is worse than never offering the feature.

AND THE CATCH IS BARE , not InsufficientPrivilege. Any failure at all — a connection error, a pool exhaustion, a typo in STABILIZE_TASK_LEASE_TTL_SECONDS making float() raise ValueError — silently disables leasing. The privilege case is one trigger of many; ci-conductor found it by that route but the defect is wider than they could see from outside.

REPORTED BY ci-conductor-dd94c8 (AgentBus 01M2Y6N9KGSGGMK22HJACD8K39) against their installed 0.21.1. Confirmed byte-identical on 0.27.0 in this checkout. They quoted my own rejection of exactly this pattern for the event store back at me: 'catching InsufficientPrivilege and carrying on produces a store silently missing commit_xid, which is precisely the silent-degradation defect I closed in 0.23.0 for breaker storage — I am not reintroducing it one module over.' It was already there, one module over, in the version I ship.

MY OWN CHECK WAS VACUOUS AND THIS IS WHY THE DEFECT SURVIVED MY AUDIT. Issue 39 records 'persistence/task_lease.py — LATENT: zero call sites in src/'. That came from , which returns 0. The class is named TaskLeaseManager. The grep had no known-positive, so its silence meant nothing, and I reported an absence with full confidence. The correct grep returns handler.py:109 and :113. Issue 39's statement must be corrected wherever it appears, including SPECS/39.

TECHNICAL PROBLEMS:
1. An opt-in correctness guarantee degrades to no guarantee without the caller learning.
2. A broad exception handler around initialisation conflates 'this feature is unavailable' with 'this feature is off', which are different states for a control the operator asked for.

SOLUTION DOMAINS:
- Fail-closed initialisation -> the existing STABILIZE_CIRCUIT_STORAGE_STRICT work (0.23.0) is the house precedent in this repository, including its probe that tests both directions. Extract the concept: a degraded substitute for a control the caller explicitly requested is a failure, not a fallback.
- Correctness controls -> a lease is a mutual-exclusion primitive; a mutual-exclusion primitive that may silently not exist is not one.

ALTERNATIVES:
- Raise unconditionally when the flag is set and initialisation fails [CHOSEN: the flag IS the opt-in, so the operator has already asked for the guarantee; there is no reading under which they wanted it optional. Matches 0.23.0's strict mode without needing a second flag] vs a STABILIZE_TASK_LEASE_STRICT flag [REJECTED: a strict flag on top of an opt-in flag means the plain opt-in still lies, and it asks the operator to opt in twice for one guarantee] vs narrow the catch to InsufficientPrivilege and keep continuing [REJECTED: fixes one trigger of several and keeps the failure-open shape; a ValueError from the TTL parse would still disable leasing].
- Table creation: TaskLeaseManager should not create its own table either; task_leases belongs in migrations/ with the rest of the schema [CHOSEN, and it is why this ticket is coupled to #39 rather than independent].

COUPLED TO #39: the DDL-from-constructor fix removes one trigger; this ticket removes the failure-open. Both are needed — #39 alone would turn a silent disable into a louder one that is still caught here.

ACCEPTANCE: a probe demonstrating the flag set + privilege present -> leasing active, and the flag set + privilege absent -> refusal naming the cause. The second direction must be shown to fail before the fix.

## Progress, evidence and synthesis changes

FIXED. Red before green, both demonstrated on artifacts rather than asserted.

CHANGE: persistence/task_lease.py gains TaskLeaseUnavailableError (mirroring the CircuitStorageUnavailableError precedent from 0.23.0). handlers/run_task/handler.py:106-116 no longer catches. The TTL parse and the manager construction are now separate try blocks so the message names which one failed, and the refusal names the consequence and two remedies rather than just the error.

RED, against stabilize==0.27.0 installed from PyPI:
    === C. FLAG SET AND INITIALISATION FAILS — must REFUSE, not continue ===
        CONSTRUCTED ANYWAY, task_lease=None
        -> leasing is silently off while the operator believes it is on
    === D. FLAG SET AND TTL UNPARSEABLE ===
        CONSTRUCTED ANYWAY, task_lease=None
    [PASS] flag unset leaves leasing off
    [PASS] flag set yields a live lease manager
    [FAIL] initialisation failure refuses
    [FAIL] refusal names the consequence
    [FAIL] unparseable TTL refuses
    VERDICT: FAIL — 3 of 5 checks failed          RED-RUN EXIT=1

GREEN, same probe on the fixed tree:
    [PASS] initialisation failure refuses: TaskLeaseUnavailableError
    [PASS] refusal names the consequence: ...could not be initialised (RuntimeError: InsufficientPrivilege...)
    [PASS] unparseable TTL refuses: TaskLeaseUnavailableError
    VERDICT: PASS — leasing is provided or refused, never silently absent (5 checks)

CHECKS A AND B PASS ON BOTH VERSIONS. That is what makes this a finding rather than a version-difference: the probe isolates the three behaviours that changed instead of failing wholesale on the old artifact. B is the load-bearing direction — without it, an engine that never leases at all would satisfy C and D.

D IS THE HALF ci-conductor COULD NOT SEE FROM OUTSIDE. They found this via the privilege route; the bare  also swallowed a ValueError from parsing STABILIZE_TASK_LEASE_TTL_SECONDS, so a typo in an env var silently disabled a concurrency-correctness control. Both triggers now refuse.

ARTEFACTS: audit/evaluations/probe_task_lease_fails_closed.py (registered in run_all.sh), plus TestLeaseFailsClosed in tests/test_task_lease.py carrying the same three cases and the availability control. make lint clean; tests/test_task_lease.py 10 passed, 10 deselected.

NOT CLOSED YET: closes only after re-verification against the published 0.28.0 artifact, per the release acceptance rule.

CORRECTION to the previous comment: a shell backtick substitution ate the code fragment. The sentence should read:

"They found this via the privilege route; the bare `except Exception` also swallowed a ValueError from parsing STABILIZE_TASK_LEASE_TTL_SECONDS, so a typo in an env var silently disabled a concurrency-correctness control."

The catch was not narrowed to InsufficientPrivilege — it was `except Exception`, which is why the TTL parse was a second, independent trigger. Both now refuse.
