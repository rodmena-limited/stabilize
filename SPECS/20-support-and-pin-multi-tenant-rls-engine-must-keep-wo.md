# 20 — Support and pin multi-tenant RLS: engine must keep working under row-level security with tenant_id defaulted from a GUC

Ticket: issuedb #20 · status in-progress · priority medium

EARS SPEC:
- While a caller connects with a DSN that sets a tenant GUC, the engine shall operate correctly against pipeline_executions, stage_executions, task_executions, queue_messages, queue_messages_dlq, processed_messages, workflow_signals and stage_claims when those tables carry a tenant_id column defaulted from that GUC under RESTRICTIVE row-level-security policies.
- The engine shall treat Workflow.application as an opaque string: stored, filtered on, and never parsed or reinterpreted.
- If a future change breaks operation under row-level security, then the engine's own test suite shall fail.

TECHNICAL PROBLEM: downstream platforms embedding stabilize in a multi-tenant database cannot scope the engine's eight tables, because the engine constructs its own connections from a raw DSN and bypasses any caller-side scoping helper.

WHY IT ALREADY WORKS, verified: pools are keyed by the exact DSN string, so a per-tenant DSN gets a dedicated pool; a libpq options parameter sets a custom GUC at connect (measured: current_setting('app.tenant_id') returns the value supplied in the DSN); INSERTs all use explicit column lists so a tenant_id column with DEFAULT current_setting(...) fills itself; and the SELECT * queries (queries.py:32,54,63,99,127,154; store.py:112,122,145,322) are safe against an added column because every converter indexes by name (converters.py:81+), never **row or positionally.

SCOPE: the value of this ticket is the TEST, not new features. Without a test pinning it, the 'yes' given to provenance-50ca06 is a promise rather than a check.

KNOWN SHARP EDGES to cover in the test: queue polling uses FOR UPDATE SKIP LOCKED and a policy must admit that access pattern, not merely plain SELECT; crash recovery sweeps run from whichever processor is live, so under RLS a tenant's orphans are only recoverable while something runs for that tenant; connection count scales with active tenants at one pool per DSN.

REPORTED BY: provenance-50ca06 (provenance #75). They offered to supply the policies and migration; pin those rather than invented ones.

BLOCKED BY: #18

## Progress, evidence and decisions

TEST DESIGN CORRECTED by a measurement from provenance-50ca06, reproduced independently here.

I had assumed a per-command policy (FOR UPDATE instead of FOR ALL) would break the queue poll. It does not: PostgreSQL applies UPDATE policies to a LOCKING select. Reproduced on postgres:15, non-owner role, tenant GUC in the DSN, 2 rows (1 own, 1 other), control first so the probe could show a leak:

    policy                        plain SELECT   SELECT ... FOR UPDATE SKIP LOCKED
    no restrictive policy         2              2      <- control
    RESTRICTIVE FOR ALL           1              1
    RESTRICTIVE FOR UPDATE only   2              1      <- leaks plain reads only

CONSEQUENCE: a behavioural test that only drives the poll CANNOT detect a per-command policy mistake, and passes confidently while queue depth, the DLQ listing and processed_messages leak across tenants. provenance reports the poll leg alone passed under three of their four sabotages.

So #20 must assert BOTH legs:
  1. drive the poll end to end (proves work still happens under RLS)
  2. assert SELECT count(*) across two tenants (proves isolation)

ADDITIONAL TEST CASES, from provenance's migration:
- THE THREE-GUC TRAP: they set provenance.tenant, provenance.sub and provenance.scopes. A connection carrying only the tenant satisfies the RESTRICTIVE policy and is REFUSED by the PERMISSIVE one; the symptom is an empty queue with no error, indistinguishable from 'no work to do'. Pin it.
- BACKFILL BEFORE POLICIES: after RLS an owner UPDATE with no GUC matches zero rows and reports success. Bit their rebuild twice. The migration order belongs in the docs, not a comment.
- NO FORCE: owner stays exempt so 'stabilize mg-up' still runs; the app role is constrained either way. Document so the next embedder does not FORCE and then wonder why migrations fail.

THE GUC NAME MUST BE A TEST PARAMETER, not hardcoded: they key on provenance.tenant via a STABLE PARALLEL SAFE provenance_tenant() function; another embedder may use app.tenant_id. Only that function changes on their side, so my test must not bake in either name.

Their migration, per table, in order: ADD COLUMN tenant_id uuid; UPDATE ... SET tenant_id=<platform uuid> WHERE tenant_id IS NULL; SET NOT NULL; SET DEFAULT provenance_tenant(); FK to tenant(id); index on tenant_id; ENABLE ROW LEVEL SECURITY; PERMISSIVE policy FOR ALL on scope/role; RESTRICTIVE policy FOR ALL on tenant_id = provenance_tenant(), both USING and WITH CHECK.

DONE. The claim is now a check rather than a promise, which this ticket said was its whole value.

audit/evaluations/probe_multitenant_rls.py, real PostgreSQL 16 via testcontainers, registered in run_all.sh:

    applied 10 migration(s); 7 tables carry tenant_id + RESTRICTIVE policy
    [PASS] the tenant GUC reaches the session: 'tenant-a'
    [PASS] tenant A workflow succeeds: SUCCEEDED
    [PASS] tenant B workflow succeeds: SUCCEEDED
    [PASS] tenant A sees exactly its own: 1 rows
    [PASS] tenant B sees exactly its own: 1 rows
    [PASS] both tenants' rows were really written: 2 rows
    [PASS] tenant_id defaulted from the GUC: ['tenant-a', 'tenant-b']
    [PASS] FOR UPDATE SKIP LOCKED is permitted: 0 rows
    VERDICT: PASS - the engine operates correctly under RESTRICTIVE RLS (8 checks)

Two tenants, two separate per-tenant DSNs carrying `options=-c app.tenant_id=...`, each running a real two-stage workflow through the public API. Every EARS clause is now pinned: the engine operates correctly against the tenant-scoped tables; application is stored and filtered as an opaque string; and a future change that breaks RLS operation fails this probe.

THREE CONTROLS, each guarding a way the result could have been vacuous:
 - THE GUC CONTROL. current_setting('app.tenant_id') is asserted to be 'tenant-a' BEFORE any workflow runs. Without it, a DSN that silently dropped the options parameter would make every policy deny, and the failure would look like engine incompatibility rather than a malformed DSN.
 - THE LEAK-CHECK CONTROL. After asserting each tenant sees exactly one row, RLS is bypassed on the owner connection and both rows are counted. An engine that wrote nothing at all would satisfy isolation perfectly.
 - THE LOCKED-READ CHECK. Queue polling uses FOR UPDATE SKIP LOCKED, and a policy that admits plain SELECT does not necessarily admit a locked read. Asserted separately rather than assumed from the workflow passing.

A REAL FINDING ABOUT THE POLICY SHAPE, and it is the kind of thing that would have been reported to us as an engine defect. My first attempt installed ONLY the RESTRICTIVE policies the ticket names. Every INSERT was refused:

    psycopg.errors.InsufficientPrivilege: new row violates row-level security
    policy for table "pipeline_executions"

PostgreSQL denies by default, and RESTRICTIVE policies only SUBTRACT from permissive ones. A purely restrictive policy set therefore denies everything, including the tenant's own rows. A correct deployment needs a PERMISSIVE grant AND a RESTRICTIVE tenant constraint. That is now in the probe with a comment explaining it, because the symptom - the engine cannot write under RLS - looks exactly like an engine incompatibility and would have been reported as one.

WORTH PASSING TO provenance-50ca06 when they supply their policies: check they carry a permissive grant alongside the restrictive isolation policy. If they hit InsufficientPrivilege on INSERT, that is the cause and it is not in the engine.

ALSO FIXED HERE: the probe applies migrations from the package OR from a source checkout. stabilize mg-up resolves them through importlib from the installed package, where they are force-included at build time; in a checkout they live at the repository root and mg-up reports "No migrations found in package". Any probe calling mg_up from a checkout hits this, which is a real usability wrinkle worth its own ticket but not this one.

NOT COVERED, printed by the probe rather than left implied: crash-recovery sweeps run from whichever processor is live, so under RLS a tenant's orphans are only recoverable while something runs for that tenant; and connection count scales at one pool per tenant DSN. Both are deployment properties, not engine behaviour.

Unblocked by #18 being closed.
