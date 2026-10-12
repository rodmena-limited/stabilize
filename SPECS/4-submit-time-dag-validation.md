# Spec: submit-time DAG validation (#4)

## EARS requirements

- When Workflow.create() receives stages, the engine shall validate the stage
  graph (cycles including self-edges, unknown requisite_stage_ref_ids,
  duplicate ref_ids) and raise a clear error naming the offending stages,
  instead of discovering the defect at runtime as "Exceeded max retries
  waiting for upstream stages".
- The cycle error shall name the cycle members.
- Where conditional execution semantics exist, the validator should flag nodes
  that can never run under any assignment of upstream terminal states.

## State found

The first two clauses were already implemented: validate_stage_graph() is
called from Workflow.create and raises InvalidStageGraphError for duplicate
refs, self-edges and unknown refs, and CircularDependencyError naming the
members for cycles. Verified by running all five defect classes, with a
control confirming valid DAGs are still accepted.

## Added for the third clause

Conditional execution in this engine is expressed as join semantics
(join_type, join_threshold) rather than predicates, so "can never run under
any assignment" has a decidable subset: an N_OF_M join whose threshold exceeds
its upstream count can never become ready, whatever every upstream does.

The engine ALREADY detected this — in recovery.py, during a crash-recovery
sweep, logging "join_threshold=%d but only %d upstreams — unreachable". That
is far too late: the workflow has run, stalled, and been picked up by a sweep
before anyone learns the graph was impossible. The same check now runs at
submit time and refuses the workflow.

Controls: thresholds of 1-of-2 and 2-of-2 are still accepted, and other join
types are untouched, so the check cannot pass by refusing every N_OF_M join.

## Not implemented, and why

Full condition-aware reachability over split_conditions / deferred_choice_group
is not implemented. Those express data-dependent branching whose satisfiability
is not decidable from the graph alone, and an approximation would either reject
valid workflows or give false assurance. The decidable case is done; the rest
is left open rather than papered over.
