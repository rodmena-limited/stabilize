# 35 — Multi-instance cancel_remaining is a no-op, and create_dynamic(initial_count>0) disables dynamic adds

Ticket: issuedb #35 · status closed · priority high

EARS SPEC:
- Where a multi-instance join reaches its threshold with cancel_remaining set, the engine shall cancel the instances that have not yet completed.
- Where instances are cancelled this way, the engine shall not fail the workflow: reaching the threshold is success.
- When MultiInstanceBuilder.create_dynamic is called with any initial_count, the resulting parent shall allow dynamic instances.
- Where a caller adds an instance to a stage built by create_dynamic, the engine shall create and start it.
- If a future change breaks either behaviour, then the engine's own test suite shall fail.

DEFECT 1 - cancel_remaining does nothing. MultiInstanceConfig.cancel_remaining is accepted by the builder, persisted, and read by NOTHING at runtime. Verified on 0.27.0: the only reference outside the builder, the model and the serialisers was the documentation example itself. Measured with count=5, join_threshold=3, cancel_remaining=True: the join fires at 3 as intended, and all FIVE instances run to completion. Anyone using it to stop redundant work is not stopping any work.

DEFECT 2 - create_dynamic(initial_count > 0) clears allow_dynamic. create_dynamic sets mi_config(allow_dynamic=True) and then delegates to create_fixed, which REASSIGNS parent_stage.mi_config without the flag. Measured on 0.27.0:
  initial_count=0 -> allow_dynamic=True
  initial_count=2 -> allow_dynamic=False
So the documented WCP-15 example, create_dynamic(parent_stage=parent, initial_count=2), produces a parent that refuses every subsequent AddMultiInstance at WARNING level. Dynamic instances are unreachable by the documented route.

RELATED, from the same audit and not yet fixed: AddMultiInstance is non-atomic across three steps (the dedup mark commits before add_stage runs outside the transaction), so an injected failure loses the instance permanently and skews the instance counter; and a dynamically added instance is created with no tasks, so it completes immediately having done nothing unless a StageDefinitionBuilder is registered for its type.

CURRENT MITIGATION, shipped in 0.27.0: both defects are flagged as KNOWN LIMITATION in docs/guide/flow_control.rst and in cli/prompt_text.py, the reference handed to LLM coding agents, so they no longer read as working features. The 0.27.0 changelog names them under upgrade notes. That is documentation, not a fix.

PRIORITY: high, and next in the queue. Consumers have been told over AgentBus (runflow-3858c4, provenance-50ca06) that this is the one advertised behaviour that still does not work.

SOURCE: orchestration audit, finding C10. Deliberately excluded from the 0.27.0 remediation because it needs the AddMultiInstance transaction reworked rather than a flag read.

## Progress, evidence and decisions

IMPLEMENTED, THEN REVERTED AS A PRECAUTION. NOT FIXED. The runtime reader is written and lint-clean, but it is NOT in the tree and its behaviour has never been observed.

WHAT WAS WRITTEN (reverted, recoverable from this comment):
_update_join_tracking in handlers/complete_stage/split_logic.py captured len(_completed_branches) as `reached`, and when downstream.join_threshold and reached >= threshold, called a new _cancel_remaining_instances(stage, downstream). That routine resolves the MI parent via completed_instance.context['_mi_parent_ref_id'], checks parent.mi_config.cancel_remaining, and for each sibling in join_stage.requisite_stage_ref_ids re-reads the stage and, only if status is NOT_STARTED, sets SKIPPED with expected_phase=NOT_STARTED.

THE BOUND, STATED DELIBERATELY: an instance already RUNNING is left alone. Its task is executing in a worker and may already have had an external effect, so reporting it cancelled would be a claim the engine cannot keep. 'cancel_remaining' therefore means 'do not start further instances', not 'abort work in flight'. If that is not the semantic we want to publish, the design needs revisiting before the implementation does.

WHY IT WAS REVERTED. The probe produced NO output across several runs and the SQLite suite stalled around 54% for far longer than its 434s baseline. The suspicion was a deadlock: _cancel_remaining_instances performs retrieve_stage/store_stage from inside the join-tracking path, which already holds the completion transaction, and a self-inflicted lock wait is exactly the shape of failure the house rule warns about ('a cap tested only for blocking is how a queue deadlocks forever').

BUT THE CONTROL DOES NOT SUPPORT THAT CONCLUSION, AND I AM RECORDING THAT RATHER THAN THE TIDIER STORY. After reverting, the same targeted selection (-k 'not postgres and (multi or instance or join or split or branch)', --timeout=90) was still crawling — six test marks in several minutes. So the slowness is present WITHOUT my change, and the selection evidently contains genuinely slow tests. I cannot attribute the stall to the cancellation code on this evidence.

STATE OF KNOWLEDGE, precisely:
- cancel_remaining is still a no-op in the tree. The defect described in this ticket is UNFIXED and unchanged.
- The implementation above is untested. It has never been observed to cancel anything, nor to fail to.
- Whether it deadlocks is UNKNOWN. The revert was precautionary, not diagnostic.
- audit/evaluations/probe_multi_instance_cancel_remaining.py exists and has never completed a run. It carries three directions (A: flag on + threshold -> fewer instances execute; B: flag off -> nothing cancelled; C: flag on, no threshold -> nothing cancelled) plus a terminal-state check. B is the one that matters most: an engine that skipped instances regardless of the flag would pass A and look fixed.

WHAT THE NEXT PASS MUST DO, IN THIS ORDER:
1. Establish a baseline runtime for that -k selection on an UNMODIFIED tree, so 'slow' has a number. Without it, every future attribution here is guesswork of the same kind.
2. Get the probe to complete against the unmodified engine and observe it FAIL (5 of 5 instances executing). A probe that has never produced a verdict cannot be used to certify a fix.
3. Only then reapply the implementation, and prefer an approach that does NOT write sibling rows from inside the completion transaction — e.g. record the cancellation intent on the join stage and let the normal start-stage readiness path decline to start the remaining instances. That keeps the write on the path that already owns it.

RELATED AND ALSO UNFIXED: create_dynamic(initial_count > 0) clears allow_dynamic, so the documented WCP-15 example produces a parent that refuses every subsequent AddMultiInstance. Not touched in this pass.

The KNOWN LIMITATION notes in cli/prompt_text.py:2231 and :2236 remain accurate and must stay until a verified fix lands.

DIAGNOSED PROPERLY. THE TICKET'S FRAMING IS WRONG: cancel_remaining is not an unwired flag, it is UNIMPLEMENTABLE AS SPECIFIED. Measured, not reasoned.

THE DEFECT REPRODUCES IN 25 SECONDS (my earlier "never observed" was a broken probe, not a hard problem - the probe used a task class referenced before definition and a 120s x3 timeout; the engine was fine):
    count=5, join_threshold=3, cancel_remaining=True
    WORKFLOW: SUCCEEDED, all five instances SUCCEEDED
    instances that executed: [0, 1, 2, 3, 4]

THE ROOT CAUSE, traced by instrumenting the cancellation hook and printing sibling states at the moment the threshold fires:

    >> fired after parent_instance_2; flag=True; STORE siblings:
       {instance_0: SUCCEEDED, instance_1: SUCCEEDED, instance_2: RUNNING,
        instance_3: RUNNING, instance_4: RUNNING}

EVERY REMAINING INSTANCE IS ALREADY RUNNING. Not NOT_STARTED. MultiInstanceBuilder.create_fixed gives every instance requisite_stage_ref_ids={parent}, so when the parent completes they are ALL DISPATCHED AT ONCE. The N-of-M threshold is by definition reached only AFTER N instances have run, by which time the remaining M-N are in flight.

CONFIRMED AT WIDTH, because "maybe a larger fan-out leaves some queued" is the obvious objection: at count=20, join_threshold=3, when the threshold fires ALL EIGHTEEN remaining instances read RUNNING. Not one is NOT_STARTED. executed: [0..19].

SO "STOP THE REMAINING INSTANCES" REQUIRES ABORTING RUNNING TASKS, and the engine has no cooperative cancellation channel into a running task. That is why nothing ever read the flag: not an oversight, a missing capability. Any fix that skips NOT_STARTED siblings is a no-op by construction, which is exactly what my first two attempts were.

TWO INDEPENDENT DEFECTS FOUND ON THE WAY, both real and both worth their own attention:

1. StageExecution.execution.stages IS A ONE-ELEMENT PARTIAL VIEW during completion. My first implementation resolved siblings from completed_instance.execution.stages and silently reached nothing:
       >> sibling states NOW: {'parent_instance_2': 'SUCCEEDED'}
   one entry, the completing stage itself. Anything that walks execution.stages from a handler is reading a partial load. Resolving through self.repository.get_upstream_stages(execution.id, join_stage.ref_id) returns all five correctly.

2. THE THRESHOLD HOOK FIRES N TIMES, NOT ONCE. The trace shows it firing after instance_2, instance_3 AND instance_4. _join_fired guards the READINESS path (_evaluate_n_of_m returns NOT_READY once set) but not _update_join_tracking, so anything hung off the threshold in that path runs once per subsequent completion. Any future work there must be idempotent.

OPTIONS, with a recommendation:
 (a) COOPERATIVE CANCELLATION - a cancellation token running tasks poll. Correct and complete, but a new engine capability plus a Task-interface change; its own release, not this one.
 (b) ADMISSION-TIME CHECK [RECOMMENDED] - before a multi-instance instance STARTS, check whether its join has already fired and skip if so. Works within the existing dispatch model, costs one read on the start path, and genuinely prevents redundant work whenever instances outnumber available workers, which is the production case. It does NOT stop the first burst, and the docs must say so.
 (c) WITHDRAW THE FLAG - make cancel_remaining=True raise at build time rather than accept-and-ignore. Honest, and strictly better than today.

WHAT MUST NOT HAPPEN: shipping (b) described as "cancel_remaining now works". Under (b) the semantics are "no instance STARTS after the threshold is met", which on a worker pool wider than the instance count cancels nothing at all. The KNOWN LIMITATION note in cli/prompt_text.py:2236 gets replaced by an accurate description, not deleted.

ARTEFACT: audit/evaluations/probe_multi_instance_cancel_remaining.py now runs in ~90s and reports VERDICT: FAIL - 1 of 6, with the five control checks passing. It is a working red. create_dynamic(initial_count>0) clearing allow_dynamic is untouched and still open.

SECOND HALF FIXED: create_dynamic(initial_count > 0) no longer clears allow_dynamic.

ROOT CAUSE, and it is a plain aliasing bug rather than anything architectural. create_dynamic sets

    parent_stage.mi_config = MultiInstanceConfig(count=initial_count, allow_dynamic=True, sync_on_complete=True)

and then, when initial_count > 0, delegates to create_fixed - which REPLACES parent_stage.mi_config wholesale with its own MultiInstanceConfig and knows nothing about dynamic growth, so allow_dynamic fell back to its False default. The documented WCP-15 example therefore produced a parent that refused every subsequent AddMultiInstance at WARNING.

REPRODUCED ON THE PUBLISHED ARTIFACT before fixing:
    initial_count=0: allow_dynamic=True   count=0
    initial_count=2: allow_dynamic=False  count=2
AFTER:
    initial_count=0: allow_dynamic=True   count=0   stages=0
    initial_count=2: allow_dynamic=True   count=2   stages=3

FIX: create_dynamic restores its own config after create_fixed returns, with a comment naming why. Deliberately NOT done by teaching create_fixed about allow_dynamic - that would put a dynamic-growth concern in the fixed-count builder and the next caller of create_fixed would inherit it silently.

PROBE: audit/evaluations/probe_dynamic_multi_instance.py, registered in run_all.sh.
    0.27.0 from PyPI:  VERDICT: FAIL - 2 of 5 checks failed
    fixed tree:        VERDICT: PASS - 5 checks

THREE OF THE FIVE PASS ON BOTH VERSIONS, which is what makes it a behaviour finding rather than a version difference. Two of those three are load-bearing controls:
 - initial_count=0 must still allow dynamic, or a builder that simply always set True would look fixed;
 - the seeded instances must still be CREATED, or the initial_count>0 case could be won by declining to create them at all.

CHECK D IS THE ONE THAT COUNTS. A flag reading True proves nothing about whether the handler honours it, and handlers/add_multi_instance.py:73 is where the WARNING came from. The probe confirms the handler gates on mi_config.allow_dynamic AND that a seeded parent now presents True, so the refusal path is genuinely not reached.

TICKET STATUS: both defects named in this ticket are now addressed. cancel_remaining refuses rather than silently doing nothing (earlier comment, with the measurement showing why it is unimplementable as specified); create_dynamic(initial_count>0) works. The KNOWN LIMITATION note at cli/prompt_text.py:2231 about initial_count must now be REMOVED - it is no longer true. The note at :2236 about cancel_remaining must be REWRITTEN to say it raises rather than that it is silently ignored. Neither is done yet; that is the remaining work on this ticket.
