# 86 — A running branch is never failed by CompleteWorkflow's wait budget (C9)

Ticket: #86. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

Status: CONFIRMED on published 0.32.1 (round 1b, pipeline 2933: TERMINAL with the slow leaf CANCELED on both backends; the control was also red because get_handler_config() caches the environment per process — probe now resets it; round 1c confirms the control) and by the peer (v3 #6, P5: two leaves, MAX_STAGE_WAIT_RETRIES=3: workflow TERMINAL at 2.2 s and the slow leaf canceled). Our probe probe_parallel_leaf_not_terminal.py was green in round 1 because QueueProcessor(handler_config=...) never reached CompleteWorkflowHandler (#85); re-run with the environment in round 1b (pipeline 2933).

## EARS spec

- While any top-level stage is RUNNING, PAUSED, SUSPENDED or BUFFERED, CompleteWorkflow shall neither re-queue itself nor mark the workflow TERMINAL; it shall leave the workflow to the completion path of that stage.
- If no top-level stage is active and some are incomplete, then CompleteWorkflow shall keep its bounded re-queue (max_stage_wait_retries) and then mark the workflow TERMINAL, as today.
- A parallel branch running for longer than max_stage_wait_retries x handler_retry_delay_seconds shall end SUCCEEDED (probe: 3 s against a 0.6 s budget), and a SUSPENDED branch waiting for a signal shall not be failed by another branch's completion.

## Technical problems

1. Completion detection in a fork/join DAG: the "wait for siblings" loop is a timeout on legitimate work.

## Solution domains

- Event-driven completion: each branch's terminal transition is the event that re-evaluates the workflow (every leaf CompleteStage pushes CompleteWorkflow; failures push it at once).

## Alternatives

- Stop polling while a stage is active [CHOSEN] vs a larger max_stage_wait_retries [REJECTED: moves the cliff; days-long SUSPENDED approval branches still fail] vs per-workflow configurable budget [REJECTED: the budget has no correct value for unbounded work].

## Callers

- _determine_final_status: CompleteWorkflowHandler._handle_with_retry only. Producers of CompleteWorkflow: complete_stage/handler.py (5 sites: leaf success, failure, halted re-delivery, blocking failure, error path), workflow_control (CancelWorkflow), recovery (#84 new). Each fires on a terminal stage transition, so a later active-stage completion always produces another CompleteWorkflow.

## Die-here / two-at-once

- The last active stage's CompleteWorkflow lost: #84 recovery pushes CompleteWorkflow for a RUNNING workflow whose stages are all complete.
- A stage wedged forever (a bug elsewhere) now leaves the workflow RUNNING instead of a false TERMINAL after 1 h: visible in status, healed by #84 where it is a lost message.
