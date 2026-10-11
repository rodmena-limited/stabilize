# 85 — QueueProcessor handler_config reaches every handler (C37)

Ticket: #85. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

Status: CONFIRMED by reading on 0.32.1 (queue/processor/mixins.py:102-125); probe probe_handler_config_reaches_handlers.py pending CI. QueueProcessor(handler_config=X) passes X only to SignalStageHandler and RunTaskHandler; StartWorkflow, StartWaitingWorkflows, StartStage, CompleteTask, CompleteStage, CompleteWorkflow, CancelStage, CancelWorkflow and the rest use HandlerConfig.from_env(). This is why max_stage_wait_retries and default_page_size given to the processor had no effect in round 1.

## EARS spec

- Where QueueProcessor is given handler_config, every default handler it registers shall use that object.
- Where it is not given one, behaviour shall be unchanged (each handler resolves get_handler_config()).

## Technical problems

1. Configuration propagation: one documented input reaching only some consumers.

## Solution domains

- Dependency injection: the composition root passes the configuration to every component it builds.

## Alternatives

- Pass handler_config to every handler in _register_default_handlers and handlers/__init__.py [CHOSEN] vs a process-global setter [REJECTED: two processors in one process need different configs].

## Callers

- _register_default_handlers: QueueProcessor.__init__. The other registration helper in handlers/__init__.py to be listed in the implementation comment.
