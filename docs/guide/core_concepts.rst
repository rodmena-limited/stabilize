Core Concepts
=============

Architecture
------------

Stabilize follows a **Message-Driven Architecture** backed by the **Transactional Outbox** pattern.

1.  **Orchestrator**: The entry point. It receives a ``Workflow`` object and enqueues a ``StartWorkflow`` message.
2.  **Queue**: A persistent message queue (SQLite or PostgreSQL). Messages represent state transitions (e.g., ``StartStage``, ``CompleteTask``).
3.  **Handlers**: Specialized components that process messages. They perform atomic state updates and enqueue subsequent messages.
4.  **WorkflowStore**: The source of truth for workflow state. It supports Optimistic Locking to ensure data consistency.

Data Model
----------

Workflow
~~~~~~~~

The top-level container for an execution. Contains metadata (ID, status, application) and a list of Stages.

StageExecution
~~~~~~~~~~~~~~

A logical step in the workflow. Stages form a **Directed Acyclic Graph (DAG)**.

*   **Dependencies**: Defined by ``requisite_stage_ref_ids``. A stage starts only when all requisites complete.
*   **Context**: Inputs for the stage. Inherits outputs from upstream stages.
*   **Outputs**: Results produced by the stage.

TaskExecution
~~~~~~~~~~~~~

The atomic unit of work within a stage. A stage runs tasks sequentially.

*   **Implementing Class**: The registered name of the ``Task`` implementation to run.
*   **Status**: ``NOT_STARTED``, ``RUNNING``, ``SUCCEEDED``, ``TERMINAL``, etc.

The Loop
--------

The engine operates in a continuous loop:

1.  **Poll**: QueueProcessor picks a message (e.g., ``CompleteTask``).
2.  **Handle**: The appropriate Handler (e.g., ``CompleteTaskHandler``) is invoked.
3.  **Transact**: The Handler opens an atomic transaction.
    
    *   Updates state in ``WorkflowStore`` (e.g., sets Task status to SUCCEEDED).
    *   Enqueues next message (e.g., ``StartTask`` for next task, or ``CompleteStage``).

4.  **Commit**: The transaction commits. If it fails (e.g., optimistic lock error), it rolls back and retries.

Event Sourcing
--------------

Optionally, Stabilize can record every state transition as an immutable event.
When enabled via ``configure_event_sourcing()``, handlers automatically record
events to an event store and publish them to an in-process event bus.

This enables full audit trails, event replay for state reconstruction,
time-travel queries, and analytics projections. See :doc:`event_sourcing` for
the full guide.
