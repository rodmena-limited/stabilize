Stabilize Documentation
=======================

.. image:: https://img.shields.io/pypi/v/stabilize.svg
   :target: https://pypi.org/project/stabilize/
   :alt: PyPI Version

**Stabilize** is a lightweight, high-performance Python workflow engine designed for **massively parallel** execution and **"enterprise-grade" reliability**. It features a DAG-based orchestration model, robust concurrency control, and pluggable architecture.

Key Features
------------

*   **Reliable Execution**: Built on the "Transactional Outbox" pattern with atomic state transitions.
*   **Concurrency Control**: Optimistic locking with phase-aware updates prevents "lost updates" in high-throughput environments.
*   **Massive Parallelism**: Supports dynamic forking, parallel branches, and joins.
*   **Pluggable Backends**: SQLite for development, PostgreSQL for production.
*   **Resilience**: Built-in retries, timeouts, circuit breakers, bulkheads, and bloom filter deduplication.
*   **Structured Errors**: Semantic error codes with chain traversal and automatic classification.
*   **Configuration Versioning**: Immutable configs with fingerprinting for reproducibility.
*   **Event Sourcing**: Full audit trail, event replay, projections, and time-travel debugging.
*   **43 Workflow Patterns**: Complete implementation of all WCP control-flow patterns (van der Aalst et al.) including OR-split/join, discriminator, N-of-M, deferred choice, milestones, mutex, signals, multi-instance, and structured loops.
*   **Observability**: Detailed structured logging and audit trails.

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   guide/getting_started
   guide/core_concepts
   guide/tasks
   guide/data_flow
   guide/flow_control
   guide/persistence
   guide/resilience
   guide/error_handling
   guide/event_sourcing

.. toctree::
   :maxdepth: 2
   :caption: Examples

   examples/simple_pipeline
   examples/parallel_processing
   examples/shell_commands
   examples/http_requests

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/models
   api/handlers
   api/persistence
   api/tasks
   api/queue
   api/events

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
