Stabilize Documentation
=======================

.. image:: https://img.shields.io/pypi/v/stabilize.svg
   :target: https://pypi.org/project/stabilize/
   :alt: PyPI Version

**Stabilize** is a lightweight, high-performance Python workflow engine designed for **massively parallel** execution and **"enterprise-grade" reliability**. It features a DAG-based orchestration model, robust concurrency control, and pluggable architecture.

Key Features
------------

*   **Reliable Execution**: Built on the "Transactional Outbox" pattern with atomic state transitions.
*   **Concurrency Control**: Optimistic locking prevents "lost updates" in high-throughput environments.
*   **Massive Parallelism**: Supports dynamic forking, parallel branches, and joins.
*   **Pluggable Backends**: SQLite for development, PostgreSQL for production.
*   **Resilience**: Built-in retries, timeouts, circuit breakers, and bulkheads.
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

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
