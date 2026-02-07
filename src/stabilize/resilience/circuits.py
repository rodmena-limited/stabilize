"""
Circuit breaker management for Stabilize.

Provides per-workflow circuit breakers using CircuitProtectorPolicy
from resilient_circuit.
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from datetime import timedelta
from typing import TYPE_CHECKING

from resilient_circuit import CircuitProtectorPolicy
from resilient_circuit.storage import CircuitBreakerStorage, InMemoryStorage

from stabilize.errors import is_transient
from stabilize.resilience.config import ResilienceConfig

if TYPE_CHECKING:
    pass


def _should_trip_circuit(error: Exception | None) -> bool:
    """Determine if an error should count towards circuit breaker failure threshold.

    TransientErrors are expected (retryable) failures and should NOT trip the circuit.
    Only permanent/unexpected errors should trip the circuit breaker.

    Also handles BulkheadError wrapping where the original TransientError is in __cause__.

    Args:
        error: The exception to check (may be None)

    Returns:
        True if this error should count as a circuit breaker failure
    """
    if error is None:
        return True  # No error info means unexpected failure
    return not is_transient(error)


logger = logging.getLogger(__name__)


def _create_storage(database_url: str | None) -> CircuitBreakerStorage:
    """
    Create circuit breaker storage based on database URL.

    Args:
        database_url: Database connection string

    Returns:
        PostgresStorage for PostgreSQL, InMemoryStorage otherwise
    """
    if database_url and database_url.startswith("postgresql"):
        try:
            from resilient_circuit.storage import PostgresStorage

            # Convert SQLAlchemy-style URL to psycopg format if needed
            # postgresql+psycopg://user:pass@host/db -> host=host dbname=db user=user password=pass
            conn_string = database_url
            if "+psycopg" in conn_string:
                conn_string = conn_string.replace("+psycopg", "")
            if "postgresql://" in conn_string:
                conn_string = conn_string.replace("postgresql://", "")

            # Parse URL format: user:pass@host:port/dbname
            # Convert to libpq format: host=X dbname=Y user=Z password=W
            if "@" in conn_string:
                auth, hostdb = conn_string.split("@", 1)
                if ":" in auth:
                    user, password = auth.split(":", 1)
                else:
                    user, password = auth, ""

                if "/" in hostdb:
                    hostport, dbname = hostdb.rsplit("/", 1)
                else:
                    hostport, dbname = hostdb, "stabilize"

                if ":" in hostport:
                    host, port = hostport.rsplit(":", 1)
                else:
                    host, port = hostport, "5432"

                conn_string = (
                    f"host={host} port={port} dbname={dbname} user={user} password={password}"
                )

            logger.info("Using PostgreSQL storage for circuit breakers")
            return PostgresStorage(connection_string=conn_string)
        except ImportError:
            logger.warning(
                "psycopg not available, falling back to in-memory circuit breaker storage"
            )
            return InMemoryStorage()
        except Exception as e:
            logger.warning(
                "Failed to create PostgreSQL storage: %s, falling back to in-memory storage", e
            )
            return InMemoryStorage()
    else:
        # SQLite or no database: use in-memory storage
        # Circuit state is per-process only (not shared across instances)
        logger.info("Using in-memory storage for circuit breakers (SQLite or no database)")
        return InMemoryStorage()


class WorkflowCircuitFactory:
    """
    Creates per-workflow, per-task-type circuit breakers.

    Each workflow execution gets isolated circuit breakers, so failures
    in one workflow don't affect others.

    Storage selection:
    - PostgreSQL: Shared state across instances via PostgresStorage
      (table created automatically by resilient_circuit)
    - SQLite: In-memory storage (per-process, not shared)

    Example:
        config = ResilienceConfig.from_env()
        factory = WorkflowCircuitFactory(config)

        # Get circuit for a specific workflow and task type
        circuit = factory.get_circuit(
            workflow_execution_id="01ABC...",
            task_type="http"
        )

        @circuit
        def make_request():
            return requests.get("https://api.example.com")
    """

    def __init__(self, config: ResilienceConfig) -> None:
        """
        Initialize the circuit factory.

        Args:
            config: Resilience configuration
        """
        self.config = config
        self._storage = _create_storage(config.database_url)
        self._circuits: OrderedDict[tuple[str, str], CircuitProtectorPolicy] = OrderedDict()

    def get_circuit(
        self,
        workflow_execution_id: str,
        task_type: str,
    ) -> CircuitProtectorPolicy:
        """
        Get or create a circuit breaker for a workflow + task type.

        Args:
            workflow_execution_id: The workflow execution ID (used as namespace)
            task_type: The task type (used as resource_key)

        Returns:
            CircuitProtectorPolicy for this workflow + task type combination
        """
        key = (workflow_execution_id, task_type)

        if key in self._circuits:
            # Move to end (most recently used)
            self._circuits.move_to_end(key)
            return self._circuits[key]

        # Evict if full
        if len(self._circuits) >= self.config.circuit_cache_size:
            # Remove oldest (first item)
            self._circuits.popitem(last=False)

        circuit = CircuitProtectorPolicy(
            resource_key=task_type,
            storage=self._storage,
            namespace=workflow_execution_id,
            failure_limit=self.config.circuit_failure_threshold,
            cooldown=timedelta(seconds=self.config.circuit_cooldown_seconds),
            # Don't trip circuit on TransientErrors - they're expected retryable failures
            should_handle=_should_trip_circuit,
        )
        self._circuits[key] = circuit
        logger.debug(
            "Created circuit breaker for workflow=%s, task_type=%s",
            workflow_execution_id,
            task_type,
        )

        return circuit

    def clear_workflow_circuits(self, workflow_execution_id: str) -> None:
        """
        Remove all circuits for a completed workflow.

        Call this when a workflow completes to free memory.

        Args:
            workflow_execution_id: The workflow execution ID
        """
        keys_to_remove = [key for key in self._circuits if key[0] == workflow_execution_id]
        for key in keys_to_remove:
            del self._circuits[key]

        if keys_to_remove:
            logger.debug(
                "Cleared %d circuit(s) for workflow=%s", len(keys_to_remove), workflow_execution_id
            )
