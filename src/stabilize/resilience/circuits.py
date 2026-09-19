"""
Circuit breaker management for Stabilize.

Provides per-workflow circuit breakers using CircuitProtectorPolicy
from resilient_circuit.
"""

from __future__ import annotations

import logging
import os
import threading
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


class CircuitStorageUnavailableError(RuntimeError):
    """Raised when shared breaker storage was requested but cannot be created."""


def _strict_storage_required() -> bool:
    """Whether an unusable PostgreSQL breaker store must abort startup."""
    return os.environ.get("STABILIZE_CIRCUIT_STORAGE_STRICT", "").lower() in {"1", "true", "yes"}


def _degrade_or_raise(reason: str, error: BaseException) -> CircuitBreakerStorage:
    """Handle a PostgreSQL breaker-store failure.

    A ``postgresql://`` URL is an explicit request for circuit state SHARED
    across instances. Silently substituting process-local state means a
    breaker that should be open everywhere stays closed on every other
    worker, so the failure is reported at ERROR, and raises outright when
    STABILIZE_CIRCUIT_STORAGE_STRICT is set.
    """
    if _strict_storage_required():
        raise CircuitStorageUnavailableError(
            f"PostgreSQL circuit breaker storage is unavailable ({reason}) and "
            "STABILIZE_CIRCUIT_STORAGE_STRICT is set"
        ) from error
    logger.error(
        "PostgreSQL circuit breaker storage unavailable (%s). Falling back to IN-MEMORY "
        "storage: circuit state is now PROCESS-LOCAL and is NOT shared across instances, "
        "so a breaker open on this worker stays closed on every other one. Set "
        "STABILIZE_CIRCUIT_STORAGE_STRICT=1 to fail startup instead.",
        reason,
    )
    return InMemoryStorage()


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

            # Pass the connection string through verbatim. psycopg3 accepts
            # both `postgresql://` URLs and libpq conninfo, and RC's
            # PostgresStorage forwards it straight to psycopg.connect() — so a
            # URL's query string survives intact. Hand-parsing the URL into
            # `host=... dbname=...` here used to drop TLS options (sslmode,
            # sslcert, sslkey, sslrootcert), silently forcing the breaker onto
            # in-memory storage on TLS-mandatory databases.
            conn_string = database_url.replace("+psycopg", "")

            storage = PostgresStorage(connection_string=conn_string)
        except ImportError as exc:
            return _degrade_or_raise("psycopg/resilient-circuit not available", exc)
        except Exception as exc:
            return _degrade_or_raise(f"{type(exc).__name__}: {exc}", exc)
        # Logged only once construction has actually succeeded: an INFO line
        # emitted before the attempt reads as confirmation of a backend that
        # may never have been created.
        logger.info("Using PostgreSQL storage for circuit breakers")
        return storage
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
        # get_circuit/clear_workflow_circuits run on concurrent QueueProcessor
        # worker threads; OrderedDict mutation (move_to_end/popitem/del) is not
        # thread-safe on its own.
        self._lock = threading.Lock()

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

        with self._lock:
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
        with self._lock:
            keys_to_remove = [key for key in self._circuits if key[0] == workflow_execution_id]
            for key in keys_to_remove:
                del self._circuits[key]

        if keys_to_remove:
            logger.debug("Cleared %d circuit(s) for workflow=%s", len(keys_to_remove), workflow_execution_id)
