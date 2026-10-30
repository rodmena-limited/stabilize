from unittest.mock import patch

from resilient_circuit.storage import InMemoryStorage

from stabilize.resilience.bulkheads import TaskBulkheadManager
from stabilize.resilience.circuits import WorkflowCircuitFactory
from stabilize.resilience.config import ResilienceConfig
from stabilize.resilience.executor import execute_with_resilience


# =============================================================================
# Test Workflow Circuit Factory
# =============================================================================


class TestWorkflowCircuitFactory:
    """Test workflow circuit factory behavior."""

    def test_factory_creates_circuits_per_workflow(self) -> None:
        """Each workflow gets isolated circuits."""
        config = ResilienceConfig()
        factory = WorkflowCircuitFactory(config)

        circuit1 = factory.get_circuit("workflow_1", "http")
        circuit2 = factory.get_circuit("workflow_2", "http")

        # Different workflows get different circuits
        assert circuit1 is not circuit2

    def test_factory_caches_circuits(self) -> None:
        """Same workflow+task_type returns same circuit."""
        config = ResilienceConfig()
        factory = WorkflowCircuitFactory(config)

        circuit1 = factory.get_circuit("workflow_1", "http")
        circuit2 = factory.get_circuit("workflow_1", "http")

        # Same key returns same circuit
        assert circuit1 is circuit2

    def test_factory_different_task_types(self) -> None:
        """Different task types in same workflow get different circuits."""
        config = ResilienceConfig()
        factory = WorkflowCircuitFactory(config)

        http_circuit = factory.get_circuit("workflow_1", "http")
        shell_circuit = factory.get_circuit("workflow_1", "shell")

        assert http_circuit is not shell_circuit

    def test_factory_clear_workflow_circuits(self) -> None:
        """Can clear circuits for a completed workflow."""
        config = ResilienceConfig()
        factory = WorkflowCircuitFactory(config)

        # Create some circuits
        factory.get_circuit("workflow_1", "http")
        factory.get_circuit("workflow_1", "shell")
        factory.get_circuit("workflow_2", "http")

        # Clear workflow_1 circuits
        factory.clear_workflow_circuits("workflow_1")

        # workflow_1 circuits should be gone, workflow_2 should remain
        assert ("workflow_1", "http") not in factory._circuits
        assert ("workflow_1", "shell") not in factory._circuits
        assert ("workflow_2", "http") in factory._circuits

    def test_factory_uses_inmemory_for_sqlite(self) -> None:
        """Factory uses InMemoryStorage when no PostgreSQL URL."""
        config = ResilienceConfig(database_url=None)
        factory = WorkflowCircuitFactory(config)

        assert isinstance(factory._storage, InMemoryStorage)

    def test_factory_uses_inmemory_for_sqlite_url(self) -> None:
        """Factory uses InMemoryStorage for SQLite URLs."""
        config = ResilienceConfig(database_url="sqlite:///test.db")
        factory = WorkflowCircuitFactory(config)

        assert isinstance(factory._storage, InMemoryStorage)

    def test_postgres_url_passes_tls_params_through(self) -> None:
        """TLS query params on a postgres URL survive _create_storage.

        Regression for the resilient-circuit pin widen: hand-parsing the URL
        into libpq `host=... dbname=...` used to drop sslmode/sslcert/sslkey,
        silently forcing the breaker onto in-memory storage on TLS databases.
        """
        tls_url = (
            "postgresql://user:pass@db.example:5432/stabilize"
            "?sslmode=verify-full&sslcert=/etc/ssl/client.crt&sslkey=/etc/ssl/client.key"
        )
        config = ResilienceConfig(database_url=tls_url)

        with patch("resilient_circuit.storage.PostgresStorage") as mock_storage:
            factory = WorkflowCircuitFactory(config)
            factory.get_circuit("wf1", "http")

        # PostgresStorage must receive the URL verbatim (only the +psycopg
        # driver suffix stripped) so psycopg sees the TLS query params.
        assert mock_storage.called
        conn_arg = mock_storage.call_args.kwargs.get("connection_string")
        if conn_arg is None:
            conn_arg = mock_storage.call_args[0][0]
        assert conn_arg == tls_url
        assert "sslmode=verify-full" in conn_arg
        assert "sslcert=" in conn_arg and "sslkey=" in conn_arg

    def test_postgres_plus_psycopg_scheme_stripped(self) -> None:
        """postgresql+psycopg:// scheme is normalized to postgresql://."""
        url = "postgresql+psycopg://user:pass@db.example:5432/stabilize?sslmode=require"
        config = ResilienceConfig(database_url=url)

        with patch("resilient_circuit.storage.PostgresStorage") as mock_storage:
            factory = WorkflowCircuitFactory(config)
            factory.get_circuit("wf1", "http")

        assert mock_storage.called
        conn_arg = mock_storage.call_args.kwargs.get("connection_string")
        if conn_arg is None:
            conn_arg = mock_storage.call_args[0][0]
        assert conn_arg == "postgresql://user:pass@db.example:5432/stabilize?sslmode=require"


# =============================================================================
# Test Integration with Handler (Unit Level)
# =============================================================================


class TestResilienceIntegration:
    """Test resilience integration patterns."""

    def test_manager_and_factory_work_together(self) -> None:
        """BulkheadManager and CircuitFactory work together."""
        config = ResilienceConfig()
        bulkhead_manager = TaskBulkheadManager(config)
        circuit_factory = WorkflowCircuitFactory(config)

        # Simulate workflow execution
        workflow_id = "test_workflow_123"
        task_type = "http"

        circuit = circuit_factory.get_circuit(workflow_id, task_type)

        def task_execute() -> dict:
            return {"status": "success"}

        result = execute_with_resilience(
            bulkhead_manager=bulkhead_manager,
            circuit=circuit,
            task_type=task_type,
            func=task_execute,
            func_args=(),
            timeout=60.0,
        )

        assert result == {"status": "success"}

        # Cleanup
        circuit_factory.clear_workflow_circuits(workflow_id)
        bulkhead_manager.shutdown(wait=True, timeout=5.0)

    def test_multiple_workflows_isolated(self) -> None:
        """Multiple workflows have isolated circuit breakers."""
        config = ResilienceConfig()
        circuit_factory = WorkflowCircuitFactory(config)

        # Create circuits for different workflows
        workflow1_circuit = circuit_factory.get_circuit("wf1", "http")
        workflow2_circuit = circuit_factory.get_circuit("wf2", "http")

        # They should be independent
        assert workflow1_circuit is not workflow2_circuit

        # Tripping one shouldn't affect the other
        # (Internal state is per-circuit via namespace)

    def test_shutdown_cleanup(self) -> None:
        """Resources are properly cleaned up on shutdown."""
        config = ResilienceConfig()
        bulkhead_manager = TaskBulkheadManager(config)

        # Execute some work
        def simple_task() -> str:
            return "done"

        bulkhead_manager.execute_with_timeout("shell", simple_task, timeout=5.0)

        # Shutdown should not raise
        bulkhead_manager.shutdown(wait=True, timeout=5.0)
