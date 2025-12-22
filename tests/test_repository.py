from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore

class TestWorkflowStore:
    """Parameterized repository tests - runs on both SQLite and PostgreSQL."""

    def test_health_check(self, repository: WorkflowStore) -> None:
        """Test database health check."""
        assert repository.is_healthy() is True
