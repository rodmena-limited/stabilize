from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue
from tests.conftest import CounterTask, setup_stabilize

class TestPipelineIntegration:
    """Parameterized integration tests - runs on both SQLite and PostgreSQL."""
