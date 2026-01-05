"""Pytest configuration for golden standard tests.

Re-exports fixtures from the main tests/conftest.py to ensure
golden standard tests run on both SQLite and PostgreSQL backends.
"""

# Re-export setup_stabilize helper for test files
# Import ALL fixtures including the postgres container
# This ensures pytest can resolve the full fixture dependency graph
from tests.conftest import (  # noqa: F401
    backend,
    postgres_container,
    postgres_url,
    queue,
    repository,
    reset_connection_manager,
    setup_stabilize,  # noqa: F401
)
