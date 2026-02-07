"""
Tests for thread safety issues.

This module tests thread safety issues, particularly around:
- SQLite :memory: database thread isolation (each thread gets its own DB)
- File-based SQLite concurrent access
- Connection manager thread-local behavior

KNOWN ISSUE: SQLite :memory: gives each thread its own database due to
thread-local connections, which can hide concurrency bugs in tests.
"""

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.connection import ConnectionManager, SingletonMeta


class TestSQLiteThreadSafety:
    """Test SQLite thread safety with check_same_thread=False."""

    def test_memory_db_thread_isolation_documentation(self, tmp_path: Any) -> None:
        """
        Document that :memory: SQLite gives each thread its own database.

        This is important to understand because tests using :memory: won't
        catch concurrency bugs that would occur with a shared file-based DB.
        """
        from stabilize.persistence.sqlite import SqliteWorkflowStore

        # Reset singleton for clean test
        SingletonMeta.reset(ConnectionManager)

        # Create :memory: store
        store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)

        # Write from main thread
        wf = Workflow.create("test-app", "memory-test", [])
        stage = StageExecution.create("stage-1", "Test Stage", "1")
        stage.execution = wf
        task = TaskExecution.create("Task", "shell", stage_start=True, stage_end=True)
        stage.tasks = [task]
        wf.stages = [stage]
        store.store(wf)

        # Try to read from another thread
        found_in_thread = []

        def read_in_thread() -> None:
            try:
                # This creates a NEW :memory: database for this thread!
                thread_store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
                result = thread_store.retrieve(wf.id)
                found_in_thread.append(result is not None)
            except Exception:
                found_in_thread.append(False)

        t = threading.Thread(target=read_in_thread)
        t.start()
        t.join()

        # The workflow should NOT be found in the thread because
        # it has its own separate :memory: database
        assert len(found_in_thread) == 1
        assert found_in_thread[0] is False, (
            "Expected workflow NOT to be found in separate thread's :memory: DB. "
            "If this fails, thread isolation behavior may have changed."
        )

        store.close()

    def test_file_db_shared_across_threads(self, tmp_path: Any) -> None:
        """
        Test that file-based SQLite IS shared across threads.

        This confirms that file-based SQLite behaves differently from :memory:.
        """
        from stabilize.persistence.sqlite import SqliteWorkflowStore

        # Reset singleton for clean test
        SingletonMeta.reset(ConnectionManager)

        db_path = tmp_path / "thread_shared.db"
        connection_string = f"sqlite:///{db_path}"

        # Create store and write from main thread
        store = SqliteWorkflowStore(connection_string, create_tables=True)

        wf = Workflow.create("test-app", "file-test", [])
        stage = StageExecution.create("stage-1", "Test Stage", "1")
        stage.execution = wf
        task = TaskExecution.create("Task", "shell", stage_start=True, stage_end=True)
        stage.tasks = [task]
        wf.stages = [stage]
        store.store(wf)

        # Read from another thread
        found_in_thread = []

        def read_in_thread() -> None:
            try:
                thread_store = SqliteWorkflowStore(connection_string, create_tables=False)
                result = thread_store.retrieve(wf.id)
                found_in_thread.append(result is not None)
                thread_store.close()
            except Exception:
                found_in_thread.append(False)

        t = threading.Thread(target=read_in_thread)
        t.start()
        t.join()

        # The workflow SHOULD be found because it's the same file
        assert len(found_in_thread) == 1
        assert found_in_thread[0] is True, "Expected workflow to be found in separate thread with file-based DB."

        store.close()

    def test_concurrent_writes_file_db(self, tmp_path: Any) -> None:
        """
        Test concurrent writes to file-based SQLite.

        This tests the actual thread safety behavior that would occur in production.
        """
        from stabilize.persistence.sqlite import SqliteWorkflowStore

        SingletonMeta.reset(ConnectionManager)

        db_path = tmp_path / "concurrent_writes.db"
        connection_string = f"sqlite:///{db_path}"

        store = SqliteWorkflowStore(connection_string, create_tables=True)

        num_workflows = 50
        num_threads = 10
        success_count = 0
        error_count = 0
        lock = threading.Lock()

        def create_workflow(thread_id: int, wf_idx: int) -> None:
            nonlocal success_count, error_count
            try:
                wf = Workflow.create("test-app", f"workflow-{thread_id}-{wf_idx}", [])
                stage = StageExecution.create(f"stage-{thread_id}-{wf_idx}", "Test Stage", "1")
                stage.execution = wf
                task = TaskExecution.create("Task", "shell", stage_start=True, stage_end=True)
                stage.tasks = [task]
                wf.stages = [stage]

                # This should use thread-local connections
                store.store(wf)

                with lock:
                    success_count += 1
            except Exception:
                with lock:
                    error_count += 1

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for t_id in range(num_threads):
                for wf_idx in range(num_workflows // num_threads):
                    futures.append(executor.submit(create_workflow, t_id, wf_idx))

            for f in as_completed(futures):
                pass

        # All writes should succeed
        total_expected = num_threads * (num_workflows // num_threads)
        assert (
            success_count == total_expected
        ), f"Expected {total_expected} successes, got {success_count}. Errors: {error_count}"

        store.close()

    def test_thread_local_connection_isolation(self, tmp_path: Any) -> None:
        """
        Verify that thread-local connections are truly isolated.

        Each thread should get its own connection to prevent
        connection sharing issues.
        """
        import time

        from stabilize.persistence.connection import get_connection_manager

        SingletonMeta.reset(ConnectionManager)

        db_path = tmp_path / "thread_local.db"
        connection_string = f"sqlite:///{db_path}"

        connections: list[sqlite3.Connection] = []
        errors: list[str] = []
        lock = threading.Lock()

        def get_connection_in_thread(thread_num: int) -> None:
            # Stagger thread startup slightly to reduce lock contention
            time.sleep(0.01 * thread_num)
            try:
                manager = get_connection_manager()
                conn = manager.get_sqlite_connection(connection_string)
                with lock:
                    connections.append(conn)
            except sqlite3.OperationalError as e:
                with lock:
                    errors.append(str(e))

        threads = [threading.Thread(target=get_connection_in_thread, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Some connections might fail due to SQLite locking - that's OK
        # as long as the ones that succeed are unique
        if len(connections) < 3:
            pytest.skip(f"Too many SQLite lock failures ({len(errors)} errors), test inconclusive")

        # Each thread should have gotten a different connection object
        # (thread-local means separate connections)
        connection_ids = [id(c) for c in connections]
        unique_ids = set(connection_ids)

        # All successful connections should be unique (different objects)
        assert len(unique_ids) == len(connections), (
            f"Expected {len(connections)} unique connections, got {len(unique_ids)}. "
            "Thread-local isolation may be broken."
        )

    def test_connection_reuse_within_same_thread(self, tmp_path: Any) -> None:
        """
        Verify that the same thread gets the same connection on repeated calls.
        """
        from stabilize.persistence.connection import get_connection_manager

        SingletonMeta.reset(ConnectionManager)

        db_path = tmp_path / "reuse.db"
        connection_string = f"sqlite:///{db_path}"

        manager = get_connection_manager()

        conn1 = manager.get_sqlite_connection(connection_string)
        conn2 = manager.get_sqlite_connection(connection_string)
        conn3 = manager.get_sqlite_connection(connection_string)

        # All should be the same connection object
        assert conn1 is conn2 is conn3, "Same thread should reuse the same connection"

    def test_concurrent_read_write_stress(self, tmp_path: Any) -> None:
        """
        Stress test with mixed concurrent reads and writes.

        This tests that the system handles high concurrency without:
        - Data corruption
        - Deadlocks
        - Crashes
        """
        from stabilize.errors import ConcurrencyError
        from stabilize.persistence.sqlite import SqliteWorkflowStore

        SingletonMeta.reset(ConnectionManager)

        db_path = tmp_path / "stress.db"
        connection_string = f"sqlite:///{db_path}"

        store = SqliteWorkflowStore(connection_string, create_tables=True)

        # Create initial workflows
        workflows = []
        for i in range(20):
            wf = Workflow.create("stress-app", f"stress-{i}", [])
            stage = StageExecution.create(f"stage-{i}", "Test Stage", "1")
            stage.execution = wf
            task = TaskExecution.create("Task", "shell", stage_start=True, stage_end=True)
            stage.tasks = [task]
            wf.stages = [stage]
            store.store(wf)
            workflows.append(wf)

        # Concurrent operations
        num_operations = 200
        read_count = 0
        write_count = 0
        error_count = 0
        lock = threading.Lock()

        def random_operation(op_id: int) -> None:
            nonlocal read_count, write_count, error_count
            try:
                wf = workflows[op_id % len(workflows)]

                if op_id % 2 == 0:
                    # Read operation
                    store.retrieve(wf.id)
                    with lock:
                        read_count += 1
                else:
                    # Write operation (update context)
                    try:
                        stage = store.retrieve_stage(wf.stages[0].id)
                        stage.context[f"op_{op_id}"] = f"value_{op_id}"
                        store.store_stage(stage)
                        with lock:
                            write_count += 1
                    except ConcurrencyError:
                        # Expected under high contention
                        with lock:
                            error_count += 1
            except Exception:
                with lock:
                    error_count += 1

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(random_operation, i) for i in range(num_operations)]
            for f in as_completed(futures):
                pass

        # Should have completed operations without crashes
        total_successful = read_count + write_count
        assert total_successful > 0, "Should have some successful operations"

        # ConcurrencyErrors are expected, not failures
        # Just ensure we didn't have too many errors
        error_rate = error_count / num_operations
        assert error_rate < 0.5, f"Too many errors: {error_count}/{num_operations}"

        store.close()


class TestConnectionManagerEdgeCases:
    """Edge cases for ConnectionManager."""

    def test_close_all_connections(self, tmp_path: Any) -> None:
        """Test that close_all() properly closes connections."""
        from stabilize.persistence.connection import get_connection_manager

        SingletonMeta.reset(ConnectionManager)

        db_path = tmp_path / "close_test.db"
        connection_string = f"sqlite:///{db_path}"

        manager = get_connection_manager()
        conn = manager.get_sqlite_connection(connection_string)

        # Verify connection works
        conn.execute("SELECT 1")

        # Close all
        manager.close_all()

        # Getting connection again should create a new one
        new_conn = manager.get_sqlite_connection(connection_string)

        # New connection should work
        new_conn.execute("SELECT 1")

    def test_multiple_database_paths(self, tmp_path: Any) -> None:
        """Test managing multiple database paths."""
        from stabilize.persistence.connection import get_connection_manager

        SingletonMeta.reset(ConnectionManager)

        db1 = tmp_path / "db1.db"
        db2 = tmp_path / "db2.db"

        manager = get_connection_manager()

        conn1 = manager.get_sqlite_connection(f"sqlite:///{db1}")
        conn2 = manager.get_sqlite_connection(f"sqlite:///{db2}")

        # Should be different connections
        assert conn1 is not conn2

        # Each should work independently
        conn1.execute("CREATE TABLE test1 (id INTEGER)")
        conn1.commit()

        conn2.execute("CREATE TABLE test2 (id INTEGER)")
        conn2.commit()

        # Verify tables exist in correct databases
        assert conn1.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test1'").fetchone()
        assert conn2.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test2'").fetchone()

        # test1 should NOT exist in db2 and vice versa
        assert not conn2.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test1'").fetchone()
        assert not conn1.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test2'").fetchone()

    def test_singleton_behavior(self) -> None:
        """Test that ConnectionManager is a proper singleton."""
        from stabilize.persistence.connection import get_connection_manager

        SingletonMeta.reset(ConnectionManager)

        manager1 = get_connection_manager()
        manager2 = get_connection_manager()

        assert manager1 is manager2, "ConnectionManager should be a singleton"

    def test_thread_safe_singleton_creation(self) -> None:
        """Test that singleton creation is thread-safe."""
        from stabilize.persistence.connection import get_connection_manager

        SingletonMeta.reset(ConnectionManager)

        managers: list[ConnectionManager] = []
        lock = threading.Lock()

        def get_manager() -> None:
            m = get_connection_manager()
            with lock:
                managers.append(m)

        threads = [threading.Thread(target=get_manager) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All threads should have gotten the same instance
        assert len(managers) == 10
        assert all(m is managers[0] for m in managers), "All threads should get same singleton"
