"""
Orchestrator - starts and manages pipeline executions.

This module provides the main entry point for running pipelines.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from stabilize.models.status import WorkflowStatus
from stabilize.persistence.committed_marks import delivery_exempt
from stabilize.persistence.tenancy import TenantError, require_same_tenant
from stabilize.queue.messages import (
    CancelWorkflow,
    CompleteWorkflow,
    RestartStage,
    ResumeStage,
    StartWorkflow,
)
from stabilize.queue.ownership import queue_table_name
from stabilize.resilience.config import HandlerConfig, get_handler_config

if TYPE_CHECKING:
    from stabilize.models.workflow import Workflow
    from stabilize.persistence.store import WorkflowStore
    from stabilize.persistence.store.transaction import StoreTransaction
    from stabilize.queue import Queue


class Orchestrator:
    """
    Runner for pipeline executions.

    Provides methods to start, cancel, restart, and resume executions
    by pushing appropriate messages to the queue atomically with the store.

    When a store is provided, all queue operations are wrapped in transactions
    to ensure atomicity - preventing orphaned workflow state if queue push fails.
    """

    _instance: Orchestrator | None = None

    def __init__(
        self,
        queue: Queue,
        store: WorkflowStore | None = None,
    ) -> None:
        """
        Initialize the runner.

        Args:
            queue: The message queue
            store: Optional workflow store for atomic operations. When provided,
                   queue operations are wrapped in store transactions.
        """
        require_same_tenant(queue=queue, store=store)
        self.queue = queue
        self.store = store
        # Register as the global instance for SubWorkflowTask access
        Orchestrator._instance = self

    @classmethod
    def get_instance(cls) -> Orchestrator | None:
        """Get the most recently created Orchestrator instance.

        Used by SubWorkflowTask to access the orchestrator for
        starting and polling child workflows.

        Returns:
            The current Orchestrator instance, or None if not created yet.
        """
        instance = cls._instance
        bound = getattr(instance.store, "tenant", None) if instance is not None else None
        if bound:
            raise TenantError(
                "Orchestrator.get_instance() is a process-global and cannot serve a tenant-scoped store; "
                "pass the orchestrator explicitly"
            )
        warnings.warn(
            "Orchestrator.get_instance() is deprecated in 0.34 and refused in tenancy mode; "
            "pass the orchestrator to the task explicitly",
            DeprecationWarning,
            stacklevel=2,
        )
        return instance

    def get_execution(self, execution_id: str) -> Workflow | None:
        """Retrieve a workflow execution by ID.

        Args:
            execution_id: The workflow ID to retrieve.

        Returns:
            The workflow, or None if not found or no store configured.
        """
        if self.store is None:
            return None
        try:
            return self.store.retrieve(execution_id)
        except Exception:
            return None

    def start(
        self,
        execution: Workflow,
        handler_config: HandlerConfig | None = None,
    ) -> None:
        """
        Start a pipeline execution atomically.

        If a store is configured, wraps the queue push in a transaction
        to ensure atomicity with any prior status updates.

        Attaches the configuration fingerprint to the workflow for
        versioning and auditing purposes.

        Args:
            execution: The execution to start
            handler_config: Optional handler config. If not provided, uses
                           the global config from environment.
        """
        # Attach config fingerprint for versioning
        config = handler_config or get_handler_config()
        execution.config_version = config.config_fingerprint()

        message = StartWorkflow(
            execution_type=execution.type.value,
            execution_id=execution.id,
        )

        if execution.queue_table is None:
            execution.queue_table = queue_table_name(self.queue)
        if self.store:
            if not self.store.exists(execution.id):
                try:
                    self.store.store(execution)
                except Exception:
                    if not self.store.exists(execution.id):
                        raise
            with self.store.transaction(self.queue) as txn:
                txn.set_config_version(execution.id, execution.config_version)
                txn.push_message(message)
        else:
            with delivery_exempt():
                self.queue.push(message)

    def cancel(
        self,
        execution: Workflow,
        user: str,
        reason: str,
    ) -> None:
        """
        Cancel a running execution atomically.

        Args:
            execution: The execution to cancel
            user: Who is canceling
            reason: Why it's being canceled
        """
        message = CancelWorkflow(
            execution_type=execution.type.value,
            execution_id=execution.id,
            user=user,
            reason=reason,
        )

        if self.store:
            with self.store.transaction(self.queue) as txn:
                txn.push_message(message)
        else:
            with delivery_exempt():
                self.queue.push(message)

    def restart(
        self,
        execution: Workflow,
        stage_id: str,
    ) -> None:
        """
        Restart a stage in an execution atomically.

        Args:
            execution: The execution
            stage_id: The stage to restart
        """
        message = RestartStage(
            execution_type=execution.type.value,
            execution_id=execution.id,
            stage_id=stage_id,
        )

        if self.store:
            with self.store.transaction(self.queue) as txn:
                txn.push_message(message)
        else:
            with delivery_exempt():
                self.queue.push(message)

    def _resume_without_paused_stages(self, execution: Workflow) -> None:
        assert self.store is not None
        with self.store.transaction(self.queue) as txn:
            status, _ = txn.workflow_status(execution.id)
            if status != WorkflowStatus.PAUSED.name:
                return
            self._resume_workflow(txn, execution)

    def _resume_workflow(self, txn: StoreTransaction, execution: Workflow) -> None:
        execution.resume()
        if execution.start_time is None:
            execution.status = WorkflowStatus.NOT_STARTED
            message: StartWorkflow | CompleteWorkflow = StartWorkflow(
                execution_type=execution.type.value, execution_id=execution.id
            )
        else:
            message = CompleteWorkflow(execution_type=execution.type.value, execution_id=execution.id)
        txn.update_workflow_status(execution)
        txn.push_message(message)

    def _unpause_locked(self, txn: StoreTransaction, execution: Workflow) -> None:
        from stabilize.persistence.store.transaction import workflow_control_key

        txn.lock_admission(workflow_control_key(execution.id))
        status, _ = txn.workflow_status(execution.id)
        paused = txn.paused_stage_ids(execution.id)
        if not paused:
            if status == WorkflowStatus.PAUSED.name:
                self._resume_workflow(txn, execution)
            return
        if status == WorkflowStatus.PAUSED.name:
            execution.resume()
            txn.update_workflow_status(execution)
        for stage_id in paused:
            txn.push_message(
                ResumeStage(execution_type=execution.type.value, execution_id=execution.id, stage_id=stage_id)
            )

    def unpause(self, execution: Workflow) -> None:
        """
        Resume a paused execution atomically.

        Loads fresh execution state from store (if available) to prevent
        stale read issues where the in-memory execution doesn't reflect
        recent changes made by other processes.

        Args:
            execution: The execution to resume
        """
        # Load fresh state if store available to prevent stale reads
        if self.store:
            fresh_execution = self.store.retrieve(execution.id)
            if fresh_execution:
                execution = fresh_execution
            try:
                with self.store.transaction(self.queue) as txn:
                    if txn.is_atomic:
                        self._unpause_locked(txn, execution)
                        return
            except NotImplementedError:
                pass

        # Collect all resume messages
        messages = []
        for stage in execution.stages:
            if stage.status.name == "PAUSED":
                messages.append(
                    ResumeStage(
                        execution_type=execution.type.value,
                        execution_id=execution.id,
                        stage_id=stage.id,
                    )
                )

        if self.store and not messages and execution.status == WorkflowStatus.PAUSED:
            self._resume_without_paused_stages(execution)
            return

        # Push all messages atomically if store available
        if self.store and messages:
            with self.store.transaction(self.queue) as txn:
                for msg in messages:
                    txn.push_message(msg)
        else:
            with delivery_exempt():
                for msg in messages:
                    self.queue.push(msg)
