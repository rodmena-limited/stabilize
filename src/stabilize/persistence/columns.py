"""Column lists of the 12 engine tables, shared by every SELECT and the row converter that reads it."""

from __future__ import annotations

TENANT_COLUMN = "stabilize_tenant"

PIPELINE_EXECUTIONS: tuple[str, ...] = (
    "id",
    "type",
    "application",
    "name",
    "status",
    "start_time",
    "end_time",
    "start_time_expiry",
    "trigger",
    "context",
    "is_canceled",
    "canceled_by",
    "cancellation_reason",
    "paused",
    "pipeline_config_id",
    "is_limit_concurrent",
    "max_concurrent_executions",
    "keep_waiting_pipelines",
    "origin",
    "created_at",
    "queue_table",
    "config_version",
    TENANT_COLUMN,
)

STAGE_EXECUTIONS: tuple[str, ...] = (
    "id",
    "execution_id",
    "ref_id",
    "type",
    "name",
    "status",
    "context",
    "outputs",
    "requisite_stage_ref_ids",
    "parent_stage_id",
    "synthetic_stage_owner",
    "start_time",
    "end_time",
    "start_time_expiry",
    "scheduled_time",
    "version",
    "join_type",
    "join_threshold",
    "split_type",
    "split_conditions",
    "mi_config",
    "deferred_choice_group",
    "milestone_ref_id",
    "milestone_status",
    "mutex_key",
    "cancel_region",
    TENANT_COLUMN,
)

TASK_EXECUTIONS: tuple[str, ...] = (
    "id",
    "stage_id",
    "name",
    "implementing_class",
    "status",
    "start_time",
    "end_time",
    "stage_start",
    "stage_end",
    "loop_start",
    "loop_end",
    "task_exception_details",
    "version",
    TENANT_COLUMN,
)

QUEUE_MESSAGES: tuple[str, ...] = (
    "id",
    "message_id",
    "message_type",
    "payload",
    "deliver_at",
    "attempts",
    "max_attempts",
    "locked_until",
    "created_at",
    TENANT_COLUMN,
)

QUEUE_MESSAGES_DLQ: tuple[str, ...] = (
    "id",
    "original_id",
    "message_id",
    "message_type",
    "payload",
    "attempts",
    "error",
    "last_error_at",
    "created_at",
    "moved_at",
    TENANT_COLUMN,
)

PROCESSED_MESSAGES: tuple[str, ...] = ("message_id", "processed_at", "handler_type", "execution_id", TENANT_COLUMN)

WORKFLOW_SIGNALS: tuple[str, ...] = (
    "id",
    "execution_id",
    "stage_ref_id",
    "signal_name",
    "signal_data",
    "consumed",
    "created_at",
    "consumed_at",
    TENANT_COLUMN,
)

STAGE_CLAIMS: tuple[str, ...] = ("execution_id", "claim_key", "stage_id", "claimed_at", TENANT_COLUMN)

EVENTS: tuple[str, ...] = (
    "sequence",
    "event_id",
    "event_type",
    "timestamp",
    "entity_type",
    "entity_id",
    "workflow_id",
    "version",
    "data",
    "correlation_id",
    "causation_id",
    "actor",
    "source_handler",
    "schema_version",
    TENANT_COLUMN,
)

SNAPSHOTS: tuple[str, ...] = (
    "id",
    "entity_type",
    "entity_id",
    "workflow_id",
    "version",
    "sequence",
    "state",
    "state_hash",
    "created_at",
    TENANT_COLUMN,
)

EVENT_SUBSCRIPTIONS: tuple[str, ...] = (
    "id",
    "event_types",
    "entity_filter",
    "last_sequence",
    "webhook_url",
    "created_at",
    "updated_at",
    "last_commit_cursor",
    TENANT_COLUMN,
)

TASK_LEASES: tuple[str, ...] = ("task_id", "owner", "acquired_at", "expires_at", TENANT_COLUMN)

ENGINE_COLUMNS: dict[str, tuple[str, ...]] = {
    "pipeline_executions": PIPELINE_EXECUTIONS,
    "stage_executions": STAGE_EXECUTIONS,
    "task_executions": TASK_EXECUTIONS,
    "queue_messages": QUEUE_MESSAGES,
    "queue_messages_dlq": QUEUE_MESSAGES_DLQ,
    "processed_messages": PROCESSED_MESSAGES,
    "workflow_signals": WORKFLOW_SIGNALS,
    "stage_claims": STAGE_CLAIMS,
    "events": EVENTS,
    "snapshots": SNAPSHOTS,
    "event_subscriptions": EVENT_SUBSCRIPTIONS,
    "task_leases": TASK_LEASES,
}

BACKEND_ONLY: dict[str, dict[str, tuple[str, ...]]] = {
    "postgres": {"events": ("commit_xid",)},
    "sqlite": {"queue_messages": ("version",)},
}


def columns(table: str, backend: str) -> tuple[str, ...]:
    """Every column of *table* on *backend*, the shared list first and the backend's own columns after it."""
    extra = BACKEND_ONLY[backend].get(table, ())
    return ENGINE_COLUMNS[table] + extra


def select_list(table: str, backend: str, alias: str | None = None) -> str:
    """The comma-separated column list a SELECT on *table* names instead of ``*``."""
    prefix = f"{alias}." if alias else ""
    return ", ".join(f"{prefix}{column}" for column in columns(table, backend))
