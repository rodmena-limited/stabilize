"""Real-time workflow monitoring for Stabilize."""

from __future__ import annotations

import curses
from typing import TYPE_CHECKING

from stabilize.monitor.data import (
    MonitorData,
    MonitorDataFetcher,
    QueueStats,
    StageView,
    TaskView,
    WorkflowStats,
    WorkflowView,
    format_duration,
)
from stabilize.monitor.display import MonitorDisplay, run_display

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue.queue import Queue

__all__ = [
    "run_monitor",
    "MonitorData",
    "MonitorDataFetcher",
    "MonitorDisplay",
    "WorkflowView",
    "StageView",
    "TaskView",
    "QueueStats",
    "WorkflowStats",
    "format_duration",
]


def run_monitor(
    store: WorkflowStore,
    queue: Queue | None = None,
    app_filter: str | None = None,
    refresh_interval: int = 2,
    status_filter: str = "all",
) -> None:
    """
    Launch the real-time monitoring dashboard.

    Args:
        store: Workflow store for fetching execution data
        queue: Optional queue for queue statistics
        app_filter: Filter workflows by application name
        refresh_interval: Refresh interval in seconds (default: 2)
        status_filter: Filter by status ('all', 'running', 'failed', 'recent')
    """
    data_fetcher = MonitorDataFetcher(store, queue)

    curses.wrapper(
        run_display,
        data_fetcher,
        refresh_interval,
        app_filter,
        status_filter,
    )
