"""Rendering functions for the monitor display."""

from __future__ import annotations

import curses
from datetime import datetime
from typing import TYPE_CHECKING, Any

from stabilize.models.status import WorkflowStatus
from stabilize.monitor.data import format_duration
from stabilize.monitor.display.constants import (
    COL_DURATION,
    COL_PROGRESS,
    COL_STATUS,
    COL_TIME,
    PAIR_DIM,
    PAIR_FAILED,
    PAIR_HEADER,
    PAIR_PAUSED,
    PAIR_RUNNING,
    PAIR_SUCCEEDED,
    STAGE_PREFIX_LAST,
    STAGE_PREFIX_MID,
    TASK_PREFIX_LAST,
    TASK_PREFIX_LAST_LAST_STAGE,
    TASK_PREFIX_MID,
    TASK_PREFIX_MID_LAST_STAGE,
)

if TYPE_CHECKING:
    from stabilize.monitor.data import MonitorData, StageView, TaskView, WorkflowView


def init_colors() -> None:
    """Initialize curses color pairs."""
    curses.start_color()
    curses.use_default_colors()

    try:
        curses.init_pair(PAIR_SUCCEEDED, curses.COLOR_GREEN, -1)
        curses.init_pair(PAIR_RUNNING, curses.COLOR_CYAN, -1)
        curses.init_pair(PAIR_FAILED, curses.COLOR_RED, -1)
        curses.init_pair(PAIR_PAUSED, curses.COLOR_YELLOW, -1)
        curses.init_pair(PAIR_DIM, curses.COLOR_WHITE, -1)
        curses.init_pair(PAIR_HEADER, curses.COLOR_BLACK, curses.COLOR_WHITE)
    except curses.error:
        pass


def get_status_attr(status: WorkflowStatus) -> int:
    """Get color attribute for a status."""
    if status == WorkflowStatus.SUCCEEDED:
        return curses.color_pair(PAIR_SUCCEEDED)
    elif status == WorkflowStatus.RUNNING:
        return curses.color_pair(PAIR_RUNNING) | curses.A_BOLD
    elif status in (
        WorkflowStatus.TERMINAL,
        WorkflowStatus.FAILED_CONTINUE,
        WorkflowStatus.CANCELED,
        WorkflowStatus.STOPPED,
    ):
        return curses.color_pair(PAIR_FAILED) | curses.A_BOLD
    elif status in (WorkflowStatus.PAUSED, WorkflowStatus.SUSPENDED):
        return curses.color_pair(PAIR_PAUSED)
    else:
        return curses.A_NORMAL


def addstr(stdscr: Any, y: int, x: int, text: str, attr: int = 0) -> None:
    """Safely add string at position."""
    try:
        height, width = stdscr.getmaxyx()
        if 0 <= y < height and 0 <= x < width:
            max_len = width - x - 1
            if max_len > 0:
                stdscr.addstr(y, x, text[:max_len], attr)
    except curses.error:
        pass


def render_header(stdscr: Any, width: int, app_filter: str | None, refresh_interval: int) -> None:
    """Render header line."""
    app_info = f"App: {app_filter}" if app_filter else "All Apps"
    left = f" STABILIZE MONITOR - {app_info}"
    right = f"Refresh: {refresh_interval}s   Q to quit "

    padding = width - len(left) - len(right)
    header = left + " " * max(0, padding) + right

    addstr(stdscr, 0, 0, header[: width - 1], curses.color_pair(PAIR_HEADER) | curses.A_BOLD)


def render_footer(stdscr: Any, data: MonitorData, height: int, width: int) -> None:
    """Render footer with stats."""
    if height < 3:
        return

    # Separator
    addstr(stdscr, height - 2, 0, "─" * (width - 1), curses.A_DIM)

    # Stats
    ws = data.workflow_stats
    qs = data.queue_stats

    left = f" Workflows: {ws.running} running, {ws.succeeded} succeeded, {ws.failed} failed"
    middle = f"  │  Queue: {qs.pending} pending, {qs.stuck} stuck"
    right = f"Updated: {data.fetch_time.strftime('%H:%M:%S')} "

    footer = left + middle
    padding = width - len(footer) - len(right)
    full_footer = footer + " " * max(0, padding) + right

    addstr(
        stdscr,
        height - 1,
        0,
        full_footer[: width - 1],
        curses.color_pair(PAIR_HEADER) | curses.A_BOLD,
    )


def build_line_metadata(data: MonitorData) -> list[dict[str, Any]]:
    """Build metadata for all display lines."""
    lines: list[dict[str, Any]] = []

    if data.error:
        lines.append({"type": "error", "text": f"Error: {data.error}"})
        return lines

    if not data.workflows:
        lines.append({"type": "empty", "text": "No workflows found"})
        return lines

    for wf in data.workflows:
        lines.append({"type": "workflow", "data": wf})

        for i, stage in enumerate(wf.stages):
            is_last_stage = i == len(wf.stages) - 1
            lines.append({"type": "stage", "data": stage, "is_last": is_last_stage})

            for j, task in enumerate(stage.tasks):
                is_last_task = j == len(stage.tasks) - 1
                lines.append(
                    {
                        "type": "task",
                        "data": task,
                        "is_last_stage": is_last_stage,
                        "is_last_task": is_last_task,
                    }
                )

        lines.append({"type": "blank"})

    return lines


def render_line(
    stdscr: Any, y: int, width: int, line_info: dict[str, Any], is_selected: bool
) -> None:
    """Render a single line based on its type."""
    line_type = line_info["type"]

    # Base attribute for the whole line if selected
    base_attr = curses.A_REVERSE if is_selected else curses.A_NORMAL

    if line_type == "error":
        addstr(stdscr, y, 0, line_info["text"], curses.A_BOLD | base_attr)
    elif line_type == "empty":
        addstr(stdscr, y, 0, line_info["text"], curses.A_DIM | base_attr)
    elif line_type == "blank":
        addstr(stdscr, y, 0, " " * (width - 1), base_attr)
    elif line_type == "workflow":
        render_workflow_line(stdscr, y, width, line_info["data"], base_attr)
    elif line_type == "stage":
        render_stage_line(stdscr, y, width, line_info["data"], line_info["is_last"], base_attr)
    elif line_type == "task":
        render_task_line(
            stdscr,
            y,
            width,
            line_info["data"],
            line_info["is_last_stage"],
            line_info["is_last_task"],
            base_attr,
        )


def render_workflow_line(
    stdscr: Any, y: int, width: int, wf: WorkflowView, base_attr: int
) -> None:
    """Render a workflow line with proper column alignment."""
    pos_progress = width - COL_PROGRESS
    pos_duration = width - COL_DURATION
    pos_status = width - COL_STATUS
    pos_time = width - COL_TIME

    wf_id = wf.id[:10] + ".."
    app = wf.application[:14] if wf.application else "-"
    status = wf.status.name[:10]
    duration = format_duration(wf.start_time, wf.end_time)[:7]

    if wf.start_time:
        start_dt = datetime.fromtimestamp(wf.start_time / 1000)
        ms = wf.start_time % 1000
        time_str = start_dt.strftime("%Y-%m-%d %H:%M:%S") + f".{ms:03d}"
    else:
        time_str = "-"

    completed, total = wf.stage_progress
    progress = f"{completed}/{total}" if total > 0 else "  -"

    if base_attr & curses.A_REVERSE:
        addstr(stdscr, y, 0, " " * (width - 1), base_attr)

    prefix = f"▼ {wf_id}  {app:<14}  "
    name_max = pos_time - len(prefix) - 2
    name = wf.name[:name_max] if wf.name else "-"

    addstr(stdscr, y, 0, prefix + name, base_attr)
    addstr(stdscr, y, pos_time, f"{time_str:>23}", base_attr)

    status_attr = get_status_attr(wf.status)
    if base_attr & curses.A_REVERSE:
        status_attr = status_attr | curses.A_REVERSE

    addstr(stdscr, y, pos_status, f"{status:>10}", status_attr)
    addstr(
        stdscr,
        y,
        pos_duration,
        f"{duration:>7}",
        base_attr | (curses.A_DIM if wf.status.is_complete else 0),
    )
    addstr(stdscr, y, pos_progress, f"{progress:>5}", base_attr)


def render_stage_line(
    stdscr: Any, y: int, width: int, stage: StageView, is_last: bool, base_attr: int
) -> None:
    """Render a stage line with proper column alignment."""
    pos_time = width - COL_TIME
    pos_duration = width - COL_DURATION
    pos_status = width - COL_STATUS

    prefix = STAGE_PREFIX_LAST if is_last else STAGE_PREFIX_MID
    status = stage.status.name[:10]
    duration = format_duration(stage.start_time, stage.end_time)[:7]

    if stage.start_time:
        start_dt = datetime.fromtimestamp(stage.start_time / 1000)
        ms = stage.start_time % 1000
        time_str = start_dt.strftime("%Y-%m-%d %H:%M:%S") + f".{ms:03d}"
    else:
        time_str = "-"

    if base_attr & curses.A_REVERSE:
        addstr(stdscr, y, 0, " " * (width - 1), base_attr)

    name_max = pos_time - len(prefix) - 2
    name = stage.name[:name_max] if stage.name else "-"

    addstr(stdscr, y, 0, prefix + name, base_attr)
    addstr(stdscr, y, pos_time, f"{time_str:>23}", base_attr)

    status_attr = get_status_attr(stage.status)
    if base_attr & curses.A_REVERSE:
        status_attr = status_attr | curses.A_REVERSE

    addstr(stdscr, y, pos_status, f"{status:>10}", status_attr)
    addstr(
        stdscr,
        y,
        pos_duration,
        f"{duration:>7}",
        base_attr | (curses.A_DIM if stage.status.is_complete else 0),
    )


def render_task_line(
    stdscr: Any,
    y: int,
    width: int,
    task: TaskView,
    is_last_stage: bool,
    is_last_task: bool,
    base_attr: int,
) -> None:
    """Render a task line with proper column alignment."""
    pos_time = width - COL_TIME
    pos_duration = width - COL_DURATION
    pos_status = width - COL_STATUS

    if is_last_stage:
        prefix = TASK_PREFIX_LAST_LAST_STAGE if is_last_task else TASK_PREFIX_MID_LAST_STAGE
    else:
        prefix = TASK_PREFIX_LAST if is_last_task else TASK_PREFIX_MID

    status = task.status.name[:10]
    duration = format_duration(task.start_time, task.end_time)[:7]

    if task.start_time:
        start_dt = datetime.fromtimestamp(task.start_time / 1000)
        ms = task.start_time % 1000
        time_str = start_dt.strftime("%Y-%m-%d %H:%M:%S") + f".{ms:03d}"
    else:
        time_str = "-"

    if base_attr & curses.A_REVERSE:
        addstr(stdscr, y, 0, " " * (width - 1), base_attr)

    name_max = pos_time - len(prefix) - 2
    name = task.name[:name_max] if task.name else "-"

    addstr(stdscr, y, 0, prefix + name, base_attr)
    addstr(stdscr, y, pos_time, f"{time_str:>23}", base_attr)

    status_attr = get_status_attr(task.status)
    if base_attr & curses.A_REVERSE:
        status_attr = status_attr | curses.A_REVERSE

    addstr(stdscr, y, pos_status, f"{status:>10}", status_attr)
    addstr(
        stdscr,
        y,
        pos_duration,
        f"{duration:>7}",
        base_attr | (curses.A_DIM if task.status.is_complete else 0),
    )
