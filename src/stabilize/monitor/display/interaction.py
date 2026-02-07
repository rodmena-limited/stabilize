"""User interaction functions for the monitor display."""

from __future__ import annotations

import curses
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any

from stabilize.monitor.display.constants import PAIR_HEADER

if TYPE_CHECKING:
    from stabilize.monitor.data import (
        MonitorDataFetcher,
        StageView,
        TaskView,
        WorkflowView,
    )


def get_input(stdscr: Any, prompt: str) -> str | None:
    """Get input from user at bottom of screen."""
    height, width = stdscr.getmaxyx()
    _addstr(stdscr, height - 1, 0, " " * (width - 1), curses.A_REVERSE)
    _addstr(stdscr, height - 1, 0, prompt, curses.A_REVERSE | curses.A_BOLD)
    curses.echo()
    curses.curs_set(1)
    try:
        input_bytes: bytes = stdscr.getstr(height - 1, len(prompt))
        return str(input_bytes.decode("utf-8").strip())
    except Exception:
        return None
    finally:
        curses.noecho()
        curses.curs_set(0)


def show_message(stdscr: Any, message: str, color: int = 0) -> None:
    """Show a temporary message at the bottom."""
    height, width = stdscr.getmaxyx()
    attr = curses.color_pair(PAIR_HEADER) | curses.A_BOLD
    if color:
        attr = curses.A_REVERSE | curses.A_BOLD

    _addstr(stdscr, height - 1, 0, f" {message} " + " " * (width - len(message) - 3), attr)
    stdscr.refresh()
    time.sleep(1.5)


def show_scrollable_modal(stdscr: Any, title: str, text: str) -> None:
    """Show a scrollable modal with text."""
    height, width = stdscr.getmaxyx()

    m_height = int(height * 0.8)
    m_width = int(width * 0.8)
    m_y = int(height * 0.1)
    m_x = int(width * 0.1)

    win = curses.newwin(m_height, m_width, m_y, m_x)
    win.keypad(True)

    lines = text.split("\n")
    total_lines = len(lines)
    offset = 0

    while True:
        win.clear()
        win.box()

        title_text = f" {title} "
        if len(title_text) > m_width - 4:
            title_text = title_text[: m_width - 4]
        win.addstr(0, 2, title_text, curses.A_BOLD)

        footer = (
            f" Lines {offset + 1}-{min(offset + m_height - 2, total_lines)}/{total_lines} "
            "| Q/Esc to close | Up/Down/PgUp/PgDn "
        )
        if len(footer) > m_width - 4:
            footer = footer[: m_width - 4]
        win.addstr(m_height - 1, 2, footer, curses.A_DIM)

        content_h = m_height - 2
        content_w = m_width - 4

        for i in range(content_h):
            line_idx = offset + i
            if line_idx < total_lines:
                line = lines[line_idx]
                if len(line) > content_w:
                    line = line[:content_w]
                win.addstr(i + 1, 2, line)

        win.refresh()

        key = win.getch()
        if key == ord("q") or key == 27:
            break
        elif key == curses.KEY_UP:
            offset = max(0, offset - 1)
        elif key == curses.KEY_DOWN:
            offset = min(offset + 1, max(0, total_lines - content_h))
        elif key == curses.KEY_PPAGE:
            offset = max(0, offset - content_h)
        elif key == curses.KEY_NPAGE:
            offset = min(offset + content_h, max(0, total_lines - content_h))
        elif key == curses.KEY_HOME:
            offset = 0
        elif key == curses.KEY_END:
            offset = max(0, total_lines - content_h)

    del win
    stdscr.touchwin()
    stdscr.refresh()


def handle_workflow_action(
    stdscr: Any,
    action: str,
    selected_item: dict[str, Any] | None,
    data_fetcher: MonitorDataFetcher,
) -> None:
    """Handle Pause/Resume/Cancel actions."""
    if not selected_item or selected_item["type"] != "workflow":
        show_message(stdscr, "Please select a Workflow first.", color=curses.COLOR_YELLOW)
        return

    wf_view: WorkflowView = selected_item["data"]
    wf_id = wf_view.id

    try:
        if action == "pause":
            data_fetcher.store.pause(wf_id, paused_by="monitor")
            show_message(stdscr, f"Paused workflow {wf_id}")
        elif action == "resume":
            data_fetcher.store.resume(wf_id)
            show_message(stdscr, f"Resumed workflow {wf_id}")
        elif action == "cancel":
            reason = get_input(stdscr, "Cancellation reason: ")
            if reason:
                data_fetcher.store.cancel(wf_id, canceled_by="monitor", reason=reason)
                show_message(stdscr, f"Canceled workflow {wf_id}")
            else:
                show_message(stdscr, "Cancellation aborted")
    except Exception as e:
        show_message(stdscr, f"Error: {str(e)}", color=curses.COLOR_RED)


def show_details(
    stdscr: Any,
    selected_item: dict[str, Any] | None,
    data_fetcher: MonitorDataFetcher,
) -> None:
    """Show full details for the selected item."""
    if not selected_item:
        return

    import json

    text = ""
    title = "Details"

    try:
        if selected_item["type"] == "workflow":
            wf_view: WorkflowView = selected_item["data"]
            title = f"Workflow: {wf_view.id}"
            wf = data_fetcher.store.retrieve(wf_view.id)
            text = json.dumps(
                {
                    "id": wf.id,
                    "status": wf.status.name,
                    "context": wf.trigger.payload if wf.trigger else {},
                    "start_time": (
                        datetime.fromtimestamp(wf.start_time / 1000).isoformat()
                        if wf.start_time
                        else None
                    ),
                    "end_time": (
                        datetime.fromtimestamp(wf.end_time / 1000).isoformat()
                        if wf.end_time
                        else None
                    ),
                    "paused": wf.paused.__dict__ if wf.paused else None,
                    "cancellation_reason": wf.cancellation_reason,
                },
                indent=2,
                default=str,
            )

        elif selected_item["type"] == "stage":
            stage_view: StageView = selected_item["data"]
            title = f"Stage: {stage_view.name}"
            if hasattr(stage_view, "_db_id"):
                stage = data_fetcher.store.retrieve_stage(stage_view._db_id)
                text = json.dumps(
                    {
                        "name": stage.name,
                        "status": stage.status.name,
                        "context": stage.context,
                        "outputs": stage.outputs,
                        "start_time": (
                            datetime.fromtimestamp(stage.start_time / 1000).isoformat()
                            if stage.start_time
                            else None
                        ),
                        "end_time": (
                            datetime.fromtimestamp(stage.end_time / 1000).isoformat()
                            if stage.end_time
                            else None
                        ),
                    },
                    indent=2,
                    default=str,
                )
            else:
                text = "Error: Missing DB ID for stage lookup."

        elif selected_item["type"] == "task":
            task_view: TaskView = selected_item["data"]
            title = f"Task: {task_view.name}"
            text = json.dumps(
                {
                    "name": task_view.name,
                    "status": task_view.status.name,
                    "implementing_class": task_view.implementing_class,
                    "error": task_view.error,
                    "start_time": (
                        datetime.fromtimestamp(task_view.start_time / 1000).isoformat()
                        if task_view.start_time
                        else None
                    ),
                },
                indent=2,
                default=str,
            )

    except Exception as e:
        text = f"Error fetching details: {str(e)}"

    if text:
        show_scrollable_modal(stdscr, title, text)


def _addstr(stdscr: Any, y: int, x: int, text: str, attr: int = 0) -> None:
    """Safely add string at position."""
    try:
        height, width = stdscr.getmaxyx()
        if 0 <= y < height and 0 <= x < width:
            max_len = width - x - 1
            if max_len > 0:
                stdscr.addstr(y, x, text[:max_len], attr)
    except curses.error:
        pass
