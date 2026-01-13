"""Curses-based display for the monitor command."""

from __future__ import annotations

import curses
import queue
import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any

from stabilize.models.status import WorkflowStatus
from stabilize.monitor.data import MonitorDataFetcher, format_duration

if TYPE_CHECKING:
    from stabilize.monitor.data import MonitorData, StageView, TaskView, WorkflowView


# Column positions from right edge (absolute positioning)
COL_PROGRESS = 6  # width - 6
COL_DURATION = 14  # width - 14
COL_STATUS = 26  # width - 26
COL_TIME = 52  # width - 52 (23 chars for "YYYY-MM-DD HH:MM:SS.mmm")

# Tree prefixes (fixed width for alignment)
STAGE_PREFIX_MID = "├── "
STAGE_PREFIX_LAST = "└── "
TASK_PREFIX_MID = "│   ├── "
TASK_PREFIX_LAST = "│   └── "
TASK_PREFIX_MID_LAST_STAGE = "    ├── "
TASK_PREFIX_LAST_LAST_STAGE = "    └── "

# Color pair IDs
PAIR_SUCCEEDED = 1
PAIR_RUNNING = 2
PAIR_FAILED = 3
PAIR_PAUSED = 4
PAIR_DIM = 5
PAIR_HEADER = 100


class DataFetcherThread(threading.Thread):
    """Background thread for fetching monitoring data."""

    def __init__(
        self,
        fetcher: MonitorDataFetcher,
        data_queue: queue.Queue,
        refresh_interval: int,
        app_filter: str | None,
        status_filter: str,
    ) -> None:
        super().__init__(daemon=True)
        self.fetcher = fetcher
        self.data_queue = data_queue
        self.refresh_interval = refresh_interval
        self.app_filter = app_filter
        self.status_filter = status_filter
        self.stop_event = threading.Event()

    def run(self) -> None:
        """Fetch data loop."""
        while not self.stop_event.is_set():
            try:
                start_time = time.time()
                data = self.fetcher.fetch(
                    app_filter=self.app_filter,
                    status_filter=self.status_filter,
                )
                self.data_queue.put(data)

                # Sleep for remaining time, checking stop event
                elapsed = time.time() - start_time
                sleep_time = max(0.1, self.refresh_interval - elapsed)
                self.stop_event.wait(sleep_time)

            except Exception:
                # Retry on error with backoff
                self.stop_event.wait(1.0)

    def stop(self) -> None:
        """Signal thread to stop."""
        self.stop_event.set()


class MonitorDisplay:
    """Curses-based monitoring display with proper column alignment."""

    def __init__(
        self,
        stdscr: Any,
        data_fetcher: MonitorDataFetcher,
        refresh_interval: int = 2,
        app_filter: str | None = None,
        status_filter: str = "all",
    ) -> None:
        self.stdscr = stdscr
        self.data_fetcher = data_fetcher
        self.refresh_interval = refresh_interval
        self.app_filter = app_filter
        self.status_filter = status_filter
        self.scroll_offset = 0
        self.selected_index = 0  # Selection cursor
        self._auto_follow = True  # Auto-scroll to keep selection in view
        self._init_colors()

        # Threading setup
        self.data_queue: queue.Queue = queue.Queue()
        self.fetcher_thread: DataFetcherThread | None = None
        self.current_data: MonitorData | None = None

    def _init_colors(self) -> None:
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

    def _get_status_attr(self, status: WorkflowStatus) -> int:
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

    def run(self) -> None:
        """Main display loop with auto-refresh."""
        curses.curs_set(0)
        # Non-blocking input (100ms timeout) to keep UI responsive
        self.stdscr.timeout(100)

        # Start background thread
        self.fetcher_thread = DataFetcherThread(
            self.data_fetcher,
            self.data_queue,
            self.refresh_interval,
            self.app_filter,
            self.status_filter,
        )
        self.fetcher_thread.start()

        try:
            while True:
                # Check for new data
                try:
                    # Get latest data, clearing queue of any older updates
                    while not self.data_queue.empty():
                        self.current_data = self.data_queue.get_nowait()
                except queue.Empty:
                    pass

                # Render if we have data
                if self.current_data:
                    self._render(self.current_data)
                else:
                    self._render_loading()

                # Handle input
                try:
                    key = self.stdscr.getch()
                    if key == -1:  # Timeout
                        continue

                    if key == ord("q") or key == ord("Q"):
                        break
                    elif key == curses.KEY_UP or key == ord("k"):
                        self.selected_index = max(0, self.selected_index - 1)
                        self._auto_follow = True
                    elif key == curses.KEY_DOWN or key == ord("j"):
                        self.selected_index += 1
                        self._auto_follow = True
                    elif key == curses.KEY_PPAGE:
                        height = self.stdscr.getmaxyx()[0]
                        self.selected_index = max(0, self.selected_index - (height - 4))
                        self._auto_follow = True
                    elif key == curses.KEY_NPAGE:
                        height = self.stdscr.getmaxyx()[0]
                        self.selected_index += height - 4
                        self._auto_follow = True
                    elif key == curses.KEY_HOME:
                        self.selected_index = 0
                        self._auto_follow = True
                    elif key == curses.KEY_END:
                        # Will be clamped in render
                        self.selected_index = 999999
                        self._auto_follow = True
                    elif key == curses.KEY_RESIZE:
                        self.stdscr.clear()

                except curses.error:
                    pass

        finally:
            if self.fetcher_thread:
                self.fetcher_thread.stop()
                self.fetcher_thread.join(timeout=1.0)

    def _render_loading(self) -> None:
        """Render loading state."""
        self.stdscr.clear()
        height, width = self.stdscr.getmaxyx()
        self._render_header(width)
        msg = "Loading data..."
        self._addstr(height // 2, (width - len(msg)) // 2, msg, curses.A_DIM)
        self.stdscr.refresh()

    def _render(self, data: MonitorData) -> None:
        """Render the complete display."""
        self.stdscr.clear()
        height, width = self.stdscr.getmaxyx()

        if height < 5 or width < 60:
            self._addstr(0, 0, "Terminal too small (min 60x5)", curses.A_BOLD)
            self.stdscr.refresh()
            return

        # Header (line 0)
        self._render_header(width)

        # Separator (line 1)
        self._addstr(1, 0, "─" * (width - 1), curses.A_DIM)

        # Content area
        content_height = max(1, height - 4)
        content_start_y = 2

        # Build line metadata for scrolling
        lines = self._build_line_metadata(data)
        total_lines = len(lines)

        # Clamp selection
        if total_lines > 0:
            self.selected_index = min(self.selected_index, total_lines - 1)
        else:
            self.selected_index = 0

        # Auto-scroll logic to keep selection in view
        if self._auto_follow:
            if self.selected_index < self.scroll_offset:
                self.scroll_offset = self.selected_index
            elif self.selected_index >= self.scroll_offset + content_height:
                self.scroll_offset = self.selected_index - content_height + 1

        # Ensure scroll offset is valid
        max_scroll = max(0, total_lines - content_height)
        self.scroll_offset = max(0, min(self.scroll_offset, max_scroll))

        visible_lines = lines[self.scroll_offset : self.scroll_offset + content_height]

        # Render each visible line
        for i, line_info in enumerate(visible_lines):
            y = content_start_y + i
            if y < height - 2:  # Ensure we don't overwrite footer
                # Check if this is the selected line
                absolute_index = self.scroll_offset + i
                is_selected = (absolute_index == self.selected_index)
                self._render_line(y, width, line_info, is_selected)

        # Footer
        self._render_footer(data, height, width)
        self.stdscr.refresh()

    def _render_header(self, width: int) -> None:
        """Render header line."""
        app_info = f"App: {self.app_filter}" if self.app_filter else "All Apps"
        left = f" STABILIZE MONITOR - {app_info}"
        right = f"Refresh: {self.refresh_interval}s   Q to quit "

        padding = width - len(left) - len(right)
        header = left + " " * max(0, padding) + right

        self._addstr(0, 0, header[: width - 1], curses.color_pair(PAIR_HEADER) | curses.A_BOLD)

    def _build_line_metadata(self, data: MonitorData) -> list[dict]:
        """Build metadata for all display lines."""
        lines: list[dict] = []

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
                        {"type": "task", "data": task, "is_last_stage": is_last_stage, "is_last_task": is_last_task}
                    )

            lines.append({"type": "blank"})

        return lines

    def _render_line(self, y: int, width: int, line_info: dict, is_selected: bool) -> None:
        """Render a single line based on its type."""
        line_type = line_info["type"]

        # Base attribute for the whole line if selected
        base_attr = curses.A_REVERSE if is_selected else curses.A_NORMAL

        if line_type == "error":
            self._addstr(y, 0, line_info["text"], curses.A_BOLD | base_attr)
        elif line_type == "empty":
            self._addstr(y, 0, line_info["text"], curses.A_DIM | base_attr)
        elif line_type == "blank":
             self._addstr(y, 0, " " * (width - 1), base_attr)
        elif line_type == "workflow":
            self._render_workflow_line(y, width, line_info["data"], base_attr)
        elif line_type == "stage":
            self._render_stage_line(y, width, line_info["data"], line_info["is_last"], base_attr)
        elif line_type == "task":
            self._render_task_line(
                y,
                width,
                line_info["data"],
                line_info["is_last_stage"],
                line_info["is_last_task"],
                base_attr,
            )

    def _render_workflow_line(self, y: int, width: int, wf: WorkflowView, base_attr: int) -> None:
        """Render a workflow line with proper column alignment."""
        # Calculate column positions
        pos_progress = width - COL_PROGRESS
        pos_duration = width - COL_DURATION
        pos_status = width - COL_STATUS
        pos_time = width - COL_TIME

        # Prepare data
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

        # Render full background for selection
        if base_attr & curses.A_REVERSE:
            self._addstr(y, 0, " " * (width - 1), base_attr)

        # Render left part (tree + id + app + name)
        prefix = f"▼ {wf_id}  {app:<14}  "
        name_max = pos_time - len(prefix) - 2
        name = wf.name[:name_max] if wf.name else "-"

        self._addstr(y, 0, prefix + name, base_attr)

        # Render columns at fixed positions
        self._addstr(y, pos_time, f"{time_str:>23}", base_attr)

        # Status has its own color, but if selected, we might want to keep selection style
        # or combine them. Reverse + Color usually works.
        status_attr = self._get_status_attr(wf.status)
        if base_attr & curses.A_REVERSE:
            status_attr = status_attr | curses.A_REVERSE

        self._addstr(y, pos_status, f"{status:>10}", status_attr)
        self._addstr(y, pos_duration, f"{duration:>7}", base_attr | (curses.A_DIM if wf.status.is_complete else 0))
        self._addstr(y, pos_progress, f"{progress:>5}", base_attr)

    def _render_stage_line(self, y: int, width: int, stage: StageView, is_last: bool, base_attr: int) -> None:
        """Render a stage line with proper column alignment."""
        pos_time = width - COL_TIME
        pos_duration = width - COL_DURATION
        pos_status = width - COL_STATUS

        prefix = STAGE_PREFIX_LAST if is_last else STAGE_PREFIX_MID
        status = stage.status.name[:10]
        duration = format_duration(stage.start_time, stage.end_time)[:7]

        # Format start time
        if stage.start_time:
            start_dt = datetime.fromtimestamp(stage.start_time / 1000)
            ms = stage.start_time % 1000
            time_str = start_dt.strftime("%Y-%m-%d %H:%M:%S") + f".{ms:03d}"
        else:
            time_str = "-"

        # Render full background for selection
        if base_attr & curses.A_REVERSE:
            self._addstr(y, 0, " " * (width - 1), base_attr)

        # Render left part (tree + name)
        name_max = pos_time - len(prefix) - 2
        name = stage.name[:name_max] if stage.name else "-"

        self._addstr(y, 0, prefix + name, base_attr)

        # Render columns
        self._addstr(y, pos_time, f"{time_str:>23}", base_attr)

        status_attr = self._get_status_attr(stage.status)
        if base_attr & curses.A_REVERSE:
            status_attr = status_attr | curses.A_REVERSE

        self._addstr(y, pos_status, f"{status:>10}", status_attr)
        self._addstr(y, pos_duration, f"{duration:>7}", base_attr | (curses.A_DIM if stage.status.is_complete else 0))

    def _render_task_line(
        self,
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

        # Choose prefix based on position
        if is_last_stage:
            prefix = TASK_PREFIX_LAST_LAST_STAGE if is_last_task else TASK_PREFIX_MID_LAST_STAGE
        else:
            prefix = TASK_PREFIX_LAST if is_last_task else TASK_PREFIX_MID

        status = task.status.name[:10]
        duration = format_duration(task.start_time, task.end_time)[:7]

        # Format start time
        if task.start_time:
            start_dt = datetime.fromtimestamp(task.start_time / 1000)
            ms = task.start_time % 1000
            time_str = start_dt.strftime("%Y-%m-%d %H:%M:%S") + f".{ms:03d}"
        else:
            time_str = "-"

        # Render full background for selection
        if base_attr & curses.A_REVERSE:
            self._addstr(y, 0, " " * (width - 1), base_attr)

        # Render left part (tree + name)
        name_max = pos_time - len(prefix) - 2
        name = task.name[:name_max] if task.name else "-"

        self._addstr(y, 0, prefix + name, base_attr)

        # Render columns
        self._addstr(y, pos_time, f"{time_str:>23}", base_attr)

        status_attr = self._get_status_attr(task.status)
        if base_attr & curses.A_REVERSE:
            status_attr = status_attr | curses.A_REVERSE

        self._addstr(y, pos_status, f"{status:>10}", status_attr)
        self._addstr(y, pos_duration, f"{duration:>7}", base_attr | (curses.A_DIM if task.status.is_complete else 0))

    def _render_footer(self, data: MonitorData, height: int, width: int) -> None:
        """Render footer with stats."""
        # Ensure we have enough height for footer
        if height < 3:
            return

        # Separator
        self._addstr(height - 2, 0, "─" * (width - 1), curses.A_DIM)

        # Stats
        ws = data.workflow_stats
        qs = data.queue_stats

        left = f" Workflows: {ws.running} running, {ws.succeeded} succeeded, {ws.failed} failed"
        middle = f"  │  Queue: {qs.pending} pending, {qs.stuck} stuck"
        right = f"Updated: {data.fetch_time.strftime('%H:%M:%S')} "

        footer = left + middle
        padding = width - len(footer) - len(right)
        full_footer = footer + " " * max(0, padding) + right

        self._addstr(height - 1, 0, full_footer[: width - 1], curses.color_pair(PAIR_HEADER) | curses.A_BOLD)

    def _addstr(self, y: int, x: int, text: str, attr: int = 0) -> None:
        """Safely add string at position."""
        try:
            height, width = self.stdscr.getmaxyx()
            if 0 <= y < height and 0 <= x < width:
                max_len = width - x - 1
                if max_len > 0:
                    self.stdscr.addstr(y, x, text[:max_len], attr)
        except curses.error:
            pass


def run_display(
    stdscr: Any,
    data_fetcher: MonitorDataFetcher,
    refresh_interval: int,
    app_filter: str | None,
    status_filter: str,
) -> None:
    """Entry point for curses wrapper."""
    display = MonitorDisplay(
        stdscr,
        data_fetcher,
        refresh_interval=refresh_interval,
        app_filter=app_filter,
        status_filter=status_filter,
    )
    display.run()
