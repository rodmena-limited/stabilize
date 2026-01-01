from __future__ import annotations
import curses
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any
from stabilize.models.status import WorkflowStatus
from stabilize.monitor.data import MonitorDataFetcher, format_duration
COL_PROGRESS = 6  # width - 6
COL_DURATION = 14  # width - 14
COL_STATUS = 26  # width - 26
COL_TIME = 52  # width - 52 (23 chars for "YYYY-MM-DD HH:MM:SS.mmm")
STAGE_PREFIX_MID = "├── "
STAGE_PREFIX_LAST = "└── "
TASK_PREFIX_MID = "│   ├── "
TASK_PREFIX_LAST = "│   └── "
TASK_PREFIX_MID_LAST_STAGE = "    ├── "
TASK_PREFIX_LAST_LAST_STAGE = "    └── "
PAIR_SUCCEEDED = 1
PAIR_RUNNING = 2
PAIR_FAILED = 3
PAIR_PAUSED = 4
PAIR_DIM = 5
PAIR_HEADER = 100

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
        self._auto_scroll = True  # Auto-scroll to bottom until user scrolls up
        self._init_colors()

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
        self.stdscr.timeout(self.refresh_interval * 1000)

        while True:
            try:
                data = self.data_fetcher.fetch(
                    app_filter=self.app_filter,
                    status_filter=self.status_filter,
                )
                self._render(data)

                key = self.stdscr.getch()
                if key == ord("q") or key == ord("Q"):
                    break
                elif key == curses.KEY_UP or key == ord("k"):
                    self.scroll_offset = max(0, self.scroll_offset - 1)
                    self._auto_scroll = False  # User scrolled up, disable auto-scroll
                elif key == curses.KEY_DOWN or key == ord("j"):
                    self.scroll_offset += 1
                elif key == curses.KEY_PPAGE:
                    height = self.stdscr.getmaxyx()[0]
                    self.scroll_offset = max(0, self.scroll_offset - (height - 4))
                    self._auto_scroll = False  # User scrolled up, disable auto-scroll
                elif key == curses.KEY_NPAGE:
                    height = self.stdscr.getmaxyx()[0]
                    self.scroll_offset += height - 4
                elif key == curses.KEY_HOME:
                    self.scroll_offset = 0
                    self._auto_scroll = False  # User scrolled to top, disable auto-scroll
                elif key == curses.KEY_END:
                    self._auto_scroll = True  # Re-enable auto-scroll to bottom
                elif key == curses.KEY_RESIZE:
                    self.stdscr.clear()

            except curses.error:
                time.sleep(0.1)

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
        content_height = height - 4
        content_start_y = 2

        # Build line metadata for scrolling
        lines = self._build_line_metadata(data)

        # Apply scroll
        max_scroll = max(0, len(lines) - content_height)

        # Auto-scroll to bottom if enabled
        if self._auto_scroll:
            self.scroll_offset = max_scroll
        else:
            self.scroll_offset = min(self.scroll_offset, max_scroll)

        visible_lines = lines[self.scroll_offset : self.scroll_offset + content_height]

        # Render each visible line
        for i, line_info in enumerate(visible_lines):
            y = content_start_y + i
            self._render_line(y, width, line_info)

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

    def _render_line(self, y: int, width: int, line_info: dict) -> None:
        """Render a single line based on its type."""
        line_type = line_info["type"]

        if line_type == "error":
            self._addstr(y, 0, line_info["text"], curses.A_BOLD)
        elif line_type == "empty":
            self._addstr(y, 0, line_info["text"], curses.A_DIM)
        elif line_type == "blank":
            pass  # Empty line
        elif line_type == "workflow":
            self._render_workflow_line(y, width, line_info["data"])
        elif line_type == "stage":
            self._render_stage_line(y, width, line_info["data"], line_info["is_last"])
        elif line_type == "task":
            self._render_task_line(y, width, line_info["data"], line_info["is_last_stage"], line_info["is_last_task"])

    def _render_workflow_line(self, y: int, width: int, wf: WorkflowView) -> None:
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

        # Render left part (tree + id + app + name)
        prefix = f"▼ {wf_id}  {app:<14}  "
        name_max = pos_time - len(prefix) - 2
        name = wf.name[:name_max] if wf.name else "-"

        self._addstr(y, 0, prefix + name, curses.A_NORMAL)

        # Render columns at fixed positions
        self._addstr(y, pos_time, f"{time_str:>23}", curses.A_NORMAL)
        self._addstr(y, pos_status, f"{status:>10}", self._get_status_attr(wf.status))
        self._addstr(y, pos_duration, f"{duration:>7}", curses.A_DIM if wf.status.is_complete else curses.A_NORMAL)
        self._addstr(y, pos_progress, f"{progress:>5}", curses.A_NORMAL)
