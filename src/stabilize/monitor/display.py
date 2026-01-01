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
