"""Curses-based display for the monitor command."""

from __future__ import annotations

import curses
import queue
from typing import TYPE_CHECKING, Any

from stabilize.monitor.display.fetcher import DataFetcherThread
from stabilize.monitor.display.interaction import (
    handle_workflow_action,
    show_details,
)
from stabilize.monitor.display.rendering import (
    addstr,
    build_line_metadata,
    init_colors,
    render_footer,
    render_header,
    render_line,
)

if TYPE_CHECKING:
    from stabilize.monitor.data import MonitorData, MonitorDataFetcher


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
        self.selected_index = 0
        self._auto_follow = True
        self._initial_scroll_done = False
        self.lines: list[dict[str, Any]] = []
        init_colors()

        try:
            curses.set_escdelay(25)
        except Exception:
            pass

        self.data_queue: queue.Queue[MonitorData] = queue.Queue()
        self.fetcher_thread: DataFetcherThread | None = None
        self.current_data: MonitorData | None = None

    def run(self) -> None:
        """Main display loop with auto-refresh."""
        curses.curs_set(0)
        self.stdscr.timeout(100)

        self.fetcher_thread = DataFetcherThread(
            self.data_fetcher,
            self.data_queue,
            self.refresh_interval,
            self.app_filter,
            self.status_filter,
        )
        self.fetcher_thread.start()

        need_render = True

        try:
            while True:
                try:
                    while not self.data_queue.empty():
                        self.current_data = self.data_queue.get_nowait()
                        need_render = True
                except queue.Empty:
                    pass

                if need_render:
                    if self.current_data:
                        self._render(self.current_data)
                    else:
                        self._render_loading()
                    need_render = False

                try:
                    key = self.stdscr.getch()
                    if key == -1:
                        continue

                    if key == ord("q") or key == ord("Q"):
                        break

                    if key == curses.KEY_UP or key == ord("k"):
                        self.selected_index = max(0, self.selected_index - 1)
                        self._auto_follow = True
                        need_render = True
                    elif key == curses.KEY_DOWN or key == ord("j"):
                        self.selected_index += 1
                        self._auto_follow = True
                        need_render = True
                    elif key == curses.KEY_PPAGE:
                        height = self.stdscr.getmaxyx()[0]
                        self.selected_index = max(0, self.selected_index - (height - 4))
                        self._auto_follow = True
                        need_render = True
                    elif key == curses.KEY_NPAGE:
                        height = self.stdscr.getmaxyx()[0]
                        self.selected_index += height - 4
                        self._auto_follow = True
                        need_render = True
                    elif key == curses.KEY_HOME:
                        self.selected_index = 0
                        self._auto_follow = True
                        need_render = True
                    elif key == curses.KEY_END:
                        self.selected_index = 999999
                        self._auto_follow = True
                        need_render = True
                    elif key == 10:
                        show_details(self.stdscr, self._get_selected_item(), self.data_fetcher)
                        need_render = True
                    elif key == ord("p"):
                        handle_workflow_action(self.stdscr, "pause", self._get_selected_item(), self.data_fetcher)
                        need_render = True
                    elif key == ord("r"):
                        handle_workflow_action(self.stdscr, "resume", self._get_selected_item(), self.data_fetcher)
                        need_render = True
                    elif key == ord("c"):
                        handle_workflow_action(self.stdscr, "cancel", self._get_selected_item(), self.data_fetcher)
                        need_render = True
                    elif key == curses.KEY_RESIZE:
                        self.stdscr.clear()
                        need_render = True

                except curses.error:
                    pass

        except KeyboardInterrupt:
            pass

        finally:
            if self.fetcher_thread:
                self.fetcher_thread.stop()
                self.fetcher_thread.join(timeout=1.0)

    def _render_loading(self) -> None:
        """Render loading state."""
        self.stdscr.erase()
        height, width = self.stdscr.getmaxyx()
        render_header(self.stdscr, width, self.app_filter, self.refresh_interval)
        msg = "Loading data..."
        addstr(self.stdscr, height // 2, (width - len(msg)) // 2, msg, curses.A_DIM)
        self.stdscr.refresh()

    def _render(self, data: MonitorData) -> None:
        """Render the complete display."""
        self.stdscr.erase()
        height, width = self.stdscr.getmaxyx()

        if height < 5 or width < 60:
            addstr(self.stdscr, 0, 0, "Terminal too small (min 60x5)", curses.A_BOLD)
            self.stdscr.refresh()
            return

        render_header(self.stdscr, width, self.app_filter, self.refresh_interval)
        addstr(self.stdscr, 1, 0, "─" * (width - 1), curses.A_DIM)

        content_height = max(1, height - 4)
        content_start_y = 2

        self.lines = build_line_metadata(data)
        total_lines = len(self.lines)

        if not self._initial_scroll_done and total_lines > 0:
            self.selected_index = total_lines - 1
            self._initial_scroll_done = True

        if total_lines > 0:
            self.selected_index = min(self.selected_index, total_lines - 1)
        else:
            self.selected_index = 0

        if self._auto_follow:
            if self.selected_index < self.scroll_offset:
                self.scroll_offset = self.selected_index
            elif self.selected_index >= self.scroll_offset + content_height:
                self.scroll_offset = self.selected_index - content_height + 1

        max_scroll = max(0, total_lines - content_height)
        self.scroll_offset = max(0, min(self.scroll_offset, max_scroll))

        visible_lines = self.lines[self.scroll_offset : self.scroll_offset + content_height]

        for i, line_info in enumerate(visible_lines):
            y = content_start_y + i
            if y < height - 2:
                absolute_index = self.scroll_offset + i
                is_selected = absolute_index == self.selected_index
                render_line(self.stdscr, y, width, line_info, is_selected)

        render_footer(self.stdscr, data, height, width)
        self.stdscr.refresh()

    def _get_selected_item(self) -> dict[str, Any] | None:
        """Get the currently selected line item."""
        if 0 <= self.selected_index < len(self.lines):
            return self.lines[self.selected_index]
        return None


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
