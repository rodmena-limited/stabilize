"""Background data fetcher thread for the monitor display."""

from __future__ import annotations

import queue
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.monitor.data import MonitorData, MonitorDataFetcher


class DataFetcherThread(threading.Thread):
    """Background thread for fetching monitoring data."""

    def __init__(
        self,
        fetcher: MonitorDataFetcher,
        data_queue: queue.Queue[MonitorData],
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
