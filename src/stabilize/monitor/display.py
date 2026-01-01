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
