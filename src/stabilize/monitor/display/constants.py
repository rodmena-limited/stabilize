"""Constants for the monitor display."""

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
