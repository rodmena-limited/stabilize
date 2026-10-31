"""`stabilize --version` prints the installed version and exits 0 (#105)."""

from __future__ import annotations

import subprocess
import sys

from stabilize import __version__


def test_the_cli_reports_its_version() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "stabilize.cli.main", "--version"], capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == f"stabilize {__version__}"


def test_the_help_still_lists_the_commands() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "stabilize.cli.main", "--help"], capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0 and "mg-up" in result.stdout and "--version" in result.stdout
