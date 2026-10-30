"""Comprehensive documentation prompt for AI coding agents."""

from __future__ import annotations

from importlib.resources import files

# =============================================================================
# PROMPT - Comprehensive documentation for AI coding agents
# =============================================================================

PROMPT_TEXT = files("stabilize.cli").joinpath("prompt_text.txt").read_text(encoding="utf-8")
