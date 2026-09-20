# Stabilize Makefile
# Run tests on SQLite and PostgreSQL backends

.PHONY: help test golden-tests golden-tests-sqlite golden-tests-postgres test-all lint type-check

# Default target
help:
	@echo "Stabilize Test Commands:"
	@echo ""
	@echo "  make test                  - Run all unit tests (both backends)"
	@echo "  make test-sqlite           - Run unit tests on SQLite only"
	@echo "  make test-postgres         - Run unit tests on PostgreSQL only (requires Docker)"
	@echo ""
	@echo "  make golden-tests          - Run golden standard tests (both backends)"
	@echo "  make golden-tests-sqlite   - Run golden tests on SQLite only"
	@echo "  make golden-tests-postgres - Run golden tests on PostgreSQL only (requires Docker)"
	@echo ""
	@echo "  make test-all              - Run all tests including golden standards"
	@echo ""
	@echo "  make lint                  - Run ruff linter"
	@echo "  make type-check            - Run mypy type checker"
	@echo "  make check                 - Run lint + type-check + tests"
	@echo ""

# =============================================================================
# Unit Tests
# =============================================================================

# Run all unit tests (both backends)
test:
	python -m pytest tests/ -v --tb=short

# Run unit tests on SQLite only
test-sqlite:
	python -m pytest tests/ -v --tb=short -k "sqlite"

# Run unit tests on PostgreSQL only (requires Docker)
test-postgres:
	python -m pytest tests/ -v --tb=short -k "postgres"

# =============================================================================
# Golden Standard Tests
# =============================================================================

# Run golden standard tests on both backends
golden-tests: golden-tests-sqlite golden-tests-postgres

# Run golden tests on SQLite only (fast, no Docker required)
golden-tests-sqlite:
	python -m pytest golden_standard_tests/ -v --tb=short -k "sqlite"

# Run golden tests on PostgreSQL only (requires Docker)
golden-tests-postgres:
	python -m pytest golden_standard_tests/ -v --tb=short -k "postgres"

# =============================================================================
# Combined Tests
# =============================================================================

# Run all tests including golden standards
test-all: test golden-tests

# =============================================================================
# Code Quality
# =============================================================================

# Run ruff linter
lint:
	ruff check src/ tests/ golden_standard_tests/

# Run ruff linter with auto-fix
lint-fix:
	ruff check src/ tests/ golden_standard_tests/ --fix

# Run mypy type checker
type-check:
	mypy src/

# =============================================================================
# Audit probes
# =============================================================================
# The probe suite is this repo's audit evidence. Until now no Makefile target
# ran it, so it was a check that existed and was never executed -- the failure
# mode the probes themselves exist to catch.
#
# Split the way the tests are split: 24 probes need nothing but Python, 6 need
# a live PostgreSQL container. A probe that CANNOT run is reported as SKIPPED,
# never as a failure.

# Docker-free probes (safe in check)
probes-sqlite:
	PROBE_SET=sqlite ./audit/evaluations/run_all.sh

# Probes needing a PostgreSQL container
probes-postgres:
	PROBE_SET=postgres ./audit/evaluations/run_all.sh

# Every probe
probes:
	PROBE_SET=all ./audit/evaluations/run_all.sh

# Run all quality checks.
# On success, record a stamp keyed to a fingerprint of the source tree so a
# release does not re-run a gate that is provably still green. Change any
# source file and the fingerprint moves, the stamp stops matching, and the
# tests run again.
check: GATE_FP := $(shell .venv/bin/python scripts/gate_stamp.py fingerprint)
check: lint type-check test probes-sqlite
	@.venv/bin/python scripts/gate_stamp.py write --results "lint+type-check+test+probes-sqlite" --expect "$(GATE_FP)"

# =============================================================================
# Development Helpers
# =============================================================================

# Install development dependencies
install-dev:
	pip install -e ".[dev]"

# Clean up cache files
clean:
	rm -rf __pycache__ .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
