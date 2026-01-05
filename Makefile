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

# Run all quality checks
check: lint type-check test

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
