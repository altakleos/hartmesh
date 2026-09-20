DEER_FLOW_HOME ?= $(CURDIR)/.deer-flow
DEER_FLOW_HOME := $(abspath $(DEER_FLOW_HOME))
BACKEND_SANDBOX_HOME := $(abspath $(CURDIR)/sandbox)

install:
	uv sync --locked

dev:
	mkdir -p "$(DEER_FLOW_HOME)" "$(BACKEND_SANDBOX_HOME)"
	PYTHONPATH=. PYTHONIOENCODING=utf-8 PYTHONUTF8=1 DEER_FLOW_HOME="$(DEER_FLOW_HOME)" uv run --locked uvicorn app.gateway.app:app --host 0.0.0.0 --port 8001 \
		--reload \
		--reload-include='*.yaml' \
		--reload-include='.env' \
		--reload-exclude='*.pyc' \
		--reload-exclude='__pycache__' \
		--reload-exclude="$(BACKEND_SANDBOX_HOME)" \
		--reload-exclude="$(DEER_FLOW_HOME)"

gateway:
	PYTHONPATH=. PYTHONIOENCODING=utf-8 PYTHONUTF8=1 uv run --locked uvicorn app.gateway.app:app --host 0.0.0.0 --port 8001

# The offline suite runs one worker per core. It is wall-clock bound, not CPU
# bound -- a serial run leaves seven of eight cores idle and takes over an hour
# where the same tests finish in a fraction of that in parallel. `loadgroup`
# scatters per test, which measured materially faster than per-file, and pins
# only the modules that declare `pytest.mark.xdist_group` because they claim a
# process-external resource by a fixed name (the Docker-backed reconciliation
# e2e removes every container matching its own prefix in an autouse fixture).
# Set PYTEST_WORKERS=0 to run serially -- required when
# DEERFLOW_TEST_POSTGRES_URL is configured, because those tests share one
# database that nothing partitions per worker.
PYTEST_WORKERS ?= auto
PYTEST_DIST ?= loadgroup
test:
	PYTHONPATH=. PYTHONIOENCODING=utf-8 PYTHONUTF8=1 uv run --locked --extra opensandbox pytest -m "not live" --ignore=tests/blocking_io $(if $(filter-out 0,$(PYTEST_WORKERS)),-n $(PYTEST_WORKERS) --dist $(PYTEST_DIST),) tests/ -rs

test-live:
	DEER_FLOW_RUN_LIVE_TESTS=1 PYTHONPATH=. PYTHONIOENCODING=utf-8 PYTHONUTF8=1 uv run pytest -m live tests/ -v -s

test-blocking-io:
	PYTHONPATH=. PYTHONIOENCODING=utf-8 PYTHONUTF8=1 uv run pytest tests/blocking_io -q --tb=short

# Run the backend unit-test suite as one shard of a parallel CI split.
#   SPLITS = total number of shards; GROUP = 1-based shard index.
# Duration-aware: `least_duration` balances the shards by real wall-clock cost
# read from .test_durations. Shards only READ that file (no --store-durations),
# so concurrent CI jobs cannot race writes on it. Refresh it with
# `make test-shard-durations` after a meaningful change to the test set.
# `make test-shard SPLITS=4 GROUP=2` runs shard 2 of 4; a bare `make test-shard`
# runs the whole (non-live) suite as a single split, matching `make test`.
SPLITS ?= 1
GROUP ?= 1
DURATIONS_FILE ?= .test_durations
test-shard:
	@test -f "$(DURATIONS_FILE)" || \
		(echo "error: $(DURATIONS_FILE) is missing; run 'make test-shard-durations' to regenerate the duration baseline" >&2; exit 1)
	PYTHONPATH=. PYTHONIOENCODING=utf-8 PYTHONUTF8=1 uv run pytest -m "not live" --ignore=tests/blocking_io --splits $(SPLITS) --group $(GROUP) --splitting-algorithm least_duration --durations-path=$(DURATIONS_FILE) tests/ -q

# Regenerate .test_durations from the full offline (non-live, non-blocking-I/O)
# suite so duration-aware sharding reflects real wall-clock cost. Run locally or
# in a periodic job, then commit the refreshed file.
test-shard-durations:
	PYTHONPATH=. PYTHONIOENCODING=utf-8 PYTHONUTF8=1 uv run pytest -m "not live" --ignore=tests/blocking_io --store-durations --clean-durations --durations-path=$(DURATIONS_FILE) tests/ -q

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check . --fix && uv run ruff format .

detect-blocking-io:
	@PYTHONPATH=. PYTHONIOENCODING=utf-8 PYTHONUTF8=1 uv run python ../scripts/detect_blocking_io_static.py --output ../.deer-flow/blocking-io-findings.json

# Generate a new alembic revision by autogenerating the diff against the live
# ORM models. Usage: make migrate-rev MSG="add foo column to runs"
# The Gateway runs `alembic upgrade head` automatically at startup via
# `bootstrap_schema`, so there is intentionally no `migrate` / `migrate-stamp`
# target -- the single execution path keeps ops mistakes off the table.
# The script builds a fresh temp SQLite at head, then diffs the live models
# against it, so a clean checkout does NOT need a pre-existing ./data/deerflow.db.
migrate-rev:
	@if [ -z "$(MSG)" ]; then echo 'usage: make migrate-rev MSG="describe the change"'; exit 1; fi
	PYTHONPATH=. PYTHONIOENCODING=utf-8 PYTHONUTF8=1 uv run python scripts/_autogen_revision.py "$(MSG)"
