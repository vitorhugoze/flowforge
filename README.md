# FlowForge (`flowforge`)

Declarative DAG workflow and task orchestration engine with priority queues, configurable retry policies, a Dead-Letter Queue (DLQ), deterministic scheduling, an immutable audit trail, a JSON CLI, and a REST API built with **FastAPI + Pydantic + SQLite**.

## Overview

**FlowForge** provides a local-first orchestration core for defining multi-step pipelines, enqueueing runs across priority levels (`low`, `normal`, `high`, `critical`), executing tasks with retry backoff (`fixed`, `linear`, `exponential`), recovering exhausted runs from the DLQ, evaluating one-shot or recurring schedules, and inspecting operational telemetry via both CLI commands and HTTP endpoints.

### Package Structure

- `flowforge/domain/` - Pydantic v2 schemas, enumerations, and finite-state-machine transition guards (`RunStatus`, `StepRunStatus`, `QueueItemStatus`).
- `flowforge/storage/` - SQLite connection/session manager (`WAL` mode + foreign keys) and repositories for workflows, runs, queues, dead letters, schedules, and audit events.
- `flowforge/handlers/` - Pluggable step handler registry (field selection, value mapping, string templates, predicate filtering, record validation, numeric aggregation, deduplication, control gates, and sandboxed JSON artifact I/O).
- `flowforge/engine/` - Topological DAG planner (cycle detection and execution wave grouping), retry backoff calculator, step executor (reusing completed upstream steps across retry attempts), priority queue worker, scheduler engine, and telemetry analyzer.
- `flowforge/services/` - Unified `OrchestratorService` facade shared by the CLI and HTTP API.
- `flowforge/api/` - Modular FastAPI application (`/workflows`, `/runs`, `/queue/stats`, `/worker/tick`, `/dlq`, `/schedules`, `/audit`, `/metrics`, `/health`).
- `flowforge/cli/` - Command-line interface returning structured JSON envelopes.
- `tests/` - `pytest` suite covering DAG planning, built-in handlers, retry/backoff execution, DLQ recovery, priority queue ordering, schedules, REST endpoints, and CLI subcommands.

---

## Setup

### Linux / macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
```

### Windows (PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
```

---

## Linting and Tests

```bash
ruff check .
pytest -v --cov=flowforge --cov-report=term-missing
```

All tests isolate SQLite databases and artifact directories inside temporary directories (`tmp_path`), leaving the working tree clean.

---

## Usage Examples

### 1. CLI Workflow Execution

Pass `--db-path` (or set `FLOWFORGE_DB_PATH`) to store state in any target SQLite file:

```bash
# List built-in step handlers
python -m flowforge.cli.main --db-path /tmp/flowforge-demo.db handlers

# Register a two-step pipeline via stdin ('-')
echo '{"name":"sales-summary","steps":[{"key":"clean","name":"Deduplicate","handler":"aggregate.deduplicate","config":{"key_field":"id"}},{"key":"stats","name":"Summarize","handler":"aggregate.numeric_summary","depends_on":["clean"],"config":{"field":"total"}}]}' \
  | python -m flowforge.cli.main --db-path /tmp/flowforge-demo.db workflow-create -

# Trigger a high-priority run
echo '{"records":[{"id":1,"total":100},{"id":1,"total":100},{"id":2,"total":250}]}' \
  | python -m flowforge.cli.main --db-path /tmp/flowforge-demo.db run-trigger sales-summary --priority high --inputs -

# Process queued runs and inspect operational metrics
python -m flowforge.cli.main --db-path /tmp/flowforge-demo.db worker-tick
python -m flowforge.cli.main --db-path /tmp/flowforge-demo.db metrics
```

### 2. HTTP Server (FastAPI)

```bash
python -m flowforge.cli.main serve --port 8000
```

Interactive OpenAPI documentation is available at `/docs` while the server is running.

---

## Environment Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `FLOWFORGE_DB_PATH` | `.flowforge/flowforge.db` | Path to the SQLite database file |
| `FLOWFORGE_ARTIFACT_DIR` | `.flowforge/artifacts` | Sandboxed directory for step JSON artifact read/write |
| `FLOWFORGE_DEFAULT_QUEUE` | `default` | Default task queue name |
| `FLOWFORGE_WORKER_BATCH_SIZE` | `10` | Default maximum tasks processed per worker tick |
