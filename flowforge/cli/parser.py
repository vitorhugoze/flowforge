"""ArgumentParser builder for the FlowForge CLI."""

from __future__ import annotations

import argparse


def build_cli_parser() -> argparse.ArgumentParser:
    """Construct the command-line parser for FlowForge operations."""
    parser = argparse.ArgumentParser(description="FlowForge Workflow & Task Orchestrator CLI")
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional SQLite database file path (overrides FLOWFORGE_DB_PATH)",
    )
    parser.add_argument(
        "--artifact-dir",
        default=None,
        help="Optional directory for sandboxed step artifacts",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("handlers", help="List registered step handlers")

    wf_create = sub.add_parser("workflow-create", help="Register a workflow from JSON or '-' (stdin)")
    wf_create.add_argument("spec_json", help="WorkflowCreate JSON payload or '-' for stdin")

    wf_list = sub.add_parser("workflow-list", help="List workflows")
    wf_list.add_argument("--status", choices=["active", "paused", "archived"], default=None)

    wf_get = sub.add_parser("workflow-get", help="Inspect a workflow by ID or name")
    wf_get.add_argument("workflow_ref", help="Workflow ID or name")

    run_trig = sub.add_parser("run-trigger", help="Trigger a workflow run and enqueue it")
    run_trig.add_argument("workflow_ref", help="Workflow ID or name")
    run_trig.add_argument("--inputs", default="{}", help="JSON object of run inputs or '-' for stdin")
    run_trig.add_argument(
        "--priority",
        choices=["low", "normal", "high", "critical"],
        default=None,
    )
    run_trig.add_argument("--dedup-ref", default=None)

    run_get = sub.add_parser("run-get", help="Inspect a workflow run and its step traces")
    run_get.add_argument("run_id", help="Target run ID")

    run_list = sub.add_parser("run-list", help="List workflow runs")
    run_list.add_argument("--workflow-id", default=None)
    run_list.add_argument("--status", default=None)
    run_list.add_argument("--limit", type=int, default=50)

    run_cancel = sub.add_parser("run-cancel", help="Cancel a queued workflow run")
    run_cancel.add_argument("run_id", help="Target run ID")

    worker_tick = sub.add_parser("worker-tick", help="Claim and execute due queued tasks")
    worker_tick.add_argument("--queue", default=None, help="Queue name")
    worker_tick.add_argument("--max-tasks", type=int, default=10)
    worker_tick.add_argument("--as-of", default=None, help="Optional ISO-8601 UTC clock override")

    q_stats = sub.add_parser("queue-stats", help="Show queue counts by status and priority")
    q_stats.add_argument("--queue", default=None, help="Queue name")

    dlq_list = sub.add_parser("dlq-list", help="List dead-letter queue entries")
    dlq_list.add_argument("--include-requeued", action="store_true")

    dlq_req = sub.add_parser("dlq-requeue", help="Requeue a dead-lettered entry")
    dlq_req.add_argument("dlq_id", help="Dead-letter entry ID")
    dlq_req.add_argument("--extra-attempts", type=int, default=1)

    sched_create = sub.add_parser("schedule-create", help="Create a workflow schedule")
    sched_create.add_argument("name", help="Unique schedule name")
    sched_create.add_argument("workflow_ref", help="Workflow ID or name")
    sched_create.add_argument("--run-at", required=True, help="First ISO-8601 UTC execution time")
    sched_create.add_argument("--interval-seconds", type=int, default=None)
    sched_create.add_argument("--max-runs", type=int, default=None)
    sched_create.add_argument("--inputs", default="{}")

    sub.add_parser("schedule-list", help="List registered workflow schedules")

    sched_tick = sub.add_parser("schedule-tick", help="Evaluate due schedules and enqueue runs")
    sched_tick.add_argument("--as-of", default=None, help="Optional ISO-8601 UTC clock override")

    audit_cmd = sub.add_parser("audit-list", help="List audit trail events")
    audit_cmd.add_argument("--run-id", default=None)
    audit_cmd.add_argument("--limit", type=int, default=50)

    sub.add_parser("metrics", help="Show aggregated operational telemetry")

    serve_cmd = sub.add_parser("serve", help="Run the HTTP server with uvicorn")
    serve_cmd.add_argument("--interface", default=None, help="Optional network interface to bind")
    serve_cmd.add_argument("--port", type=int, default=8000)

    return parser
