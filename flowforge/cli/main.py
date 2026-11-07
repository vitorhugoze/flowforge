"""Command-line execution entrypoint emitting structured JSON envelopes."""

from __future__ import annotations

import dataclasses
import json
import sys
from typing import Any

import uvicorn

from flowforge.api.app import create_app
from flowforge.cli.parser import build_cli_parser
from flowforge.config import get_settings
from flowforge.domain.enums import PriorityLevel, RunStatus, WorkflowStatus
from flowforge.domain.run import RunTriggerRequest
from flowforge.domain.schedule_audit import ScheduleCreate
from flowforge.domain.workflow import WorkflowCreate
from flowforge.services.orchestrator import OrchestratorService
from flowforge.utils_time import from_iso_utc

_PRIORITY_BY_NAME = {
    "low": PriorityLevel.LOW,
    "normal": PriorityLevel.NORMAL,
    "high": PriorityLevel.HIGH,
    "critical": PriorityLevel.CRITICAL,
}


def _read_json_arg(raw_arg: str) -> Any:
    text = sys.stdin.read() if raw_arg == "-" else raw_arg
    return json.loads(text)


def _envelope(operation: str, status: str, data: Any = None, error: str | None = None) -> str:
    return json.dumps(
        {"service": "flowforge", "operation": operation, "status": status, "data": data, "error": error},
        default=str,
    )


def run_cli(argv: list[str] | None = None) -> str:
    """Execute a FlowForge CLI command and return a serialized JSON response."""
    parser = build_cli_parser()
    args = parser.parse_args(argv)
    settings = get_settings(db_path=args.db_path, artifact_dir=args.artifact_dir)
    service = OrchestratorService(settings=settings)
    cmd: str = args.command

    try:
        if cmd == "serve":
            app = create_app(service=service)
            uvicorn.run(app, host=args.host, port=args.port)
            return _envelope(cmd, "ok", {"host": args.host, "port": args.port})

        if cmd == "handlers":
            return _envelope(cmd, "ok", service.list_handlers())

        if cmd == "workflow-create":
            payload = WorkflowCreate.model_validate(_read_json_arg(args.spec_json))
            wf = service.create_workflow(payload, actor="cli")
            return _envelope(cmd, "ok", wf.model_dump(mode="json"))

        if cmd == "workflow-list":
            wf_status = WorkflowStatus(args.status) if args.status else None
            items = [w.model_dump(mode="json") for w in service.list_workflows(status=wf_status)]
            return _envelope(cmd, "ok", items)

        if cmd == "workflow-get":
            wf = service.get_workflow(args.workflow_ref)
            return _envelope(cmd, "ok", wf.model_dump(mode="json"))

        if cmd == "run-trigger":
            prio = _PRIORITY_BY_NAME[args.priority] if args.priority else None
            req = RunTriggerRequest(
                inputs=_read_json_arg(args.inputs),
                priority=prio,
                triggered_by="cli",
                idempotency_key=args.idempotency_key,
            )
            run = service.trigger_run(args.workflow_ref, req)
            return _envelope(cmd, "ok", run.model_dump(mode="json"))

        if cmd == "run-get":
            run = service.get_run(args.run_id)
            return _envelope(cmd, "ok", run.model_dump(mode="json"))

        if cmd == "run-list":
            r_status = RunStatus(args.status) if args.status else None
            runs = service.list_runs(workflow_id=args.workflow_id, status=r_status, limit=args.limit)
            return _envelope(cmd, "ok", [r.model_dump(mode="json") for r in runs])

        if cmd == "run-cancel":
            cancelled = service.cancel_run(args.run_id, actor="cli")
            return _envelope(cmd, "ok", cancelled.model_dump(mode="json"))

        if cmd == "worker-tick":
            as_of_dt = from_iso_utc(args.as_of) if args.as_of else None
            report = service.run_worker_tick(
                queue_name=args.queue,
                max_tasks=args.max_tasks,
                as_of=as_of_dt,
            )
            return _envelope(cmd, "ok", dataclasses.asdict(report))

        if cmd == "queue-stats":
            stats = service.get_queue_stats(queue_name=args.queue)
            return _envelope(cmd, "ok", stats.model_dump(mode="json"))

        if cmd == "dlq-list":
            entries = service.list_dead_letters(include_requeued=args.include_requeued)
            return _envelope(cmd, "ok", [e.model_dump(mode="json") for e in entries])

        if cmd == "dlq-requeue":
            task = service.requeue_dead_letter(
                args.dlq_id,
                extra_attempts=args.extra_attempts,
                actor="cli",
            )
            return _envelope(cmd, "ok", task.model_dump(mode="json"))

        if cmd == "schedule-create":
            run_at_dt = from_iso_utc(args.run_at)
            assert run_at_dt is not None
            sched = service.create_schedule(
                ScheduleCreate(
                    name=args.name,
                    workflow_id=args.workflow_ref,
                    next_run_at=run_at_dt,
                    interval_seconds=args.interval_seconds,
                    max_runs=args.max_runs,
                    inputs=_read_json_arg(args.inputs),
                ),
                actor="cli",
            )
            return _envelope(cmd, "ok", sched.model_dump(mode="json"))

        if cmd == "schedule-list":
            scheds = [s.model_dump(mode="json") for s in service.list_schedules()]
            return _envelope(cmd, "ok", scheds)

        if cmd == "schedule-tick":
            as_of_dt = from_iso_utc(args.as_of) if args.as_of else None
            sched_report = service.run_scheduler_tick(as_of=as_of_dt)
            return _envelope(
                cmd,
                "ok",
                {
                    "evaluated_at": sched_report.evaluated_at.isoformat(),
                    "triggered_count": sched_report.triggered_count,
                    "skipped_paused_workflows": sched_report.skipped_paused_workflows,
                    "triggered_run_ids": sched_report.triggered_run_ids,
                },
            )

        if cmd == "audit-list":
            events = service.list_audit_events(run_id=args.run_id, limit=args.limit)
            return _envelope(cmd, "ok", [e.model_dump(mode="json") for e in events])

        if cmd == "metrics":
            return _envelope(cmd, "ok", service.get_metrics().model_dump(mode="json"))

        return _envelope(cmd, "error", None, f"Unhandled command '{cmd}'")
    except Exception as exc:
        return _envelope(cmd, "error", None, str(exc))


def main() -> None:
    print(run_cli())


if __name__ == "__main__":
    main()
