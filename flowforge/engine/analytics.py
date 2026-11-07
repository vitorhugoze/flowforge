"""Operational metrics and telemetry computation over SQLite tables."""

from __future__ import annotations

from flowforge.domain.enums import RunStatus, ScheduleStatus, StepRunStatus, WorkflowStatus
from flowforge.domain.schedule_audit import MetricsSnapshot
from flowforge.storage.database import DatabaseManager


class TelemetryAnalyzer:
    """Computes aggregated health and performance metrics across FlowForge."""

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    def snapshot(self) -> MetricsSnapshot:
        with self._db.session() as conn:
            wf_rows = conn.execute(
                "SELECT status, COUNT(*) AS cnt FROM workflows GROUP BY status"
            ).fetchall()
            run_rows = conn.execute(
                "SELECT status, COUNT(*) AS cnt FROM workflow_runs GROUP BY status"
            ).fetchall()
            avg_row = conn.execute(
                "SELECT AVG(duration_ms) AS avg_ms FROM step_runs WHERE duration_ms IS NOT NULL"
            ).fetchone()
            fail_rows = conn.execute(
                """
                SELECT step_key, COUNT(*) AS cnt
                FROM step_runs
                WHERE status = ?
                GROUP BY step_key
                ORDER BY cnt DESC
                """,
                (StepRunStatus.FAILED.value,),
            ).fetchall()
            sched_row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM schedules WHERE status = ?",
                (ScheduleStatus.ACTIVE.value,),
            ).fetchone()
            dlq_row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM dead_letters WHERE requeued = 0"
            ).fetchone()

        wf_by_status = {r["status"]: int(r["cnt"]) for r in wf_rows}
        total_workflows = sum(wf_by_status.values())
        active_workflows = wf_by_status.get(WorkflowStatus.ACTIVE.value, 0)

        runs_by_status = {r["status"]: int(r["cnt"]) for r in run_rows}
        total_runs = sum(runs_by_status.values())
        terminal_runs = runs_by_status.get(RunStatus.SUCCEEDED.value, 0) + runs_by_status.get(
            RunStatus.FAILED.value, 0
        )
        success_rate = (
            round(runs_by_status.get(RunStatus.SUCCEEDED.value, 0) / terminal_runs, 4)
            if terminal_runs > 0
            else 0.0
        )
        avg_ms = (
            round(float(avg_row["avg_ms"]), 3)
            if avg_row is not None and avg_row["avg_ms"] is not None
            else 0.0
        )
        step_failures = {r["step_key"]: int(r["cnt"]) for r in fail_rows}

        return MetricsSnapshot(
            total_workflows=total_workflows,
            active_workflows=active_workflows,
            total_runs=total_runs,
            runs_by_status=runs_by_status,
            success_rate=success_rate,
            avg_step_duration_ms=avg_ms,
            step_failure_counts=step_failures,
            active_schedules=int(sched_row["cnt"]) if sched_row else 0,
            unresolved_dead_letters=int(dlq_row["cnt"]) if dlq_row else 0,
        )
