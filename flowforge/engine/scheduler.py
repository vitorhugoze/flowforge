"""Periodic and one-shot workflow scheduler evaluation loop."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from flowforge.domain.enums import AuditCategory, WorkflowStatus
from flowforge.domain.run import RunTriggerRequest, WorkflowRun
from flowforge.storage.audit_repo import AuditRepository
from flowforge.storage.schedule_repo import ScheduleRepository
from flowforge.storage.workflow_repo import WorkflowRepository
from flowforge.utils_time import utc_now


@dataclass(frozen=True)
class SchedulerTickReport:
    """Summary of schedules evaluated and runs triggered at a point in time."""

    evaluated_at: datetime
    triggered_count: int
    skipped_paused_workflows: int
    triggered_run_ids: list[str]


class SchedulerEngine:
    """Evaluates active ScheduleRule records and triggers workflow runs when due."""

    def __init__(
        self,
        schedule_repo: ScheduleRepository,
        workflow_repo: WorkflowRepository,
        audit_repo: AuditRepository,
        trigger_callback: Callable[[str, RunTriggerRequest], WorkflowRun],
    ) -> None:
        self._schedules = schedule_repo
        self._workflows = workflow_repo
        self._audit = audit_repo
        self._trigger = trigger_callback

    def tick(self, as_of: datetime | None = None) -> SchedulerTickReport:
        cutoff = as_of or utc_now()
        due_rules = self._schedules.list_due(cutoff)
        triggered_runs: list[str] = []
        skipped_paused = 0

        for rule in due_rules:
            workflow = self._workflows.get(rule.workflow_id)
            if workflow.status != WorkflowStatus.ACTIVE:
                skipped_paused += 1
                continue

            run = self._trigger(
                rule.workflow_id,
                RunTriggerRequest(
                    inputs=dict(rule.inputs),
                    priority=rule.priority,
                    triggered_by=f"schedule:{rule.name}",
                ),
            )
            triggered_runs.append(run.run_id)

            new_count = rule.run_count + 1
            reached_max = rule.max_runs is not None and new_count >= rule.max_runs
            is_one_shot = rule.interval_seconds is None
            completed = is_one_shot or reached_max

            next_dt: datetime | None = None
            if not completed and rule.interval_seconds is not None:
                step = timedelta(seconds=rule.interval_seconds)
                candidate = rule.next_run_at + step
                while candidate <= cutoff:
                    candidate += step
                next_dt = candidate

            self._schedules.record_execution(
                rule.schedule_id,
                run_id=run.run_id,
                executed_at=cutoff,
                next_run_at=next_dt,
                completed=completed,
            )
            self._audit.record(
                AuditCategory.SCHEDULE,
                "schedule.triggered",
                rule.schedule_id,
                run_id=run.run_id,
                details={"schedule_name": rule.name, "completed": completed},
            )

        return SchedulerTickReport(
            evaluated_at=cutoff,
            triggered_count=len(triggered_runs),
            skipped_paused_workflows=skipped_paused,
            triggered_run_ids=triggered_runs,
        )
