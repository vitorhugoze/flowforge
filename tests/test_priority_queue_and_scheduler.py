"""Tests for priority queue ordering, deduplication references, and scheduled workflow triggers."""

from __future__ import annotations

from datetime import datetime, timezone

from flowforge.domain.enums import PriorityLevel, ScheduleStatus
from flowforge.domain.run import RunTriggerRequest
from flowforge.domain.schedule_audit import ScheduleCreate
from flowforge.domain.workflow import StepDefinition, WorkflowCreate
from flowforge.services.orchestrator import OrchestratorService


def test_priority_queue_claims_highest_priority_first(orchestrator: OrchestratorService) -> None:
    wf = orchestrator.create_workflow(
        WorkflowCreate(
            name="prio-flow",
            steps=[StepDefinition(key="s1", name="Step 1", handler="control.passthrough")],
        )
    )
    low_run = orchestrator.trigger_run(wf.name, RunTriggerRequest(priority=PriorityLevel.LOW))
    crit_run = orchestrator.trigger_run(wf.name, RunTriggerRequest(priority=PriorityLevel.CRITICAL))
    high_run = orchestrator.trigger_run(wf.name, RunTriggerRequest(priority=PriorityLevel.HIGH))

    stats = orchestrator.get_queue_stats()
    assert stats.queued_count == 3
    assert stats.queued_by_priority["CRITICAL"] == 1

    report = orchestrator.run_worker_tick(max_tasks=3)
    assert report.processed_run_ids == [crit_run.run_id, high_run.run_id, low_run.run_id]


def test_dedup_ref_returns_existing_run(orchestrator: OrchestratorService) -> None:
    wf = orchestrator.create_workflow(
        WorkflowCreate(
            name="deduplicated-flow",
            steps=[StepDefinition(key="passthrough", name="Step One", handler="control.passthrough")],
        )
    )
    first = orchestrator.trigger_run(wf.name, RunTriggerRequest(dedup_ref="batch_alpha"))
    second = orchestrator.trigger_run(wf.name, RunTriggerRequest(dedup_ref="batch_alpha"))
    assert first.run_id == second.run_id


def test_recurring_schedule_advances_and_respects_max_runs(
    orchestrator: OrchestratorService,
) -> None:
    wf = orchestrator.create_workflow(
        WorkflowCreate(
            name="hourly-sync",
            steps=[StepDefinition(key="s1", name="Sync", handler="control.passthrough")],
        )
    )
    base_time = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
    sched = orchestrator.create_schedule(
        ScheduleCreate(
            name="sync-every-hour",
            workflow_id=wf.name,
            next_run_at=base_time,
            interval_seconds=3600,
            max_runs=2,
            inputs={"source": "crm"},
        )
    )

    t1 = orchestrator.run_scheduler_tick(as_of=datetime(2026, 6, 1, 12, 5, tzinfo=timezone.utc))
    assert t1.triggered_count == 1
    updated = orchestrator.list_schedules()[0]
    assert updated.schedule_id == sched.schedule_id
    assert updated.status == ScheduleStatus.ACTIVE
    assert updated.next_run_at == datetime(2026, 6, 1, 13, 0, tzinfo=timezone.utc)

    t2 = orchestrator.run_scheduler_tick(as_of=datetime(2026, 6, 1, 13, 1, tzinfo=timezone.utc))
    assert t2.triggered_count == 1
    completed_sched = orchestrator.list_schedules()[0]
    assert completed_sched.status == ScheduleStatus.COMPLETED
    assert completed_sched.run_count == 2
