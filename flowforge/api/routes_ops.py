"""REST endpoints for schedules, audit events, metrics, and service health."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from flowforge.api.deps import get_orchestrator
from flowforge.domain.enums import AuditCategory, ScheduleStatus
from flowforge.domain.schedule_audit import (
    AuditEvent,
    MetricsSnapshot,
    ScheduleCreate,
    ScheduleRule,
)
from flowforge.services.orchestrator import OrchestratorService

router = APIRouter(tags=["operations"])
ServiceDep = Annotated[OrchestratorService, Depends(get_orchestrator)]


class SchedulerTickRequest(BaseModel):
    as_of: datetime | None = None


@router.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "healthy", "service": "flowforge"}


@router.post("/schedules", status_code=status.HTTP_201_CREATED)
def create_schedule(payload: ScheduleCreate, service: ServiceDep) -> ScheduleRule:
    return service.create_schedule(payload, actor="api")


@router.get("/schedules")
def list_schedules(
    service: ServiceDep,
    schedule_status: ScheduleStatus | None = None,
) -> list[ScheduleRule]:
    return service.list_schedules(status=schedule_status)


@router.post("/schedules/{schedule_ref}/pause")
def pause_schedule(schedule_ref: str, service: ServiceDep) -> ScheduleRule:
    return service.set_schedule_status(schedule_ref, ScheduleStatus.PAUSED, actor="api")


@router.post("/schedules/{schedule_ref}/resume")
def resume_schedule(schedule_ref: str, service: ServiceDep) -> ScheduleRule:
    return service.set_schedule_status(schedule_ref, ScheduleStatus.ACTIVE, actor="api")


@router.post("/schedules/tick")
def tick_schedules(
    service: ServiceDep,
    payload: SchedulerTickRequest | None = None,
) -> dict[str, Any]:
    req = payload or SchedulerTickRequest()
    report = service.run_scheduler_tick(as_of=req.as_of)
    return {
        "evaluated_at": report.evaluated_at.isoformat(),
        "triggered_count": report.triggered_count,
        "skipped_paused_workflows": report.skipped_paused_workflows,
        "triggered_run_ids": report.triggered_run_ids,
    }


@router.get("/audit")
def list_audit(
    service: ServiceDep,
    category: AuditCategory | None = None,
    entity_id: str | None = None,
    run_id: str | None = None,
    limit: int = 100,
) -> list[AuditEvent]:
    return service.list_audit_events(
        category=category,
        entity_id=entity_id,
        run_id=run_id,
        limit=limit,
    )


@router.get("/metrics")
def get_metrics(service: ServiceDep) -> MetricsSnapshot:
    return service.get_metrics()
