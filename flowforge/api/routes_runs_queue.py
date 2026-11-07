"""REST endpoints for workflow runs, priority queue stats, worker ticks, and DLQ."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, Field

from flowforge.api.deps import get_orchestrator
from flowforge.domain.enums import RunStatus
from flowforge.domain.queue import DeadLetterEntry, QueueStats, QueueTask
from flowforge.domain.run import RunSummary, RunTriggerRequest, WorkflowRun
from flowforge.services.orchestrator import OrchestratorService

router = APIRouter(tags=["runs-and-queue"])
ServiceDep = Annotated[OrchestratorService, Depends(get_orchestrator)]


class WorkerTickRequest(BaseModel):
    queue_name: str | None = None
    max_tasks: int = Field(default=10, ge=1, le=500)
    as_of: datetime | None = None


class RequeueRequest(BaseModel):
    extra_attempts: int = Field(default=1, ge=1, le=10)


@router.post("/workflows/{workflow_ref}/runs", status_code=status.HTTP_201_CREATED)
def trigger_workflow_run(
    workflow_ref: str,
    service: ServiceDep,
    request: RunTriggerRequest | None = None,
) -> WorkflowRun:
    return service.trigger_run(workflow_ref, request)


@router.get("/runs")
def list_runs(
    service: ServiceDep,
    workflow_id: str | None = None,
    run_status: RunStatus | None = None,
    limit: int = 50,
) -> list[RunSummary]:
    return service.list_runs(workflow_id=workflow_id, status=run_status, limit=limit)


@router.get("/runs/{run_id}")
def get_run(run_id: str, service: ServiceDep) -> WorkflowRun:
    return service.get_run(run_id)


@router.post("/runs/{run_id}/cancel")
def cancel_run(run_id: str, service: ServiceDep) -> WorkflowRun:
    return service.cancel_run(run_id, actor="api")


@router.get("/queue/stats")
def get_queue_stats(service: ServiceDep, queue_name: str | None = None) -> QueueStats:
    return service.get_queue_stats(queue_name=queue_name)


@router.post("/worker/tick")
def worker_tick(
    service: ServiceDep,
    payload: WorkerTickRequest | None = None,
) -> dict[str, Any]:
    req = payload or WorkerTickRequest()
    report = service.run_worker_tick(
        queue_name=req.queue_name,
        max_tasks=req.max_tasks,
        as_of=req.as_of,
    )
    return asdict(report)


@router.get("/dlq")
def list_dead_letters(
    service: ServiceDep,
    include_requeued: bool = False,
) -> list[DeadLetterEntry]:
    return service.list_dead_letters(include_requeued=include_requeued)


@router.post("/dlq/{dlq_id}/requeue")
def requeue_dead_letter(
    dlq_id: str,
    service: ServiceDep,
    payload: RequeueRequest | None = None,
) -> QueueTask:
    req = payload or RequeueRequest()
    return service.requeue_dead_letter(dlq_id, extra_attempts=req.extra_attempts, actor="api")
