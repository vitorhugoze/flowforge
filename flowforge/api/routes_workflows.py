"""REST endpoints for workflow definitions and step handler inspection."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from flowforge.api.deps import get_orchestrator
from flowforge.domain.enums import WorkflowStatus
from flowforge.domain.workflow import WorkflowCreate, WorkflowDefinition, WorkflowUpdate
from flowforge.engine.dag import TopologicalPlanner
from flowforge.services.orchestrator import OrchestratorService

router = APIRouter(tags=["workflows"])
ServiceDep = Annotated[OrchestratorService, Depends(get_orchestrator)]


@router.get("/handlers")
def list_handlers(service: ServiceDep) -> list[dict[str, str]]:
    return service.list_handlers()


@router.post("/workflows", status_code=status.HTTP_201_CREATED)
def create_workflow(payload: WorkflowCreate, service: ServiceDep) -> WorkflowDefinition:
    return service.create_workflow(payload, actor="api")


@router.get("/workflows")
def list_workflows(
    service: ServiceDep,
    workflow_status: WorkflowStatus | None = None,
) -> list[WorkflowDefinition]:
    return service.list_workflows(status=workflow_status)


@router.get("/workflows/{workflow_ref}")
def get_workflow(workflow_ref: str, service: ServiceDep) -> WorkflowDefinition:
    return service.get_workflow(workflow_ref)


@router.get("/workflows/{workflow_ref}/waves")
def get_workflow_waves(workflow_ref: str, service: ServiceDep) -> dict[str, list[list[str]]]:
    wf = service.get_workflow(workflow_ref)
    return {"waves": TopologicalPlanner.execution_waves(wf.steps)}


@router.patch("/workflows/{workflow_ref}")
def update_workflow(
    workflow_ref: str,
    patch: WorkflowUpdate,
    service: ServiceDep,
) -> WorkflowDefinition:
    return service.update_workflow(workflow_ref, patch, actor="api")
