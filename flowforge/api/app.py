"""FastAPI application factory for FlowForge."""

from __future__ import annotations

from fastapi import FastAPI

from flowforge.api.errors import register_exception_handlers
from flowforge.api.routes_ops import router as ops_router
from flowforge.api.routes_runs_queue import router as runs_queue_router
from flowforge.api.routes_workflows import router as workflows_router
from flowforge.config import Settings
from flowforge.services.orchestrator import OrchestratorService


def create_app(
    service: OrchestratorService | None = None,
    settings: Settings | None = None,
) -> FastAPI:
    """Create and configure the FlowForge FastAPI web application."""
    app = FastAPI(
        title="FlowForge Orchestrator API",
        version="0.1.0",
        description="Workflow DAG execution, priority task queues, scheduling, and audit API.",
    )
    app.state.orchestrator = service or OrchestratorService(settings=settings)
    register_exception_handlers(app)

    app.include_router(workflows_router)
    app.include_router(runs_queue_router)
    app.include_router(ops_router)
    return app
