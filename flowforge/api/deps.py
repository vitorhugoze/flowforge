"""FastAPI dependency injection helpers."""

from __future__ import annotations

from fastapi import Request

from flowforge.services.orchestrator import OrchestratorService


def get_orchestrator(request: Request) -> OrchestratorService:
    """Retrieve the active OrchestratorService attached to FastAPI app state."""
    service: OrchestratorService = request.app.state.orchestrator
    return service
