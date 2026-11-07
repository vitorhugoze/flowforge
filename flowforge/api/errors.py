"""Domain-to-HTTP exception mapping for the FastAPI application."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from flowforge.exceptions import (
    DuplicateEntityError,
    EntityNotFoundError,
    FlowForgeError,
    HandlerNotFoundError,
    InvalidStateTransitionError,
    ScheduleError,
    ValidationFailedError,
)


def register_exception_handlers(app: FastAPI) -> None:
    """Attach JSON error handlers for domain exceptions."""

    @app.exception_handler(EntityNotFoundError)
    async def _not_found(_req: Request, exc: EntityNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(DuplicateEntityError)
    async def _duplicate(_req: Request, exc: DuplicateEntityError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(InvalidStateTransitionError)
    async def _invalid_transition(_req: Request, exc: InvalidStateTransitionError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(ValidationFailedError)
    @app.exception_handler(HandlerNotFoundError)
    @app.exception_handler(ScheduleError)
    async def _unprocessable(_req: Request, exc: FlowForgeError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})
