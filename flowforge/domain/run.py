"""Domain models representing workflow executions and per-step execution traces."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from flowforge.domain.enums import PriorityLevel, RunStatus, StepRunStatus


class RunTriggerRequest(BaseModel):
    """Parameters used when triggering a new run of an existing workflow."""

    inputs: dict[str, Any] = Field(default_factory=dict)
    priority: PriorityLevel | None = None
    queue_name: str | None = None
    triggered_by: str = Field(default="manual", min_length=1, max_length=100)
    idempotency_key: str | None = Field(default=None, max_length=120)


class StepRun(BaseModel):
    """Execution record for a single step within a workflow run."""

    step_run_id: str
    run_id: str
    step_key: str
    handler: str
    status: StepRunStatus
    attempt: int = 0
    input_data: dict[str, Any] = Field(default_factory=dict)
    output_data: dict[str, Any] | None = None
    error_message: str | None = None
    duration_ms: float | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


class WorkflowRun(BaseModel):
    """Complete workflow run state including its ordered step traces."""

    run_id: str
    workflow_id: str
    workflow_name: str
    workflow_version: int
    status: RunStatus
    priority: PriorityLevel
    triggered_by: str
    idempotency_key: str | None = None
    inputs: dict[str, Any] = Field(default_factory=dict)
    outputs: dict[str, Any] = Field(default_factory=dict)
    attempt_count: int = 0
    max_retries: int = 0
    last_error: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    steps: list[StepRun] = Field(default_factory=list)


class RunSummary(BaseModel):
    """Lightweight representation of a workflow run for list endpoints."""

    run_id: str
    workflow_id: str
    workflow_name: str
    workflow_version: int
    status: RunStatus
    priority: PriorityLevel
    triggered_by: str
    attempt_count: int
    last_error: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
