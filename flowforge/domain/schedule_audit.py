"""Schedule definitions, structured audit events, and operational metrics."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from flowforge.domain.enums import AuditCategory, PriorityLevel, ScheduleStatus


class ScheduleCreate(BaseModel):
    """Input payload for creating a one-shot or recurring workflow schedule."""

    name: str = Field(..., min_length=2, max_length=100)
    workflow_id: str
    next_run_at: datetime
    interval_seconds: int | None = Field(default=None, ge=1, le=31_536_000)
    inputs: dict[str, Any] = Field(default_factory=dict)
    priority: PriorityLevel = PriorityLevel.NORMAL
    max_runs: int | None = Field(default=None, ge=1)


class ScheduleRule(BaseModel):
    """Persisted schedule entity evaluated by SchedulerEngine."""

    schedule_id: str
    name: str
    workflow_id: str
    status: ScheduleStatus
    next_run_at: datetime
    interval_seconds: int | None = None
    inputs: dict[str, Any] = Field(default_factory=dict)
    priority: PriorityLevel = PriorityLevel.NORMAL
    run_count: int = 0
    max_runs: int | None = None
    last_run_at: datetime | None = None
    last_run_id: str | None = None
    created_at: datetime
    updated_at: datetime


class AuditEvent(BaseModel):
    """Immutable audit log entry capturing important lifecycle actions."""

    event_id: str
    category: AuditCategory
    action: str
    entity_id: str
    run_id: str | None = None
    actor: str = "system"
    details: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class MetricsSnapshot(BaseModel):
    """Aggregated operational telemetry across workflows, runs, steps, and queues."""

    total_workflows: int
    active_workflows: int
    total_runs: int
    runs_by_status: dict[str, int]
    success_rate: float
    avg_step_duration_ms: float
    step_failure_counts: dict[str, int]
    active_schedules: int
    unresolved_dead_letters: int
