"""Priority queue tasks, dead-letter queue entries, and backlog statistics."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from flowforge.domain.enums import PriorityLevel, QueueItemStatus


class EnqueueRequest(BaseModel):
    """Parameters for placing a workflow run onto a priority queue."""

    run_id: str
    workflow_id: str
    queue_name: str = "default"
    priority: PriorityLevel = PriorityLevel.NORMAL
    available_at: datetime | None = None
    max_attempts: int = Field(default=3, ge=1, le=25)
    payload: dict[str, Any] = Field(default_factory=dict)


class QueueTask(BaseModel):
    """Persisted task entry managed by QueueRepository and claimed by workers."""

    task_id: str
    run_id: str
    workflow_id: str
    queue_name: str
    priority: int
    status: QueueItemStatus
    attempt: int
    max_attempts: int
    available_at: datetime
    claimed_by: str | None = None
    claimed_at: datetime | None = None
    last_error: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class DeadLetterEntry(BaseModel):
    """Record stored when a task exhausts its retry budget or fails permanently."""

    dlq_id: str
    task_id: str
    run_id: str
    workflow_id: str
    queue_name: str
    reason: str
    attempts_made: int
    payload: dict[str, Any] = Field(default_factory=dict)
    requeued: bool = False
    failed_at: datetime
    requeued_at: datetime | None = None


class QueueStats(BaseModel):
    """Summary counts of tasks across queue states and priority levels."""

    queue_name: str
    queued_count: int
    claimed_count: int
    completed_count: int
    dead_lettered_count: int
    queued_by_priority: dict[str, int] = Field(default_factory=dict)
