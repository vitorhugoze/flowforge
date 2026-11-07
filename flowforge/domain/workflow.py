"""Workflow, step, and retry policy domain schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from flowforge.domain.enums import BackoffStrategy, PriorityLevel, WorkflowStatus


class RetryPolicy(BaseModel):
    """Controls retry attempts and delay computation when a workflow or step fails."""

    max_retries: int = Field(default=2, ge=0, le=20)
    base_delay_seconds: float = Field(default=5.0, ge=0.0, le=3600.0)
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL


class StepDefinition(BaseModel):
    """Declarative definition of one pipeline step inside a workflow."""

    key: str = Field(..., min_length=1, max_length=80, pattern=r"^[a-zA-Z0-9_-]+$")
    name: str = Field(..., min_length=1, max_length=120)
    handler: str = Field(..., min_length=1, max_length=80)
    config: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)
    timeout_seconds: float = Field(default=60.0, gt=0.0, le=86400.0)

    @field_validator("depends_on")
    @classmethod
    def _deduplicate_dependencies(cls, deps: list[str]) -> list[str]:
        seen: list[str] = []
        for dep in deps:
            cleaned = dep.strip()
            if not cleaned:
                raise ValueError("Dependency step key cannot be empty")
            if cleaned not in seen:
                seen.append(cleaned)
        return seen


def _validate_step_collection(steps: list[StepDefinition]) -> None:
    if not steps:
        raise ValueError("Workflow must contain at least one step")
    keys = [s.key for s in steps]
    if len(set(keys)) != len(keys):
        raise ValueError("Workflow step keys must be unique")
    known = set(keys)
    for step in steps:
        if step.key in step.depends_on:
            raise ValueError(f"Step '{step.key}' cannot depend on itself")
        missing = [dep for dep in step.depends_on if dep not in known]
        if missing:
            raise ValueError(f"Step '{step.key}' references unknown dependencies: {missing}")


class WorkflowCreate(BaseModel):
    """Input payload for registering a new workflow pipeline."""

    name: str = Field(..., min_length=2, max_length=100, pattern=r"^[a-zA-Z0-9_.-]+$")
    description: str = Field(default="", max_length=500)
    default_priority: PriorityLevel = PriorityLevel.NORMAL
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    tags: list[str] = Field(default_factory=list)
    steps: list[StepDefinition] = Field(..., min_length=1)

    @model_validator(mode="after")
    def _check_steps(self) -> WorkflowCreate:
        _validate_step_collection(self.steps)
        return self


class WorkflowUpdate(BaseModel):
    """Input payload for updating a workflow definition and bumping its version."""

    description: str | None = Field(default=None, max_length=500)
    status: WorkflowStatus | None = None
    default_priority: PriorityLevel | None = None
    retry_policy: RetryPolicy | None = None
    tags: list[str] | None = None
    steps: list[StepDefinition] | None = None

    @model_validator(mode="after")
    def _check_optional_steps(self) -> WorkflowUpdate:
        if self.steps is not None:
            _validate_step_collection(self.steps)
        return self


class WorkflowDefinition(BaseModel):
    """Persisted workflow entity returned by repositories and API endpoints."""

    workflow_id: str
    name: str
    description: str
    version: int
    status: WorkflowStatus
    default_priority: PriorityLevel
    retry_policy: RetryPolicy
    tags: list[str]
    steps: list[StepDefinition]
    created_at: datetime
    updated_at: datetime
