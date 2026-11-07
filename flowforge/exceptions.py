"""Domain-level exception hierarchy for FlowForge."""

from __future__ import annotations


class FlowForgeError(Exception):
    """Base exception for all FlowForge errors."""


class EntityNotFoundError(FlowForgeError):
    """Raised when a workflow, run, queue item, or schedule does not exist."""


class DuplicateEntityError(FlowForgeError):
    """Raised when attempting to create an entity that violates uniqueness rules."""


class ValidationFailedError(FlowForgeError):
    """Raised when workflow definitions, inputs, or step configurations are invalid."""


class InvalidStateTransitionError(FlowForgeError):
    """Raised when a run, step, or queue item transitions between incompatible states."""


class HandlerNotFoundError(FlowForgeError):
    """Raised when a workflow step references an unregistered step handler."""


class StepExecutionError(FlowForgeError):
    """Raised by a step handler when execution fails."""

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable


class ScheduleError(FlowForgeError):
    """Raised when schedule configuration or state transitions are invalid."""
