"""Finite-state-machine transition guards for runs, steps, and queue items."""

from __future__ import annotations

from flowforge.domain.enums import QueueItemStatus, RunStatus, StepRunStatus
from flowforge.exceptions import InvalidStateTransitionError

ALLOWED_RUN_TRANSITIONS: dict[RunStatus, set[RunStatus]] = {
    RunStatus.PENDING: {RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.CANCELLED},
    RunStatus.QUEUED: {RunStatus.RUNNING, RunStatus.CANCELLED},
    RunStatus.RUNNING: {
        RunStatus.SUCCEEDED,
        RunStatus.RETRYING,
        RunStatus.FAILED,
        RunStatus.CANCELLED,
    },
    RunStatus.RETRYING: {RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.FAILED, RunStatus.CANCELLED},
    RunStatus.FAILED: {RunStatus.QUEUED},  # Allowed when requeued from DLQ
    RunStatus.SUCCEEDED: set(),
    RunStatus.CANCELLED: set(),
}

ALLOWED_STEP_TRANSITIONS: dict[StepRunStatus, set[StepRunStatus]] = {
    StepRunStatus.PENDING: {StepRunStatus.RUNNING, StepRunStatus.SKIPPED},
    StepRunStatus.RUNNING: {StepRunStatus.SUCCEEDED, StepRunStatus.FAILED},
    StepRunStatus.FAILED: {StepRunStatus.PENDING, StepRunStatus.RUNNING},
    StepRunStatus.SUCCEEDED: set(),
    StepRunStatus.SKIPPED: {StepRunStatus.PENDING},
}

ALLOWED_QUEUE_TRANSITIONS: dict[QueueItemStatus, set[QueueItemStatus]] = {
    QueueItemStatus.QUEUED: {QueueItemStatus.CLAIMED, QueueItemStatus.CANCELLED},
    QueueItemStatus.CLAIMED: {
        QueueItemStatus.COMPLETED,
        QueueItemStatus.QUEUED,
        QueueItemStatus.DEAD_LETTERED,
        QueueItemStatus.CANCELLED,
    },
    QueueItemStatus.DEAD_LETTERED: {QueueItemStatus.QUEUED},
    QueueItemStatus.COMPLETED: set(),
    QueueItemStatus.CANCELLED: set(),
}


def validate_run_transition(current: RunStatus, target: RunStatus) -> None:
    """Ensure a workflow run state change is permitted."""
    if current == target:
        return
    if target not in ALLOWED_RUN_TRANSITIONS.get(current, set()):
        raise InvalidStateTransitionError(
            f"Illegal workflow run transition from '{current.value}' to '{target.value}'"
        )


def validate_step_transition(current: StepRunStatus, target: StepRunStatus) -> None:
    """Ensure a step run state change is permitted."""
    if current == target:
        return
    if target not in ALLOWED_STEP_TRANSITIONS.get(current, set()):
        raise InvalidStateTransitionError(
            f"Illegal step run transition from '{current.value}' to '{target.value}'"
        )


def validate_queue_transition(current: QueueItemStatus, target: QueueItemStatus) -> None:
    """Ensure a queue task state change is permitted."""
    if current == target:
        return
    if target not in ALLOWED_QUEUE_TRANSITIONS.get(current, set()):
        raise InvalidStateTransitionError(
            f"Illegal queue task transition from '{current.value}' to '{target.value}'"
        )
