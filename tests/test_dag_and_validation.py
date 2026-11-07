"""Unit tests for DAG topological planning, state machine guards, and backoff calculation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from flowforge.domain.enums import BackoffStrategy, RunStatus
from flowforge.domain.transitions import validate_run_transition
from flowforge.domain.workflow import RetryPolicy, StepDefinition, WorkflowCreate
from flowforge.engine.backoff import can_retry, compute_backoff_seconds
from flowforge.engine.dag import TopologicalPlanner
from flowforge.exceptions import InvalidStateTransitionError, ValidationFailedError


def test_topological_sort_diamond_dag() -> None:
    steps = [
        StepDefinition(key="ingest", name="Ingest", handler="control.passthrough"),
        StepDefinition(
            key="branch_a",
            name="Branch A",
            handler="control.passthrough",
            depends_on=["ingest"],
        ),
        StepDefinition(
            key="branch_b",
            name="Branch B",
            handler="control.passthrough",
            depends_on=["ingest"],
        ),
        StepDefinition(
            key="join",
            name="Join",
            handler="control.merge_upstream",
            depends_on=["branch_a", "branch_b"],
        ),
    ]
    ordered = TopologicalPlanner.sort_steps(steps)
    assert [s.key for s in ordered] == ["ingest", "branch_a", "branch_b", "join"]
    waves = TopologicalPlanner.execution_waves(steps)
    assert waves == [["ingest"], ["branch_a", "branch_b"], ["join"]]


def test_cycle_detection_raises_validation_error() -> None:
    steps = [
        StepDefinition(key="a", name="A", handler="control.passthrough", depends_on=["c"]),
        StepDefinition(key="b", name="B", handler="control.passthrough", depends_on=["a"]),
        StepDefinition(key="c", name="C", handler="control.passthrough", depends_on=["b"]),
    ]
    with pytest.raises(ValidationFailedError, match="Cycle detected"):
        TopologicalPlanner.sort_steps(steps)


def test_workflow_create_rejects_unknown_dependency() -> None:
    with pytest.raises(ValidationError, match="unknown dependencies"):
        WorkflowCreate(
            name="invalid-deps",
            steps=[
                StepDefinition(
                    key="step1",
                    name="Step 1",
                    handler="control.passthrough",
                    depends_on=["missing_step"],
                )
            ],
        )


def test_backoff_strategies_and_cap() -> None:
    exp = RetryPolicy(max_retries=4, base_delay_seconds=10.0, strategy=BackoffStrategy.EXPONENTIAL)
    assert compute_backoff_seconds(exp, 1) == 10.0
    assert compute_backoff_seconds(exp, 2) == 20.0
    assert compute_backoff_seconds(exp, 3) == 40.0
    assert compute_backoff_seconds(exp, 10, max_cap_seconds=100.0) == 100.0

    lin = RetryPolicy(max_retries=3, base_delay_seconds=5.0, strategy=BackoffStrategy.LINEAR)
    assert compute_backoff_seconds(lin, 3) == 15.0
    assert can_retry(1, 3, retryable_error=True) is True
    assert can_retry(3, 3, retryable_error=True) is False
    assert can_retry(1, 3, retryable_error=False) is False


def test_run_state_transition_guards() -> None:
    validate_run_transition(RunStatus.QUEUED, RunStatus.RUNNING)
    validate_run_transition(RunStatus.FAILED, RunStatus.QUEUED)
    with pytest.raises(InvalidStateTransitionError):
        validate_run_transition(RunStatus.SUCCEEDED, RunStatus.RUNNING)
