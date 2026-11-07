"""Integration tests for DAG workflow execution, transient retries, and DLQ recovery."""

from __future__ import annotations

from datetime import timedelta

from flowforge.domain.enums import BackoffStrategy, RunStatus, StepRunStatus
from flowforge.domain.run import RunTriggerRequest
from flowforge.domain.workflow import RetryPolicy, StepDefinition, WorkflowCreate
from flowforge.services.orchestrator import OrchestratorService
from flowforge.utils_time import utc_now


def test_multi_step_pipeline_executes_and_persists_outputs(
    orchestrator: OrchestratorService,
) -> None:
    wf = orchestrator.create_workflow(
        WorkflowCreate(
            name="order-analytics",
            steps=[
                StepDefinition(
                    key="validate",
                    name="Validate Orders",
                    handler="validate.records",
                    config={"required_fields": ["order_id", "amount"], "numeric_min": {"amount": 0}},
                ),
                StepDefinition(
                    key="filter_paid",
                    name="Filter High Value",
                    handler="filter.predicate",
                    depends_on=["validate"],
                    config={"field": "amount", "operator": "gte", "value": 50},
                ),
                StepDefinition(
                    key="summarize",
                    name="Summarize Revenue",
                    handler="aggregate.numeric_summary",
                    depends_on=["filter_paid"],
                    config={"field": "amount"},
                ),
            ],
        )
    )

    run = orchestrator.trigger_run(
        wf.name,
        RunTriggerRequest(
            inputs={
                "records": [
                    {"order_id": "o1", "amount": 25},
                    {"order_id": "o2", "amount": 100},
                    {"order_id": "o3", "amount": 150},
                ]
            }
        ),
    )
    report = orchestrator.run_worker_tick()
    assert report.succeeded == 1

    completed = orchestrator.get_run(run.run_id)
    assert completed.status == RunStatus.SUCCEEDED
    assert completed.outputs["summarize"]["total"] == 250.0
    assert completed.outputs["summarize"]["count"] == 2


def test_transient_failure_retries_without_rerunning_completed_upstream_steps(
    orchestrator: OrchestratorService,
) -> None:
    wf = orchestrator.create_workflow(
        WorkflowCreate(
            name="flaky-pipeline",
            retry_policy=RetryPolicy(
                max_retries=2,
                base_delay_seconds=10.0,
                strategy=BackoffStrategy.FIXED,
            ),
            steps=[
                StepDefinition(
                    key="prepare",
                    name="Prepare Data",
                    handler="control.passthrough",
                    config={"static": {"prepared": True}},
                ),
                StepDefinition(
                    key="flaky_gate",
                    name="Succeeds on Attempt 2",
                    handler="control.assert_gate",
                    depends_on=["prepare"],
                    config={"succeed_on_attempt": 2, "retryable": True},
                ),
            ],
        )
    )

    run = orchestrator.trigger_run(wf.name)
    t0 = utc_now()

    tick1 = orchestrator.run_worker_tick(as_of=t0)
    assert tick1.rescheduled == 1
    mid_run = orchestrator.get_run(run.run_id)
    assert mid_run.status == RunStatus.RETRYING
    step_map = {s.step_key: s for s in mid_run.steps}
    assert step_map["prepare"].status == StepRunStatus.SUCCEEDED
    assert step_map["prepare"].attempt == 1
    assert step_map["flaky_gate"].status == StepRunStatus.FAILED

    # Polling before backoff expires claims nothing
    early_tick = orchestrator.run_worker_tick(as_of=t0 + timedelta(seconds=3))
    assert early_tick.claimed == 0

    # Polling after backoff succeeds and keeps prepare at attempt 1
    tick2 = orchestrator.run_worker_tick(as_of=t0 + timedelta(seconds=15))
    assert tick2.succeeded == 1
    final_run = orchestrator.get_run(run.run_id)
    assert final_run.status == RunStatus.SUCCEEDED
    final_steps = {s.step_key: s for s in final_run.steps}
    assert final_steps["prepare"].attempt == 1
    assert final_steps["flaky_gate"].attempt == 2


def test_exhausted_retries_move_to_dlq_and_can_be_requeued(
    orchestrator: OrchestratorService,
) -> None:
    wf = orchestrator.create_workflow(
        WorkflowCreate(
            name="dlq-recovery-flow",
            retry_policy=RetryPolicy(max_retries=0, base_delay_seconds=0.0),
            steps=[
                StepDefinition(
                    key="gate",
                    name="Needs Two Attempts",
                    handler="control.assert_gate",
                    config={"succeed_on_attempt": 2, "retryable": True},
                ),
                StepDefinition(
                    key="downstream",
                    name="Downstream Step",
                    handler="control.passthrough",
                    depends_on=["gate"],
                ),
            ],
        )
    )

    run = orchestrator.trigger_run(wf.name)
    tick1 = orchestrator.run_worker_tick()
    assert tick1.dead_lettered == 1

    failed_run = orchestrator.get_run(run.run_id)
    assert failed_run.status == RunStatus.FAILED
    steps = {s.step_key: s for s in failed_run.steps}
    assert steps["downstream"].status == StepRunStatus.SKIPPED

    dlq_items = orchestrator.list_dead_letters()
    assert len(dlq_items) == 1
    orchestrator.requeue_dead_letter(dlq_items[0].dlq_id, extra_attempts=1)

    tick2 = orchestrator.run_worker_tick()
    assert tick2.succeeded == 1
    recovered_run = orchestrator.get_run(run.run_id)
    assert recovered_run.status == RunStatus.SUCCEEDED
