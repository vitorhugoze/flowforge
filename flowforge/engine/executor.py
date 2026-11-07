"""Step-by-step DAG workflow execution engine with state persistence and audit logging."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flowforge.domain.enums import AuditCategory, RunStatus, StepRunStatus
from flowforge.domain.run import WorkflowRun
from flowforge.domain.workflow import WorkflowDefinition
from flowforge.engine.dag import TopologicalPlanner
from flowforge.exceptions import StepExecutionError
from flowforge.handlers.base import HandlerRegistry, StepContext
from flowforge.storage.audit_repo import AuditRepository
from flowforge.storage.run_repo import RunRepository
from flowforge.utils_time import utc_now


@dataclass(frozen=True)
class ExecutionOutcome:
    """Result returned by WorkflowExecutor after running or attempting a workflow."""

    run: WorkflowRun
    succeeded: bool
    retryable: bool = False
    error_message: str | None = None


class WorkflowExecutor:
    """Executes ordered workflow steps, reusing completed steps across retry attempts."""

    def __init__(
        self,
        run_repo: RunRepository,
        audit_repo: AuditRepository,
        registry: HandlerRegistry,
        artifact_dir: Path,
    ) -> None:
        self._runs = run_repo
        self._audit = audit_repo
        self._registry = registry
        self._artifact_dir = artifact_dir

    def execute_run(
        self,
        workflow: WorkflowDefinition,
        run_id: str,
        *,
        attempt_number: int,
        allow_retry_on_failure: bool,
    ) -> ExecutionOutcome:
        current_run = self._runs.get_run(run_id)
        started_ts = current_run.started_at or utc_now()
        self._runs.update_run_status(
            run_id,
            RunStatus.RUNNING,
            attempt_count=attempt_number,
            started_at=started_ts,
        )
        self._audit.record(
            AuditCategory.RUN,
            "run.started",
            workflow.workflow_id,
            run_id=run_id,
            details={"attempt": attempt_number},
        )

        ordered_defs = TopologicalPlanner.sort_steps(workflow.steps)
        existing_steps = {s.step_key: s for s in self._runs.get_run(run_id).steps}
        completed_outputs: dict[str, dict[str, Any]] = {}

        for step_def in ordered_defs:
            prev_step = existing_steps.get(step_def.key)
            if prev_step and prev_step.status == StepRunStatus.SUCCEEDED:
                completed_outputs[step_def.key] = dict(prev_step.output_data or {})
                continue

            upstream_subset = {
                dep_key: completed_outputs[dep_key]
                for dep_key in step_def.depends_on
                if dep_key in completed_outputs
            }
            step_attempt = (prev_step.attempt + 1) if prev_step else 1
            step_started = utc_now()

            self._runs.update_step_run(
                run_id,
                step_def.key,
                StepRunStatus.RUNNING,
                attempt=step_attempt,
                input_data={"upstream_keys": list(upstream_subset.keys()), "config": step_def.config},
                started_at=step_started,
            )

            ctx = StepContext(
                run_id=run_id,
                workflow_id=workflow.workflow_id,
                step_key=step_def.key,
                attempt=step_attempt,
                run_inputs=dict(current_run.inputs),
                upstream_outputs=upstream_subset,
                config=dict(step_def.config),
                artifact_dir=self._artifact_dir,
            )

            t0 = time.perf_counter()
            try:
                handler = self._registry.get(step_def.handler)
                output = handler.execute(ctx)
                elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)

                self._runs.update_step_run(
                    run_id,
                    step_def.key,
                    StepRunStatus.SUCCEEDED,
                    output_data=output,
                    duration_ms=elapsed_ms,
                    finished_at=utc_now(),
                )
                completed_outputs[step_def.key] = output
                self._audit.record(
                    AuditCategory.STEP,
                    "step.succeeded",
                    step_def.key,
                    run_id=run_id,
                    details={"handler": step_def.handler, "duration_ms": elapsed_ms},
                )
            except Exception as exc:
                elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
                is_retryable = exc.retryable if isinstance(exc, StepExecutionError) else True
                err_text = str(exc)

                self._runs.update_step_run(
                    run_id,
                    step_def.key,
                    StepRunStatus.FAILED,
                    error_message=err_text,
                    duration_ms=elapsed_ms,
                    finished_at=utc_now(),
                )
                self._audit.record(
                    AuditCategory.STEP,
                    "step.failed",
                    step_def.key,
                    run_id=run_id,
                    details={
                        "handler": step_def.handler,
                        "error": err_text,
                        "retryable": is_retryable,
                    },
                )

                will_retry = is_retryable and allow_retry_on_failure
                if will_retry:
                    updated_run = self._runs.update_run_status(
                        run_id,
                        RunStatus.RETRYING,
                        last_error=err_text,
                    )
                else:
                    self._skip_remaining_steps(run_id)
                    updated_run = self._runs.update_run_status(
                        run_id,
                        RunStatus.FAILED,
                        last_error=err_text,
                        finished_at=utc_now(),
                    )
                    self._audit.record(
                        AuditCategory.RUN,
                        "run.failed",
                        workflow.workflow_id,
                        run_id=run_id,
                        details={"error": err_text, "attempt": attempt_number},
                    )

                return ExecutionOutcome(
                    run=updated_run,
                    succeeded=False,
                    retryable=is_retryable,
                    error_message=err_text,
                )

        final_run = self._runs.update_run_status(
            run_id,
            RunStatus.SUCCEEDED,
            outputs=completed_outputs,
            finished_at=utc_now(),
        )
        self._audit.record(
            AuditCategory.RUN,
            "run.succeeded",
            workflow.workflow_id,
            run_id=run_id,
            details={"steps_completed": len(ordered_defs), "attempt": attempt_number},
        )
        return ExecutionOutcome(run=final_run, succeeded=True)

    def _skip_remaining_steps(self, run_id: str) -> None:
        snapshot = self._runs.get_run(run_id)
        for step in snapshot.steps:
            if step.status == StepRunStatus.PENDING:
                self._runs.update_step_run(run_id, step.step_key, StepRunStatus.SKIPPED)
