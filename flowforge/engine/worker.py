"""Priority queue worker loop handling task claims, backoff retries, and DLQ routing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from flowforge.domain.enums import AuditCategory
from flowforge.engine.backoff import can_retry, compute_backoff_seconds
from flowforge.engine.executor import WorkflowExecutor
from flowforge.storage.audit_repo import AuditRepository
from flowforge.storage.queue_repo import QueueRepository
from flowforge.storage.workflow_repo import WorkflowRepository
from flowforge.utils_time import utc_now


@dataclass(frozen=True)
class WorkerTickReport:
    """Summary of tasks processed during a worker poll/drain cycle."""

    claimed: int
    succeeded: int
    rescheduled: int
    dead_lettered: int
    processed_run_ids: list[str]


class QueueWorker:
    """Claims queued workflow runs in priority order and drives WorkflowExecutor."""

    def __init__(
        self,
        workflow_repo: WorkflowRepository,
        queue_repo: QueueRepository,
        audit_repo: AuditRepository,
        executor: WorkflowExecutor,
        *,
        worker_id: str = "worker-main",
        max_retry_cap_seconds: float = 3600.0,
    ) -> None:
        self._workflows = workflow_repo
        self._queue = queue_repo
        self._audit = audit_repo
        self._executor = executor
        self._worker_id = worker_id
        self._max_retry_cap = max_retry_cap_seconds

    def process_batch(
        self,
        *,
        queue_name: str = "default",
        max_tasks: int = 10,
        as_of: datetime | None = None,
    ) -> WorkerTickReport:
        claimed = 0
        succeeded = 0
        rescheduled = 0
        dead_lettered = 0
        run_ids: list[str] = []

        for _ in range(max(1, max_tasks)):
            effective_clock = as_of or utc_now()
            task = self._queue.claim_next(queue_name, self._worker_id, as_of=effective_clock)
            if task is None:
                break

            claimed += 1
            run_ids.append(task.run_id)
            workflow = self._workflows.get(task.workflow_id)
            has_budget_remaining = task.attempt < task.max_attempts

            outcome = self._executor.execute_run(
                workflow,
                task.run_id,
                attempt_number=task.attempt,
                allow_retry_on_failure=has_budget_remaining,
            )

            if outcome.succeeded:
                self._queue.complete_task(task.task_id)
                succeeded += 1
                continue

            err_reason = outcome.error_message or "Step execution failed"
            if can_retry(task.attempt, task.max_attempts, retryable_error=outcome.retryable):
                delay = compute_backoff_seconds(
                    workflow.retry_policy,
                    task.attempt,
                    max_cap_seconds=self._max_retry_cap,
                )
                next_available = effective_clock + timedelta(seconds=delay)
                self._queue.reschedule_task(task.task_id, next_available, err_reason)
                self._audit.record(
                    AuditCategory.QUEUE,
                    "task.rescheduled",
                    task.task_id,
                    run_id=task.run_id,
                    details={
                        "attempt": task.attempt,
                        "delay_seconds": delay,
                        "next_available_at": next_available.isoformat(),
                    },
                )
                rescheduled += 1
            else:
                dlq_entry = self._queue.move_to_dead_letter(task.task_id, err_reason)
                self._audit.record(
                    AuditCategory.DLQ,
                    "task.dead_lettered",
                    dlq_entry.dlq_id,
                    run_id=task.run_id,
                    details={"task_id": task.task_id, "reason": err_reason},
                )
                dead_lettered += 1

        return WorkerTickReport(
            claimed=claimed,
            succeeded=succeeded,
            rescheduled=rescheduled,
            dead_lettered=dead_lettered,
            processed_run_ids=run_ids,
        )
