"""Unified application facade used by both the REST API and CLI."""

from __future__ import annotations

from datetime import datetime

from flowforge.config import Settings, get_settings
from flowforge.domain.enums import (
    AuditCategory,
    RunStatus,
    ScheduleStatus,
    StepRunStatus,
    WorkflowStatus,
)
from flowforge.domain.queue import DeadLetterEntry, EnqueueRequest, QueueStats, QueueTask
from flowforge.domain.run import RunSummary, RunTriggerRequest, WorkflowRun
from flowforge.domain.schedule_audit import (
    AuditEvent,
    MetricsSnapshot,
    ScheduleCreate,
    ScheduleRule,
)
from flowforge.domain.workflow import (
    StepDefinition,
    WorkflowCreate,
    WorkflowDefinition,
    WorkflowUpdate,
)
from flowforge.engine.analytics import TelemetryAnalyzer
from flowforge.engine.dag import TopologicalPlanner
from flowforge.engine.executor import WorkflowExecutor
from flowforge.engine.scheduler import SchedulerEngine, SchedulerTickReport
from flowforge.engine.worker import QueueWorker, WorkerTickReport
from flowforge.exceptions import HandlerNotFoundError, ValidationFailedError
from flowforge.handlers import HandlerRegistry, build_default_registry
from flowforge.storage import (
    AuditRepository,
    DatabaseManager,
    QueueRepository,
    RunRepository,
    ScheduleRepository,
    WorkflowRepository,
)
from flowforge.utils_time import utc_now


class OrchestratorService:
    """High-level coordinator for workflows, runs, queues, schedules, and telemetry."""

    def __init__(
        self,
        settings: Settings | None = None,
        registry: HandlerRegistry | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.settings.ensure_directories()
        self.db = DatabaseManager(self.settings.db_path)
        self.registry = registry or build_default_registry()

        self.workflows = WorkflowRepository(self.db)
        self.runs = RunRepository(self.db)
        self.queue = QueueRepository(self.db)
        self.schedules = ScheduleRepository(self.db)
        self.audit = AuditRepository(self.db)

        self.executor = WorkflowExecutor(
            run_repo=self.runs,
            audit_repo=self.audit,
            registry=self.registry,
            artifact_dir=self.settings.artifact_dir,
        )
        self.worker = QueueWorker(
            workflow_repo=self.workflows,
            queue_repo=self.queue,
            audit_repo=self.audit,
            executor=self.executor,
            max_retry_cap_seconds=self.settings.max_retry_cap_seconds,
        )
        self.scheduler = SchedulerEngine(
            schedule_repo=self.schedules,
            workflow_repo=self.workflows,
            audit_repo=self.audit,
            trigger_callback=self.trigger_run,
        )
        self.telemetry = TelemetryAnalyzer(self.db)

    def _validate_steps_against_registry(self, steps: list[StepDefinition]) -> list[StepDefinition]:
        ordered = TopologicalPlanner.sort_steps(steps)
        for step in ordered:
            if not self.registry.has(step.handler):
                raise HandlerNotFoundError(
                    f"Step '{step.key}' references unknown handler '{step.handler}'"
                )
        return ordered

    def create_workflow(self, payload: WorkflowCreate, *, actor: str = "user") -> WorkflowDefinition:
        self._validate_steps_against_registry(payload.steps)
        created = self.workflows.create(payload)
        self.audit.record(
            AuditCategory.WORKFLOW,
            "workflow.created",
            created.workflow_id,
            actor=actor,
            details={"name": created.name, "step_count": len(created.steps)},
        )
        return created

    def update_workflow(
        self,
        workflow_id_or_name: str,
        patch: WorkflowUpdate,
        *,
        actor: str = "user",
    ) -> WorkflowDefinition:
        if patch.steps is not None:
            self._validate_steps_against_registry(patch.steps)
        updated = self.workflows.update(workflow_id_or_name, patch)
        self.audit.record(
            AuditCategory.WORKFLOW,
            "workflow.updated",
            updated.workflow_id,
            actor=actor,
            details={"version": updated.version, "status": updated.status.value},
        )
        return updated

    def get_workflow(self, workflow_id_or_name: str) -> WorkflowDefinition:
        return self.workflows.get(workflow_id_or_name)

    def list_workflows(self, status: WorkflowStatus | None = None) -> list[WorkflowDefinition]:
        return self.workflows.list_workflows(status=status)

    def list_handlers(self) -> list[dict[str, str]]:
        return self.registry.list_metadata()

    def trigger_run(
        self,
        workflow_id_or_name: str,
        request: RunTriggerRequest | None = None,
    ) -> WorkflowRun:
        req = request or RunTriggerRequest()
        if req.idempotency_key:
            existing = self.runs.find_by_idempotency_key(req.idempotency_key)
            if existing is not None:
                return existing

        workflow = self.workflows.get(workflow_id_or_name)
        if workflow.status != WorkflowStatus.ACTIVE:
            raise ValidationFailedError(
                f"Cannot trigger workflow '{workflow.name}' while status is '{workflow.status.value}'"
            )

        ordered_steps = TopologicalPlanner.sort_steps(workflow.steps)
        effective_priority = req.priority if req.priority is not None else workflow.default_priority
        queue_name = req.queue_name or self.settings.default_queue

        created_run = self.runs.create_run(
            workflow=workflow,
            ordered_steps=ordered_steps,
            inputs=req.inputs,
            priority=effective_priority,
            triggered_by=req.triggered_by,
            idempotency_key=req.idempotency_key,
        )
        max_attempts = 1 + workflow.retry_policy.max_retries
        self.queue.enqueue(
            EnqueueRequest(
                run_id=created_run.run_id,
                workflow_id=workflow.workflow_id,
                queue_name=queue_name,
                priority=effective_priority,
                max_attempts=max_attempts,
                payload={"triggered_by": req.triggered_by},
            )
        )
        self.audit.record(
            AuditCategory.RUN,
            "run.queued",
            workflow.workflow_id,
            run_id=created_run.run_id,
            actor=req.triggered_by,
            details={"priority": int(effective_priority), "queue": queue_name},
        )
        return created_run

    def get_run(self, run_id: str) -> WorkflowRun:
        return self.runs.get_run(run_id)

    def list_runs(
        self,
        workflow_id: str | None = None,
        status: RunStatus | None = None,
        limit: int = 50,
    ) -> list[RunSummary]:
        return self.runs.list_runs(workflow_id=workflow_id, status=status, limit=limit)

    def cancel_run(self, run_id: str, *, actor: str = "user") -> WorkflowRun:
        self.queue.cancel_queued_for_run(run_id)
        cancelled = self.runs.update_run_status(
            run_id,
            RunStatus.CANCELLED,
            finished_at=utc_now(),
        )
        self.audit.record(
            AuditCategory.RUN,
            "run.cancelled",
            cancelled.workflow_id,
            run_id=run_id,
            actor=actor,
        )
        return cancelled

    def run_worker_tick(
        self,
        *,
        queue_name: str | None = None,
        max_tasks: int | None = None,
        as_of: datetime | None = None,
    ) -> WorkerTickReport:
        return self.worker.process_batch(
            queue_name=queue_name or self.settings.default_queue,
            max_tasks=max_tasks or self.settings.worker_batch_size,
            as_of=as_of,
        )

    def get_queue_stats(self, queue_name: str | None = None) -> QueueStats:
        return self.queue.get_stats(queue_name or self.settings.default_queue)

    def list_dead_letters(self, include_requeued: bool = False) -> list[DeadLetterEntry]:
        return self.queue.list_dead_letters(include_requeued=include_requeued)

    def requeue_dead_letter(
        self,
        dlq_id: str,
        *,
        extra_attempts: int = 1,
        actor: str = "user",
    ) -> QueueTask:
        task = self.queue.requeue_dead_letter(dlq_id, extra_attempts=extra_attempts)
        current_run = self.runs.get_run(task.run_id)
        for step in current_run.steps:
            if step.status in {StepRunStatus.FAILED, StepRunStatus.SKIPPED}:
                self.runs.update_step_run(task.run_id, step.step_key, StepRunStatus.PENDING)
        self.runs.update_run_status(task.run_id, RunStatus.QUEUED)
        self.audit.record(
            AuditCategory.DLQ,
            "dlq.requeued",
            dlq_id,
            run_id=task.run_id,
            actor=actor,
            details={"task_id": task.task_id, "extra_attempts": extra_attempts},
        )
        return task

    def create_schedule(self, payload: ScheduleCreate, *, actor: str = "user") -> ScheduleRule:
        wf = self.workflows.get(payload.workflow_id)
        normalized = payload.model_copy(update={"workflow_id": wf.workflow_id})
        created = self.schedules.create(normalized)
        self.audit.record(
            AuditCategory.SCHEDULE,
            "schedule.created",
            created.schedule_id,
            actor=actor,
            details={"name": created.name, "workflow_id": wf.workflow_id},
        )
        return created

    def list_schedules(self, status: ScheduleStatus | None = None) -> list[ScheduleRule]:
        return self.schedules.list_schedules(status=status)

    def set_schedule_status(
        self,
        schedule_id_or_name: str,
        status: ScheduleStatus,
        *,
        actor: str = "user",
    ) -> ScheduleRule:
        updated = self.schedules.set_status(schedule_id_or_name, status)
        self.audit.record(
            AuditCategory.SCHEDULE,
            f"schedule.{status.value}",
            updated.schedule_id,
            actor=actor,
        )
        return updated

    def run_scheduler_tick(self, *, as_of: datetime | None = None) -> SchedulerTickReport:
        return self.scheduler.tick(as_of=as_of)

    def list_audit_events(
        self,
        *,
        category: AuditCategory | None = None,
        entity_id: str | None = None,
        run_id: str | None = None,
        limit: int = 100,
    ) -> list[AuditEvent]:
        return self.audit.list_events(
            category=category,
            entity_id=entity_id,
            run_id=run_id,
            limit=limit,
        )

    def get_metrics(self) -> MetricsSnapshot:
        return self.telemetry.snapshot()
