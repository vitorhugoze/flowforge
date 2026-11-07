"""Repository for workflow runs and ordered step execution traces."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime
from typing import Any

from flowforge.domain.enums import PriorityLevel, RunStatus, StepRunStatus
from flowforge.domain.run import RunSummary, StepRun, WorkflowRun
from flowforge.domain.transitions import validate_run_transition, validate_step_transition
from flowforge.domain.workflow import StepDefinition, WorkflowDefinition
from flowforge.exceptions import EntityNotFoundError
from flowforge.storage.database import DatabaseManager
from flowforge.utils_time import from_iso_utc, to_iso_utc, utc_now


class RunRepository:
    """Persistence operations for WorkflowRun and StepRun records."""

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    @staticmethod
    def _step_from_row(row: sqlite3.Row) -> StepRun:
        return StepRun(
            step_run_id=row["step_run_id"],
            run_id=row["run_id"],
            step_key=row["step_key"],
            handler=row["handler"],
            status=StepRunStatus(row["status"]),
            attempt=int(row["attempt"]),
            input_data=json.loads(row["input_json"]),
            output_data=json.loads(row["output_json"]) if row["output_json"] else None,
            error_message=row["error_message"],
            duration_ms=float(row["duration_ms"]) if row["duration_ms"] is not None else None,
            started_at=from_iso_utc(row["started_at"]),
            finished_at=from_iso_utc(row["finished_at"]),
        )

    @staticmethod
    def _summary_from_row(row: sqlite3.Row) -> RunSummary:
        created = from_iso_utc(row["created_at"])
        assert created is not None
        return RunSummary(
            run_id=row["run_id"],
            workflow_id=row["workflow_id"],
            workflow_name=row["workflow_name"],
            workflow_version=int(row["workflow_version"]),
            status=RunStatus(row["status"]),
            priority=PriorityLevel(int(row["priority"])),
            triggered_by=row["triggered_by"],
            attempt_count=int(row["attempt_count"]),
            last_error=row["last_error"],
            created_at=created,
            started_at=from_iso_utc(row["started_at"]),
            finished_at=from_iso_utc(row["finished_at"]),
        )

    def find_by_dedup_ref(self, dedup_ref: str) -> WorkflowRun | None:
        with self._db.session() as conn:
            row = conn.execute(
                "SELECT run_id FROM workflow_runs WHERE dedup_ref = ?",
                (dedup_ref,),
            ).fetchone()
        if row is None:
            return None
        return self.get_run(row["run_id"])

    def create_run(
        self,
        workflow: WorkflowDefinition,
        ordered_steps: list[StepDefinition],
        inputs: dict[str, Any],
        priority: PriorityLevel,
        triggered_by: str,
        dedup_ref: str | None = None,
    ) -> WorkflowRun:
        run_id = str(uuid.uuid4())
        now_str = to_iso_utc(utc_now())
        with self._db.session() as conn:
            conn.execute(
                """
                INSERT INTO workflow_runs (
                    run_id, workflow_id, workflow_name, workflow_version, status,
                    priority, triggered_by, dedup_ref, inputs_json, outputs_json,
                    attempt_count, max_retries, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', 0, ?, ?)
                """,
                (
                    run_id,
                    workflow.workflow_id,
                    workflow.name,
                    workflow.version,
                    RunStatus.QUEUED.value,
                    int(priority),
                    triggered_by,
                    dedup_ref,
                    json.dumps(inputs),
                    workflow.retry_policy.max_retries,
                    now_str,
                ),
            )
            for idx, step in enumerate(ordered_steps):
                conn.execute(
                    """
                    INSERT INTO step_runs (
                        step_run_id, run_id, step_key, step_order, handler,
                        status, attempt, input_json
                    ) VALUES (?, ?, ?, ?, ?, ?, 0, '{}')
                    """,
                    (
                        str(uuid.uuid4()),
                        run_id,
                        step.key,
                        idx,
                        step.handler,
                        StepRunStatus.PENDING.value,
                    ),
                )
        return self.get_run(run_id)

    def get_run(self, run_id: str) -> WorkflowRun:
        with self._db.session() as conn:
            row = conn.execute(
                "SELECT * FROM workflow_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise EntityNotFoundError(f"Workflow run '{run_id}' not found")
            step_rows = conn.execute(
                "SELECT * FROM step_runs WHERE run_id = ? ORDER BY step_order ASC",
                (run_id,),
            ).fetchall()

        created = from_iso_utc(row["created_at"])
        assert created is not None
        return WorkflowRun(
            run_id=row["run_id"],
            workflow_id=row["workflow_id"],
            workflow_name=row["workflow_name"],
            workflow_version=int(row["workflow_version"]),
            status=RunStatus(row["status"]),
            priority=PriorityLevel(int(row["priority"])),
            triggered_by=row["triggered_by"],
            dedup_ref=row["dedup_ref"],
            inputs=json.loads(row["inputs_json"]),
            outputs=json.loads(row["outputs_json"]),
            attempt_count=int(row["attempt_count"]),
            max_retries=int(row["max_retries"]),
            last_error=row["last_error"],
            created_at=created,
            started_at=from_iso_utc(row["started_at"]),
            finished_at=from_iso_utc(row["finished_at"]),
            steps=[self._step_from_row(s) for s in step_rows],
        )

    def list_runs(
        self,
        workflow_id: str | None = None,
        status: RunStatus | None = None,
        limit: int = 50,
    ) -> list[RunSummary]:
        clauses: list[str] = []
        params: list[object] = []
        if workflow_id is not None:
            clauses.append("workflow_id = ?")
            params.append(workflow_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status.value)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        with self._db.session() as conn:
            rows = conn.execute(
                f"SELECT * FROM workflow_runs {where_sql} ORDER BY created_at DESC LIMIT ?",
                tuple(params),
            ).fetchall()
        return [self._summary_from_row(r) for r in rows]

    def update_run_status(
        self,
        run_id: str,
        target_status: RunStatus,
        *,
        attempt_count: int | None = None,
        outputs: dict[str, Any] | None = None,
        last_error: str | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
    ) -> WorkflowRun:
        current_run = self.get_run(run_id)
        validate_run_transition(current_run.status, target_status)

        next_attempts = attempt_count if attempt_count is not None else current_run.attempt_count
        next_outputs = outputs if outputs is not None else current_run.outputs
        next_error = last_error if last_error is not None else current_run.last_error
        if target_status == RunStatus.SUCCEEDED:
            next_error = None
        next_started = started_at if started_at is not None else current_run.started_at
        next_finished = finished_at if finished_at is not None else current_run.finished_at

        with self._db.session() as conn:
            conn.execute(
                """
                UPDATE workflow_runs
                SET status = ?, attempt_count = ?, outputs_json = ?, last_error = ?,
                    started_at = ?, finished_at = ?
                WHERE run_id = ?
                """,
                (
                    target_status.value,
                    next_attempts,
                    json.dumps(next_outputs),
                    next_error,
                    to_iso_utc(next_started),
                    to_iso_utc(next_finished),
                    run_id,
                ),
            )
        return self.get_run(run_id)

    def update_step_run(
        self,
        run_id: str,
        step_key: str,
        target_status: StepRunStatus,
        *,
        attempt: int | None = None,
        input_data: dict[str, Any] | None = None,
        output_data: dict[str, Any] | None = None,
        error_message: str | None = None,
        duration_ms: float | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
    ) -> StepRun:
        run = self.get_run(run_id)
        matching = [s for s in run.steps if s.step_key == step_key]
        if not matching:
            raise EntityNotFoundError(f"Step '{step_key}' not found in run '{run_id}'")
        current = matching[0]
        validate_step_transition(current.status, target_status)

        next_attempt = attempt if attempt is not None else current.attempt
        next_input = input_data if input_data is not None else current.input_data
        next_output = output_data if output_data is not None else current.output_data
        next_error = error_message if target_status == StepRunStatus.FAILED else None
        next_duration = duration_ms if duration_ms is not None else current.duration_ms
        next_started = started_at if started_at is not None else current.started_at
        next_finished = finished_at if finished_at is not None else current.finished_at

        with self._db.session() as conn:
            conn.execute(
                """
                UPDATE step_runs
                SET status = ?, attempt = ?, input_json = ?, output_json = ?,
                    error_message = ?, duration_ms = ?, started_at = ?, finished_at = ?
                WHERE run_id = ? AND step_key = ?
                """,
                (
                    target_status.value,
                    next_attempt,
                    json.dumps(next_input),
                    json.dumps(next_output) if next_output is not None else None,
                    next_error,
                    next_duration,
                    to_iso_utc(next_started),
                    to_iso_utc(next_finished),
                    run_id,
                    step_key,
                ),
            )
            row = conn.execute(
                "SELECT * FROM step_runs WHERE run_id = ? AND step_key = ?",
                (run_id, step_key),
            ).fetchone()
        assert row is not None
        return self._step_from_row(row)
