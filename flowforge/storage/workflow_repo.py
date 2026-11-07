"""Repository for workflow pipeline definitions and versioning."""

from __future__ import annotations

import json
import sqlite3
import uuid

from flowforge.domain.enums import PriorityLevel, WorkflowStatus
from flowforge.domain.workflow import (
    RetryPolicy,
    StepDefinition,
    WorkflowCreate,
    WorkflowDefinition,
    WorkflowUpdate,
)
from flowforge.exceptions import DuplicateEntityError, EntityNotFoundError
from flowforge.storage.database import DatabaseManager
from flowforge.utils_time import from_iso_utc, to_iso_utc, utc_now


class WorkflowRepository:
    """CRUD persistence operations for workflow definitions."""

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    @staticmethod
    def _row_to_model(row: sqlite3.Row) -> WorkflowDefinition:
        created = from_iso_utc(row["created_at"])
        updated = from_iso_utc(row["updated_at"])
        assert created is not None and updated is not None
        return WorkflowDefinition(
            workflow_id=row["workflow_id"],
            name=row["name"],
            description=row["description"],
            version=int(row["version"]),
            status=WorkflowStatus(row["status"]),
            default_priority=PriorityLevel(int(row["default_priority"])),
            retry_policy=RetryPolicy.model_validate(json.loads(row["retry_policy_json"])),
            tags=list(json.loads(row["tags_json"])),
            steps=[StepDefinition.model_validate(s) for s in json.loads(row["steps_json"])],
            created_at=created,
            updated_at=updated,
        )

    def create(self, payload: WorkflowCreate) -> WorkflowDefinition:
        workflow_id = str(uuid.uuid4())
        now_str = to_iso_utc(utc_now())
        assert now_str is not None
        try:
            with self._db.session() as conn:
                conn.execute(
                    """
                    INSERT INTO workflows (
                        workflow_id, name, description, version, status,
                        default_priority, retry_policy_json, tags_json, steps_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        workflow_id,
                        payload.name,
                        payload.description,
                        WorkflowStatus.ACTIVE.value,
                        int(payload.default_priority),
                        json.dumps(payload.retry_policy.model_dump(mode="json")),
                        json.dumps(payload.tags),
                        json.dumps([s.model_dump(mode="json") for s in payload.steps]),
                        now_str,
                        now_str,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise DuplicateEntityError(f"Workflow named '{payload.name}' already exists") from exc

        return self.get(workflow_id)

    def get(self, workflow_id_or_name: str) -> WorkflowDefinition:
        with self._db.session() as conn:
            row = conn.execute(
                "SELECT * FROM workflows WHERE workflow_id = ? OR name = ?",
                (workflow_id_or_name, workflow_id_or_name),
            ).fetchone()
        if row is None:
            raise EntityNotFoundError(f"Workflow '{workflow_id_or_name}' not found")
        return self._row_to_model(row)

    def list_workflows(self, status: WorkflowStatus | None = None) -> list[WorkflowDefinition]:
        query = "SELECT * FROM workflows"
        params: tuple[object, ...] = ()
        if status is not None:
            query += " WHERE status = ?"
            params = (status.value,)
        query += " ORDER BY created_at DESC"
        with self._db.session() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_model(r) for r in rows]

    def update(self, workflow_id: str, patch: WorkflowUpdate) -> WorkflowDefinition:
        existing = self.get(workflow_id)
        new_desc = patch.description if patch.description is not None else existing.description
        new_status = patch.status if patch.status is not None else existing.status
        new_priority = (
            patch.default_priority if patch.default_priority is not None else existing.default_priority
        )
        new_retry = patch.retry_policy if patch.retry_policy is not None else existing.retry_policy
        new_tags = patch.tags if patch.tags is not None else existing.tags
        new_steps = patch.steps if patch.steps is not None else existing.steps
        bump_version = patch.steps is not None or patch.retry_policy is not None
        next_version = existing.version + 1 if bump_version else existing.version
        now_str = to_iso_utc(utc_now())

        with self._db.session() as conn:
            conn.execute(
                """
                UPDATE workflows
                SET description = ?, version = ?, status = ?, default_priority = ?,
                    retry_policy_json = ?, tags_json = ?, steps_json = ?, updated_at = ?
                WHERE workflow_id = ?
                """,
                (
                    new_desc,
                    next_version,
                    new_status.value,
                    int(new_priority),
                    json.dumps(new_retry.model_dump(mode="json")),
                    json.dumps(new_tags),
                    json.dumps([s.model_dump(mode="json") for s in new_steps]),
                    now_str,
                    existing.workflow_id,
                ),
            )
        return self.get(existing.workflow_id)
