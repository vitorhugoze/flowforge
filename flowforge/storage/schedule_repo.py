"""Repository for one-shot and recurring workflow schedules."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime

from flowforge.domain.enums import PriorityLevel, ScheduleStatus
from flowforge.domain.schedule_audit import ScheduleCreate, ScheduleRule
from flowforge.exceptions import DuplicateEntityError, EntityNotFoundError, ScheduleError
from flowforge.storage.database import DatabaseManager
from flowforge.utils_time import from_iso_utc, to_iso_utc, utc_now


class ScheduleRepository:
    """Persistence for scheduled workflow triggers."""

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    @staticmethod
    def _row_to_model(row: sqlite3.Row) -> ScheduleRule:
        next_run = from_iso_utc(row["next_run_at"])
        created = from_iso_utc(row["created_at"])
        updated = from_iso_utc(row["updated_at"])
        assert next_run is not None and created is not None and updated is not None
        return ScheduleRule(
            schedule_id=row["schedule_id"],
            name=row["name"],
            workflow_id=row["workflow_id"],
            status=ScheduleStatus(row["status"]),
            next_run_at=next_run,
            interval_seconds=int(row["interval_seconds"])
            if row["interval_seconds"] is not None
            else None,
            inputs=json.loads(row["inputs_json"]),
            priority=PriorityLevel(int(row["priority"])),
            run_count=int(row["run_count"]),
            max_runs=int(row["max_runs"]) if row["max_runs"] is not None else None,
            last_run_at=from_iso_utc(row["last_run_at"]),
            last_run_id=row["last_run_id"],
            created_at=created,
            updated_at=updated,
        )

    def create(self, payload: ScheduleCreate) -> ScheduleRule:
        schedule_id = str(uuid.uuid4())
        now_str = to_iso_utc(utc_now())
        next_str = to_iso_utc(payload.next_run_at)
        try:
            with self._db.session() as conn:
                conn.execute(
                    """
                    INSERT INTO schedules (
                        schedule_id, name, workflow_id, status, next_run_at,
                        interval_seconds, inputs_json, priority, run_count,
                        max_runs, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                    """,
                    (
                        schedule_id,
                        payload.name,
                        payload.workflow_id,
                        ScheduleStatus.ACTIVE.value,
                        next_str,
                        payload.interval_seconds,
                        json.dumps(payload.inputs),
                        int(payload.priority),
                        payload.max_runs,
                        now_str,
                        now_str,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise DuplicateEntityError(f"Schedule named '{payload.name}' already exists") from exc

        return self.get(schedule_id)

    def get(self, schedule_id: str) -> ScheduleRule:
        with self._db.session() as conn:
            row = conn.execute(
                "SELECT * FROM schedules WHERE schedule_id = ? OR name = ?",
                (schedule_id, schedule_id),
            ).fetchone()
        if row is None:
            raise EntityNotFoundError(f"Schedule '{schedule_id}' not found")
        return self._row_to_model(row)

    def list_schedules(self, status: ScheduleStatus | None = None) -> list[ScheduleRule]:
        sql = "SELECT * FROM schedules"
        params: tuple[object, ...] = ()
        if status is not None:
            sql += " WHERE status = ?"
            params = (status.value,)
        sql += " ORDER BY next_run_at ASC"
        with self._db.session() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_model(r) for r in rows]

    def list_due(self, as_of: datetime) -> list[ScheduleRule]:
        cutoff = to_iso_utc(as_of)
        with self._db.session() as conn:
            rows = conn.execute(
                """
                SELECT * FROM schedules
                WHERE status = ? AND next_run_at <= ?
                ORDER BY next_run_at ASC
                """,
                (ScheduleStatus.ACTIVE.value, cutoff),
            ).fetchall()
        return [self._row_to_model(r) for r in rows]

    def record_execution(
        self,
        schedule_id: str,
        *,
        run_id: str,
        executed_at: datetime,
        next_run_at: datetime | None,
        completed: bool,
    ) -> ScheduleRule:
        sched = self.get(schedule_id)
        new_count = sched.run_count + 1
        new_status = ScheduleStatus.COMPLETED if completed else ScheduleStatus.ACTIVE
        effective_next = next_run_at if next_run_at is not None else sched.next_run_at
        now_str = to_iso_utc(utc_now())

        with self._db.session() as conn:
            conn.execute(
                """
                UPDATE schedules
                SET status = ?, run_count = ?, next_run_at = ?,
                    last_run_at = ?, last_run_id = ?, updated_at = ?
                WHERE schedule_id = ?
                """,
                (
                    new_status.value,
                    new_count,
                    to_iso_utc(effective_next),
                    to_iso_utc(executed_at),
                    run_id,
                    now_str,
                    sched.schedule_id,
                ),
            )
        return self.get(sched.schedule_id)

    def set_status(self, schedule_id: str, target: ScheduleStatus) -> ScheduleRule:
        sched = self.get(schedule_id)
        if sched.status in {ScheduleStatus.COMPLETED, ScheduleStatus.CANCELLED}:
            raise ScheduleError(
                f"Cannot change schedule '{sched.name}' once in terminal status '{sched.status.value}'"
            )
        now_str = to_iso_utc(utc_now())
        with self._db.session() as conn:
            conn.execute(
                "UPDATE schedules SET status = ?, updated_at = ? WHERE schedule_id = ?",
                (target.value, now_str, sched.schedule_id),
            )
        return self.get(sched.schedule_id)
