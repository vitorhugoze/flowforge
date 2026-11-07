"""Priority task queue and dead-letter queue persistence."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime

from flowforge.domain.enums import PriorityLevel, QueueItemStatus
from flowforge.domain.queue import DeadLetterEntry, EnqueueRequest, QueueStats, QueueTask
from flowforge.domain.transitions import validate_queue_transition
from flowforge.exceptions import EntityNotFoundError, InvalidStateTransitionError
from flowforge.storage.database import DatabaseManager
from flowforge.utils_time import from_iso_utc, to_iso_utc, utc_now


class QueueRepository:
    """Manages priority task claiming, retry rescheduling, and dead-letter routing."""

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> QueueTask:
        avail = from_iso_utc(row["available_at"])
        created = from_iso_utc(row["created_at"])
        updated = from_iso_utc(row["updated_at"])
        assert avail is not None and created is not None and updated is not None
        return QueueTask(
            task_id=row["task_id"],
            run_id=row["run_id"],
            workflow_id=row["workflow_id"],
            queue_name=row["queue_name"],
            priority=int(row["priority"]),
            status=QueueItemStatus(row["status"]),
            attempt=int(row["attempt"]),
            max_attempts=int(row["max_attempts"]),
            available_at=avail,
            claimed_by=row["claimed_by"],
            claimed_at=from_iso_utc(row["claimed_at"]),
            last_error=row["last_error"],
            payload=json.loads(row["payload_json"]),
            created_at=created,
            updated_at=updated,
        )

    @staticmethod
    def _row_to_dlq(row: sqlite3.Row) -> DeadLetterEntry:
        failed_at = from_iso_utc(row["failed_at"])
        assert failed_at is not None
        return DeadLetterEntry(
            dlq_id=row["dlq_id"],
            task_id=row["task_id"],
            run_id=row["run_id"],
            workflow_id=row["workflow_id"],
            queue_name=row["queue_name"],
            reason=row["reason"],
            attempts_made=int(row["attempts_made"]),
            payload=json.loads(row["payload_json"]),
            requeued=bool(row["requeued"]),
            failed_at=failed_at,
            requeued_at=from_iso_utc(row["requeued_at"]),
        )

    def enqueue(self, req: EnqueueRequest) -> QueueTask:
        task_id = str(uuid.uuid4())
        now = utc_now()
        avail_str = to_iso_utc(req.available_at or now)
        now_str = to_iso_utc(now)
        with self._db.session() as conn:
            conn.execute(
                """
                INSERT INTO task_queue (
                    task_id, run_id, workflow_id, queue_name, priority, status,
                    attempt, max_attempts, available_at, payload_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    req.run_id,
                    req.workflow_id,
                    req.queue_name,
                    int(req.priority),
                    QueueItemStatus.QUEUED.value,
                    req.max_attempts,
                    avail_str,
                    json.dumps(req.payload),
                    now_str,
                    now_str,
                ),
            )
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> QueueTask:
        with self._db.session() as conn:
            row = conn.execute(
                "SELECT * FROM task_queue WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        if row is None:
            raise EntityNotFoundError(f"Queue task '{task_id}' not found")
        return self._row_to_task(row)

    def claim_next(
        self,
        queue_name: str,
        worker_id: str,
        as_of: datetime | None = None,
    ) -> QueueTask | None:
        cutoff = to_iso_utc(as_of or utc_now())
        now_str = to_iso_utc(utc_now())
        with self._db.session() as conn:
            row = conn.execute(
                """
                SELECT * FROM task_queue
                WHERE queue_name = ? AND status = ? AND available_at <= ?
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                """,
                (queue_name, QueueItemStatus.QUEUED.value, cutoff),
            ).fetchone()
            if row is None:
                return None

            task_id = row["task_id"]
            next_attempt = int(row["attempt"]) + 1
            conn.execute(
                """
                UPDATE task_queue
                SET status = ?, attempt = ?, claimed_by = ?, claimed_at = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (
                    QueueItemStatus.CLAIMED.value,
                    next_attempt,
                    worker_id,
                    now_str,
                    now_str,
                    task_id,
                ),
            )
            updated_row = conn.execute(
                "SELECT * FROM task_queue WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        assert updated_row is not None
        return self._row_to_task(updated_row)

    def complete_task(self, task_id: str) -> QueueTask:
        task = self.get_task(task_id)
        validate_queue_transition(task.status, QueueItemStatus.COMPLETED)
        now_str = to_iso_utc(utc_now())
        with self._db.session() as conn:
            conn.execute(
                """
                UPDATE task_queue
                SET status = ?, last_error = NULL, updated_at = ?
                WHERE task_id = ?
                """,
                (QueueItemStatus.COMPLETED.value, now_str, task_id),
            )
        return self.get_task(task_id)

    def reschedule_task(
        self,
        task_id: str,
        available_at: datetime,
        error_message: str,
    ) -> QueueTask:
        task = self.get_task(task_id)
        validate_queue_transition(task.status, QueueItemStatus.QUEUED)
        now_str = to_iso_utc(utc_now())
        with self._db.session() as conn:
            conn.execute(
                """
                UPDATE task_queue
                SET status = ?, available_at = ?, claimed_by = NULL, claimed_at = NULL,
                    last_error = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (
                    QueueItemStatus.QUEUED.value,
                    to_iso_utc(available_at),
                    error_message,
                    now_str,
                    task_id,
                ),
            )
        return self.get_task(task_id)

    def move_to_dead_letter(self, task_id: str, reason: str) -> DeadLetterEntry:
        task = self.get_task(task_id)
        validate_queue_transition(task.status, QueueItemStatus.DEAD_LETTERED)
        dlq_id = str(uuid.uuid4())
        now_str = to_iso_utc(utc_now())
        with self._db.session() as conn:
            conn.execute(
                """
                UPDATE task_queue
                SET status = ?, last_error = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (QueueItemStatus.DEAD_LETTERED.value, reason, now_str, task_id),
            )
            conn.execute(
                """
                INSERT INTO dead_letters (
                    dlq_id, task_id, run_id, workflow_id, queue_name,
                    reason, attempts_made, payload_json, requeued, failed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    dlq_id,
                    task.task_id,
                    task.run_id,
                    task.workflow_id,
                    task.queue_name,
                    reason,
                    task.attempt,
                    json.dumps(task.payload),
                    now_str,
                ),
            )
            row = conn.execute(
                "SELECT * FROM dead_letters WHERE dlq_id = ?",
                (dlq_id,),
            ).fetchone()
        assert row is not None
        return self._row_to_dlq(row)

    def cancel_queued_for_run(self, run_id: str) -> int:
        now_str = to_iso_utc(utc_now())
        with self._db.session() as conn:
            cur = conn.execute(
                """
                UPDATE task_queue
                SET status = ?, updated_at = ?
                WHERE run_id = ? AND status = ?
                """,
                (QueueItemStatus.CANCELLED.value, now_str, run_id, QueueItemStatus.QUEUED.value),
            )
            return int(cur.rowcount)

    def list_dead_letters(self, include_requeued: bool = False) -> list[DeadLetterEntry]:
        sql = "SELECT * FROM dead_letters"
        if not include_requeued:
            sql += " WHERE requeued = 0"
        sql += " ORDER BY failed_at DESC"
        with self._db.session() as conn:
            rows = conn.execute(sql).fetchall()
        return [self._row_to_dlq(r) for r in rows]

    def requeue_dead_letter(self, dlq_id: str, extra_attempts: int = 1) -> QueueTask:
        with self._db.session() as conn:
            dlq_row = conn.execute(
                "SELECT * FROM dead_letters WHERE dlq_id = ?",
                (dlq_id,),
            ).fetchone()
            if dlq_row is None:
                raise EntityNotFoundError(f"Dead-letter entry '{dlq_id}' not found")
            if bool(dlq_row["requeued"]):
                raise InvalidStateTransitionError(f"Dead-letter entry '{dlq_id}' was already requeued")

            task_id = dlq_row["task_id"]
            now_str = to_iso_utc(utc_now())
            conn.execute(
                """
                UPDATE dead_letters
                SET requeued = 1, requeued_at = ?
                WHERE dlq_id = ?
                """,
                (now_str, dlq_id),
            )
            conn.execute(
                """
                UPDATE task_queue
                SET status = ?, max_attempts = max_attempts + ?, available_at = ?,
                    claimed_by = NULL, claimed_at = NULL, updated_at = ?
                WHERE task_id = ?
                """,
                (
                    QueueItemStatus.QUEUED.value,
                    max(1, extra_attempts),
                    now_str,
                    now_str,
                    task_id,
                ),
            )
        return self.get_task(task_id)

    def get_stats(self, queue_name: str = "default") -> QueueStats:
        with self._db.session() as conn:
            status_rows = conn.execute(
                """
                SELECT status, COUNT(*) AS cnt
                FROM task_queue
                WHERE queue_name = ?
                GROUP BY status
                """,
                (queue_name,),
            ).fetchall()
            prio_rows = conn.execute(
                """
                SELECT priority, COUNT(*) AS cnt
                FROM task_queue
                WHERE queue_name = ? AND status = ?
                GROUP BY priority
                """,
                (queue_name, QueueItemStatus.QUEUED.value),
            ).fetchall()

        counts = {r["status"]: int(r["cnt"]) for r in status_rows}
        prio_map: dict[str, int] = {}
        for row in prio_rows:
            val = int(row["priority"])
            label = next((p.name for p in PriorityLevel if int(p) == val), str(val))
            prio_map[label] = int(row["cnt"])

        return QueueStats(
            queue_name=queue_name,
            queued_count=counts.get(QueueItemStatus.QUEUED.value, 0),
            claimed_count=counts.get(QueueItemStatus.CLAIMED.value, 0),
            completed_count=counts.get(QueueItemStatus.COMPLETED.value, 0),
            dead_lettered_count=counts.get(QueueItemStatus.DEAD_LETTERED.value, 0),
            queued_by_priority=prio_map,
        )
