"""Structured immutable audit trail repository."""

from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

from flowforge.domain.enums import AuditCategory
from flowforge.domain.schedule_audit import AuditEvent
from flowforge.storage.database import DatabaseManager
from flowforge.utils_time import from_iso_utc, to_iso_utc, utc_now


class AuditRepository:
    """Appends and queries structured operational audit events."""

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> AuditEvent:
        created = from_iso_utc(row["created_at"])
        assert created is not None
        return AuditEvent(
            event_id=row["event_id"],
            category=AuditCategory(row["category"]),
            action=row["action"],
            entity_id=row["entity_id"],
            run_id=row["run_id"],
            actor=row["actor"],
            details=json.loads(row["details_json"]),
            created_at=created,
        )

    def record(
        self,
        category: AuditCategory,
        action: str,
        entity_id: str,
        *,
        run_id: str | None = None,
        actor: str = "system",
        details: dict[str, Any] | None = None,
    ) -> AuditEvent:
        event_id = str(uuid.uuid4())
        now_str = to_iso_utc(utc_now())
        with self._db.session() as conn:
            conn.execute(
                """
                INSERT INTO audit_events (
                    event_id, category, action, entity_id, run_id, actor, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    category.value,
                    action,
                    entity_id,
                    run_id,
                    actor,
                    json.dumps(details or {}),
                    now_str,
                ),
            )
            row = conn.execute(
                "SELECT * FROM audit_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
        assert row is not None
        return self._row_to_event(row)

    def list_events(
        self,
        *,
        category: AuditCategory | None = None,
        entity_id: str | None = None,
        run_id: str | None = None,
        limit: int = 100,
    ) -> list[AuditEvent]:
        clauses: list[str] = []
        params: list[object] = []
        if category is not None:
            clauses.append("category = ?")
            params.append(category.value)
        if entity_id is not None:
            clauses.append("entity_id = ?")
            params.append(entity_id)
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        with self._db.session() as conn:
            rows = conn.execute(
                f"SELECT * FROM audit_events {where_sql} ORDER BY created_at DESC LIMIT ?",
                tuple(params),
            ).fetchall()
        return [self._row_to_event(r) for r in rows]
