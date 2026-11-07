"""SQLite connection management with clean transaction and handle lifecycle."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from flowforge.storage.schema import SCHEMA_STATEMENTS


class DatabaseManager:
    """Manages SQLite schema setup and short-lived transactional sessions."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=15.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        return conn

    @contextmanager
    def session(self) -> Iterator[sqlite3.Connection]:
        """Yield a SQLite connection, committing on success and always closing on exit."""
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize_schema(self) -> None:
        """Create tables and indexes if they do not exist yet."""
        with self.session() as conn:
            for statement in SCHEMA_STATEMENTS:
                conn.execute(statement)
