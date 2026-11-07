"""SQLite DDL statements for FlowForge persistence."""

from __future__ import annotations

SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS workflows (
        workflow_id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        description TEXT NOT NULL DEFAULT '',
        version INTEGER NOT NULL DEFAULT 1,
        status TEXT NOT NULL,
        default_priority INTEGER NOT NULL,
        retry_policy_json TEXT NOT NULL,
        tags_json TEXT NOT NULL,
        steps_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS workflow_runs (
        run_id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        workflow_name TEXT NOT NULL,
        workflow_version INTEGER NOT NULL,
        status TEXT NOT NULL,
        priority INTEGER NOT NULL,
        triggered_by TEXT NOT NULL,
        idempotency_key TEXT UNIQUE,
        inputs_json TEXT NOT NULL,
        outputs_json TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        created_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT,
        FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_runs_workflow_status
    ON workflow_runs (workflow_id, status, created_at);
    """,
    """
    CREATE TABLE IF NOT EXISTS step_runs (
        step_run_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        step_order INTEGER NOT NULL,
        handler TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt INTEGER NOT NULL DEFAULT 0,
        input_json TEXT NOT NULL,
        output_json TEXT,
        error_message TEXT,
        duration_ms REAL,
        started_at TEXT,
        finished_at TEXT,
        UNIQUE(run_id, step_key),
        FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS task_queue (
        task_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        workflow_id TEXT NOT NULL,
        queue_name TEXT NOT NULL,
        priority INTEGER NOT NULL,
        status TEXT NOT NULL,
        attempt INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 3,
        available_at TEXT NOT NULL,
        claimed_by TEXT,
        claimed_at TEXT,
        last_error TEXT,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE CASCADE
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_queue_claim
    ON task_queue (queue_name, status, available_at, priority DESC, created_at ASC);
    """,
    """
    CREATE TABLE IF NOT EXISTS dead_letters (
        dlq_id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        workflow_id TEXT NOT NULL,
        queue_name TEXT NOT NULL,
        reason TEXT NOT NULL,
        attempts_made INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        requeued INTEGER NOT NULL DEFAULT 0,
        failed_at TEXT NOT NULL,
        requeued_at TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS schedules (
        schedule_id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        workflow_id TEXT NOT NULL,
        status TEXT NOT NULL,
        next_run_at TEXT NOT NULL,
        interval_seconds INTEGER,
        inputs_json TEXT NOT NULL,
        priority INTEGER NOT NULL,
        run_count INTEGER NOT NULL DEFAULT 0,
        max_runs INTEGER,
        last_run_at TEXT,
        last_run_id TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_schedules_due
    ON schedules (status, next_run_at);
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_events (
        event_id TEXT PRIMARY KEY,
        category TEXT NOT NULL,
        action TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        run_id TEXT,
        actor TEXT NOT NULL,
        details_json TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_audit_lookup
    ON audit_events (category, entity_id, created_at DESC);
    """,
)
