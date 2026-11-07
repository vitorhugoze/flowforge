"""Application configuration resolved from environment variables or explicit overrides."""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, Field


class Settings(BaseModel):
    """Runtime settings for database persistence, artifacts, and worker behavior."""

    db_path: Path = Field(default_factory=lambda: Path(".flowforge") / "flowforge.db")
    artifact_dir: Path = Field(default_factory=lambda: Path(".flowforge") / "artifacts")
    default_queue: str = "default"
    worker_batch_size: int = Field(default=10, ge=1, le=500)
    max_retry_cap_seconds: float = Field(default=3600.0, ge=1.0)

    def ensure_directories(self) -> None:
        """Create parent directories needed by SQLite and local step artifacts."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.artifact_dir.mkdir(parents=True, exist_ok=True)


def get_settings(
    db_path: str | Path | None = None,
    artifact_dir: str | Path | None = None,
) -> Settings:
    """Resolve settings from explicit arguments or environment variables."""
    resolved_db = (
        Path(db_path)
        if db_path is not None
        else Path(os.environ.get("FLOWFORGE_DB_PATH", ".flowforge/flowforge.db"))
    )
    resolved_artifacts = (
        Path(artifact_dir)
        if artifact_dir is not None
        else Path(os.environ.get("FLOWFORGE_ARTIFACT_DIR", ".flowforge/artifacts"))
    )
    default_queue = os.environ.get("FLOWFORGE_DEFAULT_QUEUE", "default").strip() or "default"
    batch_raw = os.environ.get("FLOWFORGE_WORKER_BATCH_SIZE", "10")
    return Settings(
        db_path=resolved_db.resolve(),
        artifact_dir=resolved_artifacts.resolve(),
        default_queue=default_queue,
        worker_batch_size=int(batch_raw),
    )
