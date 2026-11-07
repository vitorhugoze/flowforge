"""Shared pytest fixtures providing isolated SQLite databases, services, and API clients."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from flowforge.api.app import create_app
from flowforge.config import Settings
from flowforge.services.orchestrator import OrchestratorService


@pytest.fixture()
def test_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    db_file = tmp_path / "vault" / "flowforge_test.db"
    artifacts = tmp_path / "vault" / "artifacts"
    monkeypatch.setenv("FLOWFORGE_DB_PATH", str(db_file))
    monkeypatch.setenv("FLOWFORGE_ARTIFACT_DIR", str(artifacts))
    return Settings(db_path=db_file, artifact_dir=artifacts)


@pytest.fixture()
def orchestrator(test_settings: Settings) -> OrchestratorService:
    return OrchestratorService(settings=test_settings)


@pytest.fixture()
def api_client(orchestrator: OrchestratorService) -> TestClient:
    app = create_app(service=orchestrator)
    return TestClient(app)
