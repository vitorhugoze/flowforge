"""End-to-end CLI tests verifying JSON output envelopes and persistence."""

from __future__ import annotations

import json
from pathlib import Path

from flowforge.cli.main import run_cli


def test_cli_workflow_run_schedule_and_metrics(tmp_path: Path) -> None:
    db_arg = str(tmp_path / "cli_vault.db")
    art_arg = str(tmp_path / "cli_artifacts")
    base_args = ["--db-path", db_arg, "--artifact-dir", art_arg]

    spec = json.dumps(
        {
            "name": "cli-pipeline",
            "steps": [
                {
                    "key": "save_artifact",
                    "name": "Write Artifact",
                    "handler": "io.write_json_artifact",
                    "config": {"filename": "summary.json"},
                }
            ],
        }
    )
    created = json.loads(run_cli([*base_args, "workflow-create", spec]))
    assert created["status"] == "ok"
    assert created["data"]["name"] == "cli-pipeline"

    triggered = json.loads(
        run_cli(
            [
                *base_args,
                "run-trigger",
                "cli-pipeline",
                "--priority",
                "high",
                "--inputs",
                '{"env":"staging"}',
            ]
        )
    )
    assert triggered["status"] == "ok"
    run_id = triggered["data"]["run_id"]

    tick = json.loads(run_cli([*base_args, "worker-tick"]))
    assert tick["status"] == "ok"
    assert tick["data"]["succeeded"] == 1

    fetched = json.loads(run_cli([*base_args, "run-get", run_id]))
    assert fetched["data"]["status"] == "succeeded"

    sched = json.loads(
        run_cli(
            [
                *base_args,
                "schedule-create",
                "nightly-cli",
                "cli-pipeline",
                "--run-at",
                "2026-08-01T00:00:00Z",
            ]
        )
    )
    assert sched["status"] == "ok"

    sched_tick = json.loads(
        run_cli([*base_args, "schedule-tick", "--as-of", "2026-08-01T00:05:00Z"])
    )
    assert sched_tick["data"]["triggered_count"] == 1

    metrics = json.loads(run_cli([*base_args, "metrics"]))
    assert metrics["status"] == "ok"
    assert metrics["data"]["total_runs"] == 2
