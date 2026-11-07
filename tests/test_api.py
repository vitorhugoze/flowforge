"""End-to-end HTTP API tests using FastAPI TestClient."""

from __future__ import annotations

from fastapi.testclient import TestClient


def test_api_workflow_lifecycle_execution_and_metrics(api_client: TestClient) -> None:
    health = api_client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "healthy"

    handlers_resp = api_client.get("/handlers")
    assert handlers_resp.status_code == 200
    assert any(h["name"] == "transform.map_values" for h in handlers_resp.json())

    create_resp = api_client.post(
        "/workflows",
        json={
            "name": "customer-onboarding",
            "description": "Normalize and summarize customer records",
            "steps": [
                {
                    "key": "normalize",
                    "name": "Normalize Emails",
                    "handler": "transform.map_values",
                    "config": {"lowercase_fields": ["email"]},
                },
                {
                    "key": "report",
                    "name": "Format Message",
                    "handler": "transform.template",
                    "depends_on": ["normalize"],
                    "config": {"template": "Processed {count} onboarding records"},
                },
            ],
        },
    )
    assert create_resp.status_code == 201
    wf_id = create_resp.json()["workflow_id"]

    # Duplicate workflow name returns 409
    dup_resp = api_client.post(
        "/workflows",
        json={
            "name": "customer-onboarding",
            "steps": [{"key": "s1", "name": "S1", "handler": "control.passthrough"}],
        },
    )
    assert dup_resp.status_code == 409

    waves_resp = api_client.get(f"/workflows/{wf_id}/waves")
    assert waves_resp.status_code == 200
    assert waves_resp.json()["waves"] == [["normalize"], ["report"]]

    run_resp = api_client.post(
        "/workflows/customer-onboarding/runs",
        json={"inputs": {"records": [{"email": "ALICE@EXAMPLE.COM"}]}},
    )
    assert run_resp.status_code == 201
    run_id = run_resp.json()["run_id"]

    tick_resp = api_client.post("/worker/tick", json={"max_tasks": 5})
    assert tick_resp.status_code == 200
    assert tick_resp.json()["succeeded"] == 1

    detail_resp = api_client.get(f"/runs/{run_id}")
    assert detail_resp.status_code == 200
    body = detail_resp.json()
    assert body["status"] == "succeeded"
    assert body["outputs"]["normalize"]["records"][0]["email"] == "alice@example.com"
    assert body["outputs"]["report"]["rendered"] == "Processed 1 onboarding records"

    metrics_resp = api_client.get("/metrics")
    assert metrics_resp.status_code == 200
    metrics = metrics_resp.json()
    assert metrics["total_workflows"] == 1
    assert metrics["success_rate"] == 1.0

    audit_resp = api_client.get("/audit", params={"run_id": run_id})
    assert audit_resp.status_code == 200
    assert len(audit_resp.json()) >= 3
