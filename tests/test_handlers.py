"""Unit tests for built-in transformation, validation, aggregation, and artifact handlers."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowforge.exceptions import StepExecutionError
from flowforge.handlers.aggregate import DeduplicateRecordsHandler, SummarizeNumericHandler
from flowforge.handlers.base import StepContext
from flowforge.handlers.filter_validate import FilterRecordsHandler, SchemaValidateHandler
from flowforge.handlers.io_local import JsonArtifactReadHandler, JsonArtifactWriteHandler
from flowforge.handlers.transform import (
    MapValuesHandler,
    SelectFieldsHandler,
    TemplateFormatHandler,
)


def _ctx(
    tmp_path: Path,
    *,
    inputs: dict | None = None,
    upstream: dict | None = None,
    config: dict | None = None,
) -> StepContext:
    return StepContext(
        run_id="run-1",
        workflow_id="wf-1",
        step_key="step-1",
        attempt=1,
        run_inputs=inputs or {},
        upstream_outputs=upstream or {},
        config=config or {},
        artifact_dir=tmp_path / "artifacts",
    )


def test_transform_and_filter_handlers(tmp_path: Path) -> None:
    sample = [
        {"id": "1", "tier": "gold", "amount": 120},
        {"id": "2", "tier": "silver", "amount": 40},
        {"id": "1", "tier": "gold", "amount": 120},
    ]
    dedup = DeduplicateRecordsHandler().execute(
        _ctx(tmp_path, inputs={"records": sample}, config={"key_field": "id"})
    )
    assert dedup["count"] == 2
    assert dedup["duplicates_removed"] == 1

    filtered = FilterRecordsHandler().execute(
        _ctx(
            tmp_path,
            upstream={"dedup": dedup},
            config={"field": "amount", "operator": "gte", "value": 100},
        )
    )
    assert filtered["count"] == 1

    mapped = MapValuesHandler().execute(
        _ctx(
            tmp_path,
            upstream={"flt": filtered},
            config={"uppercase_fields": ["tier"], "defaults": {"currency": "USD"}},
        )
    )
    assert mapped["records"][0] == {"id": "1", "tier": "GOLD", "amount": 120, "currency": "USD"}

    projected = SelectFieldsHandler().execute(
        _ctx(tmp_path, upstream={"map": mapped}, config={"fields": ["id", "currency"]})
    )
    assert projected["records"] == [{"id": "1", "currency": "USD"}]


def test_numeric_summary_and_template(tmp_path: Path) -> None:
    rows = [{"score": 10}, {"score": 20}, {"score": 30}]
    summary = SummarizeNumericHandler().execute(
        _ctx(tmp_path, inputs={"records": rows}, config={"field": "score"})
    )
    assert summary["count"] == 3
    assert summary["total"] == 60.0
    assert summary["mean"] == 20.0

    msg = TemplateFormatHandler().execute(
        _ctx(
            tmp_path,
            inputs={"batch_name": "daily"},
            upstream={"stats": summary},
            config={"template": "Batch {batch_name}: count={count}, mean={mean}"},
        )
    )
    assert msg["rendered"] == "Batch daily: count=3, mean=20.0"


def test_schema_validation_rejects_bad_rows(tmp_path: Path) -> None:
    bad_rows = [{"name": "item-a", "price": -5}]
    with pytest.raises(StepExecutionError, match="numeric_min|'price'"):
        SchemaValidateHandler().execute(
            _ctx(
                tmp_path,
                inputs={"records": bad_rows},
                config={"required_fields": ["name"], "numeric_min": {"price": 0}},
            )
        )


def test_sandboxed_artifact_io_and_traversal_guard(tmp_path: Path) -> None:
    write_out = JsonArtifactWriteHandler().execute(
        _ctx(
            tmp_path,
            inputs={"hello": "world"},
            config={"filename": "reports/out.json"},
        )
    )
    assert write_out["artifact_path"] == "reports/out.json"

    read_out = JsonArtifactReadHandler().execute(
        _ctx(tmp_path, config={"filename": "reports/out.json"})
    )
    assert read_out["artifact"]["inputs"] == {"hello": "world"}

    with pytest.raises(StepExecutionError, match="escapes sandbox"):
        JsonArtifactWriteHandler().execute(
            _ctx(tmp_path, config={"filename": "../../outside.json"})
        )
