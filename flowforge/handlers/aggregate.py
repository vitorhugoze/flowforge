"""Aggregation and deduplication step handlers."""

from __future__ import annotations

from typing import Any

from flowforge.exceptions import StepExecutionError
from flowforge.handlers.base import StepContext


class SummarizeNumericHandler:
    """Compute count, sum, min, max, and average for a numeric record field."""

    name = "aggregate.numeric_summary"
    description = "Compute count, sum, min, max, and mean for a target numeric field."

    def execute(self, ctx: StepContext) -> dict[str, Any]:
        field_name = ctx.config.get("field")
        if not isinstance(field_name, str) or not field_name:
            raise StepExecutionError(
                "aggregate.numeric_summary requires 'field' in step config",
                retryable=False,
            )

        records = ctx.resolve_records()
        values: list[float] = []
        for row in records:
            raw = row.get(field_name)
            if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                values.append(float(raw))

        if not values:
            return {
                "field": field_name,
                "count": 0,
                "total": 0.0,
                "min": None,
                "max": None,
                "mean": 0.0,
            }

        total = round(sum(values), 6)
        mean = round(total / len(values), 6)
        return {
            "field": field_name,
            "count": len(values),
            "total": total,
            "min": min(values),
            "max": max(values),
            "mean": mean,
        }


class DeduplicateRecordsHandler:
    """Deduplicate records while preserving first-seen order."""

    name = "aggregate.deduplicate"
    description = "Remove duplicate records based on a configured key field."

    def execute(self, ctx: StepContext) -> dict[str, Any]:
        key_field = ctx.config.get("key_field")
        if not isinstance(key_field, str) or not key_field:
            raise StepExecutionError(
                "aggregate.deduplicate requires 'key_field' in step config",
                retryable=False,
            )

        records = ctx.resolve_records()
        seen: set[Any] = set()
        unique_rows: list[dict[str, Any]] = []
        duplicates_removed = 0

        for row in records:
            marker = row.get(key_field)
            if marker in seen:
                duplicates_removed += 1
                continue
            seen.add(marker)
            unique_rows.append(row)

        return {
            "records": unique_rows,
            "count": len(unique_rows),
            "duplicates_removed": duplicates_removed,
        }
