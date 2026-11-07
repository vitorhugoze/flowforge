"""Record filtering and schema validation step handlers."""

from __future__ import annotations

from typing import Any

from flowforge.exceptions import StepExecutionError
from flowforge.handlers.base import StepContext


def _matches_condition(value: Any, operator: str, target: Any) -> bool:
    if operator == "eq":
        return bool(value == target)
    if operator == "ne":
        return bool(value != target)
    if operator == "gt":
        return value is not None and bool(value > target)
    if operator == "gte":
        return value is not None and bool(value >= target)
    if operator == "lt":
        return value is not None and bool(value < target)
    if operator == "lte":
        return value is not None and bool(value <= target)
    if operator == "in":
        return isinstance(target, list) and value in target
    if operator == "contains":
        return isinstance(value, str) and isinstance(target, str) and target in value
    raise StepExecutionError(f"Unsupported filter operator '{operator}'", retryable=False)


class FilterRecordsHandler:
    """Filter record lists using field comparison predicates."""

    name = "filter.predicate"
    description = "Keep records matching field comparison rules (eq, ne, gt, gte, lt, lte, in, contains)."

    def execute(self, ctx: StepContext) -> dict[str, Any]:
        field_name = ctx.config.get("field")
        operator = ctx.config.get("operator", "eq")
        target = ctx.config.get("value")
        if not isinstance(field_name, str) or not field_name:
            raise StepExecutionError(
                "filter.predicate requires a 'field' name in step config",
                retryable=False,
            )

        records = ctx.resolve_records()
        matched = [
            row for row in records if _matches_condition(row.get(field_name), str(operator), target)
        ]
        return {
            "records": matched,
            "count": len(matched),
            "dropped_count": len(records) - len(matched),
        }


class SchemaValidateHandler:
    """Validate that every record contains required fields and optional numeric bounds."""

    name = "validate.records"
    description = "Verify required fields and numeric minimums across records."

    def execute(self, ctx: StepContext) -> dict[str, Any]:
        required_fields: list[str] = list(ctx.config.get("required_fields", []))
        numeric_min: dict[str, float] = dict(ctx.config.get("numeric_min", {}))
        fail_on_empty: bool = bool(ctx.config.get("fail_on_empty", False))

        records = ctx.resolve_records()
        if fail_on_empty and not records:
            raise StepExecutionError("Validation failed: record batch is empty", retryable=False)

        violations: list[str] = []
        for idx, row in enumerate(records):
            for req in required_fields:
                if req not in row or row[req] is None or row[req] == "":
                    violations.append(f"Row {idx}: missing required field '{req}'")
            for num_field, min_val in numeric_min.items():
                val = row.get(num_field)
                if val is not None:
                    if not isinstance(val, (int, float)) or isinstance(val, bool):
                        violations.append(f"Row {idx}: field '{num_field}' must be numeric")
                    elif float(val) < float(min_val):
                        violations.append(f"Row {idx}: field '{num_field}' ({val}) < {min_val}")

        if violations:
            preview = "; ".join(violations[:5])
            raise StepExecutionError(f"Record validation failed: {preview}", retryable=False)

        return {"records": records, "validated_count": len(records), "valid": True}
