"""Data transformation handlers for field projection, value mapping, and string templates."""

from __future__ import annotations

from typing import Any

from flowforge.exceptions import StepExecutionError
from flowforge.handlers.base import StepContext


class SelectFieldsHandler:
    """Project a subset of keys across a batch of record dictionaries."""

    name = "transform.select_fields"
    description = "Retain only configured fields across records."

    def execute(self, ctx: StepContext) -> dict[str, Any]:
        fields = ctx.config.get("fields")
        if not isinstance(fields, list) or not fields:
            raise StepExecutionError(
                "transform.select_fields requires a non-empty 'fields' list",
                retryable=False,
            )
        records = ctx.resolve_records()
        projected = [{k: row.get(k) for k in fields if k in row} for row in records]
        return {"records": projected, "count": len(projected)}


class MapValuesHandler:
    """Apply string case normalization, field renaming, or static defaults to records."""

    name = "transform.map_values"
    description = "Normalize string casing, rename fields, and inject default values."

    def execute(self, ctx: StepContext) -> dict[str, Any]:
        uppercase_fields: list[str] = list(ctx.config.get("uppercase_fields", []))
        lowercase_fields: list[str] = list(ctx.config.get("lowercase_fields", []))
        rename_map: dict[str, str] = dict(ctx.config.get("rename", {}))
        defaults: dict[str, Any] = dict(ctx.config.get("defaults", {}))

        records = ctx.resolve_records()
        transformed: list[dict[str, Any]] = []
        for row in records:
            item = dict(row)
            for key, default_val in defaults.items():
                item.setdefault(key, default_val)
            for field_name in uppercase_fields:
                if isinstance(item.get(field_name), str):
                    item[field_name] = item[field_name].upper()
            for field_name in lowercase_fields:
                if isinstance(item.get(field_name), str):
                    item[field_name] = item[field_name].lower()
            for old_key, new_key in rename_map.items():
                if old_key in item:
                    item[new_key] = item.pop(old_key)
            transformed.append(item)

        return {"records": transformed, "count": len(transformed)}


class TemplateFormatHandler:
    """Render a summary string using Python format placeholders from inputs and upstream data."""

    name = "transform.template"
    description = "Render a formatted message from run inputs and upstream scalar outputs."

    def execute(self, ctx: StepContext) -> dict[str, Any]:
        template = ctx.config.get("template")
        if not isinstance(template, str) or not template:
            raise StepExecutionError(
                "transform.template requires a non-empty 'template' string",
                retryable=False,
            )
        variables: dict[str, Any] = dict(ctx.run_inputs)
        for step_key, step_out in ctx.upstream_outputs.items():
            variables[step_key] = step_out
            for out_k, out_v in step_out.items():
                if not isinstance(out_v, (list, dict)):
                    variables.setdefault(out_k, out_v)
        try:
            rendered = template.format(**variables)
        except KeyError as exc:
            raise StepExecutionError(
                f"Missing template placeholder variable: {exc}",
                retryable=False,
            ) from exc
        return {"rendered": rendered}
