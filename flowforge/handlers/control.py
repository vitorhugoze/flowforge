"""Control-flow, upstream merging, and fault-simulation step handlers."""

from __future__ import annotations

from typing import Any

from flowforge.exceptions import StepExecutionError
from flowforge.handlers.base import StepContext


class PassThroughHandler:
    """Echo run inputs merged with static step configuration payload."""

    name = "control.passthrough"
    description = "Forward run inputs and static payload downstream."

    def execute(self, ctx: StepContext) -> dict[str, Any]:
        extra = ctx.config.get("static", {})
        result = dict(ctx.run_inputs)
        if isinstance(extra, dict):
            result.update(extra)
        return result


class MergeUpstreamHandler:
    """Combine outputs from all upstream dependencies into a single dictionary."""

    name = "control.merge_upstream"
    description = "Aggregate upstream step outputs under their step keys and merge record lists."

    def execute(self, ctx: StepContext) -> dict[str, Any]:
        combined_records: list[dict[str, Any]] = []
        for out in ctx.upstream_outputs.values():
            recs = out.get("records")
            if isinstance(recs, list):
                combined_records.extend(r for r in recs if isinstance(r, dict))

        return {
            "sources": dict(ctx.upstream_outputs),
            "source_count": len(ctx.upstream_outputs),
            "records": combined_records,
            "total_records": len(combined_records),
        }


class ConditionalAssertHandler:
    """Guard step that can fail transiently until a target attempt or enforce invariants."""

    name = "control.assert_gate"
    description = "Verify conditions or fail transiently until min_attempt is reached."

    def execute(self, ctx: StepContext) -> dict[str, Any]:
        succeed_on_attempt = int(ctx.config.get("succeed_on_attempt", 1))
        retryable = bool(ctx.config.get("retryable", True))
        error_msg = str(ctx.config.get("error_message", "Gate assertion failed"))

        if ctx.attempt < succeed_on_attempt:
            raise StepExecutionError(
                f"{error_msg} (attempt {ctx.attempt}/{succeed_on_attempt})",
                retryable=retryable,
            )

        required_input = ctx.config.get("require_input_key")
        if isinstance(required_input, str) and required_input not in ctx.run_inputs:
            raise StepExecutionError(
                f"Required run input '{required_input}' is missing",
                retryable=False,
            )

        return {"gate_passed": True, "attempt": ctx.attempt}
