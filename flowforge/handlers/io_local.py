"""Sandboxed JSON artifact read/write handlers for persisting step results to disk."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from flowforge.exceptions import StepExecutionError
from flowforge.handlers.base import StepContext


def _resolve_safe_artifact_path(base_dir: Path, relative_name: str) -> Path:
    if not relative_name or not relative_name.strip():
        raise StepExecutionError("Artifact 'filename' must be a non-empty relative path", retryable=False)
    cleaned = relative_name.strip()
    if ":" in cleaned or cleaned.startswith(("/", "\\")):
        raise StepExecutionError(
            f"Artifact filename '{relative_name}' must be relative to artifact workspace",
            retryable=False,
        )
    root = base_dir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    candidate = (root / cleaned).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise StepExecutionError(
            f"Artifact path '{relative_name}' escapes sandbox directory",
            retryable=False,
        ) from exc
    return candidate


class JsonArtifactWriteHandler:
    """Persist upstream data or records as a JSON artifact inside the sandbox directory."""

    name = "io.write_json_artifact"
    description = "Write records or upstream outputs to a sandboxed JSON artifact file."

    def execute(self, ctx: StepContext) -> dict[str, Any]:
        filename = str(ctx.config.get("filename", f"{ctx.run_id}_{ctx.step_key}.json"))
        target = _resolve_safe_artifact_path(ctx.artifact_dir, filename)
        target.parent.mkdir(parents=True, exist_ok=True)

        payload: dict[str, Any]
        if ctx.upstream_outputs:
            payload = {"run_id": ctx.run_id, "upstream": ctx.upstream_outputs}
        else:
            payload = {"run_id": ctx.run_id, "inputs": ctx.run_inputs}

        serialized = json.dumps(payload, indent=2)
        target.write_text(serialized, encoding="utf-8")
        rel_path = target.relative_to(ctx.artifact_dir.resolve()).as_posix()
        return {"artifact_path": rel_path, "bytes_written": len(serialized.encode("utf-8"))}


class JsonArtifactReadHandler:
    """Load a previously saved JSON artifact from the sandbox directory."""

    name = "io.read_json_artifact"
    description = "Read a JSON artifact file from the sandboxed artifact directory."

    def execute(self, ctx: StepContext) -> dict[str, Any]:
        filename = str(ctx.config.get("filename", ""))
        target = _resolve_safe_artifact_path(ctx.artifact_dir, filename)
        if not target.is_file():
            raise StepExecutionError(f"Artifact file '{filename}' does not exist", retryable=True)
        content = json.loads(target.read_text(encoding="utf-8"))
        if isinstance(content, dict):
            return {"artifact": content}
        return {"artifact": {"items": content}}
