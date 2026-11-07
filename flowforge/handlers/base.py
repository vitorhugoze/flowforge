"""Step handler protocol, execution context, and pluggable registry."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from flowforge.exceptions import DuplicateEntityError, HandlerNotFoundError


@dataclass(frozen=True)
class StepContext:
    """Runtime context passed to every step handler invocation."""

    run_id: str
    workflow_id: str
    step_key: str
    attempt: int
    run_inputs: dict[str, Any]
    upstream_outputs: dict[str, dict[str, Any]]
    config: dict[str, Any] = field(default_factory=dict)
    artifact_dir: Path = field(default_factory=lambda: Path(".flowforge/artifacts"))

    def resolve_records(self, source_key: str = "records") -> list[dict[str, Any]]:
        """Locate a list of record dicts from upstream outputs or initial run inputs."""
        from_step = self.config.get("from_step")
        if isinstance(from_step, str) and from_step in self.upstream_outputs:
            candidate = self.upstream_outputs[from_step].get(source_key)
            if isinstance(candidate, list):
                return [dict(item) for item in candidate if isinstance(item, dict)]

        for output in reversed(list(self.upstream_outputs.values())):
            if isinstance(output.get(source_key), list):
                return [dict(item) for item in output[source_key] if isinstance(item, dict)]

        raw = self.run_inputs.get(source_key, [])
        if isinstance(raw, list):
            return [dict(item) for item in raw if isinstance(item, dict)]
        return []


class StepHandler(Protocol):
    """Protocol implemented by all workflow step handlers."""

    name: str
    description: str

    def execute(self, ctx: StepContext) -> dict[str, Any]:
        """Run step logic and return a JSON-serializable output dictionary."""
        ...


class HandlerRegistry:
    """In-memory catalog of step handlers available to WorkflowExecutor."""

    def __init__(self) -> None:
        self._handlers: dict[str, StepHandler] = {}

    def register(self, handler: StepHandler, *, overwrite: bool = False) -> None:
        if handler.name in self._handlers and not overwrite:
            raise DuplicateEntityError(f"Step handler '{handler.name}' is already registered")
        self._handlers[handler.name] = handler

    def get(self, name: str) -> StepHandler:
        if name not in self._handlers:
            raise HandlerNotFoundError(f"Step handler '{name}' is not registered")
        return self._handlers[name]

    def has(self, name: str) -> bool:
        return name in self._handlers

    def list_metadata(self) -> list[dict[str, str]]:
        return [
            {"name": h.name, "description": h.description}
            for h in sorted(self._handlers.values(), key=lambda item: item.name)
        ]
