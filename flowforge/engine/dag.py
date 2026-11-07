"""Topological DAG validation, wave computation, and dependency ordering."""

from __future__ import annotations

from collections import deque

from flowforge.domain.workflow import StepDefinition
from flowforge.exceptions import ValidationFailedError


class TopologicalPlanner:
    """Validates workflow step graphs and produces deterministic topological orderings."""

    @staticmethod
    def sort_steps(steps: list[StepDefinition]) -> list[StepDefinition]:
        """Return steps in topological execution order or raise ValidationFailedError on cycles."""
        if not steps:
            raise ValidationFailedError("Workflow must define at least one step")

        by_key: dict[str, StepDefinition] = {}
        order_index: dict[str, int] = {}
        for idx, step in enumerate(steps):
            if step.key in by_key:
                raise ValidationFailedError(f"Duplicate step key '{step.key}'")
            by_key[step.key] = step
            order_index[step.key] = idx

        in_degree: dict[str, int] = {s.key: 0 for s in steps}
        dependents: dict[str, list[str]] = {s.key: [] for s in steps}

        for step in steps:
            for dep in step.depends_on:
                if dep not in by_key:
                    raise ValidationFailedError(
                        f"Step '{step.key}' depends on unknown step '{dep}'"
                    )
                in_degree[step.key] += 1
                dependents[dep].append(step.key)

        ready = deque(
            sorted(
                (k for k, deg in in_degree.items() if deg == 0),
                key=lambda k: order_index[k],
            )
        )
        ordered: list[StepDefinition] = []

        while ready:
            current_key = ready.popleft()
            ordered.append(by_key[current_key])
            newly_ready: list[str] = []
            for child_key in dependents[current_key]:
                in_degree[child_key] -= 1
                if in_degree[child_key] == 0:
                    newly_ready.append(child_key)
            for child_key in sorted(newly_ready, key=lambda k: order_index[k]):
                ready.append(child_key)

        if len(ordered) != len(steps):
            unresolved = [k for k, deg in in_degree.items() if deg > 0]
            raise ValidationFailedError(
                f"Cycle detected in workflow step dependencies involving: {unresolved}"
            )

        return ordered

    @staticmethod
    def execution_waves(steps: list[StepDefinition]) -> list[list[str]]:
        """Group step keys into parallelizable topological waves."""
        ordered = TopologicalPlanner.sort_steps(steps)
        by_key = {s.key: s for s in ordered}
        levels: dict[str, int] = {}

        for step in ordered:
            if not step.depends_on:
                levels[step.key] = 0
            else:
                levels[step.key] = 1 + max(levels[dep] for dep in by_key[step.key].depends_on)

        max_level = max(levels.values()) if levels else 0
        waves: list[list[str]] = [[] for _ in range(max_level + 1)]
        for step in ordered:
            waves[levels[step.key]].append(step.key)
        return waves
