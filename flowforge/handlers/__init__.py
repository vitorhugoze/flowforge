"""Built-in step handler catalog registration."""

from __future__ import annotations

from flowforge.handlers.aggregate import DeduplicateRecordsHandler, SummarizeNumericHandler
from flowforge.handlers.base import HandlerRegistry, StepContext, StepHandler
from flowforge.handlers.control import (
    ConditionalAssertHandler,
    MergeUpstreamHandler,
    PassThroughHandler,
)
from flowforge.handlers.filter_validate import FilterRecordsHandler, SchemaValidateHandler
from flowforge.handlers.io_local import JsonArtifactReadHandler, JsonArtifactWriteHandler
from flowforge.handlers.transform import (
    MapValuesHandler,
    SelectFieldsHandler,
    TemplateFormatHandler,
)


def build_default_registry() -> HandlerRegistry:
    """Instantiate a HandlerRegistry populated with all built-in step handlers."""
    registry = HandlerRegistry()
    for handler in (
        PassThroughHandler(),
        MergeUpstreamHandler(),
        ConditionalAssertHandler(),
        SelectFieldsHandler(),
        MapValuesHandler(),
        TemplateFormatHandler(),
        FilterRecordsHandler(),
        SchemaValidateHandler(),
        SummarizeNumericHandler(),
        DeduplicateRecordsHandler(),
        JsonArtifactWriteHandler(),
        JsonArtifactReadHandler(),
    ):
        registry.register(handler)
    return registry


__all__ = [
    "HandlerRegistry",
    "StepContext",
    "StepHandler",
    "build_default_registry",
]
