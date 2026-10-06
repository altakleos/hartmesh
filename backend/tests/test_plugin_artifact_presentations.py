"""Artifact preview capability shares the existing trusted installation boundary."""

from dataclasses import replace

import pytest
from deerflow_extension_api.plugins import ArtifactPresentation, BrowserModule, PluginContribution

from deerflow.extensions.registry import ExtensionRegistry


def declaration(**changes):
    return replace(ArtifactPresentation(id="summary", suffixes=(".summary.json",), project=lambda raw: b'{"summary":"ready"}', projection_marker="example-summary-v1"), **changes)


def plugin(**changes):
    return replace(
        PluginContribution(namespace="example.summary", title="Summary", enabled=True, api_version=2, frontend=BrowserModule("summary.v1", 'export default {apiVersion:1,module:"summary.v1"};'), artifacts=(declaration(),)), **changes
    )


def test_artifact_capability_reuses_source_attribution_activation_and_rollback():
    registry = ExtensionRegistry()
    with registry.attributed_to("example:install"):
        mark = registry.mark()
        assert registry.plugin(plugin()) is True
        source, installed = registry.build().plugins[0]
        assert source == "example:install"
        assert installed.enabled is True
        assert installed.artifacts[0].id == "summary"
        registry.rollback_to(mark)
        assert registry.build().plugins == ()


def test_page_only_v1_remains_supported_and_artifacts_negotiate_explicitly():
    registry = ExtensionRegistry()
    with registry.attributed_to("example:install"):
        assert registry.plugin(plugin(api_version=1, artifacts=())) is True
        mark = registry.mark()
        with pytest.raises(ValueError):
            registry.plugin(plugin(namespace="example.other", frontend=None, api_version=1))
        assert registry.mark() == mark


@pytest.mark.parametrize(
    "changes",
    [
        {"id": "../bad"},
        {"id": None},
        {"suffixes": ()},
        {"suffixes": ("https://example.test/a.js",)},
        {"source_max_bytes": True},
        {"source_max_bytes": 16 * 1024 * 1024 + 1},
        {"preview_max_bytes": 1024 * 1024 + 1},
        {"preview_max_bytes": 0},
        {"projection_marker": None},
        {"compat_queries": ("download",)},
        {"compat_queries": ("legacy", "legacy")},
        {"project": "module:load"},
    ],
)
def test_invalid_artifact_declarations_cannot_half_register(changes):
    registry = ExtensionRegistry()
    with registry.attributed_to("example:install"):
        mark = registry.mark()
        with pytest.raises(ValueError):
            registry.plugin(plugin(artifacts=(declaration(**changes),)))
        assert registry.mark() == mark


def test_duplicate_artifact_ids_and_aliases_have_no_hidden_winner():
    registry = ExtensionRegistry()
    with registry.attributed_to("example:install"):
        with pytest.raises(ValueError):
            registry.plugin(plugin(artifacts=(declaration(), declaration())))
        first = declaration(compat_queries=("legacy_preview",))
        registry.plugin(plugin(artifacts=(first,)))
        mark = registry.mark()
        with pytest.raises(ValueError):
            registry.plugin(plugin(namespace="example.second", frontend=None, artifacts=(first,)))
        assert registry.mark() == mark
