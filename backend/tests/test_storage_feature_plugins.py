from deerflow.extensions.loader import ExtensionSpec, load_extensions
from deerflow.features.plugins import DEFAULT_FEATURES, with_default_features


def test_default_features_use_normal_loader_and_explicit_disable():
    loaded, diagnostics = load_extensions(with_default_features([]))
    assert not diagnostics
    assert {plugin.namespace for _, plugin in loaded.plugins} == {"hm.my-files", "hm.shared", "hm.projects"}
    assert len(loaded.services) == 3
    assert all(plugin.api_version == 4 and plugin.storage_api_version == 1 for _, plugin in loaded.plugins)
    shared = next(plugin for _, plugin in loaded.plugins if plugin.namespace == "hm.shared")
    assert shared.storage_controller.name == "publication"
    assert shared.storage_controller.metadata_version == 1
    disabled = [ExtensionSpec(use=entry, enabled=False) for entry in DEFAULT_FEATURES]
    loaded, diagnostics = load_extensions(with_default_features(disabled))
    assert not loaded.plugins and not loaded.services and not diagnostics


def test_default_feature_configuration_is_not_duplicated():
    spec = ExtensionSpec(use=DEFAULT_FEATURES[0], config={"enabled": False})
    entries = with_default_features([spec])
    assert len(entries) == 3 and entries[0] is spec
    loaded, diagnostics = load_extensions(entries)
    assert not diagnostics
    plugin = next(plugin for _, plugin in loaded.plugins if plugin.namespace == "hm.my-files")
    assert plugin.enabled is False
