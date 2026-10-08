from types import SimpleNamespace

import pytest
from deerflow_extension_api.storage import StorageUnsupported


def test_unqualified_sandbox_adapters_never_create_or_bind_legacy_feature_roots(monkeypatch):
    from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
    from deerflow.config import paths
    from deerflow.sandbox.local.local_sandbox_provider import LocalSandboxProvider

    monkeypatch.setattr("deerflow.config.get_app_config", lambda: SimpleNamespace(storage_spaces=SimpleNamespace(enabled=True)))

    def forbidden():
        pytest.fail("Legacy paths must not be resolved in qualified resource mode")

    monkeypatch.setattr(paths, "get_paths", forbidden)
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.get_paths", forbidden)
    with pytest.raises(StorageUnsupported, match="writer fencing"):
        AioSandboxProvider._get_thread_mounts("thread", user_id="alice")
    with pytest.raises(StorageUnsupported, match="writer fencing"):
        LocalSandboxProvider._build_thread_path_mappings("thread", user_id="alice")


def test_standalone_legacy_mount_helpers_do_not_require_app_config(tmp_path, monkeypatch):
    from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
    from deerflow.config.paths import Paths
    from deerflow.sandbox.local.local_sandbox_provider import LocalSandboxProvider

    def missing():
        raise FileNotFoundError("Synthetic standalone host has no app configuration")

    paths = Paths(tmp_path)
    monkeypatch.setattr("deerflow.config.get_app_config", missing)
    monkeypatch.setattr("deerflow.config.app_config.peek_loaded_app_config", lambda: None)
    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: paths)
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.get_paths", lambda: paths)
    thread = "11111111-1111-1111-1111-111111111111"
    assert AioSandboxProvider._get_thread_mounts(thread, user_id="alice")
    assert LocalSandboxProvider._build_thread_path_mappings(thread, user_id="alice")


@pytest.mark.parametrize("adapter", ["aio", "local"])
def test_missing_previously_loaded_resource_config_never_enables_legacy_mounts(tmp_path, monkeypatch, adapter):
    from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
    from deerflow.config import app_config, paths
    from deerflow.sandbox.local.local_sandbox_provider import LocalSandboxProvider

    config_path = tmp_path / "resource-config.yaml"
    config_path.write_text("storage_spaces:\n  enabled: true\n", encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(config_path))
    for name in ("_app_config", "_app_config_path", "_app_config_mtime", "_app_config_signature"):
        monkeypatch.setattr(app_config, name, None)
    monkeypatch.setattr(app_config, "_app_config_is_custom", False)
    # Isolate unrelated config subsystem initialization; exercise the real file/cache lifecycle.
    enabled = SimpleNamespace(storage_spaces=SimpleNamespace(enabled=True))
    monkeypatch.setattr(app_config.AppConfig, "_from_yaml_text", lambda *_args: enabled)
    override = app_config._current_app_config.set(None)
    try:
        assert app_config._load_and_cache_app_config(str(config_path)) is enabled
        config_path.unlink()
        with pytest.raises(FileNotFoundError):
            app_config.get_app_config()
        assert app_config.peek_loaded_app_config() is enabled

        def forbidden():
            pytest.fail("Missing configured storage must not resolve legacy paths")

        monkeypatch.setattr(paths, "get_paths", forbidden)
        monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.get_paths", forbidden)
        helper = AioSandboxProvider._get_thread_mounts if adapter == "aio" else LocalSandboxProvider._build_thread_path_mappings
        with pytest.raises(StorageUnsupported, match="writer fencing"):
            helper("thread", user_id="alice")
    finally:
        app_config._current_app_config.reset(override)
