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
