"""Live-owner preparation keeps cold configuration/provider work off SQL locks."""

import asyncio
import sys
import threading
from pathlib import Path

import pytest
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import db as db
from test_runtime_lifecycle_e2e import _preserve_process_config_singletons

from app.gateway import authz
from app.gateway.auth.local_provider import LocalAuthProvider
from app.telegram_bots import authority
from deerflow.config import app_config


@pytest.fixture
def cold_owner_config(tmp_path, monkeypatch):
    from deerflow.config import authorization_config, blob_storage_config, typesafe_config
    from deerflow.runtime.checkpointer import provider as checkpointers
    from deerflow.runtime.store import provider as stores

    _preserve_process_config_singletons(monkeypatch)
    for module_object, name in ((authorization_config, "_authorization_config"), (blob_storage_config, "_blob_storage_config"), (typesafe_config, "_typesafe_config")):
        monkeypatch.setattr(module_object, name, getattr(module_object, name))
    # Actual cold loading may reset these providers. Detach prior resources
    # first so the loader cannot close another test's singleton, then restore.
    for module_object, names in ((checkpointers, ("_checkpointer", "_checkpointer_ctx", "_checkpointer_cache", "_checkpointer_cache_prefix")), (stores, ("_store", "_store_ctx"))):
        for name in names:
            monkeypatch.setattr(module_object, name, None)
    module = "telegram_live_owner_cold_provider"
    missing = object()
    previous = sys.modules.pop(module, missing)
    (tmp_path / (module + ".py")).write_text(
        "import asyncio,threading\n"
        "from deerflow.authz.provider import AuthzDecision\n"
        "class Provider:\n"
        " name='cold-live-owner'\n"
        " def __init__(self):\n"
        "  self.loop=asyncio.get_running_loop(); self.thread=threading.get_ident(); self.calls=[]\n"
        " def authorize(self,request): raise AssertionError('async provider required')\n"
        " async def aauthorize(self,request):\n"
        "  assert asyncio.get_running_loop() is self.loop\n"
        "  self.calls.append(request); return AuthzDecision(allow=True)\n"
        " def filter_resources(self,principal,resource_type,candidates): return candidates\n",
        encoding="utf-8",
    )
    path = tmp_path / "config.yaml"
    path.write_text(f"config_version: 55\nsandbox:\n  use: deerflow.sandbox.local:LocalSandboxProvider\nauthorization:\n  enabled: true\n  fail_closed: false\n  provider:\n    use: {module}:Provider\n", encoding="utf-8")
    extensions = tmp_path / "extensions.json"
    extensions.write_text("{}", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(path))
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(extensions))
    for key, value in {"_app_config": None, "_app_config_path": None, "_app_config_mtime": None, "_app_config_signature": None, "_app_config_is_custom": False}.items():
        monkeypatch.setattr(app_config, key, value)
    monkeypatch.setattr(authz, "_route_provider_cache", {})
    monkeypatch.setattr(authz, "_route_provider_config_id", None)
    monkeypatch.setattr(authz, "_route_provider_config_sig", None)
    try:
        yield path
    finally:
        sys.modules.pop(module, None)
        if previous is not missing:
            sys.modules[module] = previous


@pytest.mark.asyncio
async def test_live_owner_uses_cold_disk_config_and_original_provider_loop(db, monkeypatch, cold_owner_config):
    real_config = app_config.get_app_config
    owner = await install_live_owner(db, monkeypatch)
    monkeypatch.setattr(app_config, "get_app_config", real_config)
    monkeypatch.setattr(authority, "get_local_provider", lambda: LocalAuthProvider(owner.users))
    loop, thread = asyncio.get_running_loop(), threading.get_ident()
    checker = await owner.authority.prepare_live_owner(owner.id)
    assert checker.email == "ingress@example.com"
    provider = authz._route_provider_cache["provider"]
    assert provider.loop is loop and provider.thread == thread and provider.calls
    assert Path(sys.modules[provider.__class__.__module__].__file__).parent == cold_owner_config.parent

    def no_configuration():
        pytest.fail("borrowed owner read reloaded configuration")

    async def no_provider(*args, **kwargs):
        pytest.fail("borrowed owner read reevaluated provider")

    monkeypatch.setattr(app_config, "get_app_config", no_configuration)
    monkeypatch.setattr(authz, "resolve_route_permissions", no_provider)
    async with db.sessions.begin() as writer:
        await checker(owner.id, writer)
        assert writer.in_transaction()
