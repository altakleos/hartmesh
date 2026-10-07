"""Feature discovery describes admitted operations, not configured delegation."""

from types import SimpleNamespace

import pytest

from deerflow.config.app_config import AppConfig
from deerflow.runtime.customer_administration import CustomerAdministrationPolicy


@pytest.mark.asyncio
@pytest.mark.parametrize("admin", [False, True])
async def test_missing_startup_policy_denies_management_discovery(admin, monkeypatch):
    from app.gateway.routers import features

    async def actor(request):
        return SimpleNamespace(administrator=admin)

    monkeypatch.setattr(features, "resolve_customer_management_actor", actor)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    configured = AppConfig.model_validate({"sandbox": {"use": "test"}, "customer_administration": {"plugin_management": True, "local_skill_management": True, "local_mcp_management": True}})
    result = await features._customer_administration_feature(request, configured)
    assert result.model_dump() == {"plugin_management": False, "local_skill_management": False, "local_mcp_management": False, "provider_operations": False}


@pytest.mark.asyncio
async def test_flags_without_owner_storage_or_supported_plugins_stay_unavailable(monkeypatch):
    from app.gateway.routers import features

    async def actor(request):
        return SimpleNamespace(administrator=True)

    monkeypatch.setattr(features, "resolve_customer_management_actor", actor)
    monkeypatch.setattr("app.gateway.routers.skills._get_owned_private_skill_storage", lambda config: (_ for _ in ()).throw(ValueError("unsupported storage")))
    policy = CustomerAdministrationPolicy(plugin_management=True, local_skill_management=True, local_mcp_management=True)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(customer_administration_policy=policy)))
    result = await features._customer_administration_feature(request, AppConfig.model_validate({"sandbox": {"use": "test"}}))
    assert not result.plugin_management
    assert not result.local_skill_management
    assert not result.local_mcp_management  # no approved local launch definitions
    assert not result.provider_operations


@pytest.mark.asyncio
async def test_linked_private_root_is_not_advertised_as_writable(tmp_path, monkeypatch):
    from support.symlinks import symlink_or_skip

    from app.gateway.routers import features
    from deerflow.config.paths import Paths
    from deerflow.skills.storage.user_scoped_skill_storage import UserScopedSkillStorage

    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: Paths(base_dir=tmp_path))
    store = UserScopedSkillStorage("owner", host_path=str(tmp_path / "skills"))
    external = tmp_path / "provider-root"
    external.mkdir()
    store.get_user_custom_root().parent.mkdir(parents=True)
    symlink_or_skip(store.get_user_custom_root(), external, target_is_directory=True)
    monkeypatch.setattr("app.gateway.routers.skills._get_owned_private_skill_storage", lambda config: store)

    async def actor(request):
        return SimpleNamespace(administrator=True)

    monkeypatch.setattr(features, "resolve_customer_management_actor", actor)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(customer_administration_policy=CustomerAdministrationPolicy(local_skill_management=True))))
    result = await features._customer_administration_feature(request, AppConfig.model_validate({"sandbox": {"use": "test"}}))
    assert result.local_skill_management is False
