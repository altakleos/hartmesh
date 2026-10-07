"""Conversational mutation is denied before storage or scanning admission."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["create", "patch", "edit", "delete", "write_file", "remove_file"])
async def test_skill_tool_cannot_infer_permission_from_admin_or_internal_context(action, monkeypatch):
    import importlib

    from deerflow.runtime.customer_administration import CustomerManagementDenied
    from deerflow.tools.skill_manage_tool import _skill_manage_impl

    module = importlib.import_module("deerflow.tools.skill_manage_tool")
    storage = Mock(side_effect=AssertionError("Denied conversational mutation reached storage"))
    monkeypatch.setattr(module, "get_or_new_user_skill_storage", storage)
    runtime = SimpleNamespace(context={"user_id": "owner", "user_role": "admin", "is_internal": True}, config={})
    with pytest.raises(CustomerManagementDenied):
        await _skill_manage_impl(runtime, action, "example", content="Draft", path="scripts/example.py", find="old", replace="new")
    storage.assert_not_called()


@pytest.mark.parametrize("foreign_owner", [None, "another-owner"])
def test_embedded_install_rejects_shared_or_foreign_storage_before_extraction(tmp_path, monkeypatch, foreign_owner):
    from deerflow.client import DeerFlowClient
    from deerflow.config.paths import Paths
    from deerflow.runtime.customer_administration import CustomerAdministrationPolicy, CustomerManagementDenied, bind_customer_management_actor_role
    from deerflow.skills.storage.local_skill_storage import LocalSkillStorage
    from deerflow.skills.storage.user_scoped_skill_storage import UserScopedSkillStorage

    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: Paths(base_dir=tmp_path))
    storage = LocalSkillStorage(host_path=str(tmp_path / "skills")) if foreign_owner is None else UserScopedSkillStorage(foreign_owner, host_path=str(tmp_path / "skills"))
    archive = tmp_path / "draft.skill"
    archive.write_bytes(b"inert fixture")
    client = DeerFlowClient.__new__(DeerFlowClient)
    client._customer_administration_policy = CustomerAdministrationPolicy(local_skill_management=True)
    client._app_config = SimpleNamespace()
    monkeypatch.setattr("deerflow.client.get_or_new_user_skill_storage", lambda *args, **kwargs: storage)
    extraction = Mock(side_effect=AssertionError("Denied storage reached extraction"))
    monkeypatch.setattr(storage, "install_skill_from_archive", extraction)
    with bind_customer_management_actor_role("admin"), pytest.raises(CustomerManagementDenied):
        client.install_skill(archive)
    extraction.assert_not_called()


@pytest.mark.parametrize("current_owner", [None, "different-owner"])
def test_embedded_private_install_uses_bound_actor_owner(current_owner, tmp_path, monkeypatch):
    from deerflow.client import DeerFlowClient
    from deerflow.config.paths import Paths
    from deerflow.runtime.customer_administration import CustomerAdministrationPolicy, CustomerManagementActor, bind_customer_management_actor
    from deerflow.runtime.user_context import reset_current_user, set_current_user
    from deerflow.skills.storage.user_scoped_skill_storage import UserScopedSkillStorage

    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: Paths(base_dir=tmp_path))
    requested = []
    storage = UserScopedSkillStorage("trusted-owner", host_path=str(tmp_path / "skills"))
    install = Mock(return_value={"success": True})
    monkeypatch.setattr(storage, "install_skill_from_archive", install)

    def lookup(owner, **kwargs):
        requested.append(owner)
        return storage

    monkeypatch.setattr("deerflow.client.get_or_new_user_skill_storage", lookup)
    client = DeerFlowClient.__new__(DeerFlowClient)
    client._app_config = SimpleNamespace()
    client._customer_administration_policy = CustomerAdministrationPolicy(local_skill_management=True)
    user = None if current_owner is None else SimpleNamespace(id=current_owner, system_role="user")
    token = set_current_user(user)
    try:
        with bind_customer_management_actor(CustomerManagementActor(owner_id="trusted-owner", administrator=True)):
            assert client.install_skill(tmp_path / "draft.skill") == {"success": True}
        assert requested == ["trusted-owner"]
        install.assert_called_once()
    finally:
        reset_current_user(token)


@pytest.mark.parametrize("owner_scoped", [False, True])
def test_private_management_discovery_requires_supported_owned_storage(owner_scoped, tmp_path, monkeypatch):
    from deerflow.config.paths import Paths
    from deerflow.runtime.customer_administration import CustomerAdministrationPolicy, CustomerManagementActor, bind_customer_administration_policy, bind_customer_management_actor, private_skill_management_available
    from deerflow.skills.storage.local_skill_storage import LocalSkillStorage
    from deerflow.skills.storage.user_scoped_skill_storage import UserScopedSkillStorage

    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: Paths(base_dir=tmp_path))
    storage = UserScopedSkillStorage("owner", host_path=str(tmp_path / "skills")) if owner_scoped else LocalSkillStorage(host_path=str(tmp_path / "skills"))
    monkeypatch.setattr("deerflow.skills.storage.get_or_new_user_skill_storage", lambda *args, **kwargs: storage)
    with bind_customer_administration_policy(CustomerAdministrationPolicy(local_skill_management=True)), bind_customer_management_actor(CustomerManagementActor(owner_id="owner", administrator=True)):
        assert private_skill_management_available(SimpleNamespace()) is owner_scoped
