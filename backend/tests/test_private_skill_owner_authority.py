"""Delegation admits an authenticated private owner without admin authority."""

from types import SimpleNamespace

import pytest
from _router_auth_helpers import make_authed_test_app
from fastapi.testclient import TestClient

from app.gateway.auth.models import User
from app.gateway.deps import get_config
from deerflow.config.app_config import AppConfig
from deerflow.config.paths import Paths
from deerflow.runtime.customer_administration import CustomerAdministrationPolicy, CustomerManagementActor, CustomerManagementDenied, bind_customer_management_actor
from deerflow.skills.storage.user_scoped_skill_storage import UserScopedSkillStorage


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source,role,owner_role,notification,allowed",
    [
        ("session", "user", None, False, True),
        ("session", "admin", None, False, True),
        ("pat", "admin", None, False, False),
        ("pat", "user", None, False, False),
        ("internal", "internal", None, False, False),
        ("internal", "internal", "user", False, True),
        ("internal", "internal", "admin", True, False),
    ],
)
async def test_private_owner_grant_is_minted_from_current_request(source, role, owner_role, notification, allowed):
    from app.gateway.customer_administration import resolve_customer_management_actor

    request = SimpleNamespace(state=SimpleNamespace(auth_source=source, user=SimpleNamespace(id="caller", system_role=role), customer_management_notification=notification))
    owner = SimpleNamespace(id="owner", system_role=owner_role) if owner_role else None
    actor = await resolve_customer_management_actor(request, internal_owner_user=owner)
    assert actor.private_skill_owner is allowed
    if role == "user":
        assert actor.administrator is False


@pytest.fixture
def owner_app(tmp_path, monkeypatch):
    from app.gateway.routers import skills
    from deerflow.skills.security_scanner import ScanResult

    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: Paths(base_dir=tmp_path / "home"))
    config = AppConfig.model_validate({"sandbox": {"use": "test"}, "skills": {"path": str(tmp_path / "skills")}})
    user = User(email="owner@example.com", password_hash="unused", system_role="user")
    store = UserScopedSkillStorage(str(user.id), app_config=config)
    original = "---\nname: owner-notes\ndescription: Private notes\n---\nOriginal notes.\n"
    store.write_custom_skill("owner-notes", "SKILL.md", original)
    app = make_authed_test_app(user_factory=lambda: user, bind_current_user=True, signed_in=True)
    app.dependency_overrides[get_config] = lambda: config
    app.state.customer_administration_policy = CustomerAdministrationPolicy(local_skill_management=True)
    app.include_router(skills.router)
    monkeypatch.setattr(skills, "_get_user_skill_storage", lambda _: store)

    async def scan(*args, **kwargs):
        return ScanResult(decision="allow", reason="Offline model decision")

    async def refresh(owner):
        assert owner == str(user.id)

    monkeypatch.setattr(skills, "scan_skill_content", scan)
    monkeypatch.setattr(skills, "refresh_user_skills_system_prompt_cache_async", refresh)
    return app, store, config, user


def test_delegated_nonadmin_edits_only_private_data_and_records_history(owner_app):
    app, store, _, _ = owner_app
    content = "---\nname: owner-notes\ndescription: Private notes\n---\nUpdated notes.\n"
    with TestClient(app) as client:
        response = client.put("/api/skills/custom/owner-notes", json={"content": content})
    assert response.status_code == 200, response.text
    assert store.read_custom_skill("owner-notes") == content
    assert store.read_history("owner-notes")[-1]["prev_content"].endswith("Original notes.\n")
    assert not (store.get_skills_root_path() / "custom" / "owner-notes").exists()


def test_private_read_survives_management_disable_but_mutation_does_not(owner_app):
    app, store, _, _ = owner_app
    app.state.customer_administration_policy = CustomerAdministrationPolicy()
    before = store.read_custom_skill("owner-notes")
    with TestClient(app) as client:
        assert client.get("/api/skills/custom/owner-notes").status_code == 200
        assert client.get("/api/skills/custom/owner-notes/history").status_code == 200
        assert client.delete("/api/skills/custom/owner-notes").status_code == 403
    assert store.read_custom_skill("owner-notes") == before


def test_effective_private_capability_does_not_grant_plugin_or_local_mcp_management(owner_app):
    from app.gateway.routers import features

    app, _, _, _ = owner_app
    app.include_router(features.router)
    with TestClient(app) as client:
        response = client.get("/api/features")
    assert response.status_code == 200, response.text
    capabilities = response.json()["customer_administration"]
    assert capabilities == {"local_skill_management": True, "plugin_management": False, "local_mcp_management": False, "provider_operations": False}


def test_typed_private_grant_is_owner_bound_and_does_not_make_an_admin():
    from deerflow.runtime.customer_administration import customer_management_actor_can_manage_private_skills, customer_management_actor_is_admin

    actor = CustomerManagementActor(owner_id="owner-a", private_skill_owner=True)
    assert customer_management_actor_can_manage_private_skills({"user_id": "owner-a", "__customer_management_actor": actor})
    assert not customer_management_actor_can_manage_private_skills({"user_id": "owner-b", "__customer_management_actor": actor})
    assert not customer_management_actor_can_manage_private_skills({"user_id": "owner-a", "__customer_management_actor": {"owner_id": "owner-a", "private_skill_owner": True}})
    assert not customer_management_actor_is_admin({"user_id": "owner-a", "__customer_management_actor": actor})


@pytest.mark.parametrize("suffix", ["", "/history", "/export-manifest"])
def test_private_reads_refuse_foreign_storage(owner_app, monkeypatch, tmp_path, suffix):
    from app.gateway.routers import skills

    app, _, config, _ = owner_app
    foreign = UserScopedSkillStorage("foreign-owner", app_config=config)
    foreign.write_custom_skill("owner-notes", "SKILL.md", "---\nname: owner-notes\ndescription: Other owner\n---\nPrivate foreign content.\n")
    monkeypatch.setattr(skills, "_get_user_skill_storage", lambda _: foreign)
    with TestClient(app) as client:
        response = client.get("/api/skills/custom/owner-notes" + suffix)
    assert response.status_code == 501, response.text
    assert "Private foreign content" not in response.text


def test_embedded_graph_assembly_drops_management_when_target_owner_changes(monkeypatch):
    from deerflow.client import DeerFlowClient
    from deerflow.runtime.customer_administration import bind_customer_management_actor, customer_management_actor_can_manage_private_skills

    client = DeerFlowClient.__new__(DeerFlowClient)
    client._customer_administration_policy = CustomerAdministrationPolicy(local_skill_management=True)
    observed = []
    monkeypatch.setattr(client, "_ensure_agent_with_policy", lambda *args, **kwargs: observed.append(customer_management_actor_can_manage_private_skills()))
    with bind_customer_management_actor(CustomerManagementActor(owner_id="owner-a", private_skill_owner=True)):
        client._ensure_agent({}, context={"user_id": "owner-a"})
        client._ensure_agent({}, context={"user_id": "owner-b"})
        assert customer_management_actor_can_manage_private_skills()
    assert observed == [True, False]


@pytest.mark.parametrize("method,suffix", [("GET", ""), ("GET", "/history"), ("GET", "/export-manifest"), ("PUT", ""), ("DELETE", "")])
def test_owner_grant_preserves_optional_skill_visibility_denial(owner_app, monkeypatch, method, suffix):
    from app.gateway.routers import skills

    app, store, _, user = owner_app
    provider = SimpleNamespace(filter_resources=lambda principal, resource_type, names: [])
    monkeypatch.setattr(skills, "resolve_skill_authorization", lambda *args, **kwargs: (provider, SimpleNamespace(user_id=str(user.id))))
    before = store.read_custom_skill("owner-notes")
    with TestClient(app) as client:
        response = client.request(method, "/api/skills/custom/owner-notes" + suffix, json={"content": before} if method == "PUT" else None)
    assert response.status_code == 404
    assert store.read_custom_skill("owner-notes") == before


def test_hidden_skill_toggle_does_not_touch_owner_state_or_cache(owner_app, monkeypatch):
    from app.gateway.routers import skills

    app, store, _, user = owner_app
    monkeypatch.setattr(skills, "resolve_skill_authorization", lambda *args, **kwargs: (SimpleNamespace(filter_resources=lambda *args: []), SimpleNamespace(user_id=str(user.id))))
    touched = []

    async def refresh(*args):
        touched.append("cache")

    monkeypatch.setattr(skills, "refresh_user_skills_system_prompt_cache_async", refresh)
    monkeypatch.setattr(store, "set_skill_enabled_state", lambda *args: touched.append("state"))
    with TestClient(app) as client:
        response = client.put("/api/skills/owner-notes", json={"enabled": False})
    assert response.status_code == 404
    assert touched == []


@pytest.mark.asyncio
async def test_conversational_creation_requires_explicit_baseline_override(owner_app, monkeypatch):
    import importlib

    from deerflow.skills.security_scanner import ScanResult
    from deerflow.tools.skill_manage_tool import _skill_manage_impl

    _, store, _, user = owner_app
    baseline = store.get_integrations_root() / "provider" / "office" / "provided-notes"
    baseline.mkdir(parents=True)
    content = "---\nname: provided-notes\ndescription: Notes\n---\nOwner version.\n"
    (baseline / "SKILL.md").write_text(content, encoding="utf-8")
    module = importlib.import_module("deerflow.tools.skill_manage_tool")
    monkeypatch.setattr(module, "get_or_new_user_skill_storage", lambda *args, **kwargs: store)

    async def scan(*args, **kwargs):
        return ScanResult(decision="allow", reason="Offline model decision")

    async def refresh(owner):
        assert owner == str(user.id)

    monkeypatch.setattr(module, "scan_skill_content", scan)
    monkeypatch.setattr(module, "refresh_user_skills_system_prompt_cache_async", refresh)
    runtime = SimpleNamespace(
        context={
            "user_id": str(user.id),
            "__customer_administration_policy": CustomerAdministrationPolicy(local_skill_management=True),
            "__customer_management_actor": CustomerManagementActor(owner_id=str(user.id), private_skill_owner=True),
        },
        config={},
    )
    with pytest.raises(ValueError, match="explicit"):
        await _skill_manage_impl(runtime, "create", "provided-notes", content=content)
    assert not store.custom_skill_exists("provided-notes")
    await _skill_manage_impl(runtime, "create", "provided-notes", content=content, allow_baseline_override=True)
    assert store.custom_skill_exists("provided-notes")


@pytest.mark.parametrize("operation", ["toggle", "install"])
def test_embedded_private_mutation_preserves_resource_visibility(owner_app, monkeypatch, tmp_path, operation):
    import zipfile

    from deerflow.authz.skill_filter import ResolvedSkillAuthorization
    from deerflow.client import DeerFlowClient

    _, store, config, user = owner_app
    client = DeerFlowClient.__new__(DeerFlowClient)
    client._app_config = config
    client._customer_administration_policy = CustomerAdministrationPolicy(local_skill_management=True)
    monkeypatch.setattr("deerflow.client.get_or_new_user_skill_storage", lambda *args, **kwargs: store)
    authorization = ResolvedSkillAuthorization(provider=SimpleNamespace(filter_resources=lambda *args: []), principal=object(), fail_closed=True)
    config.authorization.enabled = True
    monkeypatch.setattr("deerflow.authz.skill_filter.resolve_skill_authorization", lambda *args: authorization)
    archive = tmp_path / "hidden.skill"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("hidden/SKILL.md", "---\nname: hidden\ndescription: Owner notes\n---\nNotes.\n")
    before = store.read_custom_skill("owner-notes")
    with bind_customer_management_actor(CustomerManagementActor(owner_id=str(user.id), private_skill_owner=True)):
        with pytest.raises(CustomerManagementDenied, match="available"):
            if operation == "toggle":
                client.update_skill("owner-notes", enabled=False)
            else:
                client.install_skill(archive)
    assert store.read_custom_skill("owner-notes") == before
    assert store.get_skill_enabled_state("owner-notes")
    assert not store.custom_skill_exists("hidden")


@pytest.mark.asyncio
async def test_conversational_private_mutation_preserves_resource_visibility(owner_app, monkeypatch):
    import importlib

    from deerflow.authz.skill_filter import ResolvedSkillAuthorization
    from deerflow.tools.skill_manage_tool import _skill_manage_impl

    _, store, config, user = owner_app
    config.authorization.enabled = True
    authorization = ResolvedSkillAuthorization(provider=SimpleNamespace(filter_resources=lambda *args: []), principal=object(), fail_closed=True)
    monkeypatch.setattr("deerflow.authz.skill_filter.resolve_skill_authorization", lambda *args: authorization)
    module = importlib.import_module("deerflow.tools.skill_manage_tool")
    monkeypatch.setattr(module, "get_or_new_user_skill_storage", lambda *args, **kwargs: store)
    runtime = SimpleNamespace(
        context={
            "user_id": str(user.id),
            "__customer_administration_policy": CustomerAdministrationPolicy(local_skill_management=True),
            "__customer_management_actor": CustomerManagementActor(owner_id=str(user.id), private_skill_owner=True),
        },
        config={},
    )
    with pytest.raises(CustomerManagementDenied, match="available"):
        await _skill_manage_impl(runtime, "delete", "owner-notes")
    assert store.custom_skill_exists("owner-notes")


@pytest.mark.asyncio
async def test_cancelled_conversational_write_drains_history_cache_and_lock(owner_app, monkeypatch):
    import asyncio
    import importlib
    import threading

    from deerflow.skills.security_scanner import ScanResult

    _, store, _, user = owner_app
    module = importlib.import_module("deerflow.tools.skill_manage_tool")
    monkeypatch.setattr(module, "get_or_new_user_skill_storage", lambda *args, **kwargs: store)
    started, finish = threading.Event(), threading.Event()
    original_write = store.write_custom_skill
    content = store.read_custom_skill("owner-notes") + "Complete owner edit.\n"
    refreshed = []

    def write(*args, **kwargs):
        started.set()
        assert finish.wait(3)
        return original_write(*args, **kwargs)

    async def scan(*args, **kwargs):
        return ScanResult(decision="allow", reason="Offline model decision")

    async def refresh(owner):
        refreshed.append(owner)

    monkeypatch.setattr(store, "write_custom_skill", write)
    monkeypatch.setattr(module, "scan_skill_content", scan)
    monkeypatch.setattr(module, "refresh_user_skills_system_prompt_cache_async", refresh)
    runtime = SimpleNamespace(
        context={
            "user_id": str(user.id),
            "__customer_administration_policy": CustomerAdministrationPolicy(local_skill_management=True),
            "__customer_management_actor": CustomerManagementActor(owner_id=str(user.id), private_skill_owner=True),
        },
        config={},
    )
    task = asyncio.create_task(module._skill_manage_impl(runtime, "edit", "owner-notes", content=content))
    await asyncio.wait_for(asyncio.to_thread(started.wait, 3), 4)
    task.cancel()
    await asyncio.sleep(0.01)
    try:
        assert not task.done()
        assert module._get_lock(str(user.id), "owner-notes").locked()
    finally:
        finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert store.read_custom_skill("owner-notes") == content
    assert store.read_history("owner-notes")[-1]["action"] == "edit"
    assert refreshed == [str(user.id)]


@pytest.mark.asyncio
async def test_conversational_mutation_uses_host_config_instead_of_global_storage(owner_app, monkeypatch):
    import importlib

    from deerflow.authz.skill_filter import ResolvedSkillAuthorization

    _, original, config, user = owner_app
    host_config = config.model_copy(deep=True)
    host_config.authorization.enabled = True
    captured = UserScopedSkillStorage(str(user.id), app_config=host_config)
    module = importlib.import_module("deerflow.tools.skill_manage_tool")
    requested = []

    def storage(owner, **kwargs):
        requested.append(kwargs.get("app_config"))
        return captured if kwargs.get("app_config") is host_config else original

    def authorization(context, app_config):
        if app_config is host_config:
            return ResolvedSkillAuthorization(provider=SimpleNamespace(filter_resources=lambda *args: []), principal=object(), fail_closed=True)
        return None

    monkeypatch.setattr(module, "get_or_new_user_skill_storage", storage)
    monkeypatch.setattr("deerflow.authz.skill_filter.resolve_skill_authorization", authorization)
    runtime = SimpleNamespace(
        context={
            "user_id": str(user.id),
            "app_config": host_config,
            "__customer_administration_policy": CustomerAdministrationPolicy(local_skill_management=True),
            "__customer_management_actor": CustomerManagementActor(owner_id=str(user.id), private_skill_owner=True),
        },
        config={},
    )
    with pytest.raises(CustomerManagementDenied, match="available"):
        await module._skill_manage_impl(runtime, "delete", "owner-notes")
    assert requested == [host_config]
    assert original.custom_skill_exists("owner-notes")


def test_embedded_visibility_uses_verified_matching_owner_identity_attributes(owner_app, monkeypatch):
    from deerflow.authz.skill_filter import ResolvedSkillAuthorization
    from deerflow.client import DeerFlowClient
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    _, store, config, user = owner_app
    config.authorization.enabled = True
    client = DeerFlowClient.__new__(DeerFlowClient)
    client._app_config = config
    client._customer_administration_policy = CustomerAdministrationPolicy(local_skill_management=True)
    monkeypatch.setattr("deerflow.client.get_or_new_user_skill_storage", lambda *args, **kwargs: store)
    seen = []

    def resolve(context, app_config):
        seen.append(context)
        allowed = context.get("oauth_provider") == "oidc" and context.get("oauth_id") == "subject-42" and context.get("authz_attributes", {}).get("department") == "operations"
        return ResolvedSkillAuthorization(provider=SimpleNamespace(filter_resources=lambda principal, kind, names: names if allowed else []), principal=object(), fail_closed=True)

    monkeypatch.setattr("deerflow.authz.skill_filter.resolve_skill_authorization", resolve)
    verified = SimpleNamespace(id=user.id, system_role="user", oauth_provider="oidc", oauth_id="subject-42", authz_attributes={"department": "operations"})
    token = set_current_user(verified)
    try:
        with bind_customer_management_actor(CustomerManagementActor(owner_id=str(user.id), private_skill_owner=True)):
            client.update_skill("owner-notes", enabled=False)
    finally:
        reset_current_user(token)
    assert seen[0]["user_id"] == str(user.id)
    assert not store.get_skill_enabled_state("owner-notes")


def test_request_owner_without_matching_runtime_identity_cannot_read_default_store(owner_app, monkeypatch):
    from app.gateway.routers import skills
    from deerflow.runtime.customer_administration import bind_customer_management_actor

    _, _, config, user = owner_app
    default = UserScopedSkillStorage("default", app_config=config)
    default.write_custom_skill("private-default", "SKILL.md", "---\nname: private-default\ndescription: Default owner notes\n---\nPrivate default content.\n")
    app = make_authed_test_app(user_factory=lambda: user, signed_in=True)
    app.dependency_overrides[get_config] = lambda: config
    app.include_router(skills.router)
    monkeypatch.setattr(skills, "_get_user_skill_storage", lambda _: default)
    monkeypatch.setattr("deerflow.runtime.user_context.get_current_user", lambda: None)
    monkeypatch.setattr(skills, "get_effective_user_id", lambda: "default")
    with bind_customer_management_actor(None), TestClient(app) as client:
        response = client.get("/api/skills/custom/private-default")
    assert response.status_code == 501
    assert "Private default content" not in response.text
