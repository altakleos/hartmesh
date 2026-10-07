"""Management admission cannot be granted by admin or optional RBAC alone."""

from unittest.mock import Mock

import pytest
from _router_auth_helpers import make_authed_test_app
from fastapi.testclient import TestClient

from app.gateway.auth.models import User
from app.gateway.deps import get_config
from app.gateway.routers import mcp, personal_mcp, skills
from deerflow.config.app_config import AppConfig


@pytest.mark.parametrize("role", ["admin", "user"])
@pytest.mark.parametrize("authorization", [False, True])
@pytest.mark.parametrize(
    "method,path,body",
    [
        ("PUT", "/api/skills/custom/example", {"content": "---\nname: example\ndescription: Example\n---\nRead only."}),
        ("DELETE", "/api/skills/custom/example", None),
        ("POST", "/api/skills/reload", None),
        ("PUT", "/api/skills/example", {"enabled": True}),
    ],
)
def test_missing_provider_policy_denies_skill_mutation_before_storage(role, authorization, method, path, body, monkeypatch):
    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "authorization": {"enabled": authorization}})
    user = User(email="policy@example.com", password_hash="unused", system_role=role)
    app = make_authed_test_app(user_factory=lambda: user, bind_current_user=True, signed_in=True)
    app.dependency_overrides[get_config] = lambda: config
    app.include_router(skills.router)
    storage = Mock(side_effect=AssertionError("Denied mutation reached storage"))
    refresh = Mock(side_effect=AssertionError("Denied reload reached cache"))
    monkeypatch.setattr(skills, "_get_user_skill_storage", storage)
    monkeypatch.setattr(skills, "refresh_skills_system_prompt_cache_async", refresh)
    with TestClient(app) as client:
        response = client.request(method, path, json=body)
    assert response.status_code == 403
    storage.assert_not_called()
    refresh.assert_not_called()


@pytest.mark.parametrize("role", ["admin", "user"])
@pytest.mark.parametrize(
    "method,path,body,worker",
    [
        ("PUT", "/api/mcp/config", {"mcp_servers": {"example": {"type": "stdio", "command": "npx", "args": ["example-server@1.0.0"]}}}, "_apply_mcp_config_update"),
        ("POST", "/api/mcp/config/servers", {"mcp_servers": {"example": {"type": "stdio", "command": "npx", "args": ["example-server@1.0.0"]}}}, "_apply_mcp_servers_create"),
        ("PUT", "/api/mcp/config/server", {"server_name": "example", "server": {"type": "stdio", "command": "npx", "args": ["example-server@1.0.0"]}}, "_apply_mcp_server_config_update"),
        ("PATCH", "/api/mcp/config", {"server_name": "example", "enabled": True}, "_apply_mcp_server_state_update"),
        ("DELETE", "/api/mcp/config/servers/example", None, "_apply_mcp_server_delete"),
    ],
)
def test_default_policy_denies_deployment_mcp_before_mutation(role, method, path, body, worker, monkeypatch, tmp_path):
    user = User(email="policy@example.com", password_hash="unused", system_role=role)
    app = make_authed_test_app(user_factory=lambda: user, bind_current_user=True, signed_in=True)
    app.include_router(mcp.router)
    import json

    store = tmp_path / "extensions_config.json"
    existing_name = "existing-example" if method == "POST" else "example"
    store.write_text(json.dumps({"mcpServers": {existing_name: {"type": "stdio", "command": "npx", "args": ["example-server@1.0.0"], "enabled": False}}, "skills": {}}), encoding="utf-8")
    before = store.read_bytes()
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(store))
    mutation = Mock(side_effect=AssertionError("Denied MCP mutation reached persistence"))
    monkeypatch.setattr(mcp, "atomic_write_extensions_config", mutation)
    with TestClient(app) as client:
        response = client.request(method, path, json=body)
    assert response.status_code == 403
    mutation.assert_not_called()
    assert store.read_bytes() == before


@pytest.mark.parametrize("role", ["admin", "user"])
def test_default_policy_denies_personal_local_definition_creation(role, monkeypatch, tmp_path):
    user = User(email="policy@example.com", password_hash="unused", system_role=role)
    app = make_authed_test_app(user_factory=lambda: user, bind_current_user=True, signed_in=True)
    app.include_router(personal_mcp.router)
    monkeypatch.setattr(personal_mcp, "user_mcp_config_path", lambda owner: tmp_path / owner / "mcp.json")
    monkeypatch.setattr(personal_mcp, "_read_personal_config", lambda owner: {"mcpServers": {}})
    mutation = Mock(side_effect=AssertionError("Denied personal mutation reached persistence"))
    monkeypatch.setattr(personal_mcp, "atomic_write_extensions_config", mutation)
    with TestClient(app) as client:
        response = client.post("/api/mcp/personal/config/servers", json={"mcp_servers": {"example": {"type": "stdio", "command": "npx", "args": ["example-server@1.0.0"]}}})
    assert response.status_code == 403
    mutation.assert_not_called()


@pytest.mark.parametrize("role", ["admin", "user"])
def test_default_policy_preserves_personal_remote_connections(role, monkeypatch, tmp_path):
    user = User(email="policy@example.com", password_hash="unused", system_role=role)
    app = make_authed_test_app(user_factory=lambda: user, bind_current_user=True, signed_in=True)
    app.include_router(personal_mcp.router)
    store = tmp_path / "personal.json"
    monkeypatch.setattr(personal_mcp, "user_mcp_config_path", lambda owner: store)
    monkeypatch.setattr(personal_mcp, "_read_personal_config", lambda owner: {"mcpServers": {}})
    with TestClient(app) as client:
        response = client.post("/api/mcp/personal/config/servers", json={"mcp_servers": {"example": {"type": "http", "url": "https://example.com/mcp"}}})
    assert response.status_code == 200
    assert store.is_file()


@pytest.mark.parametrize("foreign_owner", [None, "another-owner"])
def test_private_storage_admission_rejects_shared_or_foreign_storage(foreign_owner, tmp_path, monkeypatch):
    from deerflow.config.paths import Paths
    from deerflow.runtime.user_context import get_effective_user_id
    from deerflow.skills.storage.local_skill_storage import LocalSkillStorage
    from deerflow.skills.storage.user_scoped_skill_storage import UserScopedSkillStorage

    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: Paths(base_dir=tmp_path))
    storage = LocalSkillStorage(host_path=str(tmp_path / "skills")) if foreign_owner is None else UserScopedSkillStorage(foreign_owner, host_path=str(tmp_path / "skills"))
    assert foreign_owner != get_effective_user_id()
    monkeypatch.setattr(skills, "_get_user_skill_storage", lambda config: storage)
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as denied:
        skills._get_owned_private_skill_storage(AppConfig.model_validate({"sandbox": {"use": "test"}}))
    assert denied.value.status_code == 501


@pytest.mark.parametrize("path", ["/api/mcp/cache/reset", "/api/integrations/lark/install"])
def test_global_reload_and_managed_pack_install_remain_provider_only(path, monkeypatch):
    from app.gateway.routers import integrations
    from deerflow.runtime.customer_administration import CustomerAdministrationPolicy

    user = User(email="provider-only@example.com", password_hash="unused", system_role="admin")
    app = make_authed_test_app(user_factory=lambda: user, bind_current_user=True, signed_in=True)
    app.state.customer_administration_policy = CustomerAdministrationPolicy(local_skill_management=True, local_mcp_management=True)
    app.include_router(mcp.router)
    app.include_router(integrations.router)
    app.dependency_overrides[get_config] = lambda: AppConfig.model_validate({"sandbox": {"use": "test"}})
    write = Mock(side_effect=AssertionError("Provider-only HTTP operation reached mutation"))
    monkeypatch.setattr(mcp, "publish_mcp_tools_cache_reset", write)
    monkeypatch.setattr(integrations, "install_lark_integration", write)
    with TestClient(app) as client:
        response = client.post(path)
    assert response.status_code == 403
    write.assert_not_called()


def test_request_without_an_app_scope_has_denied_policy():
    from starlette.requests import Request

    from app.gateway.customer_administration import request_customer_administration_policy
    from deerflow.runtime.customer_administration import DENIED_CUSTOMER_ADMINISTRATION

    assert request_customer_administration_policy(Request({"type": "http"})) == DENIED_CUSTOMER_ADMINISTRATION
