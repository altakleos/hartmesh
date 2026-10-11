import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from test_agent_instances import instances as instances
from test_storage_spaces_startup import startup as startup

from app.gateway.deps import get_config
from app.gateway.routers import features
from deerflow.config.ui_config import StarterConfig, UiConfig


def _app_with_config(
    *,
    agents_api_enabled: bool,
    browser_enabled: bool = False,
    browser_extra: dict | None = None,
    mcp_tasks_available: bool = False,
    subagent_batches_available: bool = False,
    subagent_batch_repo_available: bool | None = None,
    conversation_references_enabled: bool = False,
    knowledge_base_enabled: bool = False,
    scope_selection_enabled: bool = False,
    knowledge_search_provider: str | None = None,
    ui: UiConfig | None = None,
    tenant_bundle_path: str | None = None,
) -> FastAPI:
    app = FastAPI()
    app.state.mcp_tasks_available = mcp_tasks_available
    app.state.subagent_batches_available = subagent_batches_available
    if subagent_batch_repo_available is None:
        subagent_batch_repo_available = subagent_batches_available
    app.state.subagent_batch_repo = object() if subagent_batch_repo_available else None
    app.include_router(features.router)
    tools = []
    if browser_enabled:
        tools.append(SimpleNamespace(name="browser_navigate", use="deerflow.community.browser:browser_navigate_tool", model_extra=browser_extra or {}))
    if conversation_references_enabled:
        tools.append(SimpleNamespace(name="read_conversation", use="deerflow.tools.conversation:read_conversation", model_extra={}))
    fake_config = SimpleNamespace(
        agents_api=SimpleNamespace(enabled=agents_api_enabled),
        tools=tools,
        subagent_runtime=SimpleNamespace(max_running=3),
        knowledge_base=SimpleNamespace(
            enabled=knowledge_base_enabled,
            scope_selection_enabled=scope_selection_enabled,
        ),
        ui=ui if ui is not None else UiConfig(),
        tenant_bundle=SimpleNamespace(path=tenant_bundle_path),
    )
    search_tool = SimpleNamespace(use=knowledge_search_provider) if knowledge_search_provider is not None else None
    fake_config.get_tool_config = lambda name: search_tool if name == "knowledge_search" else None
    app.dependency_overrides[get_config] = lambda: fake_config
    return app


NO_BRANDING = {"company_name": None, "colors": {"primary": None, "secondary": None}, "has_logo": False, "provider": {"display_name": None, "support_url": None}}


def _default_ui_payload() -> dict:
    """What a deployment that configured no presentation reports."""
    return {
        "profile": "developer",
        "starters": [{"id": starter.id, "title": starter.title, "prompt": starter.prompt} for starter in UiConfig().starters],
    }


def test_features_reports_agents_api_enabled(monkeypatch) -> None:
    monkeypatch.setattr(features, "local_registration_open", lambda: False)
    with TestClient(_app_with_config(agents_api_enabled=True)) as client:
        response = client.get("/api/features")
    assert response.status_code == 200
    assert response.json() == {
        "storage_spaces": {"enabled": False, "backend": None, "native_attachments": False, "editor_concurrency": None, "user_spaces": False, "state": "off", "reason": None, "own": "ready"},
        "accounts": {"seats": {"allowed": 0, "in_use": 0}},
        "agent_instances": {"enabled": False, "execution": "unavailable", "reason": None, "ai_assistants": {"per_user": 0, "allowed": 0, "in_use": 0, "mine": 0}, "ai_employees": {"allowed": 0, "in_use": 0}},
        "people_search": "prefix",
        "customer_administration": {"plugin_management": False, "local_skill_management": False, "local_mcp_management": False, "provider_operations": False},
        "agents_api": {"enabled": True},
        "browser_control": {"enabled": False},
        "mcp_tasks": {"enabled": False},
        "subagent_batches": {
            "enabled": False,
            "repository_available": False,
            "worker_running": False,
            "max_running": 3,
        },
        "conversation_references": {"enabled": False, "max_references": 3},
        "knowledge_base": {
            "scope_selection_enabled": False,
        },
        "ui": _default_ui_payload(),
        "branding": NO_BRANDING,
    }


async def _managed_features(startup, *, user_id="alice", source="session", attributed_owner=None):
    import httpx

    from app.gateway.internal_auth import INTERNAL_OWNER_USER_ID_HEADER_NAME, INTERNAL_SYSTEM_ROLE

    ss, state_app, config, sf, _, _, _ = startup
    if not hasattr(state_app.state, "agent_execution"):
        await ss.initialize_storage_spaces(state_app, config, session_factory=sf)
    app = _app_with_config(agents_api_enabled=True)
    app.state = state_app.state

    @app.middleware("http")
    async def identity(request, call_next):
        request.state.user = SimpleNamespace(id=user_id, system_role=INTERNAL_SYSTEM_ROLE if source == "internal" else "user") if user_id else None
        request.state.auth_source = source
        return await call_next(request)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/features", headers={INTERNAL_OWNER_USER_ID_HEADER_NAME: attributed_owner} if attributed_owner else {})
    assert response.status_code == 200
    return response.json()


@pytest.mark.asyncio
async def test_features_report_storage_allowances_and_only_the_callers_assistants(startup):
    from _storage_spaces_test_support import ALICE, operation
    from test_agent_instances import definition

    agents = startup[-1]
    await agents._enroll(ALICE, {"name": "Pending fixture", "custody": "personal", "supervisor_id": "alice"}, definition(), operation())
    response = await _managed_features(startup)
    assert response["storage_spaces"] == {"enabled": True, "backend": "fixed-ext4", "native_attachments": False, "editor_concurrency": "shared", "user_spaces": False, "state": "ready", "reason": None, "own": "not-assigned"}
    assert response["accounts"]["seats"] == {"allowed": 1, "in_use": 2}
    assert response["agent_instances"] == {
        "enabled": True,
        "execution": "ready",
        "reason": None,
        "ai_assistants": {"per_user": 1, "allowed": 1, "in_use": 1, "mine": 1},
        "ai_employees": {"allowed": 1, "in_use": 0},
    }
    other = await _managed_features(startup, user_id="bob")
    assert other["agent_instances"]["ai_assistants"]["mine"] == 0
    assert other["agent_instances"]["ai_assistants"]["in_use"] == 1
    assert response["agents_api"] == {"enabled": True}


@pytest.mark.parametrize("source", ["pat", "internal", None])
@pytest.mark.asyncio
async def test_features_do_not_project_a_person_from_unsupported_identity(startup, source):
    response = await _managed_features(startup, user_id="default", source=source)
    assert response["agent_instances"]["ai_assistants"]["mine"] == 0
    assert response["storage_spaces"]["own"] == "ready"


@pytest.mark.asyncio
async def test_features_report_assigned_unverified_and_no_free_slot_without_reallocating(startup, monkeypatch):
    from _storage_spaces_test_support import ALICE

    from deerflow.spaces.backings import BackingUnavailable
    from deerflow.spaces.contract import Custody, MutationMode

    ss, app, config, sf, catalog, _, _ = startup
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    files = app.state.storage_spaces
    await files.assign(kind="user", actor=ALICE, name="User fixture", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    response = await _managed_features(startup)
    assert response["storage_spaces"]["own"] == "ready"
    response = await _managed_features(startup, user_id="bob")
    assert response["storage_spaces"]["own"] == "no-seat"

    def unavailable(*args, **kwargs):
        raise BackingUnavailable("fixture mount missing")

    monkeypatch.setattr(catalog, "verify", unavailable)
    response = await _managed_features(startup)
    assert response["storage_spaces"]["own"] == "unverified"
    app.state.agent_execution.storage_reason = "admission-failed"
    response = await _managed_features(startup)
    assert response["storage_spaces"]["own"] == "ready"
    assert response["agent_instances"]["reason"] == "storage-unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["execution_state", "allowance"])
async def test_features_fail_closed_when_projection_fails(startup, monkeypatch, failure):
    from unittest.mock import AsyncMock

    ss, app, config, sf, _, _, _ = startup
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    monkeypatch.setattr(ss, failure, AsyncMock(side_effect=RuntimeError("fixture state unavailable")))
    response = await _managed_features(startup)
    assert response["storage_spaces"]["state"] == "unavailable"
    assert response["agent_instances"]["enabled"] is False
    assert response["agent_instances"]["execution"] == "unavailable"
    assert response["accounts"]["seats"] == {"allowed": 0, "in_use": 0}
    assert response["agents_api"] == {"enabled": True}


@pytest.mark.parametrize("source", ["auth_disabled", "internal"])
@pytest.mark.asyncio
async def test_features_project_explicit_development_and_attributed_internal_owners(startup, source):
    from _storage_spaces_test_support import ALICE, operation
    from test_agent_instances import definition

    await startup[-1]._enroll(ALICE, {"name": "Pending fixture", "custody": "personal", "supervisor_id": "alice"}, definition(), operation())
    response = await _managed_features(startup, user_id="alice" if source == "auth_disabled" else "default", source=source, attributed_owner="alice" if source == "internal" else None)
    assert response["agent_instances"]["ai_assistants"]["mine"] == 1
    assert response["storage_spaces"]["own"] == "not-assigned"


@pytest.mark.parametrize("registration_open,expected", [(True, "exact"), (False, "prefix")])
def test_features_people_search_follows_local_registration(monkeypatch, registration_open, expected):
    monkeypatch.setattr(features, "local_registration_open", lambda: registration_open)
    with TestClient(_app_with_config(agents_api_enabled=False)) as client:
        response = client.get("/api/features")
    assert response.json()["people_search"] == expected


def test_features_reports_agents_api_disabled(monkeypatch) -> None:
    monkeypatch.setattr(features, "local_registration_open", lambda: False)
    with TestClient(_app_with_config(agents_api_enabled=False)) as client:
        response = client.get("/api/features")
    assert response.status_code == 200
    assert response.json() == {
        "storage_spaces": {"enabled": False, "backend": None, "native_attachments": False, "editor_concurrency": None, "user_spaces": False, "state": "off", "reason": None, "own": "ready"},
        "accounts": {"seats": {"allowed": 0, "in_use": 0}},
        "agent_instances": {"enabled": False, "execution": "unavailable", "reason": None, "ai_assistants": {"per_user": 0, "allowed": 0, "in_use": 0, "mine": 0}, "ai_employees": {"allowed": 0, "in_use": 0}},
        "people_search": "prefix",
        "customer_administration": {"plugin_management": False, "local_skill_management": False, "local_mcp_management": False, "provider_operations": False},
        "agents_api": {"enabled": False},
        "browser_control": {"enabled": False},
        "mcp_tasks": {"enabled": False},
        "subagent_batches": {
            "enabled": False,
            "repository_available": False,
            "worker_running": False,
            "max_running": 3,
        },
        "conversation_references": {"enabled": False, "max_references": 3},
        "knowledge_base": {
            "scope_selection_enabled": False,
        },
        "ui": _default_ui_payload(),
        "branding": NO_BRANDING,
    }


def test_features_reports_conversation_references_when_the_tool_is_configured() -> None:
    with TestClient(_app_with_config(agents_api_enabled=True, conversation_references_enabled=True)) as client:
        response = client.get("/api/features")
    assert response.status_code == 200
    assert response.json()["conversation_references"] == {"enabled": True, "max_references": 3}


def test_features_enables_scope_selection_only_for_exact_ragflow_provider() -> None:
    with TestClient(
        _app_with_config(
            agents_api_enabled=True,
            knowledge_base_enabled=True,
            scope_selection_enabled=True,
            knowledge_search_provider=("deerflow.community.ragflow.tools:knowledge_search_tool"),
        )
    ) as client:
        response = client.get("/api/features")

    assert response.status_code == 200
    assert response.json()["knowledge_base"]["scope_selection_enabled"] is True


@pytest.mark.parametrize(
    ("knowledge_base_enabled", "provider"),
    [
        (False, "deerflow.community.ragflow.tools:knowledge_search_tool"),
        (True, "deerflow.community.lightrag.tools:knowledge_search_tool"),
        (True, "custom.provider:knowledge_search_tool"),
        (True, None),
    ],
)
def test_features_scope_selection_fails_closed(
    knowledge_base_enabled: bool,
    provider: str | None,
) -> None:
    with TestClient(
        _app_with_config(
            agents_api_enabled=True,
            knowledge_base_enabled=knowledge_base_enabled,
            scope_selection_enabled=True,
            knowledge_search_provider=provider,
        )
    ) as client:
        response = client.get("/api/features")

    assert response.status_code == 200
    assert response.json()["knowledge_base"]["scope_selection_enabled"] is False


def test_features_reports_mcp_tasks_startup_capability() -> None:
    with TestClient(_app_with_config(agents_api_enabled=True, mcp_tasks_available=True)) as client:
        response = client.get("/api/features")
    assert response.status_code == 200
    assert response.json()["mcp_tasks"] == {"enabled": True}


def test_features_reports_subagent_batch_startup_capability() -> None:
    with TestClient(
        _app_with_config(
            agents_api_enabled=True,
            subagent_batches_available=True,
        )
    ) as client:
        response = client.get("/api/features")
    assert response.status_code == 200
    assert response.json()["subagent_batches"] == {
        "enabled": True,
        "repository_available": True,
        "worker_running": True,
        "max_running": 3,
    }


def test_features_distinguishes_batch_history_from_worker_availability() -> None:
    with TestClient(
        _app_with_config(
            agents_api_enabled=True,
            subagent_batches_available=False,
            subagent_batch_repo_available=True,
        )
    ) as client:
        response = client.get("/api/features")
    assert response.status_code == 200
    assert response.json()["subagent_batches"] == {
        "enabled": False,
        "repository_available": True,
        "worker_running": False,
        "max_running": 3,
    }


def test_features_reports_browser_control_enabled_when_configured_and_runtime_available() -> None:
    with (
        patch("app.gateway.browser_capability.importlib.util.find_spec", return_value=object()),
        TestClient(_app_with_config(agents_api_enabled=True, browser_enabled=True)) as client,
    ):
        response = client.get("/api/features")
    assert response.status_code == 200
    assert response.json()["browser_control"] == {"enabled": True}


def test_features_reports_browser_control_disabled_when_runtime_missing() -> None:
    with (
        patch("app.gateway.browser_capability.importlib.util.find_spec", return_value=None),
        TestClient(_app_with_config(agents_api_enabled=True, browser_enabled=True)) as client,
    ):
        response = client.get("/api/features")
    assert response.status_code == 200
    assert response.json()["browser_control"] == {"enabled": False}


def test_features_reports_browser_control_disabled_for_unguarded_cdp() -> None:
    with (
        patch("app.gateway.browser_capability.importlib.util.find_spec", return_value=object()),
        TestClient(
            _app_with_config(
                agents_api_enabled=True,
                browser_enabled=True,
                browser_extra={"cdp_url": "http://127.0.0.1:9222"},
            ),
        ) as client,
    ):
        response = client.get("/api/features")
    assert response.status_code == 200
    assert response.json()["browser_control"] == {"enabled": False}


def test_features_reports_the_workspace_profile_and_its_starters() -> None:
    # The frontend cannot decide either of these for itself: the profile is a
    # deployment's choice and the starters are its words.
    ui = UiConfig(
        profile="business",
        starters=[StarterConfig(id="review", title="Monthly review", prompt="Build my monthly review.")],
    )

    with TestClient(_app_with_config(agents_api_enabled=True, ui=ui)) as client:
        payload = client.get("/api/features").json()

    assert payload["ui"]["profile"] == "business"
    assert payload["ui"]["starters"] == [{"id": "review", "title": "Monthly review", "prompt": "Build my monthly review."}]


def test_features_reports_a_developer_deployment_with_an_empty_grid() -> None:
    # What an untouched install serves: every screen offered, and the Home it
    # already had. The helper supplies `UiConfig()`, the same value the
    # `AppConfig` default factory builds.
    with TestClient(_app_with_config(agents_api_enabled=True)) as client:
        payload = client.get("/api/features").json()

    assert payload["ui"]["profile"] == "developer"
    assert payload["ui"]["starters"] == []


def test_an_operator_can_clear_the_starter_grid() -> None:
    with TestClient(_app_with_config(agents_api_enabled=True, ui=UiConfig(profile="business", starters=[]))) as client:
        payload = client.get("/api/features").json()

    assert payload["ui"]["starters"] == []


def _bundle(tmp_path: Path, brand: dict, *, starters: list | str | None = None, logo: bool = True) -> str:
    root = tmp_path / "tenant"
    root.mkdir()
    if logo:
        (root / "logo.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\0" * 16)
    (root / "brand.json").write_text(json.dumps(brand), encoding="utf-8")
    if starters is not None:
        (root / "starters.json").write_text(starters if isinstance(starters, str) else json.dumps(starters), encoding="utf-8")
    return str(root)


BRAND = {"company_name": "Example Services Co.", "logo": "logo.png", "colors": {"primary": "#0a6b3d", "secondary": "#9ccdb4"}}


def test_features_reports_the_tenant_bundle_s_brand(tmp_path: Path) -> None:
    """The header and the About page take the company from here; the report skill reads the same file."""
    with TestClient(_app_with_config(agents_api_enabled=True, tenant_bundle_path=_bundle(tmp_path, BRAND))) as client:
        payload = client.get("/api/features").json()

    assert payload["branding"] == {"company_name": "Example Services Co.", "colors": {"primary": "#0a6b3d", "secondary": "#9ccdb4"}, "has_logo": True, "provider": {"display_name": None, "support_url": None}}


def test_a_bundle_without_a_picture_still_names_the_company(tmp_path: Path) -> None:
    with TestClient(_app_with_config(agents_api_enabled=True, tenant_bundle_path=_bundle(tmp_path, BRAND, logo=False))) as client:
        payload = client.get("/api/features").json()

    assert payload["branding"]["company_name"] == "Example Services Co."
    assert payload["branding"]["has_logo"] is False


def test_the_bundle_s_starters_replace_the_config_s_when_it_has_a_usable_list(tmp_path: Path) -> None:
    ui = UiConfig(profile="business", starters=[StarterConfig(id="config", title="From config", prompt="Config prompt.")])
    starters = [{"id": "bundle", "title": "From the bundle", "prompt": "Bundle prompt."}]

    with TestClient(_app_with_config(agents_api_enabled=True, ui=ui, tenant_bundle_path=_bundle(tmp_path, BRAND, starters=starters))) as client:
        payload = client.get("/api/features").json()

    assert payload["ui"]["profile"] == "business"
    assert payload["ui"]["starters"] == starters


def test_a_bundle_starter_list_that_breaks_the_rules_leaves_the_config_s_grid(tmp_path: Path) -> None:
    """A typo in the operator's file never empties Home; the problem is journalled and --check names it."""
    ui = UiConfig(profile="business", starters=[StarterConfig(id="config", title="From config", prompt="Config prompt.")])

    with TestClient(_app_with_config(agents_api_enabled=True, ui=ui, tenant_bundle_path=_bundle(tmp_path, BRAND, starters="{not a list"))) as client:
        payload = client.get("/api/features").json()

    assert payload["ui"]["starters"] == [{"id": "config", "title": "From config", "prompt": "Config prompt."}]
    assert payload["branding"]["company_name"] == "Example Services Co.", "one bad file degrades its own field alone"


def test_a_deployment_that_names_no_bundle_reports_no_brand() -> None:
    with TestClient(_app_with_config(agents_api_enabled=True)) as client:
        payload = client.get("/api/features").json()

    assert payload["branding"] == NO_BRANDING


def test_features_exposes_support_separately_from_customer_identity(tmp_path: Path) -> None:
    (tmp_path / "brand.json").write_text(json.dumps({"company_name": "Customer"}), encoding="utf-8")
    provider = tmp_path / "provider.json"
    provider.write_text(json.dumps({"display_name": "Hosting", "support_url": "https://help.example.test/"}), encoding="utf-8")
    with TestClient(_app_with_config(agents_api_enabled=True, tenant_bundle_path=str(tmp_path))) as client:
        branding = client.get("/api/features").json()["branding"]
        assert branding["company_name"] == "Customer"
        assert branding["provider"] == {"display_name": "Hosting", "support_url": "https://help.example.test/"}
        provider.write_text(json.dumps({"support_url": "javascript:alert(1)"}), encoding="utf-8")
        assert client.get("/api/features").json()["branding"]["provider"]["support_url"] is None
