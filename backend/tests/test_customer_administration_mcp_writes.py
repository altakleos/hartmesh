"""Approval checks use the complete candidate and precede atomic persistence."""

import json

import pytest
from fastapi import HTTPException

from app.gateway.routers import mcp
from deerflow.config.app_config import AppConfig
from deerflow.runtime.customer_administration import capture_customer_administration_policy

LAUNCH = {"type": "stdio", "command": "npx", "args": ["example-server@1.0.0"], "env": {"MODE": "read", "TOKEN": "fixture-token"}, "cwd": "/opt/provider"}


@pytest.fixture
def configured_store(tmp_path, monkeypatch):
    path = tmp_path / "extensions_config.json"
    path.write_text(json.dumps({"mcpServers": {"existing": {**LAUNCH, "enabled": False}}, "skills": {}}), encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(path))
    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "customer_administration": {"local_mcp_management": True}, "approved_local_mcp_definitions": [LAUNCH]})
    return path, capture_customer_administration_policy(config)


def test_masked_secret_update_matches_complete_merged_launch(configured_store):
    path, policy = configured_store
    incoming = {**LAUNCH, "env": {"MODE": "read", "TOKEN": "***"}, "description": "Owner label"}
    result = mcp._apply_mcp_server_config_update(mcp.McpServerConfigUpdateRequest(server_name="existing", server=mcp.McpServerConfigResponse(**incoming)), policy=policy)
    assert result["existing"].env["TOKEN"] == "fixture-token"
    assert json.loads(path.read_text(encoding="utf-8"))["mcpServers"]["existing"]["env"] == LAUNCH["env"]


@pytest.mark.parametrize("delta", [{"cwd": "/other"}, {"args": ["different-server@1.0.0"]}, {"env": {"MODE": "write", "TOKEN": "***"}}, {"unknown_launch_option": "value"}])
def test_disabled_reconfiguration_requires_exact_approval_and_preserves_bytes(configured_store, delta):
    path, policy = configured_store
    before = path.read_bytes()
    incoming = {**LAUNCH, "enabled": False, **delta}
    with pytest.raises(HTTPException) as denied:
        mcp._apply_mcp_server_config_update(mcp.McpServerConfigUpdateRequest(server_name="existing", server=mcp.McpServerConfigResponse(**incoming)), policy=policy)
    assert denied.value.status_code == 403
    assert path.read_bytes() == before


def test_bulk_admission_is_all_or_nothing(configured_store):
    path, policy = configured_store
    before = path.read_bytes()
    body = mcp.McpConfigUpdateRequest(mcp_servers={"approved": mcp.McpServerConfigResponse(**LAUNCH), "other": mcp.McpServerConfigResponse(**{**LAUNCH, "args": ["different-server@1.0.0"]})})
    with pytest.raises(HTTPException) as denied:
        mcp._apply_mcp_servers_create(body, policy=policy)
    assert denied.value.status_code == 403
    assert path.read_bytes() == before


def test_enabling_rechecks_current_persisted_definition(configured_store):
    path, policy = configured_store
    changed = json.loads(path.read_text(encoding="utf-8"))
    changed["mcpServers"]["existing"]["env"]["MODE"] = "changed-by-operator"
    path.write_text(json.dumps(changed), encoding="utf-8")
    before = path.read_bytes()
    with pytest.raises(HTTPException) as denied:
        mcp._apply_mcp_server_state_update(mcp.McpServerStateUpdateRequest(server_name="existing", enabled=True), policy=policy)
    assert denied.value.status_code == 403
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "definition", [{"command": "npx", "args": ["unapproved-server@1.0.0"]}, {"type": "", "command": "npx", "args": ["unapproved-server@1.0.0"]}, {"transport": "stdio", "command": "npx", "args": ["unapproved-server@1.0.0"]}]
)
def test_effective_stdio_defaults_cannot_skip_approval(definition):
    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "customer_administration": {"local_mcp_management": True}})
    policy = capture_customer_administration_policy(config)
    with pytest.raises(HTTPException) as denied:
        mcp._admit_mcp_definition(policy, definition)
    assert denied.value.status_code == 403


@pytest.mark.parametrize("field", ["type", "transport"])
def test_deployment_environment_transport_cannot_skip_approval(monkeypatch, field):
    monkeypatch.setenv("HARTMESH_REVIEW_TRANSPORT", "stdio")
    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "customer_administration": {"local_mcp_management": True}})
    policy = capture_customer_administration_policy(config)
    definition = {field: "$HARTMESH_REVIEW_TRANSPORT", "command": "npx", "args": ["unapproved-server@1.0.0"]}
    with pytest.raises(HTTPException) as denied:
        mcp._admit_mcp_definition(policy, definition)
    assert denied.value.status_code == 403


@pytest.mark.parametrize("previous,candidate", [(False, "true"), (False, 1), (False, "$HARTMESH_REVIEW_ENABLED"), (1, False), ("$HARTMESH_REVIEW_ENABLED", False)])
def test_embedded_effective_enabled_change_cannot_skip_delegation(configured_store, monkeypatch, previous, candidate):
    from deerflow.client import DeerFlowClient
    from deerflow.runtime.customer_administration import DENIED_CUSTOMER_ADMINISTRATION, CustomerManagementDenied

    path, _ = configured_store
    monkeypatch.setenv("HARTMESH_REVIEW_ENABLED", "true")
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["mcpServers"]["existing"]["enabled"] = previous
    path.write_text(json.dumps(raw), encoding="utf-8")
    before = path.read_bytes()
    client = DeerFlowClient.__new__(DeerFlowClient)
    client._customer_administration_policy = DENIED_CUSTOMER_ADMINISTRATION
    monkeypatch.setattr(client, "_customer_management_actor_allowed", lambda: True)
    with pytest.raises(CustomerManagementDenied):
        client.update_mcp_config({"existing": {**LAUNCH, "enabled": candidate}})
    assert path.read_bytes() == before
