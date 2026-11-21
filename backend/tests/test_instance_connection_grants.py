"""A grant follows the registry lifetime while existing custom-agent selections stay stable."""

import copy
import json

import pytest

from deerflow.capabilities.runtime import installation_id


def test_grant_round_trip_and_same_name_replacement(tmp_path, monkeypatch):
    from deerflow.agent_instances import connections

    file = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    server = {"type": "http", "url": "https://example.test/token-in-path", "headers": {"Authorization": "Bearer test-only"}}
    file.write_text(json.dumps({"mcpServers": {"records": server}, "future": [1]}), encoding="utf-8")
    prior = installation_id("records", server)
    [choice] = connections.list_connections("employee", manager=True)
    assert choice["host"] == "example.test"
    assert not choice["given"] and choice["can_give"]
    assert not connections.list_connections("employee", manager=False)
    connections.change_grant("employee", choice["id"], "alice", give=True)
    first = json.loads(file.read_text())
    assert installation_id("records", first["mcpServers"]["records"]) == prior
    assert first["future"] == [1]
    connections.change_grant("employee", choice["id"], "second", give=True)
    assert json.loads(file.read_text()) == first
    assert connections.list_connections("employee", manager=False) == [{"name": "records", "host": "example.test"}]
    [given] = connections.list_connections("employee", manager=True)
    assert given["given"] and given["given_at"]
    assert "token-in-path" not in json.dumps(given) and "test-only" not in json.dumps(given)
    updated = copy.deepcopy(first)
    updated["mcpServers"]["records"]["enabled"] = False
    file.write_text(json.dumps(updated))
    assert not connections.list_connections("employee", manager=False)
    assert connections.list_connections("employee", manager=True)[0]["given"]
    updated["mcpServers"]["records"] = server
    file.write_text(json.dumps(updated))
    assert not connections.list_connections("employee", manager=False)
    assert not connections.list_connections("employee", manager=True)[0]["given"]


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"type": "stdio", "command": "tool"}, "not-remote"),
        ({"agent_auth": True}, "agent-credential"),
        ({"task_toolsets": [{"name": "queue", "submit_tool": "start", "status_tool": "status", "cancel_tool": "cancel"}]}, "long-running"),
        ({"user_auth": {"enabled": True, "users": {}}}, "per-person-credentials"),
        ({"headers_from_context": {"enabled": True, "headers": {"Authorization": "user_token"}}}, "per-request-credentials"),
    ],
)
def test_rejected_connection_cannot_be_given(tmp_path, monkeypatch, override, reason):
    from deerflow.agent_instances import connections
    from deerflow.agent_instances.contract import AgentConflict

    file = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    file.write_text(json.dumps({"mcpServers": {"records": {"type": "http", "url": "https://example.test", **override}}}))
    before = file.read_bytes()
    [choice] = connections.list_connections("employee", manager=True)
    assert choice["reason"] == reason and not choice["can_give"]
    with pytest.raises(AgentConflict) as refused:
        connections.change_grant("employee", choice["id"], "alice", give=True)
    assert refused.value.reason == reason
    assert file.read_bytes() == before


def test_unknown_grant_entries_are_inert_and_preserved(tmp_path, monkeypatch):
    from deerflow.agent_instances import connections

    file = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    raw = {"mcpServers": {"a": {"type": "http", "url": "https://a.example.com"}, "b": {"type": "http", "url": "https://b.example.com"}}}
    file.write_text(json.dumps(raw))
    a, b = connections.list_connections("employee", manager=True)
    connections.change_grant("employee", a["id"], "alice", give=True)
    raw = json.loads(file.read_text())
    entries = raw["agentConnections"]["agents"]["employee"]
    [key] = entries
    entries[key]["future_rule"] = "unrecognized"
    raw["agentConnections"]["future"] = [3]
    file.write_text(json.dumps(raw))
    assert not connections.list_connections("employee", manager=False)
    connections.change_grant("employee", b["id"], "alice", give=True)
    stored = json.loads(file.read_text())
    assert stored["agentConnections"]["future"] == [3]
    assert stored["agentConnections"]["agents"]["employee"][key] == entries[key]
    connections.change_grant("employee", a["id"], "alice", give=False)
    assert key not in json.loads(file.read_text())["agentConnections"]["agents"]["employee"]
    assert connections.list_connections("employee", manager=False) == [{"name": "b", "host": "b.example.com"}]


def test_embedded_registry_writes_mask_only_binding_and_retire_replacements(tmp_path, monkeypatch):
    from deerflow.agent_instances import connections
    from deerflow.agent_instances.contract import AgentConflict
    from deerflow.client import DeerFlowClient
    from deerflow.config.extensions_config import reset_extensions_config
    from deerflow.runtime.customer_administration import CustomerManagementActor, bind_customer_management_actor

    file = tmp_path / "extensions.json"
    file.write_text(json.dumps({"mcpServers": {}, "custom": {"keep": True}}))
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path))
    reset_extensions_config()
    sdk = DeerFlowClient.__new__(DeerFlowClient)
    key = connections.BINDING_SECRET
    definition = {"type": "http", "url": "https://notices.example.test", "headers": {"Authorization": "ordinary-fixture"}, "capability": {"id": "custom-selection", "label": "keep", key: "a" * 64}}
    with bind_customer_management_actor(CustomerManagementActor(owner_id="admin", administrator=True)):
        created = sdk.update_mcp_config({"notices": definition})["mcp_servers"]["notices"]
        initial = json.loads(file.read_text())["mcpServers"]["notices"]
        assert connections.binding_id(initial) and initial["capability"][key] != definition["capability"][key]
        assert created == sdk.get_mcp_config()["mcp_servers"]["notices"]
        assert created["capability"] == {"id": "custom-selection", "label": "keep"} and created["headers"] == definition["headers"]
        [row] = connections.list_connections("employee", manager=True)
        connections.change_grant("employee", row["id"], "manager", give=True)
        sdk.update_mcp_config({"notices": {**created, "description": "Updated", "capability": {key: "***", "label": "keep"}}})
        updated = json.loads(file.read_text())["mcpServers"]["notices"]
        assert updated["capability"][key] == initial["capability"][key]
        assert updated["capability"]["id"] == "custom-selection"
        assert connections.current_grant("employee", connections.binding_id(initial)).name == "notices"
        sdk.update_mcp_config({})
        sdk.update_mcp_config({"notices": initial})
        replaced = json.loads(file.read_text())
        assert replaced["custom"] == {"keep": True}
        assert connections.binding_id(replaced["mcpServers"]["notices"]) != connections.binding_id(initial)
        with pytest.raises(AgentConflict) as stale:
            connections.change_grant("employee", row["id"], "manager", give=True)
        assert stale.value.reason == "removed"
        assert not connections.granted_connections("employee")
    reset_extensions_config()


def test_invalid_registry_or_grant_data_never_gives_authority(tmp_path, monkeypatch):
    from deerflow.agent_instances import connections
    from deerflow.agent_instances.contract import AgentConflict

    file = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    for definition in ({"type": "http"}, {"type": "http", "url": "record-without-a-host"}, {"type": "sse", "url": "https://notices.example.test", "capability": {"instance_binding_secret": 5}}):
        file.write_text(json.dumps({"mcpServers": {"broken": definition}}))
        [choice] = connections.list_connections("worker", manager=True)
        assert not choice["can_give"]
        before = file.read_bytes()
        with pytest.raises(AgentConflict):
            connections.change_grant("worker", choice["id"], "manager", give=True)
        assert file.read_bytes() == before
    file.write_text("not a JSON document")
    with pytest.raises(AgentConflict) as corrupt:
        connections.granted_connections("worker")
    assert corrupt.value.reason == "not-available"


def test_simultaneous_first_grants_keep_both_instances_and_their_retry(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from deerflow.agent_instances import connections

    file = tmp_path / "extensions.json"
    file.write_text(json.dumps({"mcpServers": {"notices": {"type": "http", "url": "https://notices.example.test"}}}))
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    [legacy] = connections.list_connections("first", manager=True)
    meet = Barrier(2)

    def grant(instance):
        meet.wait(timeout=5)
        connections.change_grant(instance, legacy["id"], instance, give=True)

    with ThreadPoolExecutor(2) as workers:
        futures = [workers.submit(grant, instance) for instance in ("first", "second")]
        for result in futures:
            result.result(timeout=8)
    before = file.read_bytes()
    for instance in ("first", "second"):
        assert connections.granted_connections(instance)[0].name == "notices"
        connections.change_grant(instance, legacy["id"], "another", give=True)
    assert file.read_bytes() == before


def test_host_switch_and_package_allowlist_fail_closed():
    from deerflow.config.app_config import AgentConnectionsConfig

    assert AgentConnectionsConfig().model_dump() == {"enabled": False, "packages": []}
    assert AgentConnectionsConfig.model_validate({"enabled": "true", "packages": "operator-package"}).model_dump() == {"enabled": False, "packages": []}
    assert AgentConnectionsConfig.model_validate({"enabled": True, "packages": ["approved", None]}).packages == []


@pytest.mark.parametrize("section", [{}, {"agent_connections": None}, {"agent_connections": []}, {"agent_connections": "enabled"}])
def test_real_app_config_tolerates_older_or_malformed_connection_section(section):
    from deerflow.config.app_config import AgentConnectionsConfig, AppConfig

    raw = {"sandbox": {"use": "test"}, **section}
    parsed = AppConfig.model_validate(raw)
    assert parsed.agent_connections == AgentConnectionsConfig()
    assert AppConfig.model_validate_json(parsed.model_dump_json()).agent_connections == AgentConnectionsConfig()
    enabled = AgentConnectionsConfig(enabled=True, packages=["company_work"])
    assert AppConfig.model_validate({**raw, "agent_connections": enabled}).agent_connections == enabled
