"""Personal and program connections stay outside employee conversations and assignments."""

import json
import sys
from dataclasses import replace
from types import SimpleNamespace

from support.mcp_endpoint_stubs import EndpointStub
from test_agent_execution_runtime import execution
from test_instance_middleware_tools import build_graph
from test_personal_mcp import personal_client as personal_client
from test_subagent_declared_tool_policy import _app_config

from deerflow.agent_instances import connection_tools, connections
from deerflow.agent_instances.conversations import ConnectionOrigin
from deerflow.config.extensions_config import reset_extensions_config
from deerflow.mcp.user_config import personal_server_name, read_user_mcp_config
from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.tools.mcp_metadata import is_mcp_tool


def test_conversation_and_assignment_leave_personal_and_local_connections_unopened(tmp_path, monkeypatch, personal_client):
    config_file = tmp_path / "extensions.json"
    marker = tmp_path / "program-started"
    program = tmp_path / "calendar_service.py"
    program.write_text(
        "from pathlib import Path\nimport sys\nfrom mcp.server.fastmcp import FastMCP\nPath(sys.argv[1]).touch()\nserver = FastMCP('calendar')\n@server.tool()\ndef next_date() -> str:\n    return 'Friday'\nserver.run()\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(config_file))
    monkeypatch.delenv("DEER_FLOW_TENANT_WRITTEN_ENDPOINTS", raising=False)
    monkeypatch.setattr(connection_tools, "connections_enabled", lambda: True)
    bound = execution()
    identity = set_current_user(SimpleNamespace(id=bound.requester.subject_id))
    config = _app_config()

    def external_tools(current):
        graph, _ = build_graph(monkeypatch, bound=current, app_config=config)
        return {t.name for t in graph.get_graph().nodes["tools"].data.tools_by_name.values() if is_mcp_tool(t)}

    try:
        with EndpointStub() as personal, EndpointStub() as company:
            config_file.write_text(json.dumps({"mcpServers": {name: {"type": "http", "url": company.url("/mcp")} for name in ("notes", "calendar")}}))
            saved = personal_client.post("/api/mcp/personal/config/servers", headers={"test-user": bound.requester.subject_id}, json={"mcp_servers": {"contacts": {"type": "http", "url": personal.url("/mcp")}}})
            assert saved.status_code == 200
            choices = {row["name"]: row["id"] for row in connections.list_connections(bound.instance.id, manager=True)}
            connections.change_grant(bound.instance.id, choices["calendar"], "manager", give=True)
            raw = json.loads(config_file.read_text())
            previous = raw["mcpServers"]["calendar"]
            raw["mcpServers"]["calendar"] = {"type": "stdio", "command": sys.executable, "args": [str(program), str(marker)], "capability": previous["capability"]}
            config_file.write_text(json.dumps(raw))
            assert next(r for r in connections.list_connections(bound.instance.id, manager=True) if r["name"] == "calendar")["reason"] == "not-remote"
            for given in (False, True):
                if given:
                    connections.change_grant(bound.instance.id, choices["notes"], "manager", give=True)
                for kind in ("message", "assignment"):
                    admitted = replace(bound, connection_origin=ConnectionOrigin(kind), work=SimpleNamespace() if kind == "assignment" else None)
                    assert external_tools(admitted) == ({"notes_echo"} if given else set())
                    assert not personal.calls() and not marker.exists()
                if not given:
                    assert not company.calls()
            names = external_tools(None)
            definition = read_user_mcp_config(bound.requester.subject_id)["mcpServers"]["contacts"]
            private_name = personal_server_name(bound.requester.subject_id, "contacts", definition)
            assert {"calendar_next_date", private_name + "_echo"} <= names
            assert marker.is_file() and personal.calls()
    finally:
        reset_current_user(identity)
        reset_extensions_config()
