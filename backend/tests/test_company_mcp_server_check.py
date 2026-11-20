"""Administrator diagnostics for a single saved company connection."""

import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from support.mcp_endpoint_stubs import EndpointStub

from app.gateway.routers import mcp
from deerflow.config.extensions_config import reset_extensions_config
from deerflow.mcp.cache import reset_mcp_tools_cache


@pytest.fixture
def gateway(tmp_path, monkeypatch):
    config_path = tmp_path / "extensions_config.json"
    config_path.write_text('{"mcpServers": {}}', encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(config_path))
    monkeypatch.delenv("DEER_FLOW_TENANT_WRITTEN_ENDPOINTS", raising=False)
    reset_extensions_config()
    reset_mcp_tools_cache()
    app = FastAPI()

    @app.middleware("http")
    async def identity(request, call_next):
        request.state.user = SimpleNamespace(id="operator", system_role=request.headers.get("test-role", "admin"))
        request.state.auth_source = "session"
        return await call_next(request)

    app.include_router(mcp.router)
    with TestClient(app) as client:
        yield client, config_path
    reset_extensions_config()
    reset_mcp_tools_cache()


def test_admin_checks_saved_remote_over_its_transport(gateway):
    client, file = gateway
    with EndpointStub() as service, EndpointStub() as other:
        file.write_text(json.dumps({"mcpServers": {"records": {"type": "http", "url": service.url("/mcp")}, "other": {"type": "http", "url": other.url("/mcp")}}}), encoding="utf-8")
        url = "/api/mcp/config/servers/records/check"
        assert client.post(url, headers={"test-role": "user"}).status_code == 403
        assert not service.requests
        assert client.post(url).json() == {"status": "connected", "tool_count": 1}
        before = len(service.requests)
        assert client.post(url).json() == {"status": "connected", "tool_count": 1}
        assert len(service.requests) > before
        assert not other.requests
        assert client.post("/api/mcp/config/servers/missing/check").status_code == 404


@pytest.mark.parametrize(
    ("fields", "reason"),
    [
        ({"enabled": False}, "refused"),
        ({"headers": {"Authorization": "invalid\nexample"}}, "bad-token-format"),
    ],
)
def test_invalid_or_disabled_connection_is_never_contacted(gateway, fields, reason):
    client, file = gateway
    with EndpointStub() as service:
        file.write_text(json.dumps({"mcpServers": {"records": {"type": "http", "url": service.url("/mcp"), **fields}}}), encoding="utf-8")
        assert client.post("/api/mcp/config/servers/records/check").json() == {"status": "failed", "reason": reason}
        assert not service.requests


def test_check_observes_deployment_endpoint_policy(gateway, monkeypatch):
    client, file = gateway
    monkeypatch.setenv("DEER_FLOW_TENANT_WRITTEN_ENDPOINTS", "on")
    monkeypatch.delenv("DEER_FLOW_PRIVATE_ENDPOINTS", raising=False)
    with EndpointStub() as service:
        file.write_text(json.dumps({"mcpServers": {"records": {"type": "sse", "url": service.url("/sse")}}}), encoding="utf-8")
        assert client.post("/api/mcp/config/servers/records/check").json() == {"status": "failed", "reason": "address-not-public"}
        assert service.connections == 0


def test_add_assigns_stable_id_and_checks_prefixed_remote_names(gateway):
    client, file = gateway
    definition = {"type": "http", "url": "https://mcp.example.com"}
    path = "/api/mcp/config/servers"
    assert client.post(path, json={"mcp_servers": {"with space": definition}}).status_code == 400
    assert client.post(path, json={"mcp_servers": {"with space": {**definition, "tool_name_prefix": False}}}).status_code == 200
    before = json.loads(file.read_text())["mcpServers"]["with space"]["capability"]["id"]
    assert before
    assert client.put("/api/mcp/config/server", json={"server_name": "with space", "server": {**definition, "tool_name_prefix": False, "enabled": False}}).status_code == 200
    assert json.loads(file.read_text())["mcpServers"]["with space"]["capability"]["id"] == before
    assert client.delete(path + "/with space").status_code == 200
    assert client.post(path, json={"mcp_servers": {"with space": {**definition, "tool_name_prefix": False}}}).status_code == 200
    assert json.loads(file.read_text())["mcpServers"]["with space"]["capability"]["id"] != before


@pytest.mark.parametrize(("reply", "reason"), [(403, "refused"), (200, "not-mcp"), ("stall", "timeout"), ("closed", "unreachable")])
def test_check_reports_fixed_reasons_from_remote(gateway, monkeypatch, reply, reason):
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from time import monotonic

    from deerflow.mcp import cache

    client, file = gateway
    release = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if reply == "stall":
                release.wait(3)
                return
            self.send_response(reply)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"remote diagnostic sample")

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    address = f"http://127.0.0.1:{server.server_port}/mcp"
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.03}, daemon=True)
    if reply == "closed":
        server.server_close()
    else:
        thread.start()
    try:
        if reply == "stall":
            monkeypatch.setattr(cache, "MAX_COMPANY_DISCOVERY_SECONDS", 0.25)
        file.write_text(json.dumps({"mcpServers": {"records": {"type": "http", "url": address, "headers": {"Authorization": "sample-token"}, "session_init_timeout": None}}}))
        started = monotonic()
        result = client.post("/api/mcp/config/servers/records/check")
        assert result.json() == {"status": "failed", "reason": reason}, result.text
        if reply == "stall":
            assert monotonic() - started < 2
    finally:
        release.set()
        if thread.is_alive():
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)


def test_remote_create_refuses_an_unusable_installation_identity(gateway):
    client, file = gateway
    before = file.read_bytes()
    for invalid in (7, {"id": 9}):
        response = client.post("/api/mcp/config/servers", json={"mcp_servers": {"records": {"type": "http", "url": "https://records.example.com", "capability": invalid}}})
        assert response.status_code == 400
        assert file.read_bytes() == before


def test_remote_name_rules_preserve_existing_local_catalog_identity(gateway, monkeypatch):
    from app.gateway.routers import mcp as routes
    from deerflow.capabilities.runtime import installation_id
    from deerflow.config.app_config import AppConfig
    from deerflow.runtime.customer_administration import capture_customer_administration_policy

    client, file = gateway
    # The operator delegates the exact local definition used in this fixture.
    local = {"type": "stdio", "command": "npx", "args": ["-y", "@example/fixture"]}
    policy = capture_customer_administration_policy(
        AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "customer_administration": {"local_mcp_management": True}, "approved_local_mcp_definitions": [local]})
    )
    monkeypatch.setattr(routes, "request_customer_administration_policy", lambda request: policy)
    response = client.post("/api/mcp/config/servers", json={"mcp_servers": {"local fixture": local}})
    assert response.status_code == 200, response.text
    stored = json.loads(file.read_text())["mcpServers"]["local fixture"]
    assert installation_id("local fixture", stored) == installation_id("local fixture", {})
