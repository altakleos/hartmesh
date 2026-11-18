"""The Compose bundle declares that its connections are written by tenants.

``deploy/compose/compose.yaml`` sets ``DEER_FLOW_TENANT_WRITTEN_ENDPOINTS`` to
the fixed text ``on`` in the Gateway's ``environment:`` block, where it cannot
come from the tenant's environment file, and hands the provider's optional list
of private endpoints through as ``DEER_FLOW_PRIVATE_ENDPOINTS``. The reader in
``deerflow.community.endpoint_policy`` accepts only the exact value ``on`` and
ignores a list entry that is not an exact ``host:port`` pair.
"""

from __future__ import annotations

import json
import logging
import subprocess
from pathlib import Path

import pytest
import yaml
from support import loopback_only
from support.compose import DOCKER, requires_docker_compose

from deerflow.community import endpoint_policy

REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLE = REPO_ROOT / "deploy" / "compose"
LINE = "DEER_FLOW_TENANT_WRITTEN_ENDPOINTS"
PRIVATE_ENDPOINTS = "DEER_FLOW_PRIVATE_ENDPOINTS"


@pytest.fixture(autouse=True)
def network(monkeypatch):
    """No name used here is looked up outside the process."""
    guard = loopback_only.install(monkeypatch)
    yield guard
    guard.assert_nothing_blocked()


def test_bundle_gateway_declares_tenant_written_endpoints():
    compose = yaml.safe_load((BUNDLE / "compose.yaml").read_text(encoding="utf-8"))
    environment = compose["services"]["gateway"]["environment"]

    assert environment[LINE] == "on", "a fixed value, never interpolated from the tenant's file"
    assert environment[PRIVATE_ENDPOINTS] == "${HARTMESH_PRIVATE_ENDPOINTS:-}"
    for name, service in compose["services"].items():
        if name != "gateway":
            assert not {LINE, PRIVATE_ENDPOINTS} & set(service.get("environment") or {}), name


def test_bundle_example_and_guide_name_the_private_endpoint_list():
    example = (BUNDLE / ".env.example").read_text(encoding="utf-8").splitlines()
    assert "#HARTMESH_PRIVATE_ENDPOINTS=" in example, "optional, shown commented out and empty"
    assert not any(line.startswith(("HARTMESH_PRIVATE_ENDPOINTS", LINE, PRIVATE_ENDPOINTS)) for line in example)
    assert "`HARTMESH_PRIVATE_ENDPOINTS`" in (BUNDLE / "README.md").read_text(encoding="utf-8")


@requires_docker_compose
def test_real_compose_keeps_the_fixed_value_over_the_tenant_file(tmp_path):
    """The tenant file reaches the Gateway whole (``env_file``); the ``environment:`` block wins."""
    example = (BUNDLE / ".env.example").read_text(encoding="utf-8").replace("HARTMESH_DATA_DIR=/srv/hartmesh", f"HARTMESH_DATA_DIR={tmp_path}")
    tenant_file = tmp_path / ".env"
    tenant_file.write_text(f"{example}{LINE}=off\n{PRIVATE_ENDPOINTS}=10.0.0.9:1\nHARTMESH_PRIVATE_ENDPOINTS=mcp.internal.example:8443\n", encoding="utf-8")

    result = subprocess.run([DOCKER, "compose", "--project-directory", str(BUNDLE), "--env-file", str(tenant_file), "config", "--format", "json"], capture_output=True, text=True, timeout=120, check=False)

    assert result.returncode == 0, result.stderr
    environment = json.loads(result.stdout)["services"]["gateway"]["environment"]
    assert environment["HARTMESH_TENANT"] == "example-tenant", "the tenant file was read into the Gateway's environment"
    assert environment[LINE] == "on"
    assert environment[PRIVATE_ENDPOINTS] == "mcp.internal.example:8443"


@pytest.mark.parametrize("value", ["On", "ON", "true", "1", "yes", "enabled", " on", "on ", "off", ""])
def test_only_the_exact_value_on_declares_a_tenant_written_deployment(monkeypatch, value):
    monkeypatch.setenv(PRIVATE_ENDPOINTS, "10.0.0.5:8443")
    monkeypatch.setenv(LINE, value)

    assert endpoint_policy.tenant_written() is False
    assert endpoint_policy.private_endpoints() == frozenset(), "the list is read only on a tenant-written deployment"
    assert endpoint_policy.is_private_endpoint("https", "10.0.0.5", 8443) is False


def test_an_absent_value_is_not_a_tenant_written_deployment(monkeypatch):
    monkeypatch.delenv(LINE, raising=False)
    monkeypatch.setenv(PRIVATE_ENDPOINTS, "10.0.0.5:8443")

    assert endpoint_policy.tenant_written() is False
    assert endpoint_policy.private_endpoints() == frozenset()


def test_the_value_is_read_at_the_moment_of_use(monkeypatch):
    monkeypatch.setenv(PRIVATE_ENDPOINTS, "10.0.0.5:8443")
    monkeypatch.delenv(LINE, raising=False)
    assert endpoint_policy.tenant_written() is False

    monkeypatch.setenv(LINE, "on")
    assert endpoint_policy.tenant_written() is True
    assert endpoint_policy.private_endpoints() == frozenset({("10.0.0.5", 8443)})


@pytest.mark.parametrize(
    "malformed",
    [
        pytest.param("mcp.internal.example", id="no-port"),
        pytest.param("*.internal.example:8443", id="wildcard"),
        pytest.param("*:8443", id="any-host"),
        pytest.param("mcp.internal.example:8443/mcp", id="path"),
        pytest.param("https://mcp.internal.example:8443", id="scheme"),
        pytest.param("mcp.internal.example:0", id="port-zero"),
        pytest.param("mcp.internal.example:70000", id="port-out-of-range"),
        pytest.param("mcp.internal.example:", id="empty-port"),
        pytest.param("mcp.internal.example:84a3", id="port-not-a-number"),
        pytest.param(":8443", id="no-host"),
        pytest.param("::1:8443", id="unbracketed-ipv6"),
        pytest.param("[not-an-address]:8443", id="bracketed-name"),
        pytest.param("mcp internal:8443", id="space"),
    ],
)
def test_a_malformed_list_entry_is_ignored_and_the_others_still_count(monkeypatch, caplog, malformed):
    monkeypatch.setenv(LINE, "on")
    monkeypatch.setenv(PRIVATE_ENDPOINTS, f"MCP.Internal.Example:8443, {malformed} ,10.0.4.7:9000,,[::1]:9001,")
    endpoint_policy._parse_private_endpoints.cache_clear()

    with caplog.at_level(logging.WARNING, logger=endpoint_policy.__name__):
        assert endpoint_policy.private_endpoints() == frozenset({("mcp.internal.example", 8443), ("10.0.4.7", 9000), ("::1", 9001)})

    assert [record.getMessage() for record in caplog.records] == [f"Ignoring entry 2 of {PRIVATE_ENDPOINTS}: expected host:port"]
    assert malformed not in caplog.text


def test_a_listed_pair_matches_one_host_and_one_port(monkeypatch):
    monkeypatch.setenv(LINE, "on")
    monkeypatch.setenv(PRIVATE_ENDPOINTS, "mcp.internal.example:8443,10.0.4.7:80,[::1]:443")

    for url in ("https://mcp.internal.example:8443/mcp", "http://MCP.INTERNAL.EXAMPLE:8443/sse?x=1", "http://10.0.4.7/mcp", "http://10.0.4.7:80/mcp", "https://[::1]/token", "https://[0:0:0:0:0:0:0:1]:443/token"):
        assert endpoint_policy.address_problem(url) is None, url
    for url in (
        "https://mcp.internal.example:8444/mcp",
        "https://mcp.internal.example/mcp",
        "https://other.mcp.internal.example:8443/mcp",
        "https://10.0.4.7/mcp",
        "http://10.0.4.7:8080/mcp",
        "http://[::1]/token",
        "https://10.0.4.7:80@10.0.4.8/mcp",
        "ftp://10.0.4.7:80/mcp",
        "http://127.0.0.1/mcp",
    ):
        assert endpoint_policy.address_problem(url), url
    for value in (None, 7, "", "not a url", "http://10.0.4.7:port/mcp"):
        assert endpoint_policy.address_problem(value), value


def test_a_public_address_needs_no_listing(monkeypatch):
    monkeypatch.setenv(LINE, "on")
    monkeypatch.delenv(PRIVATE_ENDPOINTS, raising=False)

    assert endpoint_policy.address_problem("https://8.8.8.8/mcp") is None
    assert endpoint_policy.address_problem("http://[2606:4700:4700::1111]:8080/mcp") is None
