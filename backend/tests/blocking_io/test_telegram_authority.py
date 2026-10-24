"""Actual cold configuration/provider preparation stays off the Gateway loop."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.gateway import authz
from app.gateway.auth.mode import account_refusal  # noqa: F401 -- HTTP auth imports before lifespan; configuration/provider remain cold.
from app.telegram_bots import authority
from deerflow.config import app_config


@pytest.fixture
def cold_authority(tmp_path, monkeypatch):
    module_name = "telegram_cold_authority_provider"
    (tmp_path / (module_name + ".py")).write_text(
        "import asyncio, threading\n"
        "from deerflow.authz.provider import AuthzDecision\n"
        "class Provider:\n"
        "    name = 'cold-loop-provider'\n"
        "    def __init__(self):\n"
        "        self.loop = asyncio.get_running_loop()\n"
        "        self.thread = threading.get_ident()\n"
        "        self.calls = []\n"
        "    def authorize(self, request):\n"
        "        raise AssertionError('asynchronous provider required')\n"
        "    async def aauthorize(self, request):\n"
        "        assert asyncio.get_running_loop() is self.loop\n"
        "        self.calls.append(request)\n"
        "        return AuthzDecision(allow=request.target != 'runs:create')\n"
        "    def filter_resources(self, principal, resource_type, candidates):\n"
        "        return candidates\n",
        encoding="utf-8",
    )
    path = tmp_path / "config.yaml"
    path.write_text(
        f"config_version: 55\nsandbox:\n  use: deerflow.sandbox.local:LocalSandboxProvider\nauthorization:\n  enabled: true\n  fail_closed: false\n  provider:\n    use: {module_name}:Provider\n",
        encoding="utf-8",
    )
    (tmp_path / "extensions.json").write_text("{}", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(path))
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(tmp_path / "extensions.json"))
    for name, value in {"_app_config": None, "_app_config_path": None, "_app_config_mtime": None, "_app_config_signature": None, "_app_config_is_custom": False}.items():
        monkeypatch.setattr(app_config, name, value)
    monkeypatch.setattr(authz, "_route_provider_cache", {})
    monkeypatch.setattr(authz, "_route_provider_config_id", None)
    monkeypatch.setattr(authz, "_route_provider_config_sig", None)
    human = SimpleNamespace(id="owner", system_role="user", disabled_at=None, released_at=None, oauth_provider=None, oauth_id=None)
    monkeypatch.setattr(authority, "get_local_provider", lambda: SimpleNamespace(get_user=AsyncMock(return_value=human)))
    return path


@pytest.mark.asyncio
async def test_real_disk_config_and_cold_provider_use_original_loop(cold_authority):
    loop, thread = asyncio.get_running_loop(), threading.get_ident()
    result = await authority.ceilings("owner")
    assert "threads:read" in result and "runs:create" not in result
    provider = authz._route_provider_cache["provider"]
    assert provider.loop is loop and provider.thread == thread
    assert provider.calls and all(request.principal.user_id == "owner" and request.principal.is_internal is False for request in provider.calls)
    # The second read still checks the real file signature, without a warm
    # in-memory AppConfig override hiding synchronous filesystem work.
    assert await authority.ceilings("owner") == result
