"""Custom model endpoints on a tenant-written deployment stay off the event loop.

The public-endpoint clients a custom model is handed look a host name up and
load TLS trust. Both are blocking, so an asynchronous call and model discovery
have to do them in a worker thread. Imports sit at module top so import-time IO
runs at collection, outside the strict gate.
"""

from __future__ import annotations

import _socket
import asyncio
import socket
import ssl
import threading

import pytest
from support.model_endpoint_stubs import ANSWER, MODEL_ID, ModelEndpointStub

from app.gateway.model_discovery import DiscoveryError, fetch_model_catalog
from deerflow.config import managed_models
from deerflow.config.app_config import reset_app_config
from deerflow.config.managed_models import ManagedModel, ManagedModelStore
from deerflow.mcp import personal_network
from deerflow.models.factory import create_chat_model

pytestmark = pytest.mark.asyncio

MADE_UP_NAME = "models.example.test"


@pytest.fixture
def deployment(tmp_path, monkeypatch):
    (tmp_path / "home").mkdir()
    (tmp_path / "config.yaml").write_text("sandbox:\n  use: test\nmodels: []\n", encoding="utf-8")
    (tmp_path / "extensions_config.json").write_text('{"mcpServers": {}, "skills": {}}', encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(tmp_path / "config.yaml"))
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(tmp_path / "extensions_config.json"))
    monkeypatch.setenv("DEER_FLOW_TENANT_WRITTEN_ENDPOINTS", "on")
    monkeypatch.delenv("DEER_FLOW_PRIVATE_ENDPOINTS", raising=False)
    monkeypatch.setattr(managed_models, "_cache", None)
    # Start from no loaded trust, so the first request has to load it.
    monkeypatch.setattr(personal_network, "_model_trust", None)
    reset_app_config()
    yield ManagedModelStore()
    reset_app_config()


@pytest.fixture
def made_up_name(monkeypatch):
    """Resolve one made-up name to the loopback address, below the instrumented lookup."""
    native_getaddrinfo = _socket.getaddrinfo

    def getaddrinfo(host, port, family=socket.AF_UNSPEC, *args, **kwargs):
        if host == MADE_UP_NAME:
            return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", port or 0))]
        return native_getaddrinfo(host, port, family, *args, **kwargs)

    monkeypatch.setattr(_socket, "getaddrinfo", getaddrinfo)


def _custom_model(base_url: str) -> ManagedModel:
    return ManagedModel(name="records-model", model=MODEL_ID, base_url=base_url, api_key="typed-model-key")


async def _built() -> object:
    return await asyncio.to_thread(create_chat_model, "records-model", attach_tracing=False, max_retries=0, timeout=5)


async def test_a_listed_custom_model_and_its_discovery_do_not_block(deployment, monkeypatch) -> None:
    trust_loaded_on: list[int] = []
    load_trust = ssl.create_default_context

    def recording(*args, **kwargs):
        trust_loaded_on.append(threading.get_ident())
        return load_trust(*args, **kwargs)

    monkeypatch.setattr(ssl, "create_default_context", recording)
    with ModelEndpointStub() as service:
        monkeypatch.setenv("DEER_FLOW_PRIVATE_ENDPOINTS", service.host_port)
        saved = await asyncio.to_thread(deployment.save, _custom_model(service.base_url()), expected_revision=None)

        assert (await (await _built()).ainvoke("hello")).content == ANSWER
        assert await fetch_model_catalog(saved) == ([MODEL_ID], False)
    # TLS trust is loaded once for every model request, and never on the loop's thread.
    assert len(trust_loaded_on) == 1 and threading.get_ident() not in trust_loaded_on


async def test_a_name_is_looked_up_off_the_event_loop(deployment, made_up_name) -> None:
    with ModelEndpointStub() as service:
        saved = await asyncio.to_thread(deployment.save, _custom_model(service.base_url(host=MADE_UP_NAME)), expected_revision=None)

        with pytest.raises(Exception) as failure:
            await (await _built()).ainvoke("hello")
        # Refused by the address check itself: the lookup ran, in a worker thread.
        assert isinstance(failure.value.__cause__, ValueError)
        assert str(failure.value.__cause__) == "Custom model endpoints require a public HTTP(S) endpoint"
        with pytest.raises(DiscoveryError):
            await fetch_model_catalog(saved)
        assert service.connections == 0
