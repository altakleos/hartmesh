"""Per-sandbox request headers flow through the AIO backend in upstream's shape.

HartMesh once carried the same headers with plumbing of its own: a frozen
mapping on ``SandboxInfo``, a keyword-only constructor argument, per-request
keyword arguments in the readiness pollers, and conditional call sites in the
provider. Upstream now carries the feature with the same signature, so HartMesh
takes that signature verbatim and the hunks stop conflicting on every merge.
The behaviour pinned here is the contract both trees now share.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from deerflow.community.aio_sandbox import backend as readiness
from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo

HEADERS = {"Authorization": "Bearer attempt-capability"}


def test_sandbox_info_request_headers_are_a_plain_dict_kept_out_of_persistence_and_logs() -> None:
    plain = SandboxInfo(sandbox_id="sb", sandbox_url="http://sb")
    assert plain.request_headers == {}
    assert type(plain.request_headers) is dict

    info = SandboxInfo(sandbox_id="sb", sandbox_url="http://sb", request_headers=dict(HEADERS))
    assert info.request_headers == HEADERS
    assert "request_headers" not in info.to_dict()
    assert "attempt-capability" not in repr(info)
    assert SandboxInfo.from_dict(info.to_dict()).request_headers == {}
    # Headers are ephemeral control-plane material, not identity.
    assert SandboxInfo(sandbox_id="sb", sandbox_url="http://sb", created_at=1.0) == SandboxInfo(sandbox_id="sb", sandbox_url="http://sb", created_at=1.0, request_headers=dict(HEADERS))


def test_wait_for_sandbox_ready_applies_headers_once_on_the_session(monkeypatch: pytest.MonkeyPatch) -> None:
    sessions: list[_FakeSession] = []

    class _FakeSession:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}
            self.calls: list[tuple[str, dict[str, object]]] = []
            self.trust_env = True
            sessions.append(self)

        def __enter__(self) -> _FakeSession:
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, url: str, **kwargs: object):
            self.calls.append((url, kwargs))
            return SimpleNamespace(status_code=200)

    monkeypatch.setattr(readiness.requests, "Session", _FakeSession)

    assert readiness.wait_for_sandbox_ready("http://sandbox", timeout=1, headers=HEADERS) is True
    assert sessions[-1].headers == HEADERS
    assert sessions[-1].calls == [("http://sandbox/v1/sandbox", {"timeout": 5})]

    assert readiness.wait_for_sandbox_ready("http://sandbox", timeout=1) is True
    assert sessions[-1].headers == {}


@pytest.mark.asyncio
async def test_wait_for_sandbox_ready_async_sets_headers_on_the_client(monkeypatch: pytest.MonkeyPatch) -> None:
    constructed: list[dict[str, object]] = []

    class _FakeAsyncClient:
        def __init__(self, **kwargs: object) -> None:
            constructed.append(kwargs)

        async def __aenter__(self) -> _FakeAsyncClient:
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def get(self, url: str, *, timeout: float):
            del url, timeout
            return SimpleNamespace(status_code=200)

    monkeypatch.setattr(readiness.httpx, "AsyncClient", _FakeAsyncClient)

    assert await readiness.wait_for_sandbox_ready_async("http://sandbox", timeout=1, headers=HEADERS) is True
    assert constructed[-1]["headers"] == HEADERS
    assert constructed[-1]["headers"] is not HEADERS, "the client gets its own copy"
    assert constructed[-1]["trust_env"] is False

    assert await readiness.wait_for_sandbox_ready_async("http://sandbox", timeout=1) is True
    assert "headers" not in constructed[-1]


def test_aio_sandbox_forwards_request_headers_to_the_client_on_both_transport_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox")
    constructed: list[dict[str, object]] = []
    direct_clients: list[dict[str, object]] = []

    class _FakeSdkClient:
        def __init__(self, **kwargs: object) -> None:
            constructed.append(kwargs)

    class _FakeHttpxClient:
        def __init__(self, **kwargs: object) -> None:
            direct_clients.append(kwargs)

    monkeypatch.setattr(aio_mod, "AioSandboxClient", _FakeSdkClient)
    monkeypatch.setattr(aio_mod.httpx, "Client", _FakeHttpxClient)

    # Control-plane endpoint: a direct client that ignores environment proxies,
    # with the headers on the SDK client that wraps it. The fourth parameter is
    # positional, as upstream declares it.
    aio_mod.AioSandbox("sb", "http://localhost:8080", None, HEADERS)
    assert constructed[-1]["base_url"] == "http://localhost:8080"
    assert constructed[-1]["headers"] == HEADERS
    assert constructed[-1]["headers"] is not HEADERS, "the client gets its own copy"
    assert isinstance(constructed[-1]["httpx_client"], _FakeHttpxClient)
    assert direct_clients[-1] == {"timeout": 600, "follow_redirects": True, "trust_env": False}

    # External endpoint: environment proxy behaviour, no explicit transport.
    aio_mod.AioSandbox("sb", "https://sandbox.example.com", request_headers=HEADERS)
    assert constructed[-1]["headers"] == HEADERS
    assert "httpx_client" not in constructed[-1]

    # No headers means no headers argument at all.
    aio_mod.AioSandbox("sb", "https://sandbox.example.com")
    assert "headers" not in constructed[-1]
    aio_mod.AioSandbox("sb", "http://localhost:8080", None, {})
    assert "headers" not in constructed[-1]
