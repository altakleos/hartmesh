"""Regression anchors: the login throttle must not block the event loop.

``_login_throttle_policy`` resolves the live policy via ``get_app_config()``,
which stats and re-hashes ``config.yaml`` on every call, and selecting the
store reads the same config. ``login_local`` is an unauthenticated async
endpoint and resolves both on every attempt — including an already-locked
account and a source being refused on the way to its 429. The resolution is
offloaded via ``asyncio.to_thread``; if it regresses onto the event loop, the
strict Blockbuster gate raises ``BlockingError``.
"""

from __future__ import annotations

import time

import pytest
from fastapi import HTTPException
from fastapi.responses import Response
from fastapi.security import OAuth2PasswordRequestForm
from starlette.requests import Request

from app.gateway.auth import login_throttle
from app.gateway.auth.login_throttle import AttemptRecord, MemoryLoginThrottleStore, account_key, source_key
from app.gateway.routers import auth as auth_router

pytestmark = pytest.mark.asyncio

_CLIENT_IP = "203.0.113.9"
_ACCOUNT = "user@example.com"


@pytest.fixture(autouse=True)
def _throttle_state(monkeypatch):
    monkeypatch.delenv("AUTH_TRUSTED_PROXIES", raising=False)
    store = MemoryLoginThrottleStore()
    login_throttle.set_login_throttle_store(store, signature=("memory",))
    yield store
    login_throttle.set_login_throttle_store(None)


class _BareApp:
    class _State:
        redis_tenant_namespace = None

    state = _State()


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/v1/auth/login/local",
            "headers": [],
            "query_string": b"",
            "client": (_CLIENT_IP, 44000),
            "server": ("testserver", 80),
            "app": _BareApp(),
        }
    )


def _form() -> OAuth2PasswordRequestForm:
    return OAuth2PasswordRequestForm(username=_ACCOUNT, password="wrong")


async def test_locked_account_policy_resolution_does_not_block_loop(_throttle_state) -> None:
    """A locked account is hammered: every request resolves the policy on the
    way to its 401, and that resolution must stay off the event loop."""
    _throttle_state._accounts[account_key(_ACCOUNT)] = AttemptRecord(fail_count=9, locked_at=time.time(), locked_duration=3600.0, label=_ACCOUNT)

    with pytest.raises(HTTPException) as exc_info:
        await auth_router.login_local(_request(), Response(), _form(), remember_me=True)

    assert exc_info.value.status_code == 401


async def test_locked_source_policy_resolution_does_not_block_loop(_throttle_state) -> None:
    """Same for a source the spray guard has already locked."""
    _throttle_state._sources[source_key(_CLIENT_IP)] = AttemptRecord(fail_count=999, locked_at=time.time(), locked_duration=3600.0, label=_CLIENT_IP)

    with pytest.raises(HTTPException) as exc_info:
        await auth_router.login_local(_request(), Response(), _form(), remember_me=True)

    assert exc_info.value.status_code == 429


async def test_failed_login_recording_does_not_block_loop(monkeypatch, _throttle_state) -> None:
    """The wrong-password path counts against both keys; counting must happen
    without blocking IO on the loop."""

    class _Provider:
        async def authenticate(self, credentials):
            return None

    monkeypatch.setattr(auth_router, "get_local_provider", lambda: _Provider())
    _throttle_state._accounts[account_key(_ACCOUNT)] = AttemptRecord(fail_count=1, locked_at=0.0, locked_duration=0.0, label=_ACCOUNT)

    with pytest.raises(HTTPException) as exc_info:
        await auth_router.login_local(_request(), Response(), _form(), remember_me=True)

    assert exc_info.value.status_code == 401
    assert _throttle_state._accounts[account_key(_ACCOUNT)].fail_count == 2
