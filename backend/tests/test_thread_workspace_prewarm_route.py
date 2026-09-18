"""``POST /api/threads/{id}/workspace/prewarm``: the sandbox built while the person types.

The route is thin on purpose: it names the thread, takes the request's own
user identity, and hands the provider's prewarm capability to the background.
What these tests pin is the contract the client relies on -- it always answers
quickly, it never surfaces a build failure, it says honestly when it built
nothing -- and the identity the build is scoped to.
"""

from __future__ import annotations

from _router_auth_helpers import make_authed_test_app
from fastapi.testclient import TestClient

from app.gateway.routers import threads
from deerflow.sandbox.capabilities import WorkspacePrewarm


class _PrewarmingProvider(WorkspacePrewarm):
    def __init__(self, *, fail: bool = False, park: bool = True) -> None:
        self.calls: list[tuple[str, str]] = []
        self.fail = fail
        self.park = park

    async def prewarm_accepted_skills_async(self, thread_id: str, *, user_id: str) -> str | None:
        self.calls.append((thread_id, user_id))
        if self.fail:
            raise RuntimeError("daemon refused")
        return f"sandbox-{thread_id}" if self.park else None


class _PlainProvider:
    """A provider with no prewarm capability at all."""


def _client(monkeypatch, provider: object, *, user_id: str | None = "user-7") -> TestClient:
    app = make_authed_test_app()
    app.include_router(threads.router)
    monkeypatch.setattr(threads, "get_sandbox_provider", lambda: provider)
    monkeypatch.setattr(threads, "get_effective_user_id", lambda: user_id)
    return TestClient(app)


def test_schedules_the_build_for_the_requests_own_identity(monkeypatch):
    provider = _PrewarmingProvider()

    response = _client(monkeypatch, provider).post("/api/threads/thread-open/workspace/prewarm")

    assert response.status_code == 202
    assert response.json() == {"thread_id": "thread-open", "scheduled": True, "reason": None}
    # TestClient runs background tasks before returning, so the call is visible.
    assert provider.calls == [("thread-open", "user-7")]


def test_a_provider_without_the_capability_is_not_an_error(monkeypatch):
    response = _client(monkeypatch, _PlainProvider()).post("/api/threads/thread-open/workspace/prewarm")

    assert response.status_code == 202
    assert response.json() == {"thread_id": "thread-open", "scheduled": False, "reason": "unsupported"}


def test_an_unavailable_provider_is_not_an_error(monkeypatch):
    def _no_provider():
        raise RuntimeError("sandbox not configured")

    app = make_authed_test_app()
    app.include_router(threads.router)
    monkeypatch.setattr(threads, "get_sandbox_provider", _no_provider)

    response = TestClient(app).post("/api/threads/thread-open/workspace/prewarm")

    assert response.status_code == 202
    assert response.json()["scheduled"] is False
    assert response.json()["reason"] == "unavailable"


def test_a_failed_build_never_reaches_the_client(monkeypatch):
    provider = _PrewarmingProvider(fail=True)

    response = _client(monkeypatch, provider).post("/api/threads/thread-fail/workspace/prewarm")

    assert response.status_code == 202
    assert response.json()["scheduled"] is True
    assert provider.calls == [("thread-fail", "user-7")]


def test_a_build_that_parks_nothing_is_still_a_normal_outcome(monkeypatch):
    provider = _PrewarmingProvider(park=False)

    response = _client(monkeypatch, provider).post("/api/threads/thread-busy/workspace/prewarm")

    assert response.status_code == 202
    assert provider.calls == [("thread-busy", "user-7")]


def test_no_identity_means_no_build(monkeypatch):
    provider = _PrewarmingProvider()

    response = _client(monkeypatch, provider, user_id=None).post("/api/threads/thread-anon/workspace/prewarm")

    assert response.status_code == 202
    assert response.json()["reason"] == "anonymous"
    assert provider.calls == []


def test_an_invalid_thread_id_is_refused_before_anything_runs(monkeypatch):
    provider = _PrewarmingProvider()

    response = _client(monkeypatch, provider).post("/api/threads/not%2Fa%2Fthread/workspace/prewarm")

    assert response.status_code in (404, 422)
    assert provider.calls == []
