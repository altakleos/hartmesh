"""The prewarm route hands the provider a way to resolve the likely first-turn snapshot.

The route already builds the container in the background. The view that
container mounts is the other half of the first turn's projection cost, and
the route is where the likely snapshot can be resolved -- it has the app
config and the request's own user, which is all the default turn's snapshot
depends on. It passes a *resolver* rather than a snapshot so that a prewarm
which builds no container costs nothing, and so the lease is taken and
released by the same party. What these pin is the route's half: the resolver
is scoped to the request's own identity, and a guess that cannot be made
still prewarms the container -- the snapshot is a bonus, never a gate.
"""

from __future__ import annotations

from _router_auth_helpers import make_authed_test_app
from fastapi.testclient import TestClient

from app.gateway.routers import threads
from deerflow.sandbox.capabilities import WorkspacePrewarm


class _Snapshot:
    """Stands in for a leased snapshot; the provider owns its release."""


class _RecordingProvider(WorkspacePrewarm):
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, object]] = []

    async def prewarm_accepted_skills_async(self, thread_id: str, *, user_id: str, resolve_skill_snapshot=None) -> str | None:
        self.calls.append((thread_id, user_id, None if resolve_skill_snapshot is None else resolve_skill_snapshot()))
        return f"sandbox-{thread_id}"


def _client(monkeypatch, provider: object, *, guess) -> TestClient:
    app = make_authed_test_app()
    app.include_router(threads.router)
    monkeypatch.setattr(threads, "get_sandbox_provider", lambda: provider)
    monkeypatch.setattr(threads, "get_effective_user_id", lambda: "user-7")
    monkeypatch.setattr(threads, "_likely_first_turn_skill_snapshot", guess)
    return TestClient(app)


def test_the_resolver_reaches_the_provider_for_the_requests_own_identity(monkeypatch):
    provider = _RecordingProvider()
    snapshot = _Snapshot()
    asked: list[str] = []

    def guess(user_id: str):
        asked.append(user_id)
        return snapshot

    response = _client(monkeypatch, provider, guess=guess).post("/api/threads/thread-open/workspace/prewarm")

    assert response.status_code == 202
    assert asked == ["user-7"], "the guess is made for the request's own identity"
    assert provider.calls == [("thread-open", "user-7", snapshot)]


def test_a_guess_that_cannot_be_made_still_prewarms_the_container(monkeypatch):
    provider = _RecordingProvider()

    def guess(user_id: str):
        raise RuntimeError("skill storage unreadable")

    response = _client(monkeypatch, provider, guess=guess).post("/api/threads/thread-open/workspace/prewarm")

    assert response.status_code == 202
    assert response.json()["scheduled"] is True
    assert provider.calls == [("thread-open", "user-7", None)]


def test_a_prewarm_that_raises_still_answers_the_client_normally(monkeypatch):
    class _Refusing(WorkspacePrewarm):
        async def prewarm_accepted_skills_async(self, thread_id: str, *, user_id: str, resolve_skill_snapshot=None) -> str | None:
            raise RuntimeError("daemon refused")

    response = _client(monkeypatch, _Refusing(), guess=lambda _user_id: _Snapshot()).post("/api/threads/thread-open/workspace/prewarm")

    assert response.status_code == 202
    assert response.json()["scheduled"] is True
