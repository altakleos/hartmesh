from __future__ import annotations

import asyncio
from uuid import UUID

from _router_auth_helpers import make_authed_test_app
from fastapi.testclient import TestClient

from app.gateway.auth.models import User
from app.gateway.routers import thread_runs
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.store.memory import MemoryRunStore


def test_evidence_summary_endpoint_authorizes_and_projects_legacy_safely() -> None:
    run_store = MemoryRunStore()
    event_store = MemoryRunEventStore()
    manager = RunManager(store=run_store)
    asyncio.run(
        run_store.put(
            "run-legacy",
            thread_id="thread-legacy",
            status="success",
        )
    )
    asyncio.run(
        event_store.put(
            thread_id="thread-legacy",
            run_id="run-legacy",
            event_type="run.delivery",
            category="outputs",
            content={
                "by_tool": {"present_files": ["/secret/internal/path.txt"]},
                "raw_prompt": "must-not-escape",
            },
        )
    )
    app = make_authed_test_app(
        user_factory=lambda: User(
            id=UUID("00000000-0000-0000-0000-000000000123"),
            email="evidence@example.com",
            password_hash="x",
            system_role="user",
        )
    )
    app.state.run_store = run_store
    app.state.run_event_store = event_store
    app.state.run_manager = manager
    app.include_router(thread_runs.router)

    with TestClient(app) as client:
        response = client.get("/api/threads/thread-legacy/runs/run-legacy/evidence")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "private, no-store"
    body = response.json()
    assert body["schema"] == "hartmesh.run-evidence-summary"
    assert body["sections"]["policy"]["state"] == "legacy"
    assert body["sections"]["artifacts"]["data"]["file_count"] == 1
    assert body["qualification"]["state"] == "legacy"
    assert "run-legacy" not in body["overview"]["run_ref"]
    assert "must-not-escape" not in response.text
    assert "/secret/internal/path.txt" not in response.text


def _evidence_app(*, owner_check_passes: bool = True) -> tuple[object, MemoryRunStore, MemoryRunEventStore]:
    run_store = MemoryRunStore()
    event_store = MemoryRunEventStore()
    asyncio.run(
        run_store.put(
            "run-legacy",
            thread_id="thread-legacy",
            status="success",
        )
    )
    app = make_authed_test_app(
        user_factory=lambda: User(
            id=UUID("00000000-0000-0000-0000-000000000123"),
            email="evidence@example.com",
            password_hash="x",
            system_role="user",
        ),
        owner_check_passes=owner_check_passes,
    )
    app.state.run_store = run_store
    app.state.run_event_store = event_store
    app.state.run_manager = RunManager(store=run_store)
    app.include_router(thread_runs.router)
    return app, run_store, event_store


def test_evidence_summary_endpoint_denies_unowned_threads_generically() -> None:
    app, _run_store, _event_store = _evidence_app(owner_check_passes=False)

    with TestClient(app) as client:
        response = client.get("/api/threads/thread-legacy/runs/run-legacy/evidence")

    assert response.status_code in (403, 404)
    assert "hartmesh.run-evidence-summary" not in response.text
    assert "run-legacy" not in response.text.replace("thread-legacy", "")


def test_trace_headers_do_not_influence_evidence_identity_or_authorization() -> None:
    app, _run_store, _event_store = _evidence_app()

    with TestClient(app) as client:
        first = client.get(
            "/api/threads/thread-legacy/runs/run-legacy/evidence",
            headers={"X-Request-ID": "trace-alpha", "X-Trace-Id": "trace-alpha"},
        )
        second = client.get(
            "/api/threads/thread-legacy/runs/run-legacy/evidence",
            headers={"X-Request-ID": "trace-bravo", "X-Trace-Id": "trace-bravo"},
        )

    assert first.status_code == 200
    body = first.json()
    assert body == second.json()
    assert "trace-alpha" not in first.text
    assert "trace-bravo" not in second.text


def test_bundle_errors_map_to_honest_artifact_states(monkeypatch) -> None:
    from deerflow.runtime.run_evidence import RunEvidenceBundleError

    app, _run_store, _event_store = _evidence_app()

    async def _unsupported(*args, **kwargs):
        raise RunEvidenceBundleError("evidence_export_unavailable")

    monkeypatch.setattr(thread_runs, "_run_evidence_snapshot", _unsupported)
    with TestClient(app) as client:
        response = client.get("/api/threads/thread-legacy/runs/run-legacy/evidence")
    assert response.status_code == 200
    assert response.json()["sections"]["artifacts"]["data"]["bundle_state"] == "unsupported"

    async def _failed(*args, **kwargs):
        raise RunEvidenceBundleError("evidence_cross_link_invalid")

    monkeypatch.setattr(thread_runs, "_run_evidence_snapshot", _failed)
    with TestClient(app) as client:
        response = client.get("/api/threads/thread-legacy/runs/run-legacy/evidence")
    assert response.status_code == 200
    assert response.json()["sections"]["artifacts"]["data"]["bundle_state"] == "error"
    assert "evidence_cross_link_invalid" not in response.text
