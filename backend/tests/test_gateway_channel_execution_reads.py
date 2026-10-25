"""Real router/manager reads cannot acknowledge missing or failed authority."""

from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI, HTTPException
from langgraph.store.memory import InMemoryStore
from test_thread_runs_internal_scope import _ScopeAuthMiddleware

from app.gateway.auth.models import User
from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
from app.gateway.routers import thread_runs
from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore
from deerflow.runtime import DisconnectMode, RunRecord, RunStatus
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.store.memory import MemoryRunStore

OWNER = "00000000-0000-0000-0000-00000000000a"
THREAD = "channel-original-thread"
REQUEST_HEADER = "X-DeerFlow-Require-Channel-Execution"
RESPONSE_HEADER = "X-DeerFlow-Channel-Execution-Version"


@pytest_asyncio.fixture
async def environment():
    set_app_config(AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}))
    user = User(id=UUID(OWNER), email="channel-proof@example.com", system_role="user")
    threads = MemoryThreadMetaStore(InMemoryStore())
    await threads.create(THREAD, user_id=OWNER)
    store = MemoryRunStore()
    app = FastAPI()
    app.add_middleware(_ScopeAuthMiddleware, user=user, auth_source=AUTH_SOURCE_SESSION)
    app.state.thread_store = threads
    app.state.run_store = store
    app.state.run_manager = RunManager(store=store)
    app.state.run_event_store = MemoryRunEventStore()
    app.include_router(thread_runs.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway.test", trust_env=False) as client:
        yield SimpleNamespace(app=app, client=client, store=store, threads=threads)
    reset_app_config()


def cached(environment, *, owner=OWNER, kwargs=None):
    record = RunRecord(run_id="channel-real-run", thread_id=THREAD, user_id=owner, assistant_id="lead_agent", status=RunStatus.success, on_disconnect=DisconnectMode.continue_, kwargs={"input": {}} if kwargs is None else kwargs)
    environment.app.state.run_manager._runs[record.run_id] = record
    return record


@pytest.mark.asyncio
async def test_strict_page_backend_failure_is_503_without_acknowledged_empty_page(environment, monkeypatch):
    async def failed(*args, **kwargs):
        raise RuntimeError("synthetic-private-store-detail")

    monkeypatch.setattr(environment.store, "list_by_thread", failed)
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/page", headers={REQUEST_HEADER: "1"})
    assert response.status_code == 503
    assert RESPONSE_HEADER not in response.headers
    assert "synthetic-private-store-detail" not in response.text


@pytest.mark.asyncio
async def test_ordinary_page_preserves_best_effort_compatibility(environment, monkeypatch):
    async def failed(*args, **kwargs):
        raise RuntimeError("unavailable")

    monkeypatch.setattr(environment.store, "list_by_thread", failed)
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/page")
    assert response.status_code == 200
    assert response.json()["data"] == []
    assert RESPONSE_HEADER not in response.headers


@pytest.mark.asyncio
async def test_strict_exact_cached_positive_acknowledges_completed_scoped_lookup(environment):
    record = cached(environment)
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/{record.run_id}", headers={REQUEST_HEADER: "1"})
    assert response.status_code == 200
    assert response.headers.get(RESPONSE_HEADER) == "1"
    assert response.json()["run_id"] == record.run_id


@pytest.mark.asyncio
async def test_strict_exact_foreign_cached_owner_is_not_a_scoped_positive_or_absence(environment):
    record = cached(environment, owner="someone-else")
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/{record.run_id}", headers={REQUEST_HEADER: "1"})
    assert response.status_code == 404
    assert RESPONSE_HEADER not in response.headers


@pytest.mark.asyncio
async def test_strict_exact_actual_miss_acknowledges_only_a_completed_store_lookup(environment):
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/missing", headers={REQUEST_HEADER: "1"})
    assert response.status_code == 404
    assert response.headers.get(RESPONSE_HEADER) == "1"


@pytest.mark.asyncio
@pytest.mark.parametrize("positive", [False, True])
async def test_strict_lookup_requires_configured_authority_even_with_cached_data(environment, positive):
    if positive:
        record = cached(environment)
        identity = record.run_id
    else:
        identity = "missing"
    environment.app.state.run_manager._store = None
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/{identity}", headers={REQUEST_HEADER: "1"})
    assert response.status_code == 503
    assert RESPONSE_HEADER not in response.headers


@pytest.mark.asyncio
async def test_strict_invalid_version_cannot_take_the_ordinary_read_path(environment):
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/page", headers={REQUEST_HEADER: "2"})
    assert response.status_code == 422
    assert RESPONSE_HEADER not in response.headers


@pytest.mark.asyncio
async def test_strict_malformed_cached_projection_is_503_without_positive_ack(environment):
    record = cached(environment)
    record.kwargs = ["not-a-mapping"]
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/{record.run_id}", headers={REQUEST_HEADER: "1"})
    assert response.status_code == 503
    assert RESPONSE_HEADER not in response.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["page", "missing-run"])
@pytest.mark.parametrize("version", ["1", "2"])
async def test_strict_version_and_fixed_failure_precede_missing_manager_dependency(environment, suffix, version):
    environment.app.state.run_manager = None
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/{suffix}", headers={REQUEST_HEADER: version})
    assert response.status_code == (503 if version == "1" else 422)
    if version == "1":
        assert response.json() == {"detail": "Channel execution lookup is unavailable"}
    assert RESPONSE_HEADER not in response.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed", [None, [], "", 0, False])
@pytest.mark.parametrize("page", [False, True])
async def test_strict_reads_never_normalize_malformed_raw_kwargs_into_legacy_absence(environment, malformed, page):
    await environment.store.put("stored", thread_id=THREAD, user_id=OWNER, status="success", kwargs={})
    environment.store._runs["stored"]["kwargs"] = malformed
    suffix = "page" if page else "stored"
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/{suffix}", headers={REQUEST_HEADER: "1"})
    assert response.status_code == 503
    assert RESPONSE_HEADER not in response.headers


@pytest.mark.asyncio
async def test_strict_cached_duplicate_cannot_hide_malformed_persisted_kwargs(environment):
    record = await environment.app.state.run_manager.create(THREAD, user_id=OWNER)
    environment.store._runs[record.run_id]["kwargs"] = []
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/page", headers={REQUEST_HEADER: "1"})
    assert response.status_code == 503
    assert RESPONSE_HEADER not in response.headers


@pytest.mark.asyncio
async def test_ordinary_hydration_keeps_legacy_null_kwargs_compatibility(environment):
    await environment.store.put("legacy", thread_id=THREAD, user_id=OWNER, status="success")
    environment.store._runs["legacy"]["kwargs"] = None
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/legacy")
    assert response.status_code == 200
    assert response.json()["kwargs"] == {}
    assert RESPONSE_HEADER not in response.headers


@pytest.mark.asyncio
async def test_backend_http_exception_cannot_impersonate_acknowledged_absence(environment, monkeypatch):
    async def failed(*args, **kwargs):
        raise HTTPException(404, "private-backend-detail", headers={RESPONSE_HEADER: "1"})

    monkeypatch.setattr(environment.store, "get", failed)
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/unknown", headers={REQUEST_HEADER: "1"})
    assert response.status_code == 503
    assert RESPONSE_HEADER not in response.headers
    assert "private-backend-detail" not in response.text


@pytest.mark.asyncio
async def test_missing_event_authority_has_the_fixed_backend_refusal(environment):
    record = cached(environment)
    environment.app.state.run_event_store = None
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/{record.run_id}/messages", headers={REQUEST_HEADER: "1"})
    assert response.status_code == 503
    assert response.json() == {"detail": "Channel execution lookup is unavailable"}
    assert RESPONSE_HEADER not in response.headers


def source_row(**changes):
    return {
        "thread_id": THREAD,
        "run_id": "channel-real-run",
        "seq": 1,
        "category": "message",
        "event_type": "llm.ai.response",
        "content": {"type": "ai", "id": "actual-root-message", "content": "Synthetic answer"},
        "metadata": {},
        "created_at": "2026-10-25T00:00:00+00:00",
        **changes,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["missing-fields", "wrong-category", "missing-content", "null-content", "list-content", "integer-content", "bad-metadata", "bad-time", "duplicate", "reverse", "after-cursor", "before-cursor"])
async def test_strict_source_pages_require_complete_ordered_scoped_envelopes(environment, shape):
    record = cached(environment)
    rows = [source_row()]
    query = {}
    if shape == "missing-fields":
        rows = [{"thread_id": THREAD, "run_id": record.run_id, "seq": 1}]
    elif shape == "missing-content":
        rows[0].pop("content")
    elif shape in {"null-content", "list-content", "integer-content"}:
        rows[0]["content"] = {"null-content": None, "list-content": [], "integer-content": 7}[shape]
    elif shape == "wrong-category":
        rows[0]["category"] = "trace"
    elif shape == "bad-metadata":
        rows[0]["metadata"] = []
    elif shape == "bad-time":
        rows[0]["created_at"] = "not-a-timestamp"
    elif shape == "duplicate":
        rows.append(source_row())
    elif shape == "reverse":
        rows = [source_row(seq=2), source_row(seq=1)]
    elif shape == "after-cursor":
        query = {"after_seq": 1}
    else:
        query = {"before_seq": 1}

    async def messages(*args, **kwargs):
        return rows

    environment.app.state.run_event_store = SimpleNamespace(list_messages_by_run=messages)
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/{record.run_id}/messages", params=query, headers={REQUEST_HEADER: "1"})
    assert response.status_code == 503
    assert RESPONSE_HEADER not in response.headers


@pytest.mark.asyncio
async def test_complete_source_page_acknowledges_lookup_without_claiming_finality(environment):
    record = cached(environment)
    await environment.app.state.run_event_store.put(thread_id=THREAD, run_id=record.run_id, category="message", event_type="llm.ai.response", content={"type": "ai", "id": "actual-id", "content": "Synthetic answer"})
    response = await environment.client.get(f"/api/threads/{THREAD}/runs/{record.run_id}/messages", headers={REQUEST_HEADER: "1"})
    assert response.status_code == 200
    assert response.headers.get(RESPONSE_HEADER) == "1"
    assert response.json()["data"][0]["content"]["id"] == "actual-id"
