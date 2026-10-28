"""Exact read-only key lookup over actual SQL and the strict Gateway route."""

import asyncio
import hashlib
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from _run_output_store_test_support import output_store as output_store
from fastapi import FastAPI, HTTPException
from sqlalchemy import event, insert, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.util.concurrency import await_only

from app.gateway import channel_execution_reads
from app.gateway.auth_disabled import AUTH_SOURCE_INTERNAL
from app.gateway.authz import AuthContext
from app.gateway.channel_execution import CHANNEL_EXECUTION_REQUEST_HEADER, CHANNEL_EXECUTION_RESPONSE_HEADER
from app.gateway.internal_auth import INTERNAL_OWNER_USER_ID_HEADER_NAME, get_internal_user
from app.gateway.routers import thread_runs
from deerflow.config import app_config as app_config_module
from deerflow.config.app_config import AppConfig, set_app_config
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.user_context import get_current_user, reset_current_user, set_current_user

OWNER = "human:lookup/owner"
OTHER = "another:lookup/owner"
THREAD = "original-lookup-thread"
KEY = "a" * 32


def normalized(owner=OWNER, thread=THREAD, key=KEY):
    return "http-run:" + hashlib.sha256(f"{owner}\0{thread}\0{key.strip()}".encode()).hexdigest()


class HostAuthentication:
    """An authenticated internal host fixture, not credential qualification."""

    def __init__(self, app, *, state):
        self.app, self.state = app, state

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            owner = httpx.Headers(scope["headers"]).get(INTERNAL_OWNER_USER_ID_HEADER_NAME)
            user = get_internal_user(owner_user_id=owner)
            scope.setdefault("state", {}).update(user=user, auth_source=AUTH_SOURCE_INTERNAL, auth=AuthContext(user=user, permissions=list(self.state.permissions)))
        await self.app(scope, receive, send)


@pytest_asyncio.fixture
async def lookup_environment(output_store, monkeypatch):
    db = output_store
    for name in ("_app_config", "_app_config_path", "_app_config_mtime", "_app_config_signature", "_app_config_is_custom"):
        monkeypatch.setattr(app_config_module, name, getattr(app_config_module, name))
    set_app_config(AppConfig(models=[], sandbox={"use": "deerflow.sandbox.local:LocalSandboxProvider"}))
    async with db.engine.begin() as connection:
        if db.dialect == "sqlite":
            await connection.execute(text("BEGIN"))
        await connection.run_sync(lambda sync: ThreadMetaRow.metadata.create_all(sync, tables=[ThreadMetaRow.__table__]))
    threads = ThreadMetaRepository(db.sf)
    await threads.create(THREAD, user_id=OWNER)
    await threads.create("other-lookup-thread", user_id=OWNER)
    await threads.create("other-owned-thread", user_id=OTHER)
    humans = {owner: SimpleNamespace(id=owner, system_role="user", disabled_at=None, released_at=None, needs_setup=False, oauth_provider=None) for owner in [OWNER, OTHER]}

    async def get_user(owner):
        return humans.get(owner)

    monkeypatch.setattr(channel_execution_reads, "get_local_provider", lambda: SimpleNamespace(get_user=get_user))
    manager = RunManager(db.repository)
    state = SimpleNamespace(permissions=["threads:read", "runs:read"])
    app = FastAPI()
    app.add_middleware(HostAuthentication, state=state)
    app.state.thread_store, app.state.run_manager = threads, manager
    app.include_router(thread_runs.router)
    headers = {INTERNAL_OWNER_USER_ID_HEADER_NAME: OWNER, CHANNEL_EXECUTION_REQUEST_HEADER: "1", "Idempotency-Key": KEY}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://lookup.test", trust_env=False, headers=headers) as client:
            yield SimpleNamespace(**vars(db), app=app, client=client, manager=manager, threads=threads, humans=humans, state=state, url=f"/api/threads/{THREAD}/runs/by-idempotency-key")
    finally:
        await manager.shutdown(timeout=0)


async def seed(e, *, run_id="exact-key-run", owner=OWNER, thread=THREAD, key=KEY):
    await e.repository.put(
        run_id,
        thread_id=thread,
        user_id=owner,
        assistant_id="lead_agent",
        status="success",
        idempotency_key=normalized(owner, thread, key),
        metadata={"channel_turn_id": key},
        kwargs={
            "input": {"messages": [{"role": "user", "content": "Original complete input"}]},
            "channel_execution": {"version": 1, "publication_id": key, "owner_user_id": owner, "thread_id": thread, "assistant_id": "lead_agent", "agent_name": None},
        },
    )


@pytest.mark.asyncio
async def test_exact_indexed_lookup_finds_a_run_behind_more_than_800_newer_rows(lookup_environment):
    e = lookup_environment
    await seed(e)
    now = datetime.now(UTC)
    async with e.engine.begin() as connection:
        await connection.execute(insert(RunRow), [{"run_id": f"newer-{i}", "thread_id": THREAD, "user_id": OWNER, "status": "success", "created_at": now, "updated_at": now} for i in range(805)])
    statements = []

    def observed(_connection, _cursor, statement, _parameters, _context, _many):
        if "FROM runs" in statement:
            statements.append(statement)

    event.listen(e.engine.sync_engine, "before_cursor_execute", observed)
    try:
        response = await e.client.get(e.url)
    finally:
        event.remove(e.engine.sync_engine, "before_cursor_execute", observed)
    assert response.status_code == 200, response.text
    assert response.headers.get(CHANNEL_EXECUTION_RESPONSE_HEADER) == "1"
    row = response.json()
    assert row["run_id"] == "exact-key-run" and row["metadata"] == {"channel_turn_id": KEY}
    assert row["kwargs"]["input"]["messages"][0]["content"] == "Original complete input"
    assert len(statements) == 1 and "runs.idempotency_key =" in statements[0]
    with pytest.raises(IntegrityError):
        async with e.engine.begin() as connection:
            await connection.execute(insert(RunRow).values(run_id="duplicate-key", thread_id=THREAD, user_id=OWNER, status="success", idempotency_key=normalized()))


@pytest.mark.asyncio
async def test_read_uses_the_actual_POST_owner_thread_key_normalization(lookup_environment, monkeypatch):
    e = lookup_environment
    e.state.permissions.append("runs:create")
    received = []

    async def admit(body, thread_id, request, *, idempotency_key=None):
        received.append(idempotency_key)
        owner = request.headers[INTERNAL_OWNER_USER_ID_HEADER_NAME]
        record = await e.manager.create_or_reject(thread_id, "lead_agent", user_id=owner, idempotency_key=idempotency_key, metadata={"channel_turn_id": KEY})
        await e.manager.set_status(record.run_id, RunStatus.success)
        return record

    monkeypatch.setattr(thread_runs, "start_run", admit)
    found = []
    for owner, thread in [(OWNER, THREAD), (OWNER, "other-lookup-thread"), (OTHER, "other-owned-thread")]:
        headers = {INTERNAL_OWNER_USER_ID_HEADER_NAME: owner, "Idempotency-Key": " " + KEY + " "}
        created = await e.client.post(f"/api/threads/{thread}/runs", headers=headers, json={"assistant_id": "lead_agent"})
        assert created.status_code == 200, created.text
        looked_up = await e.client.get(f"/api/threads/{thread}/runs/by-idempotency-key", headers=headers)
        assert looked_up.status_code == 200, looked_up.text
        assert looked_up.json()["run_id"] == created.json()["run_id"]
        found.append(created.json()["run_id"])
    assert len(set(found)) == 3
    assert received == [normalized(owner, thread) for owner, thread in [(OWNER, THREAD), (OWNER, "other-lookup-thread"), (OTHER, "other-owned-thread")]]


@pytest.mark.asyncio
async def test_lookup_is_detached_and_cannot_mark_a_live_creator_as_reused(lookup_environment):
    e = lookup_environment
    record = await e.manager.create_or_reject(THREAD, "lead_agent", user_id=OWNER, idempotency_key=normalized(), metadata={"nested": {"kept": [1]}}, kwargs={"input": {"value": [2]}})
    task = asyncio.current_task()
    record.task = task
    indexes = deepcopy(e.manager._runs_by_thread)
    result = await e.manager.lookup_idempotent_run(THREAD, normalized(), user_id=OWNER)
    assert result is not record and result.store_only and result.task is None
    result.metadata["nested"]["kept"].append(3)
    result.kwargs["input"]["value"].append(4)
    assert record.idempotency_reused is False and record.task is task
    assert e.manager._runs[record.run_id] is record and e.manager._runs_by_thread == indexes
    assert (await e.repository.get(record.run_id, user_id=OWNER))["metadata"] == {"nested": {"kept": [1]}}
    # Avoid presenting this test task to manager shutdown as a worker to cancel.
    record.task = None


@pytest.mark.asyncio
async def test_missing_and_foreign_run_scope_are_the_same_acknowledged_404(lookup_environment, monkeypatch):
    e = lookup_environment
    await seed(e)
    responses = [await e.client.get(e.url, headers={"Idempotency-Key": "missing-key"})]
    row = await e.repository.get_by_idempotency_key(normalized())
    for field, value in [("user_id", OTHER), ("thread_id", "other-owned-thread")]:
        foreign = dict(row, **{field: value})

        async def read(_key):
            return foreign

        monkeypatch.setattr(e.repository, "get_by_idempotency_key", read)
        responses.append(await e.client.get(e.url))
    assert all(r.status_code == 404 and r.headers.get(CHANNEL_EXECUTION_RESPONSE_HEADER) == "1" for r in responses)
    assert all(r.json() == {"detail": "Run not found", "lookup": "idempotency-key"} for r in responses)
    assert all(r.json() == responses[0].json() for r in responses)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["error", "backend-http-404", "unsupported", "missing-store", "wrong-key", "bad-kwargs", "bad-metadata", "bad-status"])
async def test_backend_uncertainty_never_becomes_an_acknowledged_miss_or_cached_success(lookup_environment, monkeypatch, failure):
    e = lookup_environment
    record = await e.manager.create_or_reject(THREAD, "lead_agent", user_id=OWNER, idempotency_key=normalized())
    row = await e.repository.get_by_idempotency_key(normalized())

    async def failed(_key):
        if failure == "backend-http-404":
            raise HTTPException(404, "private-lookup-detail", headers={CHANNEL_EXECUTION_RESPONSE_HEADER: "1"})
        if failure == "unsupported":
            raise NotImplementedError("private-lookup-detail")
        if failure == "error":
            raise RuntimeError("private-lookup-detail")
        field, value = {"wrong-key": ("idempotency_key", "another-key"), "bad-kwargs": ("kwargs", None), "bad-metadata": ("metadata", []), "bad-status": ("status", "malformed")}[failure]
        return dict(row, **{field: value})

    if failure == "missing-store":
        e.manager._store = None
    else:
        monkeypatch.setattr(e.repository, "get_by_idempotency_key", failed)
    response = await e.client.get(e.url)
    assert response.status_code == 503 and CHANNEL_EXECUTION_RESPONSE_HEADER not in response.headers
    assert "private-lookup-detail" not in response.text and record.idempotency_reused is False


@pytest.mark.asyncio
@pytest.mark.parametrize("permission", ["threads:read", "runs:read"])
async def test_both_current_read_ceilings_are_required(lookup_environment, permission):
    e = lookup_environment
    await seed(e)
    e.state.permissions.remove(permission)
    response = await e.client.get(e.url)
    assert response.status_code == 403 and CHANNEL_EXECUTION_RESPONSE_HEADER not in response.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["no-key", "blank-key", "long-key", "duplicate-key", "no-version", "wrong-version"])
async def test_lookup_requires_one_bounded_key_and_the_existing_literal_read_version(lookup_environment, invalid):
    e = lookup_environment
    await seed(e)
    headers = list(e.client.headers.multi_items())
    if invalid in {"no-key", "blank-key", "long-key", "duplicate-key"}:
        headers = [(key, value) for key, value in headers if key.lower() != "idempotency-key"]
        headers += {"no-key": [], "blank-key": [("Idempotency-Key", "   ")], "long-key": [("Idempotency-Key", "x" * 256)], "duplicate-key": [("Idempotency-Key", KEY), ("idempotency-key", "other")]}[invalid]
    else:
        headers = [(key, value) for key, value in headers if key.lower() != CHANNEL_EXECUTION_REQUEST_HEADER.lower()]
        if invalid == "wrong-version":
            headers.append((CHANNEL_EXECUTION_REQUEST_HEADER, "2"))
    response = await e.client.send(httpx.Request("GET", e.client.base_url.join(e.url), headers=headers))
    assert response.status_code == 422 and CHANNEL_EXECUTION_RESPONSE_HEADER not in response.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("positive", [False, True])
@pytest.mark.parametrize("withdrawal", ["human-disabled", "thread-owner"])
async def test_authority_withdrawn_during_read_cannot_release_buffered_result_or_absence(lookup_environment, monkeypatch, positive, withdrawal):
    e = lookup_environment
    if positive:
        await seed(e)
    entered, release = asyncio.Event(), asyncio.Event()
    original = e.repository.get_by_idempotency_key

    async def held(key):
        result = await original(key)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(e.repository, "get_by_idempotency_key", held)
    reading = asyncio.create_task(e.client.get(e.url))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        if withdrawal == "human-disabled":
            e.humans[OWNER].disabled_at = datetime.now(UTC)
        else:
            async with e.engine.begin() as connection:
                await connection.execute(ThreadMetaRow.__table__.update().where(ThreadMetaRow.thread_id == THREAD).values(user_id=OTHER))
        release.set()
        response = await reading
        assert response.status_code == (403 if withdrawal == "human-disabled" else 404)
        assert CHANNEL_EXECUTION_RESPONSE_HEADER not in response.headers
    finally:
        release.set()
        await asyncio.gather(reading, return_exceptions=True)


@pytest.mark.asyncio
async def test_repeated_cancellation_drains_the_actual_key_SELECT_before_restoring_scope(lookup_environment):
    e = lookup_environment
    await seed(e)
    entered, release = asyncio.Event(), asyncio.Event()
    observed = []

    async def hold():
        entered.set()
        await release.wait()

    def after_execute(_connection, _cursor, statement, _parameters, _context, _many):
        if "FROM runs" in statement and "runs.idempotency_key =" in statement:
            observed.append(get_current_user().id)
            await_only(hold())

    event.listen(e.engine.sync_engine, "after_cursor_execute", after_execute)
    prior = SimpleNamespace(id="prior-caller")
    token = set_current_user(prior)
    reading = asyncio.create_task(e.client.get(e.url))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        reading.cancel()
        await asyncio.sleep(0)
        reading.cancel()
        done, _ = await asyncio.wait({reading}, timeout=0.03)
        assert not done, "Cancellation abandoned the actual driver read/session"
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await reading
        assert observed == [OWNER] and get_current_user() is prior
        assert e.engine.sync_engine.pool.checkedout() == 0
    finally:
        release.set()
        await asyncio.gather(reading, return_exceptions=True)
        reset_current_user(token)
        event.remove(e.engine.sync_engine, "after_cursor_execute", after_execute)


@pytest.mark.asyncio
async def test_memory_lookup_also_detaches_the_store_row():
    store = MemoryRunStore()
    await store.put("memory-run", thread_id=THREAD, user_id=OWNER, status="success", idempotency_key=normalized(), metadata={"nested": [1]}, kwargs={"input": [2]})
    manager = RunManager(store)
    result = await manager.lookup_idempotent_run(THREAD, normalized(), user_id=OWNER)
    result.metadata["nested"].append(3)
    result.kwargs["input"].append(4)
    row = await store.get("memory-run")
    assert row["metadata"] == {"nested": [1]} and row["kwargs"] == {"input": [2]}
    assert not manager._runs and result.idempotency_reused is False
