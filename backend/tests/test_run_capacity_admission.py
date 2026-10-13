"""Managed chat capacity is checked before any durable run admission."""

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from test_compose_chat_storage import pinned_chat_volume
from test_gateway_services import _make_start_run_persistence_context, _run_create_request

from app.gateway import chat_storage, services
from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
from deerflow.config.paths import Paths
from deerflow.runtime import RunManager
from deerflow.spaces.chat_paths import ChatFilesFull, ChatStoragePaths


@pytest.fixture(autouse=True)
def configured_model():
    set_app_config(AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}))
    yield
    reset_app_config()


def capacity_context(monkeypatch, tmp_path, *, has_user=True, enabled=True):
    request, run_store, thread_store = _make_start_run_persistence_context()
    request.app.state.storage_spaces_enabled = enabled
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=pinned_chat_volume(tmp_path) if has_user else None)

    async def prepare(_request, thread_id):
        return chat_storage.PreparedChatStorage(paths, paths.mounts(thread_id, user_id="alice"))

    preparation = AsyncMock(side_effect=prepare)
    seed = AsyncMock()
    worker = AsyncMock()
    monkeypatch.setattr(chat_storage, "prepare_chat_request", preparation)
    monkeypatch.setattr(services, "ensure_checkpoint_history_seeded", seed)
    monkeypatch.setattr(services, "resolve_agent_factory", lambda _: object())
    monkeypatch.setattr(services, "run_agent", worker)
    monkeypatch.setattr(os, "fstatvfs", lambda _: SimpleNamespace(f_bavail=0, f_frsize=1, f_favail=0))
    return request, run_store, thread_store, preparation, seed, worker


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_thread", [False, True])
async def test_full_first_run_refuses_before_metadata_checkpoint_or_run(monkeypatch, tmp_path, existing_thread):
    request, store, threads, preparation, seed, worker = capacity_context(monkeypatch, tmp_path)
    if existing_thread:
        await threads.create("empty-chat")
    before = await threads.get("empty-chat")
    with pytest.raises(ChatFilesFull):
        await services.start_run(_run_create_request(), "empty-chat", request)
    preparation.assert_awaited_once()
    seed.assert_not_awaited()
    worker.assert_not_awaited()
    assert await store.list_by_thread("empty-chat") == []
    assert await threads.get("empty-chat") == before
    assert not (tmp_path / "legacy").exists()
    assert not (tmp_path / "slot/data/conversations/empty-chat").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_user,enabled", [(False, True), (True, False)])
async def test_mountless_or_storage_off_run_still_admits(monkeypatch, tmp_path, has_user, enabled):
    request, store, _threads, preparation, seed, worker = capacity_context(monkeypatch, tmp_path, has_user=has_user, enabled=enabled)
    record = await services.start_run(_run_create_request(), "text-chat", request)
    await record.task
    assert len(await store.list_by_thread("text-chat")) == 1
    seed.assert_awaited_once()
    worker.assert_awaited_once()
    assert preparation.await_count == int(enabled)
    assert not (tmp_path / "legacy").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("peer", [False, True])
async def test_admitted_key_retry_bypasses_full_storage_and_compares_input(monkeypatch, tmp_path, peer):
    request, store, _threads, preparation, seed, worker = capacity_context(monkeypatch, tmp_path)
    body = _run_create_request()
    first = await request.app.state.run_manager.create_or_reject("empty-chat", body.assistant_id, kwargs={"input": body.input}, idempotency_key="retry")
    if peer:
        request.app.state.run_manager = RunManager(store=store, worker_id="peer")
    retry = await services.start_run(body, "empty-chat", request, idempotency_key="retry")
    assert retry.run_id == first.run_id
    assert retry.store_only is peer
    if not peer:
        assert retry is first
    with pytest.raises(HTTPException) as changed:
        await services.start_run(_run_create_request("changed"), "empty-chat", request, idempotency_key="retry")
    assert changed.value.status_code == 409
    preparation.assert_not_awaited()
    seed.assert_not_awaited()
    worker.assert_not_awaited()
    assert len(await store.list_by_thread("empty-chat")) == 1


@pytest.mark.asyncio
async def test_peer_admits_same_key_during_capacity_refusal(monkeypatch, tmp_path):
    request, store, _threads, _preparation, seed, worker = capacity_context(monkeypatch, tmp_path)
    owner = RunManager(store=store, worker_id="owner")
    body = _run_create_request()
    admitted = []

    async def overlapping_check(_prepared, thread_id):
        admitted.append(await owner.create_or_reject(thread_id, body.assistant_id, kwargs={"input": body.input}, idempotency_key="retry"))
        raise ChatFilesFull()

    monkeypatch.setattr(chat_storage, "check_chat_thread_capacity", overlapping_check)
    record = await services.start_run(body, "empty-chat", request, idempotency_key="retry")
    assert record.run_id == admitted[0].run_id
    assert record.store_only
    seed.assert_not_awaited()
    worker.assert_not_awaited()
    assert len(await store.list_by_thread("empty-chat")) == 1


@pytest.mark.asyncio
async def test_cancelled_preflight_never_admits_or_interrupts(monkeypatch, tmp_path):
    request, store, _threads, _preparation, seed, worker = capacity_context(monkeypatch, tmp_path)
    manager = request.app.state.run_manager
    first = await manager.create_or_reject("empty-chat")
    entered = asyncio.Event()

    async def held_check(*_args):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(chat_storage, "check_chat_thread_capacity", held_check)
    task = asyncio.create_task(services.start_run(_run_create_request(multitask_strategy="interrupt"), "empty-chat", request))
    await asyncio.wait_for(entered.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    seed.assert_not_awaited()
    worker.assert_not_awaited()
    rows = await store.list_by_thread("empty-chat")
    assert [row["run_id"] for row in rows] == [first.run_id]
    assert rows[0].get("cancel_action") is None


@pytest.mark.asyncio
async def test_owner_denied_before_storage_preflight(monkeypatch, tmp_path):
    request, _store, _threads, preparation, seed, worker = capacity_context(monkeypatch, tmp_path)
    request.state.user = SimpleNamespace(id="bob")
    request.app.state.thread_store.check_access = AsyncMock(return_value=False)
    with pytest.raises(HTTPException) as denied:
        await services.start_run(_run_create_request(), "empty-chat", request)
    assert denied.value.status_code == 404
    preparation.assert_not_awaited()
    seed.assert_not_awaited()
    worker.assert_not_awaited()


@pytest.mark.parametrize("suffix", ["", "/stream", "/wait"])
def test_full_storage_run_http_routes_refuse_before_response_admission(monkeypatch, tmp_path, suffix):
    from uuid import uuid4

    from _router_auth_helpers import make_authed_test_app
    from fastapi.testclient import TestClient

    from app.gateway.auth.models import User
    from app.gateway.refusals import refusal_handler
    from app.gateway.routers import thread_runs
    from deerflow.agent_instances.contract import Refusal

    request, store, threads, _preparation, seed, worker = capacity_context(monkeypatch, tmp_path)
    user = User(id=uuid4(), email="capacity@example.com", password_hash="x", system_role="user")
    asyncio.run(threads.create("empty-chat", user_id=str(user.id)))
    app = make_authed_test_app(user_factory=lambda: user, bind_current_user=True)
    for name, value in vars(request.app.state).items():
        setattr(app.state, name, value)
    app.add_exception_handler(Refusal, refusal_handler)
    app.include_router(thread_runs.router)
    with TestClient(app) as client:
        response = client.post("/api/threads/empty-chat/runs" + suffix, json=_run_create_request().model_dump())
    assert response.status_code == 507, response.text
    assert response.json()["reason"] == "files-full"
    assert "content-location" not in response.headers
    seed.assert_not_awaited()
    worker.assert_not_awaited()
    assert asyncio.run(store.list_by_thread("empty-chat")) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["memory", "sqlite"])
async def test_idempotency_lookup_preserves_ownership_and_peer_tracking(tmp_path, backend):
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.run import RunRepository
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    try:
        if backend == "sqlite":
            await init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path / 'runs.db'}", sqlite_dir=str(tmp_path))
            store = RunRepository(get_session_factory())
        else:
            store = MemoryRunStore()
        owner = RunManager(store=store, worker_id="owner")
        peer = RunManager(store=store, worker_id="peer")
        first = await owner.create_or_reject("thread-a", user_id="alice", idempotency_key="key")
        assert await owner.find_idempotent_run("thread-a", "key", user_id="alice") is first
        found = await peer.find_idempotent_run("thread-a", "key", user_id="alice")
        assert found.run_id == first.run_id and found.store_only and found.idempotency_reused
        assert not peer._runs
        assert await peer.find_idempotent_run("thread-a", "absent", user_id="alice") is None
        for thread_id, user_id in [("thread-b", "alice"), ("thread-a", "bob")]:
            with pytest.raises(RuntimeError, match="different thread or user"):
                await peer.find_idempotent_run(thread_id, "key", user_id=user_id)
    finally:
        if backend == "sqlite":
            await close_engine()


@pytest.mark.asyncio
async def test_idempotency_lookup_rechecks_local_record_after_store_read(monkeypatch):
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    store = MemoryRunStore()
    manager = RunManager(store=store)
    entered, release = asyncio.Event(), asyncio.Event()
    original = store.get_by_idempotency_key

    async def paused(key):
        entered.set()
        await release.wait()
        return await original(key)

    monkeypatch.setattr(store, "get_by_idempotency_key", paused)
    lookup = asyncio.create_task(manager.find_idempotent_run("thread-a", "key"))
    await asyncio.wait_for(entered.wait(), timeout=2)
    first = await manager.create_or_reject("thread-a", idempotency_key="key")
    release.set()
    assert await lookup is first
