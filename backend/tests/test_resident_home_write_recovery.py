"""Real run admission, resident confirmation and completion with a recorded model."""

import asyncio
import errno
import os
import threading
from contextlib import asynccontextmanager
from contextvars import ContextVar
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, operation
from langchain_core.messages import AIMessage
from sqlalchemy import event
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_agent_work_records import work as work
from test_gateway_resident_reconciliation import build
from test_gateway_services import _make_start_run_persistence_context
from test_native_create_provider import fixture as fixture
from test_run_worker_delivery import _make_bridge
from test_work_execution import execution as execution

from app.gateway import services
from app.gateway.authz import AuthContext
from app.gateway.run_models import RunCreateRequest
from deerflow.agent_instances import runtime
from deerflow.agent_instances.bootstrap import BootstrapResult
from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.spaces.filesystem import ConfinedFilesystem
from deerflow.spaces.incarnations import BINDING_MARKER
from deerflow.spaces.service import SpaceOperationPending


@pytest_asyncio.fixture
async def turns(execution, fixture, monkeypatch):
    module, app, host, mounts, adapter, claim = await build(execution, fixture, monkeypatch)
    service, admitted, _, sessions = execution
    agents, authority = service.agents, admitted.authority
    second = await create(agents, custody="company")
    chat = await authority.create(actor=ALICE, instance_id=second.id, creation_id=operation())
    request, _, _ = _make_start_run_persistence_context()
    state = request.app.state
    state.agent_conversations = authority
    state.thread_store = ThreadMetaRepository(sessions, instance_authority=authority)
    state.stream_bridge = _make_bridge()
    host.registry.run_manager = state.run_manager
    app.state.run_manager = state.run_manager
    app.state.storage_spaces_enabled = True
    app.state.chat_storage_host = SimpleNamespace(admission=host.admission)
    host.coordinator.result = BootstrapResult(True)
    host.admission.open(host.provider)
    host._startup_settled = True
    agents.execution_probe = lambda instance_id: module.execution_state(app, instance_id)
    monkeypatch.setattr(module, "get_agents_api_config", lambda: SimpleNamespace(enabled=True, ai_assistants_per_user=2, ai_employees=2))
    monkeypatch.setattr(module, "hold_present", lambda: False)
    monkeypatch.setattr(runtime, "qualified_attachments", AsyncMock(return_value=(host.provider, mounts)))
    monkeypatch.setattr(agents.files, "quota", AsyncMock(return_value={"available_bytes": 2 << 30, "available_inodes": 50000}))
    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}})

    async def open_client(_execution, _provider, attached, _skills, app_config, **_kwargs):
        sandbox = SimpleNamespace(id=attached.id, requires_container_recycle=False, execution_outcome_uncertain=False)
        return SimpleNamespace(app_config=app_config, sandbox=sandbox, skill_revision="e" * 64, close=lambda: None)

    class Graph:
        async def aget_state(self, config):
            return SimpleNamespace(config={}, values={}, metadata={}, next=())

        async def astream(self, graph_input, config=None, **kwargs):
            yield {"messages": [AIMessage("Done")]}

    monkeypatch.setattr(runtime, "_prepare_client", open_client)
    monkeypatch.setattr(services, "resolve_agent_factory", lambda *_: lambda **kwargs: Graph())
    user = SimpleNamespace(id=ALICE.subject_id, system_role="user", role="user")
    request.state.user, request.state.auth_source = user, "session"
    request.state.auth = AuthContext(user, ["runs:create", "runs:read"])

    async def turn(thread):
        request.url = f"http://test/api/threads/{thread}/runs"
        record = await services.start_run(RunCreateRequest(input={"messages": [{"role": "user", "content": "work"}]}), thread, request)
        await record.task
        return record

    token = set_current_user(user)
    set_app_config(config)
    try:
        assert await host.reconcile()
        yield SimpleNamespace(turn=turn, other=chat["thread_id"], original=admitted, files=agents.files, host=host, claim=claim, adapter=adapter, sessions=sessions)
    finally:
        reset_current_user(token)
        reset_app_config()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["between-transactions", "file-io"])
async def test_unrelated_turn_survives_an_abandoned_home_writer(turns, monkeypatch, stage):
    files = turns.files
    home = turns.original.instance.home_id
    key, entered, release = operation(), asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    exited_gap = asyncio.Event()
    admit = files.registry.admitted

    @asynccontextmanager
    async def observe_admission(**kwargs):
        if kwargs.get("operation_id") == (home, key) and stage == "between-transactions" and exited_gap.is_set():
            entered.set()
            await asyncio.to_thread(release.wait, 20)
        async with admit(**kwargs) as admitted:
            yield admitted
        if kwargs.get("operation_id") == (home, key):
            exited_gap.set()

    def interrupt(_filesystem, _path, _content, **_kwargs):
        if stage == "file-io":
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(20)
        raise OSError(errno.EIO, "Interrupted write")

    monkeypatch.setattr(files.registry, "admitted", observe_admission)
    monkeypatch.setattr(ConfinedFilesystem, "write_atomic", interrupt)
    sqlite_locked = stage == "file-io" and turns.sessions.kw["bind"].dialect.name == "sqlite"
    turning = ContextVar("unrelated_turn", default=False)
    waiting_for_writer = asyncio.Event()
    engine = turns.sessions.kw["bind"].sync_engine

    def observe_sql(_connection, _cursor, statement, _params, _context, _many):
        if turning.get() and statement == "BEGIN IMMEDIATE":
            waiting_for_writer.set()

    async def other_turn():
        turning.set(True)
        return await turns.turn(turns.other)

    event.listen(engine, "before_cursor_execute", observe_sql)
    writer = asyncio.create_task(files.write(actor=turns.original.instance.principal, space_id=home, expected_generation=turns.original.home_generation, operation_id=key, path="note", content=b"write", create=True))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        other = asyncio.create_task(other_turn())
        if sqlite_locked:
            # SQLite's BEGIN IMMEDIATE serializes writers across different Homes.
            await asyncio.wait_for(waiting_for_writer.wait(), 10)
            assert not other.done()
        else:
            result = await asyncio.wait_for(asyncio.shield(other), 10)
            assert result.status == RunStatus.success, result.error
    finally:
        release.set()
        event.remove(engine, "before_cursor_execute", observe_sql)
        with pytest.raises(SpaceOperationPending):
            await writer
    assert (await asyncio.wait_for(other, 10)).status == RunStatus.success
    assert await turns.host.reconcile()
    result = await turns.turn(turns.other)
    assert result.status == RunStatus.success, result.error
    async with turns.sessions() as session:
        assert (await session.get(SpaceFileOperationRow, (home, key))).phase == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize("pending", [False, True])
@pytest.mark.parametrize("change", ["marker", "owner"])
async def test_unverified_binding_still_withholds_all_workspace_admission(turns, pending, change, monkeypatch):
    from deerflow.spaces import incarnations

    home = turns.original.instance.home_id
    async with turns.sessions.begin() as session:
        parent = await session.get(SpaceRow, home)
        backing = await session.get(SpaceBackingRow, parent.backing_handle)
        if pending:
            session.add(SpaceFileOperationRow(space_id=home, generation=parent.generation, operation_id=operation(), actor_kind="human", actor_id="alice", phase="pending", request={"action": "put"}))
        volume = turns.files.catalog.verified[backing.slot_id]
        marker = volume.control_path / BINDING_MARKER
        handle = backing.backing_handle
    with monkeypatch.context() as observation:
        if change == "marker":
            marker.write_text(operation(), encoding="ascii")
        else:
            # The CI bundle also checks a real uid change. Here the file
            # remains accessible to isolate ownership from access failures.
            original = volume.data_path.stat()

            def fstat(fd):
                info = os.fstat(fd)
                return SimpleNamespace(st_uid=original.st_uid + 1) if os.path.samestat(original, info) else info

            observation.setattr(incarnations, "os", SimpleNamespace(**(vars(os) | {"fstat": fstat})))
        try:
            assert not await turns.host.reconcile()
            assert not turns.host.workspace_qualified
            assert turns.claim.container_id in turns.adapter.containers
            assert (await turns.host.app.state.agent_instances.execution_probe(None)).reason == "storage-unavailable"
        finally:
            if change == "marker":
                marker.write_text(handle, encoding="ascii")
    assert await turns.host.reconcile()
    assert (await turns.turn(turns.other)).status == RunStatus.success


@pytest.mark.asyncio
async def test_refused_turns_do_not_renew_a_retained_workspace(turns):
    admitted = turns.original
    home = admitted.instance.home_id
    registry = turns.host.registry
    now = 0
    registry.clock = lambda: now
    registry.idle_timeout = 1800
    assert await turns.host.reconcile()
    now = 5
    assert await turns.host.reconcile()
    assert registry.homes[home].last_release == now
    async with turns.sessions.begin() as session:
        session.add(SpaceFileOperationRow(space_id=home, operation_id=operation(), actor_kind="human", actor_id="alice", generation=admitted.home_generation, phase="pending", request={"action": "remove", "path": "old"}))
    for now in (605, 1205, 1806):
        assert await turns.host.reconcile()
        record = await turns.turn(admitted.thread_id)
        assert record.status == RunStatus.error and record.stop_reason == "storage-busy", record.error
        await registry.reap()
    assert turns.claim.container_id not in turns.adapter.containers
    assert await turns.files.settle_interrupted() == 1
    assert (await turns.turn(admitted.thread_id)).status == RunStatus.success
