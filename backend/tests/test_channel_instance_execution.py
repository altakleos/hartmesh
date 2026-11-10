"""Channel admission composes real instance, grant, conversation and Home SQL.

The worker is captured at admission here; output execution has separate real
SDK/worker/scanner coverage in test_instance_output_capture.
"""

import asyncio
from types import SimpleNamespace

import pytest
from _channel_execution_test_support import gateway_environment as gateway_environment
from fastapi import HTTPException
from test_agent_conversations import conversations
from test_agent_instances import create, definition
from test_agent_instances import instances as instances
from test_gateway_channel_execution_admission import PUBLICATION, body, launched

from deerflow.agent_instances.contract import AgentPermission, ExecutionUnavailable
from deerflow.agent_instances.conversations import AgentExecution
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.spaces.contract import PrincipalRef, ResolvedPrincipal


async def bind_instance(e, instances, monkeypatch):
    agents, authority, threads, sf = await conversations(instances)
    actor = PrincipalRef("human", e.owner_id)
    original = agents.directory.human

    async def human(reference):
        return ResolvedPrincipal(actor, True) if reference == actor else await original(reference)

    monkeypatch.setattr(agents.directory, "human", human)
    monkeypatch.setattr(agents.files.registry._resolver, "_human", human)
    view = await create(agents, actor=actor, snapshot=definition(owner=e.owner_id, name="adopted-helper"))
    chat = await authority.create(actor=actor, instance_id=view.id)
    e.request.app.state.thread_store = threads
    e.request.app.state.agent_conversations = authority
    e.thread_id = chat["thread_id"]
    return agents, authority, view, sf


@pytest.mark.asyncio
async def test_instance_channel_run_retains_host_execution_and_human_receipt(instances, gateway_environment, monkeypatch):
    e = gateway_environment
    _agents, authority, view, _sf = await bind_instance(e, instances, monkeypatch)
    record = await launched(e, body())
    assert record.user_id == e.owner_id
    assert record.kwargs["channel_execution"] == {"version": 1, "publication_id": PUBLICATION, "owner_user_id": e.owner_id, "thread_id": e.thread_id, "assistant_id": "lead_agent", "agent_name": None}
    ctx = e.worker_inputs[0]["kwargs"]["ctx"]
    assert type(ctx.agent_execution) is AgentExecution
    assert ctx.agent_execution.instance.id == view.id
    assert ctx.agent_execution.authority is authority
    assert ctx.agent_execution.requester.subject_id == e.owner_id
    assert ctx.agent_execution.definition.config["name"] == "adopted-helper"
    assert (await launched(e, body())).run_id == record.run_id
    assert len(e.worker_inputs) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("refused", ["use", "execution"])
async def test_instance_channel_checks_use_before_execution_readiness(instances, gateway_environment, monkeypatch, refused):
    e = gateway_environment
    agents, _authority, view, sf = await bind_instance(e, instances, monkeypatch)
    probes = []

    async def probe(identity):
        probes.append(identity)
        return SimpleNamespace(execution="unavailable", reason="operator-hold")

    agents.execution_probe = probe
    if refused == "use":
        async with sf.begin() as session:
            (await session.get(AgentInstanceGrantRow, (view.id, e.owner_id))).permissions = int(AgentPermission.INSPECT)
        with pytest.raises(HTTPException) as failure:
            await launched(e, body())
        assert failure.value.status_code == 404 and not probes
    else:
        with pytest.raises(ExecutionUnavailable):
            await launched(e, body())
        assert probes == [view.id]
    assert not e.worker_inputs


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["grant", "authority"])
@pytest.mark.parametrize("outcome", ["data", "missing"])
async def test_strict_instance_read_refreshes_authority_after_awaited_source(instances, gateway_environment, monkeypatch, change, outcome):
    from app.gateway.channel_execution_reads import ChannelReadMissing, channel_read_scope

    e = gateway_environment
    _agents, _authority, view, sf = await bind_instance(e, instances, monkeypatch)
    # Use-only readers retain their own unprotected conversation; no readiness
    # probe or workspace admission is needed for retained source reads.
    async with sf.begin() as session:
        (await session.get(AgentInstanceGrantRow, (view.id, e.owner_id))).permissions = int(AgentPermission.USE)
    async with channel_read_scope(e.request, e.thread_id) as owner:
        assert owner == e.owner_id
    with pytest.raises(HTTPException) as failure:
        async with channel_read_scope(e.request, e.thread_id):
            if change == "grant":
                async with sf.begin() as session:
                    await session.delete(await session.get(AgentInstanceGrantRow, (view.id, e.owner_id)))
            else:
                e.request.app.state.agent_conversations = None
            if outcome == "missing":
                raise ChannelReadMissing()
    assert failure.value.status_code == (404 if change == "grant" else 503)
    assert not failure.value.headers


@pytest.mark.asyncio
async def test_strict_instance_read_does_not_require_use_or_execution_readiness(instances, gateway_environment, monkeypatch):
    from app.gateway.channel_execution_reads import channel_read_scope

    e = gateway_environment
    agents, _authority, view, sf = await bind_instance(e, instances, monkeypatch)
    async with sf.begin() as session:
        (await session.get(AgentInstanceGrantRow, (view.id, e.owner_id))).permissions = int(AgentPermission.INSPECT)

    async def cannot_probe(_identity):
        raise AssertionError("Retained reads do not execute")

    agents.execution_probe = cannot_probe
    async with channel_read_scope(e.request, e.thread_id) as owner:
        assert owner == e.owner_id


@pytest.mark.asyncio
async def test_real_binding_authority_retires_a_previous_memory_proof(gateway_environment):
    from app.gateway.channel_execution_reads import OrdinaryMemoryThreads, channel_read_scope

    e = gateway_environment
    # A mixed host has a Memory metadata store and an actual SQL binding reader.
    # It must not downgrade to no-instance evidence after losing that reader.
    proof = OrdinaryMemoryThreads(e.thread_store)
    e.request.app.state.channel_memory_threads = proof
    async with channel_read_scope(e.request, e.thread_id):
        pass
    e.request.app.state.agent_conversations = None
    with pytest.raises(HTTPException) as failure:
        async with channel_read_scope(e.request, e.thread_id):
            pytest.fail("Lost SQL authority cannot become an ordinary thread")
    assert failure.value.status_code == 503


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["memory_retired", "memory_replaced", "sql_removed", "sql_replaced"])
async def test_final_thread_read_cannot_outlive_its_installed_authority(gateway_environment, monkeypatch, change):
    from app.gateway.channel_execution_reads import OrdinaryMemoryThreads, channel_read_scope
    from deerflow.agent_instances.conversations import AgentConversations

    e = gateway_environment
    if change.startswith("memory"):
        e.request.app.state.agent_conversations = None
        e.request.app.state.channel_memory_threads = OrdinaryMemoryThreads(e.thread_store)
    read = e.thread_store.get
    calls = 0

    async def changed_after_read(*args, **kwargs):
        nonlocal calls
        value = await read(*args, **kwargs)
        calls += 1
        if calls == 2:
            if change == "memory_retired":
                e.request.app.state.channel_memory_threads.retire()
            elif change == "memory_replaced":
                e.request.app.state.channel_memory_threads = OrdinaryMemoryThreads(e.thread_store)
            elif change == "sql_removed":
                e.request.app.state.agent_conversations = None
            else:
                e.request.app.state.agent_conversations = AgentConversations(None, session_factory=e.sf)
        return value

    monkeypatch.setattr(e.thread_store, "get", changed_after_read)
    with pytest.raises(HTTPException) as failure:
        async with channel_read_scope(e.request, e.thread_id):
            pass
    assert failure.value.status_code == 404 and not failure.value.headers


@pytest.mark.asyncio
async def test_final_thread_read_precedes_current_instance_grant_check(instances, gateway_environment, monkeypatch):
    from app.gateway.channel_execution_reads import channel_read_scope

    e = gateway_environment
    _agents, _authority, view, sf = await bind_instance(e, instances, monkeypatch)
    store = e.request.app.state.thread_store
    read, calls = store.get, []

    async def revoke_after_read(*args, **kwargs):
        value = await read(*args, **kwargs)
        calls.append(True)
        if len(calls) == 2:
            async with sf.begin() as session:
                await session.delete(await session.get(AgentInstanceGrantRow, (view.id, e.owner_id)))
        return value

    monkeypatch.setattr(store, "get", revoke_after_read)
    with pytest.raises(HTTPException) as failure:
        async with channel_read_scope(e.request, e.thread_id):
            pass
    assert failure.value.status_code == 404 and not failure.value.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["exact", "page", "messages", "missing"])
async def test_instance_http_reads_never_acknowledge_after_source_time_revocation(instances, gateway_environment, monkeypatch, route):
    import httpx
    from fastapi import FastAPI
    from test_thread_runs_internal_scope import _ScopeAuthMiddleware

    from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
    from app.gateway.routers import thread_runs

    e = gateway_environment
    _agents, _authority, view, sf = await bind_instance(e, instances, monkeypatch)
    record = await launched(e, body())
    app = FastAPI()
    app.add_middleware(_ScopeAuthMiddleware, user=e.owner, auth_source=AUTH_SOURCE_SESSION)
    for key, value in vars(e.request.app.state).items():
        setattr(app.state, key, value)
    app.include_router(thread_runs.router)
    method = "list_by_thread" if route == "page" else "get"
    read = getattr(e.manager, method)

    async def revoke_after_read(*args, **kwargs):
        result = await read(*args, **kwargs)
        async with sf.begin() as session:
            grant = await session.get(AgentInstanceGrantRow, (view.id, e.owner_id))
            if grant is not None:
                await session.delete(grant)
        return result

    monkeypatch.setattr(e.manager, method, revoke_after_read)
    suffix = "page" if route == "page" else "missing" if route == "missing" else record.run_id + ("/messages" if route == "messages" else "")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
        response = await client.get(f"/api/threads/{e.thread_id}/runs/{suffix}", headers={"X-DeerFlow-Require-Channel-Execution": "1"})
    assert response.status_code == 404
    assert "X-DeerFlow-Channel-Execution-Version" not in response.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["initial", "final"])
async def test_cancelled_channel_binding_read_drains_the_actual_sql_scope(gateway_environment, phase):
    from sqlalchemy import event
    from sqlalchemy.util import await_only

    from app.gateway.channel_execution_reads import channel_read_scope
    from deerflow.runtime.user_context import get_current_user, reset_current_user, set_current_user

    e = gateway_environment
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    reads, owners, restored = [], [], []
    prior = SimpleNamespace(id="prior-owner")
    token = set_current_user(prior)

    async def hold():
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    def after_cursor_execute(_connection, _cursor, statement, _parameters, _context, _executemany):
        if "SELECT" not in statement.upper() or "FROM AGENT_CONVERSATIONS" not in statement.upper():
            return
        reads.append(True)
        if len(reads) == (1 if phase == "initial" else 2):
            owners.append(str(get_current_user().id))
            await_only(hold())

    async def read():
        try:
            async with channel_read_scope(e.request, e.thread_id):
                pass
        finally:
            restored.append(get_current_user())

    event.listen(e.engine.sync_engine, "after_cursor_execute", after_cursor_execute)
    task = asyncio.create_task(read())
    cancelled_query = asyncio.create_task(cancelled.wait())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        premature, _ = await asyncio.wait({task, cancelled_query}, timeout=0.1, return_when=asyncio.FIRST_COMPLETED)
        assert not premature, "Cancellation abandoned the admitted binding query"
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert restored == [prior] and get_current_user() is prior
        assert owners == [e.owner_id]
        assert e.engine.sync_engine.pool.checkedout() == 0
    finally:
        release.set()
        cancelled_query.cancel()
        await asyncio.gather(task, cancelled_query, return_exceptions=True)
        event.remove(e.engine.sync_engine, "after_cursor_execute", after_cursor_execute)
        reset_current_user(token)
