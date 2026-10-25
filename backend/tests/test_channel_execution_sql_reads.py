"""Strict ASGI reads use raw SQL owner scope and restore task-local identity.

Raw-owner cases validate the implemented scope helper. Cancellation reproduces
the started SQL read being abandoned before its drainage fix. No model or final
output is produced.
"""

import asyncio
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from _channel_execution_test_support import gateway_environment as gateway_environment
from fastapi import FastAPI
from sqlalchemy import event, text
from sqlalchemy.util.concurrency import await_only

from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.channel_execution import CHANNEL_EXECUTION_REQUEST_HEADER, CHANNEL_EXECUTION_RESPONSE_HEADER
from app.gateway.internal_auth import create_internal_auth_headers, get_internal_user
from app.gateway.routers import thread_runs
from deerflow.persistence.base import Base
from deerflow.persistence.models.run_event import RunEventRow
from deerflow.persistence.run.model import RunChangeClockRow, RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
from deerflow.runtime.events.store.db import DbRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.user_context import get_current_user, reset_current_user, set_current_user

RAW_OWNER = "human:opaque/owner"
RUN_ID = "raw-owner-stored-run"


class ContextProbe:
    """Pure ASGI wrapper observes restoration within the route's actual task."""

    def __init__(self, app, *, observations):
        self.app, self.observations = app, observations

    async def __call__(self, scope, receive, send):
        before = get_current_user()
        try:
            await self.app(scope, receive, send)
        finally:
            self.observations.append((before, get_current_user()))


@pytest_asyncio.fixture
async def sql_read_environment(gateway_environment):
    e = gateway_environment
    tables = [RunEventRow.__table__, RunRow.__table__, RunChangeClockRow.__table__, ThreadMetaRow.__table__]
    async with e.engine.begin() as connection:
        if e.dialect == "sqlite":
            await connection.execute(text("BEGIN"))
        await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=tables))
    # User.id is UUID-only. This deliberately opaque compatibility identity is
    # an explicit minimal offline human double, not an actual User model.
    e.provider.user = SimpleNamespace(id=RAW_OWNER, system_role="user", email="opaque@example.com", disabled_at=None, needs_setup=False, oauth_provider=None)
    alias = get_internal_user(owner_user_id=RAW_OWNER)
    assert alias.id != RAW_OWNER
    threads = ThreadMetaRepository(e.sf)
    runs = RunRepository(e.sf)
    events = DbRunEventStore(e.sf)
    manager = RunManager(store=runs)
    await threads.create(e.thread_id, assistant_id="lead_agent", user_id=RAW_OWNER)
    await runs.put(RUN_ID, thread_id=e.thread_id, assistant_id="lead_agent", user_id=RAW_OWNER, status="success", kwargs={})
    for owner, content in ((RAW_OWNER, "raw owned message"), (str(alias.id), "safe alias message"), ("foreign-owner", "foreign message")):
        token = set_current_user(SimpleNamespace(id=owner))
        try:
            await events.put(thread_id=e.thread_id, run_id=RUN_ID, category="message", event_type="human_message", content={"type": "human", "content": content})
        finally:
            reset_current_user(token)

    app = FastAPI()
    for key, value in vars(e.request.app.state).items():
        setattr(app.state, key, value)
    app.state.thread_store, app.state.run_manager, app.state.run_event_store = threads, manager, events
    probes = []
    app.add_middleware(ContextProbe, observations=probes)
    app.add_middleware(AuthMiddleware)
    app.include_router(thread_runs.router)
    gate = SimpleNamespace(mode="success", entered=asyncio.Event(), release=asyncio.Event(), cancelled=asyncio.Event(), sql_owners=[], sql_parameters=[])

    async def hold():
        gate.entered.set()
        try:
            await gate.release.wait()
        except asyncio.CancelledError:
            gate.cancelled.set()
            raise

    def after_cursor_execute(_connection, _cursor, statement, parameters, _context, _executemany):
        if "SELECT" not in statement.upper() or "FROM RUN_EVENTS" not in statement.upper():
            return
        gate.sql_owners.append(str(get_current_user().id))
        gate.sql_parameters.append(parameters)
        if gate.mode == "error":
            raise RuntimeError("synthetic-private-sql-detail")
        if gate.mode == "cancel":
            # The real driver SELECT has finished, but its owning SQL call has
            # not returned or released scope/session. Suspend that exact call.
            await_only(hold())

    event.listen(e.engine.sync_engine, "after_cursor_execute", after_cursor_execute)
    headers = create_internal_auth_headers(owner_user_id=RAW_OWNER) | {CHANNEL_EXECUTION_REQUEST_HEADER: "1"}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway.test", trust_env=False, headers=headers) as client:
            yield SimpleNamespace(client=client, thread_id=e.thread_id, run_id=RUN_ID, events=events, gate=gate, probes=probes, alias=alias, pool=e.engine.pool, checked_out_baseline=e.engine.pool.checkedout())
    finally:
        gate.release.set()
        event.remove(e.engine.sync_engine, "after_cursor_execute", after_cursor_execute)
        await manager.shutdown(timeout=2)


@pytest.mark.asyncio
async def test_strict_sql_messages_use_the_raw_human_owner_and_restore_prior_scope(sql_read_environment):
    e = sql_read_environment
    # A direct real SQL AUTO read under the internal alias proves the stored
    # aliases differ; the strict route must deliberately install raw scope.
    token = set_current_user(e.alias)
    try:
        alias_rows = await e.events.list_messages_by_run(e.thread_id, e.run_id)
        assert [row["content"]["content"] for row in alias_rows] == ["safe alias message"]
    finally:
        reset_current_user(token)
    e.gate.sql_owners.clear()
    e.gate.sql_parameters.clear()
    prior = SimpleNamespace(id="prior-outer-task-owner")
    token = set_current_user(prior)
    try:
        response = await e.client.get(f"/api/threads/{e.thread_id}/runs/{e.run_id}/messages")
        assert get_current_user() is prior
    finally:
        reset_current_user(token)
    assert response.status_code == 200 and response.headers.get(CHANNEL_EXECUTION_RESPONSE_HEADER) == "1"
    assert [row["content"]["content"] for row in response.json()["data"]] == ["raw owned message"]
    assert e.gate.sql_owners == [RAW_OWNER]
    assert RAW_OWNER in e.gate.sql_parameters[0]
    assert e.probes and all(before is after and str(before.id) == str(e.alias.id) for before, after in e.probes)


@pytest.mark.asyncio
async def test_strict_sql_error_has_no_ack_and_restores_both_route_and_caller_context(sql_read_environment):
    e = sql_read_environment
    e.gate.mode = "error"
    prior = SimpleNamespace(id="prior-outer-task-owner")
    token = set_current_user(prior)
    try:
        response = await e.client.get(f"/api/threads/{e.thread_id}/runs/{e.run_id}/messages")
        assert get_current_user() is prior
    finally:
        reset_current_user(token)
    assert response.status_code == 503 and CHANNEL_EXECUTION_RESPONSE_HEADER not in response.headers
    assert "synthetic-private-sql-detail" not in response.text
    assert e.gate.sql_owners == [RAW_OWNER]
    assert e.probes and all(before is after and str(before.id) == str(e.alias.id) for before, after in e.probes)


@pytest.mark.asyncio
async def test_cancelled_strict_sql_read_restores_scope_after_the_actual_driver_query(sql_read_environment):
    e = sql_read_environment
    e.gate.mode = "cancel"
    prior = SimpleNamespace(id="prior-outer-task-owner")
    request_contexts = []
    token = set_current_user(prior)

    async def request():
        try:
            await e.client.get(f"/api/threads/{e.thread_id}/runs/{e.run_id}/messages")
        finally:
            request_contexts.append(get_current_user())

    task = asyncio.create_task(request())
    cancelled_query = asyncio.create_task(e.gate.cancelled.wait())
    try:
        await asyncio.wait_for(e.gate.entered.wait(), 2)
        task.cancel()
        # A started SQL call must remain owned until its barrier is released.
        # asyncio.wait bounds observation without waiting forever for a
        # cancelled task's acknowledgement (as wait_for would).
        premature, _ = await asyncio.wait({task, cancelled_query}, timeout=0.1, return_when=asyncio.FIRST_COMPLETED)
        assert not premature, "Caller cancellation abandoned the active SQL operation before its release"
        assert not task.done()
        e.gate.release.set()
        done, _ = await asyncio.wait({task}, timeout=2)
        assert task in done, "Owned SQL read did not drain after releasing its barrier"
        with pytest.raises(asyncio.CancelledError):
            task.result()
        assert get_current_user() is prior and request_contexts == [prior]
        assert e.gate.sql_owners == [RAW_OWNER]
        assert e.probes and all(before is after and str(before.id) == str(e.alias.id) for before, after in e.probes)
        assert e.pool.checkedout() == e.checked_out_baseline
    finally:
        try:
            e.gate.release.set()
            cancelled_query.cancel()
            await asyncio.gather(cancelled_query, return_exceptions=True)
            done, _ = await asyncio.wait({task}, timeout=2)
            assert task in done, "The test-owned HTTP task failed to drain after barrier release"
            if not task.cancelled():
                task.exception()
        finally:
            reset_current_user(token)
