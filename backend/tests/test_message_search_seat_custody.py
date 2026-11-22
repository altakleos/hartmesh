"""A released personal seat retires its search rows without retiring company work."""

import asyncio
from pathlib import Path

import pytest
from _storage_spaces_test_support import operation
from deerflow_extension_api import ExtensionRuntimeDeps
from fastapi import Request
from sqlalchemy import text
from test_agent_conversations import conversations
from test_agent_kind_release import agents
from test_compose_user_assignment import account_storage as account_storage
from test_compose_user_assignment import seated_accounts as seated_accounts

from app.gateway.auth.accounts import AccountsCommand
from app.gateway.auth.models import User
from app.gateway.search_corpus import GatewaySearchCorpus
from deerflow.agent_instances.contract import AgentPermission
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.models.run_event import RunEventRow, RunEventThreadSeqRow
from deerflow.persistence.run.model import RunRow
from deerflow.runtime.events.store.db import DbRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.spaces.contract import PrincipalRef


async def pending_search(index, reader, selected, query):
    async with asyncio.timeout(30):
        while True:
            page = await index.search(reader, query=query, threads=selected, limit=10)
            if not page.indexing:
                return [(hit.thread_id, hit.message_id) for hit in page.items]
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_release_removes_personal_custody_atomically_and_the_employee_remains_searchable(account_storage, monkeypatch):  # noqa: F811 (shared fixture)
    app, repo, sf, files, catalog, _ = account_storage
    owner, create = await agents(account_storage, monkeypatch)
    colleague = await repo.create_user(User(email="colleague@example.com", password_hash="fixture"))
    actor = PrincipalRef("human", str(owner.id))
    peer = PrincipalRef("human", str(colleague.id))
    assistant, employee = await create(), await create("company")
    _, authority, threads, _ = await conversations((app.state.agent_instances, files, sf, catalog))
    app.state.agent_conversations = authority
    async with sf.begin() as session:
        for resource in (assistant, employee):
            session.add(AgentInstanceGrantRow(instance_id=resource.id, user_id=peer.subject_id, permissions=int(AgentPermission.USE | AgentPermission.INSPECT)))
    common = (await authority.create(actor=actor, instance_id=employee.id, creation_id=operation()))["thread_id"]
    private = (await threads.create(operation(), user_id=actor.subject_id))["thread_id"]
    first = (await authority.create(actor=actor, instance_id=assistant.id, creation_id=operation()))["thread_id"]
    second = (await authority.create(actor=peer, instance_id=assistant.id, creation_id=operation()))["thread_id"]
    ids = {common, private, first, second}
    async with sf.kw["bind"].begin() as conn:
        for model in (RunRow, RunEventRow, RunEventThreadSeqRow):
            await conn.run_sync(model.__table__.create, checkfirst=True)
    events = DbRunEventStore(sf)
    manager = RunManager()

    def reader(user):
        request = Request({"type": "http", "app": app, "state": {"auth_source": "session", "user": user}})
        return GatewaySearchCorpus(threads, events, manager, str(user.id), request=request)

    primary, other = reader(owner), reader(colleague)
    snapshots = {t.thread_id: t for t in await primary.threads()}
    assert set(snapshots) == ids
    for thread_id in ids:
        token = set_current_user(colleague if thread_id == second else owner)
        try:
            await events.put(thread_id=thread_id, run_id=operation(), category="message", event_type="message", content={"type": "human", "id": "note", "content": "Visible report"}, metadata={"caller": "lead_agent"})
        finally:
            reset_current_user(token)
    is_pg = sf.kw["bind"].dialect.name == "postgresql"
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "backend/extensions/sources/hartmesh-message-search"))
    from hartmesh_message_search import MessageSearch

    index = MessageSearch()
    await index.start(ExtensionRuntimeDeps(session_factory=sf))
    assert index.ready is is_pg
    tables = ("hm_message_search_words", "hm_message_search_threads")
    if is_pg:
        assert {tid for tid, _ in await pending_search(index, primary, tuple(snapshots.values()), "report")} == ids
    else:
        # SQLite never activates the provider. Exercise offline cleanup of retained tables.
        async with sf.begin() as s:
            for table in tables:
                await s.execute(text(f"CREATE TABLE {table} (thread_id varchar(64) PRIMARY KEY)"))
                await s.execute(text(f"INSERT INTO {table}(thread_id) VALUES (:id)"), [{"id": i} for i in ids])

    async def remaining():
        async with sf() as session:
            return [set(await session.scalars(text(f"SELECT thread_id FROM {table}"))) for table in tables]

    assert await remaining() == [ids, ids]

    waiting, resume = asyncio.Event(), asyncio.Event()
    stale_task = None
    if is_pg:
        # Pause an actual background worker after it read an incoming page, before its admitted write.
        write = index._write

        async def delayed_write(corpus, thread, generation, messages, *, finished):
            if thread.thread_id == private and messages:
                waiting.set()
                await resume.wait()
            return await write(corpus, thread, generation, messages, finished=finished)

        monkeypatch.setattr(index, "_write", delayed_write)
        token = set_current_user(owner)
        try:
            await events.put(thread_id=private, run_id=operation(), category="message", event_type="message", content={"type": "human", "id": "late", "content": "A late report"}, metadata={"caller": "lead_agent"})
        finally:
            reset_current_user(token)
        assert (await index.search(primary, query="late", threads=(snapshots[private],), limit=1)).indexing
        stale_task = index._jobs[actor.subject_id]
        await asyncio.wait_for(waiting.wait(), 5)

    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", account_id=actor.subject_id)
    original = command._purge_released_search_rows

    async def interrupted(*args):
        await original(*args)
        raise RuntimeError("simulated interrupted release")

    monkeypatch.setattr(command, "_purge_released_search_rows", interrupted)
    with pytest.raises(RuntimeError, match="simulated interrupted release"):
        await command.run("release-seat", account_id=actor.subject_id)
    assert (await repo.get_user_by_id(actor.subject_id)).released_at is None
    assert await remaining() == [ids, ids]
    monkeypatch.setattr(command, "_purge_released_search_rows", original)

    try:
        result = await command.run("release-seat", account_id=actor.subject_id)
        assert result["returncode"] == 0 and result["assistants"] == 1
    finally:
        resume.set()
    if stale_task is not None:
        await stale_task
    async with sf.begin() as session:
        assert not await primary.admit_index_write(session, snapshots[private])
    assert await remaining() == [{common}, {common}]
    assert (await command.run("release-seat", account_id=actor.subject_id))["returncode"] == 0
    live = await other.threads()
    assert {t.thread_id for t in live} == {common}
    if is_pg:
        matches = await pending_search(index, other, live, "report")
        assert matches == [(common, "note")]
        assert (await other.latest_message(live[0], "note")).text == "Visible report"
    await index.stop()
