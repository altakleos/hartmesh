from dataclasses import replace
from datetime import UTC, datetime

import pytest

from app.gateway.search_corpus import GatewaySearchCorpus
from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
from deerflow.persistence.user.model import UserRow
from deerflow.runtime.events.store.db import DbRunEventStore
from deerflow.runtime.runs.manager import RunManager


@pytest.mark.anyio
async def test_journal_append_replay_and_seat_release_update_search_admission(tmp_path):
    await init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path / 'heads.db'}", sqlite_dir=str(tmp_path))
    try:
        sf = get_session_factory()
        threads = ThreadMetaRepository(sf)
        events = DbRunEventStore(sf)
        manager = RunManager(RunRepository(sf))
        owner = "e203cabc-4fd4-43ea-bc6c-e0bed11ad912"
        corpus = GatewaySearchCorpus(threads, events, manager, owner)
        async with sf.begin() as session:
            session.add(UserRow(id=owner, email="owner@example.org"))
        await threads.create("chat", user_id=owner)
        await threads.create("foreign", user_id="other")
        chat = await corpus.thread("chat")
        foreign = replace(chat, thread_id="foreign")
        old = await corpus.index_heads((chat, foreign))
        assert set(old) == {"chat"}
        await events.put(thread_id="chat", run_id="source", category="message", event_type="message", content={"id": "m", "type": "ai", "content": "reply"})
        changed = await corpus.index_heads((chat,))
        assert changed["chat"] == (old["chat"][0], 1)
        async with sf.begin() as session:
            session.add(RunRow(run_id="edit", thread_id="chat", user_id=owner, status="success", metadata_json={"regenerate_from_run_id": "source"}))
        after_edit = await corpus.index_heads((chat,))
        assert after_edit["chat"][0] != changed["chat"][0]
        async with sf.begin() as session:
            assert await corpus.admit_index_write(session, chat)
            assert not await corpus.admit_index_write(session, replace(chat, incarnation="old"))
            assert not await corpus.admit_index_write(session, foreign)
            (await session.get(UserRow, owner)).released_at = datetime.now(UTC)
        async with sf.begin() as session:
            assert not await corpus.admit_index_write(session, chat)
    finally:
        await close_engine()


@pytest.mark.anyio
async def test_sql_title_enumeration_shares_the_scoped_row_projection(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock

    from deerflow.runtime.events.store.memory import MemoryRunEventStore

    await init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path / 'listed.db'}", sqlite_dir=str(tmp_path))
    try:
        store = ThreadMetaRepository(get_session_factory())
        corpus = GatewaySearchCorpus(store, MemoryRunEventStore(), RunManager(), "reader")
        for tid, owner, meta in (("public-to-reader", "reader", {}), ("sidecar", "reader", {"deerflow_sidecar": True}), ("other", "other", {})):
            await store.create(tid, user_id=owner, display_name=tid, metadata=meta)
        wanted = corpus.project_thread(await store.get("public-to-reader", user_id="reader"))
        monkeypatch.setattr(store, "search", AsyncMock(side_effect=AssertionError("do not sort and page all SQL metadata for title selection")))
        assert [item async for item in corpus.listed_threads()] == [wanted]
        assert await corpus.threads() == (wanted,)
    finally:
        await close_engine()
