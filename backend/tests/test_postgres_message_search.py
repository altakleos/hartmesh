"""The optional message index is tied to durable visible history on PostgreSQL."""

import asyncio
import os
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
import yaml
from deerflow_extension_api import ExtensionRuntimeDeps
from deerflow_extension_api.search import SearchMessage, SearchMessagePage, SearchThread
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

ROOT = Path(__file__).resolve().parents[2]
BUNDLED_ENTRY = {"name": "message-search", "package": "hartmesh-message-search", "use": "hartmesh_message_search:install", "table_prefix": "hm_message_search_", "enabled": True, "required": False}
WORDS = "hm_message_search_words"
MARKERS = "hm_message_search_threads"


@pytest.fixture
def provider(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "backend/extensions/sources/hartmesh-message-search"))
    from hartmesh_message_search import MessageSearch

    return MessageSearch()


@pytest_asyncio.fixture
async def pg():
    uri = os.getenv("TEST_POSTGRES_URI")
    if not uri:
        pytest.skip("TEST_POSTGRES_URI is required")
    url = make_url(uri).set(drivername="postgresql+asyncpg").difference_update_query(["sslmode"])
    admin = create_async_engine(url)
    schema = "test_message_search_" + uuid4().hex
    async with admin.begin() as conn:
        await conn.execute(text(f"CREATE SCHEMA {schema}"))
    db = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    try:
        async with db.begin() as conn:
            await conn.execute(text("CREATE TABLE threads_meta(thread_id varchar(64) PRIMARY KEY)"))
            await conn.execute(text("CREATE TABLE run_events(thread_id varchar(64), seq integer, CONSTRAINT uq_events_thread_seq UNIQUE(thread_id,seq))"))
            await conn.execute(text("INSERT INTO threads_meta VALUES ('one')"))
            await conn.execute(text("INSERT INTO run_events VALUES ('one',1),('one',2)"))
        yield async_sessionmaker(db, expire_on_commit=False)
    finally:
        await db.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        await admin.dispose()


@pytest.mark.asyncio
async def test_index_starts_only_with_both_verified_postgres_cascades(provider, pg):
    assert provider.ready is False
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    assert provider.ready
    async with pg.begin() as s:
        constraints = (await s.execute(text("SELECT conname,confdeltype::text FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND contype='f'"))).all()
    assert set(constraints) == {("hm_message_search_event_fk", "c"), ("hm_message_search_thread_fk", "c")}
    await provider.stop()
    assert not provider.ready


@pytest.mark.asyncio
async def test_sqlite_start_leaves_no_store(provider):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine)
    try:
        await provider.start(ExtensionRuntimeDeps(session_factory=factory))
        assert not provider.ready
        async with factory() as session:
            assert (await session.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))).all() == []
    finally:
        await provider.stop()
        await engine.dispose()


class Corpus:
    def __init__(self):
        self.user_id = "alice"
        self.thread = SearchThread("one", "original", "Notes", "2026-01-01")
        self.head = ("visible", 2)
        self.allowed = True
        self.reads = []
        self.messages_by_seq = {1: "Straße 100%_ ", 2: "Other"}

    async def index_heads(self, threads, *, session=None):
        return {t.thread_id: self.head for t in threads if self.allowed}

    async def admit_index_write(self, session, thread):
        return self.allowed

    async def messages(self, thread, *, before_seq):
        self.reads.append(before_seq)
        rows = [seq for seq in self.messages_by_seq if before_seq is None or seq < before_seq]
        rows.sort(reverse=True)
        if not rows:
            return SearchMessagePage()
        seq = rows[0]
        item = SearchMessage(thread.thread_id, thread.incarnation, f"m{seq}", "r", seq, "user", self.messages_by_seq[seq], "2026-01-01")
        return SearchMessagePage((item,), seq if len(rows) > 1 else None)


async def settled(provider, corpus, query, *, limit=10, cursor=None):
    async with asyncio.timeout(8):
        while True:
            result = await provider.search(corpus, query=query, threads=(corpus.thread,), limit=limit, cursor=cursor)
            if not result.indexing:
                return result
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_visible_rows_are_shared_cascade_with_their_history_and_stop_at_the_previous_head(provider, pg):
    corpus = Corpus()
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    first = await provider.search(corpus, query="strasse", threads=(corpus.thread,), limit=10)
    assert first.indexing and not first.items
    result = await settled(provider, corpus, "STRASSE 100%_")
    assert [(r.thread_id, r.message_id, r.seq) for r in result.items] == [("one", "m1", 1)]
    seen = list(corpus.reads)
    corpus.user_id = "bob"
    assert len((await settled(provider, corpus, "other")).items) == 1
    assert corpus.reads == seen
    async with pg.begin() as s:
        assert (await s.scalars(text(f"SELECT normalized FROM {WORDS} ORDER BY seq"))).all() == ["strasse 100%_ ", "other"]
        await s.execute(text("INSERT INTO run_events VALUES ('one',3)"))
    corpus.head = ("visible", 3)
    corpus.messages_by_seq[3] = "Other too"
    result = await settled(provider, corpus, "other", limit=1)
    assert result.items[0].seq == 3 and result.cursor
    assert (await settled(provider, corpus, "other", limit=1, cursor=result.cursor)).items[0].seq == 2
    assert corpus.reads[len(seen) :] == [None, 3]
    await provider.stop()
    # Rollback: the caller deleting events has no provider process at all.
    async with pg.begin() as s:
        await s.execute(text("DELETE FROM run_events WHERE seq=3"))
        assert (await s.scalars(text(f"SELECT seq FROM {WORDS} ORDER BY seq"))).all() == [1, 2]
        await s.execute(text("DELETE FROM run_events"))
        await s.execute(text("DELETE FROM threads_meta"))
        assert await s.scalar(text(f"SELECT count(*) FROM {WORDS}")) == 0
        assert await s.scalar(text(f"SELECT count(*) FROM {MARKERS} WHERE complete")) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(("parent", "key"), [("run_events", "hm_message_search_event_fk"), ("threads_meta", "hm_message_search_thread_fk")])
async def test_missing_constraint_times_out_retries_and_verified_start_only_inspects(provider, pg, parent, key, caplog):
    import logging

    caplog.set_level(logging.INFO)
    async with pg.begin() as lock:
        await lock.execute(text(f"LOCK TABLE {parent} IN ROW EXCLUSIVE MODE"))
        async with asyncio.timeout(6):
            await provider.start(ExtensionRuntimeDeps(session_factory=pg))
        assert not provider.ready
        notice = next(r.getMessage() for r in caplog.records if f"{key} outcome=timeout" in r.getMessage())
        assert notice.endswith(("reason=lock_timeout", "reason=attempt_deadline"))
        assert 1.60 <= float(notice.split("elapsed_seconds=")[1].split()[0]) <= 2.00
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    assert provider.ready
    await provider.stop()
    async with pg.begin() as lock:
        await lock.execute(text(f"LOCK TABLE {parent} IN ACCESS EXCLUSIVE MODE"))
        async with asyncio.timeout(1.5):
            await provider.start(ExtensionRuntimeDeps(session_factory=pg))
        assert provider.ready
    await provider.stop()


@pytest.mark.asyncio
async def test_changed_visibility_forces_a_full_reread_and_unknown_format_never_yields_a_hit(provider, pg):
    corpus = Corpus()
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    assert len((await settled(provider, corpus, "straße")).items) == 1
    old = len(corpus.reads)
    corpus.head = ("new-view", 2)
    await settled(provider, corpus, "straße")
    assert corpus.reads[old:] == [None, 2]
    async with pg.begin() as s:
        await s.execute(text(f"UPDATE {WORDS} SET format=9 WHERE seq=1"))
    assert not (await settled(provider, corpus, "straße")).items
    async with pg.begin() as s:
        await s.execute(text(f"ALTER TABLE {WORDS} DROP CONSTRAINT hm_message_search_event_fk"))
        await s.execute(text(f"ALTER TABLE {WORDS} ADD CONSTRAINT hm_message_search_event_fk FOREIGN KEY(thread_id,seq) REFERENCES run_events(thread_id,seq) ON DELETE NO ACTION"))
    with pytest.raises(RuntimeError, match="unavailable"):
        await provider.search(corpus, query="other", threads=(corpus.thread,), limit=10)
    assert not provider.ready
    await provider.stop()


@pytest.mark.asyncio
async def test_delete_during_catchup_cannot_restore_words_or_false_completion(provider, pg):
    corpus = Corpus()
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))

    original = corpus.messages

    async def delete_after_read(*a, **kw):
        page = await original(*a, **kw)
        async with pg.begin() as s:
            await s.execute(text("DELETE FROM run_events"))
        return page

    corpus.messages = delete_after_read
    await provider.search(corpus, query="straße", threads=(corpus.thread,), limit=10)
    await asyncio.gather(*provider._jobs.values())
    async with pg() as s:
        assert await s.scalar(text(f"SELECT count(*) FROM {WORDS}")) == 0
        assert await s.scalar(text(f"SELECT count(*) FROM {MARKERS} WHERE complete")) == 0
    await provider.stop()


@pytest.mark.asyncio
async def test_stop_waits_for_retained_read_to_close_before_returning_and_never_marks_complete(provider, pg):
    corpus = Corpus()
    entered = asyncio.Event()
    closed = asyncio.Event()

    async def read(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            closed.set()

    corpus.messages = read
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    assert (await provider.search(corpus, query="report", threads=(corpus.thread,), limit=10)).indexing
    await asyncio.wait_for(entered.wait(), 1)
    await provider.stop()
    assert closed.is_set() and not provider.ready
    async with pg() as s:
        assert not await s.scalar(text(f"SELECT count(*) FROM {MARKERS} WHERE complete"))


def test_package_and_optional_provider_entry_match_the_installed_loader(monkeypatch):
    import tomllib

    from deerflow.extensions.loader import ExtensionSpec, load_extensions

    project = ROOT / "backend/extensions/sources/hartmesh-message-search"
    monkeypatch.syspath_prepend(str(project))
    backend = tomllib.loads((ROOT / "backend/pyproject.toml").read_text())
    lock = tomllib.loads((ROOT / "backend/uv.lock").read_text())
    assert BUNDLED_ENTRY["package"] in backend["dependency-groups"]["extensions"]
    assert backend["tool"]["uv"]["sources"][BUNDLED_ENTRY["package"]] == {"path": "extensions/sources/hartmesh-message-search"}
    assert any(p["name"] == BUNDLED_ENTRY["package"] and p["source"] == {"directory": "extensions/sources/hartmesh-message-search"} for p in lock["package"])
    packaged = tomllib.loads((project / "pyproject.toml").read_text())
    assert packaged["project"]["entry-points"]["deerflow.extensions"][BUNDLED_ENTRY["name"]] == BUNDLED_ENTRY["use"]
    assert "deerflow-extension-api>=0.2.13,<0.3" in packaged["project"]["dependencies"]
    loaded, issues = load_extensions([ExtensionSpec.model_validate(BUNDLED_ENTRY)])
    assert not issues
    assert len(loaded.workspace_search_providers) == 1 and len(loaded.services) == 1
    assert loaded.workspace_search_providers[0][1] is loaded.services[0][1]
    assert not loaded.services[0][1].ready
    monkeypatch.setattr("deerflow.extensions.loader.API_VERSION", "0.2.12")
    older, issues = load_extensions([ExtensionSpec.model_validate(BUNDLED_ENTRY)])
    assert not older.workspace_search_providers
    assert len(issues) == 1 and "requires extension-api 0.2.13" in issues[0].message
    assert yaml.safe_load((ROOT / "docker/acceptance/message-search/plugin.yaml").read_text()) == BUNDLED_ENTRY
    template = yaml.safe_load((ROOT / "deploy/compose/config.yaml").read_text())
    assert [e for e in template["plugins"] if e.get("name") == "message-search"] in ([], [BUNDLED_ENTRY])


@pytest.mark.asyncio
async def test_changed_projection_and_repeated_identity_physically_retire_old_search_words(provider, pg):
    corpus = Corpus()
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    await settled(provider, corpus, "strasse")
    # A changed visible run set hides a previously indexed message.
    del corpus.messages_by_seq[1]
    corpus.head = ("edited", 2)
    assert not (await settled(provider, corpus, "strasse")).items
    async with pg.begin() as s:
        assert not await s.scalar(text(f"SELECT count(*) FROM {WORDS} WHERE normalized LIKE '%strasse%'"))
        await s.execute(text("INSERT INTO run_events VALUES ('one',3)"))
    original = corpus.messages

    async def newer_version(*args, **kwargs):
        from dataclasses import replace

        page = await original(*args, **kwargs)
        return SearchMessagePage(tuple(replace(m, message_id="m2") for m in page.items), page.before_seq)

    corpus.messages = newer_version
    corpus.messages_by_seq[3] = "Replacement words"
    corpus.head = ("edited", 3)
    assert (await settled(provider, corpus, "replacement")).items[0].seq == 3
    async with pg() as s:
        assert (await s.scalars(text(f"SELECT seq FROM {WORDS}"))).all() == [3]
    await provider.stop()


@pytest.mark.asyncio
async def test_delayed_worker_cannot_restore_superseded_projection_from_before_another_gateway(provider, pg):
    from hartmesh_message_search import MessageSearch

    other = MessageSearch()
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    await other.start(ExtensionRuntimeDeps(session_factory=pg))
    old = Corpus()
    fresh = Corpus()
    began, unblock = asyncio.Event(), asyncio.Event()
    old_read = old.messages

    async def delayed(*args, **kw):
        result = await old_read(*args, **kw)
        began.set()
        await unblock.wait()
        return result

    old.messages = delayed
    await provider.search(old, query="Other", threads=(old.thread,), limit=10)
    await asyncio.wait_for(began.wait(), 2)
    fresh.messages_by_seq = {1: "Present in new view"}
    fresh.head = ("new projection", 2)
    assert (await settled(other, fresh, "new view")).items
    unblock.set()
    await asyncio.gather(*provider._jobs.values())
    async with pg() as s:
        assert (await s.scalars(text(f"SELECT normalized FROM {WORDS}"))).all() == ["present in new view"]
        assert await s.scalar(text(f"SELECT revision FROM {MARKERS}")) == "new projection"
    await provider.stop()
    await other.stop()


@pytest.mark.asyncio
async def test_each_page_verifies_both_cascades_inside_its_write_transaction(provider, pg):
    corpus = Corpus()
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    original = corpus.messages

    async def altered(*args, **kwargs):
        page = await original(*args, **kwargs)
        async with pg.begin() as s:
            await s.execute(text(f"ALTER TABLE {WORDS} DROP CONSTRAINT hm_message_search_event_fk"))
        return page

    corpus.messages = altered
    assert (await provider.search(corpus, query="report", threads=(corpus.thread,), limit=10)).indexing
    with pytest.raises(RuntimeError, match="unavailable"):
        await next(iter(provider._jobs.values()))
    assert not provider.ready
    async with pg() as s:
        assert await s.scalar(text(f"SELECT count(*) FROM {WORDS}")) == 0
    await provider.stop()


@pytest.mark.asyncio
async def test_reservation_rechecks_the_head_on_the_same_admitted_connection(provider, pg):
    corpus = Corpus()
    seen = []
    original = corpus.index_heads

    async def heads(threads, *, session=None):
        if session is not None:
            seen.append(session)
        return await original(threads, session=session)

    writes = []

    async def admitted(session, thread):
        writes.append(session)
        return True

    corpus.index_heads = heads
    corpus.admit_index_write = admitted
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    assert await settled(provider, corpus, "strasse")
    assert seen == [writes[0]]
    await provider.stop()


@pytest.mark.asyncio
async def test_waits_on_child_then_parent_share_one_real_time_deadline(provider, pg, caplog):
    from time import monotonic

    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    assert provider.ready
    await provider.stop()
    async with pg.begin() as s:
        await s.execute(text(f"ALTER TABLE {WORDS} DROP CONSTRAINT hm_message_search_event_fk"))
    async with pg.begin() as parent:
        await parent.execute(text("LOCK TABLE run_events IN ROW EXCLUSIVE MODE"))
        async with pg.begin() as child:
            await child.execute(text(f"LOCK TABLE {WORDS} IN ROW EXCLUSIVE MODE"))
            pid = await child.scalar(text("SELECT pg_backend_pid()"))
            task = asyncio.create_task(provider.start(ExtensionRuntimeDeps(session_factory=pg)))
            async with asyncio.timeout(2):
                while not await child.scalar(text("SELECT EXISTS(SELECT FROM pg_locks WHERE NOT granted AND :pid=ANY(pg_blocking_pids(pid)))"), {"pid": pid}):
                    await asyncio.sleep(0.01)
            started = monotonic()
            # The second blocker stays in place after the first one releases.
            await asyncio.sleep(0.65)
        await task
        elapsed = monotonic() - started
        assert not provider.ready and elapsed <= 2.00, elapsed
        receipt = next(rec.getMessage() for rec in caplog.records if "hm_message_search_event_fk outcome=timeout" in rec.getMessage())
        assert "reason=attempt_deadline" in receipt
        assert 1.85 <= float(receipt.split("elapsed_seconds=")[1].split()[0]) <= 2.0, receipt
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    assert provider.ready
    await provider.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("offline", [True, False], ids=["provider-off", "pending-append"])
async def test_removing_a_newer_version_reveals_the_older_visible_message(provider, pg, offline, monkeypatch):
    from dataclasses import replace

    corpus = Corpus()
    async with pg.begin() as session:
        await session.execute(text("INSERT INTO run_events VALUES ('one',3)"))
    corpus.head = ("visible", 3)
    corpus.messages_by_seq[3] = "Unrelated later message"
    original = corpus.messages

    async def versions(*args, **kwargs):
        page = await original(*args, **kwargs)
        return SearchMessagePage(tuple(replace(item, message_id="same") if item.seq < 3 else item for item in page.items), page.before_seq)

    corpus.messages = versions
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    assert (await settled(provider, corpus, "other")).items[0].seq == 2
    assert not (await settled(provider, corpus, "strasse")).items
    waiting, resume = asyncio.Event(), asyncio.Event()
    if offline:
        await provider.stop()
    else:
        async with pg.begin() as session:
            await session.execute(text("INSERT INTO run_events VALUES ('one',4)"))
        corpus.head = ("visible", 4)
        corpus.messages_by_seq[4] = "Fresh append"
        write = provider._write

        async def delayed(*args, **kwargs):
            waiting.set()
            await resume.wait()
            return await write(*args, **kwargs)

        monkeypatch.setattr(provider, "_write", delayed)
        assert (await provider.search(corpus, query="fresh", threads=(corpus.thread,), limit=5)).indexing
        await asyncio.wait_for(waiting.wait(), 5)
    # An earlier Gateway, or a database repair, only knows the journal.
    previous_writer = "run_history_" + uuid4().hex
    async with pg.begin() as session:
        name = await session.scalar(text("SELECT current_schema()"))
        await session.execute(text(f"CREATE ROLE {previous_writer}"))
        await session.execute(text(f"GRANT USAGE ON SCHEMA {name} TO {previous_writer}"))
        await session.execute(text(f"GRANT SELECT, DELETE ON run_events TO {previous_writer}"))
        await session.execute(text(f"SET LOCAL ROLE {previous_writer}"))
        assert not await session.scalar(text(f"SELECT has_table_privilege('{MARKERS}', 'DELETE')"))
        await session.execute(text("DELETE FROM run_events WHERE thread_id='one' AND seq=2"))
        await session.execute(text("RESET ROLE"))
        await session.execute(text(f"DROP OWNED BY {previous_writer}"))
        await session.execute(text(f"DROP ROLE {previous_writer}"))
        assert not await session.scalar(text(f"SELECT count(*) FROM {WORDS} WHERE normalized='other'"))
    del corpus.messages_by_seq[2]
    if offline:
        await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    else:
        resume.set()
        await asyncio.gather(*provider._jobs.values())
        async with pg() as session:
            assert not await session.scalar(text(f"SELECT count(*) FROM {MARKERS}"))
    assert [(m.seq, m.message_id) for m in (await settled(provider, corpus, "strasse")).items] == [(1, "same")]
    assert (await settled(provider, corpus, "unrelated")).items[0].seq == 3
    await provider.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("broken", ["trigger", "restored table"])
async def test_broken_source_invalidation_withholds_results_and_rebuilds_on_restart(provider, pg, broken):
    corpus = Corpus()
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    assert (await settled(provider, corpus, "other")).items
    async with pg.begin() as session:
        if broken == "trigger":
            await session.execute(text(f"ALTER TABLE {WORDS} DISABLE TRIGGER hm_message_search_source_removed"))
        else:
            # Restoring the same names and keys need not preserve physical table OIDs.
            await session.execute(text(f"CREATE TABLE restored_markers (LIKE {MARKERS} INCLUDING ALL)"))
            await session.execute(text(f"INSERT INTO restored_markers SELECT * FROM {MARKERS}"))
            await session.execute(text(f"DROP TABLE {MARKERS}"))
            await session.execute(text(f"ALTER TABLE restored_markers RENAME TO {MARKERS}"))
            await session.execute(text(f"ALTER TABLE {MARKERS} ADD CONSTRAINT hm_message_search_thread_fk FOREIGN KEY(thread_id) REFERENCES threads_meta(thread_id) ON DELETE CASCADE"))
    with pytest.raises(RuntimeError, match="unavailable"):
        await provider.search(corpus, query="other", threads=(corpus.thread,), limit=1)
    assert not provider.ready
    await provider.stop()
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    assert provider.ready
    async with pg() as session:
        assert not await session.scalar(text(f"SELECT count(*) FROM {MARKERS}"))
    assert (await settled(provider, corpus, "other")).items
    await provider.stop()


@pytest.mark.asyncio
async def test_ranked_matches_page_across_tied_threads_and_reach_rare_late_messages(provider, pg):
    corpus = Corpus()
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    threads = tuple(SearchThread(name, "current", name, stamp) for name, stamp in (("z", "2026-02"), ("a", "2026-03"), ("c", "2026-02"), ("b", "2026-02")))
    async with pg.begin() as session:
        for thread in threads:
            fields = {"id": thread.thread_id, "generation": uuid4()}
            await session.execute(text("INSERT INTO threads_meta VALUES (:id)"), fields)
            await session.execute(text("INSERT INTO run_events SELECT :id,seq FROM generate_series(1,4) seq"), fields)
            await session.execute(text(f"INSERT INTO {MARKERS} VALUES (:id,1,'current','visible',4,4,:generation,true)"), fields)
            await session.execute(
                text(f"""
               INSERT INTO {WORDS} SELECT CAST(:id AS text),seq,1,'m'||seq,
                   CASE WHEN :id='b' AND seq=1 THEN 'common rare' ELSE 'common' END
               FROM generate_series(1,4) seq
            """),
                fields,
            )
    corpus.head = ("visible", 4)
    cursor = None
    seen = []
    while True:
        page = await provider.search(corpus, query="common", threads=threads, limit=3, cursor=cursor)
        seen.extend((item.thread_id, item.seq) for item in page.items)
        cursor = page.cursor
        if not cursor:
            break
    assert seen == [(tid, seq) for tid in ("a", "z", "c", "b") for seq in (4, 3, 2, 1)]
    rare = await provider.search(corpus, query="rare", threads=threads, limit=3)
    assert [(item.thread_id, item.seq) for item in rare.items] == [("b", 1)]
    assert not (await provider.search(corpus, query="absent", threads=threads, limit=3)).items
    assert not corpus.reads
    await provider.stop()


@pytest.mark.asyncio
async def test_moved_marker_keeps_its_physical_delete_with_a_multi_schema_search_path(provider, pg):
    corpus = Corpus()
    await provider.start(ExtensionRuntimeDeps(session_factory=pg))
    assert (await settled(provider, corpus, "other")).items
    await provider.stop()
    async with pg.begin() as s:
        original = await s.scalar(text("SELECT current_schema()"))
        moved = original + "_other"
        await s.execute(text(f"CREATE SCHEMA {moved}"))
        await s.execute(text(f"ALTER TABLE {MARKERS} SET SCHEMA {moved}"))
    db = create_async_engine(pg.kw["bind"].url, connect_args={"server_settings": {"search_path": original + "," + moved}})
    try:
        factory = async_sessionmaker(db, expire_on_commit=False)
        await provider.start(ExtensionRuntimeDeps(session_factory=factory))
        assert provider.ready
        async with factory.begin() as s:
            await s.execute(text("DELETE FROM run_events WHERE thread_id='one' AND seq=1"))
            assert not await s.scalar(text(f"SELECT count(*) FROM {MARKERS}"))
    finally:
        await provider.stop()
        await db.dispose()
        async with pg.begin() as s:
            await s.execute(text(f"DROP SCHEMA {moved} CASCADE"))
