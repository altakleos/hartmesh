"""One-shot managed dispatch/binding; new API absence is an explicit scaffold."""

import asyncio
import dataclasses
from contextvars import ContextVar

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import event, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import Session
from test_telegram_publication_custody import expected_input_digest, saved_origins
from test_telegram_publication_custody import publication_environment as publication_environment

from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio


def install_actual_dispatch_probe(e, monkeypatch):
    """Signal the native SQLite execute or the dispatched PostgreSQL statement."""
    active = ContextVar("publication_dispatch_probe", default=False)
    dispatched, backend_pid = asyncio.Event(), []
    if e.db.dialect == "sqlite":
        import aiosqlite

        original = aiosqlite.Connection._execute
        loop = asyncio.get_running_loop()

        async def execute(connection, function, *args, **kwargs):
            if active.get() and args and isinstance(args[0], str) and args[0].strip().upper() == "BEGIN IMMEDIATE":

                def native(*values, **options):
                    loop.call_soon_threadsafe(dispatched.set)
                    return function(*values, **options)

                return await original(connection, native, *args, **kwargs)
            return await original(connection, function, *args, **kwargs)

        monkeypatch.setattr(aiosqlite.Connection, "_execute", execute)
        return active, dispatched, backend_pid, lambda: None

    def before_execute(connection, cursor, statement, parameters, context, executemany):
        if active.get() and "FROM telegram_bot_links" in statement:
            backend_pid.append(connection.connection.driver_connection.get_server_pid())
            dispatched.set()

    event.listen(e.db.engine.sync_engine, "before_cursor_execute", before_execute)
    return active, dispatched, backend_pid, lambda: event.remove(e.db.engine.sync_engine, "before_cursor_execute", before_execute)


async def require_real_lock_wait(e, backend_pid):
    if e.db.dialect != "postgres":
        return
    assert backend_pid
    deadline = asyncio.get_running_loop().time() + 3
    while True:
        async with e.sessions() as observer:
            waiting = await observer.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=:pid AND NOT granted)"), {"pid": backend_pid[0]})
        if waiting is True:
            return
        assert asyncio.get_running_loop().time() < deadline, "Actual PostgreSQL request never entered its held row-lock wait"
        await asyncio.sleep(0.001)


async def test_marker_has_one_winner_and_bind_accepts_original_observation(publication_environment):
    e = publication_environment
    original = await e.reserve()
    results = await asyncio.gather(*(e.inbox.mark_dispatch_started(original, guard=e.guard_factory()) for _ in range(5)))
    assert results.count(True) == 1 and results.count(False) == 4
    assert original.dispatch_started is False and original.run_id is None
    bound = await e.inbox.bind_publication_run(original, run_id="the-exact-run", guard=e.guard_factory())
    assert bound.dispatch_started is True and bound.run_id == "the-exact-run"
    assert await e.inbox.bind_publication_run(original, run_id="the-exact-run", guard=e.guard_factory()) == bound
    assert (await e.reserve()).run_id == "the-exact-run"
    with pytest.raises(ValueError):
        await e.inbox.bind_publication_run(original, run_id="another-run", guard=e.guard_factory())


async def test_binding_before_actual_marker_refuses_even_with_forged_observation(publication_environment):
    e = publication_environment
    original = await e.reserve()
    forged = dataclasses.replace(original, dispatch_started=True)
    with pytest.raises(ValueError):
        await e.inbox.bind_publication_run(forged, run_id="not-dispatched", guard=e.guard_factory())
    assert (await e.reserve()).dispatch_started is False
    assert all(row.run_id is None for row in await saved_origins(e))


@pytest.mark.parametrize("change", ["partial", "publication", "event", "digest", "version-bool"])
async def test_current_complete_immutable_identity_is_required(publication_environment, change):
    e = publication_environment
    record = await e.reserve()
    with pytest.raises(ValueError):
        if change == "partial":
            altered = dataclasses.replace(record, origins=record.origins[:1])
        elif change == "publication":
            altered = dataclasses.replace(record, publication_id="a" * 32)
        elif change == "event":
            altered = dataclasses.replace(record, event_id="m999")
        elif change == "digest":
            altered = dataclasses.replace(record, input_sha256="a" * 64)
        else:
            altered = dataclasses.replace(record, input_version=True)
        await e.inbox.mark_dispatch_started(altered, guard=e.guard_factory())
    assert (await e.reserve()).dispatch_started is False


async def test_committed_marker_never_resets_after_lost_commit_response(publication_environment):
    e = publication_environment
    record = await e.reserve()
    commits = []

    def lose_response(session):
        if session.get_bind() is e.db.engine.sync_engine:
            commits.append(True)
            raise RuntimeError("synthetic committed response loss")

    event.listen(Session, "after_commit", lose_response)
    try:
        with pytest.raises(RuntimeError, match="synthetic committed response loss"):
            await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())
    finally:
        event.remove(Session, "after_commit", lose_response)
    assert commits == [True]
    saved = await e.reserve()
    assert saved.dispatch_started is True
    assert await e.inbox.mark_dispatch_started(record, guard=e.guard_factory()) is False
    assert (await e.reserve()).publication_id == record.publication_id


async def test_new_current_token_can_replay_scope_while_old_attempt_refuses(publication_environment):
    e = publication_environment
    record = await e.reserve()
    old_guard = e.guard_factory()
    async with e.sessions.begin() as session:
        await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(token_revision=e.link.token_revision + 1))
    with pytest.raises(ValueError):
        await e.inbox.mark_dispatch_started(record, guard=old_guard)
    e.link = await e.links.get(e.link.id)
    assert await e.reserve() == record
    assert await e.inbox.mark_dispatch_started(record, guard=e.guard_factory()) is True
    assert (await e.reserve()).publication_id == record.publication_id


async def test_borrowed_lookup_returns_fresh_record_and_detached_complete_rows(publication_environment):
    e = publication_environment
    old = await e.reserve()
    await e.inbox.mark_dispatch_started(old, guard=e.guard_factory())
    await e.inbox.bind_publication_run(old, run_id="original-run", guard=e.guard_factory())
    async with e.sessions() as session, session.begin():
        if e.db.dialect == "sqlite":
            await session.execute(text("BEGIN IMMEDIATE"))
        await e.guard_factory().check(session)
        record, rows = await e.inbox.lock_publication(old, session=session)
        assert session.in_transaction()
        assert record.dispatch_started is True and record.run_id == "original-run"
        assert isinstance(rows, tuple) and len(rows) == 2
        rows[0].sanitized_input["text"] = "detached mutation"
    assert (await saved_origins(e))[0].sanitized_input["text"] == "First accepted input"


async def test_borrowed_lookup_refuses_absent_transaction_and_other_engine(publication_environment):
    e = publication_environment
    record = await e.reserve()
    async with e.sessions() as session:
        with pytest.raises(ValueError):
            await e.inbox.lock_publication(record, session=session)
    # A distinct engine view shares the same underlying database/pool; backend
    # reachability alone does not make it this repository's owning writer.
    other = e.db.engine.execution_options()
    other_sessions = async_sessionmaker(other, expire_on_commit=False)
    async with other_sessions() as session, session.begin():
        with pytest.raises(ValueError):
            await e.inbox.lock_publication(record, session=session)
    assert (await e.reserve()).dispatch_started is False


async def test_borrowed_changes_leave_commit_and_rollback_with_caller(publication_environment):
    e = publication_environment
    record = await e.reserve()
    async with e.sessions() as session:
        await session.begin()
        if e.db.dialect == "sqlite":
            await session.execute(text("BEGIN IMMEDIATE"))
        await e.guard_factory().check(session)
        fresh, _ = await e.inbox.lock_publication(record, session=session)
        assert fresh == record and session.in_transaction()
        await session.execute(update(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id == 701).values(state="interrupted"))
        await session.rollback()
    assert all(row.state == "dispatched" for row in await saved_origins(e))


async def test_other_links_guard_cannot_authorize_this_record(publication_environment):
    e = publication_environment
    record = await e.reserve()
    another = dataclasses.replace(e.link, id="f" * 32)
    with pytest.raises(ValueError):
        await e.inbox.mark_dispatch_started(record, guard=e.guard_factory(captured_link=another))
    assert (await e.reserve()).dispatch_started is False


async def test_binding_preserves_immutable_saved_input(publication_environment):
    e = publication_environment
    record = await e.reserve()
    await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())
    bound = await e.inbox.bind_publication_run(record, run_id="only-saved-binding", guard=e.guard_factory())
    assert bound.run_id == "only-saved-binding"
    assert (bound.input_sha256, bound.origins, bound.scope) == (record.input_sha256, record.origins, record.scope)


async def test_borrowed_seal_refuses_unproved_restart_kind(publication_environment):
    from uuid import uuid4

    e = publication_environment
    record = await e.reserve()
    identity = dataclasses.asdict(record)
    identity["origins"] = list(identity["origins"])
    identity.pop("dispatch_started")
    identity.pop("run_id")
    outcome = {"kind": "restart_before_dispatch", "projection": None, "source_ref": {"key": "chat.restarted", "values": {}}}
    with pytest.raises(ValueError):
        async with e.sessions() as session, session.begin():
            await e.guard_factory().check(session)
            outbox = TelegramOutboxRow(
                id=uuid4().hex,
                link_id=e.link.id,
                owner_user_id=e.owner_id,
                binding_generation=e.link.binding_generation,
                dedupe_key=f"reply:{record.turn_id}",
                class_="reply",
                payload={"reply_basis": {"version": 1, "publication": identity, "outcome": outcome, "presentation": None, "parts": [], "finish_route": True}, "source": outcome},
                parts=[],
                state="pending",
                not_before=e.origins[0].accepted_at,
                chat_id=e.link.chat_id,
                created_at=e.origins[0].accepted_at,
            )
            session.add(outbox)
            await session.flush()
            await e.inbox.seal_publication(record, outbox_id=outbox.id, assistant_message_id=None, session=session)
            await e.guard_factory().check(session)
    assert all(not row.route_complete for row in await saved_origins(e))


@pytest.mark.parametrize("withdraw_token", [False, True])
async def test_started_sql_writer_is_drained_under_repeated_cancellation(publication_environment, monkeypatch, withdraw_token):
    e = publication_environment
    record = await e.reserve()
    active, dispatched, backend_pid, remove_probe = install_actual_dispatch_probe(e, monkeypatch)
    task = None
    async with e.sessions() as holder:
        await holder.begin()
        if e.db.dialect == "sqlite":
            await holder.execute(text("BEGIN IMMEDIATE"))
        else:
            await holder.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).with_for_update())
        try:
            token = active.set(True)
            try:
                task = asyncio.create_task(e.inbox.mark_dispatch_started(record, guard=e.guard_factory()))
            finally:
                active.reset(token)
            await asyncio.wait_for(dispatched.wait(), 3)
            await require_real_lock_wait(e, backend_pid)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            done, _ = await asyncio.wait({task}, timeout=0.03)
            assert not done, "Caller escaped while its admitted native writer was still blocked"
            if withdraw_token:
                await holder.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(token_revision=e.link.token_revision + 1))
            await holder.commit()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            if holder.in_transaction():
                await holder.rollback()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            remove_probe()
    saved = await saved_origins(e)
    assert all(row.route_basis["publications"][0]["publication_execution"]["dispatch_started"] is (not withdraw_token) for row in saved)
    # After the actual retained unit settles, a distinct writer must commit and
    # read back. This is post-release cleanup proof, not a pre-release lock claim.
    async with e.sessions.begin() as writer:
        await writer.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(profile_version=2))
    async with e.sessions() as observer:
        assert await observer.scalar(select(TelegramBotLinkRow.profile_version).where(TelegramBotLinkRow.id == e.link.id)) == 2
    assert e.db.engine.sync_engine.pool.checkedout() == 0


async def test_reserve_detaches_caller_input_before_waiting_for_native_writer(publication_environment, monkeypatch):
    e = publication_environment
    original = dataclasses.replace(e.origins[0], sanitized_input=dict(e.origins[0].sanitized_input))
    expected = expected_input_digest((original, e.origins[1]))
    active, dispatched, backend_pid, remove_probe = install_actual_dispatch_probe(e, monkeypatch)
    task = None
    async with e.sessions() as holder:
        await holder.begin()
        if e.db.dialect == "sqlite":
            await holder.execute(text("BEGIN IMMEDIATE"))
        else:
            await holder.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).with_for_update())
        try:
            token = active.set(True)
            try:
                task = asyncio.create_task(e.reserve())
            finally:
                active.reset(token)
            await asyncio.wait_for(dispatched.wait(), 3)
            await require_real_lock_wait(e, backend_pid)
            e.origins[0].sanitized_input["text"] = "Caller changed during the actual SQL wait"
            await holder.commit()
            record = await task
        finally:
            if holder.in_transaction():
                await holder.rollback()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            remove_probe()
    assert record.input_sha256 == expected
    assert (await saved_origins(e))[0].sanitized_input["text"] == "First accepted input"
