"""Real SQL choice custody and recorded thread HTTP; no Telegram delivery proof."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import event, select, text, update
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.util import await_only
from test_telegram_bot_handlers import message
from test_telegram_threads import close, environment

from app.telegram_bots.callbacks import CallbackRepository
from app.telegram_bots.inbox import InboxRepository, InboxRow
from app.telegram_bots.texts import KEYBOARDS
from app.telegram_bots.threads import ThreadCoordinator
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio


async def command(e, *, identity=951, target="new", topic=None):
    incoming = message(e, identity, content="/new")
    if topic is not None:
        incoming.message._unfreeze()
        incoming.message.message_thread_id = topic
        incoming.message._freeze()
    row, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"command": "new"})
    return await e.inbox.route(row, kind="command", target_id=target, basis={"key": "new.confirm_plain", "values": {}, "keyboard_template": "new.confirm_plain"}, operation_ids=[])


async def guard(e, *, current=None):
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    return e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=current or (lambda: e.active[0] and e.current[0] is e.channel))


async def saved(e, row):
    async with e.db.sessions() as session:
        return InboxRow.from_row(await session.get(TelegramInboundMarkRow, (row.link_id, row.message_id, row.chat_id)))


async def offer(e, *, row=None, source_key="new.confirm_plain"):
    row = row or await command(e)
    # Existing public primitive gives claim-only baseline cases an actual saved
    # preparation without inventing the not-yet-added offer coordinator API.
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    prep = await e.db.links.reserve_thread_preparation(e.link, row, purpose="new", endpoint_id=e.http_client.endpoint_id, check_live_owner=owner, check_attempt_current=lambda: True, require_healthy=True)
    callbacks = CallbackRepository(e.db.sessions, config=e.db.config)
    parent_id = await e.services.outbox.plan_product_keyboard(
        e.link,
        source_id=f"new:{row.message_id}",
        parts=[{"kind": "text", "source_ref": {"key": source_key, "values": {}}}],
        keyboard=KEYBOARDS["new.confirm_plain"](thread_id=prep.expected_thread_id or prep.creation_id),
        callbacks=callbacks,
        guard=await guard(e),
        inbox_rows=[row],
    )
    async with e.db.sessions() as session:
        rows = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.link_id == e.link.id).order_by(TelegramCallbackRow.id)))
    selected = {row.kind: row.id for row in rows}
    group = rows[0].group_id
    assert await callbacks.adopt(e.link, group, 1951, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=parent_id, presentation_version=0, check_current=lambda: True)
    return SimpleNamespace(origin=prep.origin, preparation=prep, callbacks=callbacks, ids=selected, group=group, parent_id=parent_id)


async def claim(e, plan, kind, *, repository=None, **kwargs):
    return await (repository or e.inbox).claim_callback_choice(e.link, plan.origin, callback_id=plan.ids[kind], chat_id=e.link.chat_id, message_id=1951, message_thread_id=None, callbacks=plan.callbacks, guard=await guard(e), **kwargs)


async def test_offer_reserves_new_identity_and_mapping_without_http(db, monkeypatch):
    e = await environment(db, monkeypatch)
    e.db = db
    try:
        await db.connections.set_thread_id(connection_id=e.link.connection_id, owner_user_id=e.link.owner_user_id, provider="telegram", external_conversation_id=str(e.link.chat_id), thread_id="old-thread")
        row = await command(e)
        first = await e.threads.reserve_new_conversation(e.link, origin=row)
        replay = await e.threads.reserve_new_conversation(e.link, origin=row)
        assert first == replay and first.purpose == "new"
        assert first.expected_thread_id == "old-thread" and first.expected_mapping_id
        assert first.creation_id != "old-thread" and len(first.creation_id) == 32
        assert first.endpoint_id == e.http_client.endpoint_id
        assert e.http_calls == []
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) == "old-thread"
        assert not first.origin.route_complete and first.origin.thread_id is None
    finally:
        await close(e)


@pytest.mark.parametrize("first,second", [("new_confirm", "new_keep"), ("new_keep", "new_confirm")])
async def test_first_choice_is_durable_and_competitor_refuses_before_http(db, monkeypatch, first, second):
    e = await environment(db, monkeypatch)
    e.db = db
    try:
        plan = await offer(e)
        chosen = await claim(e, plan, first)
        assert chosen.route_basis["callback_choice"] == {"version": 1, "callback_id": plan.ids[first], "kind": first, "group_id": plan.group, "anchor_outbox_id": plan.parent_id}
        assert await claim(e, plan, first, repository=InboxRepository(db.sessions)) == chosen
        with pytest.raises(ValueError):
            await claim(e, plan, second, repository=InboxRepository(db.sessions))
        assert e.http_calls == [] and not chosen.route_complete
        assert all([(await plan.callbacks.get(identity)).consumed_at is None for identity in plan.ids.values()])
    finally:
        await close(e)


async def test_independent_receivers_have_one_durable_choice(db, monkeypatch):
    e = await environment(db, monkeypatch)
    e.db = db
    tasks = []
    try:
        plan = await offer(e)
        tasks = [asyncio.create_task(claim(e, plan, kind, repository=InboxRepository(db.sessions))) for kind in ("new_confirm", "new_keep")]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        winners = [result for result in results if isinstance(result, InboxRow)]
        assert len(winners) == 1 and sum(isinstance(result, ValueError) for result in results) == 1
        actual = await saved(e, plan.origin)
        assert actual.route_basis["callback_choice"] == winners[0].route_basis["callback_choice"]
        assert e.http_calls == [] and not actual.route_complete
    finally:
        await asyncio.gather(*tasks, return_exceptions=True)
        await close(e)


async def test_receiver_recreation_uses_original_offer_uuid_then_exact_mapping_receipt(db, monkeypatch):
    e = await environment(db, monkeypatch)
    e.db = db
    try:
        row = await command(e)
        prep = await e.threads.reserve_new_conversation(e.link, origin=row)
        plan = await offer(e, row=row)
        chosen = await claim(e, plan, "new_confirm")
        await e.threads.stop()
        e.threads = ThreadCoordinator(links=db.links, authority=e.live.authority, client=e.http_client, channel=lambda identity: e.current[0], send_allowed=lambda: e.active[0], outbox=e.services.outbox, product_name="HartMesh")
        replay = await e.inbox.claim_callback_choice(e.link, chosen, callback_id=plan.ids["new_confirm"], chat_id=e.link.chat_id, message_id=1951, message_thread_id=None, callbacks=plan.callbacks, guard=await guard(e))
        assert replay.route_basis["thread_preparation"] == prep.origin.route_basis["thread_preparation"]
        assert await e.threads.new_conversation(e.link, origin=replay) == prep.creation_id
        assert await e.threads.new_conversation(e.link, origin=chosen) == prep.creation_id
        assert [request.method for request in e.http_calls] == ["POST", "GET", "GET"]
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) == prep.creation_id
        assert (await saved(e, row)).route_basis["callback_choice"] == chosen.route_basis["callback_choice"]
    finally:
        await close(e)


async def test_offer_mapping_bookmark_cannot_overwrite_a_newer_mapping(db, monkeypatch):
    e = await environment(db, monkeypatch)
    e.db = db
    try:
        row = await command(e)
        prep = await e.threads.reserve_new_conversation(e.link, origin=row)
        plan = await offer(e, row=row)
        chosen = await claim(e, plan, "new_confirm")
        await db.connections.set_thread_id(connection_id=e.link.connection_id, owner_user_id=e.link.owner_user_id, provider="telegram", external_conversation_id=str(e.link.chat_id), thread_id="newer-thread")
        with pytest.raises(ValueError):
            await e.threads.new_conversation(e.link, origin=chosen)
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) == "newer-thread"
        assert (await saved(e, row)).route_basis["thread_preparation"]["creation_id"] == prep.creation_id
        assert e.http_calls == []
    finally:
        await close(e)


async def test_borrowed_choice_rolls_back_with_caller_and_does_not_prepare_inside_sql(db, monkeypatch):
    e = await environment(db, monkeypatch)
    e.db = db
    try:
        plan = await offer(e)
        actual = await guard(e)

        async def forbidden_prepare():
            raise AssertionError("no preparation under borrowed writer")

        borrowed = SimpleNamespace(captured_link=e.link, check=actual.check, prepare=forbidden_prepare)
        with pytest.raises(RuntimeError, match="caller rollback"):
            async with db.sessions() as session, session.begin():
                if db.dialect == "sqlite":
                    await session.execute(text("BEGIN IMMEDIATE"))
                row = await e.inbox.claim_callback_choice(e.link, plan.origin, callback_id=plan.ids["new_keep"], chat_id=e.link.chat_id, message_id=1951, message_thread_id=None, callbacks=plan.callbacks, guard=borrowed, session=session)
                assert row.route_basis["callback_choice"]["kind"] == "new_keep"
                raise RuntimeError("caller rollback")
        assert "callback_choice" not in (await saved(e, plan.origin)).route_basis
        assert (await claim(e, plan, "new_confirm")).route_basis["callback_choice"]["kind"] == "new_confirm"
    finally:
        await close(e)


@pytest.mark.parametrize("corruption", ["other_source", "another_origin", "sealed", "route", "prep", "choice_boolean", "wrong_thread"])
async def test_choice_refuses_changed_saved_source_or_original_command(db, monkeypatch, corruption):
    e = await environment(db, monkeypatch)
    e.db = db
    try:
        plan = await offer(e, source_key="help.chat" if corruption == "other_source" else "new.confirm_plain")
        async with db.sessions.begin() as session:
            mark = await session.get(TelegramInboundMarkRow, (plan.origin.link_id, plan.origin.message_id, plan.origin.chat_id))
            if corruption == "another_origin":
                parent = await session.get(TelegramOutboxRow, plan.parent_id)
                parent.payload = {**parent.payload, "product_basis": {**parent.payload["product_basis"], "origins": [[mark.link_id, mark.message_id + 1, mark.chat_id]]}}
            elif corruption == "sealed":
                mark.route_complete = True
            elif corruption == "route":
                mark.route_target_id = "help"
            elif corruption == "prep":
                mark.route_basis = {**mark.route_basis, "thread_preparation": {**mark.route_basis["thread_preparation"], "purpose": "ensure"}}
            elif corruption == "choice_boolean":
                mark.route_basis = {**mark.route_basis, "callback_choice": {"version": True, "callback_id": plan.ids["new_confirm"], "kind": "new_confirm", "group_id": plan.group, "anchor_outbox_id": plan.parent_id}}
            elif corruption == "wrong_thread":
                # Change the durable reservation, leaving its actual button
                # template untouched; the button cannot grant another target.
                mark.route_basis = {**mark.route_basis, "thread_preparation": {**mark.route_basis["thread_preparation"], "creation_id": "a" * 32}}
        with pytest.raises(ValueError):
            await claim(e, plan, "new_confirm")
        actual = await saved(e, plan.origin)
        assert "callback_choice" not in actual.route_basis or corruption == "choice_boolean"
        assert e.http_calls == []
    finally:
        await close(e)


async def test_business_route_replay_ignores_choice_but_ingress_cannot_supply_it(db, monkeypatch):
    e = await environment(db, monkeypatch)
    e.db = db
    try:
        plan = await offer(e)
        chosen = await claim(e, plan, "new_keep")
        replay = await e.inbox.route(plan.origin, kind="command", target_id="new", basis={"key": "new.confirm_plain", "values": {}, "keyboard_template": "new.confirm_plain"}, operation_ids=[])
        assert replay.route_basis == chosen.route_basis
        another = await command(e, identity=952)
        with pytest.raises(ValueError):
            await e.inbox.route(another, kind="command", target_id="new", basis={"callback_choice": chosen.route_basis["callback_choice"]}, operation_ids=[])
    finally:
        await close(e)


@pytest.mark.parametrize("changed", ["wrong_route", "topic", "host", "health", "owner"])
async def test_offer_refuses_unsupported_or_retired_scope_without_http(db, monkeypatch, changed):
    e = await environment(db, monkeypatch)
    e.db = db
    try:
        row = await command(e, target="help" if changed == "wrong_route" else "new", topic=77 if changed == "topic" else None)
        if changed == "host":
            e.active[0] = False
        elif changed == "health":
            await db.links.set_health(e.link.id, "policy_off", holds=True, expected_token_revision=e.link.token_revision)
        elif changed == "owner":
            from deerflow.persistence.telegram_bots.model import TelegramAccountRow

            async with db.sessions.begin() as session:
                await session.execute(update(TelegramAccountRow).where(TelegramAccountRow.telegram_user_id == e.link.telegram_user_id).values(owner_user_id="another-owner"))
        with pytest.raises(ValueError):
            await e.threads.reserve_new_conversation(e.link, origin=row)
        assert "thread_preparation" not in (await saved(e, row)).route_basis
        assert e.http_calls == []
    finally:
        await close(e)


async def test_sealed_selected_command_cannot_be_offered_again(db, monkeypatch):
    e = await environment(db, monkeypatch)
    e.db = db
    try:
        row = await command(e)
        prep = await e.threads.reserve_new_conversation(e.link, origin=row)
        assert await e.threads.new_conversation(e.link, origin=prep.origin) == prep.creation_id
        await e.inbox.finish_route(prep.origin)
        with pytest.raises(ValueError):
            await e.threads.reserve_new_conversation(e.link, origin=row)
    finally:
        await close(e)


async def test_cancelled_offer_retains_committed_sql_and_reuses_exact_uuid(db, monkeypatch):
    e = await environment(db, monkeypatch)
    e.db = db
    entered, release = asyncio.Event(), asyncio.Event()
    original, committed, task = db.links.reserve_thread_preparation, [], None

    async def held(*args, **kwargs):
        result = await original(*args, **kwargs)
        committed.append(result)
        entered.set()
        await release.wait()
        return result

    try:
        row = await command(e)
        monkeypatch.setattr(db.links, "reserve_thread_preparation", held)
        task = asyncio.create_task(e.threads.reserve_new_conversation(e.link, origin=row))
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.03))[0]
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.03))[0]
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        monkeypatch.setattr(db.links, "reserve_thread_preparation", original)
        replay = await e.threads.reserve_new_conversation(e.link, origin=row)
        assert len(committed) == 1 and replay.creation_id == committed[0].creation_id
        assert e.http_calls == []
    finally:
        release.set()
        if task:
            await asyncio.gather(task, return_exceptions=True)
        await close(e)


async def test_choice_final_original_attempt_fence_rolls_back_claim(db, monkeypatch):
    e = await environment(db, monkeypatch)
    e.db = db
    try:
        plan = await offer(e)
        actual = await guard(e)
        checks = []

        async def check(session):
            current = await actual.check(session)
            checks.append(True)
            if len(checks) == 2:
                raise ValueError("original attempt retired")
            return current

        final_guard = SimpleNamespace(captured_link=e.link, check=check)
        with pytest.raises(ValueError, match="original attempt retired"):
            await e.inbox.claim_callback_choice(e.link, plan.origin, callback_id=plan.ids["new_confirm"], chat_id=e.link.chat_id, message_id=1951, message_thread_id=None, callbacks=plan.callbacks, guard=final_guard)
        assert len(checks) == 2 and "callback_choice" not in (await saved(e, plan.origin)).route_basis
        assert e.http_calls == []
    finally:
        await close(e)


async def test_forged_offer_origin_cannot_trigger_custom_deletion_notice(db, monkeypatch):
    from deerflow.config import agents_config

    e = await environment(db, monkeypatch, lead=False)
    e.db, reads = db, []

    def missing(name, *, user_id):
        reads.append(name)
        raise FileNotFoundError("synthetic absent target")

    monkeypatch.setattr(agents_config, "load_agent_config", missing)
    try:
        row = replace(await command(e), message_id=987654)
        async with db.sessions() as session:
            before = set(await session.scalars(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == e.link.id)))
        with pytest.raises(ValueError):
            await e.threads.reserve_new_conversation(e.link, origin=row)
        assert reads == [] and e.http_calls == []
        assert (await db.links.get(e.link.id)).health == "ok"
        async with db.sessions() as session:
            assert set(await session.scalars(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == e.link.id))) == before
    finally:
        await close(e)


@pytest.mark.parametrize("transaction", ["absent", "foreign_engine"])
async def test_choice_borrowed_writer_requires_active_original_engine(db, monkeypatch, tmp_path, transaction):
    e = await environment(db, monkeypatch)
    e.db = db
    other = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'foreign.sqlite'}")
    try:
        plan = await offer(e)
        if transaction == "absent":
            async with db.sessions() as session:
                with pytest.raises(ValueError, match="publication-writer-required"):
                    await claim(e, plan, "new_confirm", session=session)
        else:
            from sqlalchemy.ext.asyncio import async_sessionmaker

            async with async_sessionmaker(other)() as session, session.begin():
                with pytest.raises(ValueError, match="publication-writer-required"):
                    await claim(e, plan, "new_confirm", session=session)
        assert "callback_choice" not in (await saved(e, plan.origin)).route_basis
    finally:
        await other.dispose()
        await close(e)


async def test_cancelled_choice_retains_actual_commit_and_exact_replay(db, monkeypatch):
    """Held SQLAlchemy post-COMMIT completion, not a native cursor barrier."""
    e = await environment(db, monkeypatch)
    e.db = db
    entered, release = asyncio.Event(), asyncio.Event()
    session_class = db.sessions.class_.sync_session_class
    task = None

    def before_flush(session, *_):
        if session.get_bind() is db.engine.sync_engine and any(isinstance(row, TelegramInboundMarkRow) and "callback_choice" in row.route_basis for row in session.dirty):
            # The ORM identity map is weak: the helper returns a detached DTO
            # before commit. Carry the actual observed write to its commit hook.
            session.info["c_new_choice_commit"] = True

    def after_commit(session):
        if session.info.pop("c_new_choice_commit", False):
            entered.set()
            await_only(release.wait())

    try:
        plan = await offer(e)
        event.listen(session_class, "before_flush", before_flush)
        event.listen(session_class, "after_commit", after_commit)
        task = asyncio.create_task(claim(e, plan, "new_confirm"))
        await asyncio.wait_for(entered.wait(), 5)
        assert (await saved(e, plan.origin)).route_basis["callback_choice"]["kind"] == "new_confirm"
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.03))[0]
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.03))[0]
        release.set()
        if event.contains(session_class, "before_flush", before_flush):
            event.remove(session_class, "before_flush", before_flush)
        with pytest.raises(asyncio.CancelledError):
            await task
        event.remove(session_class, "after_commit", after_commit)
        assert (await claim(e, plan, "new_confirm")).route_basis["callback_choice"]["callback_id"] == plan.ids["new_confirm"]
        with pytest.raises(ValueError):
            await claim(e, plan, "new_keep")
        assert e.http_calls == []
    finally:
        release.set()
        if event.contains(session_class, "before_flush", before_flush):
            event.remove(session_class, "before_flush", before_flush)
        if event.contains(session_class, "after_commit", after_commit):
            event.remove(session_class, "after_commit", after_commit)
        if task:
            await asyncio.gather(task, return_exceptions=True)
        await close(e)
