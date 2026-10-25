"""Origin-backed preparation and replacement identity on real SQL backends."""

import asyncio
import dataclasses
from datetime import UTC, datetime

import pytest
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import add_link, bind_link, rows
from _telegram_repository_test_support import db as db
from sqlalchemy import event, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from telegram import Chat, Message, Update, User

from app.telegram_bots.authority import AccessOff
from app.telegram_bots.inbox import InboxRepository
from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
from deerflow.persistence.channel_connections.model import ChannelConnectionRow, ChannelConversationRow
from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramBotLinkRow, TelegramInboundMarkRow
from deerflow.persistence.user.access import DisabledIdentityRow
from deerflow.persistence.user.model import UserRow

pytestmark = pytest.mark.asyncio
NOW = datetime(2026, 10, 24, tzinfo=UTC)
ENDPOINT = "a" * 64


async def setup(db, monkeypatch):
    owner = await install_live_owner(db, monkeypatch)
    link = await bind_link(db, await add_link(db, owner=owner.id))
    return link, InboxRepository(db.sessions), await owner.authority.prepare_live_owner(owner.id)


async def origin(inbox, link, *, message=301, purpose="ensure"):
    value = Update(message + 100, message=Message(message, NOW, Chat(link.chat_id, "private"), from_user=User(link.telegram_user_id, "Fixture", False), text="safe turn"))
    accepted, _ = await inbox.accept(link, value, sanitized_input={"text": "safe turn"})
    return await inbox.route(accepted, kind="chat" if purpose == "ensure" else "command", target_id=link.agent_ref, basis={"purpose": purpose}, operation_ids=[])


async def reserve(db, link, mark, checker, *, purpose="ensure", endpoint=ENDPOINT, current=lambda: True):
    return await db.links.reserve_thread_preparation(link, mark, purpose=purpose, endpoint_id=endpoint, check_live_owner=checker, check_attempt_current=current)


async def commit(db, link, preparation, checker, *, thread=None, endpoint=ENDPOINT, current=lambda: True):
    return await db.links.commit_thread_mapping(link, preparation, thread_id=thread or preparation.creation_id, endpoint_id=endpoint, check_live_owner=checker, check_attempt_current=current)


async def set_mapping(db, link, thread):
    await db.connections.set_thread_id(connection_id=link.connection_id, owner_user_id=link.owner_user_id, provider="telegram", external_conversation_id=str(link.chat_id), thread_id=thread)


async def mapping(db):
    return (await rows(db, ChannelConversationRow))[0]


async def test_route_cannot_inject_thread_preparation(db, monkeypatch):
    link, inbox, _ = await setup(db, monkeypatch)
    value = Update(501, message=Message(401, NOW, Chat(link.chat_id, "private"), from_user=User(link.chat_id, "Fixture", False), text="safe"))
    mark, _ = await inbox.accept(link, value, sanitized_input={"text": "safe"})
    with pytest.raises(ValueError, match="invalid-inbox-route"):
        await inbox.route(mark, kind="chat", target_id=link.agent_ref, basis={"thread_preparation": {"selected_thread_id": "forged"}}, operation_ids=[])


async def test_reservation_reuses_uuid_and_preserves_business_route_and_grouping(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    first, second = await origin(inbox, link), await origin(inbox, link, message=302)
    prepared = await reserve(db, link, first, checker)
    repeated = await reserve(db, link, first, checker)
    assert prepared.creation_id == repeated.creation_id and len(prepared.creation_id) == 32
    assert prepared.expected_mapping_id is None and prepared.expected_thread_id is None
    assert prepared.selected_thread_id is None and prepared.endpoint_id == ENDPOINT
    assert "token_revision" not in prepared.scope
    replay = await inbox.route(first, kind="chat", target_id=link.agent_ref, basis={"purpose": "ensure"}, operation_ids=[])
    assert replay.route_basis["thread_preparation"]["creation_id"] == prepared.creation_id
    assert replay.sanitized_input == first.sanitized_input
    await inbox.bind_turn([replay, second], turn_id="c" * 32, thread_id="selected")


async def test_distinct_preparations_do_not_split_joined_publication_comparisons(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    first, second = await origin(inbox, link), await origin(inbox, link, message=302)
    prepared = [await reserve(db, link, mark, checker) for mark in (first, second)]
    assert prepared[0].creation_id != prepared[1].creation_id
    await inbox.bind_turn([first, second], turn_id="c" * 32, thread_id="selected")
    publication = (await inbox.route(first, kind="chat", target_id=link.agent_ref, basis={"purpose": "ensure"}, operation_ids=[])).route_basis["current_publication_id"]
    await inbox.bind_run(turn_id="c" * 32, publication_id=publication, run_id="run-one")
    assert await inbox.publish([first, second], event_id="m301:r1", expected_publication_id=publication) != publication
    marks = [row for row in await rows(db, TelegramInboundMarkRow) if row.message_id in {301, 302}]
    assert {row.route_basis["thread_preparation"]["creation_id"] for row in marks} == {item.creation_id for item in prepared}


async def test_generic_replacement_rotates_id_at_frozen_clock_and_preserves_created_at(db, monkeypatch):
    from deerflow.persistence.channel_connections import model, sql

    link, _, _ = await setup(db, monkeypatch)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW

    monkeypatch.setattr(sql, "datetime", Clock)
    monkeypatch.setattr(model, "datetime", Clock)
    await set_mapping(db, link, "thread-a")
    first = await mapping(db)
    await set_mapping(db, link, "thread-b")
    second = await mapping(db)
    await set_mapping(db, link, "thread-a")
    third = await mapping(db)
    assert len({first.id, second.id, third.id}) == 3
    assert first.created_at == second.created_at == third.created_at
    assert first.updated_at == second.updated_at == third.updated_at
    assert third.thread_id == "thread-a"


async def test_mapping_and_outcome_commit_once_and_receipt_never_rewinds_newer_mapping(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    mark = await origin(inbox, link)
    prepared = await reserve(db, link, mark, checker)
    assert await commit(db, link, prepared, checker) is True
    saved = await reserve(db, link, mark, checker)
    assert saved.selected_thread_id == prepared.creation_id and saved.mapping_id == (await mapping(db)).id
    await set_mapping(db, link, "later-new")
    later = await mapping(db)
    assert await commit(db, link, saved, checker) is True
    assert (await mapping(db)).id == later.id
    assert (await mapping(db)).thread_id == "later-new"


async def test_same_binding_token_rotation_reuses_intent_but_rejects_old_attempt(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    mark = await origin(inbox, link)
    prepared = await reserve(db, link, mark, checker)
    fresh = await db.links.store_token(link.id, "wrapped-replacement", actor_id=link.owner_user_id)
    assert await commit(db, link, prepared, checker) is False
    retry = await reserve(db, fresh, mark, checker)
    assert retry.creation_id == prepared.creation_id and retry.scope == prepared.scope
    assert await commit(db, fresh, retry, checker) is True


async def test_generic_aba_invalidates_saved_mapping_expectation(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    await set_mapping(db, link, "thread-a")
    prepared = await reserve(db, link, await origin(inbox, link), checker)
    await set_mapping(db, link, "thread-b")
    await set_mapping(db, link, "thread-a")
    assert await commit(db, link, prepared, checker) is False
    assert (await mapping(db)).thread_id == "thread-a"
    saved = await rows(db, TelegramInboundMarkRow)
    assert next(row for row in saved if row.message_id == 301).route_basis["thread_preparation"]["selected_thread_id"] is None


async def test_pending_reserve_replay_refuses_changed_mapping_before_http(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    mark = await origin(inbox, link)
    prepared = await reserve(db, link, mark, checker)
    await set_mapping(db, link, "independent-new")
    with pytest.raises(ValueError, match="thread-preparation-stale"):
        await reserve(db, link, mark, checker)
    saved = next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == 301).route_basis["thread_preparation"]
    assert saved["creation_id"] == prepared.creation_id and saved["selected_thread_id"] is None


async def test_delayed_ensure_cannot_overwrite_independent_new_conversation(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    await set_mapping(db, link, "old-thread")
    delayed = await reserve(db, link, await origin(inbox, link), checker)
    newer = await reserve(db, link, await origin(inbox, link, message=302, purpose="new"), checker, purpose="new")
    assert await commit(db, link, newer, checker) is True
    assert await commit(db, link, delayed, checker) is False
    assert (await mapping(db)).thread_id == newer.creation_id


async def test_final_attempt_check_runs_after_awaited_live_owner_guard(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    mark = await origin(inbox, link)
    current = True

    async def changed(owner, writer):
        nonlocal current
        await checker(owner, writer)
        current = False

    with pytest.raises(ValueError, match="thread-preparation-stale"):
        await reserve(db, link, mark, changed, current=lambda: current)
    assert "thread_preparation" not in next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == 301).route_basis
    current = True
    prepared = await reserve(db, link, mark, checker)
    assert await commit(db, link, prepared, changed, current=lambda: current) is False
    assert not await rows(db, ChannelConversationRow)


async def test_commit_failure_rolls_back_mapping_and_outcome_together(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    prepared = await reserve(db, link, await origin(inbox, link), checker)

    def reject(writer):
        if writer.get_bind() is db.engine.sync_engine:
            raise RuntimeError("fixture-commit-refused")

    event.listen(Session, "before_commit", reject)
    try:
        with pytest.raises(RuntimeError, match="fixture-commit-refused"):
            await commit(db, link, prepared, checker)
    finally:
        event.remove(Session, "before_commit", reject)
    assert not await rows(db, ChannelConversationRow)
    assert (await reserve(db, link, prepared.origin, checker)).selected_thread_id is None


@pytest.mark.parametrize("field,value", [("endpoint_id", "b" * 64), ("creation_id", "f" * 32), ("expected_thread_id", "forged"), ("selected_thread_id", "forged")])
async def test_forged_preparation_cannot_record_success(db, monkeypatch, field, value):
    link, inbox, checker = await setup(db, monkeypatch)
    prepared = await reserve(db, link, await origin(inbox, link), checker)
    assert await commit(db, link, dataclasses.replace(prepared, **{field: value}), checker) is False
    assert not await rows(db, ChannelConversationRow)


async def test_concurrent_first_generic_replacements_use_one_natural_key(db, monkeypatch):
    link, _, _ = await setup(db, monkeypatch)
    # Force every legacy read-before-insert through its real empty SELECT.
    # An atomic replacement statement needs no such read and bypasses this.
    barrier = asyncio.Barrier(8)
    execute = AsyncSession.execute

    async def observed(writer, statement, *args, **kwargs):
        result = await execute(writer, statement, *args, **kwargs)
        if writer.get_bind() is db.engine.sync_engine and getattr(statement, "is_select", False) and "channel_conversations" in str(statement):
            await barrier.wait()
        return result

    monkeypatch.setattr(AsyncSession, "execute", observed)
    tasks = [asyncio.create_task(set_mapping(db, link, f"thread-{index}")) for index in range(8)]
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), 10)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        monkeypatch.setattr(AsyncSession, "execute", execute)
    assert len(await rows(db, ChannelConversationRow)) == 1


async def test_forged_boolean_generation_is_not_an_integer_scope_receipt(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    prepared = await reserve(db, link, await origin(inbox, link), checker)
    assert prepared.scope["binding_generation"] == 1
    forged = dataclasses.replace(prepared, scope={**prepared.scope, "binding_generation": True})
    assert await commit(db, link, forged, checker) is False
    assert not await rows(db, ChannelConversationRow)


@pytest.mark.parametrize("allowed", [False, 1, None])
async def test_health_attempt_guard_follows_real_sql_and_does_not_publish_a_cause(db, monkeypatch, allowed):
    link, _, _ = await setup(db, monkeypatch)
    checked = False
    scalar = AsyncSession.scalar

    async def observed(writer, statement, *args, **kwargs):
        nonlocal checked
        result = await scalar(writer, statement, *args, **kwargs)
        if writer.get_bind() is db.engine.sync_engine and "telegram_bot_links" in str(statement):
            checked = True
        return result

    monkeypatch.setattr(AsyncSession, "scalar", observed)
    result = await db.links.set_health(link.id, "agent_unavailable", holds=True, expected_token_revision=link.token_revision, check_attempt_current=lambda: allowed if checked else True)
    assert checked and result is None
    assert (await db.links.get(link.id)).health == "ok"
    assert (await db.links.set_health(link.id, None, holds=False)).health == "ok"


async def test_health_old_adapter_cannot_mark_a_same_token_rebinding_after_sql_wait(db, monkeypatch):
    link, _, _ = await setup(db, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    scalar = AsyncSession.scalar
    task = None

    async def observed(writer, statement, *args, **kwargs):
        result = await scalar(writer, statement, *args, **kwargs)
        if asyncio.current_task() is task and str(statement).startswith("SELECT telegram_bot_links.owner_user_id"):
            entered.set()
            await release.wait()
        return result

    monkeypatch.setattr(AsyncSession, "scalar", observed)
    task = asyncio.create_task(db.links.set_health(link.id, "agent_unavailable", holds=True, expected_token_revision=link.token_revision, captured_link=link, check_attempt_current=lambda: True))
    waiting = asyncio.create_task(entered.wait())
    try:
        done, _ = await asyncio.wait([task, waiting], timeout=5, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            task.result()
        assert waiting in done
        unbound = await db.links.unbind(link, actor_id=link.owner_user_id)
        rebound = await bind_link(db, unbound, message=102, update=202)
        assert rebound.token_revision == link.token_revision and rebound.binding_generation != link.binding_generation
        release.set()
        assert await asyncio.wait_for(task, 10) is None
        assert (await db.links.get(link.id)).health == "ok"
        assert (await db.links.set_health(link.id, None, holds=False)).health == "ok"
    finally:
        release.set()
        for owned in (task, waiting):
            if not owned.done():
                owned.cancel()
        await asyncio.gather(task, waiting, return_exceptions=True)


@pytest.mark.parametrize("state", ["unrouted", "sealed", "interrupted", "missing", "wrong-chat", "forged-route"])
async def test_ineligible_or_forged_origin_never_allocates_an_intent(db, monkeypatch, state):
    link, inbox, checker = await setup(db, monkeypatch)
    mark = await origin(inbox, link)
    if state in {"unrouted", "sealed", "interrupted", "missing"}:
        async with db.sessions.begin() as writer:
            row = await writer.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))
            if state == "unrouted":
                row.route_kind = None
            elif state == "sealed":
                row.route_complete = True
            elif state == "interrupted":
                row.state = "interrupted"
            else:
                await writer.delete(row)
    elif state == "wrong-chat":
        mark = dataclasses.replace(mark, chat_id=mark.chat_id + 1)
    else:
        mark = dataclasses.replace(mark, route_basis={"purpose": "other"})
    with pytest.raises(ValueError, match="thread-preparation-stale"):
        await reserve(db, link, mark, checker)
    assert all("thread_preparation" not in row.route_basis for row in await rows(db, TelegramInboundMarkRow))
    assert not await rows(db, ChannelConversationRow)


async def test_sealed_pending_intent_refuses_but_committed_receipt_remains_readable(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    pending_mark = await origin(inbox, link)
    pending = await reserve(db, link, pending_mark, checker)
    await inbox.finish_route(pending_mark)
    with pytest.raises(ValueError, match="thread-preparation-stale"):
        await reserve(db, link, pending_mark, checker)
    assert await commit(db, link, pending, checker) is False
    completed_mark = await origin(inbox, link, message=302, purpose="new")
    completed = await reserve(db, link, completed_mark, checker, purpose="new")
    assert await commit(db, link, completed, checker) is True
    await inbox.finish_route(completed_mark)
    replay = await reserve(db, link, completed_mark, checker, purpose="new")
    assert replay.selected_thread_id == completed.creation_id


@pytest.mark.parametrize(
    "place,field,value",
    [
        ("link", "state", "blocked"),
        ("link", "binding_generation", 100),
        ("link", "token_revision", 100),
        ("link", "agent_ref", "another-target"),
        ("link", "bot_id", 9_000_000_099),
        ("link", "chat_id", 7_000_000_099),
        ("link", "connection_id", None),
        ("account", "owner_user_id", "foreign-owner"),
        ("connection", "owner_user_id", "foreign-owner"),
        ("connection", "provider", "foreign-provider"),
        ("connection", "external_account_id", "foreign-account"),
        ("connection", "workspace_id", "foreign-bot"),
        ("connection", "bot_user_id", "foreign-bot"),
        ("connection", "status", "held"),
    ],
)
async def test_mapping_commit_fences_every_captured_authority_fact(db, monkeypatch, place, field, value):
    link, inbox, checker = await setup(db, monkeypatch)
    prepared = await reserve(db, link, await origin(inbox, link), checker)
    cls, identity = {"link": (TelegramBotLinkRow, link.id), "account": (TelegramAccountRow, link.telegram_user_id), "connection": (ChannelConnectionRow, link.connection_id)}[place]
    async with db.sessions.begin() as writer:
        setattr(await writer.get(cls, identity), field, value)
    assert await commit(db, link, prepared, checker) is False
    assert not await rows(db, ChannelConversationRow)
    saved = next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == 301).route_basis["thread_preparation"]
    assert saved["creation_id"] == prepared.creation_id and saved["selected_thread_id"] is None


@pytest.mark.parametrize("phase", ["reserve", "commit"])
@pytest.mark.parametrize("refusal", ["disabled", "released"])
async def test_fresh_derived_account_refusal_rolls_back_preparation(db, monkeypatch, phase, refusal):
    link, inbox, checker = await setup(db, monkeypatch)
    mark = await origin(inbox, link)
    prepared = await reserve(db, link, mark, checker) if phase == "commit" else None
    async with db.sessions.begin() as writer:
        user = await writer.get(UserRow, link.owner_user_id)
        if refusal == "disabled":
            writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=link.owner_user_id, disabled_at=NOW))
        else:
            user.released_at = NOW
    with pytest.raises(AccessOff):
        await reserve(db, link, mark, checker) if phase == "reserve" else await commit(db, link, prepared, checker)
    assert not await rows(db, ChannelConversationRow)
    saved = next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == 301).route_basis.get("thread_preparation")
    assert saved is None if phase == "reserve" else saved["selected_thread_id"] is None


async def test_concurrent_absent_cas_commits_one_mapping_and_one_matching_outcome(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    preparations = [await reserve(db, link, await origin(inbox, link, message=message), checker) for message in (301, 302)]
    results = await asyncio.gather(*(commit(db, link, item, checker) for item in preparations))
    assert sorted(results) == [False, True]
    current = await mapping(db)
    outcomes = [row.route_basis["thread_preparation"] for row in await rows(db, TelegramInboundMarkRow) if row.message_id in {301, 302}]
    completed = [saved for saved in outcomes if saved["selected_thread_id"] is not None]
    assert len(completed) == 1 and completed[0]["selected_thread_id"] == current.thread_id and completed[0]["mapping_id"] == current.id


async def test_fenced_and_generic_replacements_both_defeat_frozen_clock_aba(db, monkeypatch):
    from app.telegram_bots import links
    from deerflow.persistence.channel_connections import model, sql

    link, inbox, checker = await setup(db, monkeypatch)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW

    monkeypatch.setattr(model, "datetime", Clock)
    monkeypatch.setattr(sql, "datetime", Clock)
    monkeypatch.setattr(links, "_now", lambda: NOW)
    await set_mapping(db, link, "thread-a")
    first = await mapping(db)
    delayed_a = await reserve(db, link, await origin(inbox, link), checker)
    newer = await reserve(db, link, await origin(inbox, link, message=302, purpose="new"), checker, purpose="new")
    assert await commit(db, link, newer, checker) is True
    second = await mapping(db)
    assert second.id != first.id and second.created_at == first.created_at
    delayed_b = await reserve(db, link, await origin(inbox, link, message=303), checker)
    await set_mapping(db, link, "thread-a")
    assert await commit(db, link, delayed_a, checker) is False
    await set_mapping(db, link, newer.creation_id)
    third = await mapping(db)
    assert await commit(db, link, delayed_b, checker) is False
    assert len({first.id, second.id, third.id}) == 3
    assert first.created_at == second.created_at == third.created_at and first.updated_at == second.updated_at == third.updated_at


async def test_reuse_keeps_mapping_identity_and_neither_writer_mutates_link_or_pause(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    async with db.sessions.begin() as writer:
        await writer.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(acts_paused_since=NOW))
    captured = await db.links.get(link.id)
    await set_mapping(db, captured, "reused")
    first = await mapping(db)
    prepared = await reserve(db, captured, await origin(inbox, captured), checker)
    assert await commit(db, captured, prepared, checker, thread="reused") is True
    assert (await mapping(db)).id == first.id
    assert await db.links.get(link.id) == captured


async def test_endpoint_or_purpose_change_never_rebases_an_existing_intent(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    mark = await origin(inbox, link)
    prepared = await reserve(db, link, mark, checker)
    for kwargs in ({"endpoint": "b" * 64}, {"purpose": "new"}):
        with pytest.raises(ValueError, match="thread-preparation-stale"):
            await reserve(db, link, mark, checker, **kwargs)
    assert await commit(db, link, prepared, checker, endpoint="b" * 64) is False
    assert (await reserve(db, link, mark, checker)).creation_id == prepared.creation_id


async def test_generic_and_fenced_absent_writers_keep_parent_before_mapping_lock_order(db, monkeypatch):
    link, inbox, checker = await setup(db, monkeypatch)
    prepared = await reserve(db, link, await origin(inbox, link), checker)
    held, release, attempted = asyncio.Event(), asyncio.Event(), asyncio.Event()
    generic = None
    backend_pid = None

    async def holding(owner, writer):
        await checker(owner, writer)
        held.set()
        await release.wait()

    def observed(connection, cursor, statement, parameters, context, executemany):
        nonlocal backend_pid
        if asyncio.current_task() is generic and (statement.startswith("INSERT INTO channel_conversations") or statement.startswith("SELECT channel_connections.id")):
            if db.dialect == "postgres":
                backend_pid = connection.connection.driver_connection.get_server_pid()
            attempted.set()

    event.listen(db.engine.sync_engine, "before_cursor_execute", observed)
    fenced = asyncio.create_task(commit(db, link, prepared, holding))
    try:
        await asyncio.wait_for(held.wait(), 5)
        generic = asyncio.create_task(set_mapping(db, link, "generic-later"))
        await asyncio.wait_for(attempted.wait(), 5)
        if db.dialect == "postgres":
            # Prove the actual generic statement is waiting for the held
            # parent lock before allowing the fenced insert to proceed.
            async with asyncio.timeout(5):
                async with db.sessions() as observer:
                    while True:
                        await observer.execute(text("SELECT pg_stat_clear_snapshot()"))
                        if await observer.scalar(text("SELECT wait_event_type FROM pg_stat_activity WHERE pid = :pid"), {"pid": backend_pid}) == "Lock":
                            break
                        await asyncio.sleep(0.01)
        release.set()
        results = await asyncio.wait_for(asyncio.gather(fenced, generic, return_exceptions=True), 10)
        assert results == [True, None]
        assert (await mapping(db)).thread_id == "generic-later"
    finally:
        release.set()
        event.remove(db.engine.sync_engine, "before_cursor_execute", observed)
        for task in (fenced, generic):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in (fenced, generic) if task is not None), return_exceptions=True)


@pytest.mark.parametrize("health", ["conflict", "policy_off", "agent_unavailable", "secret_unreadable"])
async def test_normal_mapping_commit_refuses_non_ok_locked_health(db, monkeypatch, health):
    link, inbox, checker = await setup(db, monkeypatch)
    mark = await origin(inbox, link)
    prepared = await reserve(db, link, mark, checker)
    async with db.sessions.begin() as writer:
        await writer.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(health=health))
    assert await commit(db, link, prepared, checker) is False
    assert not await rows(db, ChannelConversationRow)
    saved = next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == mark.message_id).route_basis["thread_preparation"]
    assert saved["selected_thread_id"] is None and saved["mapping_id"] is None
    # Reserve is also the early real-origin verification for the fixed repair;
    # it does not itself authorize HTTP or normal mapping publication.
    assert (await reserve(db, link, mark, checker)).creation_id == prepared.creation_id


@pytest.mark.parametrize("phase", ["initial", "replay"])
@pytest.mark.parametrize("health", ["conflict", "policy_off", "agent_unavailable", "secret_unreadable"])
async def test_private_preflight_refuses_health_changed_while_waiting_for_its_writer(db, monkeypatch, phase, health):
    link, inbox, checker = await setup(db, monkeypatch)
    mark = await origin(inbox, link)
    previous = await reserve(db, link, mark, checker) if phase == "replay" else None
    attempted = asyncio.Event()
    task = None

    def observed(connection, cursor, statement, parameters, context, executemany):
        if asyncio.current_task() is task and (statement == "BEGIN IMMEDIATE" or statement.startswith("SELECT telegram_person_settings.")):
            attempted.set()

    event.listen(db.engine.sync_engine, "before_cursor_execute", observed)
    try:
        async with db.links.capacity_writer(link.owner_user_id) as holder:
            task = asyncio.create_task(db.links.reserve_thread_preparation(link, mark, purpose="ensure", endpoint_id=ENDPOINT, check_live_owner=checker, check_attempt_current=lambda: True, require_healthy=True))
            waiting = asyncio.create_task(attempted.wait())
            try:
                done, _ = await asyncio.wait([task, waiting], timeout=5, return_when=asyncio.FIRST_COMPLETED)
                if task in done:
                    task.result()
                assert waiting in done
                await holder.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(health=health))
            finally:
                waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)
        with pytest.raises(ValueError, match="thread-preparation-stale"):
            await asyncio.wait_for(task, 10)
        saved = next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == mark.message_id).route_basis.get("thread_preparation")
        if previous is None:
            assert saved is None
        else:
            assert saved["creation_id"] == previous.creation_id and saved["selected_thread_id"] is None
        repair = await reserve(db, link, mark, checker)
        assert "require_healthy" not in repair.scope
        if previous is not None:
            assert repair.creation_id == previous.creation_id
    finally:
        event.remove(db.engine.sync_engine, "before_cursor_execute", observed)
        if task is not None:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("value", [None, 0, 1, "true"])
async def test_private_preflight_health_flag_requires_a_literal_bool(db, monkeypatch, value):
    link, inbox, checker = await setup(db, monkeypatch)
    mark = await origin(inbox, link)
    with pytest.raises(ValueError, match="thread-preparation-stale"):
        await db.links.reserve_thread_preparation(link, mark, purpose="ensure", endpoint_id=ENDPOINT, check_live_owner=checker, check_attempt_current=lambda: True, require_healthy=value)
    assert "thread_preparation" not in next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == mark.message_id).route_basis
