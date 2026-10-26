"""Recovery rows over real SQL; target availability is explicitly controlled.

Missing recovery methods are feature scaffolds, not old product defects. The
shared account, connection and publication writers are actual production code.
"""

import dataclasses
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
import pytest_asyncio
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db  # noqa: F401
from sqlalchemy import select, update
from telegram import Chat, Message, Update, User
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging  # noqa: F401

from app.telegram_bots import inbox as inbox_module
from app.telegram_bots.inbox import InboxRepository, InboxRow
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

pytestmark = pytest.mark.asyncio
NOW = datetime(2026, 10, 26, 19, tzinfo=UTC)
PROFILE = {"version": 1, "kind": "plain_text", "forwarded": False}


@pytest_asyncio.fixture
async def recovery_environment(db, monkeypatch):
    live = await install_live_owner(db, monkeypatch)
    link = await bind_link(db, await add_link(db, owner=live.id, target="recovery_assistant"))
    monkeypatch.setattr("deerflow.config.agents_config.load_agent_config", lambda *args, **kwargs: SimpleNamespace(name=link.agent_ref))
    inbox = InboxRepository(db.sessions)
    monkeypatch.setattr(inbox_module, "_now", lambda: NOW)
    prepared = await live.authority.prepare_live_owner(live.id)
    e = SimpleNamespace(db=db, sessions=db.sessions, links=db.links, inbox=inbox, live=live, owner_id=live.id, link=link, thread_id="recovery-original-thread", endpoint_id="e" * 64)

    def guard_factory():
        return db.links.publication_guard(e.link, check_live_owner=prepared, check_attempt_current=lambda: True)

    async def add_origin(message_id, *, profile=PROFILE, text=None):
        text = text if text is not None else f"Synthetic saved input {message_id}"
        incoming = Update(message_id + 1000, message=Message(message_id, NOW, Chat(link.chat_id, "private"), from_user=User(link.telegram_user_id, "Synthetic human", False), text=text))
        mark, _ = await inbox.accept(link, incoming, sanitized_input={"type": "text", "text": text})
        basis = {"kind": link.agent_kind, "target": link.agent_ref, "revision": 1}
        if profile is not None:
            basis["input_profile"] = deepcopy(profile)
        return await inbox.route(mark, kind="turn", target_id=link.agent_ref, basis=basis, operation_ids=[])

    async def reserve(origins=None):
        return await inbox.reserve_publication(e.link, origins if origins is not None else e.origins, thread_id=e.thread_id, endpoint_id=e.endpoint_id, guard=guard_factory())

    e.guard_factory, e.add_origin, e.reserve = guard_factory, add_origin, reserve
    e.origins = (await add_origin(701), await add_origin(702))
    yield e


async def recovery_rows(e, record):
    async with e.sessions() as session:
        rows = await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == record.turn_id).order_by(TelegramInboundMarkRow.link_id, TelegramInboundMarkRow.message_id, TelegramInboundMarkRow.chat_id))
        return tuple(InboxRow.from_row(row) for row in rows)


async def test_reserved_recovery_bookkeeping_cannot_enter_public_route(recovery_environment):
    e = recovery_environment
    incoming = Update(1999, message=Message(999, NOW, Chat(e.link.chat_id, "private"), from_user=User(e.link.telegram_user_id, "Synthetic human", False), text="new input"))
    mark, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": "new input"})
    with pytest.raises(ValueError, match="invalid-inbox-route"):
        await e.inbox.route(mark, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": PROFILE, "publication_recovery": {"version": 1}}, operation_ids=[])
    async with e.sessions() as session:
        saved = await session.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))
        assert saved.route_kind is None and saved.route_basis == {}


async def test_ordinary_managed_marker_and_binding_remain_compatible(recovery_environment):
    e = recovery_environment
    record = await e.reserve()
    assert await e.inbox.mark_dispatch_started(record, guard=e.guard_factory()) is True
    assert await e.inbox.mark_dispatch_started(record, guard=e.guard_factory()) is False
    bound = await e.inbox.bind_publication_run(record, run_id="ordinary-control-run", guard=e.guard_factory())
    assert bound.dispatch_started is True and bound.run_id == "ordinary-control-run"
    assert all("publication_recovery" not in row.route_basis for row in await recovery_rows(e, record))


async def test_recovery_page_keysets_whole_groups_without_skipping_lookahead(recovery_environment):
    e = recovery_environment
    records = [await e.reserve()]
    for message in (711, 721):
        origins = (await e.add_origin(message), await e.add_origin(message + 1))
        records.append(await e.reserve(origins))
    expected = sorted(record.turn_id for record in records)
    first = await e.inbox.recovery_page(after=None, limit=2, max_origins=64)
    assert [entry.record.turn_id for entry in first.entries] == expected[:2]
    assert first.has_more is True
    assert (first.next_after.link_id, first.next_after.turn_id) == (e.link.id, expected[1])
    assert all(entry.hold_reason is None and len(entry.origins) == 2 for entry in first.entries)
    second = await e.inbox.recovery_page(after=first.next_after, limit=2, max_origins=64)
    assert [entry.record.turn_id for entry in second.entries] == expected[2:]
    assert second.has_more is False and second.next_after is None
    assert {origin.message_id for entry in first.entries + second.entries for origin in entry.origins} == {701, 702, 711, 712, 721, 722}
    with pytest.raises(dataclasses.FrozenInstanceError):
        first.has_more = False


@pytest.mark.parametrize("change", ["done", "interrupted", "primary", "route_complete"])
async def test_head_filter_cannot_hide_ineligible_complete_group_member(recovery_environment, change):
    e = recovery_environment
    record = await e.reserve()
    facts = {"state": "done", "route_complete": True, "closed_at": NOW} if change == "done" else {"state": "interrupted"} if change == "interrupted" else {"outbox_id": "f" * 32} if change == "primary" else {"route_complete": True}
    async with e.sessions.begin() as session:
        await session.execute(update(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id == 702).values(**facts))
    page = await e.inbox.recovery_page(after=None, limit=32, max_origins=64)
    entry = next(entry for entry in page.entries if entry.key.turn_id == record.turn_id)
    assert entry.hold_reason in {"invalid_group", "incomplete_group"}
    assert len(await recovery_rows(e, record)) == 2


async def test_member_sentinel_holds_oversized_group_without_parsing_prefix(recovery_environment):
    e = recovery_environment
    origins = (*e.origins, await e.add_origin(703))
    record = await e.reserve(origins)
    page = await e.inbox.recovery_page(after=None, limit=32, max_origins=2)
    entry = next(entry for entry in page.entries if entry.key.turn_id == record.turn_id)
    assert entry.hold_reason == "group_too_large" and entry.record is None
    assert len(entry.origins) <= 3
    assert all(row.outbox_id is None and row.route_complete is False for row in await recovery_rows(e, record))


async def test_global_turn_collision_is_not_filtered_to_the_original_link(recovery_environment):
    e = recovery_environment
    record = await e.reserve()
    other = await add_link(e.db, owner=e.owner_id, bot=9_000_004_001, target="other_recovery_assistant")
    async with e.sessions.begin() as session:
        first = await session.get(TelegramInboundMarkRow, (e.link.id, 701, e.link.chat_id))
        values = {column.name: deepcopy(getattr(first, column.name)) for column in TelegramInboundMarkRow.__table__.columns}
        values.update(link_id=other.id, message_id=703, update_id=1703)
        session.add(TelegramInboundMarkRow(**values))
    page = await e.inbox.recovery_page(after=None, limit=32, max_origins=64)
    entry = next(entry for entry in page.entries if entry.key.link_id == e.link.id and entry.key.turn_id == record.turn_id)
    assert entry.hold_reason in {"invalid_group", "incomplete_group"}
    assert len(await recovery_rows(e, record)) == 3


@pytest.mark.parametrize("history", [None, {}, [], [None]])
async def test_malformed_group_history_is_held_without_stalling_another_group(recovery_environment, history):
    e = recovery_environment
    bad = await e.reserve()
    good = await e.reserve((await e.add_origin(711), await e.add_origin(712)))
    async with e.sessions.begin() as session:
        for row in await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == bad.turn_id)):
            changed = deepcopy(row.route_basis)
            changed["publications"] = history
            row.route_basis = changed
    page = await e.inbox.recovery_page(after=None, limit=32, max_origins=64)
    entries = {entry.key.turn_id: entry for entry in page.entries}
    assert entries[bad.turn_id].hold_reason == "invalid_group" and entries[bad.turn_id].record is None
    assert entries[good.turn_id].hold_reason is None and entries[good.turn_id].record == good


@pytest.mark.parametrize("stored_turn", ["", "\x01"], ids=["empty", "control"])
async def test_corrupt_turn_bookmark_advances_to_a_later_good_group(recovery_environment, stored_turn):
    e = recovery_environment
    bad = await e.reserve()
    good = await e.reserve((await e.add_origin(711), await e.add_origin(712)))
    async with e.sessions.begin() as session:
        await session.execute(update(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == bad.turn_id).values(turn_id=stored_turn))
    first = await e.inbox.recovery_page(after=None, limit=1, max_origins=64)
    assert first.entries[0].key.turn_id == stored_turn
    assert first.entries[0].hold_reason == "invalid_group" and first.entries[0].record is None
    assert first.has_more is True and first.next_after.turn_id == stored_turn
    second = await e.inbox.recovery_page(after=first.next_after, limit=1, max_origins=64)
    assert second.entries[0].record == good and second.entries[0].hold_reason is None
    assert second.has_more is False and second.next_after is None


@pytest.mark.parametrize("profile", [None, {"version": True, "kind": "plain_text", "forwarded": False}, {"version": 1, "kind": "plain_text", "forwarded": True}])
async def test_unsupported_saved_input_profile_is_held(recovery_environment, profile):
    e = recovery_environment
    origins = (await e.add_origin(711, profile=profile), await e.add_origin(712, profile=profile))
    record = await e.reserve(origins)
    page = await e.inbox.recovery_page(after=None, limit=32, max_origins=64)
    assert next(entry for entry in page.entries if entry.key.turn_id == record.turn_id).hold_reason == "unsupported_input"


@pytest.mark.parametrize("arguments", [{"limit": 0}, {"limit": True}, {"limit": 33}, {"max_origins": 0}, {"max_origins": True}, {"max_origins": 65}])
async def test_recovery_limits_reject_invalid_arguments(recovery_environment, arguments):
    e = recovery_environment
    with pytest.raises(ValueError):
        await e.inbox.recovery_page(after=None, **arguments)


async def test_sqlite_overlong_stored_bookmark_is_held_and_advances(recovery_environment):
    e = recovery_environment
    bad = await e.reserve()
    good = await e.reserve((await e.add_origin(711), await e.add_origin(712)))
    stored_turn = "!" * 33
    if e.db.engine.dialect.name == "postgresql":
        from sqlalchemy.exc import DBAPIError

        with pytest.raises(DBAPIError):
            async with e.sessions.begin() as session:
                await session.execute(update(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == bad.turn_id).values(turn_id=stored_turn))
        return
    async with e.sessions.begin() as session:
        await session.execute(update(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == bad.turn_id).values(turn_id=stored_turn))
    first = await e.inbox.recovery_page(after=None, limit=1, max_origins=64)
    assert first.entries[0].key.turn_id == stored_turn
    assert first.entries[0].hold_reason == "invalid_group" and first.entries[0].record is None
    assert first.has_more and first.next_after.turn_id == stored_turn
    second = await e.inbox.recovery_page(after=first.next_after, limit=1, max_origins=64)
    assert second.entries[0].record == good and second.has_more is False
