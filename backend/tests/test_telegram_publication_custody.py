"""Managed publication custody over actual SQL and prepared live-human authority.

The new API-availability failures are feature scaffolds, not defects in the
compatible legacy bind_turn/publish/bind_run contract.
"""

import asyncio
import dataclasses
import hashlib
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from sqlalchemy import select, update
from telegram import Chat, Message, Update, User

from app.telegram_bots import inbox as inbox_module
from app.telegram_bots.authority import AccessOff, AuthorityUnavailable
from app.telegram_bots.inbox import InboxRepository, InboxRow
from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramBotLinkRow, TelegramInboundMarkRow
from deerflow.persistence.user.access import DisabledIdentityRow

pytestmark = pytest.mark.asyncio
NOW = datetime(2026, 10, 25, 14, tzinfo=UTC)


@pytest_asyncio.fixture
async def publication_environment(db, monkeypatch):
    """Actual same-engine SQL custody and prepared public live-human checker."""
    live = await install_live_owner(db, monkeypatch)
    link = await bind_link(db, await add_link(db, owner=live.id, target="publication_assistant"))
    inbox = InboxRepository(db.sessions)
    monkeypatch.setattr(inbox_module, "_now", lambda: NOW)
    current = [True]
    prepared = await live.authority.prepare_live_owner(live.id)
    e = SimpleNamespace(
        db=db,
        sessions=db.sessions,
        links=db.links,
        inbox=inbox,
        live=live,
        owner_id=live.id,
        link=link,
        thread_id="publication-original-thread",
        endpoint_id="e" * 64,
        attempt_current=current,
        live_owner_check=prepared,
    )

    async def add_origin(*, message_id=701, update_id=1701, text="First accepted input", topic=None, basis=None, operations=None):
        incoming = Update(
            update_id,
            message=Message(
                message_id=message_id,
                date=NOW,
                chat=Chat(link.chat_id, "private"),
                from_user=User(link.telegram_user_id, "Synthetic human", False),
                text=text,
                message_thread_id=topic,
            ),
        )
        mark, _ = await inbox.accept(link, incoming, sanitized_input={"text": text})
        return await inbox.route(
            mark,
            kind="turn",
            target_id=link.agent_ref,
            basis=basis if basis is not None else {"kind": link.agent_kind, "target": link.agent_ref, "revision": 1},
            operation_ids=operations if operations is not None else [],
        )

    def guard_factory(*, captured_link=None, check_live_owner=None, check_attempt_current=None):
        return db.links.publication_guard(
            captured_link or e.link,
            check_live_owner=check_live_owner or prepared,
            check_attempt_current=check_attempt_current or (lambda: current[0]),
        )

    async def reserve(origins=None, *, guard=None, thread_id=None, endpoint_id=None):
        return await inbox.reserve_publication(
            e.link,
            origins if origins is not None else e.origins,
            thread_id=thread_id or e.thread_id,
            endpoint_id=endpoint_id or e.endpoint_id,
            guard=guard or guard_factory(),
        )

    e.add_origin, e.guard_factory, e.reserve = add_origin, guard_factory, reserve
    first = await add_origin()
    second = await add_origin(message_id=702, update_id=1702, text="Second accepted input")
    e.origins = (first, second)
    yield e


async def saved_origins(e):
    async with e.sessions() as session:
        values = await session.scalars(
            select(TelegramInboundMarkRow)
            .where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id.in_([row.message_id for row in e.origins]))
            .order_by(TelegramInboundMarkRow.message_id, TelegramInboundMarkRow.chat_id)
        )
        return tuple(InboxRow.from_row(value) for value in values)


def expected_input_digest(origins):
    """Independent, explicitly specified v1 persisted-input recipe."""
    assembly = []
    for row in sorted(origins, key=lambda item: (item.accepted_at, item.update_id, item.message_id)):
        assembly.append(
            {
                "origin": {name: getattr(row, name) for name in ("link_id", "message_id", "chat_id", "update_id", "message_thread_id", "binding_generation")},
                "accepted_at": row.accepted_at.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"),
                "sanitized_input": row.sanitized_input,
                "route_kind": row.route_kind,
                "route_target_id": row.route_target_id,
                "operation_ids": row.operation_ids,
                "route_basis": {key: value for key, value in row.route_basis.items() if key not in {"publications", "current_publication_id", "thread_preparation"}},
            }
        )
    wire = json.dumps({"version": 1, "assembly": assembly}, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(wire).hexdigest()


async def test_legacy_group_and_explicit_publication_compatibility_control(publication_environment):
    e = publication_environment
    await e.inbox.bind_turn(e.origins, turn_id="a" * 32, thread_id=e.thread_id)
    stored = await saved_origins(e)
    first = stored[0].route_basis["current_publication_id"]
    await e.inbox.bind_run(turn_id="a" * 32, publication_id=first, run_id="legacy-first-run")
    second = await e.inbox.publish(stored, event_id="m701:r1", expected_publication_id=first)
    await e.inbox.bind_run(turn_id="a" * 32, publication_id=second, run_id="legacy-second-run")
    assert (await saved_origins(e))[0].run_id == "legacy-second-run"
    with pytest.raises(ValueError):
        await e.inbox.bind_run(turn_id="a" * 32, publication_id=first, run_id="late-old-run")


async def test_existing_route_preserves_json_boolean_distinction_control(publication_environment):
    e = publication_environment
    row = e.origins[0]
    with pytest.raises(ValueError, match="inbox-route-conflict"):
        await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"kind": e.link.agent_kind, "target": e.link.agent_ref, "revision": True}, operation_ids=[])
    assert (await saved_origins(e))[0].route_basis["revision"] == 1


async def test_reserve_freezes_complete_scope_input_and_sorted_origins(publication_environment):
    e = publication_environment
    record = await e.reserve(tuple(reversed(e.origins)))
    assert isinstance(record, inbox_module.PublicationRecord)
    assert record.scope.link_id == e.link.id and record.scope.owner_user_id == e.owner_id
    assert (record.scope.bot_id, record.scope.agent_kind, record.scope.agent_ref, record.scope.assistant_id, record.scope.agent_name) == (e.link.bot_id, "custom_agent", e.link.agent_ref, "lead_agent", e.link.agent_ref)
    assert (record.scope.connection_id, record.scope.telegram_user_id, record.scope.chat_id, record.scope.message_thread_id, record.scope.binding_generation) == (
        e.link.connection_id,
        e.link.telegram_user_id,
        e.link.chat_id,
        None,
        e.link.binding_generation,
    )
    assert (record.scope.thread_id, record.scope.endpoint_id) == (e.thread_id, e.endpoint_id)
    assert record.event_id == "m701" and len(record.turn_id) == 32
    assert record.publication_id == hashlib.sha256((record.turn_id + ":m701").encode()).hexdigest()[:32]
    assert record.input_version == 1 and record.input_sha256 == expected_input_digest(e.origins)
    assert (record.dispatch_started, record.run_id) == (False, None)
    assert isinstance(record.origins, tuple) and [row.message_id for row in record.origins] == [701, 702]
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.scope.thread_id = "changed"
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.origins[0].message_id = 999
    saved = await saved_origins(e)
    assert all(row.turn_id == record.turn_id and row.thread_id == e.thread_id for row in saved)
    assert all(row.route_basis["publications"] == saved[0].route_basis["publications"] for row in saved)
    assert await e.reserve() == record


async def test_new_managed_reserve_never_adopts_receiptless_legacy_publication(publication_environment):
    e = publication_environment
    await e.inbox.bind_turn(e.origins, turn_id="b" * 32, thread_id=e.thread_id)
    before = await saved_origins(e)
    assert before[0].run_id is None and before[0].state == "dispatched"
    with pytest.raises(ValueError):
        await e.reserve()
    assert await saved_origins(e) == before


async def test_concurrent_first_reservations_fix_one_identity(publication_environment):
    e = publication_environment
    records = await asyncio.gather(*(e.reserve() for _ in range(4)))
    assert all(record == records[0] for record in records)
    saved = await saved_origins(e)
    assert all(len(row.route_basis["publications"]) == 1 and row.turn_id == records[0].turn_id for row in saved)


async def test_legacy_explicit_retry_cannot_be_upgraded_to_a_managed_marker(publication_environment):
    e = publication_environment
    record = await e.reserve()
    await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())
    fresh = await saved_origins(e)
    retry = await e.inbox.publish(fresh, event_id="m701:r1", expected_publication_id=record.publication_id)
    assert retry != record.publication_id
    with pytest.raises(ValueError):
        await e.reserve()
    with pytest.raises(ValueError):
        await e.inbox.bind_publication_run(record, run_id="late-old-observer", guard=e.guard_factory())
    current = await saved_origins(e)
    assert all(row.route_basis["current_publication_id"] == retry and row.run_id is None for row in current)
    assert all("publication_execution" not in row.route_basis["publications"][-1] for row in current)


@pytest.mark.parametrize("field,value", [("thread_id", "changed-thread"), ("endpoint_id", "d" * 64)])
async def test_reserve_cannot_retarget_saved_identity(publication_environment, field, value):
    e = publication_environment
    record = await e.reserve()
    with pytest.raises(ValueError):
        await e.reserve(**{field: value})
    assert await e.reserve() == record


@pytest.mark.parametrize("change", ["input", "route-bool", "operation", "update", "topic", "generation", "accepted-at"])
async def test_locked_saved_input_or_scope_drift_refuses_marker(publication_environment, change):
    e = publication_environment
    record = await e.reserve()
    async with e.sessions.begin() as session:
        row = await session.get(TelegramInboundMarkRow, (e.link.id, e.origins[0].message_id, e.link.chat_id))
        if change == "input":
            row.sanitized_input = {"text": "changed after reservation"}
        elif change == "route-bool":
            # ORM dirty equality treats True as 1. Make the actual SQL fault
            # explicit, then observe the persisted JSON type below.
            await session.execute(
                update(TelegramInboundMarkRow)
                .where(TelegramInboundMarkRow.link_id == row.link_id, TelegramInboundMarkRow.message_id == row.message_id, TelegramInboundMarkRow.chat_id == row.chat_id)
                .values(route_basis={**row.route_basis, "revision": True})
                .execution_options(synchronize_session=False)
            )
        elif change == "operation":
            row.operation_ids = ["c" * 32]
        elif change == "update":
            row.update_id += 100
        elif change == "topic":
            row.message_thread_id = 27
        elif change == "generation":
            row.binding_generation += 1
        else:
            row.accepted_at = NOW + timedelta(seconds=1)
    if change == "route-bool":
        assert (await saved_origins(e))[0].route_basis["revision"] is True
    with pytest.raises(ValueError):
        await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())
    saved = await saved_origins(e)
    assert all(row.route_basis["publications"][0]["publication_execution"]["dispatch_started"] is False for row in saved)


@pytest.mark.parametrize("change", ["text", "route-bool", "operation"])
async def test_reserve_rejects_changed_detached_business_input(publication_environment, change):
    e = publication_environment
    first = e.origins[0]
    if change == "text":
        altered = dataclasses.replace(first, sanitized_input={"text": "caller replacement"})
    elif change == "route-bool":
        altered = dataclasses.replace(first, route_basis={**first.route_basis, "revision": True})
    else:
        altered = dataclasses.replace(first, operation_ids=["d" * 32])
    with pytest.raises(ValueError):
        await e.reserve((altered, e.origins[1]))
    assert all(row.turn_id is None for row in await saved_origins(e))


async def test_per_origin_preparation_bookkeeping_does_not_change_business_digest(publication_environment):
    e = publication_environment
    for index, origin in enumerate(e.origins):
        async with e.sessions.begin() as session:
            row = await session.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
            row.route_basis = {**row.route_basis, "thread_preparation": {"per_origin": index}}
    fresh = await saved_origins(e)
    record = await e.reserve(fresh)
    assert record.input_sha256 == expected_input_digest(e.origins)
    assert await e.reserve(e.origins) == record


@pytest.mark.parametrize("predicate", [False, 1, None])
async def test_attempt_fence_requires_literal_true(publication_environment, predicate):
    e = publication_environment
    with pytest.raises(ValueError):
        await e.reserve(guard=e.guard_factory(check_attempt_current=lambda: predicate))
    assert all(row.turn_id is None for row in await saved_origins(e))


async def test_final_attempt_fence_follows_actual_borrowed_live_owner_await(publication_environment):
    e = publication_environment

    class RevokingCheck:
        email = e.live_owner_check.email
        calls = 0

        async def __call__(self, owner, writer):
            await e.live_owner_check(owner, writer)
            self.calls += 1
            if self.calls == 2:
                e.attempt_current[0] = False

    check = RevokingCheck()
    with pytest.raises(ValueError):
        await e.reserve(guard=e.guard_factory(check_live_owner=check))
    assert check.calls == 2
    assert all(row.turn_id is None for row in await saved_origins(e))


@pytest.mark.parametrize("change", ["account-owner", "connection-owner", "connection-status", "disabled-owner", "generation", "token"])
async def test_real_durable_custody_withdrawal_refuses_mark(publication_environment, change):
    e = publication_environment
    record = await e.reserve()
    guard = e.guard_factory()
    async with e.sessions.begin() as session:
        if change == "account-owner":
            await session.execute(update(TelegramAccountRow).where(TelegramAccountRow.telegram_user_id == e.link.telegram_user_id).values(owner_user_id="foreign-human"))
        elif change.startswith("connection"):
            values = {"owner_user_id": "foreign-human"} if change.endswith("owner") else {"status": "revoked"}
            await session.execute(update(ChannelConnectionRow).where(ChannelConnectionRow.id == e.link.connection_id).values(**values))
        elif change == "disabled-owner":
            session.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.owner_id, disabled_at=NOW))
        else:
            values = {"binding_generation": e.link.binding_generation + 1} if change == "generation" else {"token_revision": e.link.token_revision + 1}
            await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(**values))
    with pytest.raises((ValueError, AccessOff, AuthorityUnavailable)):
        await e.inbox.mark_dispatch_started(record, guard=guard)
    assert all(row.route_basis["publications"][0]["publication_execution"]["dispatch_started"] is False for row in await saved_origins(e))
