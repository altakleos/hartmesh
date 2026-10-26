"""Atomic restart primaries with real SQL and real opaque host tickets.

The shared issuer helper uses an actual Supervisor with a controlled startup
phase; genuine startup/SDK/restart composition belongs to the host tests.
"""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy

import pytest
from _telegram_recovery_test_support import issue_restart_epoch
from _telegram_repository_test_support import db as db  # noqa: F401
from sqlalchemy import event, select, update
from test_telegram_publication_recovery_custody import recovery_environment as recovery_environment
from test_telegram_publication_recovery_custody import recovery_rows
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging  # noqa: F401

from app.telegram_bots import producer
from app.telegram_bots.producer import Outbox, OutboxItem, uses_model_source
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def restart_attempt(e, monkeypatch, record):
    issued = issue_restart_epoch(e, monkeypatch, (record,))
    try:
        attempt = await issued.epoch.capture(record, e.link)
        yield issued, attempt
    finally:
        await issued.aclose()


async def read_receipt(e, issued, record):
    admission = await issued.epoch.prepare_receipt_read(record)
    return await e.inbox.read_restart_receipt(record, captured_link=admission.captured_link, check_live_owner=admission.check_live_owner, check_current=admission.check_current)


async def primary_rows(e):
    async with e.sessions() as session:
        return list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id, TelegramOutboxRow.dedupe_key.like("reply:%"))))


def item_of(row):
    return OutboxItem(**{name: deepcopy(getattr(row, name)) for name in OutboxItem.__dataclass_fields__})


async def test_restart_primary_atomically_closes_all_origins_without_model_authority(recovery_environment, monkeypatch):
    e = recovery_environment

    async def empty_ceilings(*args, **kwargs):
        return frozenset()

    monkeypatch.setattr("app.gateway.authz.resolve_route_permissions", empty_ceilings)
    record = await e.reserve()
    outbox = Outbox(e.sessions, inbox=e.inbox)
    async with restart_attempt(e, monkeypatch, record) as (issued, attempt):
        identity = await outbox.plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
        assert await outbox.plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard) == identity
        receipt = await read_receipt(e, issued, record)
        assert receipt.outbox_id == identity and receipt.record == record
    rows = await recovery_rows(e, record)
    assert all(row.outbox_id == identity and row.route_complete and row.run_id is None and row.assistant_message_id is None for row in rows)
    closure = rows[0].route_basis["publication_recovery"]
    assert set(closure) == {"version", "kind", "startup_id", "publication_id", "input_version", "input_sha256"}
    assert closure["version"] == 1 and closure["kind"] == "restart_before_dispatch"
    assert closure["publication_id"] == record.publication_id and closure["input_sha256"] == record.input_sha256
    assert all(row.route_basis["publication_recovery"] == closure for row in rows)
    primaries = await primary_rows(e)
    assert len(primaries) == 1
    item = item_of(primaries[0])
    basis = item.payload["reply_basis"]
    assert type(basis["version"]) is int and basis["version"] == 3
    assert basis["outcome"]["closure"] == closure and basis["outcome"]["source_ref"] == {"key": "chat.restarted", "values": {}}
    assert basis["presentation"] is None and basis["finish_route"] is True
    assert item.payload["source"] == basis["outcome"] and "delivery_kind" not in item.payload
    assert all(part["kind"] == "text" and part["source_ref"] == {"key": "chat.restarted", "values": {}} and part["reply_to_message_id"] == 701 for part in item.parts)
    assert uses_model_source(item) is False
    parsed = producer.validate_restart_reply_basis(item)
    assert parsed.record == record
    assert (await e.live.authority.delivery(e.link, item, item.parts[0])).decision == "allow"


async def test_marker_winner_cannot_be_reclassified_by_stale_unmarked_snapshot(recovery_environment, monkeypatch):
    e = recovery_environment
    original = await e.reserve()
    assert await e.inbox.mark_dispatch_started(original, guard=e.guard_factory()) is True
    async with restart_attempt(e, monkeypatch, original) as (issued, attempt):
        with pytest.raises(ValueError):
            await Outbox(e.sessions).plan_restart_before_dispatch(original, ticket=attempt.ticket, guard=attempt.fixed_guard)
    assert await primary_rows(e) == []
    assert all("publication_recovery" not in row.route_basis for row in await recovery_rows(e, original))


@pytest.mark.parametrize("writer", ["reserve", "marker", "bind", "legacy_bind_turn", "legacy_bind", "legacy_publish"])
async def test_closed_publication_cannot_be_reopened_by_any_old_writer(recovery_environment, monkeypatch, writer):
    e = recovery_environment
    record = await e.reserve()
    async with restart_attempt(e, monkeypatch, record) as (issued, attempt):
        identity = await Outbox(e.sessions).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
    saved = await recovery_rows(e, record)
    with pytest.raises(ValueError):
        if writer == "reserve":
            await e.reserve()
        elif writer == "marker":
            await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())
        elif writer == "bind":
            await e.inbox.bind_publication_run(record, run_id="late-run", guard=e.guard_factory())
        elif writer == "legacy_bind_turn":
            await e.inbox.bind_turn(saved, turn_id=record.turn_id, thread_id=record.scope.thread_id)
        elif writer == "legacy_bind":
            await e.inbox.bind_run(turn_id=record.turn_id, publication_id=record.publication_id, run_id="late-run")
        else:
            await e.inbox.publish(saved, event_id=record.event_id + ":r1", expected_publication_id=record.publication_id)
    assert all(row.outbox_id == identity and row.run_id is None for row in await recovery_rows(e, record))


async def test_retired_ticket_cannot_authorize_an_owned_writer(recovery_environment, monkeypatch):
    e = recovery_environment
    record = await e.reserve()
    async with restart_attempt(e, monkeypatch, record) as (issued, attempt):
        issued.epoch.retire()
        with pytest.raises(ValueError):
            await Outbox(e.sessions).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
    assert await primary_rows(e) == []
    assert all(not row.route_complete for row in await recovery_rows(e, record))


async def test_final_borrowed_owner_fence_rolls_back_primary_and_closure(recovery_environment, monkeypatch):
    e = recovery_environment
    record = await e.reserve()
    original = e.live.authority.prepare_live_owner
    calls = []
    issued = issue_restart_epoch(e, monkeypatch, (record,))

    async def preparing(owner):
        actual = await original(owner)

        class Revoke:
            email = actual.email

            async def __call__(self, user, writer):
                await actual(user, writer)
                calls.append(user)
                if len(calls) == 2:
                    issued.epoch.retire()

        return Revoke()

    monkeypatch.setattr(e.live.authority, "prepare_live_owner", preparing)
    try:
        attempt = await issued.epoch.capture(record, e.link)
        with pytest.raises(ValueError):
            await Outbox(e.sessions).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
        assert len(calls) == 2
    finally:
        await issued.aclose()
    assert await primary_rows(e) == []
    assert all(not row.route_complete and "publication_recovery" not in row.route_basis for row in await recovery_rows(e, record))


async def test_failed_seal_rolls_back_outbox_and_every_origin(recovery_environment, monkeypatch):
    e = recovery_environment
    record = await e.reserve()
    actual = e.inbox.seal_restart_before_dispatch

    async def failed(*args, **kwargs):
        await actual(*args, **kwargs)
        raise RuntimeError("controlled seal failure")

    monkeypatch.setattr(e.inbox, "seal_restart_before_dispatch", failed)
    async with restart_attempt(e, monkeypatch, record) as (issued, attempt):
        with pytest.raises(RuntimeError, match="controlled seal failure"):
            await Outbox(e.sessions, inbox=e.inbox).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
    assert await primary_rows(e) == []
    assert all(not row.route_complete for row in await recovery_rows(e, record))


async def test_committed_lost_response_confirms_exact_readonly_pair(recovery_environment, monkeypatch):
    e = recovery_environment
    record = await e.reserve()
    actual = e.inbox.seal_restart_before_dispatch

    async def marked(*args, **kwargs):
        result = await actual(*args, **kwargs)
        kwargs["session"].sync_session.info["A_restart_committed_response"] = True
        return result

    def lost(session):
        if session.info.pop("A_restart_committed_response", False):
            raise OSError("controlled committed response loss")

    monkeypatch.setattr(e.inbox, "seal_restart_before_dispatch", marked)
    event.listen(e.sessions.class_.sync_session_class, "after_commit", lost)
    async with restart_attempt(e, monkeypatch, record) as (issued, attempt):
        try:
            with pytest.raises(OSError, match="controlled committed response loss"):
                await Outbox(e.sessions, inbox=e.inbox).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
        finally:
            event.remove(e.sessions.class_.sync_session_class, "after_commit", lost)
        receipt = await read_receipt(e, issued, record)
        assert receipt is not None
        assert len(await primary_rows(e)) == 1
        assert all(row.outbox_id == receipt.outbox_id and row.route_complete for row in await recovery_rows(e, record))


async def test_repeated_cancellation_retains_flushed_writer_until_commit_exit(recovery_environment, monkeypatch):
    e = recovery_environment
    record = await e.reserve()
    entered, release = asyncio.Event(), asyncio.Event()
    actual = e.inbox.seal_restart_before_dispatch

    async def held(*args, **kwargs):
        result = await actual(*args, **kwargs)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(e.inbox, "seal_restart_before_dispatch", held)
    async with restart_attempt(e, monkeypatch, record) as (issued, attempt):
        task = asyncio.create_task(Outbox(e.sessions, inbox=e.inbox).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            task.cancel()
            assert not (await asyncio.wait({task}, timeout=0.03))[0]
            task.cancel()
            assert not (await asyncio.wait({task}, timeout=0.03))[0]
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
        assert task.cancelled()
        assert (await read_receipt(e, issued, record)).outbox_id == (await primary_rows(e))[0].id


@pytest.mark.parametrize("change", ["model_echo", "bool_version", "media", "missing_basis"])
async def test_fixed_basis_cannot_grant_model_or_part_shape_bypasses(recovery_environment, monkeypatch, change):
    e = recovery_environment
    record = await e.reserve()
    async with restart_attempt(e, monkeypatch, record) as (issued, attempt):
        await Outbox(e.sessions).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
    item = item_of((await primary_rows(e))[0])
    if change == "model_echo":
        item.payload["source"] = {"kind": "model_text"}
    elif change == "bool_version":
        item.payload["reply_basis"]["version"] = True
    elif change == "media":
        item.parts[0]["kind"] = "document"
    else:
        item.payload.pop("reply_basis")
        item.payload["source"] = {"kind": "model_text"}
    assert uses_model_source(item) is True
    with pytest.raises(ValueError):
        producer.validate_restart_reply_basis(item)


async def test_half_or_changed_closure_primary_never_confirms_readback(recovery_environment, monkeypatch):
    e = recovery_environment
    record = await e.reserve()
    async with restart_attempt(e, monkeypatch, record) as (issued, attempt):
        await Outbox(e.sessions).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
        async with e.sessions.begin() as session:
            row = await session.get(TelegramInboundMarkRow, (e.link.id, 702, e.link.chat_id))
            changed = deepcopy(row.route_basis)
            changed["publication_recovery"]["input_sha256"] = "0" * 64
            await session.execute(update(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id == 702).values(route_basis=changed))
        with pytest.raises(ValueError):
            await read_receipt(e, issued, record)


async def test_healthy_receipt_miss_requires_current_metadata_admission(recovery_environment, monkeypatch):
    e = recovery_environment
    record = await e.reserve()
    async with restart_attempt(e, monkeypatch, record) as (issued, _):
        assert await read_receipt(e, issued, record) is None


@pytest.mark.parametrize("change", ["disabled_owner", "token", "account", "retired"])
async def test_fresh_receipt_read_refuses_changed_custody(recovery_environment, monkeypatch, change):
    from test_telegram_publication_recovery_custody import NOW

    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramBotLinkRow
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = recovery_environment
    record = await e.reserve()
    async with restart_attempt(e, monkeypatch, record) as (issued, attempt):
        await Outbox(e.sessions).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
        admission = await issued.epoch.prepare_receipt_read(record)
        if change == "retired":
            issued.epoch.retire()
        else:
            async with e.sessions.begin() as session:
                if change == "disabled_owner":
                    session.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.owner_id, disabled_at=NOW))
                elif change == "token":
                    await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(token_revision=e.link.token_revision + 1))
                else:
                    await session.execute(update(TelegramAccountRow).where(TelegramAccountRow.telegram_user_id == e.link.telegram_user_id).values(owner_user_id="foreign-owner"))
        with pytest.raises(Exception):
            await e.inbox.read_restart_receipt(record, captured_link=admission.captured_link, check_live_owner=admission.check_live_owner, check_current=admission.check_current)


@pytest.mark.parametrize("withdraw", ["disabled_owner", "telegram_account", "token", "connection"])
async def test_receipt_codec_cannot_outlive_its_final_live_account_check(recovery_environment, monkeypatch, withdraw):
    import threading

    from test_telegram_publication_recovery_custody import NOW

    from app.telegram_bots.authority import AccessOff, AuthorityUnavailable
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = recovery_environment
    record = await e.reserve()
    async with restart_attempt(e, monkeypatch, record) as (issued, attempt):
        await Outbox(e.sessions).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
        admission = await issued.epoch.prepare_receipt_read(record)
        entered, release = asyncio.Event(), threading.Event()
        loop = asyncio.get_running_loop()
        actual = producer.validate_restart_reply_basis

        def held(item, part=None):
            result = actual(item, part)
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(10):
                raise RuntimeError("controlled codec barrier expired")
            return result

        monkeypatch.setattr(producer, "validate_restart_reply_basis", held)
        task = asyncio.create_task(e.inbox.read_restart_receipt(record, captured_link=admission.captured_link, check_live_owner=admission.check_live_owner, check_current=admission.check_current))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            async with e.sessions.begin() as writer:
                from deerflow.persistence.channel_connections.model import ChannelConnectionRow
                from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramBotLinkRow

                if withdraw == "disabled_owner":
                    writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.owner_id, disabled_at=NOW))
                elif withdraw == "telegram_account":
                    await writer.execute(update(TelegramAccountRow).where(TelegramAccountRow.telegram_user_id == e.link.telegram_user_id).values(owner_user_id="foreign-owner"))
                elif withdraw == "token":
                    await writer.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(token_revision=e.link.token_revision + 1))
                else:
                    await writer.execute(update(ChannelConnectionRow).where(ChannelConnectionRow.id == e.link.connection_id).values(status="revoked"))
            release.set()
            with pytest.raises((ValueError, AccessOff, AuthorityUnavailable)):
                await task
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)


async def test_fixed_parser_rejects_noninteger_detached_topic(recovery_environment, monkeypatch):
    from app.telegram_bots.inbox import InboxRow

    e = recovery_environment
    async with e.sessions.begin() as writer:
        await writer.execute(update(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id.in_([701, 702])).values(message_thread_id=2))
    async with e.sessions() as reader:
        origins = tuple(
            InboxRow.from_row(row)
            for row in await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id.in_([701, 702])).order_by(TelegramInboundMarkRow.message_id))
        )
    record = await e.reserve(origins)
    async with restart_attempt(e, monkeypatch, record) as (_, attempt):
        await Outbox(e.sessions).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
    item = item_of((await primary_rows(e))[0])
    object.__setattr__(item, "message_thread_id", 2.0)
    assert uses_model_source(item) is True
    with pytest.raises(ValueError):
        producer.validate_restart_reply_basis(item)


@pytest.mark.parametrize("withdraw_token", [False, True])
async def test_native_blocked_restart_writer_retains_cancellation_and_current_fences(recovery_environment, monkeypatch, withdraw_token):
    from sqlalchemy import text
    from test_telegram_publication_dispatch import install_actual_dispatch_probe, require_real_lock_wait

    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = recovery_environment
    record = await e.reserve()
    async with restart_attempt(e, monkeypatch, record) as (issued, attempt):
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
                    task = asyncio.create_task(Outbox(e.sessions).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard))
                finally:
                    active.reset(token)
                await asyncio.wait_for(dispatched.wait(), 5)
                await require_real_lock_wait(e, backend_pid)
                task.cancel()
                assert not (await asyncio.wait({task}, timeout=0.03))[0]
                task.cancel()
                assert not (await asyncio.wait({task}, timeout=0.03))[0]
                if withdraw_token:
                    await holder.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(token_revision=e.link.token_revision + 1))
                await holder.commit()
            finally:
                if holder.in_transaction():
                    await holder.rollback()
                if task is not None:
                    await asyncio.gather(task, return_exceptions=True)
                remove_probe()
            assert task.cancelled()
        if withdraw_token:
            assert await primary_rows(e) == []
            assert all(not row.route_complete for row in await recovery_rows(e, record))
        else:
            assert (await read_receipt(e, issued, record)).outbox_id == (await primary_rows(e))[0].id
        async with e.sessions.begin() as writer:
            await writer.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(profile_version=37))
        async with e.sessions() as reader:
            assert await reader.scalar(select(TelegramBotLinkRow.profile_version).where(TelegramBotLinkRow.id == e.link.id)) == 37
        assert e.db.engine.sync_engine.pool.checkedout() == 0


@pytest.mark.parametrize("db", ["sqlite"], indirect=True)
@pytest.mark.parametrize("journal_mode", ["delete", "wal"])
@pytest.mark.parametrize("entry", ["page", "receipt"])
async def test_native_recovery_read_drains_actual_cursor_before_cancel_returns(recovery_environment, monkeypatch, journal_mode, entry):
    from contextvars import ContextVar

    import aiosqlite
    from sqlalchemy import text

    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = recovery_environment
    record = await e.reserve()
    async with e.db.engine.connect() as connection:
        assert (await connection.scalar(text("PRAGMA journal_mode=" + journal_mode))).lower() == journal_mode
    async with restart_attempt(e, monkeypatch, record) as (issued, attempt):
        await Outbox(e.sessions).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
        admission = await issued.epoch.prepare_receipt_read(record)
        active = ContextVar("A_recovery_native_read", default=False)
        entered, release, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        cursors = []
        original = aiosqlite.Connection._execute

        async def observed(connection, function, *args, **kwargs):
            result = await original(connection, function, *args, **kwargs)
            if cursors and getattr(function, "__name__", "") == "close" and getattr(function, "__self__", None) is cursors[0]:
                closed.set()
            statement = str(args[0]).upper() if args else ""
            if not cursors and active.get() and getattr(function, "__name__", "") == "execute" and "SELECT" in statement and "FROM TELEGRAM_INBOUND_MARKS" in statement:
                cursors.append(function.__self__)
                entered.set()
                await release.wait()
            return result

        async def read():
            token = active.set(True)
            try:
                if entry == "page":
                    return await e.inbox.recovery_page(after=None, limit=32, max_origins=64)
                return await e.inbox.read_restart_receipt(record, captured_link=admission.captured_link, check_live_owner=admission.check_live_owner, check_current=admission.check_current)
            finally:
                active.reset(token)

        monkeypatch.setattr(aiosqlite.Connection, "_execute", observed)
        task = asyncio.create_task(read())
        try:
            await asyncio.wait_for(entered.wait(), 5)
            task.cancel()
            assert not (await asyncio.wait({task}, timeout=0.03))[0]
            task.cancel()
            assert not (await asyncio.wait({task}, timeout=0.03))[0]
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
        assert task.cancelled() and closed.is_set()
        async with e.sessions.begin() as writer:
            await writer.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(profile_version=37))
        async with e.sessions() as reader:
            assert await reader.scalar(select(TelegramBotLinkRow.profile_version).where(TelegramBotLinkRow.id == e.link.id)) == 37
        assert e.db.engine.sync_engine.pool.checkedout() == 0


async def test_half_primary_declared_by_basis_does_not_become_healthy_miss(recovery_environment, monkeypatch):
    e = recovery_environment
    record = await e.reserve()
    async with restart_attempt(e, monkeypatch, record) as (issued, attempt):
        await Outbox(e.sessions).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
        async with e.sessions.begin() as writer:
            rows = await writer.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == record.turn_id))
            for row in rows:
                basis = deepcopy(row.route_basis)
                basis.pop("publication_recovery")
                row.route_basis = basis
            primary = (await writer.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id, TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id))).one()
            primary.payload = {**primary.payload, "source": {}}
        with pytest.raises(ValueError):
            await read_receipt(e, issued, record)


async def test_original_navigation_and_fixed_source_survive_config_change(recovery_environment, monkeypatch):
    from app.telegram_bots import pages

    e = recovery_environment
    monkeypatch.setattr(pages, "_base_url", lambda: "https://original.invalid/root")
    record = await e.reserve()
    async with restart_attempt(e, monkeypatch, record) as (_, attempt):
        await Outbox(e.sessions).plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
    item = item_of((await primary_rows(e))[0])
    expected = "https://original.invalid/root/workspace/agents/" + e.link.agent_ref + "/chats/" + record.scope.thread_id
    assert item.parts[0]["url_button"] == {"page": "web.conversation", "url": expected}
    assert producer.url_button_label(item.parts[0]["source_ref"], item.parts[0]["url_button"]) == "label.open_conversation"
    monkeypatch.setattr(pages, "_base_url", lambda: "https://changed.invalid")
    assert producer.validate_restart_reply_basis(item).record == record
    assert item.parts[0]["url_button"]["url"] == expected
    corrupt = item_of((await primary_rows(e))[0])
    corrupt.parts[0]["url_button"]["page"] = "web.memory"
    assert uses_model_source(corrupt) is True
    with pytest.raises(ValueError):
        producer.validate_restart_reply_basis(corrupt)


@pytest.mark.parametrize("writer", ["reserve", "marker", "bind", "legacy_bind_turn", "legacy_bind", "legacy_publish"])
async def test_corrupt_recovery_state_refuses_old_writers_without_repair(recovery_environment, monkeypatch, writer):
    e = recovery_environment
    record = await e.reserve()
    async with e.sessions.begin() as session:
        for row in await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == record.turn_id)):
            row.route_basis = {**row.route_basis, "publication_recovery": None}
    saved = await recovery_rows(e, record)
    with pytest.raises(ValueError):
        if writer == "reserve":
            await e.reserve()
        elif writer == "marker":
            await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())
        elif writer == "bind":
            await e.inbox.bind_publication_run(record, run_id="late-run", guard=e.guard_factory())
        elif writer == "legacy_bind_turn":
            await e.inbox.bind_turn(saved, turn_id=record.turn_id, thread_id=record.scope.thread_id)
        elif writer == "legacy_bind":
            await e.inbox.bind_run(turn_id=record.turn_id, publication_id=record.publication_id, run_id="late-run")
        else:
            await e.inbox.publish(saved, event_id=record.event_id + ":r1", expected_publication_id=record.publication_id)
    assert all(row.route_basis["publication_recovery"] is None and not row.route_complete and row.run_id is None for row in await recovery_rows(e, record))
