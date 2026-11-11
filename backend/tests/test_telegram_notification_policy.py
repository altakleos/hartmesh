"""Notification preferences and budgets use the durable sender and real SQL."""

from datetime import timedelta

import pytest
from _telegram_repository_test_support import add_link, bind_link, rows
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_outbox_workers import Clock, allowed, transport

from app.telegram_bots.producer import Outbox, OutboxSender, PartReceipt
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def prepare(db, **kwargs):
    link = await bind_link(db, await add_link(db, **kwargs))
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id)):
            row.state = "sent"
    return link


async def notification(outbox, link, kind, key=None):
    await outbox.add(link, dedupe_key=key or f"notice:{kind}", class_=kind, payload={"parts": [{"kind": "text", "source_ref": {"key": "chat.restarted", "values": {}}}]})


@pytest.mark.asyncio
async def test_notifications_are_dormant_until_sender_is_enabled(db):
    link = await prepare(db)
    clock = Clock()
    outbox = Outbox(db.sessions, clock=clock)
    await notification(outbox, link, "question")
    seen = []

    async def send(link, row, part):
        seen.append(row.class_)
        return PartReceipt((42,))

    sender = OutboxSender(outbox, run_id="old", deliver_part=transport(db, send), eligibility=allowed)
    assert await sender.tick() == 0
    assert seen == []
    sender = OutboxSender(outbox, run_id="enabled", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        assert await sender.tick() == 1
        await sender.drain()
        assert seen == ["question"]
    finally:
        await sender.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "notify,expected",
    [
        ("all", {"reply", "question", "result", "interrupt", "progress", "service", "reminder", "info", "summary"}),
        ("questions", {"reply", "question", "progress", "service", "reminder", "summary"}),
        ("none", {"reply", "progress", "service", "summary"}),
    ],
)
async def test_preferences_filter_classes_without_blocking_essential_delivery(db, notify, expected):
    link = await prepare(db)
    await db.settings.update(link.owner_user_id, notify=notify)
    clock = Clock()
    outbox = Outbox(db.sessions, clock=clock)
    kinds = {"reply", "question", "result", "interrupt", "progress", "service", "reminder", "info", "summary"}
    for kind in sorted(kinds):
        await notification(outbox, link, kind)
    seen = []

    async def send(link, row, part):
        seen.append((row.class_, row.silent))
        return PartReceipt((100 + len(seen),))

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        for _ in range(12):
            await sender.tick()
            await sender.drain()
            clock.advance(61)
        assert {kind for kind, _ in seen} == expected
        assert all(silent for kind, silent in seen if kind in {"progress", "summary", "reminder"})
        if "question" in expected:
            assert seen[0][0] == "question"
        saved = [row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key.startswith("notice:")]
        assert {row.class_ for row in saved if row.skip_reason == "notifications-off"} == kinds - expected
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_only_pending_discretionary_rows_expire(db):
    link = await prepare(db)
    clock = Clock()
    outbox = Outbox(db.sessions, clock=clock)
    for kind in ("question", "result", "interrupt", "progress", "service", "reminder", "info", "summary"):
        await notification(outbox, link, kind)
    clock.advance(24 * 3600)
    seen = []

    async def send(link, row, part):
        seen.append(row.class_)
        return PartReceipt((100 + len(seen),))

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        for _ in range(10):
            await sender.tick()
            await sender.drain()
            clock.advance(1)
        assert set(seen) == {"question", "result", "interrupt", "progress", "service", "reminder"}
        assert {row.class_ for row in await rows(db, TelegramOutboxRow) if row.skip_reason == "expired"} == {"info", "summary"}
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_minute_budget_defers_news_but_not_questions_and_replies(db):
    link = await prepare(db)
    clock = Clock()
    outbox = Outbox(db.sessions, clock=clock)
    seen = []

    async def send(link, row, part):
        seen.append(row.dedupe_key)
        return PartReceipt((100 + len(seen),))

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        await notification(outbox, link, "info", "first")
        await sender.tick()
        await sender.drain()
        clock.advance(1)
        await notification(outbox, link, "info", "second")
        await sender.tick()
        await sender.drain()
        saved = next(row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key == "second")
        assert saved.state == "pending"
        assert saved.not_before.replace(tzinfo=clock().tzinfo) == clock() + timedelta(seconds=59)
        await notification(outbox, link, "question")
        await notification(outbox, link, "reply")
        for _ in range(2):
            await sender.tick()
            await sender.drain()
            clock.advance(1)
        assert seen == ["first", "notice:question", "notice:reply"]
        clock.advance(60)
        await sender.tick()
        await sender.drain()
        assert seen[-1] == "second"
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_daily_budget_is_shared_across_bots_and_survives_sender_restart(db):
    from app.telegram_bots.config import TelegramBotsConfig

    first = await prepare(db)
    second = await prepare(db, bot=9_000_000_002, owner=first.owner_user_id)
    clock = Clock()
    config = TelegramBotsConfig(outbox={"per_person_per_day": 1})
    outbox = Outbox(db.sessions, clock=clock, config=config)
    seen = []

    async def send(link, row, part):
        seen.append(row.dedupe_key)
        return PartReceipt((100 + len(seen),))

    sender = OutboxSender(outbox, run_id="first", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        await notification(outbox, first, "info", "first")
        await sender.tick()
        await sender.drain()
    finally:
        await sender.stop()
    sender = OutboxSender(outbox, run_id="restart", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        await notification(outbox, second, "info", "second")
        await sender.tick()
        await sender.drain()
        assert seen == ["first"]
        await notification(outbox, second, "result")
        await sender.tick()
        await sender.drain()
        assert seen == ["first", "notice:result"]
        blocked = next(row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key == "second")
        assert blocked.state == "pending" and blocked.not_before.replace(tzinfo=clock().tzinfo) > clock()
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_concurrent_bots_cannot_both_spend_last_daily_place(db):
    import asyncio

    from app.telegram_bots.config import TelegramBotsConfig

    first = await prepare(db)
    second = await prepare(db, bot=9_000_000_002, owner=first.owner_user_id)
    clock = Clock()
    outbox = Outbox(db.sessions, clock=clock, config=TelegramBotsConfig(outbox={"per_person_per_day": 1}))
    entered, release = asyncio.Event(), asyncio.Event()
    seen = []

    async def send(link, row, part):
        seen.append(row.id)
        entered.set()
        await release.wait()
        return PartReceipt((100 + len(seen),))

    await notification(outbox, first, "info")
    await notification(outbox, second, "info")
    sender = OutboxSender(outbox, run_id="parallel", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        assert await sender.tick() == 2
        await asyncio.wait_for(entered.wait(), 5)
        # Drain the second worker while the admitted transport remains owned.
        release.set()
        await sender.drain()
        assert len(seen) == 1
        saved = [row for row in await rows(db, TelegramOutboxRow) if row.class_ == "info"]
        assert sorted(row.state for row in saved) == ["pending", "sent"]
    finally:
        release.set()
        await sender.stop()


@pytest.mark.asyncio
async def test_hourly_budget_and_edits_do_not_delay_essential_messages(db):
    from app.telegram_bots.config import TelegramBotsConfig

    link = await prepare(db)
    clock = Clock()
    outbox = Outbox(db.sessions, clock=clock, config=TelegramBotsConfig(outbox={"per_link_per_hour": 1}))
    seen = []

    async def send(link, row, part):
        seen.append(row.dedupe_key)
        return PartReceipt((100 + len(seen),))

    sender = OutboxSender(outbox, run_id="hour", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        await notification(outbox, link, "info", "first")
        await sender.tick()
        await sender.drain()
        clock.advance(61)
        await notification(outbox, link, "info", "next")
        await sender.tick()
        await sender.drain()
        assert seen == ["first"]
        blocked = next(row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key == "next")
        assert blocked.not_before.replace(tzinfo=clock().tzinfo) == clock() + timedelta(seconds=3539)
        await outbox.add(link, dedupe_key="edit:status", class_="info", payload={"parts": [{"kind": "edit", "target_message_id": 101, "source_ref": {"key": "chat.restarted", "values": {}}}]})
        await sender.tick()
        await sender.drain()
        assert seen[-1] == "edit:status"
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_preference_change_stops_remaining_parts(db):
    link = await prepare(db)
    clock = Clock()
    outbox = Outbox(db.sessions, clock=clock)
    await outbox.add(link, dedupe_key="question:multipart", class_="question", payload={"parts": [{"kind": "text", "source_ref": {"key": "chat.restarted", "values": {}}} for _ in range(2)]})
    seen = []

    async def send(link, row, part):
        seen.append(part["part_id"])
        await db.settings.update(link.owner_user_id, notify="none")
        return PartReceipt((100,))

    sender = OutboxSender(outbox, run_id="preferences", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        await sender.tick()
        await sender.drain()
        current = next(row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key == "question:multipart")
        assert len(seen) == 1
        assert current.state == "skipped" and current.skip_reason == "notifications-off"
        assert current.parts[0]["state"] == "sent" and current.parts[1]["state"] == "pending"
    finally:
        await sender.stop()


@pytest.mark.parametrize("day,hours", [("2030-03-10T16:00:00+00:00", 23), ("2030-11-03T16:00:00+00:00", 25)])
def test_daily_budget_uses_local_calendar_not_fixed_24_hours(day, hours):
    from datetime import datetime
    from types import SimpleNamespace

    from app.telegram_bots.notifications import _day_window

    start, end = _day_window(SimpleNamespace(timezone="America/New_York"), datetime.fromisoformat(day))
    assert (end - start).total_seconds() == hours * 3600


@pytest.mark.asyncio
async def test_pending_optional_delivery_expires_after_partial_send_and_retry(db):
    from app.telegram_bots.producer import TransientDeliveryError

    link = await prepare(db)
    clock = Clock()
    outbox = Outbox(db.sessions, clock=clock)
    await outbox.add(link, dedupe_key="optional:partial", class_="info", payload={"parts": [{"kind": "text", "source_ref": {"key": "chat.restarted", "values": {}}} for _ in range(2)]})
    calls = []

    async def send(link, row, part):
        calls.append(part["part_id"])
        if len(calls) == 2:
            raise TransientDeliveryError()
        return PartReceipt((101,))

    sender = OutboxSender(outbox, run_id="expiry", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        await sender.tick()
        await sender.drain()
        clock.advance(1)
        await sender.tick()
        await sender.drain()
        clock.advance(24 * 3600)
        await sender.tick()
        await sender.drain()
        saved = next(row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key == "optional:partial")
        assert len(calls) == 2
        assert saved.state == "skipped" and saved.skip_reason == "expired"
        assert saved.parts[0]["message_ids"] == [101]
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_edit_budget_exemption_requires_an_actual_edit_plan(db):
    link = await prepare(db)
    with pytest.raises(ValueError, match="invalid-notification-edit"):
        await notification(Outbox(db.sessions), link, "info", "edit:forged")
    assert not any(row.dedupe_key == "edit:forged" for row in await rows(db, TelegramOutboxRow))


@pytest.mark.asyncio
async def test_rotation_never_places_optional_news_ahead_of_another_bots_question(db):
    first = await prepare(db)
    second = await prepare(db, bot=9_000_000_002)
    outbox = Outbox(db.sessions, clock=Clock())
    await notification(outbox, first, "question")
    await notification(outbox, second, "info")
    seen = []

    async def send(link, row, part):
        seen.append(row.class_)
        return PartReceipt((100,))

    sender = OutboxSender(outbox, run_id="priority", deliver_part=transport(db, send), eligibility=allowed, proactive=True, concurrency=1)
    sender._last_link = first.id
    try:
        await sender.tick()
        await sender.drain()
        assert seen == ["question"]
    finally:
        await sender.stop()


def test_repeated_midnight_keeps_the_first_midnight_as_the_daily_boundary():
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from app.telegram_bots.notifications import _day_window

    start, end = _day_window(SimpleNamespace(timezone="America/Havana"), datetime(2030, 11, 3, 5, 30, tzinfo=UTC))
    assert start == datetime(2030, 11, 3, 4, tzinfo=UTC)
    assert end == datetime(2030, 11, 4, 5, tzinfo=UTC)


@pytest.mark.asyncio
async def test_retry_across_midnight_keeps_its_original_daily_reservation(db):
    from datetime import UTC, datetime

    from app.telegram_bots.config import TelegramBotsConfig
    from app.telegram_bots.producer import TransientDeliveryError

    link = await prepare(db)
    await db.settings.update(link.owner_user_id, timezone="UTC")
    clock = Clock()
    clock.now = datetime(2030, 1, 1, 23, 59, 59, tzinfo=UTC)
    outbox = Outbox(db.sessions, clock=clock, config=TelegramBotsConfig(outbox={"per_person_per_day": 1}))
    await notification(outbox, link, "info", "old-day")
    calls = []

    async def send(link, row, part):
        calls.append(row.dedupe_key)
        if len(calls) == 1:
            raise TransientDeliveryError()
        return PartReceipt((100 + len(calls),))

    sender = OutboxSender(outbox, run_id="midnight", deliver_part=transport(db, send), eligibility=allowed, proactive=True, jitter=lambda: 0)
    try:
        await sender.tick()
        await sender.drain()
        before = next(row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key == "old-day")
        reserved = before.payload["notification_reserved_at"]
        clock.advance(10)
        await sender.tick()
        await sender.drain()
        after = next(row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key == "old-day")
        assert after.state == "sent" and after.payload["notification_reserved_at"] == reserved
        clock.advance(61)
        await notification(outbox, link, "info", "new-day")
        await sender.tick()
        await sender.drain()
        assert calls == ["old-day", "old-day", "new-day"]
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_sdk_rechecks_preferences_after_awaited_authority_read(db, monkeypatch):
    from contextvars import ContextVar
    from functools import partial
    from types import SimpleNamespace

    from test_telegram_bot_handlers import ingress

    from app.telegram_bots.adapter import BoundedRateLimiter
    from app.telegram_bots.supervisor import TelegramSupervisor

    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    inside_sdk = ContextVar("inside_notification_sdk", default=False)
    host = SimpleNamespace(_stopping=False, _cause=None, _send_ready=True, human_inputs=None, outbox=e.services.outbox, authority=e.live.authority)
    e.services.eligibility = partial(TelegramSupervisor.eligibility, host)
    e.services.authority = e.live.authority
    original = e.live.authority.delivery
    process = BoundedRateLimiter.process_request
    changed = []

    async def authority_read(*args, **kwargs):
        decision = await original(*args, **kwargs)
        if inside_sdk.get() and decision.decision == "allow":
            await db.settings.update(e.link.owner_user_id, notify="none")
            changed.append(True)
        return decision

    async def at_sdk(self, *args, **kwargs):
        token = inside_sdk.set(True)
        try:
            return await process(self, *args, **kwargs)
        finally:
            inside_sdk.reset(token)

    monkeypatch.setattr(e.live.authority, "delivery", authority_read)
    monkeypatch.setattr(BoundedRateLimiter, "process_request", at_sdk)
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    await notification(e.services.outbox, e.link, "question")
    sender = OutboxSender(e.services.outbox, run_id="sdk", deliver_part=lambda **kwargs: e.channel.send_part(**{key: value for key, value in kwargs.items() if key != "link_id"}), eligibility=e.services.eligibility, proactive=True)
    try:
        await sender.tick()
        await sender.drain()
        await sender.tick()
        await sender.drain()
        assert changed
        assert not any(call.endpoint == "sendMessage" for call in e.request.calls)
        row = next(row for row in await rows(db, TelegramOutboxRow) if row.class_ == "question")
        assert row.state == "skipped" and row.skip_reason == "notifications-off"
    finally:
        await sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("retire", ["account", "claim", "use"])
async def test_sdk_final_snapshot_rejects_authority_retired_during_settings_read(db, monkeypatch, retire):
    from contextvars import ContextVar
    from datetime import UTC, datetime
    from functools import partial
    from types import SimpleNamespace

    from sqlalchemy import update
    from test_telegram_bot_handlers import ingress
    from test_telegram_instance import instance_environment
    from test_telegram_threads import close

    from app.telegram_bots.adapter import BoundedRateLimiter
    from app.telegram_bots.supervisor import TelegramSupervisor
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = await instance_environment(db, monkeypatch) if retire == "use" else await ingress(db, monkeypatch=monkeypatch, lead=True)
    services = e.channel._services
    services.authority = e.live.authority
    host = SimpleNamespace(_stopping=False, _cause=None, _send_ready=True, human_inputs=None, outbox=services.outbox, authority=e.live.authority)
    services.eligibility = partial(TelegramSupervisor.eligibility, host)
    if retire == "use":
        async with db.sessions.begin() as session:
            await session.execute(update(AgentInstanceGrantRow).values(permissions=3))
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    await notification(services.outbox, e.link, "question")
    inside = ContextVar("notification_final_read", default=False)
    original, process = services.settings.get, BoundedRateLimiter.process_request
    changed = []

    async def read(*args, **kwargs):
        result = await original(*args, **kwargs)
        if inside.get() and not changed:
            async with db.sessions.begin() as session:
                if retire == "account":
                    session.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.link.owner_user_id, disabled_at=datetime.now(UTC)))
                elif retire == "use":
                    await session.execute(update(AgentInstanceGrantRow).values(permissions=2))
                else:
                    await session.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.class_ == "question").values(claim_id="foreign-claim"))
            changed.append(True)
        return result

    async def at_sdk(self, *args, **kwargs):
        token = inside.set(True)
        try:
            return await process(self, *args, **kwargs)
        finally:
            inside.reset(token)

    monkeypatch.setattr(services.settings, "get", read)
    monkeypatch.setattr(BoundedRateLimiter, "process_request", at_sdk)
    sender = OutboxSender(services.outbox, run_id="final-read", deliver_part=lambda **kwargs: e.channel.send_part(**{key: value for key, value in kwargs.items() if key != "link_id"}), eligibility=services.eligibility, proactive=True)
    try:
        await sender.tick()
        await sender.drain()
        assert changed
        assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)
        if retire == "claim":
            saved = next(row for row in await rows(db, TelegramOutboxRow) if row.class_ == "question")
            assert saved.claim_id == "foreign-claim"
    finally:
        await sender.stop()
        if retire == "use":
            await close(e)
        else:
            await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("unsupported", ["model", "keyboard", "file", "list", "legacy_source", "role", "files_link"])
async def test_proactive_source_specific_plans_wait_for_their_final_authority_integration(db, monkeypatch, unsupported):
    from test_telegram_bot_handlers import ingress

    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    await notification(e.services.outbox, e.link, "question")
    async with db.sessions.begin() as session:
        row = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.class_ == "question"))
        if unsupported == "model":
            row.payload = {**row.payload, "source": {"kind": "model_text"}}
        elif unsupported == "role":
            row.parts = [{**row.parts[0], "source_ref": {"key": "hello.employee", "values": {"role": "private role"}}}]
        elif unsupported == "files_link":
            row.parts = [{**row.parts[0], "url_button": {"page": "web.files"}}]
        elif unsupported == "list":
            row.payload = {**row.payload, "agent_list_basis": {}}
        elif unsupported == "legacy_source":
            row.parts = [{**row.parts[0], "source_ref": {"assistant_message_id": "old-answer", "thread_id": "old-thread"}}]
        else:
            row.parts = [{**row.parts[0], **({"stored_keyboard": True} if unsupported == "keyboard" else {"kind": "document"})}]
    sender = OutboxSender(e.services.outbox, run_id="unsupported", deliver_part=lambda **kwargs: e.channel.send_part(**{key: value for key, value in kwargs.items() if key != "link_id"}), eligibility=allowed, proactive=True)
    try:
        await sender.tick()
        await sender.drain()
        row = next(row for row in await rows(db, TelegramOutboxRow) if row.class_ == "question")
        assert row.state == "failed" and row.failed_reason == "invalid-payload"
        assert not any(call.endpoint in {"sendMessage", "sendDocument"} for call in e.request.calls)
    finally:
        await sender.stop()
        await e.channel.stop()
