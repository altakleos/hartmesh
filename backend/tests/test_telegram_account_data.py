"""Telegram retention and export over real rows on both SQL backends."""

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db  # noqa: F401
from sqlalchemy import select

from app.gateway.account_export import telegram_export_document
from app.telegram_bots import retention
from app.telegram_bots.callbacks import CallbackRepository
from app.telegram_bots.retention import sweep_rows
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkEventRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
SENTINEL = "a private act body"


async def bound(db, *, account=7_000_000_001, **kwargs):
    link = await add_link(db, **kwargs)
    await bind_link(db, link, account=account)
    return await db.links.get(link.id)


def outbox(link, *, state="sent", days=31.0, dedupe=None, payload=None):
    at = NOW - timedelta(days=days)
    return TelegramOutboxRow(
        id=uuid4().hex,
        link_id=link.id,
        owner_user_id=link.owner_user_id,
        dedupe_key=dedupe or uuid4().hex,
        class_="reply",
        binding_generation=link.binding_generation,
        state=state,
        not_before=at,
        closed_at=at,
        created_at=at,
        payload=payload or {},
    )


def callback(link, *, target=None, kind="outbox", payload=None, expires=None, consumed=None, days=1, generation=None):
    return TelegramCallbackRow(
        id=uuid4().hex,
        group_id=uuid4().hex,
        link_id=link.id,
        chat_id=link.chat_id,
        binding_generation=link.binding_generation if generation is None else generation,
        kind="settings_set",
        target_kind=kind if target else None,
        target_id=target,
        payload=payload or {},
        expires_at=expires,
        consumed_at=consumed,
        created_at=NOW - timedelta(days=days),
    )


def mark(link, *, state="done", days=8.0, message=None, **kwargs):
    message = message or uuid4().int % 2**40
    at = NOW - timedelta(days=days)
    return TelegramInboundMarkRow(link_id=link.id, message_id=message, update_id=message, chat_id=link.chat_id, binding_generation=link.binding_generation, accepted_at=at, state=state, closed_at=at, **kwargs)


def act(link, *, state="done", days=401, message=None, **kwargs):
    at = NOW - timedelta(days=days)
    return TelegramActRow(
        id=uuid4().hex,
        link_id=link.id,
        bot_id=link.bot_id,
        owner_user_id=link.owner_user_id,
        binding_generation=link.binding_generation,
        telegram_user_id=link.telegram_user_id,
        chat_id=link.chat_id,
        message_id=message,
        kind="settings_set",
        state=state,
        created_at=at,
        updated_at=at,
        **kwargs,
    )


def owner(**fields):
    return {"publication_owner": {"turn_id": uuid4().hex, "state": "accepted", **fields}}


async def seed(db, *rows):
    async with db.sessions() as session, session.begin():
        session.add_all(rows)


async def sweep(db, now=NOW):
    return await sweep_rows(db.sessions, now, callback_max_age_days=db.config.callbacks.max_age_days)


async def exists(db, cls, *key):
    async with db.sessions() as session:
        return await session.get(cls, key if len(key) > 1 else key[0]) is not None


PINS = {
    "callback-target": lambda link, id: callback(link, target=id),
    "callback-anchor": lambda link, id: callback(link, payload={"anchor_outbox_id": id}),
    "callback-not-yet-expired": lambda link, id: callback(link, target=id, expires=NOW + timedelta(hours=1)),
    "publication-source": lambda link, id: callback(link, payload=owner(source_outbox_id=id), consumed=NOW),
    "publication-delivery": lambda link, id: callback(link, payload=owner(delivery_ids=[id]), consumed=NOW),
    "publication-primary": lambda link, id: callback(link, payload=owner(outbox_id=id), consumed=NOW),
    "unfinished-delivery": lambda link, id: mark(link, state="reply_pending", delivery_ids=[id]),
    "unfinished-primary": lambda link, id: mark(link, state="dispatched", outbox_id=id),
    "interrupted-delivery": lambda link, id: mark(link, state="interrupted", delivery_ids=[id]),
}
RELEASED = {
    "consumed": lambda link, id: callback(link, target=id, consumed=NOW - timedelta(days=1)),
    "expired": lambda link, id: callback(link, target=id, expires=NOW),
    "at-the-age-limit": lambda link, id: callback(link, target=id, days=180),
    "earlier-binding": lambda link, id: callback(link, target=id, generation=link.binding_generation - 1),
    "another-target-kind": lambda link, id: callback(link, target=id, kind="work"),
    "finished-mark": lambda link, id: mark(link, days=1, delivery_ids=[id]),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("pin", PINS.values(), ids=PINS.keys())
async def test_a_live_reference_keeps_an_old_delivery(db, pin):
    link = await bound(db)
    kept, gone = outbox(link), outbox(link)
    await seed(db, kept, gone, pin(link, kept.id))
    assert (await sweep(db))["outbox"] == 1
    assert await exists(db, TelegramOutboxRow, kept.id) and not await exists(db, TelegramOutboxRow, gone.id)


@pytest.mark.asyncio
@pytest.mark.parametrize("ended", RELEASED.values(), ids=RELEASED.keys())
async def test_an_ended_reference_keeps_nothing(db, ended):
    link = await bound(db)
    row = outbox(link)
    await seed(db, row, ended(link, row.id))
    assert (await sweep(db))["outbox"] == 1
    assert not await exists(db, TelegramOutboxRow, row.id)


@pytest.mark.asyncio
async def test_a_finished_publication_owner_releases_its_deliveries(db, monkeypatch):
    link = await bound(db)
    row = outbox(link)
    await seed(db, row, callback(link, payload=owner(delivery_ids=[row.id]), consumed=NOW))
    monkeypatch.setattr(retention, "callback_publication_pending", lambda payload: False)
    assert (await sweep(db))["outbox"] == 1


@pytest.mark.asyncio
async def test_retained_delivery_goes_at_the_next_sweep_on_its_first_clock(db):
    link = await bound(db)
    row = outbox(link)
    control = callback(link, target=row.id)
    await seed(db, row, control)
    assert (await sweep(db))["outbox"] == 0
    async with db.sessions() as session, session.begin():
        (await session.get(TelegramCallbackRow, control.id)).consumed_at = NOW
    assert (await sweep(db))["outbox"] == 1


@pytest.mark.asyncio
async def test_a_live_control_keeps_the_first_reply_of_its_turn(db):
    link = await bound(db)
    turn = uuid4().hex
    first = outbox(link, dedupe="reply:" + turn)
    step = outbox(link, days=1, dedupe="step", payload={"reply_basis": {"publication": {"turn_id": turn}}})
    control = callback(link, payload={"anchor_outbox_id": step.id})
    await seed(db, first, step, control)
    assert (await sweep(db))["outbox"] == 0
    async with db.sessions() as session, session.begin():
        (await session.get(TelegramCallbackRow, control.id)).consumed_at = NOW
    assert (await sweep(db))["outbox"] == 1
    assert not await exists(db, TelegramOutboxRow, first.id)


@pytest.mark.asyncio
async def test_a_row_someone_holds_waits_for_the_next_sweep(db):
    if db.dialect != "postgres":
        pytest.skip("SQLite has one writer at a time")
    link = await bound(db)
    row = outbox(link)
    await seed(db, row)
    async with db.sessions() as session, session.begin():
        await session.get(TelegramOutboxRow, row.id, with_for_update=True)
        assert (await sweep(db))["outbox"] == 0
    assert (await sweep(db))["outbox"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["pending", "sending"])
async def test_an_undelivered_row_is_never_swept(db, state):
    link = await bound(db)
    row = outbox(link, state=state, days=400)
    await seed(db, row)
    assert (await sweep(db))["outbox"] == 0
    assert await exists(db, TelegramOutboxRow, row.id)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["sent", "skipped", "failed"])
async def test_a_settled_row_goes_after_thirty_days(db, state):
    link = await bound(db)
    boundary, past = outbox(link, state=state, days=30), outbox(link, state=state, days=30.01)
    await seed(db, boundary, past)
    assert (await sweep(db))["outbox"] == 1
    assert await exists(db, TelegramOutboxRow, boundary.id) and not await exists(db, TelegramOutboxRow, past.id)


@pytest.mark.asyncio
async def test_inbound_identity_goes_after_seven_days_only_when_completed_and_settled(db):
    link = await bound(db)
    boundary, past, unfinished = mark(link, days=7), mark(link, days=7.01), mark(link, state="reply_pending", days=30)
    waiting = mark(link, message=901)
    uncertain = mark(link, message=902)
    settled = mark(link, message=903)
    await seed(db, boundary, past, unfinished, waiting, uncertain, settled, act(link, state="waiting", days=1, message=901), act(link, state="uncertain", days=1, message=902), act(link, days=1, message=903))
    assert (await sweep(db))["inbound"] == 2

    def key(row):
        return row.link_id, row.message_id, row.chat_id

    assert not await exists(db, TelegramInboundMarkRow, *key(past)) and not await exists(db, TelegramInboundMarkRow, *key(settled))
    for row in (boundary, unfinished, waiting, uncertain):
        assert await exists(db, TelegramInboundMarkRow, *key(row))


@pytest.mark.asyncio
async def test_acts_go_after_four_hundred_days_only_when_settled(db):
    link = await bound(db)
    gone = [act(link, state=state) for state in ("done", "refused", "conflict")]
    kept = [
        act(link, days=400),
        *(act(link, state=state) for state in ("draft", "pending", "waiting", "uncertain")),
        act(link, stop_dispatch={"state": "requested"}),
        act(link, continuation={"state": "live"}),
    ]
    await seed(db, *gone, *kept)
    assert (await sweep(db))["acts"] == 3
    assert not any([await exists(db, TelegramActRow, row.id) for row in gone])
    assert all([await exists(db, TelegramActRow, row.id) for row in kept])


@pytest.mark.asyncio
async def test_retention_sweep_deletes_old_outbox_acts_and_event_rows(db):
    link = await bound(db)
    await seed(db, outbox(link), act(link), mark(link))
    async with db.sessions() as session, session.begin():
        events = list(await session.scalars(select(TelegramBotLinkEventRow).where(TelegramBotLinkEventRow.link_id == link.id)))
        for row in events:
            row.created_at = NOW - timedelta(days=401)
    assert await sweep(db) == {"inbound": 1, "outbox": 1, "acts": 1, "events": len(events)}
    assert await sweep(db) == {"inbound": 0, "outbox": 0, "acts": 0, "events": 0}


@pytest.mark.asyncio
async def test_callback_sweep_keeps_a_used_row_seven_days_and_any_row_180_days(db):
    link = await bound(db)
    used = callback(link, consumed=NOW - timedelta(days=7), days=8)
    recent = callback(link, consumed=NOW - timedelta(days=6), days=8)
    aged = callback(link, days=180)
    younger = callback(link, days=179)
    await seed(db, used, recent, aged, younger)
    assert await CallbackRepository(db.sessions, config=db.config).sweep(NOW) == 2
    assert not await exists(db, TelegramCallbackRow, used.id) and not await exists(db, TelegramCallbackRow, aged.id)
    assert await exists(db, TelegramCallbackRow, recent.id) and await exists(db, TelegramCallbackRow, younger.id)


@pytest.mark.asyncio
async def test_export_holds_a_persons_rows_without_tokens_or_bodies(db, monkeypatch):
    monkeypatch.setattr("deerflow.persistence.engine.get_session_factory", lambda: db.sessions)
    link = await bound(db)
    theirs = await bound(db, owner="owner-b", bot=9_000_000_002, account=7_000_000_002)
    mine, other = act(link, days=1, reason="settings", body={"text": SENTINEL}), act(theirs, days=1)
    await seed(db, mine, other)

    document = await telegram_export_document(link.owner_user_id)

    assert set(document) == {"settings", "bots", "acts"}
    assert document["settings"] == {
        "timezone": "America/New_York",
        "locale": "en-US",
        "quiet_enabled": True,
        "quiet_start": "21:00",
        "quiet_end": "07:00",
        "notify": "all",
        "summary_enabled": True,
        "summary_time": "08:00",
        "voice_agreed_for": None,
        "voice_agreed_at": None,
    }
    assert [bot["id"] for bot in document["bots"]] == [link.id]
    assert set(document["bots"][0]) == {"id", "agent_kind", "agent_ref", "bot_username", "origin", "state", "health", "connected_as", "created_at", "connected_at"}
    assert document["bots"][0]["state"] == "connected" and document["bots"][0]["connected_as"] == link.connected_name
    assert [row["id"] for row in document["acts"]] == [mine.id]
    assert set(document["acts"][0]) == {"id", "kind", "state", "reason", "bot_id", "instance_id", "work_id", "request_id", "created_at"}
    text = json.dumps(document)
    assert SENTINEL not in text and "wrapped-fixture" not in text


@pytest.mark.asyncio
async def test_export_omits_telegram_for_a_person_with_nothing_there(db, monkeypatch):
    monkeypatch.setattr("deerflow.persistence.engine.get_session_factory", lambda: db.sessions)
    assert await telegram_export_document("no-telegram") is None
    await db.settings.ensure("settings-only", timezone="UTC", locale="zh-CN")
    assert (await telegram_export_document("settings-only"))["bots"] == []
