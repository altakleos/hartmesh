"""Real callback SQL over seeded fixed anchors; no Telegram transport proof."""

import asyncio
import importlib
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
import pytest_asyncio
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db  # noqa: F401
from sqlalchemy import event, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from test_telegram_publication_custody import publication_environment as publication_environment  # noqa: F401

from app.telegram_bots.producer import Outbox
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)


def api():
    return importlib.import_module("app.telegram_bots.callbacks")


@pytest_asyncio.fixture
async def callbacks_environment(publication_environment):
    e = publication_environment
    clock = [NOW]
    module = api()
    repo = module.CallbackRepository(e.sessions, config=e.db.config, clock=lambda: clock[0])
    outbox = Outbox(e.sessions, inbox=e.inbox, clock=lambda: clock[0])

    async def plan(*, keyboard=None, message_id=None, topic=None, source="callback-unit"):
        keyboard = keyboard or [
            [
                dict(label="label.english", kind="settings_set", target_kind="link", target_id=e.link.id, payload={"field": "locale", "value": "en-US"}),
                dict(label="label.chinese", kind="settings_set", target_kind="link", target_id=e.link.id, payload={"field": "locale", "value": "zh-CN"}),
            ]
        ]
        # Explicit protocol seed until Root's atomic producer composition exists.
        identity = await outbox.plan_product(e.link, source_id=source, parts=[{"kind": "text", "source_ref": {"key": "settings.state_basic", "values": {"language": "English"}}}], message_thread_id=topic)
        async with outbox._writer(e.link.id) as (session, _binding):
            parent = await session.get(TelegramOutboxRow, identity, with_for_update=True)
            parent.payload = {**parent.payload, "product_keyboard_basis": module.canonical_keyboard(keyboard)}
            written = await repo.write_group(e.link, keyboard, anchor_outbox_id=identity, presentation_version=0, chat_id=e.link.chat_id, message_id=message_id, message_thread_id=topic, session=session)
            parent.parts = [{**part, "callback_group_id": written.group_id} for part in parent.parts]
        return SimpleNamespace(outbox_id=identity, group=written, keyboard=keyboard)

    yield SimpleNamespace(**vars(e), callbacks=repo, clock=clock, outbox=outbox, plan=plan)


@pytest.mark.asyncio
async def test_write_group_detaches_rows_and_allocates_random_action_ids(callbacks_environment):
    e = callbacks_environment
    first, second = await e.plan(), await e.plan(source="second")
    assert first.group.group_id != second.group.group_id
    assert len(first.group.callback_ids) == 2 and set(first.group.callback_ids).isdisjoint(second.group.callback_ids)
    row = await e.callbacks.get(first.group.callback_ids[0])
    assert row.link_id == e.link.id and row.chat_id == e.link.chat_id and row.message_id is None
    assert row.payload["field"] == "locale" and row.payload["anchor_outbox_id"] == first.outbox_id
    assert row.payload["presentation_version"] == 0 and row.payload["presentation_pending"] is False
    with pytest.raises(TypeError):
        row.payload["value"] = "forged"


@pytest.mark.asyncio
async def test_adoption_moves_whole_group_once_and_rejects_another_message(callbacks_environment):
    e = callbacks_environment
    plan = await e.plan()
    assert await e.callbacks.adopt(e.link, plan.group.group_id, 902, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=plan.outbox_id, presentation_version=0, check_current=lambda: True)
    assert [(await e.callbacks.get(identity)).message_id for identity in plan.group.callback_ids] == [902, 902]
    assert not await e.callbacks.adopt(e.link, plan.group.group_id, 903, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=plan.outbox_id, presentation_version=0, check_current=lambda: True)
    route = await e.callbacks.for_message(e.link.id, 902, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation)
    assert route is not None and not route.retired_presentation


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["pending", "version", "topic", "generation", "retired"])
async def test_adoption_refuses_stale_anchor_or_scope_without_any_partial_update(callbacks_environment, change):
    e = callbacks_environment
    plan = await e.plan()
    if change in {"pending", "version"}:
        async with e.sessions.begin() as session:
            rows = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == plan.group.group_id)))
            for row in rows:
                row.payload = {**row.payload, "presentation_pending": change == "pending", "presentation_version": 1 if change == "version" else 0}
    elif change == "generation":
        async with e.sessions.begin() as session:
            await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(binding_generation=e.link.binding_generation + 1))
    actual = await e.callbacks.adopt(e.link, plan.group.group_id, 902, chat_id=e.link.chat_id, message_thread_id=99 if change == "topic" else None, outbox_id=plan.outbox_id, presentation_version=0, check_current=lambda: change != "retired")
    assert actual is False
    assert [(await e.callbacks.get(identity)).message_id for identity in plan.group.callback_ids] == [None, None]


@pytest.mark.asyncio
async def test_expired_adoption_admission_refuses_before_any_sql(callbacks_environment):
    e, dispatched = callbacks_environment, []
    plan = await e.plan()

    def observed(*_args):
        dispatched.append(True)

    event.listen(e.db.engine.sync_engine, "before_cursor_execute", observed)
    try:
        assert not await e.callbacks.adopt(e.link, plan.group.group_id, 902, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=plan.outbox_id, presentation_version=0, check_current=lambda: False)
    finally:
        event.remove(e.db.engine.sync_engine, "before_cursor_execute", observed)
    assert dispatched == []


@pytest.mark.asyncio
async def test_typed_reply_never_adopts_an_unknown_message(callbacks_environment):
    e = callbacks_environment
    plan = await e.plan()
    assert await e.callbacks.for_message(e.link.id, 902, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation) is None
    assert [(await e.callbacks.get(identity)).message_id for identity in plan.group.callback_ids] == [None, None]


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["boolean_version", "changed_body"])
async def test_adoption_refuses_callback_state_different_from_saved_keyboard_basis(callbacks_environment, corruption):
    e = callbacks_environment
    plan = await e.plan()
    async with e.sessions.begin() as session:
        row = await session.get(TelegramCallbackRow, plan.group.callback_ids[0])
        altered = {**row.payload, **({"presentation_version": False} if corruption == "boolean_version" else {"value": "zh-CN"})}
        await session.execute(update(TelegramCallbackRow).where(TelegramCallbackRow.id == row.id).values(payload=altered))
    if corruption == "boolean_version":
        assert type((await e.callbacks.get(plan.group.callback_ids[0])).payload["presentation_version"]) is bool
    assert not await e.callbacks.adopt(e.link, plan.group.group_id, 902, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=plan.outbox_id, presentation_version=0, check_current=lambda: True)
    assert [(await e.callbacks.get(identity)).message_id for identity in plan.group.callback_ids] == [None, None]


@pytest.mark.asyncio
async def test_history_alias_is_routing_only_and_must_match_scope(callbacks_environment):
    e = callbacks_environment
    plan = await e.plan(message_id=903)
    async with e.sessions.begin() as session:
        parent = await session.get(TelegramOutboxRow, plan.outbox_id)
        parent.payload = {**parent.payload, "presentation_version": 1}
        parent.parts = [{**parent.parts[0], "state": "sent", "message_ids": [902]}, {**parent.parts[0], "presentation_version": 1, "message_ids": [903], "state": "sent"}]
        for row in await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == plan.group.group_id)):
            row.payload = {**row.payload, "presentation_version": 1, "presentation_history": [{"message_id": 902, "message_thread_id": None, "presentation_version": 0}]}
    route = await e.callbacks.for_message(e.link.id, 902, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation)
    assert route.retired_presentation and len(route.rows) == 2
    assert await e.callbacks.for_message(e.link.id, 902, chat_id=e.link.chat_id, message_thread_id=44, binding_generation=e.link.binding_generation) is None
    assert not await e.callbacks.adopt(e.link, plan.group.group_id, 902, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=plan.outbox_id, presentation_version=1, check_current=lambda: True)


@pytest.mark.asyncio
async def test_new_current_keyboard_wins_over_consumed_rows_on_the_same_edited_message(callbacks_environment):
    e = callbacks_environment
    old, fresh = await e.plan(message_id=902), await e.plan(message_id=902, source="settings-edit")
    async with e.sessions.begin() as session:
        for row in await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == old.group.group_id)):
            row.consumed_at, row.closed_reason = NOW, "done"
        for identity in (old.outbox_id, fresh.outbox_id):
            row = await session.get(TelegramOutboxRow, identity)
            row.parts = [{**part, "state": "sent", "message_ids": [902]} for part in row.parts]
    route = await e.callbacks.for_message(e.link.id, 902, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation)
    assert route is not None and not route.retired_presentation
    assert {row.group_id for row in route.rows} == {fresh.group.group_id}


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["body", "partial_message"])
async def test_typed_routing_requires_complete_current_group_and_frozen_basis(callbacks_environment, change):
    e = callbacks_environment
    plan = await e.plan(message_id=902)
    async with e.sessions.begin() as session:
        parent = await session.get(TelegramOutboxRow, plan.outbox_id)
        parent.parts = [{**part, "state": "sent", "message_ids": [902]} for part in parent.parts]
        if change == "body":
            row = await session.get(TelegramCallbackRow, plan.group.callback_ids[0])
            await session.execute(update(TelegramCallbackRow).where(TelegramCallbackRow.id == row.id).values(payload={**row.payload, "value": "zh-CN"}))
        else:
            await session.execute(update(TelegramCallbackRow).where(TelegramCallbackRow.id == plan.group.callback_ids[0]).values(message_id=None))
    route = await e.callbacks.for_message(e.link.id, 902, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation)
    assert route is not None and route.retired_presentation


@pytest.mark.asyncio
async def test_group_consumption_joins_same_writer_and_rolls_back(callbacks_environment):
    e = callbacks_environment
    plan = await e.plan(message_id=902)
    with pytest.raises(RuntimeError, match="rollback"):
        async with e.outbox._writer(e.link.id) as (session, _):
            group = await e.callbacks.lock_current(e.link, plan.group.callback_ids[0], chat_id=e.link.chat_id, message_id=902, message_thread_id=None, guard=e.guard_factory(), session=session)
            await e.callbacks.consume_group(group, reason="done", session=session)
            raise RuntimeError("rollback")
    assert [(await e.callbacks.get(identity)).consumed_at for identity in plan.group.callback_ids] == [None, None]
    async with e.outbox._writer(e.link.id) as (session, _):
        group = await e.callbacks.lock_current(e.link, plan.group.callback_ids[0], chat_id=e.link.chat_id, message_id=902, message_thread_id=None, guard=e.guard_factory(), session=session)
        await e.callbacks.consume_group(group, reason="done", session=session)
    assert [(await e.callbacks.get(identity)).closed_reason for identity in plan.group.callback_ids] == ["done", "done"]


@pytest.mark.asyncio
async def test_guard_for_another_real_owner_link_cannot_authorize_this_group(callbacks_environment, monkeypatch):
    e = callbacks_environment
    plan = await e.plan(message_id=902)
    other = await install_live_owner(e.db, monkeypatch, email="other-callback@example.com")
    foreign = await bind_link(e.db, await add_link(e.db, owner=other.id, bot=9000000002), account=7000000002, message=111, update=211)
    checker = await other.authority.prepare_live_owner(other.id)
    wrong = e.links.publication_guard(foreign, check_live_owner=checker, check_attempt_current=lambda: True)
    with pytest.raises(api().CallbackRefused):
        async with e.outbox._writer(e.link.id) as (session, _):
            await e.callbacks.lock_current(e.link, plan.group.callback_ids[0], chat_id=e.link.chat_id, message_id=902, message_thread_id=None, guard=wrong, session=session)
    assert [(await e.callbacks.get(identity)).consumed_at for identity in plan.group.callback_ids] == [None, None]


@pytest.mark.asyncio
@pytest.mark.parametrize("url_only", [False, True])
async def test_sender_keyboard_uses_current_fenced_layout_and_url_only_groups(callbacks_environment, url_only):
    from app.telegram_bots.producer import OutboxItem, SendingPart

    e = callbacks_environment
    keyboard = [[dict(label="label.open_telegram", url="https://product.example/settings/telegram")]]
    if not url_only:
        keyboard.append([dict(label="label.cancel", kind="dismiss", payload={})])
    plan = await e.plan(keyboard=keyboard)
    async with e.sessions.begin() as session:
        row = await session.get(TelegramOutboxRow, plan.outbox_id)
        row.state, row.claim_owner, row.claim_id = "sending", "process", "claim"
        row.parts = [{**part, "state": "sending"} for part in row.parts]
        item = OutboxItem(**{name: getattr(row, name) for name in OutboxItem.__dataclass_fields__})
        captured = SendingPart(e.link, item, row.parts[0], "process", "claim", e.link.token_revision)
    stored = await e.callbacks.read_keyboard(captured)
    assert stored[0][0].label == "label.open_telegram" and stored[0][0].url == "https://product.example/settings/telegram"
    if not url_only:
        assert stored[1][0].callback_data == "h1:" + plan.group.callback_ids[0]
    assert len(plan.group.callback_ids) == (0 if url_only else 1)


def test_keyboard_descriptor_cannot_claim_server_presentation_or_credentials():
    module = api()
    for payload in ({"anchor_outbox_id": "forged"}, {"presentation_pending": False}, {"bot_token": "forged"}):
        with pytest.raises(ValueError):
            module.canonical_keyboard([[dict(label="label.back", kind="back", payload=payload)]])


def test_explicit_keyboard_clear_is_canonical_but_empty_lines_are_not():
    assert api().canonical_keyboard([]) == []
    with pytest.raises(ValueError):
        api().canonical_keyboard([[]])


@pytest.mark.asyncio
async def test_admitted_actual_update_drains_commit_through_repeated_caller_cancel(callbacks_environment, monkeypatch):
    e = callbacks_environment
    plan = await e.plan()
    entered, release = asyncio.Event(), asyncio.Event()
    original = AsyncSession.flush

    async def held(session, *args, **kwargs):
        await original(session, *args, **kwargs)
        if any(isinstance(row, TelegramCallbackRow) for row in session.identity_map.values()):
            entered.set()
            await release.wait()

    monkeypatch.setattr(AsyncSession, "flush", held)
    task = asyncio.create_task(e.callbacks.adopt(e.link, plan.group.group_id, 902, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=plan.outbox_id, presentation_version=0, check_current=lambda: True))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.02))[0]
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.02))[0]
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert [(await e.callbacks.get(identity)).message_id for identity in plan.group.callback_ids] == [902, 902]
