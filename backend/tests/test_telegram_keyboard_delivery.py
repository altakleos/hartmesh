"""Fixed controls keep their stored intent, exact anchor and delivery identity."""

import asyncio
from types import SimpleNamespace

import pytest
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from sqlalchemy import select

from app.telegram_bots.producer import Outbox
from app.telegram_bots.render import render_plain
from app.telegram_bots.texts import KEYBOARDS
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.parametrize("locale", ["en-US", "zh-CN"])
def test_current_language_controls_are_scoped_and_render_as_one_short_pair(locale):
    link = SimpleNamespace(id="a" * 32)
    keyboard = KEYBOARDS["settings.state_basic"](link=link)
    assert len(keyboard) == 1 and len(keyboard[0]) == 2
    assert [button["payload"] for button in keyboard[0]] == [{"field": "locale", "value": "en-US"}, {"field": "locale", "value": "zh-CN"}]
    assert all(button["kind"] == "settings_set" and button["target_kind"] == "link" and button["target_id"] == link.id for button in keyboard[0])
    assert all(len(render_plain(button["label"], locale)) <= 14 for button in keyboard[0])
    assert [render_plain(button["label"], locale) for button in keyboard[0]] == ["English", "中文"]


def test_new_confirmation_controls_keep_the_original_thread_and_explicit_choice():
    keyboard = KEYBOARDS["new.confirm_plain"](thread_id="original-thread")
    assert [button["kind"] for row in keyboard for button in row] == ["new_confirm", "new_keep"]
    assert all(button["payload"] == {"thread_id": "original-thread"} and button["target_kind"] == "thread" and button["target_id"] == "original-thread" for row in keyboard for button in row)


@pytest.mark.parametrize("kind", ["help.chat", "help.custom_agent"])
def test_help_web_navigation_keeps_its_nonconsuming_action(kind):
    link = SimpleNamespace(id="b" * 32)
    keyboard = KEYBOARDS[kind](link=link)
    assert len(keyboard) == len(keyboard[0]) == 1
    assert keyboard[0][0] == {"label": "label.web_list", "kind": "web_list", "target_kind": "link", "target_id": link.id, "payload": {}}


async def keyboard_environment(db, monkeypatch):
    from app.telegram_bots.callbacks import CallbackRepository

    live = await install_live_owner(db, monkeypatch)
    link = await bind_link(db, await add_link(db, owner=live.id))
    owner = await live.authority.prepare_live_owner(live.id)
    guard = db.links.publication_guard(link, check_live_owner=owner, check_attempt_current=lambda: True)
    return link, CallbackRepository(db.sessions, config=db.config), guard


@pytest.mark.asyncio
async def test_keyboard_source_replay_keeps_original_random_controls_and_anchor(db, monkeypatch):
    link, callbacks, guard = await keyboard_environment(db, monkeypatch)
    outbox = Outbox(db.sessions)
    keyboard = KEYBOARDS["settings.state_basic"](link=link)
    part = {"kind": "text", "source_ref": {"key": "settings.state_basic", "values": {"language": "English"}}}
    identity = await outbox.plan_product_keyboard(link, source_id="language-controls", parts=[part], keyboard=keyboard, callbacks=callbacks, guard=guard)
    async with db.sessions() as session:
        original = await session.get(TelegramOutboxRow, identity)
        controls = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.link_id == link.id)))
        ids = {row.id for row in controls}
        assert len(ids) == 2 and all(len(value) == 32 and int(value, 16) >= 0 for value in ids)
        assert {row.group_id for row in controls} == {original.parts[-1]["callback_group_id"]}
        assert all(row.message_id is None and row.payload["anchor_outbox_id"] == identity and row.payload["presentation_version"] == 0 and not row.payload["presentation_pending"] for row in controls)
    assert await outbox.plan_product_keyboard(link, source_id="language-controls", parts=[part], keyboard=keyboard, callbacks=callbacks, guard=guard) == identity
    async with db.sessions() as session:
        assert set(await session.scalars(select(TelegramCallbackRow.id).where(TelegramCallbackRow.link_id == link.id))) == ids
    keyboard[0][0]["payload"]["value"] = "zh-CN"
    with pytest.raises(ValueError, match="product-source-conflict"):
        await outbox.plan_product_keyboard(link, source_id="language-controls", parts=[part], keyboard=keyboard, callbacks=callbacks, guard=guard)


@pytest.mark.asyncio
async def test_keyboard_and_parent_plan_rollback_together_in_borrowed_writer(db, monkeypatch):
    link, callbacks, guard = await keyboard_environment(db, monkeypatch)
    outbox = Outbox(db.sessions)
    async with db.sessions() as session, session.begin():
        identity = await outbox.plan_product_keyboard(
            link,
            source_id="rollback-controls",
            parts=[{"kind": "text", "source_ref": {"key": "settings.state_basic", "values": {"language": "English"}}}],
            keyboard=KEYBOARDS["settings.state_basic"](link=link),
            callbacks=callbacks,
            guard=guard,
            session=session,
        )
        assert await session.get(TelegramOutboxRow, identity) is not None
        assert len(list(await session.scalars(select(TelegramCallbackRow)))) == 2
        await session.rollback()
    async with db.sessions() as session:
        assert await session.get(TelegramOutboxRow, identity) is None
        assert list(await session.scalars(select(TelegramCallbackRow))) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("topic", [True, 1.0])
async def test_keyboard_general_topic_requires_a_literal_integer_before_any_mutation(db, monkeypatch, topic):
    link, callbacks, guard = await keyboard_environment(db, monkeypatch)
    async with db.sessions() as session:
        before = list(await session.scalars(select(TelegramOutboxRow.id)))
    with pytest.raises(ValueError, match="invalid-delivery-target"):
        await Outbox(db.sessions).plan_product_keyboard(
            link,
            source_id="invalid-topic",
            parts=[{"kind": "text", "source_ref": {"key": "settings.state_basic", "values": {"language": "English"}}}],
            keyboard=KEYBOARDS["settings.state_basic"](link=link),
            callbacks=callbacks,
            guard=guard,
            message_thread_id=topic,
        )
    async with db.sessions() as session:
        assert list(await session.scalars(select(TelegramOutboxRow.id))) == before
        assert list(await session.scalars(select(TelegramCallbackRow))) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("variant", ["multipart", "clear", "url"])
async def test_keyboard_variants_keep_controls_only_on_the_final_part(db, monkeypatch, variant):
    link, callbacks, guard = await keyboard_environment(db, monkeypatch)
    part = {"kind": "text", "source_ref": {"key": "settings.state_basic", "values": {"language": "English"}}}
    keyboard = [] if variant == "clear" else [[{"label": "label.open_telegram", "url": "https://product.example/workspace?settings=telegram"}]] if variant == "url" else KEYBOARDS["settings.state_basic"](link=link)
    identity = await Outbox(db.sessions).plan_product_keyboard(link, source_id="variant-controls", parts=[part, part] if variant == "multipart" else [part], keyboard=keyboard, callbacks=callbacks, guard=guard)
    async with db.sessions() as session:
        saved = await session.get(TelegramOutboxRow, identity)
        rows = list(await session.scalars(select(TelegramCallbackRow)))
        assert saved.parts[-1]["stored_keyboard"] is True
        assert all(not previous.get("stored_keyboard") and previous.get("callback_group_id") is None for previous in saved.parts[:-1])
        assert len(rows) == (2 if variant == "multipart" else 0)
        if variant == "clear":
            assert saved.payload["product_keyboard_basis"] == [] and saved.parts[-1]["callback_group_id"] is None
        else:
            assert saved.parts[-1]["callback_group_id"] is not None


@pytest.mark.asyncio
async def test_concurrent_same_source_allocates_one_complete_keyboard(db, monkeypatch):
    link, callbacks, guard = await keyboard_environment(db, monkeypatch)
    outbox = Outbox(db.sessions)

    async def plan():
        return await outbox.plan_product_keyboard(
            link,
            source_id="concurrent-controls",
            parts=[{"kind": "text", "source_ref": {"key": "settings.state_basic", "values": {"language": "English"}}}],
            keyboard=KEYBOARDS["settings.state_basic"](link=link),
            callbacks=callbacks,
            guard=guard,
        )

    identities = await asyncio.gather(plan(), plan(), plan())
    assert len(set(identities)) == 1
    async with db.sessions() as session:
        assert len(list(await session.scalars(select(TelegramCallbackRow)))) == 2
        assert len(list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:concurrent-controls")))) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("consume_during_render", [False, True])
async def test_actual_owned_sender_renders_stored_controls_and_records_acknowledged_anchor(db, monkeypatch, consume_during_render):
    from test_telegram_bot_handlers import ingress, sent

    from app.telegram_bots.callbacks import CallbackRepository
    from app.telegram_bots.producer import OutboxSender

    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    sender = None
    try:
        callbacks = e.services.callbacks = CallbackRepository(db.sessions, config=db.config)
        e.services.eligibility = e.live.authority.delivery
        link = e.channel.link
        guard = db.links.publication_guard(link, check_live_owner=await e.live.authority.prepare_live_owner(link.owner_user_id), check_attempt_current=lambda: e.active[0])
        identity = await e.services.outbox.plan_product_keyboard(
            link,
            source_id="actual-language-controls",
            parts=[{"kind": "text", "source_ref": {"key": "settings.state_basic", "values": {"language": "English"}}}],
            keyboard=KEYBOARDS["settings.state_basic"](link=link),
            callbacks=callbacks,
            guard=guard,
        )
        async with db.sessions.begin() as session:
            for row in await session.scalars(select(TelegramOutboxRow)):
                if row.id != identity:
                    row.state, row.skip_reason = "skipped", "superseded"
        if consume_during_render:
            async with db.sessions() as session:
                control = await session.scalar(select(TelegramCallbackRow))
                callback_id, group_id = control.id, control.group_id
            # An unknown previous send can be adopted before its outbox receipt.
            assert await callbacks.adopt(link, group_id, 501, chat_id=link.chat_id, message_thread_id=None, outbox_id=identity, presentation_version=0, check_current=lambda: True)
            render_source = e.channel._render_source

            async def consume_after_read(*args, **kwargs):
                result = await render_source(*args, **kwargs)
                async with db.sessions() as session, session.begin():
                    group = await callbacks.lock_current(link, callback_id, chat_id=link.chat_id, message_id=501, message_thread_id=None, guard=guard, session=session)
                    await callbacks.consume_group(group, reason="done", session=session)
                return result

            monkeypatch.setattr(e.channel, "_render_source", consume_after_read)
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        sender = OutboxSender(e.services.outbox, run_id="keyboard-sender", deliver_part=lambda **kw: e.channel.send_part(**{k: v for k, v in kw.items() if k != "link_id"}), eligibility=e.services.eligibility)
        await sender.tick()
        await sender.drain()
        calls = [call for call in e.request.calls if call.endpoint == "sendMessage"]
        if consume_during_render:
            assert not calls, "A consumed group must not reappear after awaited rendering"
            return
        assert len(calls) == 1
        buttons = calls[0].parameters["reply_markup"]["inline_keyboard"]
        assert [[button["text"] for button in row] for row in buttons] == [["English", "中文"]]
        async with db.sessions() as session:
            saved = await session.get(TelegramOutboxRow, identity)
            controls = list(await session.scalars(select(TelegramCallbackRow)))
            assert saved.state == "sent" and saved.parts[0]["message_ids"] == [501]
            assert {button["callback_data"] for row in buttons for button in row} == {"h1:" + row.id for row in controls}
            assert all(row.message_id == 501 and row.payload["presentation_version"] == 0 and row.payload["presentation_pending"] is False for row in controls)
    finally:
        if sender is not None:
            await sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("mixed", [False, True])
async def test_stored_url_refusal_keeps_same_plan_and_callbacks_for_address_fallback(db, monkeypatch, mixed):
    from test_telegram_bot_handlers import ingress, sent

    from app.telegram_bots.callbacks import CallbackRepository
    from app.telegram_bots.producer import OutboxSender

    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    sender = None
    try:
        callbacks = e.services.callbacks = CallbackRepository(db.sessions, config=db.config)
        e.services.eligibility = e.live.authority.delivery
        link = e.channel.link
        guard = db.links.publication_guard(link, check_live_owner=await e.live.authority.prepare_live_owner(link.owner_user_id), check_attempt_current=lambda: e.active[0])
        keyboard = [[{"label": "label.open_telegram", "url": "https://product.example/workspace?settings=telegram"}]]
        if mixed:
            keyboard += KEYBOARDS["settings.state_basic"](link=link)
        identity = await e.services.outbox.plan_product_keyboard(
            link,
            source_id="stored-url-fallback",
            parts=[{"kind": "text", "source_ref": {"key": "privacy.full", "values": {"product": "HartMesh"}}}],
            keyboard=keyboard,
            callbacks=callbacks,
            guard=guard,
        )
        async with db.sessions.begin() as session:
            for row in await session.scalars(select(TelegramOutboxRow)):
                if row.id != identity:
                    row.state, row.skip_reason = "skipped", "superseded"
            original = await session.get(TelegramOutboxRow, identity)
            original_part_id = original.parts[0]["part_id"]
            original_group = original.parts[0]["callback_group_id"]
            control_ids = set(await session.scalars(select(TelegramCallbackRow.id)))
        e.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "BUTTON_URL_INVALID"})
        sender = OutboxSender(e.services.outbox, run_id="stored-url-sender", deliver_part=lambda **kw: e.channel.send_part(**{k: v for k, v in kw.items() if k != "link_id"}), eligibility=e.services.eligibility)
        await sender.tick()
        await sender.drain()
        async with db.sessions() as session:
            pending = await session.get(TelegramOutboxRow, identity)
            assert pending.state == "pending" and pending.parts[0]["url_button_fallback"] is True
            assert pending.parts[0]["part_id"] == original_part_id and pending.parts[0]["callback_group_id"] == original_group
        e.clock.advance(2)
        e.request.enqueue("sendMessage", result=sent(e, identity=502))
        await sender.tick()
        await sender.drain()
        calls = [call for call in e.request.calls if call.endpoint == "sendMessage"]
        assert len(calls) == 2 and "https://product.example/" in calls[-1].parameters["text"]
        final_buttons = calls[-1].parameters.get("reply_markup", {}).get("inline_keyboard", [])
        assert not any(button.get("url") for line in final_buttons for button in line)
        assert {button["callback_data"] for line in final_buttons for button in line} == {"h1:" + value for value in control_ids}
        async with db.sessions() as session:
            complete = await session.get(TelegramOutboxRow, identity)
            assert complete.state == "sent" and complete.parts[0]["message_ids"] == [502]
            assert set(await session.scalars(select(TelegramCallbackRow.id))) == control_ids
    finally:
        if sender is not None:
            await sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_delivery_repair_can_rollback_with_its_callback_writer(db, monkeypatch):
    link, callbacks, guard = await keyboard_environment(db, monkeypatch)
    outbox = Outbox(db.sessions)
    identity = await outbox.plan_product_keyboard(
        link,
        source_id="repair-target",
        parts=[{"kind": "text", "source_ref": {"key": "settings.state_basic", "values": {"language": "English"}}}],
        keyboard=KEYBOARDS["settings.state_basic"](link=link),
        callbacks=callbacks,
        guard=guard,
    )
    async with db.sessions.begin() as session:
        (await session.get(TelegramOutboxRow, identity)).state = "failed"
    async with db.sessions() as session, session.begin():
        await outbox.retry(link, identity, session=session)
        assert (await session.get(TelegramOutboxRow, identity)).state == "pending"
        await session.rollback()
    async with db.sessions() as session:
        assert (await session.get(TelegramOutboxRow, identity)).state == "failed"
