"""Actual failure receipts, raw pinned SDK delivery and saved repair callbacks."""

import asyncio

import pytest
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db  # noqa: F401
from sqlalchemy import select, text, update
from test_telegram_bot_handlers import ingress, sent
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging  # noqa: F401

from app.telegram_bots.callbacks import CallbackRefused, CallbackRepository
from app.telegram_bots.commands import CallbackCommands
from app.telegram_bots.producer import Outbox, OutboxSender
from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("topic", [None, 99])
async def test_actual_failure_notice_renders_retry_and_repairs_only_unacknowledged_parts(db, monkeypatch, topic):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    sender = None
    try:
        link = e.channel.link
        e.services.callbacks = callbacks = CallbackRepository(db.sessions, config=db.config, clock=e.clock.now)
        e.services.eligibility = e.live.authority.delivery
        commands = CallbackCommands(e.services)
        identity = await e.services.outbox.plan_product(
            link,
            source_id="actual-repair-target",
            parts=[{"kind": "text", "source_ref": {"key": "tap.failed", "values": {}}} for _ in range(2)],
            message_thread_id=topic,
        )
        async with db.sessions.begin() as session:
            for row in await session.scalars(select(TelegramOutboxRow)):
                if row.id != identity:
                    row.state, row.skip_reason = "skipped", "superseded"
        sender = OutboxSender(e.services.outbox, run_id="actual-repair", deliver_part=lambda **kw: e.channel.send_part(**{key: value for key, value in kw.items() if key != "link_id"}), eligibility=e.services.eligibility)
        e.request.enqueue("sendMessage", result=sent(e, identity=500))
        assert await sender.tick() == 1
        await sender.drain()
        e.clock.advance(2)
        e.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "MESSAGE_TOO_LONG"})
        assert await sender.tick() == 1
        await sender.drain()
        async with db.sessions() as session:
            target = await session.get(TelegramOutboxRow, identity)
            assert target.state == "failed" and target.parts[0]["message_ids"] == [500]
            original_part_ids = [part["part_id"] for part in target.parts]
            notice = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == f"delivery-failed:{identity}:1"))
            assert notice is not None and notice.message_thread_id == topic
            notice_id = notice.id
        e.clock.advance(2)
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await sender.tick() == 1
        await sender.drain()
        calls = [call for call in e.request.calls if call.endpoint == "sendMessage"]
        assert len(calls) == 3, "The saved repair notice must reach the real SDK once"
        button = calls[-1].parameters["reply_markup"]["inline_keyboard"][0][0]
        assert button["text"] == "Retry delivery"
        callback_id = button["callback_data"].removeprefix("h1:")
        selected = await callbacks.get(callback_id)
        assert selected.kind == "delivery_retry" and selected.target_id == identity
        assert selected.message_id == 501 and selected.message_thread_id == topic
        assert selected.payload["anchor_outbox_id"] == notice_id
        assert selected.payload["outbox_id"] == identity and selected.payload["presentation_version"] == 0
        async with db.sessions() as session:
            before = set(await session.scalars(select(TelegramCallbackRow.id)))
            assert (await session.get(TelegramOutboxRow, identity)).state == "failed"
        await commands.execute(e.channel, selected, query_id="recorded-retry")
        assert (await callbacks.get(callback_id)).consumed_at is None
        e.clock.advance(2)
        e.request.enqueue("sendMessage", result=sent(e, identity=502))
        assert await sender.tick() == 1
        await sender.drain()
        async with db.sessions() as session:
            target = await session.get(TelegramOutboxRow, identity)
            assert target.state == "sent" and [part["part_id"] for part in target.parts] == original_part_ids
            assert [part["message_ids"] for part in target.parts] == [[500], [502]]
            assert set(await session.scalars(select(TelegramCallbackRow.id))) == before
            cleanup = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == f"notice:repair-cleanup:{identity}:{selected.group_id}"))
            assert cleanup is not None and cleanup.message_thread_id == topic
            assert cleanup.parts[0]["target_message_id"] == 501 and cleanup.parts[0]["remove_keyboard"] is True
        assert (await callbacks.get(callback_id)).closed_reason == "done"
        e.clock.advance(2)
        e.request.enqueue("editMessageReplyMarkup", result=sent(e, identity=501))
        assert await sender.tick() == 1
        await sender.drain()
        cleanup_calls = [call for call in e.request.calls if call.endpoint == "editMessageReplyMarkup"]
        assert len(cleanup_calls) == 1 and cleanup_calls[0].parameters["message_id"] == 501
        # The pinned SDK omits None for its existing keyboard-removal method.
        assert cleanup_calls[0].parameters.get("reply_markup") is None
        assert len([call for call in e.request.calls if call.endpoint == "sendMessage"]) == 4
    finally:
        if sender is not None:
            await sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["conflict", "generation", "account"])
async def test_inflight_permanent_failure_preserves_truth_and_only_same_custody_repair(db, monkeypatch, change):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    sender, release = None, asyncio.Event()
    try:
        link = e.channel.link
        e.services.callbacks = CallbackRepository(db.sessions, config=db.config, clock=e.clock.now)
        e.services.eligibility = e.live.authority.delivery
        identity = await e.services.outbox.plan_product(link, source_id="inflight-repair", parts=[{"kind": "text", "source_ref": {"key": "tap.failed", "values": {}}}])
        async with db.sessions.begin() as session:
            for row in await session.scalars(select(TelegramOutboxRow)):
                if row.id != identity:
                    row.state, row.skip_reason = "skipped", "superseded"
        response = e.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "MESSAGE_TOO_LONG"}, release=release)
        sender = OutboxSender(e.services.outbox, run_id="changed-repair", deliver_part=lambda **kw: e.channel.send_part(**{key: value for key, value in kw.items() if key != "link_id"}), eligibility=e.services.eligibility)
        assert await sender.tick() == 1
        await asyncio.wait_for(response.started.wait(), 3)
        if change == "conflict":
            await db.links.set_health(link.id, "conflict", holds=True)
        else:
            # Controlled SQL replacement of persisted custody while the real
            # SDK response is held; no lifecycle cleanup/result is fabricated.
            async with db.sessions.begin() as session:
                if change == "generation":
                    await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(binding_generation=link.binding_generation + 1))
                else:
                    await session.execute(update(TelegramAccountRow).where(TelegramAccountRow.telegram_user_id == link.telegram_user_id).values(owner_user_id="different-current-owner"))
        release.set()
        await sender.drain()
        async with db.sessions() as session:
            failed = await session.get(TelegramOutboxRow, identity)
            assert failed.state == "failed" and failed.failed_reason == "invalid-payload"
            assert failed.payload["failure_version"] == 1
            notices = list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["kind"].as_string() == "delivery_failure_notice")))
            controls = list(await session.scalars(select(TelegramCallbackRow)))
            if change != "conflict":
                assert notices == controls == []
                return
            assert len(notices) == len(controls) == 1
            assert notices[0].parts[0]["stored_keyboard"] is True
            assert controls[0].target_id == identity and controls[0].payload["anchor_outbox_id"] == notices[0].id
        e.clock.advance(2)
        assert await sender.tick() == 0, "Recording repair controls grants no unhealthy delivery permission"
        assert len([call for call in e.request.calls if call.endpoint == "sendMessage"]) == 1
        await db.links.set_health(link.id, "conflict", holds=False)
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await sender.tick() == 1
        await sender.drain()
        calls = [call for call in e.request.calls if call.endpoint == "sendMessage"]
        assert len(calls) == 2 and calls[-1].parameters["reply_markup"]["inline_keyboard"][0][0]["text"] == "Retry delivery"
    finally:
        release.set()
        if sender is not None:
            await sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["boolean_version", "numeric_finish", "target_pending"])
async def test_purpose_specific_repair_writer_refuses_noncanonical_or_unfailed_receipt(db, change):
    """Protocol SQL seeds exercise refusal; no transport-failure provenance claim."""
    link = await bind_link(db, await add_link(db))
    outbox, callbacks = Outbox(db.sessions), CallbackRepository(db.sessions, config=db.config)
    source = {"kind": "text", "source_ref": {"key": "delivery.failed", "values": {}}}
    target = await outbox.plan_product(link, source_id="repair-shape-target", parts=[source])
    async with db.sessions.begin() as session:
        row = await session.get(TelegramOutboxRow, target)
        row.state, row.payload = "failed", {**row.payload, "failure_version": 1}
    payload = {
        "kind": "delivery_failure_notice",
        "repair_outbox_id": target,
        "product_basis": {"origins": [], "chat_id": link.chat_id, "message_thread_id": None, "finish_route": False, "parts": [source]},
        "parts": [{**source, "stored_keyboard": True}],
    }
    assert await outbox.add(link, dedupe_key=f"delivery-failed:{target}:1", class_="reply", payload=payload)
    async with db.sessions.begin() as session:
        parent = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == f"delivery-failed:{target}:1"))
        identity = parent.id
        altered = dict(parent.payload)
        if change == "boolean_version":
            altered["presentation_version"] = False
        elif change == "numeric_finish":
            altered["product_basis"] = {**altered["product_basis"], "finish_route": 0}
        else:
            (await session.get(TelegramOutboxRow, target)).state = "pending"
        # Core UPDATE persists bool/int JSON distinctions even when Python's
        # normal mutable-value equality would otherwise skip the fixture write.
        await session.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == identity).values(payload=altered))
    with pytest.raises(CallbackRefused):
        async with db.sessions() as session, session.begin():
            if db.dialect == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            await callbacks.write_repair_group(link, failed_outbox_id=target, anchor_outbox_id=identity, session=session)
    async with db.sessions() as session:
        assert list(await session.scalars(select(TelegramCallbackRow))) == []
