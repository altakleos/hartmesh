"""Managed ingress runs the real pinned Application over SQL and recorded SDK."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _telegram_bot_test_support import update_payload
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import add_link, code_of, rows
from _telegram_repository_test_support import db as db
from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError
from telegram import Update
from test_telegram_bot_adapter import build, start
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.channels.telegram import TelegramChannel
from app.telegram_bots.authority import AccessOff
from app.telegram_bots.producer import OutboxSender
from app.telegram_bots.render import render, render_plain
from app.telegram_bots.strangers import StrangerLedger
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio


async def ingress(db, *, connected=True, monkeypatch=None, lead=False, off_loop=False):
    live = await install_live_owner(db, monkeypatch) if monkeypatch is not None else None
    owner = live.id if live is not None else "owner-a"
    if lead:
        await db.settings.ensure(owner, timezone="America/New_York", locale="en-US")
        link = await db.links.add(owner_user_id=owner, agent_kind="lead_agent", agent_ref="lead_agent", bot_id=9_000_000_001, bot_username="fixture_bot", encrypted_token="wrapped-fixture", actor_id=owner)
        if connected:
            code = code_of(await db.links.mint_code(link))
            link = await db.links.bind(
                link,
                telegram_user_id=7_000_000_001,
                chat_id=7_000_000_001,
                first_name="Fixture",
                username=None,
                code=code,
                start_identity={"update_id": 201, "message_id": 101, "chat_id": 7_000_000_001},
                hello_parts=[{"source_ref": {"key": "hello.chat", "values": {"product": "HartMesh", "email": "fixture@example.test"}}}, {"source_ref": {"key": "hello.privacy", "values": {}}}],
            )
    else:
        link = await add_link(db, owner=owner)
    e = await build(db, connected=connected and not lead, link=link, construct_off_loop=off_loop)
    e.live = live
    e.active, e.touched, e.checks = [True], [], []
    e.services.send_allowed = lambda: e.active[0]
    e.services.strangers = StrangerLedger()

    class OwnerCheck:
        email = "owner@example.test"

        async def __call__(self, owner, writer):
            e.checks.append(owner)
            await writer.execute(text("SELECT 1"))

    async def prepare_owner(owner):
        return OwnerCheck()

    async def values(link):
        return {"product": "HartMesh", "name": "Fixture target", "agent": "Fixture target", "memory_url": None}

    e.services.prepare_live_owner, e.services.prepare_ingress_values = live.authority.prepare_live_owner if live else prepare_owner, values
    e.services.product_name = "HartMesh"
    await start(e)
    return e


def message(e, identity, *, content="/agent", sender=None, chat=None, kind="private", edited=False, caption=False, entity=True, mid=None):
    sender = sender or e.link.telegram_user_id or 7_000_000_001
    chat = chat or sender
    payload = update_payload(identity, message_id=mid or identity, account=sender, text=content)
    payload["message"]["chat"]["id"] = chat
    payload["message"]["chat"]["type"] = kind
    if caption:
        payload["message"]["caption"] = payload["message"].pop("text")
    elif entity and content.startswith("/"):
        command = content.split()[0].split("-")[0]
        payload["message"]["entities"] = [{"type": "bot_command", "offset": 0, "length": len(command)}]
    if edited:
        payload["edited_message"] = payload.pop("message")
    return Update.de_json(payload, e.channel.bot)


def sent(e, *, identity=500, chat=None):
    return {"message_id": identity, "date": 1_893_456_000, "chat": {"id": chat or e.link.chat_id or 7_000_000_001, "type": "private"}, "text": "Fixed product reply"}


async def drain_disposable(e):
    await asyncio.gather(*tuple(e.channel._ingress_tasks))


async def test_group_message_is_classified_without_storage_or_reply(db):
    e = await ingress(db)
    try:
        before = len(await rows(db, TelegramInboundMarkRow))
        assert await e.channel.handle_update(message(e, 301, content="Synthetic group input", chat=-100123, kind="supergroup")) is True
        assert len(await rows(db, TelegramInboundMarkRow)) == before
        assert [call.endpoint for call in e.request.calls] == ["getMe"]
    finally:
        await e.channel.stop()


async def test_stranger_is_classified_and_replies_through_this_bot_without_inbox(db):
    e = await ingress(db)
    try:
        e.request.enqueue("sendMessage", result=sent(e, chat=7_000_000_003))
        before = len(await rows(db, TelegramInboundMarkRow))
        assert await e.channel.handle_update(message(e, 302, content="Synthetic stranger input", sender=7_000_000_003)) is True
        await drain_disposable(e)
        assert len(await rows(db, TelegramInboundMarkRow)) == before
        call = e.request.calls[-1]
        assert call.endpoint == "sendMessage" and call.parameters["chat_id"] == 7_000_000_003
        assert "HartMesh" not in call.parameters["text"] and "Fixture target" not in call.parameters["text"]
        assert call.parameters["link_preview_options"]["is_disabled"] is True
    finally:
        await e.channel.stop()


@pytest.mark.parametrize("caption,edited", [(False, False), (True, False), (False, True)])
async def test_token_refusal_is_classified_without_inbox_or_hash_retention(db, caption, edited):
    import secrets

    e = await ingress(db)
    secret = "123456:" + secrets.token_urlsafe(30)
    try:
        e.request.enqueue("deleteMessage", result=True)
        e.request.enqueue("sendMessage", result=sent(e))
        before = len(await rows(db, TelegramInboundMarkRow))
        assert await e.channel.handle_update(message(e, 303, content="/start https://api.telegram.org/bot" + secret + "/getMe", caption=caption, edited=edited)) is True
        await drain_disposable(e)
        assert len(await rows(db, TelegramInboundMarkRow)) == before
        assert not e.channel._edit_hashes
        assert [call.endpoint for call in e.request.calls] == ["getMe", "deleteMessage", "sendMessage"]
        assert all(secret not in str(call.parameters) for call in e.request.calls)
    finally:
        await e.channel.stop()


async def test_fixed_command_is_durable_sealed_and_replayed_without_new_delivery(db):
    e = await ingress(db)
    sender = None
    try:
        update = message(e, 304)
        assert await e.channel.handle_update(update) is True
        assert await e.channel.handle_update(update) is True
        deliveries = [row for row in await rows(db, TelegramOutboxRow) if row.payload.get("product_basis")]
        assert len(deliveries) == 1
        assert deliveries[0].parts[0]["source_ref"]["key"] == "cmd.agent"
        mark = next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == 304)
        assert mark.route_kind == "command" and mark.route_complete and mark.delivery_ids == [deliveries[0].id]
        e.request.enqueue("sendMessage", result=sent(e))
        # Retire the setup-only welcome; this test sends the actual command plan.
        async with db.sessions() as session, session.begin():
            for row in await session.scalars(select(TelegramOutboxRow)):
                if row.id != deliveries[0].id:
                    row.state, row.skip_reason = "skipped", "superseded"
        sender = OutboxSender(e.services.outbox, run_id="ingress-test", deliver_part=lambda **kwargs: e.channel.send_part(**{key: value for key, value in kwargs.items() if key != "link_id"}), eligibility=e.services.eligibility)
        await sender.tick()
        await sender.drain()
        assert e.request.calls[-1].endpoint == "sendMessage"
        assert "Fixture target" in e.request.calls[-1].parameters["text"]
        assert (next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == 304)).state == "done"
    finally:
        if sender is not None:
            await sender.stop()
        await e.channel.stop()


async def test_a_handler_failure_swallowed_by_ptb_reaches_the_poller(db, monkeypatch):
    e = await ingress(db)
    try:
        original = e.services.outbox.plan_product

        async def fail(*args, **kwargs):
            raise OperationalError("fixture", {}, RuntimeError("synthetic SQL failure"))

        monkeypatch.setattr(e.services.outbox, "plan_product", fail)
        with pytest.raises(OperationalError):
            await e.channel.handle_update(message(e, 305))
        monkeypatch.setattr(e.services.outbox, "plan_product", original)
        assert await e.channel.handle_update(message(e, 305)) is True
        assert await e.channel.handle_update(message(e, 306, content="Ordinary unfinished input")) is False
    finally:
        await e.channel.stop()


def membership(e, identity, *, status, chat=None, kind="private", old="member"):
    return Update.de_json(
        {
            "update_id": identity,
            "my_chat_member": {
                "chat": {"id": chat or e.link.chat_id, "type": kind, "title": "Fixture group"},
                "from": {"id": e.link.telegram_user_id or 7_000_000_001, "first_name": "Fixture", "is_bot": False},
                "date": 1_893_456_000,
                "old_chat_member": {"user": {"id": e.link.bot_id, "first_name": "Fixture bot", "is_bot": True}, "status": old, **({"until_date": 0} if old == "kicked" else {})},
                "new_chat_member": {"user": {"id": e.link.bot_id, "first_name": "Fixture bot", "is_bot": True}, "status": status, **({"until_date": 0} if status == "kicked" else {})},
            },
        },
        e.channel.bot,
    )


async def command_sender(db, e):
    async with db.sessions() as session, session.begin():
        for row in await session.scalars(select(TelegramOutboxRow)):
            if row.payload.get("kind") == "welcome":
                row.state, row.skip_reason = "skipped", "superseded"
    sender = OutboxSender(e.services.outbox, run_id="ingress-test", deliver_part=lambda **kwargs: e.channel.send_part(**{key: value for key, value in kwargs.items() if key != "link_id"}), eligibility=e.services.eligibility)
    return sender


async def test_group_addition_leaves_without_a_chat_reply(db):
    e = await ingress(db)
    try:
        e.request.enqueue("leaveChat", result=True)
        assert await e.channel.handle_update(membership(e, 401, status="member", chat=-100123, kind="supergroup", old="left")) is True
        await drain_disposable(e)
        assert [call.endpoint for call in e.request.calls] == ["getMe", "leaveChat"]
        assert e.request.calls[-1].parameters["chat_id"] == -100123
        assert (await db.links.get(e.link.id)).state == "connected"
    finally:
        await e.channel.stop()


@pytest.mark.parametrize("data,stranger,inline", [("unknown", False, False), ("h1:" + "a" * 32, False, False), ("unknown", True, False), ("unknown", False, True)])
async def test_callback_alert_never_acquires_business_authority(db, data, stranger, inline):
    e = await ingress(db)
    try:
        sender = 7_000_000_003 if stranger else e.link.telegram_user_id
        query = {"id": "fixture-query", "from": {"id": sender, "first_name": "Fixture", "is_bot": False}, "chat_instance": "fixture-instance", "data": data}
        if inline:
            query["inline_message_id"] = "fixture-inline"
        else:
            query["message"] = {"message_id": 101, "date": 0, "chat": {"id": e.link.chat_id, "type": "private"}}
        e.request.enqueue("answerCallbackQuery", result=True)
        before = len(await rows(db, TelegramInboundMarkRow))
        assert await e.channel.handle_update(Update.de_json({"update_id": 402, "callback_query": query}, e.channel.bot)) is True
        await drain_disposable(e)
        assert len(await rows(db, TelegramInboundMarkRow)) == before
        call = e.request.calls[-1]
        assert call.endpoint == "answerCallbackQuery" and call.parameters["show_alert"] is True
        assert call.parameters["text"] == render_plain("stale.other_person" if stranger or inline else "stale.not_waiting", "en-US")
    finally:
        await e.channel.stop()


async def test_all_fixed_words_are_durable_and_literal_hyphenated_words_remain_unfinished(db):
    from app.telegram_bots.adapter import FIXED_COMMANDS

    e = await ingress(db)
    try:
        for identity, word in enumerate(FIXED_COMMANDS, start=410):
            assert await e.channel.handle_update(message(e, identity, content="/" + word, entity=False)) is True
        plans = [row for row in await rows(db, TelegramOutboxRow) if row.payload.get("product_basis")]
        assert len(plans) == len(FIXED_COMMANDS)
        assert {row.parts[0]["source_ref"]["key"] for row in plans} == {"cmd.agent", "cmd.unknown"}
        for identity, word in enumerate(["/start-up words", "/web-research words", "/agent@another_bot", "/agent@fixture_bot"], start=450):
            assert await e.channel.handle_update(message(e, identity, content=word)) is True
        assert all(row.route_kind == "command" for row in await rows(db, TelegramInboundMarkRow))
    finally:
        await e.channel.stop()


def _skill_turns(accept):
    async def retire(*args, **kwargs):
        return None

    return SimpleNamespace(accept=accept, retire=retire)


def _skill_replies(plans):
    return [(row.parts[0]["source_ref"]["key"], row.parts[0]["source_ref"]["values"]) for row in plans if row.payload.get("product_basis")]


async def test_resolved_skill_word_is_published_as_chat_with_text_unchanged(db, monkeypatch):
    import app.telegram_bots.adapter as adapter

    seen = []
    monkeypatch.setattr(adapter, "slash_skill_outcome", lambda link, text: seen.append((link.id, text)) or "run")
    e = await ingress(db)
    accepted = []

    async def accept(channel, update, *, separate=False):
        accepted.append((update.message.text, separate))
        channel._classified(update)

    e.services.turns = _skill_turns(accept)
    try:
        before = len(await rows(db, TelegramOutboxRow))
        assert await e.channel.handle_update(message(e, 470, content="/weekly-report for March")) is True
        assert accepted == [("/weekly-report for March", True)]
        assert seen == [(e.link.id, "/weekly-report for March")]
        assert len(await rows(db, TelegramOutboxRow)) == before
    finally:
        await e.channel.stop()


async def test_a_skill_word_that_begins_like_a_command_is_a_skill(db, monkeypatch):
    import app.telegram_bots.adapter as adapter

    monkeypatch.setattr(adapter, "slash_skill_outcome", lambda link, text: "run")
    e = await ingress(db)
    accepted, closed = [], []

    async def accept(channel, update, *, separate=False):
        accepted.append(update.message.text)
        channel._classified(update)

    async def command(*args, **kwargs):
        closed.append("command")

    async def handle_text(*args, **kwargs):
        closed.append("text")
        return True

    e.services.turns = _skill_turns(accept)
    e.services.human_inputs = SimpleNamespace(command=command, handle_text=handle_text)
    try:
        assert await e.channel.handle_update(message(e, 473, content="/web-design-guidelines review this")) is True
        assert accepted == ["/web-design-guidelines review this"]
        assert closed == [], "a skill word neither closes nor answers an open prompt"
    finally:
        await e.channel.stop()


@pytest.mark.parametrize(("outcome", "key", "values"), [("off", "cmd.skill_off", {"skill": "weekly-report"}), ("unavailable", "cmd.skill_unavailable", {"skill": "weekly-report"}), ("failed", "error.generic", {})])
async def test_skill_that_cannot_run_says_why_and_is_not_published(db, monkeypatch, outcome, key, values):
    import app.telegram_bots.adapter as adapter

    monkeypatch.setattr(adapter, "slash_skill_outcome", lambda link, text: outcome)
    e = await ingress(db)
    accepted = []

    async def accept(channel, update, *, separate=False):
        accepted.append(update.message.text)

    e.services.turns = _skill_turns(accept)
    try:
        assert await e.channel.handle_update(message(e, 471, content="/weekly-report for March")) is True
        assert accepted == []
        assert _skill_replies(await rows(db, TelegramOutboxRow)) == [(key, values)]
    finally:
        await e.channel.stop()


async def test_unknown_or_reserved_slash_word_gets_cmd_unknown_and_is_not_published(db, monkeypatch):
    import app.telegram_bots.adapter as adapter

    monkeypatch.setattr(adapter, "slash_skill_outcome", lambda link, text: None)
    e = await ingress(db)
    accepted = []

    async def accept(channel, update, *, separate=False):
        accepted.append(update.message.text)

    e.services.turns = _skill_turns(accept)
    try:
        assert await e.channel.handle_update(message(e, 472, content="/no-such-skill words")) is True
        assert accepted == []
        assert _skill_replies(await rows(db, TelegramOutboxRow)) == [("cmd.unknown", {"command": "no-such-skill"})]
    finally:
        await e.channel.stop()


async def test_a_path_pasted_as_the_first_word_is_answered_and_stops_nothing(db, monkeypatch):
    import app.telegram_bots.adapter as adapter

    monkeypatch.setattr(adapter, "slash_skill_outcome", lambda link, text: None)
    e = await ingress(db)
    e.services.turns = _skill_turns(AsyncMock())
    path = "/users/maria/documents/invoices/2026/october/invoice-4711-final-signed-copy-of-it.pdf"
    assert len(path) > 65
    try:
        assert await e.channel.handle_update(message(e, 473, content=path + " what is the total?")) is True
        assert _skill_replies(await rows(db, TelegramOutboxRow)) == [("cmd.unknown", {"command": path[1:]})]
    finally:
        await e.channel.stop()


async def test_a_long_number_as_the_first_word_is_answered_all_the_way_to_a_reply(db, monkeypatch):
    import app.telegram_bots.adapter as adapter

    monkeypatch.setattr(adapter, "slash_skill_outcome", lambda link, text: None)
    e = await ingress(db)
    e.services.turns = _skill_turns(AsyncMock())
    word = "1762000000000:" + "a" * 30
    try:
        assert await e.channel.handle_update(message(e, 475, content="/" + word)) is True
        assert _skill_replies(await rows(db, TelegramOutboxRow)) == [("cmd.unknown", {"command": word})]
    finally:
        await e.channel.stop()


async def test_a_long_number_beside_a_hash_is_a_message_and_a_token_still_is_not(db):
    e = await ingress(db)
    inbox = e.services.outbox.inbox
    try:
        timestamp = "1762000000000:" + "a" * 30
        row, created = await inbox.accept(e.link, message(e, 476, content=timestamp), sanitized_input={"text": timestamp})
        assert created and row.sanitized_input["text"] == timestamp
        token = "123456789012:" + "a" * 30
        with pytest.raises(ValueError, match="unsafe-sanitized-input"):
            await inbox.accept(e.link, message(e, 477, content=token), sanitized_input={"text": token})
    finally:
        await e.channel.stop()


@pytest.mark.parametrize("name,expected", [("data: notes.xlsx", "data_ notes.xlsx"), ("DATA:NOTES.PDF", "DATA_NOTES.PDF"), ("plain.pdf", "plain.pdf")])
async def test_a_colon_leaves_a_file_name_and_its_extension_stays(name, expected):
    assert TelegramChannel._safe_inbound_filename(name, "fallback") == expected


async def test_an_update_that_is_let_go_is_logged_and_its_sender_told(db, caplog):
    e = await ingress(db)
    try:
        e.request.enqueue("sendMessage", result=sent(e))
        await e.channel.refuse_update(message(e, 478, content="Unreadable"), ValueError("invalid-inbox-route"))
        await drain_disposable(e)
        call = e.request.calls[-1]
        assert call.endpoint == "sendMessage" and call.parameters["text"] == render_plain("update.refused", "en-US") and call.parameters["reply_parameters"]["message_id"] == 478
        assert f"update_skipped link={e.link.id} bot={e.link.bot_id} update=478 cause=ValueError:invalid-inbox-route" in caplog.text
    finally:
        await e.channel.stop()


async def test_a_person_who_is_not_bound_yet_is_told_too(db):
    e = await ingress(db, connected=False)
    try:
        e.request.enqueue("sendMessage", result=sent(e, chat=7_000_000_003))
        await e.channel.refuse_update(message(e, 479, content="/start code", sender=7_000_000_003), ValueError("private"))
        await drain_disposable(e)
        call = e.request.calls[-1]
        assert call.endpoint == "sendMessage" and call.parameters["chat_id"] == 7_000_000_003
    finally:
        await e.channel.stop()


async def test_an_update_with_no_private_message_answers_nobody(db):
    e = await ingress(db)
    try:
        before = len(e.request.calls)
        await e.channel.refuse_update(membership(e, 480, status="member"), ValueError("private"))
        await e.channel.refuse_update(message(e, 481, content="Unreadable", chat=-100, kind="group"), ValueError("private"))
        await drain_disposable(e)
        assert len(e.request.calls) == before
    finally:
        await e.channel.stop()


async def deleted_agents_bot(db):
    e = await ingress(db)
    e.now = [1000.0]
    e.channel._time = lambda: e.now[0]
    e.stopped = AsyncMock(side_effect=lambda channel, update: channel._classified(update, remember=True))
    e.running = False
    e.services.turns = SimpleNamespace(stop_chat=e.stopped, retire=AsyncMock(), message_sent=lambda *_args, **_kwargs: None, has_turn=lambda _channel, *, topic: e.running)
    await db.links.set_health(e.link.id, "agent_unavailable", holds=True, expected_token_revision=e.link.token_revision)
    e.answers = lambda: [call.parameters["text"] for call in e.request.calls if call.endpoint == "sendMessage"]
    e.gone = render_plain("custom_agent.deleted", "en-US", agent=e.link.agent_ref, product="HartMesh")
    return e


async def test_a_deleted_agents_bot_says_so_once_an_hour_and_keeps_nothing(db):
    e = await deleted_agents_bot(db)
    try:
        kept = len(await rows(db, TelegramInboundMarkRow))
        e.request.enqueue("sendMessage", result=sent(e))
        for number, content in enumerate(("Hello", "/help", "/start again")):
            assert await e.channel.handle_update(message(e, 481 + number, content=content)) is True
        await drain_disposable(e)
        assert e.answers() == [e.gone]
        assert len(await rows(db, TelegramInboundMarkRow)) == kept, "Nothing can answer these, so none is kept"

        e.now[0] += 3600
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await e.channel.handle_update(message(e, 485, content="Hello again")) is True
        await drain_disposable(e)
        assert e.answers() == [e.gone, e.gone]
    finally:
        await e.channel.stop()


async def test_a_notice_that_could_not_be_sent_does_not_spend_the_hour(db):
    e = await deleted_agents_bot(db)
    try:
        e.request.enqueue("sendMessage", status=500, body={"ok": False, "error_code": 500, "description": "Internal"})
        assert await e.channel.handle_update(message(e, 486, content="Hello")) is True
        await drain_disposable(e)
        e.request.enqueue("sendMessage", result=sent(e))
        assert await e.channel.handle_update(message(e, 487, content="Hello again")) is True
        await drain_disposable(e)
        assert e.answers()[-1] == e.gone and len(e.answers()) == 2
    finally:
        await e.channel.stop()


async def test_stop_reaches_a_running_turn_on_a_deleted_agents_bot(db):
    e = await deleted_agents_bot(db)
    e.running = True
    try:
        assert await e.channel.handle_update(message(e, 488, content="/stop")) is True
        e.stopped.assert_awaited_once()
        assert e.answers() == []
    finally:
        await e.channel.stop()


async def test_stop_with_nothing_running_gets_the_answer_a_deleted_agents_bot_has(db):
    e = await deleted_agents_bot(db)
    try:
        e.request.enqueue("sendMessage", result=sent(e))
        assert await e.channel.handle_update(message(e, 489, content="/stop")) is True
        await drain_disposable(e)
        e.stopped.assert_not_awaited()
        assert e.answers() == [e.gone]
    finally:
        await e.channel.stop()


async def test_a_token_pasted_to_a_deleted_agents_bot_is_still_removed(db):
    e = await deleted_agents_bot(db)
    try:
        e.request.enqueue("deleteMessage", result=True)
        e.request.enqueue("sendMessage", result=sent(e))
        assert await e.channel.handle_update(message(e, 490, content="123456789:" + "A" * 35)) is True
        await drain_disposable(e)
        assert "deleteMessage" in [call.endpoint for call in e.request.calls]
    finally:
        await e.channel.stop()


async def test_a_lead_agent_link_never_says_an_agent_was_deleted(db, monkeypatch):
    e = await ingress(db, lead=True, monkeypatch=monkeypatch)
    await db.links.set_health(e.link.id, "agent_unavailable", holds=True, expected_token_revision=e.link.token_revision)
    accept = AsyncMock()
    e.services.turns = SimpleNamespace(**vars(_skill_turns(accept)), has_turn=lambda _channel, *, topic: False)
    try:
        await e.channel.handle_update(message(e, 491, content="Hello"))
        await drain_disposable(e)
        accept.assert_awaited_once()
        assert not [call for call in e.request.calls if call.endpoint == "sendMessage"]
    finally:
        await e.channel.stop()


async def test_live_owner_bind_commits_only_minimal_start_and_replays_welcome(db, monkeypatch):
    e = await ingress(db, connected=False, monkeypatch=monkeypatch, lead=True)
    code = code_of(await db.links.mint_code(e.link))
    try:
        update = message(e, 470, content="/start " + code)
        assert await e.channel.handle_update(update) is True
        assert await e.channel.handle_update(update) is True
        link = await db.links.get(e.link.id)
        assert link.state == "connected" and link.chat_id == 7_000_000_001
        marks, plans = await rows(db, TelegramInboundMarkRow), await rows(db, TelegramOutboxRow)
        assert len(marks) == len(plans) == 1
        assert marks[0].sanitized_input == {"command": "start"} and marks[0].route_complete
        assert [part["source_ref"]["key"] for part in plans[0].parts] == ["hello.chat", "hello.privacy"]
        assert plans[0].parts[-1]["silent"] is True
        assert code not in str(marks[0].sanitized_input) + str(plans[0].payload) + str(plans[0].parts)
        assert plans[0].parts[0]["source_ref"]["values"]["email"] == "ingress@example.com"
    finally:
        await e.channel.stop()


async def test_bound_member_and_message_unblock_only_the_exact_live_account(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch)
    try:
        assert await e.channel.handle_update(membership(e, 471, status="kicked")) is True
        blocked = await db.links.get(e.link.id)
        assert blocked.state == "blocked"
        assert await e.channel.handle_update(membership(e, 472, status="member")) is True
        assert (await db.links.get(e.link.id)).state == "connected"
        assert await e.channel.handle_update(membership(e, 473, status="kicked")) is True
        # The unbuilt content is still unacknowledged; restoration is a separate
        # real account transition, not a fake content consumer.
        assert await e.channel.handle_update(message(e, 474, content="Ordinary unfinished input")) is False
        restored = await db.links.get(e.link.id)
        assert restored.state == "connected" and restored.binding_generation == blocked.binding_generation
        assert restored.acts_paused_since == blocked.acts_paused_since
        assert await e.channel.handle_update(membership(e, 475, status="kicked", chat=7_000_000_099)) is True
        assert (await db.links.get(e.link.id)).state == "connected"
    finally:
        await e.channel.stop()


async def test_edited_hash_and_unsupported_throttle_commit_only_fixed_plans(db):
    e = await ingress(db)
    try:
        assert await e.channel.handle_update(message(e, 480)) is True
        assert await e.channel.handle_update(message(e, 481, mid=480, content="/agent changed", edited=True)) is True
        assert await e.channel.handle_update(message(e, 482, mid=480, content="/agent changed", edited=True)) is True
        assert await e.channel.handle_update(message(e, 483, mid=999, content="unknown edit", edited=True)) is True
        for identity in (484, 485):
            payload = update_payload(identity, account=e.link.telegram_user_id)
            payload["message"].pop("text")
            payload["message"]["location"] = {"latitude": 0.1, "longitude": 0.2}
            assert await e.channel.handle_update(Update.de_json(payload, e.channel.bot)) is True
        keys = [row.parts[0]["source_ref"]["key"] for row in await rows(db, TelegramOutboxRow) if row.payload.get("product_basis")]
        assert keys.count("edit.ignored") == 1 and keys.count("media.unsupported") == 1
        assert len(await rows(db, TelegramInboundMarkRow)) == 2  # setup Start and /agent only
    finally:
        await e.channel.stop()


@pytest.mark.parametrize("close_gate", ["shutdown", "conflict", "generation"])
async def test_disposable_notice_rechecks_the_scope_after_last_await(db, monkeypatch, close_gate):
    e = await ingress(db)
    entered, release = asyncio.Event(), asyncio.Event()
    original = e.services.settings.get

    async def delayed(owner):
        entered.set()
        await release.wait()
        return await original(owner)

    monkeypatch.setattr(e.services.settings, "get", delayed)
    try:
        assert await e.channel.handle_update(message(e, 490, sender=7_000_000_003, content="Synthetic stranger")) is True
        await asyncio.wait_for(entered.wait(), 3)
        if close_gate == "shutdown":
            e.active[0] = False
        elif close_gate == "conflict":
            await db.links.set_health(e.link.id, "conflict", holds=True, expected_token_revision=e.link.token_revision)
        else:
            await db.links.unbind(e.link, actor_id=e.link.owner_user_id)
        release.set()
        await drain_disposable(e)
        assert [call.endpoint for call in e.request.calls] == ["getMe"]
    finally:
        release.set()
        await e.channel.stop()


@pytest.mark.parametrize("with_button", [False, True])
async def test_memory_button_and_structural_fallback_keep_the_exact_source(db, with_button):
    e = await ingress(db, lead=True)
    address = "https://example.test/workspace?settings=memory&view=all"
    original = e.services.prepare_ingress_values

    async def prepared(link):
        values = await original(link)
        return {**values, "memory_url": address if with_button else None}

    e.services.prepare_ingress_values = prepared
    sender = None
    try:
        assert await e.channel.handle_update(message(e, 495, content="/memory")) is True
        plan = next(row for row in await rows(db, TelegramOutboxRow) if row.payload.get("product_basis"))
        part_id = plan.parts[0]["part_id"]
        if with_button:
            e.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Button URL invalid"})
            e.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Can't parse entities"})
        e.request.enqueue("sendMessage", result=sent(e))
        sender = await command_sender(db, e)
        for _ in range(3 if with_button else 1):
            await sender.tick()
            await sender.drain()
            e.clock.advance(1)
        calls = [call for call in e.request.calls if call.endpoint == "sendMessage"]
        if with_button:
            button = calls[0].parameters["reply_markup"]["inline_keyboard"][0][0]
            assert button == {"text": "Open Memory", "url": address}
            assert "reply_markup" not in calls[-1].parameters
            assert calls[-1].parameters["text"].endswith(address) and "parse_mode" not in calls[-1].parameters
        else:
            assert "reply_markup" not in calls[0].parameters
        saved = next(row for row in await rows(db, TelegramOutboxRow) if row.id == plan.id)
        assert saved.state == "sent" and saved.parts[0]["part_id"] == part_id
        assert saved.parts[0]["source_ref"] == plan.parts[0]["source_ref"]
        assert all(call.parameters["link_preview_options"]["is_disabled"] for call in calls)
    finally:
        if sender is not None:
            await sender.stop()
        await e.channel.stop()


async def test_real_polltask_keeps_failed_sql_suffix_and_does_not_confirm_it(db):
    from sqlalchemy import event

    from app.telegram_bots.polling import PollRecord, PollTask

    e = await ingress(db)
    failed = [False]

    def fail_plan(connection, cursor, statement, parameters, context, many):
        if not failed[0] and statement.lstrip().startswith("INSERT INTO telegram_outbox"):
            failed[0] = True
            raise OperationalError("fixture", {}, RuntimeError("synthetic SQL failure"))

    event.listen(db.engine.sync_engine, "before_cursor_execute", fail_plan)
    batch = [message(e, identity).to_dict() for identity in (510, 511)]
    e.updates.enqueue("getUpdates", result=batch)
    next_poll = e.updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    async def health(*args):
        raise AssertionError("No transport health transition belongs to a handler SQL fault")

    poll = PollTask(e.channel, on_health=health, record=PollRecord(), clock=e.clock.now, sleep=e.clock.sleep)
    running = asyncio.create_task(poll.run())
    try:
        await e.clock.wait_for_sleep()
        assert failed[0] and poll.offset is None and len(e.updates.calls) == 1
        marks = await rows(db, TelegramInboundMarkRow)
        assert {row.message_id for row in marks} == {101, 510}
        assert not next(row for row in marks if row.message_id == 510).route_complete
        e.clock.advance(1)
        await asyncio.wait_for(next_poll.started.wait(), 4)
        assert poll.offset == 512
        assert e.updates.calls[-1].parameters["offset"] == 512
        assert all(row.route_complete for row in await rows(db, TelegramInboundMarkRow))
    finally:
        event.remove(db.engine.sync_engine, "before_cursor_execute", fail_plan)
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        await e.channel.stop()


async def test_background_error_cannot_poison_or_bless_another_admission(db, caplog):
    e = await ingress(db)
    original = e.services.prepare_ingress_values

    async def fail():
        raise RuntimeError("private fixture content must not be logged")

    async def prepared(link):
        assert e.channel._spawn_ingress(fail())
        await asyncio.gather(*tuple(e.channel._ingress_tasks), return_exceptions=True)
        return await original(link)

    e.services.prepare_ingress_values = prepared
    try:
        assert await e.channel.handle_update(message(e, 520)) is True
        assert await e.channel.handle_update(message(e, 521, content="Unfinished ordinary content")) is False
        assert "cause=RuntimeError:" in caplog.text
        assert "private fixture content must not be logged" not in caplog.text
    finally:
        await e.channel.stop()


@pytest.mark.parametrize("deleted", [False, True])
async def test_token_bearing_unbound_start_is_deleted_before_binding_and_warns(db, deleted):
    import secrets

    e = await ingress(db, connected=False)
    token = "123456:" + secrets.token_urlsafe(30)
    try:
        if deleted:
            e.request.enqueue("deleteMessage", result=True)
        else:
            e.request.enqueue("deleteMessage", status=403, body={"ok": False, "error_code": 403, "description": "Forbidden: fixture delete refused"})
        e.request.enqueue("sendMessage", result=sent(e))
        assert await e.channel.handle_update(message(e, 530, content="/start " + token)) is True
        await drain_disposable(e)
        assert (await db.links.get(e.link.id)).state == "waiting_connect"
        assert await rows(db, TelegramInboundMarkRow) == [] and await rows(db, TelegramOutboxRow) == []
        assert e.request.calls[-1].parameters["text"] == render("token.pasted" if deleted else "token.pasted_kept", "en-US", product="HartMesh")
        assert not e.channel._edit_hashes
    finally:
        await e.channel.stop()


async def test_fresh_disabled_owner_cannot_bind_from_its_still_valid_code(db, monkeypatch):
    from datetime import UTC, datetime

    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = await ingress(db, connected=False, monkeypatch=monkeypatch, lead=True)
    code = code_of(await db.links.mint_code(e.link))
    async with db.sessions.begin() as session:
        session.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.live.id, disabled_at=datetime.now(UTC)))
    try:
        with pytest.raises(AccessOff):
            await e.channel.handle_update(message(e, 540, content="/start " + code))
        assert (await db.links.get(e.link.id)).state == "waiting_connect"
        assert await rows(db, TelegramInboundMarkRow) == [] and await rows(db, TelegramOutboxRow) == []
        assert [call.endpoint for call in e.request.calls] == ["getMe"]
    finally:
        await e.channel.stop()


@pytest.mark.parametrize("operation", ["start", "member", "fixed"])
async def test_owner_preparation_cannot_admit_a_retired_adapter(db, monkeypatch, operation):
    e = await ingress(db, connected=operation != "start", monkeypatch=monkeypatch)
    if operation == "start":
        update = message(e, 550, content="/start " + code_of(await db.links.mint_code(e.link)))
    elif operation == "member":
        await db.links.mark_blocked_if_current(link_id=e.link.id, chat_id=e.link.chat_id, binding_generation=e.link.binding_generation, token_revision=e.link.token_revision)
        update = membership(e, 550, status="member")
    else:
        update = message(e, 550)
    entered, release = asyncio.Event(), asyncio.Event()
    original = e.services.prepare_live_owner

    async def delayed(owner):
        result = await original(owner)
        entered.set()
        await release.wait()
        return result

    e.services.prepare_live_owner = delayed
    processing = asyncio.create_task(e.channel.handle_update(update))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        e.services.channel = lambda identity: None
        release.set()
        assert await asyncio.wait_for(processing, 3) is False
        assert (await db.links.get(e.link.id)).state == ("waiting_connect" if operation == "start" else "blocked" if operation == "member" else "connected")
        assert not any(row.message_id == 550 for row in await rows(db, TelegramInboundMarkRow))
    finally:
        release.set()
        await asyncio.gather(processing, return_exceptions=True)
        await e.channel.stop()


async def test_actual_supervisor_held_gate_and_transition_share_one_notice_claim(db, monkeypatch):
    from _telegram_repository_test_support import bind_link
    from test_telegram_supervisor import build as supervisor_build
    from test_telegram_supervisor import credential

    e = supervisor_build(db, monkeypatch)
    link, token = await credential(db, e.env)
    link = await bind_link(db, link)
    e.normal.enqueue_get_me(bot_id=link.bot_id)
    e.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    response = e.normal.enqueue("sendMessage", result={"message_id": 600, "date": 1_893_456_000, "chat": {"id": link.chat_id, "type": "private"}, "text": "Access off"}, release=asyncio.Event())
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        channel = e.supervisor.channel(link.id)
        held = await db.links.hold_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)
        payload = update_payload(560, text="Held input")
        assert await channel.handle_update(Update.de_json(payload, channel.bot)) is True
        await asyncio.wait_for(response.started.wait(), 3)
        e.clock.advance(1)
        # The reconcile path uses the same claim operation while the first
        # direct notice still owns a live SDK attempt.
        channel.link = held
        await e.supervisor._notice(channel, "access.off")
        assert await channel.handle_update(Update.de_json(update_payload(561, text="Second held input"), channel.bot)) is True
        response.release.set()
        await asyncio.gather(*tuple(channel._ingress_tasks))
        assert [call.endpoint for call in e.normal.calls] == ["getMe", "sendMessage"]
        assert "access_off" in (await db.links.get(link.id)).notices
    finally:
        response.release.set()
        await e.supervisor.stop()


async def test_bot_authored_start_does_not_bind_or_consume_the_human_code(db, monkeypatch):
    e = await ingress(db, connected=False, monkeypatch=monkeypatch, lead=True)
    code = code_of(await db.links.mint_code(e.link))
    payload = update_payload(570, text="/start " + code)
    payload["message"]["from"]["is_bot"] = True
    try:
        assert await e.channel.handle_update(Update.de_json(payload, e.channel.bot)) is True
        assert (await db.links.get(e.link.id)).state == "waiting_connect"
        assert await rows(db, TelegramInboundMarkRow) == [] and await rows(db, TelegramOutboxRow) == []
        async with db.links.capacity_writer(e.link.owner_user_id) as session:
            assert await db.links.consume_code(e.link, code, session=session) == "ok"
        assert [call.endpoint for call in e.request.calls] == ["getMe"]
    finally:
        await e.channel.stop()


async def test_supervisor_preserves_the_real_stranger_charge_across_rotation_and_readdition(db, monkeypatch):
    import secrets
    from types import SimpleNamespace

    from _telegram_bot_test_support import RecordingRequest
    from test_telegram_supervisor import build as supervisor_build
    from test_telegram_supervisor import credential

    from app.telegram_bots.adapter import TelegramBotChannel

    created = []

    def factory(*, link, token, supervisor, **kwargs):
        normal, updates = RecordingRequest(), RecordingRequest()
        normal.enqueue_get_me(bot_id=link.bot_id)
        updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        services = SimpleNamespace(
            config=supervisor.config,
            links=supervisor.links,
            outbox=supervisor.outbox,
            settings=supervisor.settings,
            tls_context=supervisor.tls_context,
            strangers=supervisor.strangers,
            product_name=supervisor.product_name,
            prepare_live_owner=supervisor.prepare_live_owner,
            prepare_ingress_values=supervisor.prepare_ingress_values,
            notice_access_off=supervisor.notice_access_off,
            eligibility=supervisor.eligibility,
            protect_notice=supervisor.protect_notice,
            channel=supervisor.channel,
            send_allowed=supervisor.send_allowed,
            request_factory=lambda **facts: normal if facts["connection_pool_size"] == 8 else updates,
        )
        channel = TelegramBotChannel(link=link, token=token, supervisor=services, **kwargs)
        created.append((channel, normal))
        return channel

    e = supervisor_build(db, monkeypatch, factory=factory)
    link, token = await credential(db, e.env)
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        first, normal = created[-1]
        normal.enqueue("sendMessage", result={"message_id": 600, "date": 1_893_456_000, "chat": {"id": 7_000_000_001, "type": "private"}, "text": "Private bot"})
        assert await first.handle_update(Update.de_json(update_payload(580, text="Synthetic stranger"), first.bot)) is True
        await asyncio.gather(*tuple(first._ingress_tasks))
        assert [call.endpoint for call in normal.calls] == ["getMe", "sendMessage"]
        await e.supervisor.replace_token(link, str(link.bot_id) + ":" + secrets.token_urlsafe(30), actor_id=link.owner_user_id)
        second, normal = created[-1]
        assert second is not first and first.services.strangers is second.services.strangers is e.supervisor.strangers
        assert await second.handle_update(Update.de_json(update_payload(581, text="Same stranger"), second.bot)) is True
        await asyncio.gather(*tuple(second._ingress_tasks))
        assert [call.endpoint for call in normal.calls] == ["getMe"]
        normal.enqueue("setMyDescription", result=True)
        await e.supervisor.remove(link.id, actor_id=link.owner_user_id, last_text=None)
        again, token = await credential(db, e.env, bot=link.bot_id)
        await e.supervisor.add(again, token)
        third, normal = created[-1]
        assert third.services.strangers is first.services.strangers
        assert await third.handle_update(Update.de_json(update_payload(582, text="Same stranger after readdition"), third.bot)) is True
        await asyncio.gather(*tuple(third._ingress_tasks))
        assert [call.endpoint for call in normal.calls] == ["getMe"]
    finally:
        await e.supervisor.stop()


async def test_disposable_task_does_not_inherit_the_update_admission_graph(db):
    from app.telegram_bots.adapter import _ADMISSION

    e = await ingress(db)
    original, observed = e.services.prepare_ingress_values, []

    async def observe():
        observed.append(_ADMISSION.get())

    async def prepared(link):
        assert e.channel._spawn_ingress(observe())
        await asyncio.gather(*tuple(e.channel._ingress_tasks))
        return await original(link)

    e.services.prepare_ingress_values = prepared
    try:
        assert await e.channel.handle_update(message(e, 590)) is True
        assert observed == [None]
    finally:
        await e.channel.stop()


@pytest.mark.parametrize("operation", ["leave", "delete", "callback"])
async def test_raw_ingress_control_has_a_final_gate_after_its_sql_guard(db, monkeypatch, operation):
    from contextvars import ContextVar

    e = await ingress(db)
    entered, release = asyncio.Event(), asyncio.Event()
    guarded = ContextVar("fixture_raw_guard", default=False)
    current, refresh = e.channel._ingress_current, e.channel.refresh_link

    async def guarded_current(context):
        marker = guarded.set(True)
        try:
            return await current(context)
        finally:
            guarded.reset(marker)

    async def delayed_refresh():
        result = await refresh()
        if guarded.get():
            entered.set()
            await release.wait()
        return result

    monkeypatch.setattr(e.channel, "_ingress_current", guarded_current)
    monkeypatch.setattr(e.channel, "refresh_link", delayed_refresh)
    if operation == "leave":
        update = membership(e, 600, status="member", chat=-100123, kind="supergroup", old="left")
    elif operation == "delete":
        import secrets

        update = message(e, 600, content="123456:" + secrets.token_urlsafe(30))
    else:
        query = {
            "id": "fixture-query",
            "from": {"id": e.link.telegram_user_id, "first_name": "Fixture", "is_bot": False},
            "chat_instance": "fixture-instance",
            "data": "unknown",
            "message": {"message_id": 101, "date": 0, "chat": {"id": e.link.chat_id, "type": "private"}},
        }
        update = Update.de_json({"update_id": 600, "callback_query": query}, e.channel.bot)
    try:
        assert await e.channel.handle_update(update) is True
        await asyncio.wait_for(entered.wait(), 3)
        e.active[0] = False
        release.set()
        await drain_disposable(e)
        assert [call.endpoint for call in e.request.calls] == ["getMe"]
    finally:
        release.set()
        await e.channel.stop()


async def test_cancelled_supervisor_stop_waiters_retain_ingress_sql_and_clients(db, monkeypatch):
    from contextvars import ContextVar

    from _telegram_repository_test_support import bind_link
    from sqlalchemy import event
    from test_telegram_supervisor import build as supervisor_build
    from test_telegram_supervisor import credential

    e = supervisor_build(db, monkeypatch)
    link, token = await credential(db, e.env)
    link = await bind_link(db, link)
    e.normal.enqueue_get_me(bot_id=link.bot_id)
    e.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    entered, release, interrupted = asyncio.Event(), asyncio.Event(), asyncio.Event()
    owned = ContextVar("fixture_retained_ingress_sql", default=False)
    original = e.supervisor.notice_access_off

    async def notice(captured):
        marker = owned.set(True)
        try:
            await original(captured)
        finally:
            owned.reset(marker)

    monkeypatch.setattr(e.supervisor, "notice_access_off", notice)

    def pause_sql(connection, cursor, statement, parameters, context, many):
        if not owned.get() or entered.is_set() or not statement.lstrip().startswith("SELECT"):
            return

        async def wait(driver):
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                interrupted.set()
                raise

        # The real statement has completed in its actual async driver; its
        # repository session/guard still owns completion on the original loop.
        connection.connection.dbapi_connection.run_async(wait)

    event.listen(db.engine.sync_engine, "after_cursor_execute", pause_sql)
    waiters = []
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        channel = e.supervisor.channel(link.id)
        await db.links.hold_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)
        assert await channel.handle_update(Update.de_json(update_payload(610, text="Held input"), channel.bot)) is True
        await asyncio.wait_for(entered.wait(), 3)
        first = asyncio.create_task(e.supervisor.stop())
        waiters.append(first)

        async def cancelling():
            while not any(task.cancelling() for task in channel._ingress_tasks):
                await asyncio.sleep(0)

        await asyncio.wait_for(cancelling(), 3)
        cleanup = e.supervisor._stop_task
        for _ in range(2):
            waiter = waiters[-1]
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            assert e.supervisor._stop_task is cleanup and not cleanup.done()
            assert not interrupted.is_set()
            assert not e.normal.closed and not e.updates.closed
            if len(waiters) == 1:
                waiters.append(asyncio.create_task(e.supervisor.stop()))
                await asyncio.sleep(0)
        release.set()
        await asyncio.wait_for(e.supervisor.stop(), 3)
        assert e.normal.closed and e.updates.closed and not channel._ingress_tasks
        assert not interrupted.is_set()
        assert [call.endpoint for call in e.normal.calls] == ["getMe"]
    finally:
        release.set()
        await asyncio.gather(*waiters, return_exceptions=True)
        await e.supervisor.stop()
        event.remove(db.engine.sync_engine, "after_cursor_execute", pause_sql)


@pytest.mark.parametrize("token_field", ["text", "caption"])
async def test_edited_update_scans_text_and_caption_independently(db, token_field):
    import secrets

    e = await ingress(db)
    token = "123456:" + secrets.token_urlsafe(30)
    payload = update_payload(620, text="Safe edited text")
    payload["message"]["caption"] = "Safe edited caption"
    payload["message"][token_field] = "https://api.telegram.org/bot" + token + "/getMe"
    payload["edited_message"] = payload.pop("message")
    e.request.enqueue("deleteMessage", result=True)
    e.request.enqueue("sendMessage", result=sent(e))
    try:
        before = len(await rows(db, TelegramInboundMarkRow))
        assert await e.channel.handle_update(Update.de_json(payload, e.channel.bot)) is True
        await drain_disposable(e)
        assert [call.endpoint for call in e.request.calls] == ["getMe", "deleteMessage", "sendMessage"]
        assert not e.channel._edit_hashes
        assert len(await rows(db, TelegramInboundMarkRow)) == before
        assert all(token not in str(call.parameters) for call in e.request.calls)
    finally:
        await e.channel.stop()


async def test_ten_minutes_of_refused_sends_mark_the_bot_and_an_accepted_send_clears_it(db):
    from datetime import UTC, datetime, timedelta

    e = await ingress(db)
    moment = [datetime(2030, 1, 1, 12, tzinfo=UTC)]
    e.channel._now = lambda: moment[0]
    first = moment[0]
    try:
        await e.channel._send_refused_by_limit()
        moment[0] = first + timedelta(seconds=599)
        await e.channel._send_refused_by_limit()
        assert (await db.links.get(e.link.id)).limited_since is None

        moment[0] = first + timedelta(seconds=600)
        await e.channel._send_refused_by_limit()
        assert (await db.links.get(e.link.id)).limited_since == first

        await e.channel._after_send_success(counts=False)
        assert (await db.links.get(e.link.id)).limited_since == first, "a draft or a preview does not end it"

        await e.channel._after_send_success()
        assert (await db.links.get(e.link.id)).limited_since is None
        assert e.channel._refused_since is None
    finally:
        await e.channel.stop()


async def test_a_removed_link_cannot_turn_an_accepted_send_into_a_failure(db, monkeypatch):
    from dataclasses import replace
    from datetime import UTC, datetime

    e = await ingress(db)
    moment = datetime(2030, 1, 1, 12, tzinfo=UTC)

    async def removed(*args, **kwargs):
        raise ValueError("link-not-found")

    try:
        e.channel.link = replace(e.channel.link, limited_since=moment)
        monkeypatch.setattr(e.services.links, "update", removed)
        await e.channel._after_send_success()
    finally:
        await e.channel.stop()
