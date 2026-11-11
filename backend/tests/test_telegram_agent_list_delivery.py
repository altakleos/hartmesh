"""Real admission/SQL/sender/SDK; only the still-inactive command switch is injected."""

import asyncio

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import select, update
from telegram.ext import CommandHandler
from test_telegram_agent_list_source import lists as lists
from test_telegram_bot_handlers import message, sent
from test_telegram_callback_commands import drain
from test_telegram_instance_presentation import presentation as presentation

from app.telegram_bots.commands import CallbackCommands
from app.telegram_bots.producer import OutboxSender
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def delivery(lists):
    e = lists
    e.services.commands = CallbackCommands(e.services)
    e.services.eligibility = e.live.authority.delivery

    async def route(update, context):
        word, _ = e.channel._command(update.message)
        await e.services.commands.handle_list(e.channel, update, word)
        e.channel._classified(update, remember=True)

    # No production command menu or dispatch switch is activated by this slice.
    for handler in e.channel.application.handlers[0]:
        if isinstance(handler, CommandHandler) and "employees" in handler.commands:
            handler.callback = route
    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            row.state, row.skip_reason = "skipped", "superseded"
    e.sender = OutboxSender(e.services.outbox, run_id="agent-list", deliver_part=lambda **kw: e.channel.send_part(**{key: value for key, value in kw.items() if key != "link_id"}), eligibility=e.services.eligibility)
    try:
        yield e
    finally:
        await e.sender.stop()


async def offer(e, *, topic=None):
    from telegram import Update

    incoming = message(e, 1901, content="/employees")
    if topic is not None:
        raw = incoming.to_dict()
        raw["message"]["message_thread_id"] = topic
        incoming = Update.de_json(raw, e.channel.bot)
    assert await e.channel.handle_update(incoming)
    await asyncio.gather(*tuple(e.channel._ingress_tasks))
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id, TelegramOutboxRow.state == "pending")))
        assert len(rows) == 1
        assert e.instance_id in {target["id"] for target in rows[0].payload["agent_list_basis"]["targets"]}
        return rows[0].id


async def test_list_replay_sends_one_owner_bound_list_with_open_link(delivery):
    e = delivery
    identity = await offer(e)
    assert await e.channel.handle_update(message(e, 1901, content="/employees"))
    e.request.enqueue("sendMessage", result=sent(e, identity=901))
    await drain(e)
    calls = [call for call in e.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1
    assert calls[0].parameters["text"] == "AI employees you can chat with:\nFixture employee"
    assert calls[0].parameters["reply_markup"]["inline_keyboard"][0][0]["url"] == "https://t.me/instance_fixture_bot"
    assert (await e.services.outbox.get(e.link, identity)).parts[0]["state"] == "sent"


@pytest.mark.parametrize("when", ["queued", "limiter"])
async def test_lost_use_never_reaches_the_raw_sdk(delivery, when):
    e = delivery
    await offer(e)
    changed = []

    async def withdraw(*args):
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        changed.append(True)
        e.channel.bot.rate_limiter._short_hold_until = 0

    if when == "queued":
        await withdraw()
    else:
        limiter = e.channel.bot.rate_limiter
        limiter._sleep = withdraw
        limiter._short_hold_until = limiter._time() + 10
    e.request.enqueue("sendMessage", result=sent(e, identity=901))
    await drain(e)
    assert changed
    assert not [call for call in e.request.calls if call.endpoint == "sendMessage"]


async def test_obsolete_list_does_not_hold_up_later_fixed_replies(delivery):
    e = delivery
    identity = await offer(e)
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
    e.clock.advance(1)  # The later reply must sort after the list under this frozen clock.
    await e.services.outbox.plan_product(e.link, source_id="after-list", parts=[{"kind": "text", "source_ref": {"key": "hello.privacy", "values": {}}}])
    await drain(e)
    async with e.db.sessions() as reader:
        obsolete = await reader.get(TelegramOutboxRow, identity)
        assert obsolete.state == "skipped" and obsolete.skip_reason == "superseded"
    e.request.enqueue("sendMessage", result=sent(e, identity=902))
    await drain(e)
    calls = [call for call in e.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and "Fixture employee" not in calls[0].parameters["text"]


async def test_list_uses_current_language_for_text_and_url_labels(delivery):
    e = delivery
    await offer(e)
    await e.services.settings.update(e.link.owner_user_id, locale="zh-CN")
    e.request.enqueue("sendMessage", result=sent(e, identity=902))
    await drain(e)
    call = next(call for call in e.request.calls if call.endpoint == "sendMessage")
    assert call.parameters["text"] == "你可以聊天的 AI 员工：\nFixture employee"
    assert call.parameters["reply_markup"]["inline_keyboard"][0][0]["text"] == "打开 Fixture employee"


@pytest.mark.parametrize("change", ["inspect", "deleted", "open_bot", "owner", "claim", "lifetime"])
async def test_final_sdk_fence_rechecks_the_full_list_scope(delivery, change):
    from datetime import UTC, datetime

    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = delivery
    if change == "inspect":
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=3))
    identity = await offer(e)
    changed = []
    limiter = e.channel.bot.rate_limiter

    async def withdraw(_delay):
        limiter._short_hold_until = 0
        async with e.db.sessions.begin() as writer:
            if change == "inspect":
                await writer.execute(update(AgentInstanceGrantRow).values(permissions=1))
            elif change == "deleted":
                await writer.execute(update(AgentInstanceRow).values(status="deleted"))
            elif change == "open_bot":
                await writer.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.instance_link.id).values(bot_username="other_bot"))
            elif change == "owner":
                writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.link.owner_user_id, disabled_at=datetime.now(UTC)))
            elif change == "claim":
                await writer.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == identity).values(claim_id="replacement"))
        if change == "lifetime":
            e.services.send_allowed = lambda: False
        changed.append(True)

    limiter._sleep = withdraw
    limiter._short_hold_until = limiter._time() + 10
    e.request.enqueue("sendMessage", result=sent(e, identity=901))
    try:
        await drain(e)
        assert changed
        assert not [call for call in e.request.calls if call.endpoint == "sendMessage"]
    finally:
        e.services.send_allowed = lambda: True


async def many_employees(e):
    from copy import deepcopy
    from uuid import uuid4

    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.spaces.model import SpaceRow

    e.live.config.model_extra["telegram_bots"] = {"enabled": True, "public_base_url": "https://product.example"}
    async with e.db.sessions.begin() as writer:
        original = await writer.get(AgentInstanceRow, e.instance_id)
        home = await writer.get(SpaceRow, original.home_id)
        original.name = "😀" * 128
        for _ in range(19):
            fields = {column.name: deepcopy(getattr(home, column.name)) for column in SpaceRow.__table__.columns}
            fields.update(id=uuid4().hex, backing_handle=uuid4().hex)
            writer.add(SpaceRow(**fields))
            await writer.flush()
            agent = {column.name: deepcopy(getattr(original, column.name)) for column in AgentInstanceRow.__table__.columns}
            identity = uuid4().hex
            agent.update(id=identity, principal_id="agent:" + identity, home_id=fields["id"], creation_id=uuid4().hex)
            writer.add(AgentInstanceRow(**agent))
            await writer.flush()
            writer.add(AgentInstanceGrantRow(instance_id=identity, user_id=e.link.owner_user_id, permissions=1))


@pytest.mark.parametrize("relocate", [False, True])
async def test_multipart_list_defers_links_and_preserves_acknowledged_lines(delivery, relocate):
    e = delivery
    await many_employees(e)
    identity = await offer(e, topic=7 if relocate else None)
    item = await e.services.outbox.get(e.link, identity)
    assert len(item.parts) == 2
    e.request.enqueue("sendMessage", result=sent(e, identity=901))
    await drain(e)
    first = next(call for call in e.request.calls if call.endpoint == "sendMessage")
    assert not first.parameters.get("reply_markup", {}).get("inline_keyboard")
    if relocate:
        e.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message thread not found"})
        await drain(e)
    e.request.enqueue("sendMessage", result=sent(e, identity=902))
    await drain(e)
    calls = [call for call in e.request.calls if call.endpoint == "sendMessage"]
    assert len(calls[-1].parameters["reply_markup"]["inline_keyboard"]) == 20
    assert calls[-1].parameters["text"] != first.parameters["text"]
    if relocate:
        assert calls[-1].parameters.get("message_thread_id") is None


async def test_empty_list_keeps_owner_authority_without_any_target_rows(delivery):
    e = delivery
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
    assert await e.channel.handle_update(message(e, 1901, content="/employees"))
    e.request.enqueue("sendMessage", result=sent(e, identity=901))
    await drain(e)
    call = next(call for call in e.request.calls if call.endpoint == "sendMessage")
    assert call.parameters["text"] == "No AI employees you can chat with yet. Administrators add AI employees."
    assert not call.parameters.get("reply_markup", {}).get("inline_keyboard")
