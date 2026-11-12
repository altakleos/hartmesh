"""Current news recovery preserves delivery evidence and never starts Work."""

import pytest
from _telegram_bot_test_support import ManualClock
from _telegram_repository_test_support import db as db
from test_telegram_bot_adapter import message
from test_telegram_work_news_delivery import prepare
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow, TelegramOutboxRow, TelegramPersonSettingsRow


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["locale", "protection"])
async def test_news_refresh_before_send_uses_current_presentation(work_delivery, change):
    e = work_delivery
    _plans, identity = await prepare(e)
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    async with e.db.sessions.begin() as writer:
        if change == "locale":
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
        else:
            writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=clock.now()))
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.payload["presentation_version"] == 1
        assert row.parts[0]["state"] == "retired" and not row.parts[0]["message_ids"]
    message(e.channel.bot.request, identity=2401)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1
    assert ("event-peer@example.com" in calls[0].parameters["text"]) == (change == "locale")
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent"
        assert row.parts[-1]["message_ids"] == [2401]


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [None, "claim_id", "claim_owner", "claimed_at", "topic", "event", "permission"])
async def test_missing_news_anchor_repair_keeps_show_proof_and_refuses_live_claim(work_delivery, refusal):
    from sqlalchemy import select
    from test_telegram_work_news_delivery import test_protected_colleague_show_edits_actual_teaser_without_starting_work

    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    await test_protected_colleague_show_edits_actual_teaser_without_starting_work(e, "missing")
    clock = e.sender.outbox.clock.__self__
    async with e.db.sessions.begin() as writer:
        row = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.failed_reason == "work-news-refresh-required"))
        identity = row.id
        if refusal in {"claim_id", "claim_owner", "claimed_at"}:
            setattr(row, refusal, clock.now() if refusal == "claimed_at" else "unreconciled")
        elif refusal == "topic":
            row.message_thread_id = 91
        elif refusal == "event":
            (await writer.get(AgentWorkEventRow, row.event_id)).actor_id = "changed-actor"
        elif refusal == "permission":
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    recovery = e.channel._services.work_news_sources.recovery
    if refusal:
        if refusal in {"event", "permission"}:
            with pytest.raises(ProductSourceRetired):
                await recovery.repair(e.link, identity, guard=guard)
        else:
            assert await recovery.repair(e.link, identity, guard=guard) is None
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.state == "failed" and row.payload["presentation_version"] == 1
        return
    assert await recovery.repair(e.link, identity, guard=guard) == identity
    assert await recovery.repair(e.link, identity, guard=guard) is None
    sdk = e.channel.bot.request
    sdk.enqueue("editMessageReplyMarkup", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message to edit not found"})
    message(sdk, identity=2501)
    for _ in range(2):
        clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, row.failed_reason, e.dispatch_errors)
        assert row.parts[0]["message_ids"] == [2101]
        assert row.parts[-1]["message_ids"] == [2501]
        assert row.payload["presentation_version"] == 2


@pytest.mark.asyncio
async def test_partial_news_refresh_preserves_exact_old_ack(work_delivery):
    from test_telegram_work_news_delivery import long_review_news

    e = work_delivery
    _plans, identity = await prepare(e, item=await long_review_news(e))
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    sdk = e.channel.bot.request
    message(sdk, identity=2601)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        first = row.parts[0]
        assert first["message_ids"] == [2601] and row.state == "pending"
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["presentation_version"] == 1 and row.parts[0] == first
        pending = [part for part in row.parts if part["presentation_version"] == 1]
    for index in range(len(pending)):
        message(sdk, identity=2602 + index)
        clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, row.failed_reason, e.dispatch_errors)
        assert row.parts[0] == first


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "show_group", "history"])
async def test_show_on_refreshed_teaser_rechecks_its_ack_and_origin(work_delivery, damage):
    from copy import deepcopy
    from uuid import uuid4

    from sqlalchemy import select

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    await test_news_refresh_before_send_uses_current_presentation(e, "protection")
    clock = e.sender.outbox.clock.__self__
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_news_basis"]["version"].as_integer() == 3))
        identity = row.id
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    await e.channel._services.work_news_sources.recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=2401, message_thread_id=None, guard=guard)
    if damage:
        async with e.db.sessions.begin() as writer:
            if damage == "show_group":
                (await writer.get(TelegramCallbackRow, show.id)).group_id = uuid4().hex
            else:
                row = await writer.get(TelegramOutboxRow, identity)
                payload = deepcopy(row.payload)
                payload["work_news_basis"]["history"][-1]["parts"][-1]["message_ids"] = [True]
                row.payload = payload
    sdk = e.channel.bot.request
    if not damage:
        message(sdk, "editMessageText", identity=2401)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in sdk.calls if call.endpoint == "editMessageText"]
    assert len(calls) == (0 if damage else 1)
    if not damage:
        assert "event-peer@example.com" in calls[0].parameters["text"]
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.state == "sent" and row.payload["presentation_version"] == 2
            assert row.parts[-1]["message_ids"] == [2401]


@pytest.mark.asyncio
async def test_repeated_news_refresh_keeps_bounded_json_depth(work_delivery):
    e = work_delivery
    _plans, identity = await prepare(e)
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    for version in range(1, 11):
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN" if version % 2 else "en-US"
        clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.payload["presentation_version"] == version, (row.state, row.failed_reason, e.dispatch_errors)
    message(e.channel.bot.request, identity=2701)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["locale", "timezone", "protection", "base", "quiet"])
async def test_final_presentation_drift_defers_instead_of_skipping_news(work_delivery, monkeypatch, change):
    e = work_delivery
    _plans, identity = await prepare(e, "cancel" if change == "quiet" else "edit")
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    sources = e.channel._services.work_news_sources
    original, calls = sources.settings.get, []
    if change == "quiet":
        from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramBotLinkRow, e.link.id)).last_update_at = None
            settings_row = await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)
            settings_row.quiet_enabled = False
            settings_row.timezone = "UTC"
            settings_row.quiet_start_minute, settings_row.quiet_end_minute = 0, 1439

    async def drift(*args, **kwargs):
        settings = await original(*args, **kwargs)
        calls.append(True)
        if len(calls) == 2:
            if change == "base":
                e.live.config.model_extra["telegram_bots"] = {**(e.live.config.model_extra.get("telegram_bots") or {}), "public_base_url": "https://changed.example.test"}
            else:
                async with e.db.sessions.begin() as writer:
                    if change == "protection":
                        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=clock.now()))
                    else:
                        settings_row = await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)
                        if change == "quiet":
                            settings_row.quiet_enabled = True
                        else:
                            setattr(settings_row, change, "zh-CN" if change == "locale" else "Asia/Tokyo")
        return settings

    monkeypatch.setattr(sources.settings, "get", drift)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending", (row.state, row.skip_reason, e.dispatch_errors)
    assert len(calls) >= 2 and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    message(e.channel.bot.request, identity=2801)
    for _ in range(3):
        clock.advance(10)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        async with e.db.sessions() as reader:
            if (await reader.get(TelegramOutboxRow, identity)).state == "sent":
                break
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
