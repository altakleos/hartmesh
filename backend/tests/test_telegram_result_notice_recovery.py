"""Source-aware result refresh never intentionally uploads an ACKed file twice."""

from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_result_notice_delivery import prepare
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInstancePolicyRow, TelegramOutboxRow, TelegramPersonSettingsRow


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
@pytest.mark.parametrize("change", ["locale", "read"])
async def test_partial_result_refresh_keeps_real_file_ack_and_updates_card(work_exports, change):
    e = work_exports
    _plans, identity = await prepare(e)
    sdk = e.channel.bot.request
    message(sdk, "sendDocument", identity=1101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors
    async with e.db.sessions.begin() as writer:
        if change == "locale":
            (await writer.get(TelegramPersonSettingsRow, e.actor.subject_id)).locale = "zh-CN"
        else:
            await writer.delete(await writer.get(SpaceGrantRow, (e.home.id, "human", e.actor.subject_id)))
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.payload["presentation_version"] == 1
        assert row.parts[0]["message_ids"] == [1101]
    message(sdk, identity=1102)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in sdk.calls if call.endpoint in {"sendDocument", "sendMessage"}]
    assert [call.endpoint for call in calls] == ["sendDocument", "sendMessage"]
    if change == "locale":
        assert "已完成" in calls[-1].parameters["text"]
    else:
        assert "result.txt" not in calls[-1].parameters["text"]
    assert e.native_reads == ["outputs/result.txt"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
async def test_protection_upgrade_requires_new_show_without_relabeling_old_file(work_exports):
    e = work_exports
    _plans, identity = await prepare(e)
    recovery = e.channel._services.result_notice_sources.recovery
    sdk = e.channel.bot.request
    message(sdk, "sendDocument", identity=1301)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    message(sdk, identity=1302)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.payload["result_notice_basis"]["concealed"], (row.state, row.failed_reason, e.dispatch_errors)
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    await recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=1302, message_thread_id=None, guard=guard)
    message(sdk, "editMessageReplyMarkup", identity=1302)
    message(sdk, identity=1303)
    for _ in range(2):
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, row.failed_reason, e.dispatch_errors)
        assert row.parts[0]["message_ids"] == [1301] and row.parts[0]["protect_content"] is False
        assert row.parts[-1]["message_ids"] == [1303] and row.parts[-1]["protect_content"] is True
    assert len([call for call in sdk.calls if call.endpoint == "sendDocument"]) == 1
    assert e.native_reads == ["outputs/result.txt"]


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
@pytest.mark.parametrize("refusal", [None, "claim", "claim_owner", "claimed_at", "permission", "outcome", "topic"])
async def test_explicit_repair_preserves_uploaded_file_and_replaces_only_card(work_exports, monkeypatch, refusal):
    from copy import deepcopy

    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_exports
    _plans, identity = await prepare(e)
    if refusal == "topic":
        from sqlalchemy import select

        from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, identity)
            row.message_thread_id = 91
            for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])):
                control.message_thread_id = 91
    sdk = e.channel.bot.request
    message(sdk, "sendDocument", identity=1501)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    sdk.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message thread not found"})
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "failed" and row.failed_reason == "work-result-refresh-required", (row.state, row.failed_reason, e.dispatch_errors)
        assert row.claim_id is None
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    recovery = e.channel._services.result_notice_sources.recovery
    if refusal in {"claim", "claim_owner", "claimed_at", "permission"}:
        async with e.db.sessions.begin() as writer:
            if refusal in {"claim", "claim_owner", "claimed_at"}:
                field = "claim_id" if refusal == "claim" else refusal
                setattr(await writer.get(TelegramOutboxRow, identity), field, e.delivery_clock.now() if refusal == "claimed_at" else "unreconciled-claim")
            else:
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 1
    elif refusal == "outcome":
        original = recovery.reader.read

        async def changed(*args, **kwargs):
            result = await original(*args, **kwargs)
            async with e.db.sessions.begin() as writer:
                work = await writer.get(AgentWorkRow, e.result["id"])
                outcome = deepcopy(work.outcome)
                outcome["statement"] = "Changed after the source read"
                work.outcome = outcome
            return result

        monkeypatch.setattr(recovery.reader, "read", changed)
    if refusal:
        if refusal in {"claim", "claim_owner", "claimed_at", "topic"}:
            assert await recovery.repair(e.link, identity, guard=guard) is None
        else:
            with pytest.raises((ProductSourceRetired, AgentDenied)):
                await recovery.repair(e.link, identity, guard=guard)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.state == "failed" and row.payload["presentation_version"] == 0
            assert row.parts[0]["message_ids"] == [1501]
            if refusal == "topic":
                assert row.message_thread_id == 91
                calls = [call for call in sdk.calls if call.endpoint in {"sendDocument", "sendMessage"}]
                assert len(calls) == 2 and all(call.parameters["message_thread_id"] == 91 for call in calls)
        return
    assert await recovery.repair(e.link, identity, guard=guard) == identity
    assert await recovery.repair(e.link, identity, guard=guard) is None
    message(sdk, identity=1502)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, row.failed_reason, e.dispatch_errors)
        assert row.parts[0]["message_ids"] == [1501] and row.parts[-1]["message_ids"] == [1502]
    assert len([call for call in sdk.calls if call.endpoint == "sendDocument"]) == 1
    assert e.native_reads == ["outputs/result.txt"]


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_album_event"], indirect=True)
@pytest.mark.parametrize("change", ["read", "pending_read", "bytes"])
async def test_partial_album_refresh_survives_restart_and_keeps_unsent_hash(work_exports, change):
    import hashlib
    from uuid import uuid4

    from test_telegram_output_planning import png_header

    from app.telegram_bots.producer import OutboxSender
    from deerflow.persistence.agent_instances.work import AgentWorkRow
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.spaces.contract import Permission

    e = work_exports
    _plans, identity = await prepare(e)
    sdk = e.channel.bot.request
    sdk.enqueue("sendMediaGroup", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: invalid media"})
    message(sdk, "sendDocument", identity=1201)
    for _ in range(2):
        assert await e.sender.tick() == 1
        await e.sender.drain()
        e.delivery_clock.advance(2)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentWorkRow, e.result["id"])).revision += 1
        if change in {"read", "pending_read"}:
            space_id = e.home.id if change == "read" else e.source_space.id
            (await writer.get(SpaceGrantRow, (space_id, "human", e.actor.subject_id))).permissions = int(Permission.EXPORT)
    if change == "bytes":
        await e.files.write(
            actor=e.actor,
            space_id=e.source_space.id,
            expected_generation=e.source_space.generation,
            operation_id=uuid4().hex,
            path="outputs/picture.png",
            content=png_header(3, 3),
            expected_sha256=hashlib.sha256(png_header(2, 2)).hexdigest(),
        )
    assert await e.sender.tick() == 1
    await e.sender.drain()
    await e.sender.stop()
    e.sender = OutboxSender(e.sender.outbox, run_id="result-recovery-restart", deliver_part=e.sender.deliver_part, eligibility=e.sender.eligibility, proactive=True)
    if change == "read":
        message(sdk, "sendDocument", identity=1202)
    else:
        message(sdk, identity=1202)
    message(sdk, identity=1203)
    for _ in range(2 if change == "read" else 3):
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, row.failed_reason, e.dispatch_errors, row.parts)
        assert [part["message_ids"] for part in row.parts if part["message_ids"]] == [[1201], [1202], [1203]]
        fresh = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
        if change == "bytes":
            assert any(part.get("result_file_warning", {}).get("key") == "file.out_failed" for part in fresh)
        elif change == "pending_read":
            warning = next(part["result_file_warning"] for part in fresh if "result_file_warning" in part)
            assert warning["key"] == "file.no_access"
            assert warning["file"]["source"] is None and warning["file"]["filename"] == "File"
    assert len([call for call in sdk.calls if call.endpoint == "sendDocument"]) == (2 if change == "read" else 1)
    assert e.export_calls[-1][1] == (e.home.id if change == "pending_read" else e.source_space.id)
    assert e.channel._services.file_bytes_budget.used == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
@pytest.mark.parametrize("damage", ["event", "history", "receipt", "hash"])
async def test_changed_event_or_historical_media_cannot_authorize_refreshed_card(work_exports, damage):
    from copy import deepcopy

    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_exports
    _plans, identity = await prepare(e)
    sdk = e.channel.bot.request
    message(sdk, "sendDocument", identity=1701)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.actor.subject_id)).locale = "zh-CN"
        if damage == "event":
            row = await writer.get(TelegramOutboxRow, identity)
            (await writer.get(AgentWorkEventRow, row.event_id)).actor_id = "different-originator"
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    if damage != "event":
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, identity)
            assert row.payload["presentation_version"] == 1
            payload = deepcopy(row.payload)
            if damage == "history":
                payload["work_result_history"] = []
            elif damage == "receipt":
                payload["work_result_history"][0]["parts"][0]["message_ids"] = [True]
            else:
                payload["work_result_history"][0]["parts"][0]["result_file_hashes"]["0"] = "f" * 64
            row.payload = payload
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    assert not [call for call in sdk.calls if call.endpoint == "sendMessage"]
    assert e.native_reads == ["outputs/result.txt"]
