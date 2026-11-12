"""Protected review results clear their teaser before publishing any files."""

from copy import deepcopy
from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_result_delivery import prepare_delivery
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.work_result_recovery import WorkResultRecovery
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInstancePolicyRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review", "review_empty"], indirect=True)
@pytest.mark.parametrize("cleanup", ["ok", "retry", "missing"])
async def test_show_clears_teaser_before_protected_files_and_final_controls(work_exports, cleanup):
    from test_telegram_work_callback_dispatch import install_dispatch, press_work
    from test_telegram_work_control_admission import admissions

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans

    e = work_exports
    has_files = bool(e.result["outcome"]["sources"])
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    identity = await prepare_delivery(e)
    sdk = e.channel.bot.request
    message(sdk, identity=1001)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.native_reads
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        assert show.kind == "reveal"
    recovery = WorkResultRecovery(e.plans)
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    scope = dict(chat_id=e.link.chat_id, message_id=1001, message_thread_id=None, guard=guard)
    install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts))
    await press_work(e, show, message_id=1001)
    with pytest.raises(CallbackRefused):
        await recovery.reveal(e.link, show.id, **scope)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["work_request_basis"]["cleanup_id"]
        fresh = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
        assert [part["kind"] for part in fresh] == (["document", "text"] if has_files else ["text"])
        if not has_files:
            from types import SimpleNamespace

            from app.telegram_bots.producer import PermanentDeliveryError, _item
            from app.telegram_bots.work_request_source import frozen

            damaged = _item(row)
            damaged.payload["work_request_basis"]["cleanup_id"] = None
            part = deepcopy(fresh[0])
            part["state"] = "sending"
            damaged.parts[-1]["state"] = "sending"
            with pytest.raises(PermanentDeliveryError):
                frozen(SimpleNamespace(link=e.link, item=damaged, part=part))
    e.delivery_clock.advance(2)
    if cleanup == "ok":
        message(sdk, "editMessageReplyMarkup", identity=1001)
    else:
        sdk.enqueue(
            "editMessageReplyMarkup",
            status=503 if cleanup == "retry" else 400,
            body={"ok": False, "error_code": 503 if cleanup == "retry" else 400, "description": "Retry later" if cleanup == "retry" else "Bad Request: message to edit not found"},
        )
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.native_reads
    if cleanup == "retry":
        e.delivery_clock.advance(60)
        message(sdk, "editMessageReplyMarkup", identity=1001)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert not e.native_reads
    if has_files:
        message(sdk, "sendDocument", identity=1002)
    message(sdk, identity=1003)
    for _ in range(2 if has_files else 1):
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    from telegram.error import BadRequest, NetworkError

    assert [type(error) for error in e.dispatch_errors] == ([] if cleanup == "ok" else [NetworkError if cleanup == "retry" else BadRequest])
    calls = [call for call in sdk.calls if call.endpoint in {"sendDocument", "sendMessage"}]
    assert [call.endpoint for call in calls] == (["sendMessage", "sendDocument", "sendMessage"] if has_files else ["sendMessage", "sendMessage"])
    assert all(call.parameters["protect_content"] for call in calls)
    assert e.native_reads == (["outputs/result.txt"] if has_files else [])
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.failed_reason, row.parts)
        assert row.parts[0]["message_ids"] == [1001]
        for control in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])):
            assert control.message_id == 1003 and not control.payload["presentation_pending"]


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
@pytest.mark.parametrize("damage", ["ledger", "receipt"])
async def test_show_refuses_corrupt_teaser_without_consuming_control(work_exports, damage):
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_exports
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    identity = await prepare_delivery(e)
    message(e.channel.bot.request, identity=1001)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        show = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        if damage == "ledger":
            payload = deepcopy(row.payload)
            payload["work_result_ledger"]["roots"] = []
            row.payload = payload
        else:
            parts = deepcopy(row.parts)
            parts[0]["sent_at"] = None
            row.parts = parts
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    with pytest.raises((ProductSourceRetired, CallbackRefused)):
        await WorkResultRecovery(e.plans).reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=1001, message_thread_id=None, guard=guard)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, show.id)).consumed_at is None
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.payload["work_request_basis"]["concealed"]
    assert not e.native_reads


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
@pytest.mark.parametrize("change", ["revision", "read", "read_tamper", "language"])
async def test_refresh_after_file_ack_sends_current_card_without_uploading_again(work_exports, change):
    from deerflow.persistence.agent_instances.work import AgentWorkRow
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow
    from deerflow.spaces.contract import Permission

    e = work_exports
    identity = await prepare_delivery(e)
    e.channel._services.work_result_sources.recovery = WorkResultRecovery(e.plans)
    sdk = e.channel.bot.request
    message(sdk, "sendDocument", identity=1101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        if change == "revision":
            (await writer.get(AgentWorkRow, e.result["id"])).revision += 1
        elif change in {"read", "read_tamper"}:
            (await writer.get(SpaceGrantRow, (e.home.id, "human", e.actor.subject_id))).permissions = int(Permission.EXPORT)
        else:
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    if change == "read_tamper":
        async with e.db.sessions.begin() as writer:
            work = await writer.get(AgentWorkRow, e.result["id"])
            outcome = deepcopy(work.outcome)
            outcome["sources"][0]["path"] = "outputs/other.txt"
            work.outcome = outcome
    message(sdk, identity=1102)
    e.delivery_clock.advance(2)
    async with e.db.sessions() as reader:
        state = await reader.get(TelegramOutboxRow, identity)
        before = (state.state, state.failed_reason, state.not_before, state.parts, e.dispatch_errors)
    assert await e.sender.tick() == 1, before
    await e.sender.drain()
    if change == "read_tamper":
        assert not [call for call in sdk.calls if call.endpoint == "sendMessage"]
        return
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, row.failed_reason, e.dispatch_errors)
        assert row.parts[0]["message_ids"] == [1101]
        assert row.payload["presentation_version"] == 1
        assert row.parts[-1]["message_ids"] == [1102]
    assert len([call for call in sdk.calls if call.endpoint == "sendDocument"]) == 1
    assert e.native_reads == ["outputs/result.txt"]
    text = [call.parameters["text"] for call in sdk.calls if call.endpoint == "sendMessage"][-1]
    if change == "read":
        assert "result.txt" not in text


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_album"], indirect=True)
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
    identity = await prepare_delivery(e)
    e.channel._services.work_result_sources.recovery = WorkResultRecovery(e.plans)
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
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_protection_upgrade_requires_new_show_without_relabeling_old_file(work_exports):
    e = work_exports
    identity = await prepare_delivery(e)
    recovery = WorkResultRecovery(e.plans)
    e.channel._services.work_result_sources.recovery = recovery
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
        assert row.state == "sent" and row.payload["work_request_basis"]["concealed"], (row.state, row.failed_reason, e.dispatch_errors)
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
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_anchor_refresh_cannot_reclaim_upload_waiting_for_sdk_response(work_exports):
    import asyncio

    from test_telegram_outbound_upload import message as response

    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_exports
    identity = await prepare_delivery(e)
    recovery = WorkResultRecovery(e.plans)
    e.channel._services.work_result_sources.recovery = recovery
    sdk, released = e.channel.bot.request, asyncio.Event()
    held = sdk.enqueue("sendDocument", result=response(1401), release=released)
    try:
        assert await e.sender.tick() == 1
        await asyncio.wait_for(held.started.wait(), timeout=10)
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentWorkRow, e.result["id"])).revision += 1
        owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
        guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
        assert await recovery.refresh_anchor(e.link, identity, expected_version=0, guard=guard) is None
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.state == "sending" and row.payload["presentation_version"] == 0
            assert row.parts[0]["message_ids"] == []
    finally:
        released.set()
        await e.sender.drain()
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    message(sdk, identity=1402)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, row.failed_reason, e.dispatch_errors)
        assert row.parts[0]["message_ids"] == [1401]
        assert row.payload["presentation_version"] == 1
    assert len([call for call in sdk.calls if call.endpoint == "sendDocument"]) == 1
