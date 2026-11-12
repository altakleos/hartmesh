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
