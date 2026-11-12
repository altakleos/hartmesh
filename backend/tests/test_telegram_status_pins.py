"""Only complete acknowledged Work status messages can own a silent pin."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_bot_adapter import message
from test_telegram_status_updates import progress, shown
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@pytest.mark.asyncio
async def test_status_pin_uses_exact_ack_and_is_silent_and_idempotent(work_delivery):
    from app.telegram_bots.work_status_pins import WorkStatusPins

    e = work_delivery
    root, _work, _execution, _scope, _run, _worker, clock, updates = await shown(e)
    pins = WorkStatusPins(updates)
    e.channel._services.work_status_pins = pins
    identity = await pins.plan(e.link, root.id)
    assert identity and await pins.plan(e.link, root.id) == identity
    e.channel.bot.request.enqueue("pinChatMessage")
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "pinChatMessage"]
    assert len(calls) == 1 and calls[0].parameters == {"chat_id": e.link.chat_id, "message_id": 3101, "disable_notification": True}
    assert await pins.plan(e.link, root.id) is None
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"


@pytest.mark.asyncio
async def test_status_pin_refuses_incomplete_message_proof(work_delivery):
    from app.telegram_bots.work_status_pins import WorkStatusPins

    e = work_delivery
    root, _work, _execution, _scope, _run, _worker, _clock, updates = await shown(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, root.id)
        row.parts = [{**part, "message_ids": []} for part in row.parts]
    with pytest.raises((ProductSourceRetired, PermanentDeliveryError)):
        await WorkStatusPins(updates).plan(e.link, root.id)


@pytest.mark.asyncio
async def test_status_terminal_unpins_only_its_own_message(work_delivery):
    from uuid import uuid4

    from app.telegram_bots.work_status_pins import WorkStatusPins
    from deerflow.agent_instances.work_execution_contract import ReportWork

    e = work_delivery
    root, _work, execution, scope, run, _worker, clock, updates = await shown(e)
    pins = WorkStatusPins(updates)
    e.channel._services.work_status_pins = pins
    await pins.plan(e.link, root.id)
    e.channel.bot.request.enqueue("pinChatMessage")
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="Verified the findings."))
    await scope.finish(execution, run, settled=True)
    from datetime import timedelta

    from app.telegram_bots.retention import _pinned_deliveries

    async with e.db.sessions() as reader:
        assert root.id in await _pinned_deliveries(reader, clock.now() + timedelta(days=40), 180)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, root.id)
        saved_steps, tail = row.payload["status_pin_steps"], row.payload["status_pin_tail"]
        row.payload = {**row.payload, "status_pin_steps": None}
    async with e.db.sessions() as reader:
        assert {root.id, tail} <= await _pinned_deliveries(reader, clock.now() + timedelta(days=40), 180)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, root.id)
        row.payload = {**row.payload, "status_pin_steps": saved_steps}
    identity = await pins.plan(e.link, root.id)
    assert identity
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.state, row.sent_at, row.closed_at = "sent", clock.now(), clock.now()
    async with e.db.sessions() as reader:
        assert root.id in await _pinned_deliveries(reader, clock.now() + timedelta(days=40), 180)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.state, row.sent_at, row.closed_at = "pending", None, None
    e.channel.bot.request.enqueue("unpinChatMessage")
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "unpinChatMessage"]
    assert len(calls) == 1 and calls[0].parameters == {"chat_id": e.link.chat_id, "message_id": 3101}
    assert await pins.plan(e.link, root.id) is None
    async with e.db.sessions() as reader:
        assert root.id not in await _pinned_deliveries(reader, clock.now() + timedelta(days=40), 180)


@pytest.mark.asyncio
@pytest.mark.parametrize("failed", [False, True])
async def test_status_replacement_moves_pin_after_complete_ack(work_delivery, failed):
    from app.telegram_bots.work_status_pins import WorkStatusPins
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    pins = WorkStatusPins(updates)
    e.channel._services.work_status_pins = pins
    await pins.plan(e.link, root.id)
    if failed:
        e.channel.bot.request.enqueue("pinChatMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message to pin not found"})
    else:
        e.channel.bot.request.enqueue("pinChatMessage")
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    change = await progress(e, execution, scope, worker, "Current private progress.")
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.link.owner_user_id, updated_at=clock.now()))
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    assert await pins.plan(e.link, root.id) is None
    message(e.channel.bot.request, identity=4101)
    await e.sender.tick()
    await e.sender.drain()
    for endpoint in ("unpinChatMessage", "pinChatMessage"):
        assert await pins.plan(e.link, root.id)
        e.channel.bot.request.enqueue(endpoint)
        clock.advance(2)
        await e.sender.tick()
        await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint in {"pinChatMessage", "unpinChatMessage"}]
    assert [(call.endpoint, call.parameters["message_id"]) for call in calls] == [("pinChatMessage", 3101), ("unpinChatMessage", 3101), ("pinChatMessage", 4101)]


@pytest.mark.asyncio
@pytest.mark.parametrize("owned", [True, False])
async def test_status_pin_service_message_is_deleted_only_for_an_owned_pin(work_delivery, owned):
    from telegram import Update

    from app.telegram_bots.work_status_pins import WorkStatusPins

    e = work_delivery
    root, _work, _execution, _scope, _run, _worker, clock, updates = await shown(e)
    pins = WorkStatusPins(updates)
    e.channel._services.work_status_pins = pins
    await pins.plan(e.link, root.id)
    e.channel.bot.request.enqueue("pinChatMessage")
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    bot = {"id": e.link.bot_id, "is_bot": True, "first_name": "Offline bot"}
    chat = {"id": e.link.chat_id, "type": "private"}
    update = Update.de_json(
        {"update_id": 4901, "message": {"message_id": 4902, "date": 1893456000, "chat": chat, "from": bot, "pinned_message": {"message_id": 3101 if owned else 9999, "date": 1893456000, "chat": chat, "from": bot, "text": "Saved status"}}},
        e.channel.bot,
    )
    if owned:
        e.channel.bot.request.enqueue("deleteMessage")
    assert await e.channel.handle_update(update)
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "deleteMessage"]
    assert len(calls) == int(owned)
    if owned:
        assert calls[0].parameters == {"chat_id": e.link.chat_id, "message_id": 4902}


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["grant", "claim", "event"])
async def test_status_pin_final_fence_rejects_provider_time_changes(work_delivery, monkeypatch, damage):
    from app.telegram_bots.work_status_pins import WorkStatusPins
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    root, _work, _execution, _scope, _run, _worker, clock, updates = await shown(e)
    pins = WorkStatusPins(updates)
    e.channel._services.work_status_pins = pins
    identity = await pins.plan(e.link, root.id)
    original, calls = pins.reader.settings.get, []

    async def changed(*args, **kwargs):
        settings = await original(*args, **kwargs)
        calls.append(True)
        if len(calls) == 2:
            async with e.db.sessions.begin() as writer:
                if damage == "claim":
                    (await writer.get(TelegramOutboxRow, identity)).claim_id = "different-claim"
                elif damage == "grant":
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 1
                else:
                    (await writer.get(AgentWorkEventRow, root.event_id)).actor_id = "different-person"
        return settings

    monkeypatch.setattr(pins.reader.settings, "get", changed)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len(calls) >= 2
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "pinChatMessage"]


@pytest.mark.asyncio
async def test_stale_pin_is_retired_after_work_ends(work_delivery):
    from uuid import uuid4

    from app.telegram_bots.work_status_pins import WorkStatusPins
    from deerflow.agent_instances.work_execution_contract import ReportWork

    e = work_delivery
    root, _work, execution, scope, run, _worker, clock, updates = await shown(e)
    pins = WorkStatusPins(updates)
    e.channel._services.work_status_pins = pins
    identity = await pins.plan(e.link, root.id)
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="Verified the findings."))
    await scope.finish(execution, run, settled=True)
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "pinChatMessage"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "skipped"


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("sent_at", "not-a-time"), ("dispatched_at", "not-a-time"), ("part_id", "not-an-identity")])
async def test_status_pin_refuses_malformed_saved_receipts(work_delivery, field, value):
    from app.telegram_bots.work_status_pins import WorkStatusPins

    e = work_delivery
    root, _work, _execution, _scope, _run, _worker, clock, updates = await shown(e)
    pins = WorkStatusPins(updates)
    e.channel._services.work_status_pins = pins
    identity = await pins.plan(e.link, root.id)
    e.channel.bot.request.enqueue("pinChatMessage")
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.parts = [{**row.parts[0], field: value}]
    with pytest.raises(ProductSourceRetired):
        await pins.plan(e.link, root.id)


@pytest.mark.asyncio
async def test_failed_exact_unpin_can_be_repaired_without_repinning(work_delivery):
    from uuid import uuid4

    from app.telegram_bots.work_status_pins import WorkStatusPins
    from deerflow.agent_instances.work_execution_contract import ReportWork

    e = work_delivery
    root, _work, execution, scope, run, _worker, clock, updates = await shown(e)
    pins = WorkStatusPins(updates)
    e.channel._services.work_status_pins = pins
    await pins.plan(e.link, root.id)
    e.channel.bot.request.enqueue("pinChatMessage")
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="Verified the findings."))
    await scope.finish(execution, run, settled=True)
    identity = await pins.plan(e.link, root.id)
    e.channel.bot.request.enqueue("unpinChatMessage", status=400, body={"ok": False, "error_code": 400, "description": "Synthetic pin unavailable"})
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "failed"
    assert await pins.repair(e.link, identity)
    e.channel.bot.request.enqueue("unpinChatMessage")
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    assert await pins.plan(e.link, root.id) is None
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "pinChatMessage"]) == 1


@pytest.mark.asyncio
async def test_status_pin_shutdown_drains_a_dispatched_sdk_request(work_delivery):
    import asyncio

    from app.telegram_bots.work_status_pins import WorkStatusPins

    e = work_delivery
    root, _work, _execution, _scope, _run, _worker, clock, updates = await shown(e)
    pins = WorkStatusPins(updates)
    e.channel._services.work_status_pins = pins
    identity = await pins.plan(e.link, root.id)
    release = asyncio.Event()
    call = e.channel.bot.request.enqueue("pinChatMessage", release=release)
    clock.advance(2)
    await e.sender.tick()
    await asyncio.wait_for(call.started.wait(), 3)
    stopping = asyncio.create_task(e.sender.stop())
    try:
        await asyncio.sleep(0.05)
        assert not stopping.done() and not call.cancelled.is_set()
    finally:
        release.set()
        await asyncio.wait_for(stopping, 3)
    assert not call.cancelled.is_set()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.claim_id is None and row.parts[0].get("dispatched_at")


@pytest.mark.asyncio
async def test_skipped_pin_is_retried_after_a_late_display_change(work_delivery, monkeypatch):
    from app.telegram_bots.work_status_pins import WorkStatusPins
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    root, _work, _execution, _scope, _run, _worker, clock, updates = await shown(e)
    pins = WorkStatusPins(updates)
    e.channel._services.work_status_pins = pins
    identity = await pins.plan(e.link, root.id)
    original, reads = pins.reader.settings.get, []

    async def switch(*args, **kwargs):
        result = await original(*args, **kwargs)
        reads.append(True)
        if len(reads) == 2:
            async with e.db.sessions.begin() as writer:
                (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
        return result

    monkeypatch.setattr(pins.reader.settings, "get", switch)
    e.channel.bot.request.enqueue("pinChatMessage")
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "pinChatMessage"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "skipped" and row.parts[0].get("dispatched_at")
    assert await pins.plan(e.link, root.id) not in (None, identity)
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "pinChatMessage"]) == 1


@pytest.mark.asyncio
async def test_status_pin_ledger_cannot_reassign_a_confirmed_message(work_delivery):
    from app.telegram_bots.work_status_pins import WorkStatusPins

    e = work_delivery
    root, _work, _execution, _scope, _run, _worker, clock, updates = await shown(e)
    pins = WorkStatusPins(updates)
    e.channel._services.work_status_pins = pins
    identity = await pins.plan(e.link, root.id)
    e.channel.bot.request.enqueue("pinChatMessage")
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.payload = {**row.payload, "work_status_pin": {**row.payload["work_status_pin"], "message_id": 9999}}
        row.parts = [{**row.parts[0], "target_message_id": 9999, "message_ids": [9999]}]
    with pytest.raises(ProductSourceRetired):
        await pins.plan(e.link, root.id)


@pytest.mark.asyncio
@pytest.mark.parametrize("capsule", [None, [], {"root_id": False}])
async def test_malformed_pin_capsule_retires_without_poisoning_retry(work_delivery, capsule):
    from app.telegram_bots.work_status_pins import WorkStatusPins

    e = work_delivery
    root, _work, _execution, _scope, _run, _worker, clock, updates = await shown(e)
    pins = WorkStatusPins(updates)
    e.channel._services.work_status_pins = pins
    identity = await pins.plan(e.link, root.id)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.payload = {**row.payload, "work_status_pin": capsule}
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "pinChatMessage"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "skipped"
