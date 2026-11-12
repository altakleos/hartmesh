"""Status updates edit the exact acknowledged attempt and coalesce progress."""

from uuid import uuid4

import pytest
from _telegram_bot_test_support import ManualClock
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_status import status_attempt
from test_telegram_work_status_delivery import prepare
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import ProductSourceRetired
from deerflow.agent_instances.work_execution_contract import ReportWork
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow, TelegramOutboxRow


async def shown(e, *, protected=False):
    item, work, execution, scope, run, worker = await status_attempt(e)
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    e.channel._now = clock.now
    if protected:
        async with e.db.sessions.begin() as writer:
            writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.link.owner_user_id, updated_at=clock.now()))
    plans = await prepare(e, item)
    message(e.channel.bot.request, identity=3101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors
    from app.telegram_bots.work_status_updates import WorkStatusUpdates

    updates = WorkStatusUpdates(plans)
    e.channel._services.work_status_update_sources = updates
    return item, work, execution, scope, run, worker, clock, updates


async def progress(e, execution, scope, worker, statement):
    current = (await scope.context(execution))["work"]
    operation = uuid4().hex
    await scope.report(execution, ReportWork(operation_id=operation, expected_revision=current["revision"], action="progress", statement=statement))
    await worker.tick()
    async with e.db.sessions() as reader:
        event = await reader.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == operation))
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "edit:" + event.id))
        return row.id


@pytest.mark.asyncio
async def test_status_edits_coalesce_current_progress_and_obey_thirty_second_floor(work_delivery):
    e = work_delivery
    root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    first = await progress(e, execution, scope, worker, "Checked the first guide.")
    second = await progress(e, execution, scope, worker, "Checked both guides.")
    clock.advance(29)
    assert await updates.materialize(e.link, first) is False
    clock.advance(1)
    assert await updates.materialize(e.link, first) is True
    message(e.channel.bot.request, "editMessageText", identity=3101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(calls) == 1 and calls[0].parameters["message_id"] == 3101
    assert "Checked both guides." in calls[0].parameters["text"]
    with pytest.raises(ProductSourceRetired):
        await updates.materialize(e.link, second)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, root.id)
        assert parent.payload["status_anchor"]["outbox_id"] == first


@pytest.mark.asyncio
async def test_status_edits_skip_an_unopened_protected_preview(work_delivery):
    e = work_delivery
    _root, _work, execution, scope, _run, worker, clock, updates = await shown(e, protected=True)
    change = await progress(e, execution, scope, worker, "Private progress.")
    clock.advance(30)
    with pytest.raises(ProductSourceRetired):
        await updates.materialize(e.link, change)
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]


@pytest.mark.asyncio
async def test_status_refreshes_later_progress_before_dispatch(work_delivery):
    e = work_delivery
    _root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    change = await progress(e, execution, scope, worker, "Earlier progress.")
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    await progress(e, execution, scope, worker, "The latest progress.")
    message(e.channel.bot.request, "editMessageText", identity=3101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(calls) == 1 and "The latest progress." in calls[0].parameters["text"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, change)
        assert row.state == "sent" and row.payload["presentation_version"] == 1


@pytest.mark.asyncio
async def test_status_wrong_edit_acknowledgement_does_not_move_anchor(work_delivery):
    e = work_delivery
    root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    change = await progress(e, execution, scope, worker, "Current progress.")
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    message(e.channel.bot.request, "editMessageText", identity=9999)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, change)
        assert row.state == "failed" and row.failed_reason == "invalid-work-status-receipt"
        assert "status_anchor" not in (await reader.get(TelegramOutboxRow, root.id)).payload


@pytest.mark.asyncio
async def test_status_protection_upgrade_uses_new_message_before_moving_anchor(work_delivery):
    e = work_delivery
    root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    change = await progress(e, execution, scope, worker, "Current private progress.")
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.link.owner_user_id, updated_at=clock.now()))
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    message(e.channel.bot.request, identity=3102)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint in {"sendMessage", "editMessageText"}]
    assert [call.endpoint for call in calls] == ["sendMessage", "sendMessage"]
    assert calls[-1].parameters["protect_content"] is True
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, change)).state == "sent"
        assert (await reader.get(TelegramOutboxRow, root.id)).payload["status_anchor"]["outbox_id"] == change


@pytest.mark.asyncio
async def test_missing_status_target_repairs_by_acknowledged_complete_replacement(work_delivery):
    e = work_delivery
    root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    change = await progress(e, execution, scope, worker, "Current progress.")
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    e.channel.bot.request.enqueue("editMessageText", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message to edit not found"})
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, change)
        assert row.state == "failed" and row.failed_reason == "work-status-refresh-required"
        assert "status_anchor" not in (await reader.get(TelegramOutboxRow, root.id)).payload
    assert await updates.repair(e.link, change)
    message(e.channel.bot.request, identity=3103)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 2 and "Current progress." in calls[-1].parameters["text"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, change)
        assert row.state == "sent" and row.parts[-1]["message_ids"] == [3103]
        assert (await reader.get(TelegramOutboxRow, root.id)).payload["status_anchor"]["outbox_id"] == change


@pytest.mark.asyncio
async def test_status_retention_keeps_original_and_moved_active_attempt_proof(work_delivery):
    from datetime import timedelta

    from app.telegram_bots.retention import _pinned_deliveries

    e = work_delivery
    root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    async with e.db.sessions() as reader:
        assert root.id in await _pinned_deliveries(reader, clock.now() + timedelta(days=8), 180)
    change = await progress(e, execution, scope, worker, "Current progress.")
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    message(e.channel.bot.request, "editMessageText", identity=3101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert {root.id, change} <= await _pinned_deliveries(reader, clock.now() + timedelta(days=8), 180)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["source", "anchor", "history"])
async def test_status_update_malformed_saved_basis_retires_instead_of_retrying(work_delivery, damage):
    e = work_delivery
    _root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    change = await progress(e, execution, scope, worker, "Current progress.")
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, change)
        basis = row.payload["work_status_update"]
        row.payload = {**row.payload, "work_status_update": {**basis, damage: None}}
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, change)).state in {"failed", "skipped"}


@pytest.mark.asyncio
async def test_status_edit_interval_is_rechecked_at_dispatch(work_delivery, monkeypatch):
    from datetime import timedelta

    e = work_delivery
    _root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    change = await progress(e, execution, scope, worker, "Current progress.")
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    message(e.channel.bot.request, "editMessageText", identity=3101)
    with monkeypatch.context() as patch:
        patch.setattr(e.sender.outbox, "clock", lambda: clock.now() - timedelta(seconds=10))
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_status_update_retires_when_no_original_delivery_can_be_used(work_delivery, missing):
    e = work_delivery
    root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    change = await progress(e, execution, scope, worker, "Current progress.")
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, root.id)
        if missing:
            await writer.delete(parent)
        else:
            parent.state = "skipped"
    clock.advance(30)
    with pytest.raises(ProductSourceRetired):
        await updates.materialize(e.link, change)


@pytest.mark.asyncio
async def test_status_multipart_replacement_keeps_complete_progress_and_next_target(work_delivery):
    from app.telegram_bots.split import visible_text

    e = work_delivery
    root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    content = "Verified: " + "🙂" * 3000
    change = await progress(e, execution, scope, worker, content)
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    message(e.channel.bot.request, identity=3111)
    message(e.channel.bot.request, identity=3112)
    message(e.channel.bot.request, identity=3113)
    message(e.channel.bot.request, identity=3114)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    for _ in range(3):
        clock.advance(2)
        await e.sender.tick()
        await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    texts = [visible_text(call.parameters["text"]) for call in calls[1:]]
    assert len(calls) == 5
    assert content in "".join(texts), [(len(text), text[:60], text[-20:], text.count("🙂")) for text in texts]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, change)
        assert row.state == "sent" and [part["message_ids"] for part in row.parts] == [[3111], [3112], [3113], [3114]]
    next_change = await progress(e, execution, scope, worker, "Finished checking sources.")
    clock.advance(30)
    assert await updates.materialize(e.link, next_change)
    message(e.channel.bot.request, identity=3115)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, next_change)).state == "sent"
        assert (await reader.get(TelegramOutboxRow, root.id)).payload["status_anchor"]["outbox_id"] == next_change


@pytest.mark.asyncio
async def test_status_missing_moved_anchor_cannot_fall_back_to_original_message(work_delivery):
    e = work_delivery
    root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    first = await progress(e, execution, scope, worker, "Protected progress.")
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.link.owner_user_id, updated_at=clock.now()))
    clock.advance(30)
    assert await updates.materialize(e.link, first)
    message(e.channel.bot.request, identity=3121)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, root.id)
        assert row.payload["status_anchor"]["outbox_id"] == first
        row.payload = {key: value for key, value in row.payload.items() if key != "status_anchor"}
    second = await progress(e, execution, scope, worker, "Later protected progress.")
    clock.advance(30)
    with pytest.raises(ProductSourceRetired):
        await updates.materialize(e.link, second)


@pytest.mark.asyncio
async def test_status_late_locale_change_waits_for_refresh_instead_of_retiring(work_delivery, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    _root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    change = await progress(e, execution, scope, worker, "Current progress.")
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    original, calls = updates.reader.settings.get, []

    async def changed(*args, **kwargs):
        snapshot = await original(*args, **kwargs)
        calls.append(True)
        if len(calls) == 2:
            async with e.db.sessions.begin() as writer:
                (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
        return snapshot

    monkeypatch.setattr(updates.reader.settings, "get", changed)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, change)).state == "pending"
    message(e.channel.bot.request, "editMessageText", identity=3101)
    for _ in range(2):
        clock.advance(2)
        await e.sender.tick()
        await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, change)
        assert row.state == "sent" and row.payload["work_status_update"]["source"]["locale"] == "zh-CN"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["grant", "event", "claim"])
async def test_status_update_final_authority_refuses_changed_source(work_delivery, monkeypatch, damage):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    _root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    change = await progress(e, execution, scope, worker, "Current progress.")
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    original, calls = updates.reader.settings.get, []

    async def withdraw(*args, **kwargs):
        snapshot = await original(*args, **kwargs)
        calls.append(True)
        if len(calls) == 2:
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramOutboxRow, change)
                if damage == "grant":
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 1
                elif damage == "event":
                    (await writer.get(AgentWorkEventRow, row.event_id)).actor_id = "different-person"
                else:
                    row.claim_id = "different-claim"
        return snapshot

    monkeypatch.setattr(updates.reader.settings, "get", withdraw)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len(calls) >= 2
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]


@pytest.mark.asyncio
async def test_status_repair_cannot_take_an_active_claim(work_delivery):
    e = work_delivery
    _root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    change = await progress(e, execution, scope, worker, "Current progress.")
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, change)
        row.state, row.failed_reason = "failed", "work-status-refresh-required"
        row.claim_id, row.claim_owner, row.claimed_at = "owned-claim", "live-sender", clock.now()
    assert not await updates.repair(e.link, change)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, change)
        assert row.claim_id == "owned-claim" and row.payload["presentation_version"] == 0


@pytest.mark.asyncio
async def test_status_transport_retry_preserves_acknowledged_prefix(work_delivery):
    e = work_delivery
    root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    change = await progress(e, execution, scope, worker, "A" * 4096)
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    message(e.channel.bot.request, identity=3131)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, change)
        first = row.parts[0]
        assert first["message_ids"] == [3131] and len(row.parts) == 3
    e.channel.bot.request.enqueue("sendMessage", status=503, body={"ok": False, "error_code": 503, "description": "Synthetic transient failure"})
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, change)
        assert row.state == "pending" and row.parts[0] == first
        assert "status_anchor" not in (await reader.get(TelegramOutboxRow, root.id)).payload
    for identity in (3132, 3133):
        message(e.channel.bot.request, identity=identity)
        clock.advance(10)
        await e.sender.tick()
        await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, change)
        assert row.state == "sent" and row.parts[0] == first
        assert [part["message_ids"] for part in row.parts] == [[3131], [3132], [3133]]
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert sum(call.parameters["text"] == first["html"] for call in calls) == 1
