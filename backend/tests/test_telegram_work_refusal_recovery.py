"""Conclusive Work refusals can refresh changed previews without rewriting Acts."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_control_admission import admissions, review_confirmation, stage
from test_telegram_work_interaction_delivery import guard_for, open_view, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typing_prompts import clear_old
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.contract import AgentConflict
from deerflow.agent_instances.human_input_contract import Respond
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def refused(e, monkeypatch, *, unknown=False):
    record, request, seen, callback = await review_confirmation(e)
    controller = admissions(e)
    act = await stage(e, controller, callback)
    original = e.human.work.command

    async def refuse(**kwargs):
        if unknown:
            raise RuntimeError("Unknown service result")
        raise AgentConflict("The attempt is still closing", reason="attempt-unresolved")

    monkeypatch.setattr(e.human.work, "command", refuse)
    result = await controller.acts.check(act.id)
    monkeypatch.setattr(e.human.work, "command", original)
    assert result.state == "pending" and result.reason == (None if unknown else "attempt-unresolved")
    return controller, record, request, seen, callback, result


async def comment(e, request):
    await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="Please inspect the revised source before accepting."))


async def refresh(e, controller, callback):
    from app.telegram_bots.work_refusal_recovery import WorkRefusalRecovery

    return await WorkRefusalRecovery(controller).refresh(e.link, callback.id, chat_id=e.link.chat_id, message_id=callback.message_id, message_thread_id=None, guard=await guard_for(e))


@pytest.mark.asyncio
async def test_changed_refused_confirmation_gets_full_preview_and_new_operation(work_delivery, monkeypatch):
    e = work_delivery
    controller, record, request, seen, callback, act = await refused(e, monkeypatch)
    await comment(e, request)
    identity = await refresh(e, controller, callback)
    assert identity and identity != e.identity
    assert await refresh(e, controller, callback) == identity
    async with e.db.sessions() as reader:
        old = await reader.get(TelegramActRow, act.id)
        row = await reader.get(TelegramOutboxRow, identity)
        assert old.state == "conflict" and old.cancel_requested_at is not None
        assert old.body["expected_revision"] == seen["work_revision"]
        assert row.class_ == "reply" and row.state == "pending"
        assert row.payload["work_request_basis"]["source"]["seen"]["revision"] > seen["revision"]
        assert "revised source" in "".join(part["html"] for part in row.parts)
        assert all(control.consumed_at is not None for control in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == callback.group_id)))
    assert (await controller.acts.check(act.id)).state == "conflict"
    e.identity = identity
    await clear_old(e)
    await dispatch(e, 803)
    first = await selected(e, "accept_open")
    e.identity = await open_view(e, first)
    await clear_old(e)
    await dispatch(e, 804)
    fresh = await stage(e, controller, await selected(e, "accept_confirm"))
    assert fresh.id != act.id and fresh.body["expected_revision"] > act.body["expected_revision"]
    assert (await controller.acts.check(fresh.id)).state == "done"
    assert (await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"]))["status"] == "completed"


@pytest.mark.asyncio
async def test_unchanged_refusal_preserves_same_controls_and_operation(work_delivery, monkeypatch):
    e = work_delivery
    controller, _record, _request, _seen, callback, act = await refused(e, monkeypatch)
    assert await refresh(e, controller, callback) is None
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, act.id)).cancel_requested_at is None
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is None
    assert (await controller.acts.check(act.id)).state == "done"


@pytest.mark.asyncio
async def test_unknown_attempt_is_never_superseded_by_a_new_preview(work_delivery, monkeypatch):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    controller, _record, request, _seen, callback, act = await refused(e, monkeypatch, unknown=True)
    await comment(e, request)
    with pytest.raises(CallbackRefused):
        await refresh(e, controller, callback)
    async with e.db.sessions() as reader:
        old = await reader.get(TelegramActRow, act.id)
        assert old.state == "pending" and old.cancel_requested_at is None
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is None


@pytest.mark.asyncio
async def test_retry_starting_before_refresh_writer_prevents_supersession(work_delivery, monkeypatch):
    from contextlib import asynccontextmanager

    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    controller, _record, request, _seen, callback, act = await refused(e, monkeypatch)
    await comment(e, request)
    original = controller.callbacks.writer
    changed = []

    @asynccontextmanager
    async def begin_retry(*args, **kwargs):
        if not changed:
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramActRow, act.id)
                row.reason = None
                row.attempts += 1
            changed.append(True)
        async with original(*args, **kwargs) as writer:
            yield writer

    monkeypatch.setattr(controller.callbacks, "writer", begin_retry)
    with pytest.raises(CallbackRefused):
        await refresh(e, controller, callback)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, act.id)
        assert saved.reason is None and saved.cancel_requested_at is None
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is None


@pytest.mark.asyncio
async def test_late_grant_withdrawal_rolls_back_retirement_and_fresh_preview(work_delivery, monkeypatch):
    from app.telegram_bots.work_interaction_delivery import WorkInteractionPlans
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    controller, _record, request, _seen, callback, act = await refused(e, monkeypatch)
    await comment(e, request)
    original = WorkInteractionPlans._publish
    changed = []

    async def withdraw(self, writer, *args, **kwargs):
        result = await original(self, writer, *args, **kwargs)
        (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 3
        await writer.flush()
        changed.append(True)
        return result

    monkeypatch.setattr(WorkInteractionPlans, "_publish", withdraw)
    with pytest.raises(AgentDenied):
        await refresh(e, controller, callback)
    assert changed
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, act.id)
        assert saved.state == "pending" and saved.reason == "attempt-unresolved" and saved.cancel_requested_at is None
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is None
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("notice:work-refusal-refresh:"))))


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["reason", "identity"])
async def test_refresh_dispatch_requires_its_exact_consumed_origin(work_delivery, monkeypatch, damage):
    e = work_delivery
    controller, _record, request, _seen, callback, _act = await refused(e, monkeypatch)
    await comment(e, request)
    identity = await refresh(e, controller, callback)
    await clear_old(e)
    async with e.db.sessions.begin() as writer:
        old = await writer.get(TelegramCallbackRow, callback.id)
        if damage == "reason":
            old.closed_reason = "work-preview"
        else:
            row = await writer.get(TelegramOutboxRow, identity)
            basis = row.payload["work_request_basis"]
            row.payload = {**row.payload, "work_request_basis": {**basis, "origin": {**basis["origin"], "message_id": 999}}}
    before = len(e.channel.bot.request.calls)
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "skipped"


@pytest.mark.asyncio
async def test_protected_multipart_refresh_is_a_reply_with_controls_only_after_full_ack(work_delivery, monkeypatch):
    from datetime import UTC, datetime

    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow, TelegramPersonSettingsRow

    e = work_delivery
    controller, _record, request, _seen, callback, _act = await refused(e, monkeypatch)
    for label in ("First", "Last"):
        await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text=label + " review detail " * 250))
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = "none"
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    identity = await refresh(e, controller, callback)
    await clear_old(e)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, identity)
        count = len(parent.parts)
        assert count > 1
        group = parent.parts[-1]["callback_group_id"]
    before = len(e.channel.bot.request.calls)
    for index in range(count):
        await dispatch(e, 810 + index)
        async with e.db.sessions() as reader:
            parent = await reader.get(TelegramOutboxRow, identity)
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group)))
            assert controls
            assert all(row.payload["presentation_pending"] == (index < count - 1) for row in controls)
            if index == count - 1:
                assert parent.state == "sent", (parent.failed_reason, e.dispatch_errors)
    calls = [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]
    assert len(calls) == count and all(call.parameters["protect_content"] for call in calls)
    assert "First review detail" in calls[0].parameters["text"]
    assert "Last review detail" in "".join(call.parameters["text"] for call in calls)
