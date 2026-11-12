"""A saved comment returns complete current Work controls, never a decision."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_card_routing import test_actual_quoted_card_text_saves_one_answer_or_comment as saved_comment
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["decision", "review"])
async def test_saved_comment_shows_complete_current_request_before_fresh_controls(work_delivery, purpose):
    from test_telegram_work_callback_dispatch import press_work

    e = work_delivery
    await saved_comment(e, purpose)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        assert parent.state == "sent"
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == parent.id)))
        kind = "decide_own" if purpose == "decision" else "accept_open"
        selected = next((row for row in controls if row.kind == kind), None)
        assert selected is not None
        assert selected.message_id == 802 and selected.consumed_at is None
        html = "".join(part["html"] for part in parent.parts)
        assert "Added as a comment" in html and "synthetic accepted text" in html
        actions = list(await reader.scalars(select(TelegramActRow)))
        assert len(actions) == 1 and actions[0].kind == "respond" and actions[0].state == "done"
    await press_work(e, selected, message_id=802)
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
        previews = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["origin"]["callback_id"].as_string() == selected.id)))
        assert len(previews) == 1
        expected = "prompt" if purpose == "decision" else "accept"
        assert previews[0].payload["work_request_basis"]["interaction"]["view"] == expected
        e.identity = previews[0].id
    from test_telegram_work_input_refresh import deliver_refreshed

    await deliver_refreshed(e, 803)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, e.identity)).state == "sent", e.dispatch_errors


@pytest.mark.asyncio
async def test_reply_to_saved_comment_card_creates_only_one_new_comment(work_delivery):
    from test_telegram_inbox import incoming

    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    await saved_comment(e, "review")
    update = incoming(message=901, update_id=2901, account=e.link.chat_id, reply=802)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    await e.channel._services.work_text_inputs.handle_text(e.channel, update, captured_link=e.link)
    async with e.db.sessions() as reader:
        actions = list(await reader.scalars(select(TelegramActRow)))
        assert len(actions) == 2 and all(row.kind == "respond" and row.state == "done" for row in actions)
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id))
        assert mark.route_kind == "work_input" and len(mark.operation_ids) == 1
        assert len({row.id for row in actions}) == 2
        request_id = actions[0].request_id
    assert len((await e.human.responses(actor=e.actor, request_id=request_id))["responses"]) == 2


async def pending_comment(e, *, long=False):
    from test_telegram_inbox import incoming
    from test_telegram_work_card_inputs import card
    from test_telegram_work_typed_input import install_inputs

    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    request = await card(e, "review", long=long)
    async with e.db.sessions.begin() as writer:
        for attempt in await writer.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == request["work_id"])):
            attempt.request = {"expected_revision": 1}
    plans, inputs = install_inputs(e)
    assert await e.channel.handle_update(incoming(message=900, update_id=2900, account=e.link.chat_id, reply=801))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        act = await reader.scalar(select(TelegramActRow))
        assert act is not None and act.state == "done"
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == act.id))
        assert row is not None
        e.identity = e.outcome_identity = row.id
    return plans, inputs, act.id


@pytest.mark.asyncio
async def test_long_comment_card_arms_controls_only_after_complete_delivery(work_delivery):
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_preview_recovery import dispatch

    e = work_delivery
    await pending_comment(e, long=True)
    await clear_outcome_cleanup(e)
    async with e.db.sessions() as reader:
        count = len((await reader.get(TelegramOutboxRow, e.identity)).parts)
    assert count > 1
    for index in range(count):
        await dispatch(e, 820 + index)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, e.identity)
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
            assert controls
            assert all(control.message_id == (820 + index if index == count - 1 else None) for control in controls)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent", e.dispatch_errors
        assert "Complete question. " * 20 in "".join(part["html"] for part in row.parts)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["receipt", "grant", "comment"])
async def test_comment_card_final_source_check_rejects_late_changes(work_delivery, monkeypatch, damage):
    from uuid import uuid4

    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_preview_recovery import dispatch

    import app.telegram_bots.work_outcome_delivery as module
    from deerflow.agent_instances.human_input_contract import Respond
    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    _plans, _inputs, act_id = await pending_comment(e)
    await clear_outcome_cleanup(e)
    original, reached = module.current_attention, []

    async def changed(*args, **kwargs):
        if not reached:
            async with e.db.sessions.begin() as writer:
                act = await writer.get(TelegramActRow, act_id)
                request_id, body = act.request_id, act.body
                if damage == "receipt":
                    event = await writer.scalar(select(HumanInputEventRow).where(HumanInputEventRow.operation_id == act_id))
                    event.receipt = {**event.receipt, "response_id": "not-the-saved-response"}
                elif damage == "grant":
                    (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 3
            if damage == "comment":
                await e.human.respond(
                    actor=e.actor,
                    request_id=request_id,
                    request=Respond(operation_id=uuid4().hex, expected_request_revision=body["expected_request_revision"], expected_assignment_revision=body["expected_assignment_revision"], text="A concurrent comment"),
                )
            reached.append(True)
        return await original(*args, **kwargs)

    monkeypatch.setattr(module, "current_attention", changed)
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 820)
    assert reached
    assert len(e.channel.bot.request.calls) == before
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state != "sent"
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert all(control.message_id is None for control in controls)


@pytest.mark.asyncio
async def test_comment_followup_refresh_is_full_and_protected_with_notifications_off(work_delivery):
    from test_telegram_work_input_refresh import deliver_refreshed
    from test_telegram_work_interaction_delivery import guard_for

    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow, TelegramPersonSettingsRow

    e = work_delivery
    plans, _inputs, act_id = await pending_comment(e)
    previous = e.identity
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=e.clock()))
        settings = await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)
        settings.notify = "none"
    e.identity = await plans.plan(e.link, act_id, guard=await guard_for(e))
    assert e.identity != previous
    await deliver_refreshed(e, 820)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent", e.dispatch_errors
        assert all(part["protect_content"] for part in row.parts)
        assert "synthetic accepted text" in "".join(part["html"] for part in row.parts)
        assert row.parts[-1]["callback_group_id"]
        assert (await reader.get(TelegramOutboxRow, previous)).state == "skipped"
        assert (await reader.get(TelegramActRow, act_id)).attempts == 1
    sent = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
    assert sent.parameters["protect_content"] is True


@pytest.mark.asyncio
async def test_comment_card_rechecks_referenced_space_before_sdk(work_delivery, monkeypatch):
    from types import SimpleNamespace
    from uuid import uuid4

    from test_telegram_work_interaction_delivery import guard_for
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_preview_recovery import dispatch

    import app.telegram_bots.work_outcome_delivery as module
    from deerflow.agent_instances.human_input_contract import Respond
    from deerflow.agent_instances.work_contract import WorkSource
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.spaces.registry import SpaceRegistry

    e = work_delivery
    plans, _inputs, act_id = await pending_comment(e)
    e.human.agents.files.registry = SimpleNamespace(_view=SpaceRegistry._view)
    async with e.db.sessions.begin() as writer:
        writer.add(SpaceGrantRow(space_id=e.instance.home_id, principal_kind="human", subject_id=e.link.owner_user_id, permissions=1))
        act = await writer.get(TelegramActRow, act_id)
        request_id, body = act.request_id, act.body
    await e.human.respond(
        actor=e.actor,
        request_id=request_id,
        request=Respond(
            operation_id=uuid4().hex,
            expected_request_revision=body["expected_request_revision"],
            expected_assignment_revision=body["expected_assignment_revision"],
            text="Referenced comment",
            sources=[WorkSource(space_id=e.instance.home_id, path="private-guide.txt")],
        ),
    )
    e.identity = e.outcome_identity = await plans.plan(e.link, act_id, guard=await guard_for(e))
    await clear_outcome_cleanup(e)
    original, reached = module.current_attention, []

    async def withdraw(*args, **kwargs):
        async with e.db.sessions.begin() as writer:
            await writer.delete(await writer.get(SpaceGrantRow, (e.instance.home_id, "human", e.link.owner_user_id)))
        reached.append(True)
        return await original(*args, **kwargs)

    monkeypatch.setattr(module, "current_attention", withdraw)
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 820)
    assert reached
    assert len(e.channel.bot.request.calls) == before
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, e.identity)).state != "sent"


@pytest.mark.asyncio
async def test_comment_card_transport_replacement_keeps_comment_basis(work_delivery):
    from test_telegram_work_interaction_delivery import guard_for
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_preview_recovery import dispatch

    e = work_delivery
    plans, _inputs, act_id = await pending_comment(e, long=True)
    original_id = e.identity
    await clear_outcome_cleanup(e)
    await dispatch(e, 820)
    e.clock.advance(10)
    e.channel.bot.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message thread not found"})
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, original_id)
        assert row.failed_reason == "work-outcome-refresh-required" and row.state == "failed"
        assert row.parts[0]["message_ids"] == [820]
    e.identity = await plans.plan(e.link, act_id, guard=await guard_for(e))
    assert e.identity != original_id
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.payload["work_outcome_basis"]["version"] == 5
        count = len(row.parts)
    for index in range(count):
        await dispatch(e, 830 + index)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent", e.dispatch_errors
        assert (await reader.get(TelegramActRow, act_id)).attempts == 1
        assert (await reader.get(TelegramOutboxRow, original_id)).parts[0]["message_ids"] == [820]


@pytest.mark.asyncio
async def test_persisted_plain_comment_acknowledgment_remains_deliverable(work_delivery):
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_preview_recovery import dispatch

    from app.telegram_bots.work_outcome_delivery import materialize

    e = work_delivery
    plans, _inputs, act_id = await pending_comment(e)
    # Construct the exact closed V1 plan generated before full comment cards.
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, e.identity)
        basis = {key: value for key, value in row.payload["work_outcome_basis"].items() if key != "comment"}
        basis["version"] = 1
        parts, keyboard, _ = materialize(e.link, basis["context"], locale=basis["locale"], timezone=basis["timezone"], product=plans.product)
        basis.update(parts=parts, keyboard=keyboard)
        row.payload = {**row.payload, "work_outcome_basis": basis, "product_keyboard_basis": keyboard}
        saved = plans.outbox._parts(parts, 0)
        if keyboard:
            group = await plans.callbacks.write_group(e.link, keyboard, anchor_outbox_id=row.id, presentation_version=0, chat_id=e.link.chat_id, message_id=None, message_thread_id=None, session=writer)
            saved[-1]["callback_group_id"] = group.group_id
        row.parts = saved
    await clear_outcome_cleanup(e)
    await dispatch(e, 820)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent", e.dispatch_errors
        assert row.payload["work_outcome_basis"]["version"] == 1
        assert (await reader.get(TelegramActRow, act_id)).attempts == 1
