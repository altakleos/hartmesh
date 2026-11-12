"""Closed reviews keep typed notes unsubmitted and replacement previews explicit."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_callback_dispatch import install_dispatch, press_work
from test_telegram_work_control_admission import admissions, stage
from test_telegram_work_input_refresh import deliver_refreshed
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_retained_drafts import draft
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
from deerflow.agent_instances.human_input_contract import CreateRequest
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


async def close_review(e, request, *, replacement):
    source = await e.reader.read_request(e.link, request["id"])
    command = admissions(e).acts._changes_command(operation_id=uuid4().hex, request_id=request["id"], note="Review the replacement instead.", seen=source.seen)
    await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=request["work_id"], request=command)
    if not replacement:
        return None
    from test_agent_work_records import submit_fixture

    work = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=request["work_id"])
    await submit_fixture(e.db.sessions, work)
    return await e.human.create(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=request["work_id"],
        request=CreateRequest(
            operation_id=uuid4().hex,
            expected_work_revision=work["revision"],
            expected_assignment_revision=work["assignment_revision"],
            purpose="review",
            question="Review the replacement result",
            reason="The previous result needed changes",
            expected_response="Accept or request changes",
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", [False, True])
async def test_retired_review_callback_retires_old_controls_and_keeps_note(work_delivery, monkeypatch, replacement):
    e = work_delivery
    request, _inbox, origin = await draft(e, monkeypatch)
    control = await selected(e, "changes_confirm")
    old_identity = e.identity
    current = await close_review(e, request, replacement=replacement)
    controls = admissions(e)
    install_dispatch(e, WorkOutcomePlans(e.plans, controls.acts))
    await press_work(e, control, message_id=803)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is not None
        mark = await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
        assert mark.route_basis["work_input"]["text"] == origin.route_basis["work_input"]["text"]
        assert mark.route_complete is not replacement
        assert not list(await reader.scalars(select(TelegramActRow)))
        if replacement:
            fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.id != old_identity, TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == origin.operation_ids[0]))
            assert fresh is not None and fresh.request_id == current["id"]
            assert fresh.payload["work_request_basis"]["interaction"]["note"] == origin.route_basis["work_input"]["text"]
            assert "This question was replaced." in "".join(part["html"] for part in fresh.parts)
        else:
            fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:work-review-retired:" + origin.operation_ids[0]))
            assert fresh is not None and fresh.parts[-1]["source_ref"]["key"] == "stale.closed"
            assert not any(part.get("callback_group_id") for part in fresh.parts)
        e.identity = fresh.id
    await deliver_refreshed(e, 804)
    if replacement:
        fresh = await selected(e, "changes_confirm")
        saved = await stage(e, controls, fresh)
        assert saved.request_id == current["id"] and saved.body["note"] == origin.route_basis["work_input"]["text"]
        assert (await controls.acts.check(saved.id)).state == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", [False, True])
async def test_text_arriving_after_review_closure_is_kept_without_submission(work_delivery, monkeypatch, replacement):
    from test_telegram_inbox import incoming
    from test_telegram_work_input_capture import prompt
    from test_telegram_work_preview_recovery import dispatch
    from test_telegram_work_typed_input import install_inputs

    from app.telegram_bots import inbox

    e = work_delivery
    request, _prompt, _seen = await prompt(e)
    monkeypatch.setattr(inbox, "_now", e.clock)
    current = await close_review(e, request, replacement=replacement)
    install_inputs(e)
    update = incoming(message=900, update_id=1900, account=e.link.chat_id, reply=802)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_basis["work_input"]["text"] == update.message.text
        assert mark.route_complete is not replacement
        assert not list(await reader.scalars(select(TelegramActRow)))
        if replacement:
            fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == mark.operation_ids[0]))
            assert fresh is not None and fresh.request_id == current["id"]
        else:
            fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:work-review-retired:" + mark.operation_ids[0]))
        e.identity = fresh.id
    await dispatch(e, 803)
    assert not e.dispatch_errors, e.dispatch_errors


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_admitted_old_confirmation_is_not_reinterpreted_after_closure(work_delivery, monkeypatch, missing):
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.work_review_recovery import WorkReviewRecovery

    e = work_delivery
    request, _inbox, origin = await draft(e, monkeypatch)
    control = await selected(e, "changes_confirm")
    action = await stage(e, admissions(e), control)
    if missing:
        async with e.db.sessions.begin() as writer:
            await writer.delete(await writer.get(TelegramActRow, action.id))
    await close_review(e, request, replacement=True)
    with pytest.raises(CallbackRefused):
        await WorkReviewRecovery(e.plans).from_callback(e.link, control.id, chat_id=e.link.chat_id, message_id=803, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is None
        assert not (await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))).route_complete
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.id != e.identity, TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == origin.operation_ids[0])))


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["input", "source"])
async def test_retirement_final_guard_rolls_back_note_settlement(work_delivery, monkeypatch, damage):
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired
    from app.telegram_bots.work_review_recovery import WorkReviewRecovery
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    request, _inbox, origin = await draft(e, monkeypatch)
    control = await selected(e, "changes_confirm")
    await close_review(e, request, replacement=False)
    base = await guard_for(e)
    changed = []

    class Drift:
        @property
        def captured_link(self):
            return base.captured_link

        def current(self):
            return base.current()

        async def check(self, writer):
            current = await base.check(writer)
            mark = await writer.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
            if mark.route_complete and not changed:
                changed.append(True)
                if damage == "input":
                    mark.sanitized_input = {"text": "Changed after settlement"}
                else:
                    work = await writer.get(AgentWorkRow, request["work_id"])
                    work.revision += 1
                await writer.flush()
            return current

    with pytest.raises((ProductSourceRetired, DeliveryRetry)):
        await WorkReviewRecovery(e.plans).recover(e.link, origin, guard=Drift())
    assert changed
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
        assert not mark.route_complete
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is None
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:work-review-retired:" + origin.operation_ids[0])))


@pytest.mark.asyncio
async def test_newly_eligible_replacement_prevents_final_closed_notice(work_delivery, monkeypatch):
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.producer import DeliveryRetry
    from app.telegram_bots.work_review_recovery import WorkReviewRecovery
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    request, _inbox, origin = await draft(e, monkeypatch)
    await close_review(e, request, replacement=True)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
    base = await guard_for(e)
    changed = []

    class Restore:
        @property
        def captured_link(self):
            return base.captured_link

        def current(self):
            return base.current()

        async def check(self, writer):
            current = await base.check(writer)
            mark = await writer.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
            if mark.route_complete and not changed:
                changed.append(True)
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 7
                await writer.flush()
            return current

    with pytest.raises(DeliveryRetry):
        await WorkReviewRecovery(e.plans).recover(e.link, origin, guard=Restore())
    assert changed
    async with e.db.sessions() as reader:
        assert not (await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))).route_complete


@pytest.mark.asyncio
@pytest.mark.parametrize("admitted", ["confirmation", "input"])
async def test_recovered_note_cannot_bypass_a_missing_admission_journal(work_delivery, monkeypatch, admitted):
    from test_telegram_work_input_capture import accept, capture, prompt
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired
    from app.telegram_bots.work_input_admission import WorkInputAdmissions
    from app.telegram_bots.work_input_capture import WorkInputCaptures
    from app.telegram_bots.work_review_recovery import WorkReviewRecovery

    e = work_delivery
    if admitted == "confirmation":
        request, _inbox, origin = await draft(e, monkeypatch)
        action = await stage(e, admissions(e), await selected(e, "changes_confirm"))
    else:
        request, prompt_id, _seen = await prompt(e)
        inbox, mark = await accept(e)
        origin = await capture(e, inbox, mark, prompt_id)
        action = await WorkInputAdmissions(admissions(e), WorkInputCaptures(inbox, e.channel._services.callbacks)).stage(e.link, origin, guard=await guard_for(e))
    async with e.db.sessions.begin() as writer:
        await writer.delete(await writer.get(TelegramActRow, action.id))
    await close_review(e, request, replacement=True)
    with pytest.raises((CallbackRefused, ProductSourceRetired, DeliveryRetry)):
        await WorkReviewRecovery(e.plans).recover(e.link, origin, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert not (await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))).route_complete
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
@pytest.mark.parametrize("reservation", ["original", None, True, 10**40, {}])
async def test_resumed_input_cannot_reopen_a_missing_confirmation_journal(work_delivery, monkeypatch, reservation):
    from test_telegram_work_typed_input import install_inputs

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired

    e = work_delivery
    request, _inbox, origin = await draft(e, monkeypatch)
    action = await stage(e, admissions(e), await selected(e, "changes_confirm"))
    async with e.db.sessions.begin() as writer:
        await writer.delete(await writer.get(TelegramActRow, action.id))
        if reservation != "original":
            parent = await writer.get(TelegramOutboxRow, e.identity)
            for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"])):
                control.payload = {**control.payload, "work_action_id": reservation}
    await close_review(e, request, replacement=True)
    _plans, inputs = install_inputs(e)
    with pytest.raises((CallbackRefused, ProductSourceRetired, DeliveryRetry)):
        await inputs.resume(e.channel, origin)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.id != e.identity, TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == origin.operation_ids[0])))
