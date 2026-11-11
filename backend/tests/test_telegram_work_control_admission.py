"""An acknowledged button group admits one immutable Work service operation."""

import asyncio
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_actions import management_request
from test_telegram_work_interaction_delivery import guard_for, open_view, selected
from test_telegram_work_preview_recovery import dispatch, install
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typing_prompts import clear_old
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import Acts
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


def admissions(e):
    from app.telegram_bots.work_control_admission import WorkControlAdmissions

    acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    return WorkControlAdmissions(e.plans, acts)


async def stage(e, controller, callback):
    return await controller.stage(e.link, callback.id, chat_id=e.link.chat_id, message_id=callback.message_id, message_thread_id=None, guard=await guard_for(e))


async def review_confirmation(e):
    install(e)
    record, request, seen = await management_request(e, "review")
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="review-action")
    await dispatch(e, 801)
    first = await selected(e, "accept_open")
    identity = await open_view(e, first)
    await clear_old(e)
    await dispatch(e, 802)
    e.identity = identity
    return record, request, seen, await selected(e, "accept_confirm")


@pytest.mark.asyncio
async def test_second_tap_stages_exact_acceptance_then_worker_applies_once(work_delivery):
    from app.telegram_bots.work_actions import ActsWorker

    e = work_delivery
    record, request, seen, callback = await review_confirmation(e)
    controller = admissions(e)
    act = await stage(e, controller, callback)
    assert act.id == callback.id and act.kind == "accept" and act.state == "pending" and act.attempts == 0
    assert act.body["acknowledge_unchecked_sources"] is True
    assert act.body["expected_revision"] == seen["work_revision"]
    assert act.body["request_basis"]["revision"] == seen["revision"]
    assert (await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"]))["status"] == "submitted"
    assert (await stage(e, controller, callback)).id == act.id
    async with e.db.sessions() as reader:
        assert all(row.payload["work_action_id"] == act.id for row in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == callback.group_id)))
    # A restart after admission but before the immediate service call leaves a
    # complete operation for the existing bounded receipt worker.
    fresh = admissions(e)
    e.clock.advance(60)
    await ActsWorker(fresh.acts).tick()
    result = await fresh.acts.check(act.id)
    assert result.state == "done"
    assert (await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"]))["status"] == "completed"
    assert (await stage(e, fresh, callback)).id == act.id
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1


@pytest.mark.asyncio
async def test_two_answer_choices_reserve_one_group_before_any_response(work_delivery):
    from deerflow.agent_instances.human_input_contract import CreateRequest
    from deerflow.agent_instances.work_contract import DelegateWork

    e = work_delivery
    install(e)
    work = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Select a guide", success_criteria="A reference"))
    request = await e.human.create(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=work["id"],
        request=CreateRequest(
            operation_id=uuid4().hex,
            expected_work_revision=1,
            expected_assignment_revision=1,
            purpose="information",
            question="Which guide?",
            reason="A source is needed",
            expected_response="A reference",
            recipient_id=e.link.owner_user_id,
            choices=["Guide A", "Guide B"],
        ),
    )
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="choice-admission")
    await dispatch(e, 801)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        choices = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "hi_choice")))
    assert len(choices) == 2
    controller = admissions(e)
    results = await asyncio.gather(*(stage(e, controller, callback) for callback in choices), return_exceptions=True)
    accepted = [result for result in results if not isinstance(result, BaseException)]
    assert len(accepted) == 1, results
    assert (await controller.acts.check(accepted[0].id)).state == "done"
    responses = (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]
    assert len(responses) == 1 and responses[0]["choice"] == accepted[0].body["choice"]
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["revision", "grant", "pause"])
async def test_late_admission_change_rolls_back_choice_and_journal(work_delivery, monkeypatch, change):
    from datetime import UTC, datetime

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import DeliveryRetry
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = work_delivery
    record, _request, _seen, callback = await review_confirmation(e)
    controller = admissions(e)
    original = controller.acts._stage_work_act
    changed = []

    async def withdraw(*args, **kwargs):
        result = await original(*args, **kwargs)
        writer = kwargs["session"]
        if change == "revision":
            (await writer.get(AgentWorkRow, record["id"])).revision += 1
        elif change == "grant":
            (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 3
        else:
            (await writer.get(TelegramBotLinkRow, e.link.id)).acts_paused_since = datetime.now(UTC)
        await writer.flush()
        changed.append(True)
        return result

    monkeypatch.setattr(controller.acts, "_stage_work_act", withdraw)
    with pytest.raises({"revision": DeliveryRetry, "grant": AgentDenied, "pause": CallbackRefused}[change]):
        await stage(e, controller, callback)
    assert changed
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        assert all("work_action_id" not in row.payload for row in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == callback.group_id)))


@pytest.mark.asyncio
async def test_changed_review_after_admission_keeps_original_body_and_applies_nothing(work_delivery):
    from deerflow.agent_instances.human_input_contract import Respond

    e = work_delivery
    record, request, seen, callback = await review_confirmation(e)
    controller = admissions(e)
    act = await stage(e, controller, callback)
    await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="Another source must be checked"))
    result = await controller.acts.check(act.id)
    assert result.state == "conflict"
    assert result.body == act.body and result.id == act.id
    assert result.body["expected_revision"] == seen["work_revision"]
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"])
    assert current["status"] == "submitted" and current["outcome_id"] == seen["outcome_id"]


@pytest.mark.asyncio
async def test_unshown_first_tap_cannot_admit_acceptance(work_delivery):
    e = work_delivery
    install(e)
    _record, request, _seen = await management_request(e, "review")
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="first-tap")
    await dispatch(e, 801)
    controller = admissions(e)
    with pytest.raises(ValueError):
        await stage(e, controller, await selected(e, "accept_open"))
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_stale_preview_never_admits_a_new_operation(work_delivery):
    from deerflow.agent_instances.contract import AgentConflict
    from deerflow.agent_instances.human_input_contract import Respond

    e = work_delivery
    _record, request, _seen, callback = await review_confirmation(e)
    await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="Review a new comment"))
    with pytest.raises(AgentConflict):
        await stage(e, admissions(e), callback)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_mutated_saved_body_is_not_treated_as_identical(work_delivery):
    from sqlalchemy.orm.attributes import flag_modified

    e = work_delivery
    _record, _request, _seen, callback = await review_confirmation(e)
    controller = admissions(e)
    act = await stage(e, controller, callback)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, act.id)
        row.body = {**row.body, "expected_assignment_revision": True}
        flag_modified(row, "body")
    with pytest.raises(ValueError, match="act-source-conflict"):
        await stage(e, controller, callback)


@pytest.mark.asyncio
async def test_decision_confirmation_records_only_the_displayed_choice(work_delivery):
    from deerflow.agent_instances.human_input_contract import CreateRequest
    from deerflow.agent_instances.work_contract import DelegateWork

    e = work_delivery
    install(e)
    record = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Choose a guide", success_criteria="One selection"))
    request = await e.human.create(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=record["id"],
        request=CreateRequest(
            operation_id=uuid4().hex,
            expected_work_revision=1,
            expected_assignment_revision=1,
            purpose="decision",
            question="Which guide?",
            reason="A choice is needed",
            expected_response="One decision",
            recipient_id=e.link.owner_user_id,
            choices=["Guide A", "Guide B"],
        ),
    )
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="decision-admission")
    await dispatch(e, 801)
    first = await selected(e, "decide_choice")
    e.identity = await open_view(e, first)
    await clear_old(e)
    await dispatch(e, 802)
    callback = await selected(e, "decide_confirm")
    controller = admissions(e)
    act = await stage(e, controller, callback)
    assert act.kind == "decide" and act.body["note"] == first.payload["decision"]
    assert (await controller.acts.check(act.id)).state == "done"
    current = await e.human.get(actor=e.actor, request_id=request["id"])
    assert current["state"] == "closed"


@pytest.mark.asyncio
async def test_withdrawn_outer_lifetime_rolls_back_admission(work_delivery, monkeypatch):
    e = work_delivery
    _record, _request, _seen, callback = await review_confirmation(e)
    controller = admissions(e)
    live = [True]
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: live[0])
    original = controller.acts._stage_work_act

    async def withdraw(*args, **kwargs):
        result = await original(*args, **kwargs)
        live[0] = False
        return result

    monkeypatch.setattr(controller.acts, "_stage_work_act", withdraw)
    with pytest.raises(ValueError, match="publication-stale"):
        await controller.stage(e.link, callback.id, chat_id=e.link.chat_id, message_id=callback.message_id, message_thread_id=None, guard=guard)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        assert "work_action_id" not in (await reader.get(TelegramCallbackRow, callback.id)).payload


@pytest.mark.asyncio
async def test_disappearing_replay_journal_never_recreates_an_operation(work_delivery, monkeypatch):
    from contextlib import asynccontextmanager

    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    _record, _request, _seen, callback = await review_confirmation(e)
    controller = admissions(e)
    act = await stage(e, controller, callback)
    assert (await controller.acts.check(act.id)).state == "done"
    original = controller.callbacks.writer
    removed = []

    @asynccontextmanager
    async def delete_before_writer(*args, **kwargs):
        if not removed:
            async with e.db.sessions.begin() as writer:
                await writer.delete(await writer.get(TelegramActRow, act.id))
            removed.append(True)
        async with original(*args, **kwargs) as writer:
            yield writer

    monkeypatch.setattr(controller.callbacks, "writer", delete_before_writer)
    with pytest.raises(CallbackRefused):
        await stage(e, controller, callback)
    assert removed
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, act.id) is None
