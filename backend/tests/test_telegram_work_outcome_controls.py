"""Native controls follow actual saved-answer receipts and complete previews."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_inbox import incoming
from test_telegram_work_input_admission import controller, stage_input
from test_telegram_work_input_capture import capture
from test_telegram_work_interaction_delivery import guard_for, open_view, selected
from test_telegram_work_outcome_delivery import clear_outcome_cleanup
from test_telegram_work_preview_recovery import dispatch, install
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typing_prompts import clear_old
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.inbox import InboxRepository
from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def saved_answer(e, purpose, *, before_outcome=None):
    install(e)
    await dispatch(e, 801)
    if purpose == "cannot_provide":
        e.identity = await open_view(e, await selected(e, "hi_cant_open"))
        await clear_old(e)
        await dispatch(e, 802)
    e.identity = await open_view(e, await selected(e, "hi_cannot" if purpose == "cannot_provide" else "hi_answer"))
    await clear_old(e)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        prompt = (await reader.get(TelegramOutboxRow, e.identity)).payload["work_request_basis"]["prompt_id"]
    inbox = InboxRepository(e.db.sessions)
    mark, _ = await inbox.accept(e.link, incoming(message=900, update_id=1900, account=e.link.chat_id, reply=803), sanitized_input={"text": "Use the published reference."})
    await capture(e, inbox, mark, prompt)
    host = controller(e, inbox)
    act = await stage_input(e, host, mark)
    done = await host.controls.acts.check(act.id)
    assert done.state == "done"
    if before_outcome is not None:
        await before_outcome(e, done)
    plans = WorkOutcomePlans(e.plans, host.controls.acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, act.id, guard=await guard_for(e))
    e.outcome_identity = identity
    await clear_outcome_cleanup(e)
    return plans, done, identity


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose,kind", [("answer", "work_start"), ("cannot_provide", "work_change")])
async def test_saved_answer_outcome_has_acknowledged_native_control(work_delivery, purpose, kind):
    e = work_delivery
    plans, act, identity = await saved_answer(e, purpose)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1].get("callback_group_id"))))
        assert [control.kind for control in controls] == [kind]
        assert all(control.message_id is None and control.payload["presentation_pending"] for control in controls)
    await dispatch(e, 804)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", e.dispatch_errors
        control = await reader.get(TelegramCallbackRow, controls[0].id)
        assert control.message_id == 804 and not control.payload.get("presentation_pending")
        assert control.payload["work_id"] == act.work_id
        if kind == "work_start":
            text = "\n".join(part["visible_text"] for part in row.parts)
            assert "Review the guide" in text and "Explain verified gaps" in text
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
    assert await plans.plan(e.link, act.id, guard=await guard_for(e)) == identity


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["none", "event", "answer", "act", "control", "all-source"])
async def test_resume_from_saved_answer_retains_its_original_primary_receipt(work_delivery, monkeypatch, damage):
    from test_telegram_work_start import prepare_startable

    from app.telegram_bots import work_start_actions
    from app.telegram_bots.work_start_controls import WorkStartAdmissions
    from app.telegram_bots.work_view_delivery import WorkViewPlans
    from app.telegram_bots.work_view_sources import WorkViewReader

    e = work_delivery
    outcomes, primary, identity = await saved_answer(e, "answer")
    await dispatch(e, 804)
    e.identity = identity
    button = await selected(e, "work_start")
    target = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=primary.work_id)
    await prepare_startable(e, monkeypatch)
    e.work = target
    views = WorkViewPlans(reader=WorkViewReader(work=e.human.work, authority=e.live.authority, links=e.db.links), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
    starts = WorkStartAdmissions(views, outcomes.acts)
    args = dict(chat_id=e.link.chat_id, message_id=804, message_thread_id=None)
    staged = await starts.stage(e.link, button.id, **args, guard=await guard_for(e))
    assert staged.id == button.id and staged.kind == "activate" and staged.state == "pending"
    current = await outcomes.acts._work_record(staged.id)
    assert current.continuation["news_origin"]["anchor_outbox_id"] == identity
    assert (await starts.stage(e.link, button.id, **args, guard=await guard_for(e))).id == staged.id
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, primary.id)).state == "done"
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 2
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_news_carriers import news_witness_id
    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputResponseRow

    if damage != "none":
        async with e.db.sessions.begin() as writer:
            if damage in {"event", "answer"}:
                event = await writer.scalar(select(HumanInputEventRow).where(HumanInputEventRow.operation_id == primary.id))
                if damage == "event":
                    event.actor_id = "another-person"
                else:
                    answer = await writer.get(HumanInputResponseRow, event.receipt["response_id"])
                    answer.text = "A different saved answer"
            elif damage == "act":
                await writer.delete(await writer.get(TelegramActRow, primary.id))
            elif damage == "control":
                await writer.delete(await writer.get(TelegramCallbackRow, button.id))
            else:
                row = await writer.get(TelegramOutboxRow, identity)
                row.payload = {key: value for key, value in row.payload.items() if key != "work_outcome_basis"}
                (await writer.get(TelegramActRow, staged.id)).continuation = None
                await writer.delete(await writer.get(TelegramCallbackRow, news_witness_id(staged.id)))
    original, errors = work_start_actions.activation_admission, []

    async def admission(*args, **kwargs):
        try:
            current = await original(*args, **kwargs)
        except Exception as error:
            errors.append(error)
            raise

        async def checked(writer):
            try:
                return await current(writer)
            except Exception as error:
                errors.append(error)
                raise

        return checked

    monkeypatch.setattr(work_start_actions, "activation_admission", admission)
    result = await outcomes.acts.check(staged.id)
    if damage == "none":
        assert result.state == "done" and len(e.launches) == 1, (result.state, result.reason, errors)
        assert (await outcomes.acts.check(staged.id)).id == staged.id and len(e.launches) == 1
    else:
        assert result.state != "done" and not e.launches
        assert any(isinstance(error, ProductSourceRetired) for error in errors), errors


@pytest.mark.asyncio
@pytest.mark.parametrize("pending", [False, True, "missing", "malformed", "foreign", "control", "control-marker", "control-child"])
async def test_live_outcome_control_retains_its_original_settled_act(work_delivery, pending):
    from datetime import timedelta

    from app.telegram_bots.retention import ACT_RETENTION_DAYS, _sweep_acts

    e = work_delivery
    plans, primary, identity = await saved_answer(e, "answer")
    await dispatch(e, 804)
    if pending:
        from app.telegram_bots.work_start_controls import WorkStartAdmissions
        from app.telegram_bots.work_view_delivery import WorkViewPlans
        from app.telegram_bots.work_view_sources import WorkViewReader

        e.identity = identity
        button = await selected(e, "work_start")
        views = WorkViewPlans(reader=WorkViewReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.sender.outbox.clock), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
        child = await WorkStartAdmissions(views, plans.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=804, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramActRow, primary.id)).created_at = e.clock() - timedelta(days=ACT_RETENTION_DAYS + 1)
    assert await _sweep_acts(e.db.sessions, e.clock()) == 0
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
        assert await reader.get(TelegramActRow, primary.id) is not None
    e.clock.advance(e.channel._services.callbacks.config.callbacks.max_age_days * 86400 + 1)
    if isinstance(pending, str):
        async with e.db.sessions.begin() as writer:
            if pending in {"control", "control-marker", "control-child"}:
                await writer.delete(await writer.get(TelegramCallbackRow, button.id))
                if pending == "control-marker":
                    (await writer.get(TelegramActRow, child.id)).continuation = None
                elif pending == "control-child":
                    await writer.delete(await writer.get(TelegramActRow, child.id))
            else:
                journal = await writer.get(TelegramActRow, child.id)
                if pending == "missing":
                    await writer.delete(journal)
                elif pending == "malformed":
                    journal.body = {}
                else:
                    journal.binding_generation += 1
        from app.telegram_bots.retention import _sweep_outbox

        await e.channel._services.callbacks.sweep(e.clock())
        await _sweep_outbox(e.db.sessions, e.clock(), e.channel._services.callbacks.config.callbacks.max_age_days)
    assert await _sweep_acts(e.db.sessions, e.clock()) == (0 if pending else 1)
    if pending:
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramOutboxRow, identity) is not None
            assert await reader.get(TelegramActRow, primary.id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["plain", "protected", "refresh"])
async def test_no_result_offers_current_manager_change_without_mutating_work(work_delivery, mode):
    from test_telegram_attempt_notices import ended
    from test_telegram_work_news_delivery import prepare

    from app.telegram_bots.work_management_delivery import WorkManagementPlans
    from app.telegram_bots.work_management_sources import WorkManagementReader
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    e = work_delivery
    install(e)
    item = await ended(e, "attempt.no_result")
    if mode == "protected":
        from test_telegram_digest_item_families import protect

        await protect(e)
    _plans, identity = await prepare(e, item=item)
    if mode == "refresh":
        from test_agent_work_records import command

        current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id)
        await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id, request=command(current, "edit", objective="Current corrected assignment"))
        e.clock.advance(10)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    await dispatch(e, 810)
    e.identity = identity
    if mode == "protected":
        from test_telegram_bot_adapter import message

        show = await selected(e, "reveal")
        await e.channel._services.work_news_sources.recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=810, message_thread_id=None, guard=await guard_for(e))
        e.clock.advance(10)
        message(e.channel.bot.request, "editMessageText", identity=810)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        basis = (await reader.get(TelegramOutboxRow, identity)).payload["work_news_basis"]
        assert basis["manager_navigation"] == 1
        if mode in {"protected", "refresh"}:
            assert basis["history"] and all(record["basis"]["manager_navigation"] == 1 for record in basis["history"])
    button = await selected(e, "work_change")
    assert button is not None
    from test_telegram_work_control_admission import admissions

    acts = admissions(e).acts
    management = WorkManagementPlans(reader=WorkManagementReader(acts), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
    e.channel._services.work_management_sources = WorkNewsSources(management)
    before = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id)
    target = await management.open(e.link, button.id, query_id="no-result-change", chat_id=e.link.chat_id, message_id=810, message_thread_id=None, guard=await guard_for(e))
    await dispatch(e, 811)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, target)
        assert row.state == "sent", e.dispatch_errors
        assert before["success_criteria"] in "\n".join(part["visible_text"] for part in row.parts)
    assert await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["primary", "detail", "ceiling", "control"])
async def test_outcome_final_sdk_fence_qualifies_primary_detail_and_control(work_delivery, monkeypatch, damage):
    from app.gateway import authz
    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    _plans, primary, identity = await saved_answer(e, "answer")
    owner = e.channel._services.work_outcome_sources
    authorize, resolve = owner.authorize, authz.resolve_route_permissions
    armed, changed = [], []

    async def final(*args, **kwargs):
        armed.append(True)
        return await authorize(*args, **kwargs)

    async def provider(*args, **kwargs):
        held = await resolve(*args, **kwargs)
        if armed and not changed:
            if damage != "ceiling":
                async with e.db.sessions.begin() as writer:
                    if damage == "primary":
                        event = await writer.scalar(select(HumanInputEventRow).where(HumanInputEventRow.operation_id == primary.id))
                        event.actor_id = "another-person"
                    elif damage == "detail":
                        (await writer.get(AgentWorkRow, primary.work_id)).success_criteria = "Changed during final provider await"
                    else:
                        row = await writer.get(TelegramOutboxRow, identity)
                        control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
                        await writer.delete(control)
            changed.append(True)
        return frozenset(held) - {"agents:write"} if changed and damage == "ceiling" else held

    monkeypatch.setattr(owner, "authorize", final)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert armed and changed
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == before
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
        assert (await reader.get(TelegramActRow, primary.id)).attempts == primary.attempts


@pytest.mark.asyncio
async def test_multipart_outcome_waits_for_its_last_actual_ack(work_delivery):
    from test_agent_work_records import command

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_start_controls import WorkStartAdmissions
    from app.telegram_bots.work_view_delivery import WorkViewPlans
    from app.telegram_bots.work_view_sources import WorkViewReader

    async def change(e, primary):
        current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=primary.work_id)
        await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=primary.work_id, request=command(current, "edit", objective="o" * 3000, success_criteria="c" * 3000))

    e = work_delivery
    outcomes, _primary, identity = await saved_answer(e, "answer", before_outcome=change)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        parts = row.parts
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "work_start"))
        assert len(parts) > 1 and control.message_id is None
        assert "o" * 3000 in "".join(part["visible_text"] for part in parts)
        assert "c" * 3000 in "".join(part["visible_text"] for part in parts)
    views = WorkViewPlans(reader=WorkViewReader(work=e.human.work, authority=e.live.authority, links=e.db.links), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
    starts = WorkStartAdmissions(views, outcomes.acts)
    for index in range(len(parts)):
        if index < len(parts) - 1:
            with pytest.raises((CallbackRefused, ProductSourceRetired)):
                await starts.stage(e.link, control.id, chat_id=e.link.chat_id, message_id=820 + index, message_thread_id=None, guard=await guard_for(e))
        await dispatch(e, 820 + index)
        async with e.db.sessions() as reader:
            held = await reader.get(TelegramCallbackRow, control.id)
            assert held.message_id == (820 + index if index == len(parts) - 1 else None)
            assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
    staged = await starts.stage(e.link, control.id, chat_id=e.link.chat_id, message_id=820 + len(parts) - 1, message_thread_id=None, guard=await guard_for(e))
    assert staged.body["expected_assignment_revision"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decide", "changes_requested"])
async def test_decision_and_review_changes_keep_their_canonical_event_family(work_delivery, kind):
    from test_telegram_work_control_admission import admissions
    from test_telegram_work_control_admission import test_decision_confirmation_records_only_the_displayed_choice as decision
    from test_telegram_work_input_capture import accept, prompt

    from app.telegram_bots.work_start_controls import WorkStartAdmissions
    from app.telegram_bots.work_view_delivery import WorkViewPlans
    from app.telegram_bots.work_view_sources import WorkViewReader
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = work_delivery
    if kind == "decide":
        await decision(e)
        acts = admissions(e).acts
    else:
        request, prompt_id, _seen = await prompt(e)
        current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=request["work_id"])
        # This shared review fixture seeds a report. Preserve its actual first
        # activation revision for the outcome reader's logical ordering.
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentWorkAttemptRow, current["attempt"]["id"])).request = {"expected_revision": 1}
        inbox, mark = await accept(e)
        await capture(e, inbox, mark, prompt_id)
        host = controller(e, inbox)
        primary = await stage_input(e, host, mark)
        assert (await host.controls.acts.check(primary.id)).state == "done"
        acts = host.controls.acts
    async with e.db.sessions() as reader:
        primary = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == kind))
        assert primary.state == "done"
        primary_id = primary.id
    outcomes = WorkOutcomePlans(e.plans, acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(outcomes)
    identity = await outcomes.plan(e.link, primary_id, guard=await guard_for(e))
    e.outcome_identity = identity
    await clear_outcome_cleanup(e)
    await dispatch(e, 830)
    e.identity = identity
    button = await selected(e, "work_start")
    assert button is not None
    views = WorkViewPlans(reader=WorkViewReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.sender.outbox.clock), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
    staged = await WorkStartAdmissions(views, acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=830, message_thread_id=None, guard=await guard_for(e))
    assert staged.kind == "activate" and staged.state == "pending"
    async with e.db.sessions() as reader:
        basis = (await reader.get(TelegramOutboxRow, identity)).payload["work_outcome_basis"]["context"]
        assert basis["origin_proof"]["act"]["kind"] == kind
        assert basis["origin_proof"]["event"]["work_id"] == staged.work_id
        assert basis["origin_proof"]["answer"] is None


@pytest.mark.asyncio
async def test_retry_suffixed_outcome_keeps_receipts_retention_and_real_control(work_delivery):
    from datetime import timedelta

    from app.telegram_bots.retention import ACT_RETENTION_DAYS, _sweep_acts
    from app.telegram_bots.work_start_controls import WorkStartAdmissions
    from app.telegram_bots.work_view_delivery import WorkViewPlans
    from app.telegram_bots.work_view_sources import WorkViewReader

    e = work_delivery
    plans, primary, first = await saved_answer(e, "answer")
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, first)
        row.state, row.skip_reason = "skipped", "source-gone"
    replacement = await plans.plan(e.link, primary.id, guard=await guard_for(e))
    assert replacement != first
    e.outcome_identity = replacement
    await clear_outcome_cleanup(e)
    await dispatch(e, 840)
    e.identity = replacement
    button = await selected(e, "work_start")
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, replacement)
        assert row.state == "sent" and "~" in row.dedupe_key
        (await writer.get(TelegramActRow, primary.id)).created_at = e.clock() - timedelta(days=ACT_RETENTION_DAYS + 1)
    assert await _sweep_acts(e.db.sessions, e.clock()) == 0
    views = WorkViewPlans(reader=WorkViewReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.sender.outbox.clock), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
    starts = WorkStartAdmissions(views, plans.acts)
    args = dict(chat_id=e.link.chat_id, message_id=840, message_thread_id=None)
    act = await starts.stage(e.link, button.id, **args, guard=await guard_for(e))
    assert (await starts.stage(e.link, button.id, **args, guard=await guard_for(e))).id == act.id


@pytest.mark.asyncio
async def test_legacy_no_result_keyboard_decodes_and_delivers_unchanged(work_delivery):
    from copy import deepcopy

    from test_telegram_attempt_notices import ended
    from test_telegram_work_news_delivery import prepare

    e = work_delivery
    install(e)
    item = await ended(e, "attempt.no_result")
    _plans, identity = await prepare(e, item=item)
    # Reproduce the prior producer's saved keyboard on this actual canonical
    # settlement. Its complete preview already offered Resume, before Change.
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        basis = deepcopy(row.payload["work_news_basis"])
        basis.pop("manager_navigation", None)
        keyboard = [[button for button in line if button.get("kind") != "work_change"] for line in basis["keyboard"]]
        keyboard = [line for line in keyboard if line]
        assert any(button.get("kind") == "work_start" for line in keyboard for button in line)
        basis["keyboard"] = keyboard
        old_group = row.parts[-1]["callback_group_id"]
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == old_group)):
            await writer.delete(control)
        row.payload = {**row.payload, "work_news_basis": basis, "product_keyboard_basis": keyboard}
        await writer.flush()
        group = await e.channel._services.callbacks.write_group(e.link, keyboard, anchor_outbox_id=identity, presentation_version=0, chat_id=e.link.chat_id, message_id=None, message_thread_id=None, session=writer)
        row.parts = [{**part, **({"callback_group_id": group.group_id} if index == len(row.parts) - 1 else {})} for index, part in enumerate(row.parts)]
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
            control.created_at = row.created_at
            control.payload = {**control.payload, "presentation_pending": True}
    await dispatch(e, 850)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", e.dispatch_errors
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert [control.kind for control in controls] == ["work_start"]
        assert all(control.message_id == 850 for control in controls)


@pytest.mark.asyncio
async def test_cannot_help_change_opens_a_fresh_full_management_preview(work_delivery):
    from app.telegram_bots.work_management_delivery import WorkManagementPlans
    from app.telegram_bots.work_management_sources import WorkManagementReader
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    e = work_delivery
    outcomes, primary, identity = await saved_answer(e, "cannot_provide")
    await dispatch(e, 804)
    e.identity = identity
    button = await selected(e, "work_change")
    plans = WorkManagementPlans(reader=WorkManagementReader(outcomes.acts), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
    e.channel._services.work_management_sources = WorkNewsSources(plans)
    target = await plans.open(e.link, button.id, query_id="change-after-cannot-help", chat_id=e.link.chat_id, message_id=804, message_thread_id=None, guard=await guard_for(e))
    await dispatch(e, 805)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, target)
        assert row.state == "sent", e.dispatch_errors
        text = "\n".join(part["visible_text"] for part in row.parts)
        assert "Review the guide" in text and "Explain verified gaps" in text
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 2
        assert (await reader.get(TelegramActRow, primary.id)).state == "done"
