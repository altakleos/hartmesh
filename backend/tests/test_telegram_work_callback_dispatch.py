"""A shown Retry gesture keeps the original immutable Work operation."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_outcome_delivery import outcome
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_shown_work_retry_completes_original_operation_without_a_new_act(work_delivery):
    e = work_delivery
    plans, act_id, identity = await outcome(e, uncertain=True)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, identity)
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"]))
        original = await reader.get(TelegramActRow, act_id)
        body = dict(original.body)
    from app.telegram_bots.work_action_retries import WorkActionRetries

    result = await WorkActionRetries(plans).retry(e.link, callback.id, chat_id=e.link.chat_id, message_id=803, message_thread_id=None, guard=await guard_for(e))
    from app.telegram_bots.work_actions import same_body

    assert result.id == act_id and result.state == "done" and same_body(result.body, body)
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1


async def retry_setup(e, *, acknowledged=True):
    from app.telegram_bots.work_action_retries import WorkActionRetries

    plans, act_id, identity = await outcome(e, uncertain=True)
    if acknowledged:
        await dispatch(e, 803)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, identity)
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"]))
    return plans, WorkActionRetries(plans), act_id, identity, control


async def retry(e, retries, callback, *, guard=None):
    return await retries.retry(e.link, callback.id, chat_id=e.link.chat_id, message_id=803, message_thread_id=None, guard=guard or await guard_for(e))


@pytest.mark.asyncio
async def test_repeated_shown_retry_does_not_repeat_an_unknown_attempt(work_delivery, monkeypatch):
    import asyncio

    e = work_delivery
    plans, retries, act_id, _parent, callback = await retry_setup(e)
    calls = []

    async def unknown(*args, **kwargs):
        calls.append(kwargs["request"].operation_id)
        raise TimeoutError()

    monkeypatch.setattr(plans.acts.work, "command", unknown)
    results = await asyncio.gather(retry(e, retries, callback), retry(e, retries, callback))
    assert calls == [act_id]
    assert all(result.id == act_id and result.state == "uncertain" and result.attempts == 4 for result in results)
    again = await retry(e, retries, callback)
    assert again.attempts == 4 and calls == [act_id]


@pytest.mark.asyncio
@pytest.mark.parametrize("previous_retry", [False, True])
async def test_retry_recovers_a_late_canonical_receipt_without_another_command(work_delivery, monkeypatch, previous_retry):
    from app.telegram_bots.work_actions import request_for
    from deerflow.agent_instances.contract import PrincipalRef

    e = work_delivery
    plans, retries, act_id, _parent, callback = await retry_setup(e)
    original = await plans.acts._work_record(act_id)
    if previous_retry:
        command = plans.acts.work.command

        async def unknown(*args, **kwargs):
            raise TimeoutError()

        monkeypatch.setattr(plans.acts.work, "command", unknown)
        earlier = await retry(e, retries, callback)
        assert earlier.state == "uncertain" and earlier.attempts == 4
        monkeypatch.setattr(plans.acts.work, "command", command)
    await plans.acts.work.command(actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.link.agent_ref, work_id=original.work_id, request=request_for(original.kind, original.body, identity=original.id), can_stop=False)

    async def unexpected(*args, **kwargs):
        pytest.fail("A canonical receipt must settle without another command")

    monkeypatch.setattr(plans.acts.work, "command", unexpected)
    result = await retry(e, retries, callback)
    assert result.state == "done" and result.id == act_id and result.attempts == (4 if previous_retry else 3)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["pending", "message", "body", "extra", "topic"])
async def test_retry_requires_the_complete_original_shown_result(work_delivery, damage):
    from copy import deepcopy

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    _plans, retries, act_id, parent, callback = await retry_setup(e, acknowledged=damage != "pending")
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, parent)
        if damage == "message":
            parts = deepcopy(row.parts)
            parts[-1]["message_ids"] = [True]
            row.parts = parts
        elif damage == "body":
            act = await writer.get(TelegramActRow, act_id)
            act.body = {**act.body, "expected_revision": 999}
        elif damage == "extra":
            payload = deepcopy(row.payload)
            payload["work_outcome_basis"]["unknown"] = True
            row.payload = payload
        elif damage == "topic":
            row.message_thread_id = 88
    with pytest.raises((CallbackRefused, ProductSourceRetired)):
        await retry(e, retries, callback)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, act_id)).attempts == 3


@pytest.mark.asyncio
async def test_retry_rechecks_shown_proof_after_outer_guard_await(work_delivery):
    from copy import deepcopy

    from app.telegram_bots.links import prepare_publication_guard
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    _plans, retries, act_id, parent, callback = await retry_setup(e)
    base = await guard_for(e)
    reached = []

    class Withdraw:
        async def prepare(self):
            await prepare_publication_guard(base)

        def current(self):
            base.current()

        async def check(self, writer):
            result = await base.check(writer)
            reached.append(True)
            if len(reached) == 3:
                row = await writer.get(TelegramOutboxRow, parent)
                parts = deepcopy(row.parts)
                parts[0]["sent_at"] = "2026-01-01T00:00:00"
                row.parts = parts
                await writer.flush()
            return result

    with pytest.raises(ProductSourceRetired):
        await retry(e, retries, callback, guard=Withdraw())
    assert len(reached) == 3
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, act_id)).attempts == 3


@pytest.mark.asyncio
async def test_actual_callback_dispatch_retries_and_plans_the_canonical_result(work_delivery):
    from test_telegram_callback_commands import press

    from app.telegram_bots.commands import CallbackCommands

    e = work_delivery
    plans, _retries, act_id, _parent, callback = await retry_setup(e)
    from app.telegram_bots.work_callbacks import WorkCallbacks

    services = e.channel._services
    services.commands = CallbackCommands(services)
    services.work_callbacks = WorkCallbacks(services, plans)
    # The low-level Work fixture supplies a real account authority, but its
    # transport-only service container has no fixed-command collaborators.
    services.prepare_live_owner = e.live.authority.prepare_live_owner

    async def ingress_values(link):
        return {"product": "HartMesh", "name": "Fixture employee", "agent": "Fixture employee"}

    services.prepare_ingress_values = ingress_values
    e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
    assert await e.channel.handle_update(press(e, 2900, callback.id, message_id=803))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, act_id)).state == "done"
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == act_id)))
        assert any(row.payload["work_outcome_basis"]["context"]["act"]["state"] == "done" for row in rows)


def install_dispatch(e, plans):
    from app.telegram_bots.commands import CallbackCommands
    from app.telegram_bots.work_callbacks import WorkCallbacks

    services = e.channel._services
    services.commands = CallbackCommands(services)
    services.work_callbacks = WorkCallbacks(services, plans)
    services.prepare_live_owner = e.live.authority.prepare_live_owner

    async def ingress_values(link):
        return {"product": "HartMesh", "name": "Fixture employee", "agent": "Fixture employee"}

    services.prepare_ingress_values = ingress_values
    return services.work_callbacks


async def press_work(e, callback, *, message_id, update_id=2901):
    from test_telegram_callback_commands import press

    e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
    assert await e.channel.handle_update(press(e, update_id, callback.id, message_id=message_id))
    await e.channel._drain_ingress()


@pytest.mark.asyncio
async def test_actual_confirmation_callback_saves_once_then_plans_its_result(work_delivery):
    from test_telegram_work_control_admission import admissions
    from test_telegram_work_outcome_reads import review_confirmation

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans

    e = work_delivery
    _work, _request, _seen, callback = await review_confirmation(e)
    plans = WorkOutcomePlans(e.plans, admissions(e).acts)
    install_dispatch(e, plans)
    await press_work(e, callback, message_id=802)
    async with e.db.sessions() as reader:
        result = await reader.get(TelegramActRow, callback.id)
        assert result.state == "done" and result.kind == "accept"
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is not None
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == result.id))
        assert row.state == "pending" and row.payload["work_outcome_basis"]["context"]["act"]["state"] == "done"


@pytest.mark.asyncio
async def test_actual_first_taps_create_a_shown_prompt_without_an_action(work_delivery):
    from test_telegram_work_control_admission import admissions
    from test_telegram_work_interaction_delivery import selected
    from test_telegram_work_preview_recovery import install
    from test_telegram_work_typing_prompts import clear_old

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans

    e = work_delivery
    install(e)
    plans = WorkOutcomePlans(e.plans, admissions(e).acts)
    install_dispatch(e, plans)
    await dispatch(e, 801)
    for index, kind in enumerate(("hi_cant_open", "hi_wrong")):
        callback = await selected(e, kind)
        await press_work(e, callback, message_id=801 + index, update_id=2902 + index)
        async with e.db.sessions() as reader:
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:work-interaction:" + callback.id))
            assert row is not None
            e.identity = row.id
        await clear_old(e)
        await dispatch(e, 802 + index)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        prompt = await reader.get(TelegramCallbackRow, row.payload["work_request_basis"]["prompt_id"])
        assert prompt.payload["prompt_visibility"]["state"] == "visible" and prompt.message_id == 803
        assert list(await reader.scalars(select(TelegramActRow))) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("withdraw", [False, True])
async def test_actual_routing_callback_uses_receipt_or_refreshes_current_choices(work_delivery, monkeypatch, withdraw):
    from test_telegram_work_routing_followups import followup

    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    plans, _admission, original, _mark, parent, callback = await followup(e, monkeypatch)
    install_dispatch(e, plans)
    if withdraw:
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
    await press_work(e, callback, message_id=804)
    async with e.db.sessions() as reader:
        action = await reader.get(TelegramActRow, callback.id)
        assert (await reader.get(TelegramActRow, original.id)).state == "done"
        if withdraw:
            assert action is None
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["refresh"]["anchor_outbox_id"].as_string() == parent.id))
            assert row is not None and row.payload["work_outcome_basis"]["routing"]["routing"]["recipients"] == []
        else:
            assert action.state == "done" and action.kind == "route"
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == action.id))
            assert row.parts[0]["kind"] == "edit" and row.parts[0]["target_message_id"] == 804


@pytest.mark.asyncio
async def test_retired_retry_lifetime_cannot_start_a_new_attempt(work_delivery, monkeypatch):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.links import prepare_publication_guard

    e = work_delivery
    plans, retries, act_id, _parent, callback = await retry_setup(e)
    base = await guard_for(e)
    live = [True]
    reached = []
    original = plans.acts._work_link

    async def retire(*args, **kwargs):
        result = await original(*args, **kwargs)
        reached.append(True)
        live[0] = False
        return result

    class Lifetime:
        async def prepare(self):
            await prepare_publication_guard(base)

        async def check(self, writer):
            return await base.check(writer)

        def current(self):
            if not live[0]:
                raise CallbackRefused()
            base.current()

    monkeypatch.setattr(plans.acts, "_work_link", retire)
    result = await retry(e, retries, callback, guard=Lifetime())
    assert reached and result.id == act_id and result.state == "uncertain" and result.attempts == 3


@pytest.mark.asyncio
async def test_saved_action_with_unreadable_outcome_is_deferred_without_failed_reply(work_delivery, monkeypatch):
    from test_telegram_work_control_admission import admissions
    from test_telegram_work_outcome_reads import review_confirmation

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.agent_instances.contract import AgentDenied

    e = work_delivery
    _work, _request, _seen, callback = await review_confirmation(e)
    plans = WorkOutcomePlans(e.plans, admissions(e).acts)
    install_dispatch(e, plans)
    reached = []

    async def unavailable(*args, **kwargs):
        reached.append(True)
        raise AgentDenied("The reader changed after the saved action")

    monkeypatch.setattr(plans, "plan", unavailable)
    await press_work(e, callback, message_id=802)
    assert reached
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, callback.id)).state == "done"
        pending = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.state == "pending")))
        assert pending == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["accept", "route"])
@pytest.mark.parametrize("concurrent", [False, True])
async def test_duplicate_original_control_cannot_retry_an_unknown_command(work_delivery, monkeypatch, kind, concurrent):
    from test_telegram_work_control_admission import admissions
    from test_telegram_work_outcome_reads import review_confirmation
    from test_telegram_work_routing_followups import followup

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans

    e = work_delivery
    if kind == "accept":
        _work, _request, _seen, callback = await review_confirmation(e)
        plans = WorkOutcomePlans(e.plans, admissions(e).acts)
        message_id = 802
    else:
        plans, _admission, _original, _mark, _parent, callback = await followup(e, monkeypatch)
        message_id = 804
    dispatcher = install_dispatch(e, plans)
    calls = []

    async def unknown(*args, **kwargs):
        calls.append(kwargs["request"].operation_id)
        raise TimeoutError()

    from deerflow.agent_instances.human_input import HumanInput

    monkeypatch.setattr(plans.acts.work if kind == "accept" else HumanInput, "command", unknown)
    if concurrent:
        import asyncio

        from test_telegram_callback_commands import press

        admission = dispatcher.controls if kind == "accept" else dispatcher.routing
        original_stage, ready, staged = admission.stage, asyncio.Event(), []

        async def together(*args, **kwargs):
            result = await original_stage(*args, **kwargs)
            staged.append(result.attempts)
            if len(staged) == 2:
                ready.set()
            await asyncio.wait_for(ready.wait(), 10)
            return result

        monkeypatch.setattr(admission, "stage", together)
        for update_id in (3101, 3102):
            e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(press(e, update_id, callback.id, message_id=message_id))
        await e.channel._drain_ingress()
        assert staged == [0, 0]
    else:
        await press_work(e, callback, message_id=message_id, update_id=3101)
        await press_work(e, callback, message_id=message_id, update_id=3102)
    assert calls == [callback.id]
    async with e.db.sessions() as reader:
        action = await reader.get(TelegramActRow, callback.id)
        assert action.state == "pending" and action.reason is None and action.attempts == 1


@pytest.mark.asyncio
async def test_known_non_effect_refusal_can_check_the_original_confirmation(work_delivery, monkeypatch):
    from test_telegram_work_refusal_recovery import refused

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans

    e = work_delivery
    controls, _record, _request, _seen, callback, first = await refused(e, monkeypatch)
    plans = WorkOutcomePlans(e.plans, controls.acts)
    install_dispatch(e, plans)
    await press_work(e, callback, message_id=callback.message_id)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, first.id)
        assert saved.state == "done" and saved.attempts == first.attempts + 1
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
