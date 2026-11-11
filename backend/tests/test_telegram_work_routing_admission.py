"""An acknowledged recipient choice owns one immutable routing operation."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_bot_adapter import message
from test_telegram_work_control_admission import admissions
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_routing_delivery import routing
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow


async def setup(e, monkeypatch, *, acknowledged=True):
    from app.telegram_bots.work_routing_admission import WorkRoutingAdmissions

    plans, _ = await routing(e, monkeypatch)
    controller = WorkRoutingAdmissions(plans, admissions(e).acts)
    if acknowledged:
        message(e.channel.bot.request, identity=951)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert not e.dispatch_errors
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"]))
    return controller, control


async def stage(e, controller, control):
    return await controller.stage(e.link, control.id, chat_id=e.link.chat_id, message_id=951, message_thread_id=None, guard=await guard_for(e))


@pytest.mark.asyncio
async def test_route_choice_reserves_current_cas_and_replays_exact_body_during_pause(work_delivery, monkeypatch):
    from deerflow.agent_instances.human_input_contract import Respond

    e = work_delivery
    controller, control = await setup(e, monkeypatch)
    # An intervening comment changes the general CAS, not the stored question
    # revision or selected person. Capture current CAS once at first admission.
    await e.human.respond(actor=e.actor, request_id=e.request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="Additional context"))
    source = await e.human.get(actor=e.actor, request_id=e.request["id"])
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramBotLinkRow, e.link.id)).acts_paused_since = e.sender.outbox.clock()
    e.link = await e.db.links.get(e.link.id)
    staged = await stage(e, controller, control)
    assert staged.kind == "route" and staged.state == "pending" and staged.attempts == 0
    assert staged.body["expected_revision"] == source["revision"]
    assert staged.body["expected_request_revision"] == control.payload["request_revision"] == 1
    assert staged.body["recipient_id"] == e.recipient_id
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(HumanInputEventRow).where(HumanInputEventRow.action == "route")) == 0
    completed = await controller.acts.check(staged.id)
    assert completed.state == "done"
    replay = await stage(e, controller, control)
    assert replay.id == completed.id and replay.body == staged.body and replay.state == "done"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(HumanInputEventRow).where(HumanInputEventRow.action == "route")) == 1
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow)) == 1
        assert (await reader.get(TelegramCallbackRow, control.id)).payload["work_action_id"] == staged.id


@pytest.mark.asyncio
async def test_unacknowledged_recipient_choice_never_creates_an_action(work_delivery, monkeypatch):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    controller, control = await setup(e, monkeypatch, acknowledged=False)
    with pytest.raises(CallbackRefused):
        await stage(e, controller, control)
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["candidate", "manager", "revision", "binding"])
async def test_recipient_choice_revalidates_before_admission(work_delivery, monkeypatch, change):
    from app.telegram_bots.callbacks import CallbackRefused
    from deerflow.agent_instances.contract import AgentConflict, AgentDenied
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    controller, control = await setup(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        if change in {"candidate", "manager"}:
            identity = e.recipient_id if change == "candidate" else e.actor.subject_id
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, identity))).permissions = 1
        elif change == "revision":
            (await writer.get(HumanInputRequestRow, e.request["id"])).request_revision += 1
        else:
            (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation += 1
    with pytest.raises((AgentDenied, AgentConflict, CallbackRefused)):
        await stage(e, controller, control)
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["candidate", "disabled", "manager", "source"])
async def test_final_borrowed_writer_fence_rolls_back_reservation(work_delivery, monkeypatch, change):
    from app.telegram_bots.authority import AccessOff
    from app.telegram_bots.callbacks import CallbackRefused
    from deerflow.agent_instances.contract import AgentConflict, AgentDenied
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = work_delivery
    controller, control = await setup(e, monkeypatch)
    original = controller.acts._stage_work_act
    reached = []

    async def withdraw(*args, **kwargs):
        result = await original(*args, **kwargs)
        writer = kwargs["session"]
        assert writer.in_transaction()
        reached.append(True)
        if change in {"candidate", "manager"}:
            identity = e.recipient_id if change == "candidate" else e.actor.subject_id
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, identity))).permissions = 1
        elif change == "disabled":
            writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.recipient_id, disabled_at=e.sender.outbox.clock()))
        else:
            (await writer.get(HumanInputRequestRow, e.request["id"])).revision += 1
        await writer.flush()
        return result

    monkeypatch.setattr(controller.acts, "_stage_work_act", withdraw)
    with pytest.raises((AgentDenied, AgentConflict, CallbackRefused, AccessOff)) as error:
        await stage(e, controller, control)
    if change == "disabled":
        assert isinstance(error.value, AgentDenied) and error.value.reason == "recipient-ineligible"
    assert reached
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow)) == 0
        assert "work_action_id" not in (await reader.get(TelegramCallbackRow, control.id)).payload


@pytest.mark.asyncio
@pytest.mark.parametrize("same", [True, False])
async def test_concurrent_choices_share_one_group_reservation(work_delivery, monkeypatch, same):
    import asyncio

    from app.gateway.auth.models import User
    from app.telegram_bots.callbacks import CallbackRefused
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    user = await e.live.users.create_user(User(email="second-recipient@example.com"))
    async with e.db.sessions.begin() as writer:
        writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=str(user.id), permissions=3))
    controller, first = await setup(e, monkeypatch)
    async with e.db.sessions() as reader:
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == first.group_id)))
    assert len(controls) == 2
    results = await asyncio.gather(stage(e, controller, controls[0]), stage(e, controller, controls[0] if same else controls[1]), return_exceptions=True)
    if same:
        assert results[0].id == results[1].id
        assert results[0].body == results[1].body
    else:
        assert sum(isinstance(result, CallbackRefused) for result in results) == 1, results
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow)) == 1
        assert len({(await reader.get(TelegramCallbackRow, control.id)).payload["work_action_id"] for control in controls}) == 1


@pytest.mark.asyncio
async def test_collected_replay_journal_cannot_be_recreated(work_delivery, monkeypatch):
    from contextlib import asynccontextmanager

    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    controller, control = await setup(e, monkeypatch)
    original = await stage(e, controller, control)
    writer = controller.callbacks.writer

    @asynccontextmanager
    async def collect_then_write(*args, **kwargs):
        async with e.db.sessions.begin() as collector:
            await collector.delete(await collector.get(TelegramActRow, original.id))
        async with writer(*args, **kwargs) as session:
            yield session

    monkeypatch.setattr(controller.callbacks, "writer", collect_then_write)
    with pytest.raises(CallbackRefused):
        await stage(e, controller, control)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, original.id) is None
        assert await reader.scalar(select(func.count()).select_from(HumanInputEventRow).where(HumanInputEventRow.action == "route")) == 0


@pytest.mark.asyncio
async def test_missing_reserved_journal_never_becomes_a_new_operation(work_delivery, monkeypatch):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    controller, control = await setup(e, monkeypatch)
    original = await stage(e, controller, control)
    async with e.db.sessions.begin() as writer:
        await writer.delete(await writer.get(TelegramActRow, original.id))
    with pytest.raises(CallbackRefused):
        await stage(e, controller, control)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, original.id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("after_source", [False, True])
@pytest.mark.parametrize("change", ["comment", "closed"])
async def test_journal_appearing_after_discovery_keeps_original_cas(work_delivery, monkeypatch, after_source, change):
    import asyncio

    from deerflow.agent_instances.human_input_contract import Respond

    e = work_delivery
    controller, control = await setup(e, monkeypatch)
    target, method = (controller.plans.reader, "read_routing") if after_source else (controller.acts, "_work_link")
    from contextvars import ContextVar

    original = getattr(target, method)
    caller = ContextVar("routing_test_caller")
    second_ready, release = asyncio.Event(), asyncio.Event()

    async def interleave(*args, **kwargs):
        try:
            result = await original(*args, **kwargs)
        finally:
            if caller.get() == "second":
                second_ready.set()
        if caller.get() == "first":
            await second_ready.wait()
        else:
            await release.wait()
        return result

    async def press(label):
        caller.set(label)
        return await stage(e, controller, control)

    monkeypatch.setattr(target, method, interleave)
    first = asyncio.create_task(press("first"))
    second = asyncio.create_task(press("second"))
    try:
        staged = await first
        if change == "closed":
            from deerflow.agent_instances.work_contract import WorkCommand

            work = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
            await e.human.work.command(
                actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"], request=WorkCommand(operation_id=uuid4().hex, action="cancel", expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"])
            )
        else:
            await e.human.respond(actor=e.actor, request_id=e.request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="New general revision"))
        current = await e.human.get(actor=e.actor, request_id=e.request["id"])
        assert current["revision"] > staged.body["expected_revision"]
        release.set()
        replay = await second
        assert replay.id == staged.id and replay.body == staged.body
    finally:
        second_ready.set()
        release.set()
        await asyncio.gather(first, second, return_exceptions=True)


@pytest.mark.asyncio
async def test_general_topic_alias_is_valid_for_first_press_and_replay(work_delivery, monkeypatch):
    e = work_delivery
    controller, control = await setup(e, monkeypatch)
    first = await controller.stage(e.link, control.id, chat_id=e.link.chat_id, message_id=951, message_thread_id=1, guard=await guard_for(e))
    replay = await stage(e, controller, control)
    again = await controller.stage(e.link, control.id, chat_id=e.link.chat_id, message_id=951, message_thread_id=1, guard=await guard_for(e))
    assert first.id == replay.id == again.id and first.body == replay.body == again.body
