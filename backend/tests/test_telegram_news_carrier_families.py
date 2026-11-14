"""Actual news families reach SQL admission and an explicit offline host launcher."""

import pytest
from _telegram_bot_test_support import ManualClock
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_attempt_notices import ended
from test_telegram_bot_adapter import message
from test_telegram_work_answer_news import answered
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_news_delivery import prepare
from test_telegram_work_news_sources import review_news
from test_telegram_work_proposal_news import proposed
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import Acts
from app.telegram_bots.work_start_controls import WorkStartAdmissions
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def pending_news(e, monkeypatch, kind):
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    if kind == "work.proposed":
        item = await proposed(e)
    elif kind == "request.answered":
        item = await answered(e)
    elif kind in {"decision.recorded", "result.changes"}:
        item, _extra, _source = await review_news(e, "decide" if kind == "decision.recorded" else "changes_requested", kind)
    else:
        item = await ended(e, kind)
    plans, identity = await prepare(e, item=item)
    target = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert target["success_criteria"] in "".join(part["visible_text"] for part in row.parts)
        count = len(row.parts)
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "work_start"))
        assert control is not None
    for index in range(count):
        message(e.channel.bot.request, identity=9601 + index)
        clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        control = await reader.get(TelegramCallbackRow, control.id)
        assert control.message_id == 9600 + count
    await prepare_startable(e, monkeypatch)
    e.work = target
    acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    starts = WorkStartAdmissions(plans, acts)
    scope = dict(chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None)
    act = await starts.stage(e.link, control.id, **scope, guard=await guard_for(e))
    assert act.state == "pending" and not e.launches
    return acts, act, plans, item, starts, control, scope


async def start_news(e, monkeypatch, kind):
    acts, act, plans, item, starts, control, scope = await pending_news(e, monkeypatch, kind)
    result = await acts.check(act.id)
    assert result.state == "done", (result.state, result.reason)
    assert len(e.launches) == 1
    replay = await starts.stage(e.link, control.id, **scope, guard=await guard_for(e))
    assert replay.body == act.body
    assert (await acts.check(act.id)).result == result.result and len(e.launches) == 1
    return acts, result, plans, item


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["work.proposed", "request.answered", "decision.recorded", "result.changes", "attempt.no_result", "attempt.failed", "attempt.stopped_restart", "interrupted"])
async def test_each_news_carrier_starts_only_its_actual_assignment(work_delivery, monkeypatch, kind):
    await start_news(work_delivery, monkeypatch, kind)


@pytest.mark.asyncio
async def test_native_news_source_preparation_owns_at_most_one_connection(work_delivery, monkeypatch):
    from sqlalchemy import event

    from app.telegram_bots.work_news_carriers import ActivationNewsOrigin

    e = work_delivery
    original = ActivationNewsOrigin.prepare
    observed = []

    async def bounded(self, *args, **kwargs):
        active = set()

        def checkout(connection, record, proxy):
            active.add(record)
            assert len(active) <= 1, "news source preparation borrowed a second connection"

        def checkin(connection, record):
            active.discard(record)

        event.listen(e.db.engine.sync_engine, "checkout", checkout)
        event.listen(e.db.engine.sync_engine, "checkin", checkin)
        try:
            result = await original(self, *args, **kwargs)
            assert not active
            observed.append(True)
            return result
        finally:
            event.remove(e.db.engine.sync_engine, "checkout", checkout)
            event.remove(e.db.engine.sync_engine, "checkin", checkin)

    monkeypatch.setattr(ActivationNewsOrigin, "prepare", bounded)
    await start_news(work_delivery, monkeypatch, "work.proposed")
    assert observed


@pytest.mark.asyncio
@pytest.mark.parametrize("later", [False, True])
async def test_original_proposal_launch_proof_remains_readable_after_work_advances(work_delivery, monkeypatch, later):
    from dataclasses import replace
    from uuid import uuid4

    from test_agent_work_records import command
    from test_work_execution import activation

    from app.telegram_bots.attention_authority import current_attention
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_continuation_sources import ActivationParentProjection
    from deerflow.agent_instances.work_execution import WorkExecution
    from deerflow.agent_instances.work_execution_contract import ActivateWork, ReportWork
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    acts, act, plans, item = await start_news(e, monkeypatch, "work.proposed")
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=act.work_id)
    execution = await e.http_app.state.agent_conversations.execution(actor=e.actor, thread_id=act.body["thread_id"])
    service = WorkExecution(e.human.work)
    scope = await service.reserve(execution=execution, work_id=act.work_id, request=ActivateWork.model_validate(dict(act.body)))
    execution = replace(execution, work=scope)
    run = await e.manager.get(current["attempt"]["run_id"], user_id=e.link.owner_user_id)
    await scope.bind(execution, run, execution.thread_incarnation)
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="Verified the references"))
    run.status = "success"
    await scope.finish(execution, run, settled=True)
    if later:
        current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=act.work_id)
        current = await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=act.work_id, request=command(current, "changes_requested", note="Verify another source"))
        next_execution = replace(execution, work=None)
        successor = await service.reserve(execution=next_execution, work_id=act.work_id, request=activation(next_execution, current))
        assert successor.attempt_id != scope.attempt_id

    class Source:
        def __init__(self, *, for_effect=False):
            self.parent = ActivationParentProjection(act, for_effect=for_effect)

        def extend(self, query):
            self.column = len(query.column_descriptions)
            return self.parent.extend(query.add_columns(TelegramActRow).join(TelegramActRow, TelegramActRow.id == act.id))

        def check_with_permissions(self, rows, held):
            self.parent.check(rows, rows[0][self.column], held)

    proof = Source()
    await proof.parent.prepare(acts, e.link)
    await current_attention(acts, e.link, projection=proof)
    mutation = Source(for_effect=True)
    await mutation.parent.prepare(acts, e.link)
    with pytest.raises(ProductSourceRetired):
        await current_attention(acts, e.link, projection=mutation)
    # Historical receipts cannot make the old proposal a fresh Start card.
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramOutboxRow, item.id)
        from app.telegram_bots.producer import _item

        original = _item(saved)
    with pytest.raises(ProductSourceRetired):
        await plans.reader.read(e.link, original)
    assert len(e.launches) == 1


@pytest.mark.asyncio
async def test_proposed_start_cannot_adopt_another_activation_reservation(work_delivery, monkeypatch):
    from test_work_execution import activation

    from deerflow.agent_instances.work_execution import WorkExecution
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = work_delivery
    acts, act, _plans, _item, _starts, _control, _scope = await pending_news(e, monkeypatch, "work.proposed")
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=act.work_id)
    execution = await e.http_app.state.agent_conversations.execution(actor=e.actor, thread_id=act.body["thread_id"])
    foreign = await WorkExecution(e.human.work).reserve(execution=execution, work_id=act.work_id, request=activation(execution, current))
    result = await acts.check(act.id)
    assert result.state != "done" and not e.launches
    async with e.db.sessions() as reader:
        assert await reader.get(AgentWorkAttemptRow, foreign.attempt_id) is not None
        assert await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == act.id)) is None
