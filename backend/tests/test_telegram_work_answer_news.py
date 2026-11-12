"""Information-answer news requires the exact canonical human response receipt."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import _item
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def answered(e):
    from test_telegram_work_event_intents import activate, setup

    from deerflow.agent_instances.human_input_contract import RequestCommand, Respond
    from deerflow.agent_instances.work_execution_contract import ReportWork

    peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    e.answer_execution = execution
    current = (await scope.context(execution))["work"]
    blocked = await scope.report(
        execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="request_input", statement="Which guide?", purpose="information", reason="Need a source", expected_response="A reference")
    )
    await scope.finish(execution, run, settled=True)
    req = await e.human.get(actor=e.actor, request_id=blocked["human_input_request_id"])
    await e.human.command(
        actor=e.actor, request_id=req["id"], request=RequestCommand(operation_id=uuid4().hex, action="route", expected_revision=req["revision"], expected_request_revision=req["request_revision"], recipient_id=peer.subject_id)
    )
    req = await e.human.get(actor=peer, request_id=req["id"])
    await e.human.respond(
        actor=peer, request_id=req["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=req["request_revision"], expected_assignment_revision=req["assignment_revision"], text="Use the published guide.")
    )
    await worker.tick()
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.work_id == work["id"], TelegramOutboxRow.payload["work_event_intent"]["key"].as_string() == "request.answered"))
        assert row is not None
        return _item(row)


@pytest.mark.asyncio
async def test_answer_news_delivers_from_saved_information_receipt(work_delivery):
    from test_telegram_bot_adapter import message
    from test_telegram_work_news_delivery import prepare

    e = work_delivery
    item = await answered(e)
    _plans, identity = await prepare(e, item=item)
    message(e.channel.bot.request, identity=2901)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1
    assert calls[0].parameters["text"] == 'event-peer@example.com answered the question\n"Report findings"'
    assert calls[0].parameters["disable_notification"] is True
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["receipt", "response", "disposition", "actor", "purpose", "boolean"])
async def test_initial_answer_news_rejects_mismatched_receipt_or_comment(work_delivery, damage):
    from sqlalchemy.orm.attributes import flag_modified

    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_news_sources import WorkNewsReader
    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputRequestRow, HumanInputResponseRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    item = await answered(e)
    async with e.db.sessions.begin() as writer:
        event = await writer.get(AgentWorkEventRow, item.event_id)
        receipt = await writer.scalar(select(HumanInputEventRow).where(HumanInputEventRow.operation_id == event.operation_id))
        response = await writer.get(HumanInputResponseRow, item.payload["work_event_intent"]["response_id"])
        if damage == "receipt":
            receipt.request = {**receipt.request, "text": "A different response"}
        elif damage == "response":
            response.text = "Changed saved response"
        elif damage == "disposition":
            response.disposition = "cannot_provide"
        elif damage == "actor":
            response.actor_id = e.actor.subject_id
        elif damage == "purpose":
            request = await writer.get(HumanInputRequestRow, item.request_id)
            request.purpose, request.state = "decision", "pending"
        else:
            receipt.receipt = {**receipt.receipt, "request_revision": True}
            flag_modified(receipt, "receipt")
    reader = WorkNewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    with pytest.raises(ProductSourceRetired):
        await reader.read(e.link, item)


@pytest.mark.asyncio
async def test_answer_receipt_withdrawn_at_final_sdk_fence_is_not_sent(work_delivery, monkeypatch):
    from test_telegram_work_news_delivery import prepare

    from deerflow.persistence.agent_instances.human_input import HumanInputResponseRow

    e = work_delivery
    item = await answered(e)
    await prepare(e, item=item)
    sources = e.channel._services.work_news_sources
    original, reads = sources.settings.get, []

    async def change(*args, **kwargs):
        settings = await original(*args, **kwargs)
        reads.append(True)
        if len(reads) == 2:
            async with e.db.sessions.begin() as writer:
                response = await writer.get(HumanInputResponseRow, item.payload["work_event_intent"]["response_id"])
                response.text = "Changed during final preparation"
        return settings

    monkeypatch.setattr(sources.settings, "get", change)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len(reads) == 2
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, item.id)).state == "skipped"


@pytest.mark.asyncio
@pytest.mark.parametrize("unresolved", [False, True])
async def test_answer_news_offers_assignment_link_only_without_unresolved_attempt(work_delivery, unresolved):
    from dataclasses import replace

    from test_telegram_bot_adapter import message
    from test_telegram_work_news_delivery import prepare
    from test_work_execution import activation

    from deerflow.agent_instances.work_execution import WorkExecution

    e = work_delivery
    item = await answered(e)
    e.live.config.model_extra["telegram_bots"] = {**(e.live.config.model_extra.get("telegram_bots") or {}), "public_base_url": "https://example.test"}
    if unresolved:
        execution = replace(e.answer_execution, work=None)
        work = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id)
        await WorkExecution(e.human.work).reserve(execution=execution, work_id=item.work_id, request=activation(execution, work))
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=3001)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    sent = next(call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage")
    keyboard = sent.parameters.get("reply_markup")
    assert bool(keyboard) is not unresolved
    if keyboard:
        assert "https://example.test" in str(keyboard) and item.work_id in str(keyboard)


@pytest.mark.asyncio
async def test_answer_link_rechecks_resume_at_final_provider_boundary(work_delivery, monkeypatch):
    from dataclasses import replace

    from _telegram_bot_test_support import ManualClock
    from test_telegram_bot_adapter import message
    from test_telegram_work_news_delivery import prepare
    from test_work_execution import activation

    from deerflow.agent_instances.work_execution import WorkExecution

    e = work_delivery
    item = await answered(e)
    e.live.config.model_extra["telegram_bots"] = {**(e.live.config.model_extra.get("telegram_bots") or {}), "public_base_url": "https://example.test"}
    await prepare(e, item=item)
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    sources = e.channel._services.work_news_sources
    original, reads = sources.settings.get, []

    async def resume(*args, **kwargs):
        settings = await original(*args, **kwargs)
        reads.append(True)
        if len(reads) == 2:
            execution = replace(e.answer_execution, work=None)
            work = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id)
            await WorkExecution(e.human.work).reserve(execution=execution, work_id=item.work_id, request=activation(execution, work))
        return settings

    monkeypatch.setattr(sources.settings, "get", resume)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len(reads) >= 2 and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    message(e.channel.bot.request, identity=3101)
    for _ in range(2):
        clock.advance(10)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    sent = next(call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage")
    assert not sent.parameters.get("reply_markup")
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        assert row.state == "sent" and row.payload["presentation_version"] == 1
