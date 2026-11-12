"""Colleague news derives its words and recipient from actual Work commands."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_agent_work_records import command
from test_telegram_work_event_intents import setup
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import _item
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def news(e, action="edit"):
    peer, worker = await setup(e)
    work = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Original assignment", success_criteria="Check every fact"))
    if action == "reopen":
        work = await e.human.work.command(actor=peer, instance_id=e.instance.id, work_id=work["id"], request=command(work, "cancel"))
    extra = {"objective": "Changed assignment"} if action == "edit" else {"note": "Please revisit the findings"} if action == "reopen" else {}
    result = await e.human.work.command(actor=peer, instance_id=e.instance.id, work_id=work["id"], request=command(work, action, **extra))
    await worker.tick()
    key = {"edit": "work.changed", "cancel": "work.cancelled", "reopen": "work.reopened"}[action]
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.work_id == work["id"], TelegramOutboxRow.payload["work_event_intent"]["key"].as_string() == key))
        assert row is not None
        return _item(row), result, peer


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["edit", "cancel", "reopen"])
async def test_news_source_uses_colleague_and_canonical_assignment(work_delivery, action):
    from app.telegram_bots.work_news_sources import WorkNewsReader

    e = work_delivery
    item, result, _peer = await news(e, action)
    reader = WorkNewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    source = await reader.read(e.link, item)
    assert source.key == item.payload["work_event_intent"]["key"]
    assert source.values["objective"] == result["objective"]
    assert source.values["person"] == "event-peer@example.com"
    assert source.work_id == result["id"]
    assert source.protected is False


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["event", "work", "claim", "person"])
async def test_news_final_source_snapshot_refuses_mutation_after_provider(work_delivery, monkeypatch, change):
    import app.gateway.authz as authz
    from app.telegram_bots.attention_authority import prepare_attention
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_news_sources import WorkNewsProjection, WorkNewsReader
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow
    from deerflow.persistence.user.model import UserRow

    e = work_delivery
    item, _result, peer = await news(e)
    reader = WorkNewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    source = await reader.read(e.link, item)
    original, active, changed = authz.resolve_route_permissions, {}, []

    async def provider(*args, **kwargs):
        permissions = await original(*args, **kwargs)
        writer = active.get("writer")
        if writer is not None and not changed:
            if change == "event":
                (await writer.get(AgentWorkEventRow, item.event_id)).actor_id = e.actor.subject_id
            elif change == "work":
                (await writer.get(AgentWorkRow, item.work_id)).objective = "Changed after provider read"
            elif change == "person":
                (await writer.get(UserRow, peer.subject_id)).email = "other-name@example.com"
            else:
                (await writer.get(TelegramOutboxRow, item.id)).claim_id = "other-claim"
            await writer.flush()
            changed.append(True)
        return permissions

    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    admission = await prepare_attention(reader, e.link)
    with pytest.raises(ProductSourceRetired):
        async with e.db.sessions.begin() as writer:
            active["writer"] = writer
            await admission(session=writer, projection=WorkNewsProjection(reader, e.link, item, source))
    assert changed


@pytest.mark.asyncio
@pytest.mark.parametrize("action,key", [("accept", "result.accepted"), ("changes_requested", "result.changes"), ("decide", "decision.recorded")])
async def test_review_and_decision_news_use_original_requester_and_recorded_statement(work_delivery, action, key):
    from test_telegram_work_event_intents import activate

    from app.telegram_bots.work_news_sources import WorkNewsReader
    from deerflow.agent_instances.work_execution_contract import ReportWork

    e = work_delivery
    peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    current = (await scope.context(execution))["work"]
    if action == "decide":
        report = dict(action="request_input", statement="Which reference?", purpose="decision", reason="Need a decision", expected_response="Choose the reference")
    else:
        report = dict(action="outcome", statement="Verified result")
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], **report))
    await scope.finish(execution, run, settled=True)
    await worker.tick()
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"])
    if action == "accept":
        extra = dict(outcome_id=current["outcome_id"], evidence_revision=current["outcome"]["evidence_revision"], basis="outcome_statement", acknowledge_unchecked_sources=True)
    elif action == "decide":
        extra = dict(blocker_id=current["blocker"]["id"], blocker_revision=current["blocker"]["revision"], note="Use the published reference.")
    else:
        extra = dict(note="Explain the missing evidence.")
    if action in {"accept", "decide"}:
        req = await e.human.get(actor=peer, request_id=current["human_input_request_id"])
        extra["request_basis"] = {"id": req["id"], "revision": req["revision"], "request_revision": req["request_revision"], "response_ids": []}
    await e.human.work.command(actor=peer, instance_id=e.instance.id, work_id=work["id"], request=command(current, action, **extra))
    await worker.tick()
    async with e.db.sessions() as reader:
        item = _item(await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.work_id == work["id"], TelegramOutboxRow.payload["work_event_intent"]["key"].as_string() == key)))
    source = await WorkNewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read(e.link, item)
    assert source.key == key and source.values["person"] == "event-peer@example.com"
    if action == "decide":
        assert source.values["decision"] == extra["note"] and source.values["note"] is None
    elif action == "changes_requested":
        assert source.values["text"] == extra["note"]


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["work.changed", "concealed.info"])
async def test_generic_product_reply_cannot_impersonate_colleague_news(work_delivery, key):
    from test_telegram_bot_adapter import message

    e = work_delivery
    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    await e.sender.outbox.plan_product(e.link, source_id="unqualified-colleague-news", parts=[{"kind": "text", "source_ref": {"key": key, "values": {"objective": "Fabricated assignment", "person": "Fabricated colleague"}}}])
    message(e.channel.bot.request, identity=1901)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["operation", "attempt", "revision", "assignment", "result", "extra", "boolean"])
async def test_initial_news_signature_cannot_seal_malformed_command_receipt(work_delivery, damage):
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_news_sources import WorkNewsReader
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    item, _work, _peer = await news(e)
    async with e.db.sessions.begin() as writer:
        event = await writer.get(AgentWorkEventRow, item.event_id)
        if damage == "result":
            event.result = {**event.result, "revision": event.revision + 1}
        else:
            field, value = {
                "operation": ("operation_id", "e" * 32),
                "attempt": ("attempt_id", "e" * 32),
                "revision": ("expected_revision", 99),
                "assignment": ("expected_assignment_revision", 99),
                "extra": ("unrecognized", "value"),
                "boolean": ("expected_revision", True),
            }[damage]
            event.request = {**event.request, field: value}
            from sqlalchemy.orm.attributes import flag_modified

            flag_modified(event, "request")
    with pytest.raises(ProductSourceRetired):
        await WorkNewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read(e.link, item)
