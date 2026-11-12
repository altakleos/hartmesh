"""Canonical unanswered Work events reach eligible managers through the SDK."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_event_intents import intents, setup
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.human_input_contract import CreateRequest, Respond
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def routing_event(e, monkeypatch, disposition=None, *, reference=False):
    from app.gateway.routers import agent_instances
    from app.telegram_bots.work_routing import WorkRoutingReader
    from app.telegram_bots.work_routing_delivery import WorkRoutingPlans, WorkRoutingSources
    from deerflow.config import agents_api_config

    monkeypatch.setattr(agents_api_config, "_agents_api_config", agents_api_config.AgentsApiConfig(enabled=True))
    e.http_app.include_router(agent_instances.router)
    peer, worker = await setup(e)
    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    work = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Investigate evidence", success_criteria="Explain findings"))
    req = await e.human.create(
        actor=peer,
        instance_id=e.instance.id,
        work_id=work["id"],
        request=CreateRequest(
            operation_id=uuid4().hex, expected_work_revision=1, expected_assignment_revision=1, purpose="information", question="Which source?", reason="Need context", expected_response="Source identity", recipient_id=peer.subject_id
        ),
    )
    sources = []
    if reference:
        from types import SimpleNamespace

        from deerflow.agent_instances.work_contract import WorkSource
        from deerflow.persistence.spaces.model import SpaceGrantRow
        from deerflow.spaces.registry import SpaceRegistry

        e.human.agents.files.registry = SimpleNamespace(_view=SpaceRegistry._view)
        async with e.db.sessions.begin() as writer:
            for person in (peer.subject_id, e.actor.subject_id):
                writer.add(SpaceGrantRow(space_id=e.instance.home_id, principal_kind="human", subject_id=person, permissions=1))
        sources = [WorkSource(space_id=e.instance.home_id, path="evidence.txt")]
    if disposition:
        await e.human.respond(
            actor=peer, request_id=req["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, disposition=disposition, text="The archivist has this evidence.", sources=sources)
        )
    else:
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, peer.subject_id))).permissions = 2
    if reference == "hidden":
        async with e.db.sessions.begin() as writer:
            await writer.delete(await writer.get(SpaceGrantRow, (e.instance.home_id, "human", e.actor.subject_id)))
    await worker.tick()
    row = next(item for item in await intents(e) if item.work_id == work["id"] and item.payload["work_event_intent"]["key"] == "request.unrouted")
    reader = WorkRoutingReader(work=e.human.work, authority=e.live.authority, links=e.db.links, client=e.http_client)
    plans = WorkRoutingPlans(outbox=e.sender.outbox, reader=reader, callbacks=e.channel._services.callbacks)
    e.channel._services.work_routing_sources = WorkRoutingSources(reader=reader, outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
    return plans, row.id, req, peer


@pytest.mark.asyncio
@pytest.mark.parametrize("disposition", [None, "cannot_provide", "wrong_recipient"])
@pytest.mark.parametrize("reference", [False, True, "hidden"])
async def test_manager_gets_original_routing_reason_and_current_choices(work_delivery, monkeypatch, disposition, reference):
    from app.telegram_bots.work_routing_event_delivery import WorkRoutingEventDelivery

    e = work_delivery
    plans, identity, req, peer = await routing_event(e, monkeypatch, disposition, reference=reference)
    assert await WorkRoutingEventDelivery(plans).materialize(e.link, identity)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        parts = row.parts
        if reference and disposition:
            projected = row.payload["work_request_basis"]["source"]["responses"][0]["sources"][0]
            assert projected["current_contents"] == "not_checked"
            if reference == "hidden":
                assert projected == {"kind": "unavailable", "current_contents": "not_checked"}
            else:
                assert projected["path"] == "evidence.txt"
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parts[-1]["callback_group_id"])))
        assert controls and all(control.payload["presentation_pending"] for control in controls)
        if disposition == "wrong_recipient":
            assert all(control.payload["recipient_id"] != peer.subject_id for control in controls)
    assert await WorkRoutingEventDelivery(plans).materialize(e.link, identity)
    message(e.channel.bot.request, identity=890)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    call = next(call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage")
    assert "Which source?" in call.parameters["text"]
    if disposition:
        assert "The archivist has this evidence." in call.parameters["text"]
        assert "event-peer@example.com" in call.parameters["text"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.request_id == req["id"]
        assert row.parts[-1]["callback_group_id"] == parts[-1]["callback_group_id"]
        for control in controls:
            assert (await reader.get(TelegramCallbackRow, control.id)).payload["presentation_pending"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["event", "response", "response_body", "recipient", "manager"])
async def test_routing_event_final_sdk_snapshot_rejects_changed_source(work_delivery, monkeypatch, change):
    from app.telegram_bots.work_routing_event_delivery import WorkRoutingEventDelivery
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    plans, identity, _req, peer = await routing_event(e, monkeypatch, "cannot_provide")
    await WorkRoutingEventDelivery(plans).materialize(e.link, identity)
    sources = e.channel._services.work_routing_sources
    get, changed = sources.settings.get, []

    async def alter(*args, **kwargs):
        settings = await get(*args, **kwargs)
        if not changed:
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramOutboxRow, identity)
                if change == "event":
                    (await writer.get(AgentWorkEventRow, row.event_id)).actor_id = "other-actor"
                elif change == "response":
                    intent = row.payload["work_event_intent"]
                    row.payload = {**row.payload, "work_event_intent": {**intent, "response_id": "wrong-source"}}
                elif change == "response_body":
                    from deerflow.persistence.agent_instances.human_input import HumanInputResponseRow

                    (await writer.get(HumanInputResponseRow, row.payload["work_event_intent"]["response_id"])).text = "Changed after preparation"
                else:
                    owner = e.actor.subject_id if change == "manager" else peer.subject_id
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, owner))).permissions = 2
            changed.append(True)
        return settings

    monkeypatch.setattr(sources.settings, "get", alter)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and e.dispatch_errors
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", [1, 2])
@pytest.mark.parametrize("change", ["event", "response", "recipient"])
async def test_routing_materialization_rechecks_event_and_roster_after_provider(work_delivery, monkeypatch, phase, change):
    import app.gateway.authz as authz
    import app.telegram_bots.work_event_delivery as module
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_routing_event_delivery import WorkRoutingEventDelivery, _RoutingMaterializationProjection
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    plans, identity, _req, peer = await routing_event(e, monkeypatch, "cannot_provide")
    prepare, resolve = module.prepare_attention, authz.resolve_route_permissions
    active, changed = {}, []

    async def prepared(*args, **kwargs):
        admission = await prepare(*args, **kwargs)

        async def check(**options):
            active["writer"] = options["session"]
            active["phase"] = 2 if isinstance(options["projection"], _RoutingMaterializationProjection) else 1
            try:
                return await admission(**options)
            finally:
                active.clear()

        return check

    async def provider(*args, **kwargs):
        held = await resolve(*args, **kwargs)
        if active.get("phase") == phase and not changed:
            writer = active["writer"]
            row = await writer.get(TelegramOutboxRow, identity)
            if change == "event":
                (await writer.get(AgentWorkEventRow, row.event_id)).actor_id = "other-actor"
            elif change == "response":
                from deerflow.persistence.agent_instances.human_input import HumanInputResponseRow

                (await writer.get(HumanInputResponseRow, row.payload["work_event_intent"]["response_id"])).text = "Changed during provider"
            else:
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, peer.subject_id))).permissions = 2
            await writer.flush()
            changed.append(True)
        return held

    monkeypatch.setattr(module, "prepare_attention", prepared)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    with pytest.raises(ProductSourceRetired):
        await WorkRoutingEventDelivery(plans).materialize(e.link, identity)
    assert changed
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert not row.parts and "work_event_source" not in row.payload


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [False, True])
@pytest.mark.parametrize("reference", [False, True, "hidden"])
async def test_protected_manager_notice_reveals_exact_explanation_and_roster(work_delivery, monkeypatch, damage, reference):
    from test_telegram_work_preview_recovery import dispatch, install, reveal

    from app.telegram_bots.work_routing_event_delivery import WorkRoutingEventDelivery
    from app.telegram_bots.work_routing_recovery import WorkRoutingRecovery
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    install(e)
    plans, identity, _req, _peer = await routing_event(e, monkeypatch, "wrong_recipient", reference=reference)
    e.identity = identity
    e.recovery = WorkRoutingRecovery(plans)
    e.channel._services.work_routing_sources.recovery = e.recovery
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.actor.subject_id, updated_at=e.clock()))
    await WorkRoutingEventDelivery(plans).materialize(e.link, identity)
    await dispatch(e, 821)
    call = next(call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage")
    assert call.parameters["text"] == "A question is waiting."
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    if damage:
        from app.telegram_bots.producer import ProductSourceRetired
        from deerflow.persistence.agent_instances.human_input import HumanInputResponseRow

        async with e.db.sessions.begin() as writer:
            (await writer.get(HumanInputResponseRow, row.payload["work_event_intent"]["response_id"])).text = "Changed before Show"
        with pytest.raises(ProductSourceRetired):
            await reveal(e, show.id)
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, identity)).payload["presentation_version"] == 0
            assert (await reader.get(TelegramCallbackRow, show.id)).consumed_at is None
        return
    await reveal(e, show.id)
    e.clock.advance(10)
    message(e.channel.bot.request, endpoint="editMessageText", identity=821)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    call = next(call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText")
    assert "The archivist has this evidence." in call.parameters["text"]
    assert call.parameters["reply_markup"]


@pytest.mark.asyncio
async def test_original_routing_intent_does_not_follow_a_later_route(work_delivery, monkeypatch):
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_routing_event_delivery import WorkRoutingEventDelivery
    from deerflow.agent_instances.human_input_contract import RequestCommand

    e = work_delivery
    plans, identity, req, _peer = await routing_event(e, monkeypatch, "wrong_recipient")
    current = await e.human.get(actor=e.actor, request_id=req["id"])
    await e.human.command(actor=e.actor, request_id=req["id"], request=RequestCommand(operation_id=uuid4().hex, action="route", expected_revision=current["revision"], expected_request_revision=1, recipient_id=e.actor.subject_id))
    with pytest.raises(ProductSourceRetired):
        await WorkRoutingEventDelivery(plans).materialize(e.link, identity)


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["decision", "review"])
async def test_runtime_question_waits_for_attempt_and_review_uses_settlement(work_delivery, monkeypatch, purpose):
    from test_telegram_work_event_intents import activate

    from app.telegram_bots.work_event_intents import WorkEventIntents
    from app.telegram_bots.work_event_reader import WorkEventReader
    from app.telegram_bots.work_routing_event_delivery import WorkRoutingEventDelivery
    from deerflow.agent_instances.work_execution_contract import ReportWork
    from deerflow.persistence.agent_instances.model import AgentInstanceRow

    e = work_delivery
    plans, _old, _req, _peer = await routing_event(e, monkeypatch)
    work, execution, scope, run = await activate(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceRow, e.instance.id)).supervisor_id = "no-supervisor"
    current = (await scope.context(execution))["work"]
    fields = (
        {"action": "outcome", "statement": "Verified evidence."}
        if purpose == "review"
        else {"action": "request_input", "statement": "Choose evidence", "purpose": "decision", "reason": "Need authorization", "expected_response": "A decision"}
    )
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], **fields))
    if purpose == "review":
        await scope.finish(execution, run, settled=True)
    worker = WorkEventReader(links=e.db.links, project=WorkEventIntents(links=e.db.links, human=e.human).project)
    await worker.tick()
    row = next(item for item in await intents(e) if item.work_id == work["id"] and item.payload["work_event_intent"]["key"] == "request.unrouted")
    materializer = WorkRoutingEventDelivery(plans)
    if purpose == "decision":
        assert await materializer.materialize(e.link, row.id) is False
        await scope.finish(execution, run, settled=True)
    assert await materializer.materialize(e.link, row.id)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, row.id)).parts


@pytest.mark.asyncio
async def test_routing_refresh_cannot_replace_the_canonical_response_explanation(work_delivery, monkeypatch):
    from test_telegram_work_preview_recovery import install

    from app.telegram_bots.work_routing_event_delivery import WorkRoutingEventDelivery
    from app.telegram_bots.work_routing_recovery import WorkRoutingRecovery
    from deerflow.persistence.agent_instances.human_input import HumanInputResponseRow

    e = work_delivery
    install(e)
    plans, identity, _req, _peer = await routing_event(e, monkeypatch, "cannot_provide")
    sources = e.channel._services.work_routing_sources
    sources.recovery = WorkRoutingRecovery(plans)
    await WorkRoutingEventDelivery(plans).materialize(e.link, identity)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        response = await writer.get(HumanInputResponseRow, row.payload["work_event_intent"]["response_id"])
        response.text = "A different explanation after materialization."
    for _ in range(2):
        e.clock.advance(10)
        message(e.channel.bot.request, identity=897)
        await e.sender.tick()
        await e.sender.drain()
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["presentation_version"] == 0
