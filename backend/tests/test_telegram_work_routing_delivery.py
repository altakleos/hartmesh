"""Recipient selection is a source-owned, fully acknowledged presentation."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def routing(e, monkeypatch, *, protected=False):
    from app.gateway.auth.models import User
    from app.gateway.routers import agent_instances
    from app.telegram_bots.work_routing import WorkRoutingReader
    from app.telegram_bots.work_routing_delivery import WorkRoutingPlans, WorkRoutingSources
    from deerflow.agent_instances.human_input_contract import Respond
    from deerflow.config import agents_api_config
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    monkeypatch.setattr(agents_api_config, "_agents_api_config", agents_api_config.AgentsApiConfig(enabled=True))
    e.http_app.include_router(agent_instances.router)
    person = await e.live.users.create_user(User(email="another@example.com"))
    e.recipient_id = str(person.id)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "sent"
        writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=e.recipient_id, permissions=3))
        if protected:
            policy = await writer.get(TelegramInstancePolicyRow, e.instance.id)
            if policy is None:
                writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.actor.subject_id, updated_at=e.sender.outbox.clock()))
            else:
                policy.policy = "protected"
    await e.human.respond(actor=e.actor, request_id=e.request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, disposition="wrong_recipient", text="A colleague knows"))
    reader = WorkRoutingReader(work=e.human.work, authority=e.live.authority, links=e.db.links, client=e.http_client)
    plans = WorkRoutingPlans(outbox=e.sender.outbox, reader=reader, callbacks=e.channel._services.callbacks)
    sources = WorkRoutingSources(reader=reader, outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
    e.channel._services.work_routing_sources = sources
    e.identity = await plans.plan(e.link, request_id=e.request["id"], dedupe_key="routing:" + uuid4().hex)
    return plans, sources


@pytest.mark.asyncio
async def test_routing_delivery_uses_saved_recipients_and_arms_only_after_ack(work_delivery, monkeypatch):
    e = work_delivery
    await routing(e, monkeypatch)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert len(controls) == 1 and controls[0].payload["presentation_pending"] is True
        assert controls[0].payload["recipient_id"] == e.recipient_id
    message(e.channel.bot.request, identity=901)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    call = next(call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage")
    assert "Who should answer instead?" in call.parameters["text"]
    assert "another@example.com" in str(call.parameters["reply_markup"])
    async with e.db.sessions() as reader:
        control = await reader.get(TelegramCallbackRow, controls[0].id)
        assert control.message_id == 901 and control.payload["presentation_pending"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["grant", "disabled", "rename", "manager", "request", "claim", "navigation"])
async def test_final_routing_snapshot_retires_changed_facts(work_delivery, monkeypatch, change):
    from datetime import UTC, datetime

    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.user.access import DisabledIdentityRow
    from deerflow.persistence.user.model import UserRow

    e = work_delivery
    _, sources = await routing(e, monkeypatch)
    original = sources.settings.get
    changed = []

    async def withdraw(*args, **kwargs):
        result = await original(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                if change in {"grant", "manager"}:
                    identity = e.recipient_id if change == "grant" else e.actor.subject_id
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, identity))).permissions = 1
                elif change == "disabled":
                    writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.recipient_id, disabled_at=datetime.now(UTC)))
                elif change == "rename":
                    (await writer.get(UserRow, e.recipient_id)).email = "changed@example.com"
                elif change == "navigation":
                    e.live.config.model_extra["telegram_bots"] = {**(e.live.config.model_extra.get("telegram_bots") or {}), "public_base_url": "https://changed.example"}
                elif change == "request":
                    (await writer.get(HumanInputRequestRow, e.request["id"])).revision += 1
                else:
                    (await writer.get(TelegramOutboxRow, e.identity)).claim_id = "foreign"
        return result

    monkeypatch.setattr(sources.settings, "get", withdraw)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryRetry, ProductSourceRetired

    assert changed and e.dispatch_errors
    assert isinstance(e.dispatch_errors[-1], (DeliveryClaimRetired, DeliveryRetry, ProductSourceRetired)), e.dispatch_errors
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)


@pytest.mark.asyncio
async def test_protected_routing_conceals_names_and_question(work_delivery, monkeypatch):
    e = work_delivery
    await routing(e, monkeypatch, protected=True)
    message(e.channel.bot.request, identity=902)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    call = next(call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage")
    assert "another@example.com" not in str(call.parameters)
    assert "Which guide?" not in call.parameters["text"]
    assert call.parameters["protect_content"] is True
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert [control.kind for control in controls] == ["reveal"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["recipient", "keyboard", "text", "version", "service"])
async def test_routing_cannot_bypass_its_closed_source(work_delivery, monkeypatch, damage):
    from copy import deepcopy

    e = work_delivery
    await routing(e, monkeypatch)
    if damage == "service":
        e.channel._services.work_routing_sources = None
    else:
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, e.identity)
            payload = deepcopy(row.payload)
            basis = payload["work_request_basis"]
            if damage == "recipient":
                basis["routing"]["recipients"][0]["name"] = "Invented name"
            elif damage == "keyboard":
                basis["keyboard"][0][0]["payload"]["recipient_id"] = e.actor.subject_id
            elif damage == "version":
                basis["version"] = True
            else:
                basis["parts"][0]["html"] = "Invented content"
            row.payload = payload
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert e.dispatch_errors
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)


@pytest.mark.asyncio
async def test_routing_preserves_complete_multipart_question_before_buttons(work_delivery, monkeypatch):
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow, HumanInputResponseRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    plans, _ = await routing(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
        for row in await writer.scalars(select(HumanInputResponseRow)):
            await writer.delete(row)
        request = await writer.get(HumanInputRequestRow, e.request["id"])
        request.recipient_id = None
        request.question = "Complete routing question. " * 230
        request.revision += 1
        (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
    e.identity = await plans.plan(e.link, request_id=e.request["id"], dedupe_key="routing-multipart")
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        total = len(row.parts)
        assert total > 1 and not row.parts[0].get("stored_keyboard")
    from datetime import UTC, datetime

    from test_telegram_outbox_workers import Clock
    from test_telegram_work_preview_recovery import dispatch

    e.clock = Clock()
    e.clock.now = datetime.now(UTC)
    e.sender.outbox.clock = e.clock
    for index in range(total):
        await dispatch(e, 910 + index)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, e.identity)
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
            assert all(control.payload["presentation_pending"] is (index < total - 1) for control in controls)
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == total
    assert "".join(call.parameters["text"] for call in calls).count("Complete routing question.") == 230


@pytest.mark.asyncio
async def test_generic_question_reveal_cannot_replace_routing_choices(work_delivery, monkeypatch):
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_request_recovery import WorkRequestRecovery

    e = work_delivery
    await routing(e, monkeypatch, protected=True)
    message(e.channel.bot.request, identity=920)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        original = row.payload
    with pytest.raises(ProductSourceRetired):
        await WorkRequestRecovery(e.plans).reveal(e.link, control.id, chat_id=e.link.chat_id, message_id=920, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, e.identity)).payload == original
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is None


@pytest.mark.asyncio
async def test_restored_excluded_recipient_retires_unrouted_card_at_final_snapshot(work_delivery, monkeypatch):
    from deerflow.agent_instances.human_input_contract import Respond
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.spaces.contract import PrincipalRef

    e = work_delivery
    plans, sources = await routing(e, monkeypatch)
    # A different person, now ineligible, is the latest wrong recipient. That
    # exclusion must not hide the fact that they become eligible again.
    await e.human.respond(
        actor=PrincipalRef("human", e.recipient_id), request_id=e.request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, disposition="wrong_recipient", text="No access yet")
    )
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
        (await writer.get(HumanInputRequestRow, e.request["id"])).recipient_id = e.recipient_id
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
    e.identity = await plans.plan(e.link, request_id=e.request["id"], dedupe_key="restored-recipient")
    original = sources.settings.get

    async def restore(*args, **kwargs):
        result = await original(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 3
        return result

    monkeypatch.setattr(sources.settings, "get", restore)
    message(e.channel.bot.request, identity=930)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 8])
async def test_routing_navigation_never_loads_configuration_in_rendering(work_delivery, monkeypatch, count):
    from app.gateway.auth.models import User
    from app.telegram_bots import pages
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    plans, _ = await routing(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
    for index in range(count):
        user = await e.live.users.create_user(User(email=f"routing{index}@example.com"))
        async with e.db.sessions.begin() as writer:
            writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=str(user.id), permissions=3))
    e.live.config.model_extra["telegram_bots"] = {**(e.live.config.model_extra.get("telegram_bots") or {}), "public_base_url": "https://product.example"}

    def forbidden():
        raise AssertionError("Routing rendering must not load file-backed config")

    monkeypatch.setattr(pages, "get_app_config", forbidden)
    e.identity = await plans.plan(e.link, request_id=e.request["id"], dedupe_key="routing-navigation")
    message(e.channel.bot.request, identity=940)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    call = next(call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage")
    assert "https://product.example/workspace" in str(call.parameters["reply_markup"])


@pytest.mark.asyncio
async def test_transport_replacement_keeps_routing_source_for_qualified_repair(work_delivery, monkeypatch):
    e = work_delivery
    await routing(e, monkeypatch)
    async with e.db.sessions() as reader:
        original = (await reader.get(TelegramOutboxRow, e.identity)).payload
    e.channel.bot.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message thread not found"})
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.payload == original
        assert row.state == "failed" and row.failed_reason == "work-routing-refresh-required"
        assert row.claim_id is None


@pytest.mark.asyncio
async def test_routing_read_ignores_unrelated_execution_probe_changes(work_delivery, monkeypatch):
    from dataclasses import replace

    e = work_delivery
    plans, _ = await routing(e, monkeypatch)
    original = plans.reader.read_request
    calls = []

    async def changing_probe(*args, **kwargs):
        source = await original(*args, **kwargs)
        calls.append(True)
        return replace(source, work={**source.work, "execution_available": len(calls) == 1})

    monkeypatch.setattr(plans.reader, "read_request", changing_probe)
    value = await plans.reader.read_routing(e.link, e.request["id"])
    assert len(calls) == 2 and value.recipients[0]["user_id"] == e.recipient_id
