"""Routing uses the existing human request transaction and immutable receipts."""

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_work_actions import actions as actions

from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow


@pytest.mark.asyncio
async def test_route_is_receipted_once_works_during_pause_and_grants_nothing(actions):
    e = actions
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramBotLinkRow, e.link.id)).acts_paused_since = e.acts.outbox.clock()
    link = await e.db.links.get(e.link.id)
    identity = operation()
    options = dict(operation_id=identity, request_id=e.request["id"], request_revision=e.request["request_revision"], recipient_id=e.actor.subject_id)
    first = await e.acts.route(link, **options)
    second = await e.acts.route(link, **options)
    assert first.state == second.state == "done" and first.id == second.id
    assert first.body == second.body
    current = await e.human.get(actor=e.actor, request_id=e.request["id"])
    assert current["request_revision"] == e.request["request_revision"] + 1
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(HumanInputEventRow).where(HumanInputEventRow.action == "route")) == 1
        assert await reader.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == 0
        assert (await reader.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions == 7
    with pytest.raises(ValueError, match="act-source-conflict"):
        await e.acts.route(link, **{**options, "recipient_id": "someone-else"})


@pytest.mark.asyncio
@pytest.mark.parametrize("recovery", ["explicit", "worker"])
async def test_route_lost_result_recovers_receipt_without_repeating_command(actions, monkeypatch, recovery):
    from deerflow.agent_instances.human_input import HumanInput

    e = actions
    original = HumanInput.command
    calls = []

    async def lose(self, **kwargs):
        calls.append(kwargs["request"].operation_id)
        await original(self, **kwargs)
        raise RuntimeError("lost command acknowledgement")

    monkeypatch.setattr(HumanInput, "command", lose)
    identity = operation()
    first = await e.acts.route(e.link, operation_id=identity, request_id=e.request["id"], request_revision=1, recipient_id=e.actor.subject_id)
    assert first.state == "pending"
    if recovery == "worker":
        from datetime import timedelta

        from app.telegram_bots.work_actions import ActsWorker

        now = e.acts.outbox.clock()
        e.acts.outbox.clock = lambda: now + timedelta(seconds=10)
        await ActsWorker(e.acts).tick()
        assert (await e.acts._work_record(identity)).state == "done"
    else:
        assert (await e.acts.check(identity)).state == "done"
    assert calls == [identity]


@pytest.mark.asyncio
async def test_route_binding_withdrawal_before_domain_commit_refuses_effect(actions, monkeypatch):
    from deerflow.agent_instances.human_input import HumanInput

    e = actions
    original = HumanInput.command

    async def withdraw(self, **kwargs):
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation += 1
        return await original(self, **kwargs)

    monkeypatch.setattr(HumanInput, "command", withdraw)
    result = await e.acts.route(e.link, operation_id=operation(), request_id=e.request["id"], request_revision=1, recipient_id=e.actor.subject_id)
    assert result.state == "refused"
    assert (await e.human.get(actor=e.actor, request_id=e.request["id"]))["request_revision"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["information", "decision", "review"])
async def test_recipient_read_uses_owner_http_grants_and_filters_before_cap(actions, monkeypatch, purpose):
    from app.gateway.auth.models import User
    from app.gateway.routers import agent_instances
    from app.telegram_bots.work_routing import WorkRoutingReader, present_routing
    from deerflow.agent_instances.human_input_contract import Respond
    from deerflow.config import agents_api_config
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = actions
    request = e.request
    if purpose != "information":
        from test_telegram_work_actions import management_request

        _record, request, _seen = await management_request(e, purpose)
    monkeypatch.setattr(agents_api_config, "_agents_api_config", agents_api_config.AgentsApiConfig(enabled=True))
    e.http_app.include_router(agent_instances.router)
    people = []
    for number in range(9):
        user = await e.live.users.create_user(User(email=f"person{number}@example.com"))
        people.append(str(user.id))
        async with e.db.sessions.begin() as writer:
            writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=str(user.id), permissions=(1 if purpose == "information" else 3) if number == 1 else 3 if purpose == "information" else 6))
            if number == 0:
                writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=str(user.id), disabled_at=e.acts.outbox.clock()))
    reader = WorkRoutingReader(work=e.human.work, authority=e.live.authority, links=e.db.links, client=e.http_client)
    value = await reader.read_routing(e.link, request["id"])
    assert value.recipients[0]["user_id"] == e.actor.subject_id
    assert [row["user_id"] for row in value.recipients[1:]] == people[2:]
    preview = present_routing(value, locale="en", link=e.link)
    controls = [button for row in preview.keyboard for button in row if button.get("kind") == "route_to"]
    assert len(controls) == 6
    assert all(button["label_values"]["label"] != button["payload"]["recipient_id"] for button in controls)
    assert any(method == "GET" and path.endswith("/grants") and user == e.actor.subject_id for method, path, user, _ in e.observed)
    await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=operation(), expected_request_revision=1, expected_assignment_revision=1, disposition="wrong_recipient", text="A colleague owns this"))
    refreshed = await reader.read_routing(e.link, request["id"])
    assert e.actor.subject_id not in {row["user_id"] for row in refreshed.recipients}
    if purpose == "information":
        from deerflow.spaces.contract import PrincipalRef

        await e.human.respond(actor=PrincipalRef("human", people[2]), request_id=request["id"], request=Respond(operation_id=operation(), expected_request_revision=1, expected_assignment_revision=1, text="A later colleague comment"))
        later = await reader.read_routing(e.link, request["id"])
        assert e.actor.subject_id not in {row["user_id"] for row in later.recipients}


@pytest.mark.asyncio
async def test_command_admission_rolls_back_after_projection_and_receipt_replay_is_read_only(actions):
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.agent_instances.human_input_contract import RequestCommand

    e = actions
    calls = []

    async def refuse_final(writer):
        calls.append(writer)
        assert writer.in_transaction()
        if len(calls) == 2:
            raise AgentDenied("Custody ended")

    body = RequestCommand(operation_id=operation(), action="route", expected_revision=1, expected_request_revision=1, recipient_id=e.actor.subject_id)
    with pytest.raises(AgentDenied):
        await e.human.command(actor=e.actor, request_id=e.request["id"], request=body, admission=refuse_final)
    assert len(calls) == 2 and calls[0] is calls[1]
    assert (await e.human.get(actor=e.actor, request_id=e.request["id"]))["revision"] == 1
    saved = await e.human.command(actor=e.actor, request_id=e.request["id"], request=body)

    async def forbidden(writer):
        raise AssertionError("An exact receipt cannot admit a new effect")

    replay = await e.human.command(actor=e.actor, request_id=e.request["id"], request=body, admission=forbidden)
    assert saved["receipt"] == replay["receipt"]


@pytest.mark.asyncio
async def test_concurrent_route_replays_the_original_body(actions):
    import asyncio

    e = actions
    options = dict(operation_id=operation(), request_id=e.request["id"], request_revision=1, recipient_id=e.actor.subject_id)
    first, second = await asyncio.gather(e.acts.route(e.link, **options), e.acts.route(e.link, **options))
    assert first.state == second.state == "done"
    assert first.body == second.body
    assert (await e.human.get(actor=e.actor, request_id=e.request["id"]))["request_revision"] == 2


@pytest.mark.asyncio
async def test_ineligible_recipient_and_stale_button_are_refused_without_grants(actions):
    from deerflow.agent_instances.human_input_contract import RequestCommand

    e = actions
    denied = await e.acts.route(e.link, operation_id=operation(), request_id=e.request["id"], request_revision=1, recipient_id="no-grant")
    assert denied.state == "refused"
    await e.human.command(actor=e.actor, request_id=e.request["id"], request=RequestCommand(operation_id=operation(), action="route", expected_revision=1, expected_request_revision=1, recipient_id=e.actor.subject_id))
    stale = await e.acts.route(e.link, operation_id=operation(), request_id=e.request["id"], request_revision=1, recipient_id=e.actor.subject_id)
    assert stale.state == "conflict" and stale.body["expected_request_revision"] == 1
    async with e.db.sessions() as reader:
        assert await reader.get(AgentInstanceGrantRow, (e.instance.id, "no-grant")) is None


@pytest.mark.asyncio
async def test_recipient_read_refuses_manager_withdrawal_during_http_read(actions, monkeypatch):
    from app.gateway.routers import agent_instances
    from app.telegram_bots.work_routing import WorkRoutingReader
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.config import agents_api_config

    e = actions
    monkeypatch.setattr(agents_api_config, "_agents_api_config", agents_api_config.AgentsApiConfig(enabled=True))
    e.http_app.include_router(agent_instances.router)
    original = e.http_client.get_agent_instance_grants

    async def withdraw(*args, **kwargs):
        result = await original(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 3
        return result

    monkeypatch.setattr(e.http_client, "get_agent_instance_grants", withdraw)
    reader = WorkRoutingReader(work=e.human.work, authority=e.live.authority, links=e.db.links, client=e.http_client)
    with pytest.raises(AgentDenied):
        await reader.read_routing(e.link, e.request["id"])


@pytest.mark.asyncio
@pytest.mark.parametrize("when", ["candidate", "final"])
async def test_routing_read_retires_in_place_auth_mode_change(actions, monkeypatch, when):
    from app.gateway.auth.models import User
    from app.gateway.routers import agent_instances
    from app.telegram_bots.work_routing import WorkRoutingReader
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.config import agents_api_config
    from deerflow.persistence.user.model import UserRow

    e = actions
    monkeypatch.setattr(agents_api_config, "_agents_api_config", agents_api_config.AgentsApiConfig(enabled=True))
    e.http_app.include_router(agent_instances.router)
    recipient = await e.live.users.create_user(User(email="recipient@example.com"))
    async with e.db.sessions.begin() as writer:
        owner = await writer.get(UserRow, e.actor.subject_id)
        owner.oauth_provider, owner.oauth_id, owner.oauth_issuer = "fixture", "owner", "https://identity.example"
        writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=str(recipient.id), permissions=3))
    original = type(e.live.users).get_user_by_id
    changed = []

    async def flip(repository, identity, *args, **kwargs):
        user = await original(repository, identity, *args, **kwargs)
        if when == "candidate" and identity == str(recipient.id) and not changed:
            e.live.config.auth.local.enabled = False
            changed.append(True)
        return user

    monkeypatch.setattr(type(e.live.users), "get_user_by_id", flip)
    reader = WorkRoutingReader(work=e.human.work, authority=e.live.authority, links=e.db.links, client=e.http_client)
    if when == "final":
        original_read, original_current = reader.read_request, reader._current
        completed = []

        async def read_request(*args):
            result = await original_read(*args)
            completed.append(True)
            return result

        async def last_check(*args):
            result = await original_current(*args)
            if len(completed) == 2 and not changed:
                e.live.config.auth.local.enabled = False
                changed.append(True)
            return result

        monkeypatch.setattr(reader, "read_request", read_request)
        monkeypatch.setattr(reader, "_current", last_check)
    with pytest.raises(AgentDenied):
        await reader.read_routing(e.link, e.request["id"])
    assert changed
