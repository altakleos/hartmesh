"""Durable human input through the qualified Work adapter, without execution."""

import asyncio

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_instances import instances as instances
from test_agent_work_records import command, delegate
from test_agent_work_records import work as work

from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.human_input_contract import CreateRequest, RequestCommand, Respond
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow


@pytest_asyncio.fixture
async def attention(work):
    service, instance, sf = work
    record = await delegate(service, instance)
    return HumanInput(service), instance, record, sf


async def ask(fixture, **changes):
    service, instance, work, _ = fixture
    body = CreateRequest(
        operation_id=operation(),
        expected_work_revision=work["revision"],
        expected_assignment_revision=work["assignment_revision"],
        purpose="information",
        question="Which guide should be checked?",
        reason="The source is missing.",
        expected_response="A file reference or explanation",
        **changes,
    )
    return await service.create(actor=ALICE, instance_id=instance.id, work_id=work["id"], request=body)


def reply(request, **changes):
    return Respond(operation_id=operation(), expected_request_revision=request["request_revision"], expected_assignment_revision=request["assignment_revision"], text="Use the current guide.", **changes)


@pytest.mark.asyncio
async def test_addressed_discovery_and_concurrent_attributed_replies(attention):
    service, instance, work, sf = attention
    request = await ask(attention)
    assert request["recipient_id"] == "bob" and request["state"] == "pending"
    assert (await service.list(actor=BOB))["counts"]["pending"] == 1
    assert (await service.list(actor=ALICE))["counts"]["pending"] == 0
    bodies = [reply(request), reply(request)]
    results = await asyncio.gather(*(service.respond(actor=actor, request_id=request["id"], request=body) for actor, body in zip((ALICE, BOB), bodies)))
    assert len({r["receipt"]["response_id"] for r in results}) == 2
    current = await service.get(actor=BOB, request_id=request["id"])
    assert current["request_revision"] == 1 and current["revision"] == 3
    assert current["state"] == "answered" and current["execution_available"] is False
    assert (await service.list(actor=BOB))["counts"] == {"pending": 0, "routing": 0, "answered": 1}
    assert len((await service.responses(actor=BOB, request_id=request["id"]))["responses"]) == 2
    retried = await service.respond(actor=ALICE, request_id=request["id"], request=bodies[0])
    assert retried["receipt"] == results[0]["receipt"]
    with pytest.raises(AgentConflict):
        await service.respond(actor=BOB, request_id=request["id"], request=bodies[0])
    assert (await service.work.get(actor=ALICE, instance_id=instance.id, work_id=work["id"]))["status"] == "blocked"


@pytest.mark.asyncio
async def test_revocation_routes_without_leaking_or_silently_reassigning(attention):
    service, instance, work, sf = attention
    request = await ask(attention)
    async with sf() as s, s.begin():
        (await s.get(AgentInstanceGrantRow, (instance.id, "bob"))).permissions = 2
    assert (await service.list(actor=BOB))["counts"]["pending"] == 0
    assert (await service.get(actor=BOB, request_id=request["id"]))["needs_routing"] is True
    with pytest.raises(AgentDenied):
        await service.respond(actor=BOB, request_id=request["id"], request=reply(request))
    assert (await service.list(actor=ALICE, view="routing"))["counts"]["routing"] == 1
    routed = await service.command(actor=ALICE, request_id=request["id"], request=RequestCommand(operation_id=operation(), action="route", expected_revision=1, expected_request_revision=1, recipient_id="alice"))
    assert routed["request_revision"] == 2
    with pytest.raises(AgentConflict):
        await service.respond(actor=ALICE, request_id=request["id"], request=reply(request))
    async with sf() as s, s.begin():
        (await s.get(AgentInstanceGrantRow, (instance.id, "bob"))).permissions = 1
    with pytest.raises(AgentDenied):
        await service.get(actor=BOB, request_id=request["id"])
    assert not (await service.list(actor=BOB, view="all"))["requests"]


@pytest.mark.asyncio
async def test_read_and_cannot_provide_do_not_resolve(attention):
    service, _, _, _ = attention
    request = await ask(attention)
    await service.mark_read(actor=BOB, request_id=request["id"], revision=1)
    current = await service.get(actor=BOB, request_id=request["id"])
    assert current["revision"] == 1 and current["read_revision"] == 1
    await service.respond(actor=BOB, request_id=request["id"], request=reply(request, disposition="cannot_provide"))
    assert (await service.get(actor=BOB, request_id=request["id"]))["state"] == "pending"
    assert (await service.list(actor=BOB))["counts"]["pending"] == 1


@pytest.mark.asyncio
async def test_work_scope_change_atomically_supersedes_and_rejects_late_reply(attention):
    service, instance, work, _ = attention
    request = await ask(attention)
    current = await service.work.get(actor=ALICE, instance_id=instance.id, work_id=work["id"])
    await service.work.command(actor=ALICE, instance_id=instance.id, work_id=work["id"], request=command(current, "edit", objective="Changed scope"))
    closed = await service.get(actor=BOB, request_id=request["id"])
    assert closed["state"] == "closed" and closed["closed_reason"] == "superseded"
    with pytest.raises(AgentConflict):
        await service.respond(actor=BOB, request_id=request["id"], request=reply(request))


@pytest.mark.asyncio
async def test_decision_requires_canonical_command_with_exact_response_set(attention):
    service, instance, work, _ = attention
    body = CreateRequest(operation_id=operation(), expected_work_revision=1, expected_assignment_revision=1, purpose="decision", question="Select the scope", reason="A scope decision is needed", expected_response="An explicit decision")
    req = await service.create(actor=ALICE, instance_id=instance.id, work_id=work["id"], request=body)
    response = await service.respond(actor=BOB, request_id=req["id"], request=reply(req).model_copy(update={"text": "approved"}))
    assert response["state"] == "pending"
    current = await service.work.get(actor=BOB, instance_id=instance.id, work_id=work["id"])
    args = dict(blocker_id=req["basis_id"], blocker_revision=req["basis_revision"], note="Proceed with the guide")
    for action in ("input", "decide"):
        with pytest.raises(AgentConflict):
            await service.work.command(actor=BOB, instance_id=instance.id, work_id=work["id"], request=command(current, action, **args))
    basis = dict(id=req["id"], revision=response["revision"], request_revision=response["request_revision"], response_ids=[])
    with pytest.raises(AgentConflict):
        await service.work.command(actor=BOB, instance_id=instance.id, work_id=work["id"], request=command(current, "decide", **args, request_basis=basis))
    basis["response_ids"] = [response["receipt"]["response_id"]]
    decided = await service.work.command(actor=BOB, instance_id=instance.id, work_id=work["id"], request=command(current, "decide", **args, request_basis=basis))
    assert decided["status"] == "open"
    assert (await service.get(actor=BOB, request_id=req["id"]))["closed_reason"] == "resolved"


@pytest.mark.asyncio
async def test_review_context_is_not_acceptance_and_exact_outcome_is_required(attention):
    from test_agent_work_records import submit_fixture

    service, instance, work, sf = attention
    await submit_fixture(sf, work)
    body = CreateRequest(operation_id=operation(), expected_work_revision=1, expected_assignment_revision=1, purpose="review", question="Review the result", reason="Required review", expected_response="Review the outcome statement")
    req = await service.create(actor=ALICE, instance_id=instance.id, work_id=work["id"], request=body)
    responded = await service.respond(actor=BOB, request_id=req["id"], request=reply(req))
    assert responded["state"] == "pending"
    current = await service.work.get(actor=BOB, instance_id=instance.id, work_id=work["id"])
    accepted = await service.work.command(
        actor=BOB,
        instance_id=instance.id,
        work_id=work["id"],
        request=command(
            current,
            "accept",
            outcome_id=req["basis_id"],
            evidence_revision=req["basis_revision"],
            basis="outcome_statement",
            acknowledge_unchecked_sources=True,
            request_basis=dict(id=req["id"], revision=responded["revision"], request_revision=req["request_revision"], response_ids=[responded["receipt"]["response_id"]]),
        ),
    )
    assert accepted["status"] == "completed"
    assert (await service.get(actor=BOB, request_id=req["id"]))["closed_reason"] == "resolved"


@pytest.mark.asyncio
async def test_withdrawal_fences_unseen_replies_and_keeps_attribution(attention):
    service, instance, work, sf = attention
    req = await ask(attention)
    answer = await service.respond(actor=BOB, request_id=req["id"], request=reply(req))
    second = await service.respond(actor=ALICE, request_id=req["id"], request=reply(req, disposition="cannot_provide"))
    assert second["state"] == "answered"
    body = RequestCommand(
        operation_id=operation(), action="withdraw", note="The scope no longer needs this source", expected_revision=second["revision"], expected_request_revision=req["request_revision"], response_ids=[answer["receipt"]["response_id"]]
    )
    with pytest.raises(AgentConflict):
        await service.command(actor=ALICE, request_id=req["id"], request=body)
    body = body.model_copy(update={"response_ids": [answer["receipt"]["response_id"], second["receipt"]["response_id"]]})
    closed = await service.command(actor=ALICE, request_id=req["id"], request=body)
    assert closed["closed_reason"] == "withdrawn"
    assert len((await service.responses(actor=ALICE, request_id=req["id"]))["responses"]) == 2
    assert (await service.history(actor=ALICE, request_id=req["id"]))["events"][0]["actor_id"] == "alice"


@pytest.mark.asyncio
async def test_archived_lifecycle_removes_response_controls_and_admission(attention):
    from deerflow.persistence.agent_instances.model import AgentInstanceRow

    service, instance, _, sf = attention
    req = await ask(attention)
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceRow, instance.id)).status = "archived"
    shown = await service.get(actor=BOB, request_id=req["id"])
    assert not shown["can_respond"] and shown["needs_routing"]
    with pytest.raises(AgentDenied):
        await service.respond(actor=BOB, request_id=req["id"], request=reply(req))


@pytest.mark.asyncio
async def test_decision_rechecks_source_references_in_human_replies(attention):
    from deerflow.agent_instances.work_contract import WorkSource
    from deerflow.persistence.spaces.model import SpaceGrantRow

    service, instance, work, sf = attention
    req = await service.create(
        actor=ALICE,
        instance_id=instance.id,
        work_id=work["id"],
        request=CreateRequest(operation_id=operation(), expected_work_revision=1, expected_assignment_revision=1, purpose="decision", question="Select a source", reason="Need authority", expected_response="A decision"),
    )
    response = await service.respond(actor=BOB, request_id=req["id"], request=reply(req, sources=[WorkSource(space_id=instance.home_id, path="guide.txt")]))
    async with sf() as session, session.begin():
        await session.delete(await session.get(SpaceGrantRow, (instance.home_id, "human", "alice")))
    current = await service.work.get(actor=ALICE, instance_id=instance.id, work_id=work["id"])
    with pytest.raises(AgentDenied):
        await service.work.command(
            actor=ALICE,
            instance_id=instance.id,
            work_id=work["id"],
            request=command(
                current,
                "decide",
                note="Use this source",
                blocker_id=req["basis_id"],
                blocker_revision=1,
                request_basis={"id": req["id"], "revision": response["revision"], "request_revision": 1, "response_ids": [response["receipt"]["response_id"]]},
            ),
        )
    assert (await service.get(actor=BOB, request_id=req["id"]))["state"] == "pending"
    assert (await service.responses(actor=ALICE, request_id=req["id"]))["responses"][0]["sources"][0]["kind"] == "unavailable"


@pytest.mark.asyncio
async def test_request_migration_roundtrip_and_used_history_protection(attention):
    import importlib

    from alembic.operations import Operations
    from alembic.runtime.migration import MigrationContext

    service, instance, work, sf = attention
    migration = importlib.import_module("deerflow.persistence.migrations.versions.0039_human_input")

    def rebuild(conn):
        with Operations.context(MigrationContext.configure(conn)):
            migration.upgrade()
            migration.downgrade()
            migration.upgrade()
            migration.upgrade()

    async with sf.kw["bind"].begin() as conn:
        await conn.run_sync(rebuild)
    req = await ask(attention)

    def refuse(conn):
        with Operations.context(MigrationContext.configure(conn)), pytest.raises(RuntimeError, match="Refusing to erase"):
            migration.downgrade()

    async with sf.kw["bind"].begin() as conn:
        await conn.run_sync(refuse)
    # A fresh service still discovers the persisted request without a chat/run.
    assert (await HumanInput(service.work).get(actor=BOB, request_id=req["id"]))["id"] == req["id"]


@pytest.mark.asyncio
async def test_http_request_permissions_discovery_and_no_execution(attention, monkeypatch):
    from types import SimpleNamespace

    import httpx
    from test_agent_instances import request_app

    from app.gateway.routers import agent_instances as routes
    from app.gateway.routers.human_input import router

    service, instance, work, sf = attention
    req = await ask(attention)
    monkeypatch.setattr(routes, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    for permissions, status in [(("agents:read",), 403), (("agents:read", "agents:write"), 200)]:
        app = request_app(service.agents, actor=BOB, permissions=permissions)
        app.include_router(router)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            inbox = await client.get("/api/human-input")
            assert inbox.status_code == 200, inbox.text
            assert inbox.headers["cache-control"] == "no-store"
            assert inbox.json()["counts"]["pending"] == (1 if status == 200 else 0)
            response = await client.post(f"/api/human-input/{req['id']}/responses", json=reply(req).model_dump(mode="json"))
            assert response.status_code == status, response.text
            assert (await client.post(f"/api/human-input/{req['id']}/resume", json={})).status_code == 404


@pytest.mark.asyncio
async def test_grant_restoration_preserves_recorded_recipient_and_supervisor_change_does_not_route(attention):
    from deerflow.persistence.agent_instances.model import AgentInstanceRow

    service, instance, _, sf = attention
    req = await ask(attention)
    async with sf() as s, s.begin():
        (await s.get(AgentInstanceGrantRow, (instance.id, "bob"))).permissions = 2
        (await s.get(AgentInstanceRow, instance.id)).supervisor_id = "alice"
    assert (await service.get(actor=ALICE, request_id=req["id"]))["recipient_id"] == "bob"
    assert (await service.list(actor=ALICE, view="routing"))["counts"]["routing"] == 1
    async with sf() as s, s.begin():
        (await s.get(AgentInstanceGrantRow, (instance.id, "bob"))).permissions = 7
    assert (await service.list(actor=BOB))["counts"]["pending"] == 1
    assert (await service.list(actor=ALICE))["counts"]["pending"] == 0


@pytest.mark.asyncio
async def test_closed_history_is_excluded_from_action_poll_and_receipt_recovery_stays_authorized(attention, monkeypatch):
    service, instance, work, sf = attention
    req = await ask(attention)
    body = RequestCommand(operation_id=operation(), action="withdraw", note="The scope changed", expected_revision=1, expected_request_revision=1)
    closed = await service.command(actor=ALICE, request_id=req["id"], request=body)
    assert not closed["can_manage"] and closed["can_recover_management"]
    original = service._project

    async def project(*args, **kwargs):
        assert args[4].state != "closed", "An actionable poll must not project closed history"
        return await original(*args, **kwargs)

    monkeypatch.setattr(service, "_project", project)
    assert (await service.list(actor=BOB))["counts"] == dict(pending=0, routing=0, answered=0)
    monkeypatch.setattr(service, "_project", original)
    assert (await service.command(actor=ALICE, request_id=req["id"], request=body))["receipt"] == closed["receipt"]
    async with sf() as s, s.begin():
        (await s.get(AgentInstanceGrantRow, (instance.id, "alice"))).permissions = 2
    shown = await service.get(actor=ALICE, request_id=req["id"])
    assert not shown["can_recover_management"] and not shown["can_recover_response"]
    with pytest.raises(AgentDenied):
        await service.command(actor=ALICE, request_id=req["id"], request=body)


@pytest.mark.asyncio
async def test_failed_canonical_acceptance_rolls_back_request_closure(attention):
    from test_agent_work_records import submit_fixture

    service, instance, work, sf = attention
    await submit_fixture(sf, work)
    req = await service.create(
        actor=ALICE,
        instance_id=instance.id,
        work_id=work["id"],
        request=CreateRequest(operation_id=operation(), expected_work_revision=1, expected_assignment_revision=1, purpose="review", question="Review outcome", reason="Required review", expected_response="Acceptance or changes"),
    )
    current = await service.work.get(actor=ALICE, instance_id=instance.id, work_id=work["id"])
    with pytest.raises(AgentConflict):
        await service.work.command(
            actor=ALICE,
            instance_id=instance.id,
            work_id=work["id"],
            request=command(
                current, "accept", outcome_id=operation(), evidence_revision=1, basis="outcome_statement", acknowledge_unchecked_sources=True, request_basis={"id": req["id"], "revision": 1, "request_revision": 1, "response_ids": []}
            ),
        )
    assert (await service.get(actor=ALICE, request_id=req["id"]))["state"] == "pending"
    assert len((await service.history(actor=ALICE, request_id=req["id"]))["events"]) == 1
