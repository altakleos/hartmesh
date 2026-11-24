"""An employee's request changes its adopted definition only after a human decision."""

import asyncio
from dataclasses import replace

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_instances import create, definition
from test_agent_instances import instances as instances
from test_agent_lifecycle import lifecycle_fixture
from test_resident_registry import registry as registry
from test_resident_registry import running

from app.gateway.routers import agent_instances
from deerflow.agent_instances.contract import AgentConflict, AgentDenied, DefinitionSnapshot
from deerflow.agent_instances.proposals import ConfigurationProposals, candidate_for
from deerflow.persistence.agent_instances.proposals import AgentProposalRow


async def source(actor, name):
    return definition(actor.subject_id, name=name, soul="A later edit")


async def setup(instances, **extras):
    agents, authority, sf, lifecycle, attachments, provider, _memory = await lifecycle_fixture(instances)
    async with sf.kw["bind"].begin() as c:
        await c.run_sync(AgentProposalRow.__table__.create)
    employee = await create(agents, custody="company", supervisor=BOB, **extras)
    chat = await authority.create(actor=BOB, instance_id=employee.id)

    async def account(session):
        await agents._human(BOB, session=session)

    execution = replace(await authority.execution(actor=BOB, thread_id=chat["thread_id"]), proposal_submission=True, proposal_account=account)
    return ConfigurationProposals(agents), execution, lifecycle, attachments, provider


@pytest.mark.parametrize("policy", [{}, {"work_policy": None}, {"work_policy": {"responsibilities": [], "allow_derived": True}}])
def test_candidate_preserves_document_and_namespace(policy):
    config = {"name": "analyst", "skills": [], "model": None, **policy}
    snapshot = DefinitionSnapshot.capture(owner_id="alice", config=config, soul="Retain\nexactly\n")
    updated = candidate_for(snapshot, True)
    assert updated.config == {**config, "work_policy": {**(policy.get("work_policy") or {}), "enabled": True}}
    assert updated.soul == snapshot.soul and updated.owner_id == "alice"
    with pytest.raises(AgentConflict):
        candidate_for(updated, True)


@pytest.mark.asyncio
async def test_same_issuance_is_idempotent_and_its_manager_adopts_exact_snapshot(instances):
    service, execution, *_ = await setup(instances)
    scope = dict(run_id=operation(), call_id="one-call", issuer_id="message")
    receipt, duplicate = await asyncio.gather(*(service.submit(execution, proposed_value=True, **scope) for _ in range(2)))
    assert receipt == duplicate
    with pytest.raises(AgentConflict):
        await service.submit(execution, proposed_value=False, **scope)
    employee_id, proposal_id = execution.instance.id, receipt["id"]
    detail = await service.detail(ALICE, employee_id, proposal_id, source)
    assert detail["status"] == "open" and detail["current_value"] is False and detail["effective_work_policy"]["review_required"]
    assert detail["adopt_blocked_reason"] is None
    basis = {key: detail[key] for key in ("base_generation", "base_revision", "candidate_digest")}
    assert (await service.detail(BOB, employee_id, proposal_id, source))["adopt_blocked_reason"] == "different-definition-owner"
    with pytest.raises(AgentDenied):
        await service.adopt(BOB, employee_id, proposal_id, basis, source)
    decision = await service.adopt(ALICE, employee_id, proposal_id, basis, source)
    assert decision["status"] == "adopted" and decision["resolved_by"] == "alice"
    assert decision["current_revision"] == detail["candidate_digest"] and decision["base_value"] is False and decision["current_value"] is True
    assert (await service.agents.definition(actor=ALICE, instance_id=employee_id)).soul == execution.definition.soul
    assert await service.adopt(ALICE, employee_id, proposal_id, basis, source) == decision


async def submitted(service, execution):
    receipt = await service.submit(execution, proposed_value=True, run_id=operation(), issuer_id=operation(), call_id=operation())
    proj = await service.detail(ALICE, execution.instance.id, receipt["id"], source)
    return proj["id"], {key: proj[key] for key in ("base_generation", "base_revision", "candidate_digest")}


@pytest.mark.asyncio
async def test_two_accepts_racing_decline_have_one_terminal_decision(instances):
    service, execution, *_ = await setup(instances)
    employee = execution.instance
    proposal_id, basis = await submitted(service, execution)
    results = await asyncio.gather(
        service.adopt(ALICE, employee.id, proposal_id, basis, source),
        service.adopt(ALICE, employee.id, proposal_id, basis, source),
        service.decline(BOB, employee.id, proposal_id, source),
        return_exceptions=True,
    )
    assert all(isinstance(item, (dict, AgentConflict)) for item in results), results
    detail = await service.detail(ALICE, employee.id, proposal_id, source)
    assert detail["status"] in ("declined", "adopted")
    assert detail["current_value"] == (detail["status"] == "adopted")
    assert detail["current_generation"] == employee.generation + int(detail["status"] == "adopted")


@pytest.mark.asyncio
async def test_outdated_remains_declinable_after_ordinary_lifecycle(instances):
    service, execution, lifecycle, *_ = await setup(instances)
    employee = execution.instance
    proposal_id, basis = await submitted(service, execution)
    await lifecycle.change(actor=ALICE, instance_id=employee.id, expected_generation=employee.generation, operation_id=operation(), action="adopt", definition=definition(soul="Updated independently"))
    old = await service.detail(ALICE, employee.id, proposal_id, source)
    assert old["stale"] and old["adopt_blocked_reason"] == "stale"
    with pytest.raises(AgentConflict):
        await service.adopt(ALICE, employee.id, proposal_id, basis, source)
    assert (await service.decline(BOB, employee.id, proposal_id, source))["status"] == "declined"


@pytest.mark.asyncio
@pytest.mark.parametrize("resolve", ["retry", "abandon"])
async def test_unknown_containment_joins_only_committed_operation_and_delegate_can_resolve(instances, resolve):
    from deerflow.spaces.attachments import ResourceMount
    from deerflow.spaces.contract import Permission

    service, execution, lifecycle, attachments, provider = await setup(instances)
    employee = execution.instance
    proposal_id, basis = await submitted(service, execution)
    registry = service.agents.files.registry
    home = await registry.get(actor=ALICE, space_id=employee.home_id)
    await registry.set_grant(actor=ALICE, space_id=home.id, expected_generation=home.generation, subject=BOB, permissions=Permission.READ | Permission.WRITE | Permission.ADMIN, acknowledge_existing_data=True)
    home = await registry.get(actor=employee.principal, space_id=home.id)
    writer = await attachments.attach(actor=employee.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    provider.uncertain = True
    pending = await service.adopt(ALICE, employee.id, proposal_id, basis, source)
    assert pending["status"] == "applying" and pending["decided_by"] == "alice" and pending["resolved_by"] is None
    provider.uncertain = False
    if resolve == "retry":
        result = await lifecycle.change(actor=BOB, instance_id=employee.id, expected_generation=basis["base_generation"], operation_id=pending["operation_id"], action="adopt", definition=candidate_for(execution.definition, True))
        assert result["complete"]
    else:
        await attachments.retire(reason="lifecycle", actor=BOB, space_id=home.id, expected_generation=home.generation, attachment_ids=[writer.id])
        result = await lifecycle.abandon(actor=BOB, instance_id=employee.id, expected_generation=pending["current_generation"], operation_id=pending["operation_id"])
        assert result["abandoned"]
    terminal = await service.detail(ALICE, employee.id, proposal_id, source)
    assert terminal["status"] == ("adopted" if resolve == "retry" else "abandoned")
    assert terminal["decided_by"] == "alice" and terminal["resolved_by"] == "bob"


@pytest.mark.asyncio
async def test_source_vanished_and_busy_refusals_leave_open_and_unmodified(instances, monkeypatch):
    from types import SimpleNamespace

    from deerflow.agent_instances.contract import Refusal

    service, execution, *_ = await setup(instances)
    proposal_id, basis = await submitted(service, execution)
    employee = execution.instance

    async def missing(*args):
        raise FileNotFoundError("Source was removed")

    assert (await service.detail(ALICE, employee.id, proposal_id, missing))["adopt_blocked_reason"] == "definition-unavailable"
    with pytest.raises(FileNotFoundError):
        await service.adopt(ALICE, employee.id, proposal_id, basis, missing)
    monkeypatch.setattr(service.agents, "residents", SimpleNamespace(in_flight=lambda _: ["existing-run"], entry=lambda _: SimpleNamespace(lock=asyncio.Lock())))
    with pytest.raises(Refusal, match="working"):
        await service.adopt(ALICE, employee.id, proposal_id, basis, source)
    refreshed = await service.detail(ALICE, employee.id, proposal_id, source)
    assert refreshed["status"] == "open" and refreshed["operation_id"] is None
    assert refreshed["current_revision"] == execution.definition.revision


@pytest.mark.asyncio
async def test_revocation_after_withdrawal_requires_a_new_authorized_resolver(instances, monkeypatch):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentLifecycleRow

    service, execution, _, attachments, _ = await setup(instances)
    proposal_id, basis = await submitted(service, execution)
    employee = execution.instance
    retire = attachments.retire

    async def revoke_after_retire(**kwargs):
        await retire(**kwargs)
        async with service.agents._sf() as session, session.begin():
            (await session.get(AgentInstanceGrantRow, (employee.id, "alice"))).permissions = 4

    monkeypatch.setattr(attachments, "retire", revoke_after_retire)
    with pytest.raises(AgentDenied):
        await service.adopt(ALICE, employee.id, proposal_id, basis, source)
    denied_replay = await service.detail(BOB, employee.id, proposal_id, source)
    assert denied_replay["status"] == "applying"
    async with service.agents._sf() as session:
        intent = await session.get(AgentLifecycleRow, (employee.id, denied_replay["operation_id"]))
        assert intent.phase == "pending" and intent.resolved_by is None
    assert (await service.adopt(BOB, employee.id, proposal_id, basis, source))["resolved_by"] == "bob"


@pytest.mark.asyncio
async def test_tool_receipt_rechecks_current_binding_and_manager_reads_omit_source(instances):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.thread_meta.model import ThreadMetaRow

    service, execution, *_ = await setup(instances)
    scope = dict(run_id=operation(), call_id="one", issuer_id="model-message", proposed_value=True)
    receipt = await service.submit(execution, **scope)
    for disallowed in (execution.for_helper(), replace(execution, execution_allowed=False), replace(execution, work=object()), replace(execution, proposal_account=None)):
        with pytest.raises(AgentDenied):
            await service.submit(disallowed, **scope)
    async with service.agents._sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (execution.instance.id, "bob"))).permissions = 2
    with pytest.raises(AgentDenied):
        await service.submit(execution, **scope)
    for permission in (1, 2, 4):
        async with service.agents._sf() as session, session.begin():
            (await session.get(AgentInstanceGrantRow, (execution.instance.id, "bob"))).permissions = permission
        with pytest.raises(AgentDenied):
            await service.detail(BOB, execution.instance.id, receipt["id"], source)
    async with service.agents._sf() as session, session.begin():
        await session.delete(await session.get(ThreadMetaRow, execution.thread_id))
    projection = await service.list(ALICE, execution.instance.id, source)
    assert [entry["id"] for entry in projection["proposals"]] == [receipt["id"]]
    assert {"run_id", "thread_id", "requester_id", "thread_incarnation", "issuer_id", "call_id"}.isdisjoint(projection["proposals"][0])


@pytest.mark.asyncio
async def test_http_permission_scope_and_exact_request_body(instances, monkeypatch):
    from types import SimpleNamespace

    import httpx
    from test_agent_instances import request_app

    from app.gateway import deps
    from deerflow.agent_instances import proposal_tools

    service, execution, *_ = await setup(instances)
    proposal_id, basis = await submitted(service, execution)
    url = f"/api/agent-instances/{execution.instance.id}/proposals"
    monkeypatch.setattr(agent_instances, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    monkeypatch.setattr(proposal_tools, "installed_forms", lambda loaded: [object()])

    async def current(request, *, session=None):
        return request.state.user

    monkeypatch.setattr(deps, "resolve_session_cookie", current)

    async def load(request, actor, name):
        return await source(actor, name)

    monkeypatch.setattr(agent_instances, "_load_definition", load)

    async def request(method, path, *, permissions=("agents:read", "agents:write"), credential="session", body=None):
        app = request_app(service.agents, permissions=permissions, source=credential)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            return await client.request(method, url + path, json=body)

    for credential in ("internal", "pat", "auth-disabled"):
        assert (await request("GET", "", credential=credential)).status_code in (401, 403)
    assert (await request("GET", "", permissions=("agents:write",))).status_code == 403
    assert (await request("GET", "", permissions=("agents:read",))).json()["proposals"][0]["id"] == proposal_id
    assert (await request("POST", f"/{proposal_id}/decline", body={}, permissions=("agents:read",))).status_code == 403
    assert (await request("POST", f"/{proposal_id}/adopt", body={**basis, "owner_id": "alice"})).status_code == 422
    assert (await request("POST", f"/{proposal_id}/adopt", body={**basis, "candidate_digest": "0" * 64})).status_code == 409
    response = await request("POST", f"/{proposal_id}/adopt", body=basis)
    assert response.status_code == 200 and response.json()["status"] == "adopted"


@pytest.mark.asyncio
async def test_enable_disable_and_retained_work_under_original_revision(instances):
    from sqlalchemy import func, select

    from deerflow.agent_instances.work import AgentWork
    from deerflow.agent_instances.work_contract import DelegateWork
    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputReadRow, HumanInputRequestRow, HumanInputResponseRow
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow

    service, execution, *_ = await setup(instances)
    sf = service.agents._sf
    async with sf.kw["bind"].begin() as conn:
        for model in (AgentWorkRow, AgentWorkAttemptRow, AgentWorkEventRow, HumanInputRequestRow, HumanInputResponseRow, HumanInputEventRow, HumanInputReadRow):
            await conn.run_sync(model.__table__.create)
    employee = execution.instance
    sibling = await create(service.agents, custody="company", snapshot=execution.definition)

    async def delegate():
        return await AgentWork(service.agents).delegate(actor=ALICE, instance_id=employee.id, request=DelegateWork(operation_id=operation(), objective="Review", success_criteria="Report gaps"))

    with pytest.raises(AgentConflict):
        await delegate()
    proposal_id, basis = await submitted(service, execution)
    await service.adopt(ALICE, employee.id, proposal_id, basis, source)
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(AgentWorkRow)) == 0
    created = await delegate()
    fresh = replace(await execution.authority.execution(actor=BOB, thread_id=execution.thread_id), proposal_submission=True, proposal_account=execution.proposal_account)
    off = await service.submit(fresh, run_id=operation(), issuer_id=operation(), call_id=operation(), proposed_value=False)
    detail = await service.detail(ALICE, employee.id, off["id"], source)
    await service.adopt(ALICE, employee.id, off["id"], {key: detail[key] for key in basis}, source)
    with pytest.raises(AgentConflict):
        await delegate()
    async with sf() as session:
        assert (await session.get(AgentWorkRow, created["id"])).definition_revision == basis["candidate_digest"]
    assert (await service.agents.definition(actor=ALICE, instance_id=sibling.id)) == execution.definition


@pytest.mark.asyncio
async def test_review_rechecks_grants_after_the_deployment_readiness_probe(instances, monkeypatch):
    from types import SimpleNamespace

    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    service, execution, *_ = await setup(instances)
    proposal_id, _ = await submitted(service, execution)

    async def probe(instance_id):
        async with service.agents._sf() as session, session.begin():
            (await session.get(AgentInstanceGrantRow, (instance_id, "alice"))).permissions = 4
        return SimpleNamespace(execution="ready")

    monkeypatch.setattr(service.agents, "execution_probe", probe)
    with pytest.raises(AgentDenied):
        await service.detail(ALICE, execution.instance.id, proposal_id, source)


@pytest.mark.asyncio
async def test_paginated_review_and_fresh_adoption_require_the_current_home_admin(instances):
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.spaces.contract import Permission, SpaceNotFound

    service, execution, *_ = await setup(instances)
    first, basis = await submitted(service, execution)
    second, _ = await submitted(service, execution)
    employee = execution.instance
    async with service.agents._sf() as session, session.begin():
        (await session.get(SpaceGrantRow, (employee.home_id, "human", "alice"))).permissions = int(Permission.READ)
    page = await service.list(ALICE, employee.id, source, limit=1)
    assert [p["id"] for p in page["proposals"]] == [first]
    assert page["proposals"][0]["adopt_blocked_reason"] == "home-admin-required"
    following = await service.list(ALICE, employee.id, source, cursor=page["next_cursor"], limit=1)
    assert [p["id"] for p in following["proposals"]] == [second] and following["next_cursor"] is None
    with pytest.raises((AgentDenied, SpaceNotFound, PermissionError)):
        await service.adopt(ALICE, employee.id, first, basis, source)
    assert (await service.decline(ALICE, employee.id, first, source))["status"] == "declined"


@pytest.mark.asyncio
async def test_run_registration_owns_home_until_idle_only_withdrawal_can_check(instances, registry):
    from app.gateway.agent_conversations import resident_run_admission
    from deerflow.agent_instances.contract import Refusal

    service, execution, *_ = await setup(instances)
    proposal_id, basis = await submitted(service, execution)
    residents, _, records, _ = registry()
    service.agents.residents = residents
    registering, competing, registered = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class ObservedLock(asyncio.Lock):
        async def acquire(self):
            if self.locked():
                competing.set()
            return await super().acquire()

    residents.entry(execution.instance).lock = ObservedLock()

    async def register_run():
        async with resident_run_admission(execution, residents.run_manager, idempotency_key=None):
            await execution.authority.admit(execution)
            registering.set()
            await competing.wait()
            running(records, "real-registration", execution.instance)
        registered.set()

    async def capture_source(actor, name):
        # Without the exclusion this callback runs after the idle sample and
        # before the generation write, allowing the admitted run to register.
        competing.set()
        await registered.wait()
        return await source(actor, name)

    async with asyncio.timeout(15):
        task = asyncio.create_task(register_run())
        try:
            await registering.wait()
            with pytest.raises(Refusal) as refused:
                await service.adopt(ALICE, execution.instance.id, proposal_id, basis, capture_source)
            assert refused.value.extra["run_ids"] == ["real-registration"]
        finally:
            competing.set()
            await task
    detail = await service.detail(ALICE, execution.instance.id, proposal_id, source)
    assert detail["status"] == "open" and detail["current_generation"] == basis["base_generation"]


@pytest.mark.asyncio
async def test_cookie_decision_and_issued_form_recheck_real_accounts(instances, monkeypatch):
    from types import SimpleNamespace

    import httpx
    from test_agent_instances import request_app

    from app.gateway import deps
    from app.gateway.auth import create_access_token
    from app.gateway.auth.models import User
    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
    from app.gateway.company_connection_runs import proposal_account_admission
    from app.gateway.storage_spaces import human_lookup
    from deerflow.agent_instances import proposal_tools
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.user.access import DisabledIdentityRow
    from deerflow.persistence.user.model import UserRow
    from deerflow.spaces.contract import PrincipalRef

    agents, authority, sf, *_ = await lifecycle_fixture(instances)
    async with sf.kw["bind"].begin() as connection:
        for model in (AgentProposalRow, UserRow, DisabledIdentityRow):
            await connection.run_sync(lambda c: model.__table__.create(c, checkfirst=True))
    accounts = SQLiteUserRepository(sf)
    manager = await accounts.create_user(User(email="manager@example.com", system_role="admin"))
    employee_user = await accounts.create_user(User(email="author@example.com"))
    owner, requester = (PrincipalRef("human", str(user.id)) for user in (manager, employee_user))
    monkeypatch.setattr(deps, "get_user_repo", lambda: accounts)
    monkeypatch.setattr(agents.directory, "human", human_lookup(accounts))
    monkeypatch.setattr(agents.files.registry._resolver, "_human", agents.directory.human)
    monkeypatch.setattr("deerflow.config.agents_api_config.get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    monkeypatch.setattr(agent_instances, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    monkeypatch.setattr(proposal_tools, "installed_forms", lambda loaded: [object()])

    async def load(request, actor, name):
        return await source(actor, name)

    monkeypatch.setattr(agent_instances, "_load_definition", load)
    employee = await create(agents, actor=owner, snapshot=definition(owner.subject_id), custody="company", supervisor=requester)
    chat = await authority.create(actor=requester, instance_id=employee.id)
    execution = replace(await authority.execution(actor=requester, thread_id=chat["thread_id"]), proposal_submission=True, proposal_account=proposal_account_admission(employee_user))
    service = ConfigurationProposals(agents)
    now = dict(run_id=operation(), issuer_id="model-task", call_id="call", proposed_value=True)
    receipt = await service.submit(execution, **now)
    async with sf() as session, session.begin():
        (await session.get(UserRow, requester.subject_id)).token_version += 1
    with pytest.raises(AgentDenied):
        await service.submit(execution, **now)
    updated = await accounts.get_user_by_id(requester.subject_id)
    execution = replace(execution, proposal_account=proposal_account_admission(updated))
    assert await service.submit(execution, **now) == receipt
    async with sf() as session, session.begin():
        from datetime import UTC, datetime

        session.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=requester.subject_id, disabled_at=datetime.now(UTC)))
    with pytest.raises(AgentDenied):
        await service.submit(execution, **now)

    app = request_app(agents, actor=owner)
    url = f"/api/agent-instances/{employee.id}/proposals/{receipt['id']}"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": create_access_token(owner.subject_id)}) as client:
        detail = await client.get(url)
        assert detail.status_code == 200, detail.text
        basis = {key: detail.json()[key] for key in ("base_revision", "base_generation", "candidate_digest")}
        accepted = await client.post(url + "/adopt", json=basis)
        assert accepted.status_code == 200 and accepted.json()["status"] == "adopted"
        async with sf() as session, session.begin():
            (await session.get(UserRow, owner.subject_id)).token_version += 1
        assert (await client.get(url)).status_code == 401
        assert (await client.post(url + "/adopt", json=basis)).status_code == 401


def test_only_a_direct_session_run_can_request_the_form():
    from types import SimpleNamespace

    from starlette.requests import Request

    from app.gateway.company_connection_runs import interactive_proposal_turn
    from app.gateway.routers.thread_runs import router
    from app.gateway.run_models import RunCreateRequest

    route = next(route for route in router.routes if route.name == "stream_run")
    request = Request({"type": "http", "route": route})
    request.state.auth_source = "session"
    msg = {"messages": [{"type": "human", "content": "Review tracked assignments"}]}
    assert interactive_proposal_turn(request, RunCreateRequest(input=msg))
    for body in (
        SimpleNamespace(input=msg, command={"resume": True}),
        SimpleNamespace(input=msg, checkpoint_id="prior"),
        SimpleNamespace(input={"messages": [{"role": "assistant", "content": "resume"}]}),
        SimpleNamespace(input={"messages": [{"type": "human", "additional_kwargs": {"hide_from_ui": True}}]}),
    ):
        assert not interactive_proposal_turn(request, body)
    request.state.admitted_queued_human = True
    assert not interactive_proposal_turn(request, SimpleNamespace(input=msg))
    request.state.admitted_queued_human = False
    for credential in ("internal", "pat", "auth_disabled"):
        request.state.auth_source = credential
        assert not interactive_proposal_turn(request, SimpleNamespace(input=msg))
