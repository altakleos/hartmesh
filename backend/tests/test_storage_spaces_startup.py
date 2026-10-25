"""No filesystem fallback or unvalidated human projection at startup."""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from test_agent_instances import instances as instances

from deerflow.spaces.contract import PrincipalRef


@asynccontextmanager
async def _startup_app_lifetime(ss):
    # Independent startups have independent cleanup owners, even when sharing
    # the fixture database. Reusing a closed app skips the next generation's
    # drain and can race its maintenance against SQL schema teardown.
    app = SimpleNamespace(state=SimpleNamespace(thread_store=SimpleNamespace(instance_authority=None)))
    async with ss.storage_spaces_lifecycle(app):
        yield app


@pytest.mark.asyncio
@pytest.mark.parametrize("with_home", [False, True])
@pytest.mark.parametrize("custody", ["personal", "company"])
async def test_unfinished_expiry_keeps_younger_intent_and_releases_the_old_home_in_the_same_pass(startup, monkeypatch, caplog, with_home, custody):
    from sqlalchemy import select
    from test_agent_instances import pending_creation
    from test_compose_user_reclamation import ContainmentFixture

    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.spaces.files import SpaceBackingRow

    ss, app, config, sf, catalog, _adapter, _agents = startup
    monkeypatch.setattr(ss, "RECHECK_SECONDS", 3600)
    caplog.set_level(logging.INFO, logger=ss.__name__)
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    await ss._maintenance_step(app, timeout=30)
    app.state.agent_execution.qualified_provider = ContainmentFixture(sf)
    agents = app.state.agent_instances
    old = await pending_creation(agents, with_home=with_home, custody=custody)
    young = await pending_creation(agents, custody="company" if custody == "personal" else "personal")
    slot = None
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceRow, old.id)).created_at = datetime.now(UTC) - timedelta(hours=25)
        if with_home:
            slot = (await session.execute(select(SpaceBackingRow.slot_id).where(SpaceBackingRow.space_id == old.home_id))).scalar_one()
    await ss._maintenance_step(app, timeout=30)
    async with sf() as session:
        row = await session.get(AgentInstanceRow, old.id)
        if with_home:
            assert row.status == "deleted"
        else:
            assert row is None
        assert (await session.get(AgentInstanceRow, young.id)).status == "provisioning"
        if with_home:
            assert (await session.execute(select(SpaceBackingRow).where(SpaceBackingRow.space_id == old.home_id))).scalar_one_or_none() is None
    kind = "assistant" if custody == "personal" else "employee"
    assert f"agents: unfinished {kind} {old.id} removed after 24 hours" in caplog.text
    if with_home:
        assert slot not in agents.files.bound_slots[kind] and slot not in agents.files.unclaimed_slots[kind]
        replacement = await pending_creation(agents, with_home=True, custody=custody)
        async with sf() as session:
            assert (await session.execute(select(SpaceBackingRow.slot_id).where(SpaceBackingRow.space_id == replacement.home_id))).scalar_one() == slot


@pytest.mark.asyncio
@pytest.mark.parametrize("first_fails", [False, True])
async def test_expiry_runs_without_an_adapter_and_continues_after_one_row_fails(startup, monkeypatch, caplog, first_fails):
    from sqlalchemy import update
    from test_agent_instances import pending_creation

    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.spaces.contract import SpaceConflict

    ss, app, config, sf, _catalog, _adapter, _agents = startup
    monkeypatch.setattr(ss, "RECHECK_SECONDS", 3600)
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    await ss._maintenance_step(app, timeout=30)
    agents = app.state.agent_instances
    pending = [await pending_creation(agents, custody=custody) for custody in ("personal", "company")]
    pending.sort(key=lambda row: row.id)
    async with sf() as session, session.begin():
        await session.execute(update(AgentInstanceRow).values(created_at=datetime.now(UTC) - timedelta(hours=25)))
    original = agents.abandon_creation

    async def abandon(instance_id):
        if first_fails and instance_id == pending[0].id:
            raise SpaceConflict("private content must not reach the journal")
        return await original(instance_id)

    monkeypatch.setattr(agents, "abandon_creation", abandon)
    app.state.agent_execution.qualified_provider = None
    app.state.agent_execution.storage_reason = "admission-failed"
    await ss._maintenance_step(app, timeout=30)
    async with sf() as session:
        assert (await session.get(AgentInstanceRow, pending[0].id) is not None) is first_fails
        assert await session.get(AgentInstanceRow, pending[1].id) is None
    assert "private content must not reach the journal" not in caplog.text


@pytest.mark.asyncio
async def test_unfinished_scan_failure_logs_only_its_class_and_continues_release(monkeypatch, caplog):
    from app.gateway import storage_spaces as ss

    def failed_session():
        raise RuntimeError("private SQL parameters and tenant content")

    files = SimpleNamespace(settle_interrupted=AsyncMock(return_value=0))
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=files, agent_instances=SimpleNamespace(_sf=failed_session), agent_execution=ss.ExecutionStatus(agents_ready_reported=True)))
    monkeypatch.setattr(ss, "_resident_step", AsyncMock())
    monkeypatch.setattr(ss, "storage_state", AsyncMock(return_value=ss.StorageState("ready")))
    release = AsyncMock(return_value=1)
    monkeypatch.setattr(ss, "finish_releases", release)
    monkeypatch.setattr(ss, "assign_spaces", AsyncMock())
    assert await ss._maintenance_step(app, timeout=5)
    release.assert_awaited_once_with(app)
    assert app.state.agent_execution.spaces_reclaimed == 1
    assert "unfinished creation cleanup remains pending" in caplog.text and "RuntimeError" in caplog.text
    assert "private SQL parameters and tenant content" not in caplog.text


@pytest_asyncio.fixture
async def startup(instances, monkeypatch, tmp_path):
    from app.gateway import storage_spaces as ss
    from deerflow.config.agents_api_config import AgentsApiConfig
    from deerflow.config.storage_spaces_config import StoragePlan, StorageSpacesConfig
    from deerflow.features.resources import StorageFeatureLinkRow
    from deerflow.persistence.user.model import UserRow
    from deerflow.spaces.provision import KINDS, StorageFit, plan_counts

    agents, files, sf, catalog = instances
    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(UserRow.__table__.create)
        await connection.run_sync(StorageFeatureLinkRow.__table__.create)
    async with sf() as session, session.begin():
        session.add_all([UserRow(id="alice", email="alice@example.invalid", system_role="admin"), UserRow(id="bob", email="bob@example.invalid")])
    config = StorageSpacesConfig(
        enabled=True, inventory_path=str(tmp_path / "inventory.v2.json"), user_spaces=False, plan=StoragePlan(users=1, user_gib=1, ai_assistants_per_user=1, ai_assistant_gib=1, ai_employees=1, ai_employee_gib=1, company_gib=1)
    )
    catalog.data_disk = tmp_path
    for spec, kind in zip(catalog.volumes.values(), KINDS, strict=True):
        spec.kind = kind
    monkeypatch.setattr(ss.PreparedVolumeCatalog, "from_manifest", lambda path: catalog)
    monkeypatch.setattr(ss, "human_lookup", lambda _repository: files.registry._resolver._human)
    monkeypatch.setattr(ss, "get_agents_api_config", lambda: AgentsApiConfig(enabled=True, ai_assistants_per_user=1, ai_employees=1))

    def fit(pool, plan, inventory):
        required = plan_counts(plan)
        present = {kind: sum(getattr(spec, "kind", None) == kind for spec in inventory.volumes.values()) for kind in KINDS}
        missing = {kind: max(0, required[kind] - present[kind]) for kind in KINDS}
        return StorageFit(required, present, missing, sum(missing.values()) << 30, 50 << 30, 40 << 30, 100000, 5 << 30, True)

    monkeypatch.setattr(ss, "storage_fit", fit)
    adapter = AsyncMock(return_value=(None, SimpleNamespace(provider=SimpleNamespace(host_id="qualified-test-host"))))
    monkeypatch.setattr(ss, "qualified_attachments", adapter)
    async with _startup_app_lifetime(ss) as app:
        yield ss, app, config, sf, catalog, adapter, agents


@pytest.mark.asyncio
async def test_startup_tolerates_missing_or_unreadable_inventory(startup, monkeypatch):
    ss, _app, config, sf, _, adapter, _ = startup
    for error, reason in [(FileNotFoundError("missing"), "storage-pool-missing"), (ValueError("malformed"), "inventory-unreadable")]:

        def missing(_path):
            raise error

        monkeypatch.setattr(ss.PreparedVolumeCatalog, "from_manifest", missing)
        async with _startup_app_lifetime(ss) as app:
            await ss.initialize_storage_spaces(app, config, session_factory=sf)
            state = await ss.storage_state(app)
            assert (state.state, state.reason) == ("unavailable", reason)
            assert app.state.storage_spaces is not None and not app.state.storage_spaces.catalog.volumes
            assert app.state.agent_instances is not None
            assert app.state.thread_store.instance_authority is app.state.agent_conversations
            assert (await ss.execution_state(app)).reason == "storage-unavailable"
            recheck = app.state.storage_recheck_task
        assert recheck.done()
    assert not adapter.await_count


@pytest.mark.asyncio
async def test_startup_skips_bad_slot_and_keeps_pinned_inventory(startup, monkeypatch, caplog):
    from deerflow.spaces.backings import BackingUnavailable

    ss, app, config, sf, catalog, _, _ = startup
    original = catalog.verify
    bad_slot = next(iter(catalog.volumes))

    def verify(slot, **kwargs):
        if slot == bad_slot:
            raise BackingUnavailable("fixture mount is missing")
        return original(slot, **kwargs)

    monkeypatch.setattr(catalog, "verify", verify)
    caplog.set_level(logging.INFO, logger=ss.__name__)
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    assert (await ss.storage_state(app)).state == "ready"
    assert app.state.agent_execution.unverified == {bad_slot: "fixture mount is missing"}
    assert app.state.storage_spaces.catalog is catalog
    assert bad_slot in app.state.storage_spaces.unclaimed_slots[catalog.volumes[bad_slot].kind]
    assert any("unverified" in r.message and bad_slot in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_incomplete_and_empty_nonfitting_plans_have_distinct_states(startup, monkeypatch, caplog):
    from dataclasses import replace

    ss, app, config, sf, catalog, _, _ = startup
    catalog.volumes.pop(next(iter(catalog.volumes)))
    caplog.set_level(logging.INFO, logger=ss.__name__)
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    assert (await ss.storage_state(app)).state == "incomplete"
    assert any("storage: ready, 1 space short" in r.message for r in caplog.records)
    await ss.close_storage_spaces(app)
    fit = ss.storage_fit
    monkeypatch.setattr(ss, "storage_fit", lambda *a: replace(fit(*a), fits=False, free_bytes=1 << 30))
    catalog.volumes.clear()
    async with _startup_app_lifetime(ss) as app:
        await ss.initialize_storage_spaces(app, config, session_factory=sf)
        assert (await ss.storage_state(app)).reason == "disk-too-small"
        recheck = app.state.storage_recheck_task
    assert recheck.done()


@pytest.mark.asyncio
async def test_admission_failure_recovers_without_a_second_pending_adapter(startup, monkeypatch):
    from deerflow.spaces.docker import StorageAdapterUnsupported

    ss, app, config, sf, _, adapter, _ = startup
    gate = asyncio.Event()
    calls = []

    async def slow(files):
        calls.append(files)
        await gate.wait()
        return adapter.return_value

    monkeypatch.setattr(ss, "qualified_attachments", slow)
    monkeypatch.setattr(ss, "ADAPTER_LIMIT_SECONDS", 0.01)
    monkeypatch.setattr(ss, "RECHECK_SECONDS", 3600)
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    task = app.state.agent_execution.adapter_task
    assert task is not None and not task.done() and not task.cancelled()
    assert (await ss.storage_state(app)).reason == "admission-failed"
    await ss.storage_recheck(app)
    assert len(calls) == 1
    gate.set()
    await task
    await ss.storage_recheck(app)
    assert (await ss.storage_state(app)).state == "ready"
    assert app.state.agent_execution.host_id == "qualified-test-host"
    assert app.state.agent_execution.adapter_task is None and len(calls) == 1
    await ss.close_storage_spaces(app)
    adapter.side_effect = StorageAdapterUnsupported("fixture unavailable daemon")
    monkeypatch.setattr(ss, "qualified_attachments", adapter)
    async with _startup_app_lifetime(ss) as app:
        await ss.initialize_storage_spaces(app, config, session_factory=sf)
        assert (await ss.storage_state(app)).reason == "admission-failed"
        adapter.side_effect = None
        await ss.storage_recheck(app)
        assert (await ss.storage_state(app)).state == "ready"
        recheck = app.state.storage_recheck_task
    assert recheck.done(), "The second startup's maintenance escaped its cleanup owner"


@pytest.mark.asyncio
async def test_host_unqualified_is_not_retried_and_history_authority_survives(startup):
    from deerflow.spaces.docker import StorageHostUnqualified

    ss, app, config, sf, _, adapter, _ = startup
    adapter.side_effect = StorageHostUnqualified("fixture non-Docker provider")
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    assert (await ss.storage_state(app)).reason == "host-unqualified"
    await ss.storage_recheck(app)
    assert adapter.await_count == 1 and app.state.agent_instances is not None
    assert app.state.agent_instances.execution_probe is not None


@pytest.mark.asyncio
async def test_allowance_counts_seated_users_including_admin_and_preserves_released_rows(startup):
    from deerflow.persistence.user.model import UserRow

    ss, app, config, sf, _, _, _ = startup
    async with sf() as session, session.begin():
        session.add(UserRow(id="released", email="released@example.invalid", released_at=datetime.now(UTC)))
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    allowance = await ss.allowance(app, "user")
    assert (allowance.allowed, allowance.allowed_key, allowance.prepared, allowance.in_use, allowance.free) == (1, 1, 1, 2, -1)
    async with sf() as session:
        assert await session.get(UserRow, "released") is not None
    with pytest.raises(ValueError):
        await ss.allowance(app, "unknown")


@pytest.mark.asyncio
async def test_execution_precedence_and_structured_refusal(startup, monkeypatch):
    from deerflow.agent_instances.contract import ExecutionUnavailable
    from deerflow.config.agents_api_config import AgentsApiConfig

    ss, app, config, sf, _, _, _ = startup
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    monkeypatch.setattr(ss, "get_agents_api_config", lambda: AgentsApiConfig(enabled=True, ai_assistants_per_user=0, ai_employees=0))
    assert (await ss.execution_state(app)).reason == "agents-none"
    app.state.agent_execution.storage_reason = "admission-failed"
    assert (await ss.execution_state(app)).reason == "storage-unavailable"
    with pytest.raises(ExecutionUnavailable) as refusal:
        await ss.require_execution(app, None)
    assert refusal.value.reason == "execution-unavailable"
    assert refusal.value.extra == {"execution_reason": "storage-unavailable"}


@pytest.mark.asyncio
async def test_execution_checks_only_the_requested_home_and_ignores_a_freed_binding(startup):
    from _storage_spaces_test_support import ALICE
    from sqlalchemy import select
    from test_agent_instances import create

    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.spaces.files import SpaceBackingRow

    ss, app, config, sf, catalog, _, agents = startup
    for spec in catalog.volumes.values():
        spec.kind = None
    first, second = await create(agents), await create(agents)
    for spec in catalog.volumes.values():
        spec.kind = "assistant"
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    async with sf() as session:
        bad_slot = await session.scalar(select(SpaceBackingRow.slot_id).where(SpaceBackingRow.space_id == first.home_id))
    app.state.agent_execution.unverified[bad_slot] = "fixture unverified home"
    assert (await ss.execution_state(app, first.id)).reason == "space-unverified"
    assert (await ss.execution_state(app, second.id)).execution == "ready"
    assert (await ss.execution_state(app)).execution == "ready"
    assert len(await app.state.agent_instances.list(actor=ALICE)) == 2
    async with sf() as session, session.begin():
        binding = (await session.execute(select(SpaceBackingRow).where(SpaceBackingRow.space_id == first.home_id))).scalar_one()
        await session.delete(binding)
        row = await session.get(AgentInstanceRow, first.id)
        row.status = "deleted"
    assert (await ss.execution_state(app, first.id)).reason is None
    async with sf() as session, session.begin():
        row = await session.get(AgentInstanceRow, second.id)
        row.status, row.home_id = "provisioning", None
    assert (await ss.execution_state(app, second.id)).reason == "space-not-assigned"


@pytest.mark.parametrize("status", ["provisioning", "active", "suspended", "archived", "deleted"])
@pytest.mark.asyncio
async def test_allowance_counts_every_non_deleted_agent_status(startup, status):
    from test_agent_instances import create

    from deerflow.persistence.agent_instances.model import AgentInstanceRow

    ss, app, config, sf, catalog, _, agents = startup
    for spec in catalog.volumes.values():
        spec.kind = None
    agent = await create(agents, custody="company")
    for spec in catalog.volumes.values():
        spec.kind = "employee"
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceRow, agent.id)).status = status
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    count = await ss.allowance(app, "employee")
    assert count.prepared == 4 and count.in_use == (0 if status == "deleted" else 1)
    assert count.allowed == 1 and count.free == count.allowed - count.in_use


@pytest.mark.asyncio
async def test_inventory_worker_past_budget_is_owned_without_replacing_the_catalogue(startup, monkeypatch):
    import threading

    ss, app, config, sf, catalog, _, _ = startup
    gate = threading.Event()
    entered = threading.Event()
    original = ss._read_pool

    def slow(config):
        entered.set()
        gate.wait(timeout=10)
        return original(config)

    monkeypatch.setattr(ss, "_read_pool", slow)
    monkeypatch.setattr(ss, "STARTUP_BUDGET_SECONDS", 0.01)
    try:
        await ss.initialize_storage_spaces(app, config, session_factory=sf)
        assert entered.is_set()
        task = app.state.storage_inventory_task
        assert not task.done() and not task.cancelled()
        assert (await ss.storage_state(app)).reason == "inventory-unreadable"
        pinned = app.state.storage_spaces.catalog
        assert not pinned.volumes
        gate.set()
        await task
        assert app.state.storage_spaces.catalog is pinned and app.state.storage_spaces.catalog is not catalog
    finally:
        gate.set()


@pytest.mark.asyncio
async def test_repeated_verification_respects_remaining_startup_budget(startup, monkeypatch):
    import threading

    ss, app, config, sf, catalog, adapter, _ = startup
    entered, finish = threading.Event(), threading.Event()
    verify, calls = catalog.verify, {}

    def blocked(slot, **kwargs):
        calls[slot] = calls.get(slot, 0) + 1
        if calls[slot] == 2:
            entered.set()
            assert finish.wait(timeout=10)
        return verify(slot, **kwargs)

    monkeypatch.setattr(catalog, "verify", blocked)
    monkeypatch.setattr(ss, "STARTUP_BUDGET_SECONDS", 0.02)
    try:
        await ss.initialize_storage_spaces(app, config, session_factory=sf)
        for _ in range(200):
            if entered.is_set():
                break
            await asyncio.sleep(0.01)
        assert entered.is_set()
        assert app.state.agent_execution.metadata_task is not None
        assert (await ss.storage_state(app)).reason == "admission-failed"
        assert adapter.await_count == 0
        finish.set()
        await app.state.agent_execution.metadata_task
        await ss.storage_recheck(app)
        assert (await ss.storage_state(app)).state == "ready"
        assert adapter.await_count == 1
    finally:
        finish.set()


@pytest.mark.asyncio
async def test_bound_verification_timeout_leaves_database_writable(startup, monkeypatch):
    import threading

    from _storage_spaces_test_support import home

    from deerflow.persistence.user.model import UserRow

    ss, app, config, sf, catalog, _, agents = startup
    kinds = {slot: spec.kind for slot, spec in catalog.volumes.items()}
    for spec in catalog.volumes.values():
        spec.kind = None
    await home(agents.files)
    for slot, spec in catalog.volumes.items():
        spec.kind = kinds[slot]
    entered, finish = threading.Event(), threading.Event()
    verify, calls = catalog.verify, {}

    def blocked(slot, **kwargs):
        calls[slot] = calls.get(slot, 0) + 1
        if calls[slot] == 2:
            entered.set()
            assert finish.wait(timeout=10)
        return verify(slot, **kwargs)

    monkeypatch.setattr(catalog, "verify", blocked)
    monkeypatch.setattr(ss, "STARTUP_BUDGET_SECONDS", 0.05)
    try:
        await ss.initialize_storage_spaces(app, config, session_factory=sf)
        assert entered.is_set() and app.state.agent_execution.metadata_task is not None
        assert (await ss.storage_state(app)).reason == "admission-failed"
        async with asyncio.timeout(1):
            async with sf() as session, session.begin():
                session.add(UserRow(id="independent-writer", email="writer@example.invalid"))
                await session.flush()
        assert not app.state.agent_execution.metadata_task.done()
    finally:
        finish.set()


@pytest.mark.asyncio
async def test_oversized_plan_keeps_startup_and_existing_authority(startup, monkeypatch):
    from deerflow.spaces.provision import storage_fit

    ss, app, config, sf, _, _, _ = startup
    config.plan = config.plan.model_copy(update={"users": 100, "ai_assistants_per_user": 10})
    monkeypatch.setattr(ss, "storage_fit", storage_fit)

    def missing(_path):
        raise FileNotFoundError("fixture missing inventory")

    monkeypatch.setattr(ss.PreparedVolumeCatalog, "from_manifest", missing)
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    assert app.state.agent_execution.fit is None
    assert (await ss.storage_state(app)).reason == "storage-pool-missing"
    assert app.state.agent_instances is not None


@pytest.mark.asyncio
async def test_storage_lifecycle_cleans_up_on_later_startup_failure(startup):
    ss, app, config, sf, _, _, _ = startup
    with pytest.raises(RuntimeError, match="later startup failed"):
        async with ss.storage_spaces_lifecycle(app):
            await ss.initialize_storage_spaces(app, config, session_factory=sf)
            raise RuntimeError("later startup failed")
    assert app.state.storage_recheck_task.done()


@pytest.mark.asyncio
async def test_shutdown_drains_pending_qualification_before_service_teardown(startup, monkeypatch):
    ss, app, config, sf, _, adapter, _ = startup
    finish = asyncio.Event()

    async def pending(_files):
        await finish.wait()
        return adapter.return_value

    monkeypatch.setattr(ss, "qualified_attachments", pending)
    monkeypatch.setattr(ss, "ADAPTER_LIMIT_SECONDS", 0.01)
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    owned = app.state.agent_execution.adapter_task
    shutdown = asyncio.create_task(ss.close_storage_spaces(app))
    try:
        await asyncio.sleep(0.02)
        assert not shutdown.done() and not owned.cancelled()
        finish.set()
        await shutdown
        assert owned.done() and app.state.storage_recheck_task.done()
    finally:
        finish.set()
        await shutdown


@pytest.mark.asyncio
async def test_application_refusal_handler_preserves_fields_without_router_interception():
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    from app.gateway.refusals import refusal_handler
    from app.gateway.routers.agent_instances import InstanceRoute
    from deerflow.agent_instances.contract import ExecutionUnavailable, Refusal

    app = FastAPI()
    app.add_exception_handler(Refusal, refusal_handler)
    app.router.route_class = InstanceRoute

    @app.get("/refusal")
    async def refused():
        raise ExecutionUnavailable("space-unverified")

    async with AsyncClient(transport=ASGITransport(app), base_url="http://test") as client:
        response = await client.get("/refusal")
    assert response.status_code == 409
    assert response.json() == {"detail": "AI assistants and AI employees cannot run on this deployment right now.", "reason": "execution-unavailable", "execution_reason": "space-unverified"}


@pytest.mark.asyncio
async def test_public_health_reports_words_only_and_storage_failure_precedes_agent_counts(startup, monkeypatch):
    from app.gateway import app as app_module
    from app.gateway.auth import mode
    from deerflow.config.agents_api_config import AgentsApiConfig

    ss, state_app, config, sf, _, _, _ = startup
    await ss.initialize_storage_spaces(state_app, config, session_factory=sf)
    monkeypatch.setattr(mode, "auth_mode", lambda: "local")
    monkeypatch.setattr(mode, "registration_state", lambda: "closed")
    app = app_module.create_app()
    app.state = state_app.state
    health = next(route.endpoint for route in app.routes if getattr(route, "path", None) == "/health")
    result = await health()
    assert result == {"status": "healthy", "service": "deer-flow-gateway", "auth_mode": "local", "registration": "closed", "storage": "ready", "agents": "ready"}
    monkeypatch.setattr(ss, "get_agents_api_config", lambda: AgentsApiConfig(ai_assistants_per_user=0, ai_employees=0))
    assert (await health())["agents"] == "none"
    app.state.agent_execution.storage_reason = "admission-failed"
    result = await health()
    assert result["agents"] == "unavailable" and result["storage"] == "unavailable"
    assert all(isinstance(value, str) for value in result.values())
    app.state.storage_spaces_enabled = False
    assert (await health())["agents"] == (await health())["storage"] == "off"


@pytest.mark.asyncio
async def test_human_directory_rechecks_disabled_and_role_limits():
    from app.gateway.storage_spaces import human_lookup

    ref = PrincipalRef("human", "person")
    user = SimpleNamespace(id="person", system_role="admin", disabled_at=None, role_limit=None, needs_setup=False, oauth_provider=None)
    repository = SimpleNamespace(get_user_by_id=AsyncMock(return_value=user))
    lookup = human_lookup(repository)
    assert (await lookup(ref)).can_provision_company
    user.role_limit = "user"
    assert not (await lookup(ref)).can_provision_company
    user.disabled_at = "retired"
    assert await lookup(ref) is None
    assert repository.get_user_by_id.await_count == 3


@pytest.mark.asyncio
async def test_memory_backend_cannot_start_managed_storage():
    from app.gateway.storage_spaces import initialize_storage_spaces
    from deerflow.config.storage_spaces_config import StorageSpacesConfig

    app = SimpleNamespace(state=SimpleNamespace())
    config = StorageSpacesConfig(enabled=True, inventory_path="/provider/inventory.v1.json")
    with pytest.raises(RuntimeError, match="durable database"):
        await initialize_storage_spaces(app, config, session_factory=None)
    assert getattr(app.state, "storage_spaces", None) is None


def test_enabled_inventory_must_be_explicit_absolute_and_startup_only():
    from pydantic import ValidationError

    from deerflow.config.reload_boundary import is_startup_only_field
    from deerflow.config.storage_spaces_config import StorageSpacesConfig

    with pytest.raises(ValidationError):
        StorageSpacesConfig(enabled=True)
    with pytest.raises(ValidationError):
        StorageSpacesConfig(enabled=True, inventory_path="relative/inventory")
    assert StorageSpacesConfig().enabled is False
    assert is_startup_only_field("storage_spaces")


@pytest.mark.asyncio
async def test_disabled_storage_startup_installs_tombstone_authority(instances):
    from _storage_spaces_test_support import ALICE, operation
    from test_agent_conversations import conversations
    from test_agent_instances import create

    from app.gateway.storage_spaces import initialize_storage_spaces
    from deerflow.config.storage_spaces_config import StorageSpacesConfig

    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    app = SimpleNamespace(state=SimpleNamespace(thread_store=threads))
    await initialize_storage_spaces(app, StorageSpacesConfig(), session_factory=sf)
    assert app.state.agent_instances is None and threads.instance_authority is app.state.agent_conversations
    assert await threads.get(chat["thread_id"], user_id="alice") is None


@pytest.mark.asyncio
async def test_managed_runtime_ownership_failure_retries_without_losing_history(startup):
    from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable

    ss, app, config, sf, _, adapter, _ = startup
    adapter.side_effect = StorageRuntimeUnavailable("Pool owned by another Gateway")
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    assert (await ss.storage_state(app)).reason == "host-unqualified"
    assert app.state.agent_instances is not None
    adapter.side_effect = None
    await ss.storage_recheck(app)
    assert (await ss.storage_state(app)).state == "ready"
    assert adapter.await_count >= 2
