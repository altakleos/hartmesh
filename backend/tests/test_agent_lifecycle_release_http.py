"""Deletion publishes its release outcome through actual management routes."""

import httpx
import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_conversation_http import app_for
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_agent_lifecycle import lifecycle_fixture
from test_compose_user_reclamation import ContainmentFixture
from test_resident_registry import registry as registry
from test_resident_registry import running

from app.gateway import storage_spaces as ss
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
from deerflow.spaces.attachments import ResourceMount
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.recovery import SpaceRecovery


@pytest.mark.asyncio
async def test_pending_and_abandoned_delete_never_attempt_release_or_report_a_release_outcome(instances, monkeypatch):
    from unittest.mock import AsyncMock

    agents, authority, sf, _lifecycle, attachments, provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    provider.uncertain = True
    release = AsyncMock(side_effect=AssertionError("Only a completed deletion may release its Space"))
    monkeypatch.setattr("app.gateway.storage_reclamation.release_deleted_space", release)
    app = app_for(agents, authority, ThreadMetaRepository(sf, instance_authority=authority), monkeypatch, actor=ALICE)
    body = {"generation": agent.generation, "operation_id": operation(), "action": "delete"}
    endpoint = f"/api/agent-instances/{agent.id}/lifecycle"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        pending = await client.post(endpoint, json=body)
        assert pending.status_code == 202 and pending.json()["complete"] is False, pending.text
        assert "space_released" not in pending.json()
        provider.uncertain = False
        await attachments.retire(reason="lifecycle", actor=ALICE, space_id=home.id, expected_generation=home.generation)
        abandoned = await client.post(endpoint + "/abandon", json={"generation": pending.json()["instance"]["generation"], "operation_id": body["operation_id"]})
        assert abandoned.status_code == 200 and abandoned.json()["abandoned"], abandoned.text
        replay = await client.post(endpoint, json=body)
        assert replay.status_code == 200 and replay.json()["abandoned"], replay.text
        assert "space_released" not in abandoned.json() and "space_released" not in replay.json()
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_http_only_if_idle_uses_the_owning_registry_run_ids(instances, registry, monkeypatch):
    from app.gateway.refusals import refusal_handler
    from deerflow.agent_instances.contract import Refusal

    agents, authority, sf, _lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    residents, _provider, records, _clock = registry()
    agents.residents = residents
    running(records, "actual-running-task", agent)
    running(records, "ended-task", agent)
    residents.ended.add("ended-task")
    app = app_for(agents, authority, ThreadMetaRepository(sf, instance_authority=authority), monkeypatch, actor=ALICE)
    app.add_exception_handler(Refusal, refusal_handler)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        response = await client.post(f"/api/agent-instances/{agent.id}/lifecycle", json={"generation": agent.generation, "operation_id": operation(), "action": "grant", "member_id": "bob", "permissions": 1, "only_if_idle": True})
        assert response.status_code == 409, response.text
        assert response.json()["reason"] == "work-in-flight" and response.json()["run_ids"] == ["actual-running-task"]


@pytest.mark.asyncio
@pytest.mark.parametrize("custody", ["personal", "company"])
@pytest.mark.parametrize("outcome", ["released", "unverified", "missing-inventory", "geometry", "failed"])
async def test_deleted_agent_http_releases_once_and_replays_its_committed_intent(instances, custody, outcome, monkeypatch):
    agents, authority, sf, _lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    kind = "assistant" if custody == "personal" else "employee"
    catalog = agents.files.catalog
    for spec in catalog.volumes.values():
        spec.kind = kind
    agent = await create(agents, custody=custody, supervisor=BOB if custody == "company" else ALICE)
    home = await agents.files.registry.get(actor=ALICE, space_id=agent.home_id)
    async with sf() as session:
        binding = await session.get(SpaceBackingRow, home.backing_handle)
        slot = binding.slot_id
    volume = catalog.verified[slot]
    (volume.data_path / "old").write_bytes(b"old tenant data")
    provider = ContainmentFixture(sf)
    if outcome == "unverified":

        def unavailable(*_args, **_kwargs):
            raise BackingUnavailable("Offline verification failure")

        monkeypatch.setattr(catalog, "verify", unavailable)
    elif outcome == "failed":
        provider.proof_error = RuntimeError("uncertain absence")
    elif outcome == "missing-inventory":
        catalog.volumes.pop(slot)
    elif outcome == "geometry":
        catalog.volumes[slot].filesystem_uuid = "changed-geometry"
    app = app_for(agents, authority, ThreadMetaRepository(sf, instance_authority=authority), monkeypatch, actor=ALICE)
    app.state.storage_spaces, app.state.storage_spaces_enabled = agents.files, True
    app.state.agent_execution = ss.ExecutionStatus(qualified_provider=provider)
    calls = []
    original = SpaceRecovery.host_release

    async def kernel(self, space_id, **kwargs):
        calls.append(space_id)
        return await original(self, space_id, **kwargs)

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("Agent deletion must use only host_release")

    monkeypatch.setattr(SpaceRecovery, "host_release", kernel)
    monkeypatch.setattr(SpaceRecovery, "release", forbidden)
    monkeypatch.setattr(SpaceRecovery, "delete", forbidden)
    body = {"generation": agent.generation, "operation_id": operation(), "action": "delete"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        endpoint = f"/api/agent-instances/{agent.id}/lifecycle"
        response = await client.post(endpoint, json=body)
        assert response.status_code == 200, response.text
        assert response.json()["instance"]["status"] == "deleted"
        assert response.json()["space_released"] is (outcome == "released")
        assert calls == [agent.home_id]
        if outcome == "released":
            replay = await client.post(endpoint, json=body)
            assert replay.status_code == 200 and replay.json()["space_released"] is True, replay.text
            assert calls == [agent.home_id, agent.home_id]
    if outcome == "released":
        assert slot in agents.files.assignable_slots(kind) and not any(volume.data_path.iterdir())
        fresh = await create(agents, custody=custody, supervisor=BOB if custody == "company" else ALICE)
        fresh_home = await agents.files.registry.get(actor=ALICE, space_id=fresh.home_id)
        async with sf() as session:
            assert (await session.get(SpaceBackingRow, fresh_home.backing_handle)).slot_id == slot
    elif outcome in {"unverified", "missing-inventory", "geometry"}:
        assert any(slot in slots for slots in agents.files.unclaimed_slots.values())
        assert (volume.data_path / "old").read_bytes() == b"old tenant data"
        async with sf() as session:
            assert await session.get(SpaceBackingRow, home.backing_handle) is None
