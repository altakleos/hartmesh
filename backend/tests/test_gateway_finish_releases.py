"""All non-User kinds use real SQL and the existing immutable release boundary."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _storage_spaces_test_support import ALICE
from test_compose_user_reclamation import ContainmentFixture
from test_storage_spaces_release import deleted
from test_storage_spaces_service import space_file_storage as space_file_storage

from app.gateway import storage_spaces as ss
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.spaces.contract import Custody, MutationMode
from deerflow.spaces.recovery import SpaceRecovery


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["assistant", "employee", "company"])
async def test_finish_releases_renews_roots_for_each_non_user_kind(space_file_storage, kind, monkeypatch):
    files, sf, catalog = space_file_storage
    for spec in catalog.volumes.values():
        spec.kind = kind
    async with files.provisioning(actor=ALICE, name="Released", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, kind=kind) as (_session, space):
        pass
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "old").write_bytes(b"old tenant data")
    provider = ContainmentFixture(sf)
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=files, storage_spaces_enabled=True, agent_execution=ss.ExecutionStatus(qualified_provider=provider)))
    monkeypatch.setattr("app.gateway.storage_reclamation.reclaim_released_user_spaces", AsyncMock(return_value=0))
    assert await ss.finish_releases(app) == 1
    assert provider.calls == [("space:" + space.id, slot)]
    assert provider.proof_count > 0
    assert not any(volume.data_path.iterdir())
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is None


@pytest.mark.asyncio
async def test_release_keeps_changed_space_after_lifecycle_scope_await(space_file_storage, monkeypatch):
    from deerflow.persistence.spaces.model import SpaceRow

    files, sf, catalog = space_file_storage
    for spec in catalog.volumes.values():
        spec.kind = "employee"
    async with files.provisioning(actor=ALICE, name="Changed", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, kind="employee") as (_session, space):
        pass
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "keep").write_bytes(b"new generation")
    provider = ContainmentFixture(sf)

    async def changed():
        async with sf() as session, session.begin():
            (await session.get(SpaceRow, space.id)).generation += 1

    provider.on_enter = changed
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=files, storage_spaces_enabled=True, agent_execution=ss.ExecutionStatus(qualified_provider=provider)))
    monkeypatch.setattr("app.gateway.storage_reclamation.reclaim_released_user_spaces", AsyncMock(return_value=0))
    assert await ss.finish_releases(app) == 0
    assert (volume.data_path / "keep").read_bytes() == b"new generation"
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["ordinary", "unverified", "proof-failure"])
async def test_single_deleted_space_release_uses_containment_and_preserves_uncertainty(space_file_storage, mode, monkeypatch):
    from app.gateway.storage_reclamation import release_deleted_space

    files, sf, catalog = space_file_storage
    for spec in catalog.volumes.values():
        spec.kind = "assistant"
    async with files.provisioning(actor=ALICE, name="Deleted", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, kind="assistant") as (_session, space):
        pass
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "old").write_bytes(b"old data")
    provider = ContainmentFixture(sf)
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=files, storage_spaces_enabled=True, agent_execution=ss.ExecutionStatus(qualified_provider=provider)))
    if mode == "unverified":
        from deerflow.spaces.backings import BackingUnavailable

        def unavailable(*_args):
            raise BackingUnavailable("Offline double")

        monkeypatch.setattr(catalog, "verify", unavailable)
    elif mode == "proof-failure":
        provider.proof_error = RuntimeError("absence is uncertain")
    result = await release_deleted_space(app, space.id)
    assert provider.calls == [("space:" + space.id, slot)]
    assert result["slot_released"] is (mode == "ordinary")
    async with sf() as session:
        assert (await session.get(SpaceBackingRow, space.backing_handle) is not None) is (mode == "proof-failure")
    if mode == "unverified":
        assert result["complete"] and (volume.data_path / "old").read_bytes() == b"old data"


@pytest.mark.asyncio
async def test_single_space_release_replay_calls_kernel_once_without_reacquiring_a_reused_slot(space_file_storage, monkeypatch):
    from app.gateway.storage_reclamation import release_deleted_space

    files, sf, catalog = space_file_storage
    for spec in catalog.volumes.values():
        spec.kind = "employee"
    async with files.provisioning(actor=ALICE, name="Deleted", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, kind="employee") as (_session, space):
        pass
    await deleted(sf, space)
    provider = ContainmentFixture(sf)
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=files, storage_spaces_enabled=True, agent_execution=ss.ExecutionStatus(qualified_provider=provider)))
    assert (await release_deleted_space(app, space.id))["slot_released"]
    calls = []
    original = SpaceRecovery.host_release

    async def kernel(self, space_id, **kwargs):
        calls.append(space_id)
        return await original(self, space_id, **kwargs)

    monkeypatch.setattr(SpaceRecovery, "host_release", kernel)
    result = await release_deleted_space(app, space.id)
    assert result["complete"] and result["slot_released"] and not result["newly_reclaimed"]
    assert calls == [space.id] and len(provider.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["status", "provider", "reason", "files"])
async def test_single_space_release_refuses_changed_qualification_after_external_await(space_file_storage, change):
    from app.gateway.storage_reclamation import release_deleted_space

    files, sf, catalog = space_file_storage
    for spec in catalog.volumes.values():
        spec.kind = "assistant"
    async with files.provisioning(actor=ALICE, name="Deleted", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, kind="assistant") as (_session, space):
        pass
    slot = await deleted(sf, space)
    volume = catalog.verified[slot]
    (volume.data_path / "keep").write_bytes(b"preserve")
    provider = ContainmentFixture(sf)
    state = SimpleNamespace(storage_spaces=files, storage_spaces_enabled=True, agent_execution=ss.ExecutionStatus(qualified_provider=provider))

    async def changed():
        if change == "status":
            state.agent_execution = ss.ExecutionStatus(qualified_provider=ContainmentFixture(sf))
        elif change == "provider":
            state.agent_execution.qualified_provider = ContainmentFixture(sf)
        elif change == "reason":
            state.agent_execution.storage_reason = "admission-failed"
        else:
            state.storage_spaces = object()

    provider.on_enter = changed
    assert (await release_deleted_space(SimpleNamespace(state=state), space.id))["slot_released"] is False
    assert (volume.data_path / "keep").read_bytes() == b"preserve"
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is not None
