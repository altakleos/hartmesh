"""Gateway eligibility borrows SQL; release descriptor work stays off-loop."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from threading import Lock
from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import ALICE, home, make_storage_fixture

from app.gateway import storage_spaces as ss
from deerflow.community.aio_sandbox.native_retirement import NativeRetirementScope
from deerflow.community.aio_sandbox.owner_admission import OwnerAdmissions
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceRow
from deerflow.persistence.user.model import UserRow


@pytest.mark.asyncio
async def test_released_user_reclamation_keeps_real_filesystem_proof_off_loop(tmp_path, monkeypatch):
    async for files, sf, catalog in make_storage_fixture(tmp_path, SimpleNamespace(param="sqlite")):

        def qualify():
            for slot, volume in list(catalog.verified.items()):
                catalog.verified[slot] = SimpleNamespace(
                    spec=volume.spec, data_path=volume.data_path, control_path=volume.control_path, device=volume.device, root_inode=volume.root_inode, control_inode=volume.control_inode, filesystem=volume.filesystem
                )

        await asyncio.to_thread(qualify)
        space = await home(files)
        for spec in catalog.volumes.values():
            spec.kind = "user"
        async with sf.kw["bind"].begin() as connection:
            await connection.run_sync(UserRow.__table__.create)
            await connection.run_sync(StorageFeatureLinkRow.__table__.create)
        async with sf() as session, session.begin():
            session.add(UserRow(id=ALICE.subject_id, email="fixture@example.com", released_at=datetime.now(UTC)))
            session.add(StorageFeatureLinkRow(namespace="hm.user-space", key="human:" + ALICE.subject_id, space_id=space.id))
            row = await session.get(SpaceRow, space.id)
            row.status, row.generation = "deleted", 2
            binding = await session.get(SpaceBackingRow, space.backing_handle)
            session.add(SpaceEventRow(space_id=space.id, generation=2, action="delete", actor_kind="human", actor_id=ALICE.subject_id, details={"retained_backups": False}))
        volume = catalog.verified[binding.slot_id]
        proofs = []
        gate = OwnerAdmissions(lock=Lock())

        def proof(scope):
            scope.assert_current()
            proofs.append(volume.data_path.stat())

        provider = SimpleNamespace(_native_creation=object(), _pool_runtime_current=lambda: True, _prove_native_retirement_absent=proof)

        @asynccontextmanager
        async def retained(owner, slot):
            assert (owner, slot) == (ALICE.subject_id, binding.slot_id)
            reservation = gate.reserve(owner, slot)
            try:
                yield NativeRetirementScope(provider, gate, reservation, provider._native_creation)
            finally:
                gate.end_retirement(reservation)

        provider._native_retirement_scope = retained
        status = ss.ExecutionStatus(host_id="fixture-host", qualified_provider=provider)
        app = SimpleNamespace(state=SimpleNamespace(storage_spaces=files, storage_spaces_enabled=True, agent_execution=status))

        async def available(_app):
            return ss.StorageState("ready")

        # This test exercises actual admission/reclamation, while the status
        # projection needs unrelated account-limit tables in the full app.
        monkeypatch.setattr(ss, "storage_state", available)
        assert await ss.reclaim_released_user_spaces(app) == 1
        assert len(proofs) == 1
        async with sf() as session:
            assert await session.get(SpaceBackingRow, space.backing_handle) is None
            assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + ALICE.subject_id)) is None
