"""Strict real factory admission keeps owner, disk journal and root proof off-loop."""

import time
from threading import get_ident
from types import SimpleNamespace

import pytest
from test_native_create_bridge import fixture as fixture
from test_native_resident_containment_scope import prepare

from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.utils.file_io import run_file_io


@pytest.mark.asyncio
async def test_actual_factory_admission_offloads_full_bridge_and_bound_root_proof(tmp_path, fixture, monkeypatch):
    import _storage_spaces_test_support as support
    from test_storage_spaces_attachments import ContainedProvider

    from deerflow.spaces.attachments import SpaceAttachments

    catalog = await run_file_io(support.FixtureCatalog, tmp_path)

    def facts():
        return {slot: (volume.root_inode, volume.control_inode) for slot, volume in catalog.verified.items()}

    identities = await run_file_io(facts)
    monkeypatch.setattr(support.FixtureVolume, "root_inode", property(lambda volume: identities[volume.spec.slot_id][0]))
    monkeypatch.setattr(support.FixtureVolume, "control_inode", property(lambda volume: identities[volume.spec.slot_id][1]))
    monkeypatch.setattr(support, "FixtureCatalog", lambda _path: catalog)
    async for files, sf, catalog in support.make_storage_fixture(tmp_path, SimpleNamespace(param="sqlite")):
        adapter = ContainedProvider()
        attachments = SpaceAttachments(files, adapter)
        factory, saved, captured, _objects, _calls = await prepare((files, sf, catalog, attachments, adapter), fixture)
        owner_thread = get_ident()
        observed = []
        check = factory.provider._native_creation.assert_current

        def offloop(**kwargs):
            assert get_ident() != owner_thread
            observed.append(True)
            return check(**kwargs)

        monkeypatch.setattr(factory.provider._native_creation, "assert_current", offloop)
        async with factory(attachments, captured) as scope:
            async with files.registry.host_locked(scope.parent_ids) as (session, _parents):
                await scope.admit(session, {saved.attachment_id: await session.get(SpaceAttachmentRow, saved.attachment_id)})
        assert len(observed) >= 3


@pytest.mark.asyncio
async def test_actual_orphan_recovery_drains_bridge_roots_and_journal_offloop(tmp_path, fixture, monkeypatch):
    import _storage_spaces_test_support as support
    from test_native_resident_recovery_scope import eligibility
    from test_storage_spaces_attachments import ContainedProvider

    from deerflow.spaces.attachments import SpaceAttachments
    from deerflow.spaces.resident_recovery import NativeResidentRecoveryScopes

    catalog = await run_file_io(support.FixtureCatalog, tmp_path)
    identities = await run_file_io(lambda: {slot: (volume.root_inode, volume.control_inode) for slot, volume in catalog.verified.items()})
    monkeypatch.setattr(support.FixtureVolume, "root_inode", property(lambda volume: identities[volume.spec.slot_id][0]))
    monkeypatch.setattr(support.FixtureVolume, "control_inode", property(lambda volume: identities[volume.spec.slot_id][1]))
    monkeypatch.setattr(support, "FixtureCatalog", lambda _path: catalog)
    async for files, sf, catalog in support.make_storage_fixture(tmp_path, SimpleNamespace(param="sqlite")):
        adapter = ContainedProvider()
        attachments = SpaceAttachments(files, adapter)
        storage = files, sf, catalog, attachments, adapter
        factory, saved, _captured, objects, _calls = await prepare(storage, fixture)
        eligible = await eligibility(storage, saved, missing=True)
        owner_thread = get_ident()
        check = factory.provider._native_creation.assert_current
        observed = []

        def offloop(**kwargs):
            assert get_ident() != owner_thread
            observed.append(True)
            return check(**kwargs)

        monkeypatch.setattr(factory.provider._native_creation, "assert_current", offloop)
        async with NativeResidentRecoveryScopes(factory)(saved, eligible=eligible, deadline=time.monotonic() + 30) as scope:
            await scope.contain()
            await scope.finalize()
        assert not objects and len(observed) > 3
        assert factory.provider._native_creation.residents.records() == ()
