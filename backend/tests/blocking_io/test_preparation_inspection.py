"""Exact preparation facts use asynchronous SQL without provider effects."""

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from _storage_spaces_test_support import ALICE, home, make_storage_fixture, operation
from test_storage_spaces_attachments import ContainedProvider

from deerflow.spaces.attachments import ResourceMount, SpaceAttachments


@pytest.mark.asyncio
async def test_preparation_inspection_uses_only_async_metadata(tmp_path):
    async for files, _sf, catalog in make_storage_fixture(tmp_path, SimpleNamespace(param="sqlite")):

        def qualify():
            for slot, volume in list(catalog.verified.items()):
                catalog.verified[slot] = SimpleNamespace(
                    spec=volume.spec, data_path=volume.data_path, control_path=volume.control_path, device=volume.device, root_inode=volume.root_inode, control_inode=volume.control_inode, filesystem=volume.filesystem
                )

        await asyncio.to_thread(qualify)
        provider = ContainedProvider()
        mounts = SpaceAttachments(files, provider)
        space = await home(files)
        resources = [ResourceMount(space.id, 1, "home", writable=True)]
        attached = await mounts.attach(actor=ALICE, incarnation=operation(), resources=resources)
        for name in ("prepare", "start", "fence", "discover", "cleanup", "verify_active"):
            setattr(provider, name, Mock(side_effect=AssertionError("No provider effect during inspection")))
        facts = await mounts.inspect_preparation(actor=ALICE, incarnation=attached.incarnation, resources=resources, host_id=provider.host_id)
        assert facts.id == attached.id and facts.phase == "active"
        assert await mounts.inspect_preparation(actor=ALICE, incarnation=operation(), resources=resources, host_id=provider.host_id) is None
