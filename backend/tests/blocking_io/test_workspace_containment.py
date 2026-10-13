"""Workspace provider calls remain off-loop through real SQL closure."""

import asyncio
import time
from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import ALICE, home, make_storage_fixture, operation
from test_storage_spaces_attachments import ContainedProvider

from deerflow.spaces.attachments import ResourceMount, SpaceAttachments


@pytest.mark.asyncio
async def test_workspace_containment_offloads_inspection_removal_and_cleanup(tmp_path):
    async for files, _sf, catalog in make_storage_fixture(tmp_path, SimpleNamespace(param="sqlite")):

        def qualify():
            for slot, volume in list(catalog.verified.items()):
                catalog.verified[slot] = SimpleNamespace(
                    spec=volume.spec, data_path=volume.data_path, control_path=volume.control_path, device=volume.device, root_inode=volume.root_inode, control_inode=volume.control_inode, filesystem=volume.filesystem
                )

        await asyncio.to_thread(qualify)
        provider = ContainedProvider()
        attachments = SpaceAttachments(files, provider)
        space = await home(files)
        attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
        discover, fence = provider.discover, provider.fence
        calls = []

        def inspection(container, identifier):
            time.sleep(0.01)
            calls.append("inspect")
            return "running"

        def census(identifier):
            time.sleep(0.01)
            calls.append("discover")
            return discover(identifier)

        def removal(container, identifier):
            time.sleep(0.01)
            calls.append("fence")
            fence(container, identifier)

        def cleanup(identifier):
            time.sleep(0.01)
            calls.append("cleanup")

        provider.container_state, provider.discover, provider.fence, provider.cleanup = inspection, census, removal, cleanup
        assert await attachments.container_state(attached.id) == "running"
        assert await attachments.contain_host(attached.id, "idle")
        assert calls == ["inspect", "discover", "fence", "cleanup"]
