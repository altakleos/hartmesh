"""Serve the actual Home routes and a private SQLite store to the browser tests."""

import asyncio
import errno
import json
import socket
import sys
from contextlib import aclosing, asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import _storage_spaces_test_support as storage_support
import pytest
import uvicorn
from _storage_spaces_test_support import ALICE, FixtureCatalog, FixtureVolume
from sqlalchemy import func, select
from test_agent_file_http import file_app
from test_agent_instances import make_instances

from app.gateway.routers.spaces import router as space_router
from deerflow.agent_instances.contract import AgentPermission
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.spaces.filesystem import ConfinedFilesystem


def hold_first_intent(app, registry, root, sf, patch):
    """Hold a real uncommitted intent while another connection reads its status."""
    gates = {action: (asyncio.Event(), asyncio.Event()) for action in ("put", "rename", "remove")}
    admitted = registry.admitted

    @asynccontextmanager
    async def gated(**kwargs):
        async with admitted(**kwargs) as (session, rows):
            yield session, rows
            for row in session.new:
                if not isinstance(row, SpaceFileOperationRow):
                    continue
                action, path = row.request.get("action"), row.request.get("path")
                if (action, path) not in (("put", "deferred.txt"), ("rename", "concurrent.txt"), ("remove", "concurrent.txt")):
                    continue
                entered, release = gates[action]
                if action != "put":
                    # The browser's first change is still uncommitted. Replace
                    # the name from outside that writer, then roll the writer
                    # back after the test has observed automatic and manual reads.
                    replacement = root / "replacement-fixture"
                    replacement.write_text("replacement-" + action, encoding="utf-8")
                    replacement.replace(root / path)
                entered.set()
                await asyncio.wait_for(release.wait(), 45)
                if action != "put":
                    raise ConnectionAbortedError(errno.ECONNABORTED, "test rollback before intent commit")

    patch.setattr(registry, "admitted", gated)

    @app.get("/test/intent")
    async def at_commit(action: str = "put"):
        await asyncio.wait_for(gates[action][0].wait(), 30)
        return {"entered": True}

    @app.post("/test/commit")
    async def resume(action: str = "put"):
        gates[action][1].set()
        return {"released": True}

    @app.get("/test/observed")
    async def observed():
        async with sf() as session:
            count = await session.scalar(select(func.count()).select_from(SpaceFileOperationRow))
        return {"receipts": count, "current": (root / "concurrent.txt").read_text(encoding="utf-8"), "destination": (root / "moved-concurrent.txt").exists()}


def inject_file_failures(app, files, patch):
    write = ConfinedFilesystem.write_atomic
    refuse = True

    def injected(filesystem, path, *args, **kwargs):
        nonlocal refuse
        if path == "uploads/clean-chat.txt" and refuse:
            refuse = False
            raise PermissionError(errno.EACCES, "test permission refusal")
        result = write(filesystem, path, *args, **kwargs)
        if path == "uploads/collision (2).txt":
            raise OSError(errno.EIO, "test interruption after publication")
        return result

    patch.setattr(ConfinedFilesystem, "write_atomic", injected)

    @app.post("/test/settle")
    async def settle():
        return {"settled": await files.settle_interrupted()}


class BrowserCatalog(FixtureCatalog):
    """Reopen the test host's same volume identities after a process restart."""

    def __init__(self, folder):
        manifest = folder / "volumes.json"
        if manifest.exists():
            self.volumes, self.verified = {}, {}
            for slot, entry in json.loads(manifest.read_text(encoding="utf-8")).items():
                spec = SimpleNamespace(**entry["spec"])
                root = folder / entry["directory"]
                self.volumes[slot] = spec
                self.verified[slot] = FixtureVolume(spec, root / "data", root / "control")
        else:
            super().__init__(folder)
            manifest.write_text(json.dumps({slot: {"spec": vars(volume.spec), "directory": volume.data_path.parent.name} for slot, volume in self.verified.items()}), encoding="utf-8")


async def serve(thread_id, folder):
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(storage_support, "FixtureCatalog", BrowserCatalog)
        async with aclosing(make_instances(folder, SimpleNamespace(param="sqlite"), patch)) as fixture:
            app, agents, agent, authority, sf = await file_app(await anext(fixture), patch, creation_id="a" * 32)
            app.include_router(space_router)
            home = await agents.files.registry.get(actor=ALICE, space_id=agent.home_id)
            async with sf() as session:
                backing = await session.get(SpaceBackingRow, home.backing_handle)
            hold_first_intent(app, agents.files.registry, agents.files.catalog.verified[backing.slot_id].data_path, sf, patch)
            inject_file_failures(app, agents.files, patch)

            @app.post("/test/revoke-use")
            async def revoke_use():
                async with sf() as session, session.begin():
                    grant = await session.get(AgentInstanceGrantRow, (agent.id, ALICE.subject_id))
                    grant.permissions = int(AgentPermission.INSPECT)
                return {"revoked": True}

            await authority.create(actor=ALICE, instance_id=agent.id, thread_id=thread_id, creation_id="b" * 32)
            with socket.socket() as endpoint:
                endpoint.bind(("127.0.0.1", 0))
                server = uvicorn.Server(uvicorn.Config(app, log_level="error", access_log=False))
                task = asyncio.create_task(server.serve(sockets=[endpoint]))
                while not server.started:
                    if task.done():
                        await task
                        raise RuntimeError("File gateway stopped during startup")
                    await asyncio.sleep(0.02)
                print(json.dumps({"origin": f"http://127.0.0.1:{endpoint.getsockname()[1]}", "instance": agent.id, "home": agent.home_id}), flush=True)
                await task


if __name__ == "__main__":
    asyncio.run(serve(sys.argv[1], Path(sys.argv[2])))
