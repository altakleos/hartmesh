"""Held worker and revocation barriers for the direct agent-file boundary."""

import asyncio
import threading
from types import SimpleNamespace

import httpx
import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_file_http import file_app
from test_agent_instances import instances as instances

from deerflow.agent_instances.agent_files import AgentFiles
from deerflow.agent_instances.contract import AgentPermission
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.spaces.contract import Permission


async def checkpoints():
    # Explicit event-loop turns, with no elapsed-time correctness threshold.
    for _ in range(3):
        ready = asyncio.get_running_loop().create_future()
        asyncio.get_running_loop().call_soon(ready.set_result, None)
        await ready


@pytest.mark.asyncio
@pytest.mark.parametrize("conversation", [False, True])
async def test_revocation_after_body_admission_but_during_hash_refuses_publication(instances, monkeypatch, conversation):
    app, agents, agent, authority, sf = await file_app(instances, monkeypatch, actor=BOB)
    if conversation:
        async with sf() as session, session.begin():
            session.add(AgentInstanceGrantRow(instance_id=agent.id, user_id="bob", permissions=int(AgentPermission.USE)))
        chat = await authority.create(actor=BOB, instance_id=agent.id, creation_id=operation())
    else:
        home = await agents.files.registry.get(actor=ALICE, space_id=agent.home_id)
        await agents.files.registry.set_grant(actor=ALICE, space_id=agent.home_id, subject=BOB, permissions=Permission.READ | Permission.WRITE, expected_generation=home.generation, acknowledge_existing_data=True)
    entered, release = asyncio.Event(), threading.Event()
    loop, original_hash = asyncio.get_running_loop(), AgentFiles._hash

    def held_hash(content, size):
        value = original_hash(content, size)
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(10)
        return value

    monkeypatch.setattr(AgentFiles, "_hash", staticmethod(held_hash))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        prefix = f"/api/agent-instances/{agent.id}"
        if conversation:
            task = asyncio.create_task(client.post(prefix + f"/conversations/{chat['thread_id']}/files", params={"operation_id": operation()}, files={"file": ("given.txt", b"not published")}))
        else:
            task = asyncio.create_task(client.put(prefix + "/home/content", params={"operation_id": operation(), "path": "uploads/given.txt"}, content=b"not published"))
        try:
            await asyncio.wait_for(entered.wait(), 10)
            if conversation:
                async with sf() as session, session.begin():
                    await session.delete(await session.get(AgentInstanceGrantRow, (agent.id, "bob")))
            else:
                home = await agents.files.registry.get(actor=ALICE, space_id=agent.home_id)
                await agents.files.registry.set_grant(actor=ALICE, space_id=agent.home_id, subject=BOB, permissions=Permission.READ, expected_generation=home.generation, acknowledge_existing_data=True)
            release.set()
            response = await asyncio.wait_for(task, 10)
            assert response.status_code == (404 if conversation else 403), response.text
            with pytest.raises(FileNotFoundError):
                await agents.files.read(actor=agent.principal, space_id=agent.home_id, path="uploads/given.txt", max_bytes=100)
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_multipart_cancellation_drains_the_exact_disk_writer_before_close(monkeypatch):
    from starlette import formparsers
    from starlette.datastructures import Headers

    from app.gateway.routers import agent_home

    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    close_admitted = asyncio.Event()
    owned = []
    original_spool, original_io = formparsers.SpooledTemporaryFile, agent_home.run_file_io

    class HeldSpool:
        def __init__(self, **kwargs):
            self.raw = original_spool(**kwargs)
            self.raw.rollover()  # Exercise Starlette's existing disk-worker path.
            owned.append(self)

        def __getattr__(self, name):
            return getattr(self.raw, name)

        def write(self, data):
            result = self.raw.write(data)
            entered.set()
            assert release.wait(10)
            finished.set()
            return result

    async def observed_io(call, *args, **kwargs):
        if call is agent_home._close:
            close_admitted.set()
        return await original_io(call, *args, **kwargs)

    async def stream():
        yield b'--held\r\nContent-Disposition: form-data; name="file"; filename="file"\r\n\r\n'
        yield b"payload"
        yield b"\r\n--held--\r\n"

    request = SimpleNamespace(headers=Headers({"Content-Type": "multipart/form-data; boundary=held"}), stream=stream)
    monkeypatch.setattr(formparsers, "SpooledTemporaryFile", HeldSpool)
    monkeypatch.setattr(agent_home, "run_file_io", observed_io)

    async def consume():
        async with agent_home._body(request, multipart=True):
            raise AssertionError("Parsing cannot finish before the held writer")

    task = asyncio.create_task(consume())
    try:
        assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 5), 6)
        task.cancel()
        await checkpoints()
        task.cancel()
        await checkpoints()
        assert not close_admitted.is_set(), "Closure was admitted over a live spool writer"
        assert not task.done()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.wait_for(asyncio.to_thread(finished.wait, 5), 6)
    assert len(owned) == 1 and owned[0].closed


@pytest.mark.asyncio
async def test_cancelled_agent_upload_metadata_scan_drains_before_scope_cleanup(instances, monkeypatch):
    from langchain_core.messages import HumanMessage
    from test_instance_output_presentation import prepared

    from deerflow.agent_instances import presentation
    from deerflow.agent_instances.runtime import InstanceSandboxProvider, execution_scope
    from deerflow.agents.middlewares.uploads_middleware import UploadsMiddleware

    execution, runtime, root = await prepared(instances)
    (root / "uploads").mkdir(exist_ok=True)
    (root / "uploads/given.txt").write_bytes(b"given")
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original_metadata = presentation._metadata
    middleware = UploadsMiddleware()
    original_before = middleware.before_agent

    def held_metadata(*args, **kwargs):
        value = original_metadata(*args, **kwargs)
        entered.set()
        assert release.wait(10)
        return value

    def observed_before(*args, **kwargs):
        try:
            return original_before(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(presentation, "_metadata", held_metadata)
    monkeypatch.setattr(middleware, "before_agent", observed_before)
    message = HumanMessage(content="Read this", additional_kwargs={"files": [{"filename": "given.txt"}]})
    with execution_scope(execution) as environment:
        environment.provider = InstanceSandboxProvider(execution, SimpleNamespace(id="fixture"))
        task = asyncio.create_task(middleware.abefore_agent({"messages": [message]}, runtime))
        try:
            assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 5), 6)
            task.cancel()
            await checkpoints()
            task.cancel()
            await checkpoints()
            assert not task.done(), "The metadata worker outlived its middleware owner"
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            assert await asyncio.wait_for(asyncio.to_thread(finished.wait, 5), 6)
