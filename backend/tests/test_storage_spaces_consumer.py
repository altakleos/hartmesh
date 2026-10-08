"""Async consumer semantics on injected SQL/file fixtures, not native quota proof."""

import asyncio
import hashlib
import importlib.util
from pathlib import Path

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, make_storage_fixture, operation
from deerflow_extension_api import ResourceReference, StorageAccessDenied, StorageActor, StorageConflict, StorageIdentityRequired

from deerflow.extensions.loader import load_extensions
from deerflow.runtime import user_context
from deerflow.runtime.user_context import get_current_user
from deerflow.spaces.contract import Permission, PrincipalRef, ResolvedPrincipal
from deerflow.spaces.facade import HostStorageProvider, storage_actor_scope
from deerflow.spaces.principals import HostPrincipalResolver

WORKER = PrincipalRef("nonhuman", "worker:wiki")


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def consumer(tmp_path, request):
    async for files, _, _ in make_storage_fixture(tmp_path, request):
        active = {ALICE: ResolvedPrincipal(ALICE, True), BOB: ResolvedPrincipal(BOB), WORKER: ResolvedPrincipal(WORKER)}

        async def lookup(reference):
            return active.get(reference)

        files.registry._resolver = HostPrincipalResolver(human=lookup, nonhuman=lookup)
        provider = HostStorageProvider(lambda: files)
        token = user_context._current_user.set(None)
        try:
            yield files, provider, active
        finally:
            user_context._current_user.reset(token)


@pytest.mark.asyncio
async def test_restricted_company_wiki_accepts_async_nonhuman_edits_and_explicit_import(consumer):
    _, provider, active = consumer
    installed, errors = load_extensions([])
    assert not errors and not installed.plugins  # Storage needs no optional view.
    assert get_current_user() is None
    with storage_actor_scope(ALICE):
        owner = await provider.current()
        wiki = await owner.provision(name="Team wiki", custody="company")
        private = await owner.provision(name="Sources", custody="personal")
        await owner.write(space_id=private.id, generation=1, operation_id=operation(), path="source.md", content=b"Private source\n", create=True)
        wiki = await owner.grant(space_id=wiki.id, generation=wiki.generation, subject=StorageActor(WORKER.kind, WORKER.subject_id), permissions=int(Permission.READ | Permission.WRITE | Permission.EXPORT), acknowledge_existing_data=True)
    with storage_actor_scope(BOB), pytest.raises(StorageAccessDenied):
        await (await provider.current()).get(space_id=wiki.id)
    with storage_actor_scope(ALICE):
        wiki = await owner.grant(space_id=wiki.id, generation=wiki.generation, subject=StorageActor(BOB.kind, BOB.subject_id), permissions=int(Permission.READ), acknowledge_existing_data=True)
        transfer = dict(source=ResourceReference(private.id, "source.md"), destination=ResourceReference(wiki.id, "source.md"), source_generation=1, destination_generation=wiki.generation)
        with pytest.raises(StorageAccessDenied):
            await owner.copy(**transfer, operation_id=operation())
        await owner.copy(**transfer, operation_id=operation(), acknowledge_disclosure=True)

    async def update():
        assert get_current_user() is None
        storage = await provider.current()
        assert storage.actor.kind == "nonhuman" and storage.actor.subject_id == WORKER.subject_id
        await storage.mkdir(space_id=wiki.id, generation=wiki.generation, operation_id=operation(), path="pages")
        first = await storage.write(space_id=wiki.id, generation=wiki.generation, operation_id=operation(), path="pages/home.md", content=b"# Home\nFirst\n", create=True)
        second = await storage.write(space_id=wiki.id, generation=wiki.generation, operation_id=operation(), path="pages/home.md", content=b"# Home\nSecond\n", expected_sha256=first)
        assert second == hashlib.sha256(b"# Home\nSecond\n").hexdigest()
        return storage

    with storage_actor_scope(WORKER):
        captured = await asyncio.create_task(update())
    with storage_actor_scope(WORKER):
        independent = await provider.current()
        assert await independent.read(space_id=wiki.id, path="pages/home.md", max_bytes=100) == b"# Home\nSecond\n"
        assert await independent.read(space_id=wiki.id, path="source.md", max_bytes=100) == b"Private source\n"
    with storage_actor_scope(PrincipalRef("nonhuman", "worker:forged")), pytest.raises(StorageIdentityRequired):
        await provider.current()

    ready, resume = asyncio.Event(), asyncio.Event()

    async def delayed_read():
        ready.set()
        await resume.wait()
        return await captured.read(space_id=wiki.id, path="pages/home.md", max_bytes=100)

    with storage_actor_scope(WORKER):
        delayed = asyncio.create_task(delayed_read())
    try:
        await ready.wait()
        del active[WORKER]
        resume.set()
        with pytest.raises(StorageIdentityRequired):
            await delayed
    finally:
        resume.set()
        await asyncio.gather(delayed, return_exceptions=True)
    with storage_actor_scope(ALICE):
        retained = await owner.get(space_id=wiki.id)
        assert retained.id == wiki.id and retained.custody == "company" and retained.custodian is None
        assert await owner.read(space_id=wiki.id, path="pages/home.md", max_bytes=100) == b"# Home\nSecond\n"
    with storage_actor_scope(BOB):
        reader = await provider.current()
        assert await reader.read(space_id=wiki.id, path="source.md", max_bytes=100) == b"Private source\n"
        with pytest.raises(StorageAccessDenied):
            await reader.write(space_id=wiki.id, generation=wiki.generation, operation_id=operation(), path="unauthorized", content=b"x", create=True)


@pytest.mark.asyncio
async def test_delayed_async_consumer_cannot_write_a_stale_generation(consumer):
    files, provider, _ = consumer
    with storage_actor_scope(WORKER):
        storage = await provider.current()
        resource = await storage.provision(name="Home", custody="personal")
        await storage.write(space_id=resource.id, generation=1, operation_id=operation(), path="page", content=b"before", create=True)
        ready, resume = asyncio.Event(), asyncio.Event()

        async def delayed():
            ready.set()
            await resume.wait()
            await storage.write(space_id=resource.id, generation=resource.generation, operation_id=operation(), path="page", content=b"late", expected_sha256=hashlib.sha256(b"before").hexdigest())

        pending = asyncio.create_task(delayed())
        try:
            await ready.wait()
            await files.registry.rename(actor=WORKER, space_id=resource.id, expected_generation=1, name="Renamed")
            resume.set()
            with pytest.raises(StorageConflict):
                await pending
        finally:
            resume.set()
            await asyncio.gather(pending, return_exceptions=True)
        assert await storage.read(space_id=resource.id, path="page", max_bytes=100) == b"before"


@pytest.mark.asyncio
async def test_public_consumer_example_preserves_intent_identity_and_captured_reference(consumer):
    from deerflow_extension_api import StorageUnsupported

    _, provider, _ = consumer
    source = Path(__file__).resolve().parents[2] / "examples" / "storage-consumer" / "consumer.py"
    spec = importlib.util.spec_from_file_location("storage_consumer_example", source)
    example = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(example)
    with storage_actor_scope(WORKER):
        resource = await (await provider.current()).provision(name="Example home", custody="personal")
        intent = operation()
        arguments = dict(resource=resource, path="home.md", content=b"# Home\n", operation_id=intent)
        reference = await example.create_file(provider, **arguments)
        assert reference == ResourceReference(resource.id, "home.md", hashlib.sha256(b"# Home\n").hexdigest())
        assert await example.create_file(provider, **arguments) == reference
        with pytest.raises(StorageConflict):
            await example.create_file(provider, **{**arguments, "content": b"changed"})
        assert await (await provider.current()).read(space_id=resource.id, path=reference.path, max_bytes=100) == b"# Home\n"
        with pytest.raises(StorageUnsupported):
            await example.create_file(HostStorageProvider(lambda: None), **arguments)
