"""Ordinary and Shared imports against assigned native storage and SQL authority."""

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import Request
from test_compose_native_file_sources import native_request
from test_storage_feature_routes import feature_app as feature_app

from app.gateway.conversation_file_import import ConversationSource, SharedPublicationSource, capture_source
from app.gateway.conversation_file_upload import import_into_chat
from app.gateway.routers import conversation_files
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentDefinitionRevisionRow, AgentInstanceRow
from deerflow.persistence.shared_publications.model import SharedPublicationRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.spaces.contract import Permission, PrincipalRef


@pytest.mark.asyncio
@pytest.mark.parametrize("source_kind", ["conversation", "shared_publication"])
async def test_native_import_captures_the_assigned_source_and_rechecks_in_the_home_sql_writer(feature_app, source_kind):
    client, app, files, catalog, base, alice, _, _ = feature_app
    native_request(app)
    app.include_router(conversation_files.router)
    owner = str(alice.id)
    actor = PrincipalRef("human", owner)
    spaces = await files.registry.list(actor=actor)
    user = next(s for s in spaces if s.custody.kind == "personal")
    shared = next(s for s in spaces if s.custody.kind == "company")
    sf = files.registry._sf
    async with sf() as session:
        user_backing = await session.get(SpaceBackingRow, user.backing_handle)
        company_backing = await session.get(SpaceBackingRow, shared.backing_handle)
    personal = catalog.verified[user_backing.slot_id].data_path
    company = catalog.verified[company_backing.slot_id].data_path
    rows = {name: {"thread_id": name, "user_id": owner, "incarnation": name} for name in ["earlier", "later"]}
    app.state.thread_store.get = AsyncMock(side_effect=lambda name, **kw: rows.get(name))
    app.state.run_store = MemoryRunStore()
    app.state.run_event_store = MemoryRunEventStore()
    await app.state.run_store.put("answer", thread_id="earlier", user_id=owner, status="success")
    path = "/mnt/user-data/outputs/notes.txt"
    from deerflow.runtime.user_context import bind_authenticated_user

    with bind_authenticated_user(owner):
        await app.state.run_event_store.put(thread_id="earlier", run_id="answer", event_type="run.delivery", category="outputs", content={"presented_files": [path]})
    data = b"native meeting notes"
    sha = hashlib.sha256(data).hexdigest()
    config = SimpleNamespace(uploads={})
    if source_kind == "conversation":
        source = ConversationSource(thread_id="earlier", run_id="answer", path=path)
        folder = personal / "conversations/earlier/user-data/outputs"
        folder.mkdir(parents=True)
        (folder / "notes.txt").write_bytes(data)
    else:
        source = SharedPublicationSource(publication_id="published", sha256=sha)
        (company / "shared/notes.txt").write_bytes(data)
        await app.state.shared_publications_repo.record_publication(publication_id="published", path="notes.txt", size=len(data), sha256=sha, published_by=owner, from_thread_id=None, from_path=None)
    # Use the actual SQL tables checked inside the borrowed Home writer.
    async with sf().bind.begin() as conn:
        for model in (ThreadMetaRow, AgentDefinitionRevisionRow, AgentInstanceRow, AgentConversationRow):
            await conn.run_sync(lambda sync, m=model: m.__table__.create(sync, checkfirst=True))
    async with sf() as session, session.begin():
        session.add(ThreadMetaRow(thread_id="earlier", user_id=owner, incarnation="earlier"))

    @app.post("/test-import")
    async def do_import(request: Request):
        return await import_into_chat(request=request, thread_id="later", source=source, config=config)

    @app.post("/test-admit/{change}")
    async def guard(request: Request, change: str):
        capture = await capture_source(request, source, maximum_bytes=1024, home_destination=True, acknowledge_disclosure=True)
        async with sf() as session, session.begin():
            if change == "remove":
                if source_kind == "conversation":
                    thread = await session.get(ThreadMetaRow, "earlier")
                    thread.incarnation = "replacement"
                else:
                    from datetime import UTC, datetime

                    publication = await session.get(SharedPublicationRow, "published")
                    publication.removed_at = datetime.now(UTC)
            if change == "revoke":
                grant = await session.get(SpaceGrantRow, (user.id if source_kind == "conversation" else shared.id, "human", owner))
                grant.permissions = int(Permission.READ)
            await session.flush()
            await capture.admit_home_publication(session)
        return {"sha256": capture.sha256}

    answer = await client.post("/test-import")
    assert answer.status_code == 200, answer.text
    assert answer.json()["sha256"] == sha
    assert (personal / "conversations/later/user-data/uploads/notes.txt").read_bytes() == data
    assert not base.sandbox_outputs_dir("earlier", user_id=owner).exists()
    repeated = await client.post("/test-import")
    assert repeated.status_code == 200, repeated.text
    assert repeated.json()["filename"] == "notes.txt"
    assert (await client.post("/test-admit/none")).status_code == 200
    assert (await client.post("/test-admit/remove")).status_code in (404, 409)
    assert (await client.post("/test-admit/revoke")).status_code == 403
