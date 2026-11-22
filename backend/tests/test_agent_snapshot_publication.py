"""Agent shares use an interactive manager's authority for a specific active Home."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from test_agent_conversations import conversations
from test_agent_instances import create, definition
from test_compose_native_file_sources import native_request
from test_storage_feature_routes import feature_app as feature_app

from app.gateway import snapshot_publication
from app.gateway.routers import shared_conversations
from deerflow.agent_instances.contract import AgentPermission
from deerflow.agent_instances.directory import InstanceDirectory
from deerflow.agent_instances.service import AgentInstances
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow, AgentProtectedContextRow
from deerflow.persistence.base import Base
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.user_context import bind_authenticated_user
from deerflow.spaces.contract import Permission, PrincipalRef
from deerflow.spaces.principals import HostPrincipalResolver

PATH = "/mnt/user-data/outputs/sample.txt"
API = "/api/shared-conversations"


async def employee_case(feature_app, monkeypatch, *, custody="company", started_by_manager=False):
    client, app, files, catalog, _, alice, bob, current = feature_app
    native_request(app)
    sf = files.registry._sf
    async with sf.kw["bind"].begin() as conn:
        await conn.run_sync(Base.metadata.create_all, tables=[r.__table__ for r in (AgentDefinitionRevisionRow, AgentInstanceRow, AgentInstanceGrantRow, AgentLifecycleRow)])
    human = files.registry._resolver._human
    directory = InstanceDirectory(sf, human=human)
    files.registry._resolver = HostPrincipalResolver(human=human, nonhuman=directory.lookup, nonhuman_in_session=directory.lookup_in_session)
    agents = AgentInstances(files, directory)
    _, authority, threads, _ = await conversations((agents, files, sf, catalog))
    manager, starter = (PrincipalRef("human", str(u.id)) for u in (alice, bob))
    agent = await create(agents, actor=manager, supervisor=manager, custody=custody, snapshot=definition(manager.subject_id))
    async with sf.begin() as s:
        s.add(AgentInstanceGrantRow(instance_id=agent.id, user_id=starter.subject_id, permissions=int(AgentPermission.USE)))
    row = await authority.create(actor=manager if started_by_manager else starter, instance_id=agent.id, thread_id="meeting")
    app.state.agent_conversations, app.state.agent_instances, app.state.thread_store = authority, agents, threads
    app.state.run_store = MemoryRunStore()
    app.state.run_manager = RunManager(store=app.state.run_store)
    app.state.run_event_store = MemoryRunEventStore()
    await app.state.run_store.put("finished", thread_id=row["thread_id"], user_id=starter.subject_id, status="success")
    with bind_authenticated_user(starter.subject_id):
        await app.state.run_event_store.put(thread_id="meeting", run_id="finished", event_type="run.delivery", category="outputs", content={"presented_files": [PATH]})
        for kind, event, words in (("human", "llm.human.input", "Meeting note"), ("ai", "llm.ai.response", "Ready")):
            await app.state.run_event_store.put(thread_id="meeting", run_id="finished", event_type=event, category="message", content={"type": kind, "content": words})

    # Feed, source inspection and attachment discovery use the actual Gateway.
    # The independent checkpoint only supplies the saved display title.
    def checkpoint(scope, *, thread_id, assistant_id, agent_inspection):
        assert thread_id == "meeting"
        assert agent_inspection.instance.id == agent.id
        assert agent_inspection.requester.subject_id == manager.subject_id
        state = SimpleNamespace(values={"title": "Weekly note"}, config={"configurable": {"checkpoint_id": "saved"}})
        return SimpleNamespace(aget=AsyncMock(return_value=state)), {}

    monkeypatch.setattr("app.gateway.transcript.build_checkpoint_state_accessor", checkpoint)
    app.include_router(shared_conversations.router)
    home = next(v.data_path for v in catalog.verified.values() if (v.data_path / "outputs").is_dir())
    (home / "outputs" / "sample.txt").write_bytes(b"Home bytes")
    (home / "outputs" / "unpresented.txt").write_bytes(b"Other note")
    return client, app, sf, agent, alice, bob, current, home


@pytest.mark.asyncio
@pytest.mark.parametrize("custody,kind", [("company", "employee"), ("personal", "assistant")])
async def test_manager_shares_another_speakers_presented_home_bytes_only(feature_app, monkeypatch, custody, kind):
    client, app, sf, agent, manager, starter, current, home = await employee_case(feature_app, monkeypatch, custody=custody)
    response = await client.get(API + "/draft", params={"thread_id": "meeting"})
    assert response.status_code == 200, response.text
    d = response.json()
    assert d["with"] == {"kind": kind, "id": agent.id, "name": "Analyst"}
    assert (d["audience"]["chat_count"], d["audience"]["inspect_count"]) == (2, 1)
    assert [f["path"] for f in d["files"]] == ["files/sample.txt"]
    body = {"thread_id": "meeting", "snapshot": d["snapshot"]}
    for ack in ({}, {"acknowledge": {"audience": "company", "snapshot": "f" * 64}}):
        response = await client.post(API, json=body | ack)
        assert response.status_code == 409 and response.json()["detail"]["code"] == "snapshot-acknowledgement-required", response.text
    assert not await app.state.shared_publications_repo.live_publications()
    async with sf.begin() as session:
        link = await session.get(StorageFeatureLinkRow, ("hm.user-space", f"human:{manager.id}"))
        assert link is not None
        user_space = link.space_id
        user_slot = await session.scalar(select(SpaceBackingRow.slot_id).where(SpaceBackingRow.space_id == user_space))
        await session.delete(link)
    body["acknowledge"] = {"snapshot": d["snapshot"], "audience": "company"}
    shared = await client.post(API, json=body)
    assert shared.status_code == 201, shared.text
    result = shared.json()
    snap = (await client.get(API + "/" + result["publication_id"])).json()
    assert snap["with"] == d["with"] and len(snap["files"]) == 1
    assert snap["messages"] == [{"type": "human", "content": "Meeting note"}, {"type": "ai", "content": "Ready"}]
    assert not {"audience", "thread_id", "requester_id", "authors"}.intersection(snap)
    assert (await client.get("/api/shared/" + snap["files"][0]["shared_path"])).content == b"Home bytes"
    record = await app.state.shared_publications_repo.publication(snap["files"][0]["publication_id"])
    assert (record["from_path"], record["from_thread_id"], record["published_by"]) == (PATH, "meeting", str(manager.id))
    # Generic flat and folder responses must also keep the bound source private.
    from app.gateway.routers import file_browser

    app.include_router(file_browser.router)
    folders = {snap["files"][0]["shared_path"].rsplit("/", 1)[0], snap["transcript"]["shared_path"].rsplit("/", 1)[0]}
    batched = AsyncMock(wraps=app.state.agent_conversations.bindings)
    monkeypatch.setattr(app.state.agent_conversations, "bindings", batched)
    for route, params in [("/api/shared", {})] + [("/api/file-browser/v1/shared/folder", {"path": folder}) for folder in sorted(folders)]:
        batched.reset_mock()
        listing = await client.get(route, params=params)
        assert listing.status_code == 200, listing.text
        listed = [row for row in listing.json()["files"] if row.get("publication_id")]
        assert listed and all(row["from_thread_id"] is None for row in listed)
        batched.assert_awaited_once_with(["meeting"])
    # A separate My Files publish can also resolve to an existing snapshot row.
    async with sf.begin() as session:
        session.add(StorageFeatureLinkRow(namespace="hm.user-space", key=f"human:{manager.id}", space_id=user_space))
    folder = record["path"].rsplit("/", 1)[0]
    (app.state.storage_spaces.catalog.verified[user_slot].data_path / "files" / "same-note.txt").write_bytes(b"Home bytes")
    duplicate = await client.post("/api/shared/publish", json={"path": "/mnt/user-data/files/same-note.txt", "folder": folder})
    assert duplicate.status_code == 200, duplicate.text
    assert duplicate.json()["publication_id"] == record["publication_id"] and duplicate.json()["from_thread_id"] is None
    assert (await client.post(API, json=body)).json()["already_shared"]
    undo = await client.post(API + "/" + result["publication_id"] + "/undo", json={"undo_receipt": result["undo_receipt"], "publication_ids": result["created_publication_ids"]})
    assert undo.status_code == 200, undo.text


@pytest.mark.asyncio
async def test_each_publication_requires_the_current_manager_home_and_interactive_session(feature_app, monkeypatch):
    from app.gateway import agent_snapshot_source
    from app.gateway.auth_disabled import AUTH_SOURCE_AUTH_DISABLED, AUTH_SOURCE_INTERNAL, AUTH_SOURCE_PAT

    client, app, sf, agent, manager, starter, current, home = await employee_case(feature_app, monkeypatch)

    async def profile(identifier):
        return {str(manager.id): manager, str(starter.id): starter}.get(identifier)

    monkeypatch.setattr(agent_snapshot_source, "get_user_repo", lambda: SimpleNamespace(get_user_by_id=profile))
    original = (await client.get(API + "/draft", params={"thread_id": "meeting"})).json()
    assert original["audience"] == {"authors": [starter.email], "starter": starter.email, "starter_can_read": True, "inspect_count": 1, "chat_count": 2}
    body = {"thread_id": "meeting", "snapshot": original["snapshot"], "acknowledge": {"snapshot": original["snapshot"], "audience": "company"}}

    async def rejected():
        for method in ("get", "post"):
            response = await (client.get(API + "/draft", params={"thread_id": "meeting"}) if method == "get" else client.post(API, json=body))
            assert response.status_code == 404, response.text
            assert "sample.txt" not in response.text

    current[0] = starter
    await rejected()
    current[0] = manager
    for bit in [AgentPermission.MANAGE, AgentPermission.INSPECT, AgentPermission.USE]:
        async with sf.begin() as s:
            (await s.get(AgentInstanceGrantRow, (agent.id, str(manager.id)))).permissions = int(bit)
        await rejected()
    async with sf.begin() as s:
        (await s.get(AgentInstanceGrantRow, (agent.id, str(manager.id)))).permissions = 7
        grant = await s.get(SpaceGrantRow, (agent.home_id, "human", str(manager.id)))
        original_home = grant.permissions
        grant.permissions = original_home & ~int(Permission.ADMIN)
    await rejected()
    async with sf.begin() as s:
        (await s.get(SpaceGrantRow, (agent.home_id, "human", str(manager.id)))).permissions = original_home
    # The route and its live bindings are the product handlers. Substitute only
    # the credential already resolved by Gateway authentication middleware.
    original_admission = agent_snapshot_source.AgentSnapshotSource.for_thread.__func__
    for source in [AUTH_SOURCE_AUTH_DISABLED, AUTH_SOURCE_INTERNAL, AUTH_SOURCE_PAT, None]:

        async def as_credential(cls, request, thread_id):
            request.state.auth_source = source
            return await original_admission(cls, request, thread_id)

        with monkeypatch.context() as m:
            m.setattr(agent_snapshot_source.AgentSnapshotSource, "for_thread", classmethod(as_credential))
            await rejected()
    assert not await app.state.shared_publications_repo.live_publications()

    (home / "outputs/sample.txt").write_bytes(b"updated before publishing")
    response = await client.post(API, json=body)
    assert response.status_code == 409 and response.json()["detail"]["code"] == "snapshot-changed"
    assert not await app.state.shared_publications_repo.live_publications()


@pytest.mark.asyncio
async def test_source_manager_is_rechecked_inside_the_shared_writer_before_any_publication(feature_app, monkeypatch):
    client, app, sf, agent, manager, starter, current, home = await employee_case(feature_app, monkeypatch)
    preview = await client.get(API + "/draft", params={"thread_id": "meeting"})
    assert preview.status_code == 200, preview.text
    digest = preview.json()["snapshot"]
    writer = snapshot_publication.publish_snapshot_file

    async def after_capture(**kwargs):
        async with sf.begin() as s:
            (await s.get(AgentInstanceGrantRow, (agent.id, str(manager.id)))).permissions = int(AgentPermission.INSPECT)
        return await writer(**kwargs)

    monkeypatch.setattr(snapshot_publication, "publish_snapshot_file", after_capture)
    denied = await client.post(API, json={"thread_id": "meeting", "snapshot": digest, "acknowledge": {"snapshot": digest, "audience": "company"}})
    assert denied.status_code == 404, denied.text
    assert not await app.state.shared_publications_repo.live_publications()
    assert (await client.post("/api/shared/publish", json={"path": PATH, "thread_id": "meeting", "folder": "Normal"})).status_code in {403, 404, 501}


@pytest.mark.asyncio
async def test_starter_current_inspection_and_distinct_active_grants(feature_app, monkeypatch):
    client, app, sf, agent, manager, starter, current, home = await employee_case(feature_app, monkeypatch)

    async def audience():
        response = await client.get(API + "/draft", params={"thread_id": "meeting"})
        assert response.status_code == 200, response.text
        data = response.json()["audience"]
        return data["starter_can_read"], data["inspect_count"], data["chat_count"]

    async with sf.begin() as session:
        session.add(AgentInstanceGrantRow(instance_id=agent.id, user_id="removed-account", permissions=7))
    assert await audience() == (True, 1, 2)
    async with sf.begin() as session:
        session.add(AgentProtectedContextRow(thread_id="meeting"))
    assert await audience() == (False, 1, 2)
    async with sf.begin() as session:
        (await session.get(AgentInstanceGrantRow, (agent.id, str(starter.id)))).permissions = int(AgentPermission.INSPECT | AgentPermission.USE)
    assert await audience() == (True, 2, 2)
    async with sf.begin() as session:
        await session.delete(await session.get(AgentInstanceGrantRow, (agent.id, str(starter.id))))
    assert await audience() == (False, 1, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("cut", ["before-identity", "before-name", "after-name"])
async def test_manager_copy_uses_standard_attributed_settlement_and_stable_retry(feature_app, monkeypatch, cut):
    from threading import Event

    from app.gateway import native_shared_publications
    from app.gateway.chat_files import QualifiedUploadStage
    from deerflow.persistence.spaces.files import SpaceFileOperationRow
    from deerflow.spaces.workflows import WorkflowReceipt

    client, app, sf, agent, manager, starter, current, home = await employee_case(feature_app, monkeypatch)
    files = app.state.storage_spaces
    company = next(volume for volume in files.catalog.verified.values() if volume.spec.kind == "company")
    draft = await client.get(API + "/draft", params={"thread_id": "meeting"})
    assert draft.status_code == 200, draft.text
    digest = draft.json()["snapshot"]
    body = {"thread_id": "meeting", "snapshot": digest, "acknowledge": {"audience": "company", "snapshot": digest}}
    app.state.snapshot_stopping = Event()
    publish = QualifiedUploadStage.publish

    def interrupted(*args, **kwargs):
        if cut == "after-name":
            publish(*args, **kwargs)
        app.state.snapshot_stopping.set()
        raise OSError("stopped during publication of the presented file")

    with monkeypatch.context() as patch:
        if cut == "before-identity":
            patch.setattr(native_shared_publications, "record_fact", interrupted)
        else:
            patch.setattr(QualifiedUploadStage, "publish", interrupted)
        stop = await client.post(API, json=body)
        assert stop.status_code == 503 and stop.json()["detail"]["code"] == "gateway-stopping", stop.text
    app.state.snapshot_stopping.clear()
    async with sf() as session:
        operation = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending"))).one()
    assert operation.request["source"]["path"] == PATH and operation.request["source"]["thread_id"] == "meeting"
    assert operation.actor_id == str(manager.id)
    receipt = WorkflowReceipt(company, operation.operation_id, operation.request)
    receipt.load()
    facts = receipt.value["facts"]
    assert facts["publication_order"] == "fact-before-name"
    if cut == "before-identity":
        assert set(facts) == {"publication_order"}
    else:
        disk = company.data_path / "shared" / facts["publication"]["path"]
        assert disk.exists() is (cut == "after-name")
        if disk.exists():
            assert disk.read_bytes() == b"Home bytes"
            assert facts["publication_identity"] == {"inode": disk.stat().st_ino, "device": disk.stat().st_dev}
    assert (await client.get(API)).json()["conversations"] == []
    assert await files.settle_interrupted() == 1
    async with sf() as session:
        settled = await session.get(SpaceFileOperationRow, (operation.space_id, operation.operation_id))
        assert settled.phase == ("complete" if cut == "after-name" else "failed")
    finished = await client.post(API, json=body)
    assert finished.status_code == 201, finished.text
    assert (await client.post(API, json=body)).json()["publication_id"] == finished.json()["publication_id"]


@pytest.mark.asyncio
async def test_a_manager_who_can_read_as_the_starter_still_needs_explicit_inspect_to_share(feature_app, monkeypatch):
    client, app, sf, agent, manager, *_ = await employee_case(feature_app, monkeypatch, started_by_manager=True)
    draft = await client.get(API + "/draft", params={"thread_id": "meeting"})
    assert draft.status_code == 200, draft.text
    digest = draft.json()["snapshot"]
    async with sf.begin() as session:
        (await session.get(AgentInstanceGrantRow, (agent.id, str(manager.id)))).permissions = int(AgentPermission.MANAGE | AgentPermission.USE)
    assert await app.state.agent_conversations.allowed(actor=PrincipalRef("human", str(manager.id)), thread_id="meeting", permission=AgentPermission.INSPECT)
    denied = await client.get(API + "/draft", params={"thread_id": "meeting"})
    assert denied.status_code == 404, denied.text
    denied = await client.post(API, json={"thread_id": "meeting", "snapshot": digest, "acknowledge": {"snapshot": digest, "audience": "company"}})
    assert denied.status_code == 404, denied.text
    assert not await app.state.shared_publications_repo.live_publications()
