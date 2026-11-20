"""Import receipts preserve the exact destination through collisions and retries."""

import hashlib

import pytest
from _storage_spaces_test_support import ALICE, operation
from test_agent_files import backing_root, put, setup
from test_agent_instances import instances as instances

from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.spaces.contract import SpaceConflict


def origin(marker="a", publication=None):
    from deerflow.spaces.file_import import FileImport

    return FileImport(binding=marker * 64, publication_id=publication)


@pytest.mark.asyncio
async def test_import_remembers_chosen_path_and_retries_with_original_name(instances, monkeypatch):
    agents, agent, files, sf = await setup(instances, monkeypatch)
    await put(agents, agent, b"occupied")
    key = operation()
    first = await put(agents, agent, operation_id=key, file_import=origin())
    assert first.path == "uploads/report (2).pdf"
    async with sf() as session:
        row = await session.get(SpaceFileOperationRow, (agent.home_id, key))
        assert row.request == {
            "action": "put",
            "path": first.path,
            "requested_path": "uploads/report.pdf",
            "destination": None,
            "size": 8,
            "sha256": hashlib.sha256(b"document").hexdigest(),
            "on_behalf_of": "alice",
            "source": {"kind": "own_conversation"},
            "import_binding": "a" * 64,
        }
    root = await backing_root(instances, agent)
    (root / first.path).unlink()
    (root / "uploads/report (3).pdf").write_bytes(b"later")
    assert await put(agents, agent, operation_id=key, file_import=origin()) == first
    assert not (root / first.path).exists()
    home = await files.registry.get(actor=agent.principal, space_id=agent.home_id)
    status = await agents.agent_files.status(instance=agent, requester=ALICE, generation=home.generation, operation_id=key, upload_name="report.pdf")
    assert status["status"] == "complete"
    assert status["file"]["sandbox_path"] == "/mnt/user-data/" + first.path
    assert "source" not in status["file"]
    for kwargs in ({"file_import": origin("b")}, {"file_import": origin(publication="b" * 32)}, {"file_import": origin(), "path": "uploads/different.pdf"}, {"file_import": origin(), "content": b"changed"}):
        with pytest.raises(SpaceConflict, match="different request"):
            await put(agents, agent, operation_id=key, **kwargs)


@pytest.mark.asyncio
async def test_concurrent_occupant_cannot_redirect_an_import(instances, monkeypatch):
    from deerflow.agent_instances.agent_files import AgentFileRefusal

    agents, agent, _, sf = await setup(instances, monkeypatch)
    await put(agents, agent, b"occupied")
    root = await backing_root(instances, agent)
    key = operation()

    async def concurrent_publication(session, _):
        row = await session.get(SpaceFileOperationRow, (agent.home_id, key))
        if row is not None:
            assert row.request["path"] == "uploads/report (2).pdf"
            (root / row.request["path"]).write_bytes(b"other writer")

    with pytest.raises(AgentFileRefusal) as refused:
        await put(agents, agent, operation_id=key, file_import=origin(), admission=concurrent_publication)
    assert refused.value.reason == "name-collision"
    assert (root / "uploads/report (2).pdf").read_bytes() == b"other writer"
    assert not (root / "uploads/report (3).pdf").exists()
    async with sf() as session:
        row = await session.get(SpaceFileOperationRow, (agent.home_id, key))
        assert row.phase == "failed"


@pytest.mark.asyncio
async def test_home_output_export_reads_exact_named_file_with_current_human_grants(instances):
    from test_instance_output_presentation import prepared

    from deerflow.agent_instances.contract import AgentDenied, AgentPermission
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.spaces.contract import Permission, SpaceDenied

    execution, _, root = await prepared(instances)
    agents, files, sf, _ = instances
    home, actor = execution.instance.home_id, execution.requester
    inspection = await execution.authority.inspection(actor=actor, thread_id=execution.thread_id)

    async def admit(session, rows):
        await execution.authority.admit_file_export(session, inspection, rows[home][0].generation)

    options = dict(actor=actor, space_id=home, path="outputs/result.txt", controller_admission=admit)

    async def set_agent_grant(permissions):
        async with sf() as session, session.begin():
            grant = await session.get(AgentInstanceGrantRow, (execution.instance.id, actor.subject_id))
            grant.permissions = int(permissions)

    await set_agent_grant(AgentPermission.USE)
    with pytest.raises(AgentDenied):
        await files.export_regular(**options, maximum_bytes=16)
    await set_agent_grant(AgentPermission.INSPECT)
    metadata = await files.export_regular(**options)
    assert metadata == (11, (root / "outputs/result.txt").stat().st_mtime)
    assert await files.export_regular(**options, maximum_bytes=16) == (b"real output", metadata[1])
    async with sf() as session, session.begin():
        grant = await session.get(SpaceGrantRow, (home, actor.kind, actor.subject_id))
        grant.permissions = int(Permission.READ)
    with pytest.raises(SpaceDenied):
        await files.export_regular(**options, maximum_bytes=16)


@pytest.mark.parametrize(
    "source, expected",
    [
        ({"kind": "own_conversation"}, {"kind": "own_conversation"}),
        ({"kind": "shared_publication", "publication_id": "sample-publication"}, {"kind": "shared_publication", "publication_id": "sample-publication"}),
        ({"kind": "future"}, None),
        ({"kind": "own_conversation", "thread_id": "sample-private-conversation"}, None),
    ],
)
def test_administrator_sees_only_recognized_provenance(source, expected):
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from deerflow.spaces.recovery import SpaceRecovery

    request = {"action": "put", "path": "uploads/sample.txt", "source": source, "import_binding": "a" * 64}
    row = SimpleNamespace(operation_id="1" * 32, generation=1, phase="complete", request=request, result={}, created_at=datetime.now(UTC))
    public = SpaceRecovery._operation_view(row)["request"]
    assert public.get("source") == expected
    assert "import_binding" not in public
    assert row.request["source"] is source


@pytest.mark.asyncio
@pytest.mark.parametrize("cut", ["before_write", "private_stage", "after_publication", "admission_refused"])
async def test_interrupted_import_settles_only_its_recorded_name(instances, monkeypatch, cut):
    import errno
    import os

    from deerflow.agent_instances.contract import AgentDenied, ResidentFilesBusy
    from deerflow.spaces.filesystem import ConfinedFilesystem
    from deerflow.spaces.service import SpaceFiles

    agents, agent, files, sf = await setup(instances, monkeypatch)
    await put(agents, agent, b"original")
    key = operation()
    root = await backing_root(instances, agent)

    async def admit(session, rows):
        row = await session.get(SpaceFileOperationRow, (agent.home_id, key))
        if cut == "admission_refused" and row is not None:
            raise AgentDenied("Source unavailable")

    original = ConfinedFilesystem.write_atomic

    def interrupt(filesystem, path, content, **kwargs):
        if cut == "after_publication":
            original(filesystem, path, content, **kwargs)
        elif cut == "private_stage":
            fd = os.open("stage-" + key, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600, dir_fd=filesystem._control_fd)
            try:
                os.write(fd, content)
            finally:
                os.close(fd)
        raise OSError(errno.EIO, "Write interrupted")

    with monkeypatch.context() as fault:
        fault.setattr(ConfinedFilesystem, "write_atomic", interrupt)
        with pytest.raises(AgentDenied if cut == "admission_refused" else ResidentFilesBusy):
            await put(agents, agent, operation_id=key, file_import=origin(), admission=admit)
    async with sf() as session:
        row = await session.get(SpaceFileOperationRow, (agent.home_id, key))
        assert row.request["path"] == "uploads/report (2).pdf"
        assert row.request["action"] == "put"
        assert row.phase == ("failed" if cut == "admission_refused" else "pending")
    if cut != "admission_refused":
        fresh = SpaceFiles(files.registry, instances[3])
        assert await fresh.settle_interrupted(space_id=agent.home_id, operation_id=key) == 1
    async with sf() as session:
        row = await session.get(SpaceFileOperationRow, (agent.home_id, key))
        if cut == "after_publication":
            assert row.phase == "complete"
            assert row.result["value"]["path"] == row.request["path"]
            assert (root / row.request["path"]).read_bytes() == b"document"
        else:
            assert row.phase == "failed"
            assert not (root / row.request["path"]).exists()
        assert row.request["source"] == {"kind": "own_conversation"}
    assert (root / "uploads/report.pdf").read_bytes() == b"original"
    await put(agents, agent, b"later", path="uploads/other.pdf")


@pytest.mark.asyncio
async def test_home_picker_obtains_consent_before_read_and_refuses_agent_sources(instances, monkeypatch):
    import httpx
    from sqlalchemy import func, select
    from test_agent_file_http import file_app
    from test_agent_instances import create

    app, agents, target, authority, sf = await file_app(instances, monkeypatch)
    chat = await authority.create(actor=ALICE, instance_id=target.id, creation_id=operation())
    earlier = await app.state.thread_store.create(operation(), user_id="alice")
    other_agent = await create(agents)
    other = await authority.create(actor=ALICE, instance_id=other_agent.id, creation_id=operation())
    dest = f"/api/agent-instances/{target.id}/conversations/{chat['thread_id']}/files/from-conversation"
    data = {"source_thread_id": earlier["thread_id"], "source_run_id": "example-run", "path": "/mnt/user-data/outputs/note.txt"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        for ack in ({}, {"acknowledge_disclosure": False}):
            response = await client.post(dest, params={"operation_id": operation()}, json={**data, **ack})
            assert response.status_code == 409, response.text
            assert response.json()["detail"]["code"] == "disclosure-needs-confirmation"
        for source in (chat, other):
            response = await client.post(dest, params={"operation_id": operation()}, json={**data, "source_thread_id": source["thread_id"], "acknowledge_disclosure": True})
            assert response.status_code == 403, response.text
            assert response.json()["detail"]["code"] == "not-between-agents"
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == target.home_id)) == 0


@pytest.mark.asyncio
async def test_home_route_scopes_private_origin_and_checks_it_again_at_publication(instances, monkeypatch):
    from types import SimpleNamespace

    import httpx
    from fastapi import HTTPException
    from sqlalchemy import select
    from test_agent_file_http import file_app

    from app.gateway.conversation_file_import import CapturedFile
    from app.gateway.routers import agent_home

    app, agents, target, authority, sf = await file_app(instances, monkeypatch)
    chat = await authority.create(actor=ALICE, instance_id=target.id, creation_id=operation())
    monkeypatch.setattr(agent_home, "get_auth_config", lambda: SimpleNamespace(jwt_secret="test signing key"))
    monkeypatch.setattr(agent_home, "existing_signing_key", lambda: "test signing key")
    earlier = operation()
    body = {"source_thread_id": earlier, "source_run_id": operation(), "path": "/mnt/user-data/outputs/note.txt", "acknowledge_disclosure": True}
    base = f"/api/agent-instances/{target.id}/conversations/{chat['thread_id']}/files"
    allowed = True
    phases = []

    async def capture(request, source, **options):
        assert options["acknowledge_disclosure"] is True
        assert options["home_destination"] is True

        async def admit(session):
            pending = await session.scalar(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == target.home_id, SpaceFileOperationRow.phase == "pending"))
            phases.append("effect" if pending else "admitted")
            if pending is not None and not allowed:
                raise HTTPException(403, "Source access changed")

        async def recheck():
            pass

        return CapturedFile("note.txt", b"data", hashlib.sha256(b"data").hexdigest(), recheck, admit)

    monkeypatch.setattr(agent_home, "capture_source", capture)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        key = operation()
        response = await client.post(base + "/from-conversation", params={"operation_id": key}, json=body)
        assert response.status_code == 201, response.text
        assert response.json() == {"filename": "note.txt", "size": 4, "sha256": hashlib.sha256(b"data").hexdigest(), "virtual_path": "/mnt/user-data/uploads/note.txt"}
        assert phases[-1] == "effect"
        retry = await client.post(base + "/from-conversation", params={"operation_id": key}, json=body)
        assert retry.status_code == 201 and retry.json() == response.json()
        another = await client.post(base + "/from-conversation", params={"operation_id": key}, json={**body, "source_thread_id": operation()})
        assert another.status_code == 409, another.text
        status = await client.get(base + "/operations/" + key, params={"filename": "note.txt"})
        assert status.status_code == 200 and status.json()["file"]["sandbox_path"] == response.json()["virtual_path"]
        allowed = False
        denied = await client.post(base + "/from-conversation", params={"operation_id": operation()}, json=body)
        assert denied.status_code == 403, denied.text
    async with sf() as session:
        rows = list(await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == target.home_id)))
        assert sorted(row.phase for row in rows) == ["complete", "failed"]
        assert rows[0].operation_id != key
        assert len({row.request["import_binding"] for row in rows}) == 2
        for row in rows:
            assert row.request["source"] == {"kind": "own_conversation"}
            assert earlier not in str(row.request) and body["source_run_id"] not in str(row.request)


def test_existing_feature_workflow_recovery_keeps_its_source():
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from deerflow.spaces.recovery import SpaceRecovery

    request = {"action": "feature-workflow", "source": {"space": "shared", "object": "example"}}
    row = SimpleNamespace(operation_id="1" * 32, generation=1, phase="pending", request=request, result=None, created_at=datetime.now(UTC))
    assert SpaceRecovery._operation_view(row)["request"] == request


@pytest.mark.asyncio
async def test_chat_only_cannot_list_or_copy_agent_files_even_when_there_are_no_runs(instances, monkeypatch):
    from unittest.mock import AsyncMock

    import httpx
    from _storage_spaces_test_support import BOB
    from test_agent_file_http import file_app

    from app.gateway.routers import conversation_files
    from deerflow.agent_instances.contract import AgentPermission
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    app, agents, target, authority, sf = await file_app(instances, monkeypatch, actor=BOB)
    async with sf() as session, session.begin():
        session.add(AgentInstanceGrantRow(instance_id=target.id, user_id=BOB.subject_id, permissions=int(AgentPermission.USE)))
    chat = await authority.create(actor=BOB, instance_id=target.id, creation_id=operation())
    app.include_router(conversation_files.router)
    destination = await app.state.thread_store.create(operation(), user_id=BOB.subject_id)
    from types import SimpleNamespace

    app.dependency_overrides[conversation_files.get_config] = lambda: SimpleNamespace(uploads={})
    observed = AsyncMock(return_value=[])
    app.state.run_manager.list_by_thread = observed
    app.state.run_store.get = AsyncMock()
    app.state.run_event_store.list_events = AsyncMock()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        response = await client.get(f"/api/threads/{chat['thread_id']}/presented-files")
        assert response.status_code == 403, response.text
        copy = await client.post(f"/api/threads/{destination['thread_id']}/uploads/from-conversation", json={"source_thread_id": chat["thread_id"], "source_run_id": "answer", "path": "/mnt/user-data/outputs/example.txt"})
        assert copy.status_code == 403, copy.text
        observed.assert_not_awaited()
        app.state.run_store.get.assert_not_awaited()
        app.state.run_event_store.list_events.assert_not_awaited()


def test_effect_admission_requires_the_exact_reserved_import_path():
    from types import SimpleNamespace

    from deerflow.spaces.service import SpaceFiles

    request = {"path": "uploads/report (2).pdf", **origin().request_fields("uploads/report.pdf")}
    row = SimpleNamespace(actor_kind=ALICE.kind, actor_id=ALICE.subject_id, generation=1, request={**request, "path": "uploads/report (3).pdf"})
    with pytest.raises(SpaceConflict):
        SpaceFiles._same_operation(row, ALICE, 1, request)


@pytest.mark.asyncio
async def test_shared_origin_on_completed_import_exposes_only_public_reference(instances, monkeypatch):
    from deerflow.spaces.recovery import SpaceRecovery

    agents, agent, _, sf = await setup(instances, monkeypatch)
    key = operation()
    await put(agents, agent, operation_id=key, file_import=origin(publication="public-sample"))
    async with sf() as session:
        row = await session.get(SpaceFileOperationRow, (agent.home_id, key))
        assert row.phase == "complete"
        request = SpaceRecovery._operation_view(row)["request"]
        assert request["source"] == {"kind": "shared_publication", "publication_id": "public-sample"}
        assert "import_binding" not in request


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata_only", [True, False])
async def test_changed_home_maps_to_a_retryable_state_conflict(monkeypatch, metadata_only):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from fastapi import HTTPException

    from app.gateway import conversation_file_home

    @asynccontextmanager
    async def changed_home(**_):
        raise SpaceConflict("Stale space generation")
        yield

    files = SimpleNamespace(registry=SimpleNamespace(admitted=changed_home), export_regular=AsyncMock(side_effect=SpaceConflict("Stale space generation")))
    inspection = SimpleNamespace(instance=SimpleNamespace(home_id="home"), requester=ALICE, home_generation=1, authority=SimpleNamespace(instances=SimpleNamespace(files=files)))
    monkeypatch.setattr(conversation_file_home, "inspection_for_read", AsyncMock(return_value=inspection))
    with pytest.raises(HTTPException) as refused:
        if metadata_only:
            await conversation_file_home.admit_agent_presented_source(None, "thread")
        else:
            await conversation_file_home.read_agent_presented_file(None, "thread", "/mnt/user-data/outputs/sample.txt", maximum_bytes=8)
    assert refused.value.status_code == 409


@pytest.mark.asyncio
@pytest.mark.parametrize("change_home_after_read", [False, True])
async def test_inspector_can_list_and_copy_another_requesters_presented_home_file(instances, monkeypatch, tmp_path, change_home_after_read):
    from types import SimpleNamespace

    import httpx
    from _storage_spaces_test_support import BOB
    from test_agent_file_http import file_app

    from app.gateway.routers import conversation_files, uploads
    from deerflow.agent_instances.contract import AgentPermission
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.runtime.user_context import bind_authenticated_user

    app, _, agent, authority, sf = await file_app(instances, monkeypatch)
    async with sf() as session, session.begin():
        session.add(AgentInstanceGrantRow(instance_id=agent.id, user_id=BOB.subject_id, permissions=int(AgentPermission.USE)))
        inspector = await session.get(AgentInstanceGrantRow, (agent.id, ALICE.subject_id))
        inspector.permissions = int(AgentPermission.INSPECT)
    chat = await authority.create(actor=BOB, instance_id=agent.id, creation_id=operation())
    source = chat["thread_id"]
    dest = (await app.state.thread_store.create(operation(), user_id=ALICE.subject_id))["thread_id"]
    root = await backing_root(instances, agent)
    path = "/mnt/user-data/outputs/example.txt"
    (root / "outputs").mkdir(exist_ok=True)
    (root / "outputs/example.txt").write_bytes(b"field notes")
    run = operation()
    await app.state.run_store.put(run, thread_id=source, user_id=BOB.subject_id, status="success", created_at="2026-01-02T00:00:00+00:00")
    with bind_authenticated_user(BOB.subject_id):
        await app.state.run_event_store.put(thread_id=source, run_id=run, event_type="run.delivery", category="outputs", content={"presented_files": [path]})
    app.include_router(conversation_files.router)
    app.dependency_overrides[conversation_files.get_config] = lambda: SimpleNamespace(uploads=SimpleNamespace(auto_convert_documents=False, max_file_size=96 << 20, max_total_size=128 << 20))
    uploads_dir = tmp_path / "ordinary-destination"
    uploads_dir.mkdir()
    monkeypatch.setattr(uploads, "ensure_uploads_dir", lambda *args, **kwargs: uploads_dir)
    monkeypatch.setattr(uploads, "get_sandbox_provider", lambda: None)
    monkeypatch.setattr(uploads, "_uses_thread_data_mounts", lambda _: True)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        listing = await client.get(f"/api/threads/{source}/presented-files")
        assert listing.status_code == 200, listing.text
        assert [(row["run_id"], row["path"], row["size"]) for row in listing.json()["files"]] == [(run, path, 11)]
        if change_home_after_read:
            from app.gateway import conversation_file_upload
            from deerflow.persistence.spaces.model import SpaceRow

            capture = conversation_file_upload.capture_source

            async def change_after_capture(*args, **kwargs):
                captured = await capture(*args, **kwargs)
                assert captured.content == b"field notes"
                async with sf() as session, session.begin():
                    (await session.get(SpaceRow, agent.home_id)).generation += 1
                return captured

            monkeypatch.setattr(conversation_file_upload, "capture_source", change_after_capture)

        response = await client.post(f"/api/threads/{dest}/uploads/from-conversation", json={"source_thread_id": source, "source_run_id": run, "path": path})
        if change_home_after_read:
            assert response.status_code == 404, response.text
            assert list(uploads_dir.iterdir()) == []
            return
        assert response.status_code == 200, response.text
        assert response.json()["sha256"] == hashlib.sha256(b"field notes").hexdigest()
    assert (uploads_dir / "example.txt").read_bytes() == b"field notes"
