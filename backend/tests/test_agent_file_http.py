"""Authenticated direct-file routes with streaming bodies and real SQL rights."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from sqlalchemy import select
from test_agent_conversation_http import app_for
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances

from deerflow.agent_instances.contract import AgentPermission, Refusal
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.spaces.contract import SpaceConflict, SpaceDenied
from deerflow.spaces.filesystem import FilesystemUnavailable


async def file_app(instances, monkeypatch, *, actor=ALICE, permissions=None, creation_id=None):
    from app.gateway.refusals import refusal_handler
    from app.gateway.routers.agent_home import router

    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, creation_id=creation_id)
    agents.execution_probe = AsyncMock(return_value=SimpleNamespace(execution="ready", reason=None))
    monkeypatch.setattr(agents.files, "quota", AsyncMock(return_value={"available_bytes": 128 << 20, "available_inodes": 100}))
    app = app_for(agents, authority, threads, monkeypatch, actor=actor, permissions=permissions)
    app.add_exception_handler(Refusal, refusal_handler)
    app.include_router(router)
    return app, agents, agent, authority, sf


@pytest.mark.asyncio
async def test_conversation_route_needs_use_on_that_conversation_and_space_routes_need_write(instances, monkeypatch):
    app, agents, agent, authority, sf = await file_app(instances, monkeypatch, actor=BOB)
    async with sf() as session, session.begin():
        session.add(AgentInstanceGrantRow(instance_id=agent.id, user_id="bob", permissions=int(AgentPermission.USE)))
    chat = await authority.create(actor=BOB, instance_id=agent.id, creation_id=operation())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        response = await client.post(f"/api/agent-instances/{agent.id}/conversations/{chat['thread_id']}/files", params={"operation_id": operation()}, files={"file": ("folder/report.pdf", b"given file", "application/pdf")})
        assert response.status_code == 201, response.text
        assert response.json()["path"] == "uploads/report.pdf" and response.json()["sandbox_path"] == "/mnt/user-data/uploads/report.pdf"
        assert response.json()["size"] == 10
        denied = await client.put(f"/api/agent-instances/{agent.id}/home/content", params={"path": "uploads/private", "operation_id": operation()}, content=b"denied")
        assert denied.status_code == 404 and denied.json()["detail"] == "Resource or file is unavailable"
        wrong = await client.post(f"/api/agent-instances/{agent.id}/conversations/{operation()}/files", params={"operation_id": operation()}, files={"file": ("report.pdf", b"denied")})
        assert wrong.status_code == 404 and wrong.json()["detail"] == "Agent conversation is unavailable"


@pytest.mark.asyncio
async def test_files_tab_put_edit_and_folder_actions_return_their_public_shapes(instances, monkeypatch):
    app, _, agent, _, _ = await file_app(instances, monkeypatch)
    prefix = f"/api/agent-instances/{agent.id}/home"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        folder = await client.post(prefix + "/files", json={"operation_id": operation(), "action": "mkdir", "path": "notes"})
        assert folder.status_code == 200 and folder.json() == {"complete": True, "path": "notes"}
        query = {"path": "notes/a.txt", "operation_id": operation()}
        first = await client.put(prefix + "/content", params=query, content=b"first")
        assert first.status_code == 200, first.text
        assert set(first.json()) == {"path", "size", "sha256", "deduplicated"}
        edit = await client.put(prefix + "/content", params={**query, "operation_id": operation(), "expected_sha256": first.json()["sha256"]}, content=b"second")
        assert edit.status_code == 200 and edit.json()["size"] == 6
        moved = await client.post(prefix + "/files", json={"operation_id": operation(), "action": "rename", "path": "notes/a.txt", "destination": "notes/b.txt"})
        assert moved.json() == {"complete": True, "path": "notes/b.txt"}
        removed = await client.post(prefix + "/files", json={"operation_id": operation(), "action": "remove", "path": "notes/b.txt"})
        assert removed.json() == {"complete": True, "path": "notes/b.txt"}
        extra = await client.post(prefix + "/files", json={"operation_id": operation(), "action": "mkdir", "path": "other", "actor": "forged"})
        assert extra.status_code == 422


@pytest.mark.asyncio
async def test_file_routes_are_refused_when_execution_is_unavailable(instances, monkeypatch):
    app, agents, agent, _, _ = await file_app(instances, monkeypatch)
    agents.execution_probe.return_value = SimpleNamespace(execution="unavailable", reason="operator-hold")
    prefix = f"/api/agent-instances/{agent.id}"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        responses = [
            await client.put(prefix + "/home/content", params={"operation_id": operation(), "path": "a"}, content=b"a"),
            await client.post(prefix + "/home/files", json={"operation_id": operation(), "path": "a", "action": "mkdir"}),
            await client.post(prefix + f"/conversations/{operation()}/files", params={"operation_id": operation()}, files={"file": ("a", b"a")}),
        ]
        assert all(r.status_code == 409 and r.json()["reason"] == "execution-unavailable" for r in responses)


@pytest.mark.asyncio
@pytest.mark.parametrize("name", [".", "..", "", "a\x00b", "a\x1fb", "é" * 128])
async def test_the_composer_route_refuses_an_invalid_part_name(instances, monkeypatch, name):
    app, _, agent, authority, _ = await file_app(instances, monkeypatch)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        # HTTPX quotes controls as literal percent sequences in its file helper.
        # Send the actual forbidden name bytes instead of a different filename.
        body = b'--filename\r\nContent-Disposition: form-data; name="file"; filename="' + name.encode("utf-8") + b'"\r\n\r\nrefused\r\n--filename--\r\n'
        response = await client.post(f"/api/agent-instances/{agent.id}/conversations/{chat['thread_id']}/files", params={"operation_id": operation()}, headers={"Content-Type": "multipart/form-data; boundary=filename"}, content=body)
    assert response.status_code == 400 and response.json()["reason"] == "invalid-name", response.text


@pytest.mark.asyncio
@pytest.mark.parametrize("multipart", [False, True])
async def test_file_over_64_mebibytes_is_refused_as_file_too_large(instances, monkeypatch, multipart):
    app, _, agent, authority, _ = await file_app(instances, monkeypatch)
    consumed = 0
    block = b"x" * (1 << 20)

    async def body():
        nonlocal consumed
        if multipart:
            yield b'--limit\r\nContent-Disposition: form-data; name="file"; filename="large"\r\n\r\n'
        for _ in range(100):
            consumed += 1
            yield block
        if multipart:
            yield b"\r\n--limit--\r\n"

    prefix = f"/api/agent-instances/{agent.id}"
    if multipart:
        chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
        path, headers = f"/conversations/{chat['thread_id']}/files", {"Content-Type": "multipart/form-data; boundary=limit"}
    else:
        path, headers = "/home/content", {}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        response = await client.request("POST" if multipart else "PUT", prefix + path, params={"operation_id": operation(), "path": "large"}, headers=headers, content=body())
    assert response.status_code == 413 and response.json()["reason"] == "file-too-large", response.text
    assert consumed == 65, "The route must stop reading as soon as the file exceeds its limit"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cause,status", [(SpaceDenied("private grant detail"), 403), (SpaceConflict("private operation detail"), 409), (FilesystemUnavailable("private filesystem detail"), 501), (ValueError("private validation detail"), 400)]
)
async def test_direct_file_routes_never_return_storage_exception_text(instances, monkeypatch, cause, status):
    app, agents, agent, authority, _ = await file_app(instances, monkeypatch)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    monkeypatch.setattr(agents.agent_files, "apply", AsyncMock(side_effect=cause))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        prefix = f"/api/agent-instances/{agent.id}"
        responses = [
            await client.put(prefix + "/home/content", params={"operation_id": operation(), "path": "safe"}, content=b"bytes"),
            await client.post(prefix + "/home/files", json={"operation_id": operation(), "path": "safe", "action": "mkdir"}),
            await client.post(prefix + f"/conversations/{chat['thread_id']}/files", params={"operation_id": operation()}, files={"file": ("safe", b"bytes")}),
        ]
    for response in responses:
        assert response.status_code == status, response.text
        assert "private" not in response.text and str(cause) not in response.text


@pytest.mark.asyncio
async def test_a_different_request_under_an_upload_id_returns_a_fixed_conflict(instances, monkeypatch):
    app, _, agent, _, _ = await file_app(instances, monkeypatch)
    key = operation()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        query = {"operation_id": key, "path": "uploads/file"}
        first = await client.put(f"/api/agent-instances/{agent.id}/home/content", params=query, content=b"first")
        assert first.status_code == 200
        conflict = await client.put(f"/api/agent-instances/{agent.id}/home/content", params=query, content=b"other")
    assert conflict.status_code == 409
    assert conflict.json() == {"detail": "This file request conflicts with the current state."}


@pytest.mark.asyncio
async def test_checking_an_unrecorded_operation_never_changes_the_current_file(instances, monkeypatch):
    app, _, agent, _, sf = await file_app(instances, monkeypatch)
    prefix = f"/api/agent-instances/{agent.id}/home"
    missing = operation()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        known = operation()
        saved = await client.put(prefix + "/operations/content", params={"path": "note", "operation_id": known}, content=b"current")
        assert saved.status_code == 200, saved.text
        for _ in range(2):
            status = await client.get(f"{prefix}/operations/{missing}")
            assert status.status_code == 200 and status.json() == {"status": "unknown"}
        status = await client.get(f"{prefix}/operations/{known}")
        assert status.json() == {"status": "complete", "file": saved.json()}
        async with sf() as session:
            rows = list(await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == agent.home_id)))
            assert len(rows) == 1 and rows[0].operation_id not in (missing, known)
        edited = await client.put(prefix + "/content", params={"path": "note", "operation_id": operation(), "expected_sha256": saved.json()["sha256"]}, content=b"updated")
        assert edited.status_code == 200, edited.text


@pytest.mark.asyncio
async def test_conversation_operation_lookup_is_confined_to_its_upload_and_current_authority(instances, monkeypatch):
    app, agents, agent, authority, sf = await file_app(instances, monkeypatch)
    chat = (await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation()))["thread_id"]
    other = (await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation()))["thread_id"]
    key = operation()
    prefix = f"/api/agent-instances/{agent.id}/conversations"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        lookup = f"{prefix}/{chat}/files/operations/{key}"
        options = {"params": {"filename": "sample.txt"}}
        # All writers choose exactly the public handle the target chat will use.
        for route in (f"{prefix}/{other}/files", f"{prefix}/{other}/files/operations"):
            saved = await client.post(route, params={"operation_id": key}, files={"file": ("sample.txt", b"content")})
            assert saved.status_code == 201, saved.text
            assert (await client.get(lookup, **options)).json() == {"status": "unknown"}
        home = f"/api/agent-instances/{agent.id}/home"
        saved = await client.put(home + "/operations/content", params={"operation_id": key, "path": "uploads/sample.txt"}, content=b"content")
        assert saved.status_code == 200
        assert (await client.get(lookup, **options)).json() == {"status": "unknown"}
        saved = await client.post(f"{prefix}/{chat}/files/operations", params={"operation_id": key}, files={"file": ("sample.txt", b"recorded in this conversation")})
        assert saved.status_code == 201, saved.text
        assert saved.json()["path"] == "uploads/sample (2).txt"
        assert (await client.get(lookup, **options)).json() == {"status": "complete", "file": saved.json()}
        assert (await client.get(f"{prefix}/{other}/files/operations/{key}", **options)).json()["file"]["path"] == "uploads/sample.txt"
        assert (await client.get(lookup, params={"filename": "different"})).json() == {"status": "unknown"}
        second = await create(agents, name="Second")
        second_chat = (await authority.create(actor=ALICE, instance_id=second.id, creation_id=operation()))["thread_id"]
        assert (await client.get(f"/api/agent-instances/{second.id}/conversations/{second_chat}/files/operations/{key}", **options)).json() == {"status": "unknown"}
        from app.gateway import deps
        from app.gateway.routers import agent_home

        async with sf() as session, session.begin():
            session.add(AgentInstanceGrantRow(instance_id=agent.id, user_id=BOB.subject_id, permissions=int(AgentPermission.USE | AgentPermission.INSPECT)))
        with monkeypatch.context() as alternate:
            alternate.setattr(deps, "get_current_user_from_request", AsyncMock(return_value=SimpleNamespace(id=BOB.subject_id, system_role="user")))
            assert (await client.get(lookup, **options)).json() == {"status": "unknown"}
        with monkeypatch.context() as rotated:
            rotated.setattr(agent_home, "existing_signing_key", lambda: "rotated-only-for-this-test")
            assert (await client.get(lookup, **options)).json() == {"status": "unknown"}
        async with sf() as session, session.begin():
            rows = list(await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == agent.home_id)))
            (row,) = (row for row in rows if row.result["value"]["path"] == "uploads/sample (2).txt")
            op = row.operation_id
            assert op != key and op not in saved.text
            row.phase, row.result = "pending", None
        agents.execution_probe = AsyncMock(side_effect=AssertionError("A status read cannot depend on executable storage"))
        assert (await client.get(lookup, **options)).json() == {"status": "pending"}
        for result in [None, {"resolution": "host-settled"}, {"resolution": "unknown-version"}]:
            async with sf() as session, session.begin():
                row = await session.get(SpaceFileOperationRow, (agent.home_id, op))
                row.phase, row.result = "failed", result
            assert (await client.get(lookup, **options)).json() == {"status": "nothing-changed" if result is None else "unconfirmed"}
        async with sf() as session, session.begin():
            row = await session.get(SpaceFileOperationRow, (agent.home_id, op))
            original = dict(row.request)
        for altered in ({"on_behalf_of": "another"}, {"action": "write"}, {"path": "private-manager-file"}):
            async with sf() as session, session.begin():
                row = await session.get(SpaceFileOperationRow, (agent.home_id, op))
                row.request = {**original, **altered}
            assert (await client.get(lookup, **options)).json() == {"status": "unknown"}
        async with sf() as session, session.begin():
            row = await session.get(SpaceFileOperationRow, (agent.home_id, op))
            row.request = original
            row.generation += 1
        assert (await client.get(lookup, **options)).json() == {"status": "unknown"}
        async with sf() as session, session.begin():
            await session.delete(await session.get(AgentInstanceGrantRow, (agent.id, ALICE.subject_id)))
        assert (await client.get(lookup, **options)).status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("action,cause", [("put", "workflow"), ("backup", "lifecycle")])
async def test_pending_home_writes_keep_the_typed_busy_cause(instances, monkeypatch, action, cause):
    app, agents, agent, authority, sf = await file_app(instances, monkeypatch)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    home = await agents.files.registry.get(actor=ALICE, space_id=agent.home_id)
    async with sf() as session, session.begin():
        session.add(SpaceFileOperationRow(space_id=agent.home_id, operation_id=operation(), actor_kind="nonhuman", actor_id=agent.principal.subject_id, generation=home.generation, phase="pending", request={"action": action}))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        prefix = f"/api/agent-instances/{agent.id}"
        responses = [
            await client.put(prefix + "/home/content", params={"operation_id": operation(), "path": "safe"}, content=b"bytes"),
            await client.post(prefix + "/home/files", json={"operation_id": operation(), "path": "safe", "action": "mkdir"}),
            await client.post(prefix + f"/conversations/{chat['thread_id']}/files", params={"operation_id": operation()}, files={"file": ("safe", b"bytes")}),
        ]
    for response in responses:
        assert response.status_code == 409, response.text
        assert response.json()["reason"] == "storage-busy" and response.json()["cause"] == cause
        assert "unconfirmed outcome" not in response.text
