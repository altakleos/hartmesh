"""Direct host file writes on real SQL/confined-directory test adapters."""

import hashlib
import io
import os
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from _storage_spaces_test_support import ALICE, operation
from sqlalchemy import func, select
from test_agent_instances import create
from test_agent_instances import instances as instances

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.contract import SpaceConflict
from deerflow.spaces.filesystem import FileConflict, SpaceFull


async def setup(instances, monkeypatch):
    agents, files, sf, _ = instances
    agent = await create(agents)
    # This adapter tests admission/math; only CI qualifies native capacity.
    monkeypatch.setattr(files, "quota", AsyncMock(return_value={"available_bytes": 8 << 20, "available_inodes": 100}))
    return agents, agent, files, sf


async def put(agents, agent, content=b"document", **kwargs):
    options = {"instance": agent, "requester": ALICE, "operation_id": operation(), "action": "put", "path": "uploads/report.pdf", "content": io.BytesIO(content), "size": len(content)}
    options.update(kwargs)
    return await agents.agent_files.apply(**options)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["active", "fenced"])
async def test_apply_writes_on_the_host_as_the_agent_whether_or_not_a_workspace_runs(instances, monkeypatch, phase):
    agents, agent, files, sf = await setup(instances, monkeypatch)
    home = await files.registry.get(actor=agent.principal, space_id=agent.home_id)
    attachment = operation()
    async with sf() as session, session.begin():
        session.add(SpaceAttachmentRow(id=attachment, incarnation=operation(), actor_kind="nonhuman", actor_id=agent.principal.subject_id, host_id="fixture", container_id="a" * 64, phase=phase))
        await session.flush()
        session.add(SpaceMountRow(attachment_id=attachment, space_id=agent.home_id, generation=home.generation, alias="home", writable=1))

    class NoWorkspaceCalls:
        def __getattr__(self, name):
            raise AssertionError("A host file write must not inspect a workspace")

    agents.residents = NoWorkspaceCalls()
    result = await put(agents, agent)
    assert (result.path, result.size, result.sha256, result.deduplicated) == ("uploads/report.pdf", 8, hashlib.sha256(b"document").hexdigest(), False)
    assert await files.read(actor=agent.principal, space_id=agent.home_id, path=result.path, max_bytes=20) == b"document"
    async with sf() as session:
        row = await session.scalar(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == agent.home_id))
        assert (row.actor_kind, row.actor_id, row.phase) == ("nonhuman", agent.principal.subject_id, "complete")


@pytest.mark.asyncio
@pytest.mark.parametrize("name,expected", [("report.pdf", "report (2).pdf"), ("archive.tar.gz", "archive.tar (2).gz"), (".env", ".env (2)")])
async def test_put_keeps_an_existing_name_and_saves_as_the_next_free_numbered_name(instances, monkeypatch, name, expected):
    agents, agent, files, _ = await setup(instances, monkeypatch)
    await put(agents, agent, b"old", path="uploads/" + name)
    result = await put(agents, agent, b"new", path="uploads/" + name)
    assert result.path == "uploads/" + expected and not result.deduplicated
    assert await files.read(actor=agent.principal, space_id=agent.home_id, path="uploads/" + name, max_bytes=20) == b"old"


@pytest.mark.asyncio
async def test_put_of_the_same_size_and_sha256_is_returned_as_deduplicated(instances, monkeypatch):
    agents, agent, _, _ = await setup(instances, monkeypatch)
    first = await put(agents, agent)
    again = await put(agents, agent)
    assert again.path == first.path and again.sha256 == first.sha256 and again.deduplicated


@pytest.mark.asyncio
async def test_each_apply_records_one_operation_row_with_on_behalf_of(instances, monkeypatch):
    agents, agent, _, sf = await setup(instances, monkeypatch)
    for action, path, extra in [("mkdir", "folder", {}), ("put", "folder/file", {"content": io.BytesIO(b"abc"), "size": 3}), ("rename", "folder/file", {"destination": "folder/new"}), ("remove", "folder/new", {})]:
        await agents.agent_files.apply(instance=agent, requester=ALICE, operation_id=operation(), action=action, path=path, **extra)
    async with sf() as session:
        rows = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == agent.home_id))).all()
        assert len(rows) == 4
        for row in rows:
            assert row.actor_id == agent.principal.subject_id and row.request["on_behalf_of"] == "alice" and row.phase == "complete"
            assert set(row.request) == {"action", "path", "destination", "sha256", "size", "on_behalf_of"}


@pytest.mark.asyncio
async def test_expected_sha256_mismatch_answers_file_changed(instances, monkeypatch):
    agents, agent, files, _ = await setup(instances, monkeypatch)
    from deerflow.agent_instances.agent_files import AgentFileRefusal

    first = await put(agents, agent)
    with pytest.raises(AgentFileRefusal) as error:
        await put(agents, agent, b"changed", expected_sha256="0" * 64)
    assert error.value.reason == "file-changed" and error.value.status == 409
    assert await files.read(actor=agent.principal, space_id=agent.home_id, path=first.path, max_bytes=20) == b"document"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "action,path,destination,reason",
    [("put", ".agent-skill-packages/a", None, "reserved-path"), ("rename", "uploads/a", ".agent-skill-packages/a", "reserved-path"), ("put", "../a", None, "invalid-name"), ("put", "a", None, "agent-not-active")],
)
async def test_reserved_path_and_inactive_agent_are_refused(instances, monkeypatch, action, path, destination, reason):
    agents, agent, _, _ = await setup(instances, monkeypatch)
    from deerflow.agent_instances.agent_files import AgentFileRefusal

    if reason == "agent-not-active":
        agent = replace(agent, status="suspended")
    with pytest.raises(AgentFileRefusal) as error:
        await put(agents, agent, action=action, path=path, destination=destination)
    assert error.value.reason == reason


@pytest.mark.asyncio
@pytest.mark.parametrize("quota", [{"available_bytes": 1 << 20, "available_inodes": 2}, {"available_bytes": 8 << 20, "available_inodes": 1}])
async def test_a_write_that_does_not_fit_is_refused_as_files_full_before_the_operation_row(instances, monkeypatch, quota):
    agents, agent, files, sf = await setup(instances, monkeypatch)
    from deerflow.agent_instances.agent_files import AgentFileRefusal

    files.quota.return_value = quota
    with pytest.raises(AgentFileRefusal) as error:
        await put(agents, agent)
    assert error.value.reason == "files-full" and error.value.status == 507
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(SpaceFileOperationRow)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cause,reason",
    [
        (BackingUnavailable("private backing detail"), "file-write-failed"),
        (SpaceConflict("Stale space generation"), "file-write-failed"),
        (SpaceFull("private host detail"), "files-full"),
        (FileNotFoundError("private host path"), "not-found"),
        (FileConflict("Directory is not empty"), "directory-not-empty"),
    ],
)
async def test_backing_unavailable_and_a_stale_generation_answer_file_write_failed(instances, monkeypatch, cause, reason):
    agents, agent, files, _ = await setup(instances, monkeypatch)
    from deerflow.agent_instances.agent_files import AgentFileRefusal

    monkeypatch.setattr(files, "put_unique", AsyncMock(side_effect=cause))
    with pytest.raises(AgentFileRefusal) as error:
        await put(agents, agent)
    assert error.value.reason == reason and "private" not in str(error.value)


@pytest.mark.asyncio
async def test_a_repeated_operation_id_returns_the_recorded_result_and_writes_nothing(instances, monkeypatch):
    agents, agent, files, sf = await setup(instances, monkeypatch)
    from deerflow.agent_instances.agent_files import AgentFileRefusal
    from deerflow.agent_instances.contract import ResidentFilesBusy

    key = operation()
    first = await put(agents, agent, operation_id=key)
    monkeypatch.setattr(files, "put_unique", AsyncMock(side_effect=AssertionError("replay wrote bytes")))
    assert await put(agents, agent, operation_id=key) == first
    with pytest.raises(SpaceConflict, match="different request"):
        await put(agents, agent, b"different", operation_id=key)
    async with sf() as session, session.begin():
        row = await session.get(SpaceFileOperationRow, (agent.home_id, key))
        row.phase, row.result = "failed", {"reason": "file-changed"}
    with pytest.raises(AgentFileRefusal) as error:
        await put(agents, agent, operation_id=key)
    assert error.value.reason == "file-changed"
    async with sf() as session, session.begin():
        (await session.get(SpaceFileOperationRow, (agent.home_id, key))).phase = "pending"
    with pytest.raises(ResidentFilesBusy) as error:
        await put(agents, agent, operation_id=key)
    assert error.value.extra["cause"] == "workflow"


@pytest.mark.asyncio
@pytest.mark.parametrize("action,cause", [("backup", "lifecycle"), ("put", "workflow")])
async def test_apply_answers_storage_busy_with_its_cause_for_a_lifecycle_or_writer_row(instances, monkeypatch, action, cause):
    agents, agent, files, sf = await setup(instances, monkeypatch)
    from deerflow.agent_instances.contract import ResidentFilesBusy

    home = await files.registry.get(actor=agent.principal, space_id=agent.home_id)
    async with sf() as session, session.begin():
        session.add(SpaceFileOperationRow(space_id=agent.home_id, operation_id=operation(), actor_kind="nonhuman", actor_id=agent.principal.subject_id, generation=home.generation, phase="pending", request={"action": action}))
    with pytest.raises(ResidentFilesBusy) as error:
        await put(agents, agent)
    assert error.value.extra["cause"] == cause


@pytest.mark.asyncio
async def test_body_size_and_operation_identity_are_validated_before_writing(instances, monkeypatch):
    agents, agent, _, _ = await setup(instances, monkeypatch)
    for options in ({"size": 1}, {"size": -1}, {"size": True}, {"operation_id": "invalid"}, {"destination": "other"}):
        with pytest.raises(ValueError):
            await put(agents, agent, **options)
    from deerflow.agent_instances.agent_files import AgentFileRefusal

    with pytest.raises(AgentFileRefusal) as error:
        await put(agents, agent, size=(64 << 20) + 1)
    assert error.value.reason == "file-too-large"


async def backing_root(instances, agent):
    async with instances[2]() as session:
        binding = await session.scalar(select(SpaceBackingRow).where(SpaceBackingRow.space_id == agent.home_id))
    return instances[3].verified[binding.slot_id].data_path


@pytest.mark.asyncio
async def test_a_directory_at_the_requested_name_uses_the_next_free_name(instances, monkeypatch):
    agents, agent, files, _ = await setup(instances, monkeypatch)
    root = await backing_root(instances, agent)
    (root / "uploads").mkdir(exist_ok=True)
    (root / "uploads/report.pdf").mkdir()
    answer = await put(agents, agent)
    assert answer.path == "uploads/report (2).pdf" and not answer.deduplicated
    assert (root / "uploads/report.pdf").is_dir()
    assert await files.read(actor=agent.principal, space_id=agent.home_id, path=answer.path, max_bytes=20) == b"document"


@pytest.mark.asyncio
async def test_dedup_rechecks_the_named_inode_after_hashing_an_open_file(instances, monkeypatch):
    agents, agent, _, _ = await setup(instances, monkeypatch)
    await put(agents, agent)
    root = await backing_root(instances, agent)
    original = root / "uploads/report.pdf"
    replacement = root / "uploads/native.pdf"
    replacement.write_bytes(b"native replacement")
    identity = original.stat()
    fstat, read = os.fstat, os.read
    finished_read = replaced = False

    def observed_read(fd, size):
        nonlocal finished_read
        value = read(fd, size)
        metadata = fstat(fd)
        if not value and (metadata.st_dev, metadata.st_ino) == (identity.st_dev, identity.st_ino):
            finished_read = True
        return value

    def replace_after_snapshot(fd):
        nonlocal replaced
        metadata = fstat(fd)
        if finished_read and not replaced and (metadata.st_dev, metadata.st_ino) == (identity.st_dev, identity.st_ino):
            # The snapshot is real. The native replacement lands immediately
            # after it, so unchanged open-FD snapshots alone prove too little.
            os.replace(replacement, original)
            replaced = True
        return metadata

    monkeypatch.setattr(os, "read", observed_read)
    monkeypatch.setattr(os, "fstat", replace_after_snapshot)
    answer = await put(agents, agent)
    assert replaced and not answer.deduplicated
    assert answer.path == "uploads/report (2).pdf"
    assert original.read_bytes() == b"native replacement"


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["put", "mkdir", "rename", "remove"])
async def test_mutations_refuse_a_symlink_in_any_parent_component(instances, monkeypatch, action):
    agents, agent, _, _ = await setup(instances, monkeypatch)
    from deerflow.agent_instances.agent_files import AgentFileRefusal

    root = await backing_root(instances, agent)
    (root / "folder").mkdir()
    (root / "folder/old").write_bytes(b"old")
    (root / "alias").symlink_to("folder", target_is_directory=True)
    options = {"path": "alias/old"}
    if action == "put":
        options.update(content=io.BytesIO(b"new"), size=3, expected_sha256=hashlib.sha256(b"old").hexdigest())
    elif action == "mkdir":
        options["path"] = "alias/new"
    elif action == "rename":
        options["destination"] = "renamed"
    with pytest.raises(AgentFileRefusal) as error:
        await agents.agent_files.apply(instance=agent, requester=ALICE, operation_id=operation(), action=action, **options)
    assert error.value.reason == "invalid-name"
    assert (root / "folder/old").read_bytes() == b"old"


@pytest.mark.asyncio
async def test_edit_replay_keeps_the_exact_attributed_request(instances, monkeypatch):
    agents, agent, files, sf = await setup(instances, monkeypatch)
    first = await put(agents, agent)
    key = operation()
    changed = await put(agents, agent, b"edited", operation_id=key, expected_sha256=first.sha256)
    assert changed.path == first.path and changed.sha256 == hashlib.sha256(b"edited").hexdigest()
    assert await put(agents, agent, b"edited", operation_id=key, expected_sha256=first.sha256) == changed
    assert await files.read(actor=agent.principal, space_id=agent.home_id, path=changed.path, max_bytes=20) == b"edited"
    async with sf() as session:
        row = await session.get(SpaceFileOperationRow, (agent.home_id, key))
        assert row.request == {"action": "write", "path": changed.path, "destination": None, "sha256": changed.sha256, "size": 6, "on_behalf_of": "alice"}


@pytest.mark.asyncio
async def test_all_99_occupied_names_are_refused_without_overwriting(instances, monkeypatch):
    agents, agent, _, sf = await setup(instances, monkeypatch)
    from deerflow.agent_instances.agent_files import AgentFileRefusal

    root = await backing_root(instances, agent)
    uploads = root / "uploads"
    uploads.mkdir(exist_ok=True)
    for number in range(1, 100):
        (uploads / ("report.pdf" if number == 1 else f"report ({number}).pdf")).write_bytes(b"old")
    with pytest.raises(AgentFileRefusal) as error:
        await put(agents, agent)
    assert error.value.reason == "name-collision"
    assert len(list(uploads.iterdir())) == 99
    assert all(path.read_bytes() == b"old" for path in uploads.iterdir())
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "failed")) == 1


@pytest.mark.asyncio
async def test_a_maximum_length_name_collision_is_settled_without_a_pending_operation(instances, monkeypatch):
    from deerflow.agent_instances.agent_files import AgentFileRefusal

    agents, agent, _, sf = await setup(instances, monkeypatch)
    name = "é" * 125 + ".txtx"
    assert len(name.encode("utf-8")) == 255
    await put(agents, agent, b"old", path="uploads/" + name)
    with pytest.raises(AgentFileRefusal) as error:
        await put(agents, agent, b"new", path="uploads/" + name)
    assert error.value.reason == "name-collision"
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending")) == 0


@pytest.mark.asyncio
async def test_an_overlong_path_component_is_refused_before_an_operation_is_recorded(instances, monkeypatch):
    from deerflow.agent_instances.agent_files import AgentFileRefusal

    agents, agent, _, sf = await setup(instances, monkeypatch)
    with pytest.raises(AgentFileRefusal) as error:
        await put(agents, agent, path="uploads/" + "é" * 128)
    assert error.value.reason == "invalid-name"
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(SpaceFileOperationRow)) == 0
