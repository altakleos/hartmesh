"""Offline ownership and command contracts for Gateway storage mounting."""

import asyncio
import json
import logging
import re
import subprocess
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml
from test_agent_instances import instances as instances
from test_storage_spaces_startup import startup as startup

from app.gateway import storage_spaces as ss

IMAGE = "example.invalid/backend@sha256:" + "1" * 64
KEYS = ("HARTMESH_USERS", "HARTMESH_USER_STORAGE_GIB", "HARTMESH_AI_ASSISTANTS_PER_USER", "HARTMESH_AI_ASSISTANT_STORAGE_GIB", "HARTMESH_AI_EMPLOYEES", "HARTMESH_AI_EMPLOYEE_STORAGE_GIB", "HARTMESH_COMPANY_STORAGE_GIB")


@pytest.fixture(autouse=True)
def mount_environment(monkeypatch):
    monkeypatch.delenv("HARTMESH_STORAGE_INIT_IMAGE", raising=False)
    for key in KEYS:
        monkeypatch.delenv(key, raising=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("verb", ["verify", "prepare", "fstab-sync", "resize", "forget", "quiesce", "status", "mount-all extra", ""])
async def test_the_gateway_admits_mount_all_only(monkeypatch, verb):
    def forbidden(*_args, **_kwargs):
        pytest.fail("an invalid verb must start no subprocess")

    monkeypatch.setattr(subprocess, "run", forbidden)
    with pytest.raises(ValueError):
        await ss.run_storage_init(verb)


@pytest.mark.asyncio
async def test_storage_init_is_not_run_when_the_image_key_is_unset(monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *_a, **_k: pytest.fail("unset image must not call Docker"))
    result = await ss.run_storage_init("mount-all")
    assert result["skipped"] is True


@pytest.mark.asyncio
async def test_gateway_storage_init_command_matches_the_compose_service(monkeypatch):
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)
    monkeypatch.setenv("HARTMESH_DATA_DIR", "/srv/hartmesh")
    for index, key in enumerate(KEYS):
        monkeypatch.setenv(key, str(index))
    calls = []
    loop_thread = threading.get_ident()

    def run(command, **kwargs):
        assert threading.get_ident() != loop_thread
        assert kwargs == {"capture_output": True, "text": True, "timeout": 60, "check": False}
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, json.dumps({"command": "mount-all", "returncode": 0, "note": "complete"}), "")

    monkeypatch.setattr(subprocess, "run", run)
    assert (await ss.run_storage_init("mount-all"))["returncode"] == 0
    (command,) = calls
    name = command[command.index("--name") + 1]
    assert re.fullmatch(r"hartmesh-storage-init-mount-all-[0-9a-f]{8}", name)
    assert command == [
        "docker",
        "run",
        "--rm",
        "--name",
        name,
        "--label",
        "hartmesh.storage.init=mount-all",
        "--privileged",
        "--pid",
        "host",
        "--network",
        "none",
        "--memory",
        "128m",
        "--memory-swap",
        "128m",
        "--pids-limit",
        "64",
        "--workdir",
        "/app/backend",
        "--env",
        "PYTHONPATH=.",
        "--env",
        "UV_CACHE_DIR=/tmp/uv-cache",
        "--env",
        "HARTMESH_DATA_DIR=/srv/hartmesh",
        *(part for index, key in enumerate(KEYS) for part in ("--env", f"{key}={index}")),
        "--mount",
        "type=bind,src=/srv/hartmesh/spaces,dst=/srv/hartmesh/spaces,bind-propagation=rslave",
        "--entrypoint",
        "uv",
        IMAGE,
        "run",
        "--no-sync",
        "python",
        "-m",
        "app.gateway.storage_init",
        "mount-all",
    ]
    service = yaml.safe_load((Path(__file__).resolve().parents[2] / "deploy/compose/compose.yaml").read_text(encoding="utf-8"))["services"]["storage-init"]
    assert command[-6:-1] == service["entrypoint"][1:]
    assert command[command.index("--memory") + 1] == service["mem_limit"]
    assert set(service["environment"]) == {"PYTHONPATH", "UV_CACHE_DIR", "HARTMESH_DATA_DIR", *KEYS}


@pytest.mark.asyncio
@pytest.mark.parametrize("output", ["", "oops", "[]", "null", "42", "{}", '{"returncode":0,"note":"ok"} {}', '{"returncode":true,"note":"ok"}'])
async def test_storage_init_requires_one_result_object(monkeypatch, output):
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)
    monkeypatch.setattr(subprocess, "run", lambda command, **_kwargs: subprocess.CompletedProcess(command, 0, output, "private diagnostic"))
    result = await ss.run_storage_init("mount-all")
    assert result == {"command": "mount-all", "returncode": 2, "note": "storage-init printed no result"}


@pytest.mark.asyncio
async def test_a_call_past_its_limit_removes_its_container_and_returns_returncode_2(monkeypatch):
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        if command[1] == "run":
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    result = await ss.run_storage_init("mount-all")
    assert result == {"command": "mount-all", "returncode": 2, "timed_out": True, "note": "storage-init mount-all did not finish in 60 s"}
    name = calls[0][0][calls[0][0].index("--name") + 1]
    assert calls[1][0] == ["docker", "rm", "--force", name]
    assert calls[1][1]["timeout"] == 5 and len(calls) == 2


@pytest.mark.asyncio
async def test_failed_cleanup_is_retained_when_docker_cannot_confirm_absence(monkeypatch):
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        if command[1] == "run":
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        return subprocess.CompletedProcess(command, 1, "", "daemon unavailable")

    monkeypatch.setattr(subprocess, "run", run)
    result = await ss.run_storage_init("mount-all")
    assert result["cleanup_pending"] == calls[0][calls[0].index("--name") + 1]
    assert calls[1] == ["docker", "rm", "--force", result["cleanup_pending"]]
    assert len(calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("code,error", [(0, None), (1, "slot-id-only refusal"), (2, None)])
async def test_every_call_writes_one_journal_line_with_ids_only(monkeypatch, caplog, code, error):
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)
    document = {"command": "mount-all", "returncode": code, "note": "1 space: slot-id"}
    if error:
        document["error"] = error
    monkeypatch.setattr(subprocess, "run", lambda command, **_kwargs: subprocess.CompletedProcess(command, code, json.dumps(document), "private name never logged"))
    caplog.set_level(logging.INFO, logger=ss.__name__)
    assert await ss.run_storage_init("mount-all") == document
    (record,) = [r for r in caplog.records if "storage: storage-init" in r.message]
    expected_note = document["note"] + (f": {error}" if error else "")
    assert re.fullmatch(rf"storage: storage-init mount-all exit {code} in [0-9.]+ s \({re.escape(expected_note)}\)", record.message)
    assert "private name" not in caplog.text


@pytest.mark.asyncio
async def test_a_spawn_failure_is_a_result_without_aborting_startup(monkeypatch):
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)

    def run(*_args, **_kwargs):
        raise FileNotFoundError("private path")

    monkeypatch.setattr(subprocess, "run", run)
    result = await ss.run_storage_init("mount-all")
    assert result["returncode"] == 2 and result["error"] == "FileNotFoundError"
    assert "private path" not in json.dumps(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [0, 1, 2])
async def test_mount_all_is_first_and_a_completed_failure_does_not_stop_startup(startup, monkeypatch, code):
    ss, app, config, sf, _catalog, _adapter, _agents = startup
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)
    order = []

    async def mount(verb):
        order.append(verb)
        return {"returncode": code, "note": "finished"}

    read = ss._read_pool

    def read_pool(settings):
        order.append("inventory")
        return read(settings)

    monkeypatch.setattr(ss, "run_storage_init", mount)
    monkeypatch.setattr(ss, "_read_pool", read_pool)
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    assert order[:2] == ["mount-all", "inventory"]
    assert (await ss.storage_state(app)).state == "ready"


@pytest.mark.asyncio
async def test_pending_mount_is_owned_without_duplicate_startup_or_recheck(startup, monkeypatch):
    ss, app, config, sf, _catalog, adapter, _agents = startup
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)
    monkeypatch.setattr(ss, "STORAGE_INIT_VERBS", {"mount-all": 0.01})
    monkeypatch.setattr(ss, "RECHECK_SECONDS", 3600)
    gate = asyncio.Event()
    calls = []

    async def mount(verb):
        calls.append(verb)
        await gate.wait()
        return {"returncode": 0, "note": "complete"}

    monkeypatch.setattr(ss, "run_storage_init", mount)
    try:
        await ss.initialize_storage_spaces(app, config, session_factory=sf)
        task = app.state.agent_execution.mount_task
        assert task is not None and not task.done() and not task.cancelled()
        assert app.state.agent_execution.mount_pending is True
        assert (await ss.storage_state(app)).reason == "admission-failed"
        assert app.state.agent_execution.storage_text == "storage-init mount-all did not finish in 60 s"
        assert not adapter.await_count
        await ss.storage_recheck(app)
        assert calls == ["mount-all"]
        gate.set()
        await task
        await ss.storage_recheck(app)
        assert (await ss.storage_state(app)).state == "ready"
        assert calls == ["mount-all"] and adapter.await_count == 1
    finally:
        gate.set()


@pytest.mark.asyncio
async def test_successful_mount_retry_reverifies_pinned_slots_and_clears_errors(startup, monkeypatch):
    from deerflow.spaces.backings import BackingUnavailable

    ss, app, config, sf, catalog, _adapter, _agents = startup
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)
    monkeypatch.setattr(ss, "RECHECK_SECONDS", 3600)
    mount = AsyncMock(return_value={"returncode": 2, "timed_out": True, "note": "storage-init mount-all did not finish in 60 s"})
    monkeypatch.setattr(ss, "run_storage_init", mount)
    verify, recovered = catalog.verify, False
    slot = next(iter(catalog.volumes))

    def verify_slot(value, **kwargs):
        if value == slot and not recovered:
            raise BackingUnavailable("not mounted")
        return verify(value, **kwargs)

    monkeypatch.setattr(catalog, "verify", verify_slot)
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    assert slot in app.state.agent_execution.unverified
    recovered = True
    mount.return_value = {"returncode": 0, "note": "mounted"}
    app.state.agent_execution.mount_retry_at = 0
    monkeypatch.setattr(ss.PreparedVolumeCatalog, "from_manifest", lambda _p: pytest.fail("retry must not reload inventory"))
    await ss.storage_recheck(app)
    assert (await ss.storage_state(app)).state == "ready"
    assert app.state.agent_execution.unverified == {} and app.state.storage_spaces.catalog is catalog


@pytest.mark.asyncio
@pytest.mark.parametrize("failure,reason", [(FileNotFoundError("missing"), "storage-pool-missing"), (ValueError("malformed"), "inventory-unreadable")])
async def test_mount_timeout_cannot_replace_a_stronger_pool_failure(startup, monkeypatch, failure, reason):
    ss, app, config, sf, _catalog, adapter, _agents = startup
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)
    mount = AsyncMock(return_value={"returncode": 2, "timed_out": True, "note": "storage-init mount-all did not finish in 60 s"})
    monkeypatch.setattr(ss, "run_storage_init", mount)

    def unreadable(_path):
        raise failure

    monkeypatch.setattr(ss.PreparedVolumeCatalog, "from_manifest", unreadable)
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    assert (await ss.storage_state(app)).reason == reason
    mount.return_value = {"returncode": 0, "note": "complete"}
    app.state.agent_execution.mount_retry_at = 0
    await ss.storage_recheck(app)
    assert (await ss.storage_state(app)).reason == reason
    assert not app.state.storage_spaces.catalog.volumes
    assert mount.await_count == 1 and not adapter.await_count


@pytest.mark.asyncio
async def test_cancellation_drains_docker_creation_and_cleanup(monkeypatch):
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)
    entered, release = threading.Event(), threading.Event()
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        if command[1] == "run":
            entered.set()
            assert release.wait(timeout=5)
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    task = asyncio.create_task(ss.run_storage_init("mount-all"))
    try:
        for _ in range(100):
            if entered.is_set():
                break
            await asyncio.sleep(0.01)
        assert entered.is_set()
        task.cancel()
        await asyncio.sleep(0.01)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert calls[-1][1:3] == ["rm", "--force"]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_stalled_cleanup_keeps_ownership_blocks_new_mounts_and_drains_at_shutdown(startup, monkeypatch):
    ss, app, config, sf, _catalog, adapter, _agents = startup
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)
    monkeypatch.setattr(ss, "RECHECK_SECONDS", 3600)
    mount = AsyncMock(return_value={"returncode": 2, "timed_out": True, "cleanup_pending": "hartmesh-storage-init-mount-all-12345678", "note": "storage-init mount-all did not finish in 60 s"})
    monkeypatch.setattr(ss, "run_storage_init", mount)
    entered, release = threading.Event(), threading.Event()

    def remove(name):
        assert name == "hartmesh-storage-init-mount-all-12345678"
        entered.set()
        assert release.wait(timeout=5)
        return False

    monkeypatch.setattr(ss, "_remove_storage_init", remove)
    closing = None
    try:
        await ss.initialize_storage_spaces(app, config, session_factory=sf)
        for _ in range(100):
            if entered.is_set():
                break
            await asyncio.sleep(0.01)
        assert entered.is_set()
        owned = app.state.agent_execution.mount_cleanup_task
        assert owned is not None and not owned.done()
        await ss.storage_recheck(app)
        assert mount.await_count == 1 and not adapter.await_count
        closing = asyncio.create_task(ss.close_storage_spaces(app))
        await asyncio.sleep(0.01)
        assert not closing.done() and not owned.cancelled()
        release.set()
        await closing
        assert owned.done() and mount.await_count == 1
    finally:
        release.set()
        if closing is not None:
            await asyncio.gather(closing, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_startup_lifecycle_drains_its_pending_mount(startup, monkeypatch):
    ss, app, config, sf, _catalog, _adapter, _agents = startup
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)
    entered, release = asyncio.Event(), asyncio.Event()

    async def mount(verb):
        assert verb == "mount-all"
        entered.set()
        await release.wait()
        return {"returncode": 0, "note": "complete"}

    monkeypatch.setattr(ss, "run_storage_init", mount)

    async def start():
        async with ss.storage_spaces_lifecycle(app):
            await ss.initialize_storage_spaces(app, config, session_factory=sf)

    task = asyncio.create_task(start())
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        owned = app.state.agent_execution.mount_task
        task.cancel()
        await asyncio.sleep(0.01)
        assert not task.done() and not owned.cancelled()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert owned.done()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


def test_storage_key_mirror_is_platform_neutral_and_matches_the_operator_cli():
    import ast

    gateway = Path(__file__).resolve().parents[1] / "app/gateway"
    source = ast.parse((gateway / "storage_init.py").read_text(encoding="utf-8"))
    keys = next(node.value for node in source.body if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "KEYS" for t in node.targets))
    assert tuple(ast.literal_eval(key) for key in keys.keys) == ss.STORAGE_KEYS == KEYS
    wiring = ast.parse((gateway / "storage_spaces.py").read_text(encoding="utf-8"))
    assert not any(isinstance(node, ast.ImportFrom) and node.module == "app.gateway.storage_init" for node in wiring.body)


@pytest.mark.asyncio
async def test_shutdown_retries_retained_cleanup_even_when_no_worker_is_pending(monkeypatch):
    name = "hartmesh-storage-init-mount-all-12345678"
    status = ss.ExecutionStatus(mount_cleanup=name, mount_pending=True)
    app = SimpleNamespace(state=SimpleNamespace(agent_execution=status))
    calls = []
    monkeypatch.setattr(ss, "_remove_storage_init", lambda owned: calls.append(owned) or False)
    await ss.close_storage_spaces(app)
    assert calls == [name]
    assert status.mount_cleanup_task.done() and status.mount_cleanup == name


@pytest.mark.asyncio
async def test_shutdown_consumes_cleanup_uncertainty_from_the_mount_result(monkeypatch):
    name = "hartmesh-storage-init-mount-all-12345678"

    async def finished():
        return {"returncode": 2, "cleanup_pending": name}

    task = asyncio.create_task(finished())
    await task
    status = ss.ExecutionStatus(mount_task=task, mount_pending=True)
    app = SimpleNamespace(state=SimpleNamespace(agent_execution=status))
    calls = []
    monkeypatch.setattr(ss, "_remove_storage_init", lambda owned: calls.append(owned) or True)
    await ss.close_storage_spaces(app)
    assert calls == [name] and status.mount_cleanup is None


@pytest.mark.asyncio
async def test_shutdown_consumes_successful_pending_cleanup_without_removing_again(monkeypatch):
    name = "hartmesh-storage-init-mount-all-12345678"
    release = asyncio.Event()

    async def finish():
        await release.wait()
        return True

    owned = asyncio.create_task(finish())
    status = ss.ExecutionStatus(mount_cleanup=name, mount_cleanup_task=owned, mount_pending=True)
    app = SimpleNamespace(state=SimpleNamespace(agent_execution=status))
    monkeypatch.setattr(ss, "_remove_storage_init", lambda _name: pytest.fail("acknowledged removal must not be repeated"))
    closing = asyncio.create_task(ss.close_storage_spaces(app))
    try:
        await asyncio.sleep(0.01)
        assert not closing.done() and not owned.cancelled()
        release.set()
        await closing
        assert status.mount_cleanup is None
    finally:
        release.set()
        await asyncio.gather(closing, return_exceptions=True)


@pytest.mark.asyncio
async def test_pending_mount_does_not_start_stale_metadata_verification(startup, monkeypatch):
    ss, app, config, sf, _catalog, _adapter, _agents = startup
    monkeypatch.setenv("HARTMESH_STORAGE_INIT_IMAGE", IMAGE)
    monkeypatch.setattr(ss, "STORAGE_INIT_VERBS", {"mount-all": 0.01})
    monkeypatch.setattr(ss, "RECHECK_SECONDS", 3600)
    release = asyncio.Event()

    async def mount(_verb):
        await release.wait()
        return {"returncode": 0, "note": "complete"}

    monkeypatch.setattr(ss, "run_storage_init", mount)
    metadata = AsyncMock(return_value=({}, {}))
    monkeypatch.setattr(ss, "_load_metadata", metadata)
    try:
        await ss.initialize_storage_spaces(app, config, session_factory=sf)
        assert app.state.agent_execution.mount_pending
        assert app.state.agent_execution.metadata_task is None and not metadata.await_count
    finally:
        release.set()
