"""Complete Home output observations with real SQL and confined directory fixtures."""

import asyncio
import hashlib
import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_agent_instances import instances as instances
from test_instance_output_presentation import prepared

from deerflow.agent_instances.runtime import InstanceSandboxProvider, execution_scope

pytestmark = pytest.mark.asyncio


async def test_instance_output_snapshot_observes_the_admitted_home_only(instances):
    from deerflow.agent_instances.presentation import capture_instance_outputs

    execution, _, root = await prepared(instances)
    (root / "uploads/private.txt").write_text("not output", encoding="utf-8")
    (root / "outputs/.tool-results").mkdir()
    (root / "outputs/.tool-results/internal.txt").write_text("process feedback", encoding="utf-8")
    with execution_scope(execution) as environment:
        environment.provider = InstanceSandboxProvider(execution, SimpleNamespace(id="fixture"))
        snapshot = await capture_instance_outputs(execution, observe_roots=True)
    assert set(snapshot.files) == {"/mnt/user-data/outputs/result.txt"}
    file = snapshot.files["/mnt/user-data/outputs/result.txt"]
    assert file.sha256 == hashlib.sha256(b"real output").hexdigest()
    assert file.text is None and file.text_path is None and snapshot.text_cache_dir is None
    assert not snapshot.truncated
    assert [(root.name, root.virtual_prefix, root.state) for root in snapshot.root_observations] == [("outputs", "/mnt/user-data/outputs", "opened")]
    assert ".tool-results" in snapshot.scan_excluded_dir_names


@pytest.mark.parametrize("kind", ["missing", "symlink"])
async def test_missing_or_replaced_output_root_is_never_complete_empty_evidence(instances, kind):
    from deerflow.agent_instances.presentation import capture_instance_outputs

    execution, _, root = await prepared(instances)
    (root / "outputs").rename(root / "private")
    if kind == "symlink":
        (root / "outputs").symlink_to("private", target_is_directory=True)
    with execution_scope(execution) as environment:
        environment.provider = InstanceSandboxProvider(execution, SimpleNamespace(id="fixture"))
        snapshot = await capture_instance_outputs(execution, observe_roots=True)
    assert not snapshot.files
    assert snapshot.root_observations[0].state != "opened"


async def test_instance_snapshot_never_follows_output_links(instances):
    from deerflow.agent_instances.presentation import capture_instance_outputs

    execution, _, root = await prepared(instances)
    (root / "private.txt").write_text("private", encoding="utf-8")
    (root / "outputs/escape.txt").symlink_to("../private.txt")
    with execution_scope(execution) as environment:
        environment.provider = InstanceSandboxProvider(execution, SimpleNamespace(id="fixture"))
        snapshot = await capture_instance_outputs(execution, observe_roots=True)
    link = snapshot.files["/mnt/user-data/outputs/escape.txt"]
    assert link.symlink and link.sha256 is None and link.text is None


async def test_empty_home_outputs_are_observed_without_blocking_the_event_loop(instances, monkeypatch):
    from _storage_spaces_test_support import FixtureVolume
    from support.detectors.blocking_io_runtime import detect_blocking_io_strict

    from deerflow.agent_instances.presentation import capture_instance_outputs

    execution, _, root = await prepared(instances)
    (root / "outputs/result.txt").unlink()
    # Production qualified volumes carry these captured inodes as plain fields.
    identities = {key: (volume.root_inode, volume.control_inode) for key, volume in instances[3].verified.items()}
    monkeypatch.setattr(FixtureVolume, "root_inode", property(lambda volume: identities[volume.spec.slot_id][0]))
    monkeypatch.setattr(FixtureVolume, "control_inode", property(lambda volume: identities[volume.spec.slot_id][1]))
    with execution_scope(execution) as environment:
        environment.provider = InstanceSandboxProvider(execution, SimpleNamespace(id="fixture"))
        with detect_blocking_io_strict():
            snapshot = await capture_instance_outputs(execution, observe_roots=True)
    assert not snapshot.files and not snapshot.truncated
    assert snapshot.root_observations[0].state == "opened"


@pytest.mark.parametrize("kind", ["unprepared", "different-execution", "inspection"])
async def test_instance_snapshot_needs_exact_prepared_execution(instances, kind):
    from deerflow.agent_instances.presentation import capture_instance_outputs

    execution, _, _ = await prepared(instances)
    if kind == "inspection":
        execution = replace(execution, execution_allowed=False)
    with execution_scope(execution) as environment:
        if kind != "unprepared":
            environment.provider = InstanceSandboxProvider(replace(execution) if kind == "different-execution" else execution, SimpleNamespace(id="fixture"))
        with pytest.raises((ValueError, PermissionError)):
            await capture_instance_outputs(execution, observe_roots=True)


async def test_generation_changed_during_scan_cannot_publish_evidence(instances, monkeypatch):
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.agent_instances.presentation import capture_instance_outputs
    from deerflow.persistence.spaces.model import SpaceRow

    execution, _, _ = await prepared(instances)
    validate = execution.authority.validate
    calls = 0

    async def changed_after_scan(bound):
        nonlocal calls
        calls += 1
        if calls == 2:
            async with instances[2].begin() as writer:
                (await writer.get(SpaceRow, execution.instance.home_id)).generation += 1
        await validate(bound)

    monkeypatch.setattr(execution.authority, "validate", changed_after_scan)
    with execution_scope(execution) as environment:
        environment.provider = InstanceSandboxProvider(execution, SimpleNamespace(id="fixture"))
        with pytest.raises(AgentDenied):
            await capture_instance_outputs(execution, observe_roots=True)
    assert calls == 2


@pytest.mark.parametrize("phase", ["volume", "scan"])
async def test_cancellation_drains_qualification_and_scan_before_releasing_home(instances, monkeypatch, phase):
    from contextlib import asynccontextmanager

    from deerflow.agent_instances.presentation import capture_instance_outputs
    from deerflow.spaces import chat_snapshots

    execution, _, _ = await prepared(instances)
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    catalog = instances[3]
    scan = catalog.verify if phase == "volume" else chat_snapshots.scan_confined_roots
    owner_thread = threading.get_ident()
    released = asyncio.Event()
    registry = execution.authority.instances.files.registry
    admitted = registry.admitted

    @asynccontextmanager
    async def owned(**kwargs):
        async with admitted(**kwargs) as value:
            try:
                yield value
            finally:
                # Validation has its own earlier admission; observe only the
                # read that reached the qualification/scan worker barrier.
                if entered.is_set():
                    released.set()

    def held(*args, **kwargs):
        assert threading.get_ident() != owner_thread
        entered.set()
        try:
            assert release.wait(5)
            return scan(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(catalog if phase == "volume" else chat_snapshots, "verify" if phase == "volume" else "scan_confined_roots", held)
    monkeypatch.setattr(registry, "admitted", owned)
    task = None
    try:
        with execution_scope(execution) as environment:
            environment.provider = InstanceSandboxProvider(execution, SimpleNamespace(id="fixture"))
            task = asyncio.create_task(capture_instance_outputs(execution, observe_roots=True))
            assert await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            done, _ = await asyncio.wait({task}, timeout=0.1)
            assert not done and not released.is_set()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert finished.is_set()
            assert released.is_set()
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
