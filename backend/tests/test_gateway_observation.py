"""Private observational Gateway lifetime; never native admission or authority."""

import asyncio
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Event
from uuid import uuid4

import pytest

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Gateway process observation requires Linux procfs")


def test_gateway_start_is_the_actual_supplied_lifetime_not_the_cli_start(tmp_path):
    from app.gateway.gateway_observation import publish, read

    started = datetime(2026, 10, 16, tzinfo=UTC)
    lifetime = publish(tmp_path, started_at=started)
    try:
        observation = read(tmp_path)
        assert observation.started_at == started
        observation.require_current(tmp_path)
        assert lifetime.path.stat().st_mode & 0o777 == 0o600
    finally:
        lifetime.close()
    assert not lifetime.path.exists()


@pytest.mark.parametrize("change", ["missing", "pid", "start", "boot", "namespace", "mode", "symlink", "hardlink", "large", "json", "duplicate"])
def test_unconfirmed_gateway_start_never_becomes_an_invented_date(tmp_path, change):
    from app.gateway.gateway_observation import GatewayObservationUnavailable, publish, read

    lifetime = publish(tmp_path, started_at=datetime.now(UTC))
    path = lifetime.path
    secret = "private-payload-must-not-appear-in-diagnostics"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if change == "missing":
            path.unlink()
        elif change in {"pid", "start", "boot", "namespace"}:
            key = {"pid": "pid", "start": "process_start", "boot": "boot_id", "namespace": "pid_namespace"}[change]
            value[key] = str(uuid4()) if change == "boot" else 2**30 if change == "pid" else value[key] + 1
            path.write_text(json.dumps(value), encoding="utf-8")
        elif change == "mode":
            path.chmod(0o644)
        elif change == "symlink":
            other = tmp_path / "unrelated"
            path.rename(other)
            path.symlink_to(other)
        elif change == "hardlink":
            os.link(path, tmp_path / "another-link")
        else:
            text = secret * 200 if change == "large" else secret if change == "json" else '{"version":1,"version":1}'
            path.write_text(text, encoding="utf-8")
        with pytest.raises(GatewayObservationUnavailable) as error:
            read(tmp_path)
        assert secret not in str(error.value)
    finally:
        lifetime.close()


def test_restart_during_status_snapshot_invalidates_the_observation(tmp_path):
    from app.gateway.gateway_observation import GatewayObservationUnavailable, publish, read

    first = publish(tmp_path, started_at=datetime.now(UTC))
    observation = read(tmp_path)
    first.close()
    second = publish(tmp_path, started_at=datetime.now(UTC))
    try:
        with pytest.raises(GatewayObservationUnavailable):
            observation.require_current(tmp_path)
    finally:
        second.close()


def test_observation_owner_never_replaces_another_live_lifetime(tmp_path):
    from app.gateway.gateway_observation import GatewayObservationUnavailable, publish, read

    first = publish(tmp_path, started_at=datetime.now(UTC))
    observation = read(tmp_path)
    try:
        with pytest.raises(GatewayObservationUnavailable):
            publish(tmp_path, started_at=datetime.now(UTC))
        observation.require_current(tmp_path)
    finally:
        first.close()


def test_new_writer_cannot_validate_a_previous_failed_publication(tmp_path, monkeypatch):
    from app.gateway import gateway_observation as module

    real_sync, real_replace = os.fsync, os.replace
    entered, release = Event(), Event()

    def lost_directory_ack(descriptor):
        import stat

        if stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise OSError("Lost publication acknowledgement")
        return real_sync(descriptor)

    with monkeypatch.context() as fault:
        fault.setattr(module.os, "fsync", lost_directory_ack)
        with pytest.raises(module.GatewayObservationUnavailable):
            module.publish(tmp_path, started_at=datetime.now(UTC))
    assert (tmp_path / ".gateway-runtime/start.json").exists()

    def held_replace(*args, **kwargs):
        entered.set()
        assert release.wait(10), "Test must release the new publication"
        return real_replace(*args, **kwargs)

    monkeypatch.setattr(module.os, "replace", held_replace)
    with ThreadPoolExecutor(max_workers=1) as worker:
        future = worker.submit(module.publish, tmp_path, started_at=datetime.now(UTC))
        try:
            assert entered.wait(5)
            with pytest.raises(module.GatewayObservationUnavailable):
                module.read(tmp_path)
        finally:
            release.set()
            replacement = future.result(timeout=10)
            try:
                module.read(tmp_path).require_current(tmp_path)
            finally:
                replacement.close()


def test_cleanup_removes_only_the_exact_published_receipt(tmp_path):
    from app.gateway.gateway_observation import GatewayObservationUnavailable, publish, read

    lifetime = publish(tmp_path, started_at=datetime.now(UTC))
    value = json.loads(lifetime.path.read_text(encoding="utf-8"))
    value["lifetime_id"] = uuid4().hex
    replacement = lifetime.path.with_name("replacement")
    replacement.write_text(json.dumps(value), encoding="utf-8")
    replacement.chmod(0o600)
    replacement.replace(lifetime.path)
    lifetime.close()
    assert lifetime.path.exists()
    # Its process is still alive, but no Gateway observation writer owns it.
    with pytest.raises(GatewayObservationUnavailable):
        read(tmp_path)


def test_replaced_lifetime_proof_is_not_active_and_cleanup_preserves_the_replacement(tmp_path):
    from app.gateway.gateway_observation import GatewayObservationUnavailable, publish, read

    lifetime = publish(tmp_path, started_at=datetime.now(UTC))
    proof = lifetime.path.parent / lifetime._proof_name
    original = proof.with_name("original-proof")
    proof.rename(original)
    proof.touch(mode=0o600)
    try:
        with pytest.raises(GatewayObservationUnavailable):
            read(tmp_path)
    finally:
        lifetime.close()
    assert proof.exists() and original.exists()


@pytest.mark.parametrize("fsync_number", [1, 2])
def test_interrupted_publication_drains_descriptors_and_leaves_no_stage(tmp_path, monkeypatch, fsync_number):
    from app.gateway import gateway_observation as module

    real_sync = os.fsync
    calls = 0

    def interrupted(descriptor):
        nonlocal calls
        calls += 1
        if calls == fsync_number:
            raise OSError("offline durability failure")
        return real_sync(descriptor)

    with monkeypatch.context() as fault:
        fault.setattr(module.os, "fsync", interrupted)
        with pytest.raises(module.GatewayObservationUnavailable):
            module.publish(tmp_path, started_at=datetime.now(UTC))
    assert not list((tmp_path / ".gateway-runtime").glob(".stage-*"))
    with pytest.raises(module.GatewayObservationUnavailable):
        module.read(tmp_path)
    replacement = module.publish(tmp_path, started_at=datetime.now(UTC))
    replacement.close()


@pytest.mark.asyncio
async def test_disabled_observation_never_opens_a_receipt(tmp_path, monkeypatch):
    from app.gateway import gateway_observation as module

    def unexpected(*_args, **_kwargs):
        pytest.fail("Disabled observation must not open the filesystem")

    monkeypatch.setattr(module, "publish", unexpected)
    async with module.gateway_observation_lifetime(enabled=False, home=tmp_path, started_at=datetime.now(UTC)):
        assert not (tmp_path / ".gateway-runtime").exists()


@pytest.mark.asyncio
async def test_observation_failure_does_not_prevent_gateway_startup(tmp_path, monkeypatch, caplog):
    from app.gateway import gateway_observation as module

    def unavailable(*_args, **_kwargs):
        raise OSError("untrusted diagnostic text")

    monkeypatch.setattr(module, "publish", unavailable)
    async with module.gateway_observation_lifetime(enabled=True, home=tmp_path, started_at=datetime.now(UTC)):
        pass
    assert "observation unavailable" in caplog.text
    assert "untrusted diagnostic text" not in caplog.text


@pytest.mark.asyncio
async def test_cancelled_observation_entry_drains_and_closes_its_late_receipt(tmp_path, monkeypatch):
    from app.gateway import gateway_observation as module
    from deerflow.utils.file_io import run_file_io

    entered, release = Event(), Event()
    original, receipts = module.publish, []

    def delayed(*args, **kwargs):
        entered.set()
        assert release.wait(10), "Test must release the observation writer"
        result = original(*args, **kwargs)
        receipts.append(result)
        return result

    monkeypatch.setattr(module, "publish", delayed)

    async def start():
        async with module.gateway_observation_lifetime(enabled=True, home=tmp_path, started_at=datetime.now(UTC)):
            pytest.fail("Cancelled startup must not publish a running application")

    task = asyncio.create_task(start())
    try:
        assert await run_file_io(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(receipts) == 1 and not receipts[0].path.exists()
        with pytest.raises(module.GatewayObservationUnavailable):
            module.read(tmp_path)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_gateway_entry_records_its_start_before_resource_initialization(monkeypatch):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from app.gateway import app as module
    from app.gateway import customer_administration

    before = datetime.now(UTC)
    application = SimpleNamespace(state=SimpleNamespace(customer_administration_policy=object()))
    observed = []

    @asynccontextmanager
    async def scope(_app):
        yield

    @asynccontextmanager
    async def resources(app):
        observed.append(app.state.gateway_started_at)
        assert before <= app.state.gateway_started_at <= datetime.now(UTC)
        yield

    monkeypatch.setattr(customer_administration, "customer_administration_host_scope", scope)
    monkeypatch.setattr(module, "_lifespan_resources", resources)
    async with module.lifespan(application):
        assert observed == [application.state.gateway_started_at]
