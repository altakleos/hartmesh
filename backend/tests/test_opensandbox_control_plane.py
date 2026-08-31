from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta

import pytest

from deerflow.community.opensandbox.control_plane import (
    ClaimResult,
    OpenSandboxControlPlaneError,
    OpenSandboxSdkControlPlane,
    RemoteSandbox,
    RemoteSandboxSpec,
    SetupRequest,
    StatefulOpenSandboxControlPlane,
)


def _spec() -> RemoteSandboxSpec:
    return RemoteSandboxSpec(
        image_digest="registry.example/hartmesh@sha256:" + "1" * 64,
        labels={
            "hm_contract": "accepted-material-v1",
            "hm_tenant": "tenant-1234",
        },
        expires_at=datetime(2026, 8, 31, 12, 5, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_stateful_control_plane_models_claim_and_epoch_fences() -> None:
    control_plane = StatefulOpenSandboxControlPlane()
    created = await control_plane.create(_spec())

    assert await control_plane.list_by_labels(_spec().labels) == (created,)
    first = await control_plane.claim(
        created.remote_id,
        expected_epoch=0,
        owner="worker-a",
        expires_at=datetime(2026, 8, 31, 12, 3, tzinfo=UTC),
    )
    assert first == ClaimResult(claimed=True, ownership_epoch=1)
    assert not (
        await control_plane.claim(
            created.remote_id,
            expected_epoch=None,
            owner="worker-b",
            expires_at=datetime(2026, 8, 31, 12, 4, tzinfo=UTC),
        )
    ).claimed
    assert not (
        await control_plane.claim(
            created.remote_id,
            expected_epoch=0,
            owner="worker-b",
            expires_at=datetime(2026, 8, 31, 12, 4, tzinfo=UTC),
        )
    ).claimed
    assert not await control_plane.renew(
        created.remote_id,
        epoch=1,
        owner="worker-b",
        expires_at=datetime(2026, 8, 31, 12, 4, tzinfo=UTC),
    )
    assert await control_plane.renew(
        created.remote_id,
        epoch=1,
        owner="worker-a",
        expires_at=datetime(2026, 8, 31, 12, 4, tzinfo=UTC),
    )

    await control_plane.upload_file(created.remote_id, "staging/SKILL.md", b"ok", 0o444)
    setup = await control_plane.exec_setup(
        created.remote_id,
        SetupRequest(
            verifier_digest="2" * 64,
            manifest_digest="3" * 64,
        ),
    )
    assert setup.succeeded

    await control_plane.destroy(created.remote_id, epoch=0)
    assert await control_plane.get(created.remote_id) is not None
    await control_plane.destroy(created.remote_id, epoch=1)
    assert await control_plane.get(created.remote_id) is None


class _BlockingSdkDriver:
    def __init__(self, remote: RemoteSandbox) -> None:
        self.remote = remote
        self.entered = threading.Event()
        self.release = threading.Event()

    def get(self, remote_id: str) -> RemoteSandbox | None:
        self.entered.set()
        self.release.wait(timeout=2)
        return self.remote if remote_id == self.remote.remote_id else None


@pytest.mark.asyncio
async def test_sdk_control_plane_offloads_sync_sdk_calls() -> None:
    spec = _spec()
    remote = RemoteSandbox(
        remote_id="remote-1",
        reported_image_ref=spec.image_digest,
        labels=spec.labels,
        expires_at=spec.expires_at,
    )
    driver = _BlockingSdkDriver(remote)
    control_plane = OpenSandboxSdkControlPlane(
        driver=driver,
        call_timeout_seconds=1,
        max_attempts=1,
    )

    operation = asyncio.create_task(control_plane.get("remote-1"))
    await asyncio.to_thread(driver.entered.wait, 1)
    event_loop_progressed = False

    async def tick() -> None:
        nonlocal event_loop_progressed
        await asyncio.sleep(0)
        event_loop_progressed = True

    await tick()
    assert event_loop_progressed
    driver.release.set()
    assert await operation == remote


class _FailingSdkDriver:
    def get(self, remote_id: str) -> RemoteSandbox | None:
        del remote_id
        raise RuntimeError("Authorization: Bearer super-secret; body=<provider dump>")


@pytest.mark.asyncio
async def test_sdk_errors_are_bounded_and_secret_safe() -> None:
    control_plane = OpenSandboxSdkControlPlane(
        driver=_FailingSdkDriver(),
        call_timeout_seconds=1,
        max_attempts=1,
    )

    with pytest.raises(OpenSandboxControlPlaneError) as raised:
        await control_plane.get("remote-1")

    rendered = str(raised.value)
    assert raised.value.code == "opensandbox_control_plane_unavailable"
    assert "super-secret" not in rendered
    assert "provider dump" not in rendered
    assert len(rendered) < 160


@pytest.mark.asyncio
async def test_sdk_facade_rejects_injected_create_image_mismatch_and_cleans_up() -> None:
    spec = _spec()

    class Driver:
        def __init__(self) -> None:
            self.destroyed = []

        def create(self, _spec):
            return RemoteSandbox(
                remote_id="remote-mismatch",
                reported_image_ref="registry.example/other@sha256:" + "9" * 64,
                labels=spec.labels,
                expires_at=spec.expires_at,
            )

        def destroy(self, remote_id):
            self.destroyed.append(remote_id)

    driver = Driver()
    control_plane = OpenSandboxSdkControlPlane(
        driver=driver,
        call_timeout_seconds=1,
        max_attempts=1,
    )

    with pytest.raises(
        OpenSandboxControlPlaneError,
        match="opensandbox_image_digest_mismatch",
    ):
        await control_plane.create(spec)

    assert driver.destroyed == ["remote-mismatch"]


@pytest.mark.asyncio
async def test_sdk_adapter_rejects_missing_atomic_claim_and_trusted_setup() -> None:
    control_plane = OpenSandboxSdkControlPlane(
        driver=_FailingSdkDriver(),
        call_timeout_seconds=1,
        max_attempts=1,
    )

    with pytest.raises(
        OpenSandboxControlPlaneError,
        match="opensandbox_accepted_claim_cas_unsupported",
    ):
        await control_plane.claim(
            "remote-1",
            expected_epoch=None,
            owner="worker-a",
            expires_at=datetime.now(UTC) + timedelta(minutes=1),
        )
    with pytest.raises(
        OpenSandboxControlPlaneError,
        match="opensandbox_trusted_setup_unsupported",
    ):
        await control_plane.exec_setup(
            "remote-1",
            SetupRequest(
                verifier_digest="2" * 64,
                manifest_digest="3" * 64,
            ),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "mode"),
    [
        ("../escape", 0o444),
        ("/absolute", 0o444),
        ("staging\\alias", 0o444),
        ("staging//alias", 0o444),
        ("staging/writable", 0o644),
        ("staging/special", 0o4444),
    ],
)
async def test_sdk_upload_rejects_unsafe_paths_and_modes(
    path: str,
    mode: int,
) -> None:
    class Driver:
        def upload_file(self, *_args) -> None:
            raise AssertionError("invalid uploads must not reach the SDK")

    control_plane = OpenSandboxSdkControlPlane(
        driver=Driver(),
        call_timeout_seconds=1,
        max_attempts=1,
    )

    with pytest.raises(ValueError, match="OpenSandbox upload"):
        await control_plane.upload_file("remote-1", path, b"content", mode)
