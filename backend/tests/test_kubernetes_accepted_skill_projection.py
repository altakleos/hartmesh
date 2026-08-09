from __future__ import annotations

import hashlib
import importlib.util
import json
import socket
import threading
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import requests

from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
from deerflow.community.aio_sandbox.remote_backend import RemoteSandboxBackend
from deerflow.community.aio_sandbox.sandbox_info import (
    AcceptedSkillMaterialReceiptV1,
    SandboxInfo,
)
from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.runtime.runs.manager import RunManager, RunStartOutcome, RunStartupError
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.skill_projection import SkillProjectionEvidence
from deerflow.runtime.skill_snapshot import SkillSnapshotProjection
from deerflow.sandbox.sandbox_provider import (
    AcceptedSkillExecutionEvidenceV1,
    AcceptedSkillMaterialCapability,
    AcceptedSkillSandboxBindingError,
    AcceptedSkillSandboxBindingV1,
)


def _projection_evidence(*, content: bytes = b"# accepted\n") -> SkillProjectionEvidence:
    header = json.dumps(
        ["public", "demo", "SKILL.md", "regular"],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    tree = hashlib.sha256()
    tree.update(len(header).to_bytes(4, "big"))
    tree.update(header)
    tree.update(len(content).to_bytes(8, "big"))
    tree.update(content)
    projection = SkillSnapshotProjection(
        name="demo",
        category="public",
        relative_path="demo",
        manifest_digest=hashlib.sha256(content).hexdigest(),
        content_digest=tree.hexdigest(),
        file_count=1,
        total_bytes=len(content),
    )
    snapshot_id = hashlib.sha256(
        json.dumps(
            {"version": 1, "skills": [projection.to_json()]},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return SkillProjectionEvidence(
        snapshot_id=snapshot_id,
        content_digest=snapshot_id,
        projections=(projection,),
        file_count=1,
        total_bytes=len(content),
    )


def _load_verifier_module():
    path = Path(__file__).resolve().parents[2] / "docker" / "provisioner" / "accepted_skills.py"
    spec = importlib.util.spec_from_file_location("accepted_skills_verifier_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_verifier_rejects_symlink_in_projection_ancestor(
    tmp_path: Path,
) -> None:
    verifier = _load_verifier_module()
    evidence = _projection_evidence()
    outside = tmp_path / "outside"
    skill = outside / "demo"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_bytes(b"# accepted\n")
    source = tmp_path / "source"
    source.mkdir()
    (source / "public").symlink_to(outside, target_is_directory=True)

    with pytest.raises(
        verifier.AcceptedSkillMaterializationError,
        match="skill_snapshot_symlink",
    ):
        verifier.materialize_verified_snapshot(
            source=source,
            destination=tmp_path / "private",
            evidence={
                "snapshot_id": evidence.snapshot_id,
                "content_digest": evidence.content_digest,
                "projections": [evidence.projections[0].to_json()],
                "file_count": evidence.file_count,
                "total_bytes": evidence.total_bytes,
            },
        )


def test_materialization_receipt_rejects_unbounded_or_malformed_identity() -> None:
    with pytest.raises(ValueError, match="runtime_image_ids_digest"):
        AcceptedSkillMaterialReceiptV1(
            profile="rwx_verified_copy_v1",
            attempt_id="sandbox-attempt",
            snapshot_id="a" * 64,
            content_digest="a" * 64,
            run_id="run-1",
            generation=1,
            pod_uid="pod-uid",
            lease_uid="lease-uid",
            runtime_image_ids_digest="not-a-digest",
            verifier_receipt_digest="c" * 64,
            materialization_evidence_digest="b" * 64,
        )


@pytest.mark.asyncio
async def test_remote_execution_fence_reduces_provisioner_failures_to_false(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A data-plane fence must never carry raw provisioner text into run state."""

    receipt = AcceptedSkillMaterialReceiptV1(
        profile="rwx_verified_copy_v1",
        attempt_id="sandbox-attempt",
        snapshot_id="a" * 64,
        content_digest="a" * 64,
        run_id="run-1",
        generation=1,
        pod_uid="pod-uid",
        lease_uid="lease-uid",
        runtime_image_ids_digest="b" * 64,
        verifier_receipt_digest="c" * 64,
        materialization_evidence_digest="d" * 64,
    )
    evidence = AcceptedSkillExecutionEvidenceV1(
        profile=receipt.profile,
        attempt_id=receipt.attempt_id,
        snapshot_id=receipt.snapshot_id,
        run_id=receipt.run_id,
        generation=receipt.generation,
        pod_uid=receipt.pod_uid,
        lease_uid=receipt.lease_uid,
        runtime_image_ids_digest=receipt.runtime_image_ids_digest,
        verifier_receipt_digest=receipt.verifier_receipt_digest,
        materialization_evidence_digest=receipt.materialization_evidence_digest,
    )
    info = SandboxInfo(
        sandbox_id="sandbox-1",
        sandbox_url="http://127.0.0.1:1",
        accepted_skill_material=receipt,
    )
    backend = object.__new__(RemoteSandboxBackend)

    def fail_with_private_text(_info: SandboxInfo) -> bool:
        raise RuntimeError("private-provisioner-response-body")

    backend.is_alive = fail_with_private_text  # type: ignore[method-assign]
    provider = object.__new__(AioSandboxProvider)
    provider._backend = backend
    provider._accepted_execution_info = lambda *_args: info  # type: ignore[method-assign]

    assert (
        await provider.validate_accepted_skill_execution_async(
            "sandbox-1",
            evidence,
        )
        is False
    )
    assert "private-provisioner-response-body" not in caplog.text


def test_remote_request_carries_exact_accepted_projection_and_private_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _projection_evidence()
    binding = AcceptedSkillSandboxBindingV1(
        snapshot_id=evidence.snapshot_id,
        run_id="run-1",
        generation=7,
        evidence=evidence,
    )
    captured: dict[str, object] = {}

    class Response:
        ok = True
        status_code = 200
        text = ""

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            receipt: dict[str, object] = {
                "version": 1,
                "profile": "rwx_verified_copy_v1",
                "attempt_id": "sandbox-sandbox-1-accepted-attempt",
                "snapshot_id": evidence.snapshot_id,
                "content_digest": evidence.content_digest,
                "run_id": "run-1",
                "generation": 7,
                "pod_uid": "pod-uid-1",
                "lease_uid": "lease-uid-1",
                "runtime_image_ids_digest": "d" * 64,
                "verifier_receipt_digest": "e" * 64,
            }
            receipt["materialization_evidence_digest"] = hashlib.sha256(
                json.dumps(
                    receipt,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8"),
            ).hexdigest()
            return {
                "sandbox_id": "sandbox-1",
                "sandbox_url": "http://10.0.0.8:8081",
                "status": "Pending",
                "accepted_skill_material": receipt,
            }

    def post(_url: str, *, json: dict[str, object], **_kwargs):
        captured.update(json)
        return Response()

    monkeypatch.setattr("requests.post", post)
    monkeypatch.setattr(
        "deerflow.community.aio_sandbox.remote_backend.user_should_see_legacy_skills",
        lambda _user_id: False,
    )
    backend = RemoteSandboxBackend("http://provisioner:8002")

    info = backend.create(
        "thread-1",
        "sandbox-1",
        user_id="owner-1",
        accepted_skill_binding=binding,
    )

    projection = captured["accepted_skill_projection"]
    assert isinstance(projection, dict)
    assert projection["profile"] == "rwx_verified_copy_v1"
    assert projection["snapshot_id"] == evidence.snapshot_id
    assert projection["content_digest"] == evidence.content_digest
    assert projection["run_id"] == "run-1"
    assert projection["generation"] == 7
    assert projection["projections"] == [evidence.projections[0].to_json()]
    assert "capability" not in projection
    assert info.request_headers == {"Authorization": f"Bearer {captured['attempt_capability']}"}
    assert info.accepted_skill_material is not None
    assert info.accepted_skill_material.pod_uid == "pod-uid-1"


def test_remote_retries_response_loss_with_the_same_attempt_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _projection_evidence()
    binding = AcceptedSkillSandboxBindingV1(
        snapshot_id=evidence.snapshot_id,
        run_id="run-1",
        generation=7,
        evidence=evidence,
    )
    payloads: list[dict[str, object]] = []

    class Response:
        ok = True
        status_code = 200
        text = ""

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            receipt: dict[str, object] = {
                "version": 1,
                "profile": "rwx_verified_copy_v1",
                "attempt_id": "sandbox-sandbox-1-accepted-attempt",
                "snapshot_id": evidence.snapshot_id,
                "content_digest": evidence.content_digest,
                "run_id": "run-1",
                "generation": 7,
                "pod_uid": "pod-uid-1",
                "lease_uid": "lease-uid-1",
                "runtime_image_ids_digest": "d" * 64,
                "verifier_receipt_digest": "e" * 64,
            }
            receipt["materialization_evidence_digest"] = hashlib.sha256(
                json.dumps(
                    receipt,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8"),
            ).hexdigest()
            return {
                "sandbox_id": "sandbox-1",
                "sandbox_url": "http://10.0.0.8:8081",
                "status": "Running",
                "accepted_skill_material": receipt,
            }

    def post(_url: str, *, json: dict[str, object], **_kwargs):
        payloads.append(dict(json))
        if len(payloads) == 1:
            raise requests.ConnectionError("response lost after commit")
        return Response()

    monkeypatch.setattr("requests.post", post)
    monkeypatch.setattr(
        "deerflow.community.aio_sandbox.remote_backend.user_should_see_legacy_skills",
        lambda _user_id: False,
    )

    info = RemoteSandboxBackend("http://provisioner:8002").create(
        "thread-1",
        "sandbox-1",
        user_id="owner-1",
        accepted_skill_binding=binding,
    )

    assert len(payloads) == 2
    assert payloads[0] == payloads[1]
    assert payloads[0]["attempt_capability"] == payloads[1]["attempt_capability"]
    assert info.accepted_skill_material is not None
    assert info.accepted_skill_material.pod_uid == "pod-uid-1"


def test_remote_preflight_uses_and_rereads_projected_service_account_token(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("token-one", encoding="utf-8")
    observed_headers: list[dict[str, str]] = []

    class Response:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    def request_get(url: str, *, headers: dict[str, str], **_kwargs):
        observed_headers.append(dict(headers))
        if url.endswith("/ready"):
            return Response({"status": "ready"})
        return Response(
            {
                "accepted_skill_projection_profiles": [
                    "rwx_verified_copy_v1",
                ],
            },
        )

    monkeypatch.setattr("requests.get", request_get)
    backend = RemoteSandboxBackend(
        "http://provisioner:8002",
        service_account_token_file=str(tmp_path / "token"),
    )

    assert backend.accepted_skill_projection_ready() is True
    (tmp_path / "token").write_text("token-two", encoding="utf-8")
    assert backend.accepted_skill_projection_ready() is True
    assert observed_headers == [
        {"Authorization": "Bearer token-one"},
        {"Authorization": "Bearer token-one"},
        {"Authorization": "Bearer token-two"},
        {"Authorization": "Bearer token-two"},
    ]


def test_remote_empty_accepted_set_still_requests_an_isolated_pod(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = AcceptedSkillSandboxBindingV1(
        snapshot_id=None,
        run_id="run-empty",
        generation=3,
        evidence=SkillProjectionEvidence.from_snapshot(None),
    )
    captured: dict[str, object] = {}

    class Response:
        ok = True
        status_code = 200
        text = ""

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "sandbox_id": "sandbox-empty",
                "sandbox_url": "http://sandbox-empty:8080",
                "status": "Pending",
                "accepted_skill_material": None,
            }

    def post(_url: str, *, json: dict[str, object], **_kwargs):
        captured.update(json)
        return Response()

    monkeypatch.setattr("requests.post", post)
    monkeypatch.setattr(
        "deerflow.community.aio_sandbox.remote_backend.user_should_see_legacy_skills",
        lambda _user_id: False,
    )

    info = RemoteSandboxBackend("http://provisioner:8002").create(
        "thread-empty",
        "sandbox-empty",
        user_id="owner-empty",
        accepted_skills_only=True,
        accepted_skill_binding=binding,
    )

    assert captured["accepted_skills_only"] is True
    assert "accepted_skill_projection" not in captured
    assert "attempt_capability" not in captured
    assert info.request_headers == {}
    assert info.accepted_skill_material is None


def test_verified_copy_recomputes_existing_canonical_digest_and_is_source_independent(
    tmp_path: Path,
) -> None:
    verifier = _load_verifier_module()
    evidence = _projection_evidence()
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    skill = source / "public" / "demo"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_bytes(b"# accepted\n")

    receipt = verifier.materialize_verified_snapshot(
        source=source,
        destination=destination,
        evidence={
            "snapshot_id": evidence.snapshot_id,
            "content_digest": evidence.content_digest,
            "projections": [evidence.projections[0].to_json()],
            "file_count": evidence.file_count,
            "total_bytes": evidence.total_bytes,
        },
    )

    assert receipt["content_digest"] == evidence.content_digest
    projected = destination / evidence.snapshot_id / "public" / "demo" / "SKILL.md"
    assert projected.read_bytes() == b"# accepted\n"
    assert projected.stat().st_mode & 0o777 == 0o444
    assert projected.parent.stat().st_mode & 0o777 == 0o555
    (skill / "SKILL.md").write_bytes(b"# changed after materialization\n")
    assert projected.read_bytes() == b"# accepted\n"


def test_verified_copy_rejects_symlink_and_digest_mismatch(tmp_path: Path) -> None:
    verifier = _load_verifier_module()
    evidence = _projection_evidence()
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    skill = source / "public" / "demo"
    skill.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.write_text("secret", encoding="utf-8")
    (skill / "SKILL.md").symlink_to(outside)
    wire = {
        "snapshot_id": evidence.snapshot_id,
        "content_digest": evidence.content_digest,
        "projections": [evidence.projections[0].to_json()],
        "file_count": evidence.file_count,
        "total_bytes": evidence.total_bytes,
    }

    with pytest.raises(verifier.AcceptedSkillMaterializationError, match="skill_snapshot_symlink"):
        verifier.materialize_verified_snapshot(
            source=source,
            destination=destination,
            evidence=wire,
        )

    (skill / "SKILL.md").unlink()
    (skill / "SKILL.md").write_bytes(b"wrong")
    with pytest.raises(verifier.AcceptedSkillMaterializationError, match="skill_snapshot_drift"):
        verifier.materialize_verified_snapshot(
            source=source,
            destination=destination,
            evidence=wire,
        )


def test_verified_copy_rejects_category_path_escape(tmp_path: Path) -> None:
    verifier = _load_verifier_module()
    evidence = _projection_evidence()
    projection = evidence.projections[0].to_json()
    projection["category"] = ".."

    with pytest.raises(
        verifier.AcceptedSkillMaterializationError,
        match="skill_snapshot_category_invalid",
    ):
        verifier.materialize_verified_snapshot(
            source=tmp_path / "source",
            destination=tmp_path / "destination",
            evidence={
                "snapshot_id": evidence.snapshot_id,
                "content_digest": evidence.content_digest,
                "projections": [projection],
                "file_count": evidence.file_count,
                "total_bytes": evidence.total_bytes,
            },
        )


def test_provisioner_accepted_pod_uses_exact_rwx_source_and_private_read_only_copy(
    provisioner_module,
) -> None:
    evidence = _projection_evidence()
    provisioner_module.USERDATA_PVC_NAME = "home-rwx"
    provisioner_module.ACCEPTED_SKILL_PROJECTION_PROFILE = "rwx_verified_copy_v1"
    provisioner_module.ACCEPTED_SKILL_RUNTIME_IMAGE = "registry.example/provisioner@sha256:" + ("a" * 64)
    provisioner_module.SANDBOX_IMAGE = "registry.example/aio@sha256:" + ("b" * 64)
    provisioner_module.ACCEPTED_SKILL_PROJECTION_PROFILE = "rwx_verified_copy_v1"
    provisioner_module.USERDATA_PVC_NAME = "home-rwx"
    projection = provisioner_module.AcceptedSkillProjectionV1(
        profile="rwx_verified_copy_v1",
        snapshot_id=evidence.snapshot_id,
        content_digest=evidence.content_digest,
        run_id="run-1",
        generation=7,
        projections=[item.to_json() for item in evidence.projections],
        file_count=evidence.file_count,
        total_bytes=evidence.total_bytes,
    )

    pod = provisioner_module._build_pod(
        "sandbox-1",
        "thread-1",
        user_id="owner-1",
        accepted_skill_projection=projection,
        attempt_capability="A" * 43,
    )

    assert pod.spec.automount_service_account_token is False
    assert [container.name for container in pod.spec.init_containers] == ["accepted-skill-verifier"]
    assert [container.name for container in pod.spec.containers] == ["sandbox", "accepted-skill-gate"]
    verifier = pod.spec.init_containers[0]
    verifier_mounts = {mount.name: mount for mount in verifier.volume_mounts}
    assert verifier_mounts["accepted-skill-source"].read_only is True
    assert verifier_mounts["accepted-skill-source"].sub_path == ("deer-flow/runtime/skill-snapshots/subject-" + hashlib.sha256(b"owner-1").hexdigest()[:32] + f"/{evidence.snapshot_id}")
    sandbox_mounts = {mount.mount_path: mount for mount in pod.spec.containers[0].volume_mounts}
    assert sandbox_mounts["/mnt/skills/.accepted"].read_only is True
    assert not any(path in sandbox_mounts for path in ("/mnt/skills", "/mnt/skills/public", "/mnt/skills/custom", "/mnt/skills/legacy"))
    assert all(mount.name != "accepted-skill-source" for mount in pod.spec.containers[0].volume_mounts)
    gate_mounts = {mount.name for mount in pod.spec.containers[1].volume_mounts}
    assert "accepted-skill-material" not in gate_mounts
    assert "accepted-skill-capability" in gate_mounts
    assert pod.spec.containers[0].security_context.allow_privilege_escalation is False
    preferences = pod.spec.affinity.pod_anti_affinity.preferred_during_scheduling_ignored_during_execution
    assert len(preferences) == 1
    assert preferences[0].weight == 100
    term = preferences[0].pod_affinity_term
    assert term.topology_key == "kubernetes.io/hostname"
    assert term.label_selector.match_labels == {
        "app.kubernetes.io/component": "gateway",
    }
    assert pod.spec.affinity.pod_anti_affinity.required_during_scheduling_ignored_during_execution is None


def test_provisioner_empty_accepted_pod_excludes_every_live_skill_mount(
    provisioner_module,
) -> None:
    pod = provisioner_module._build_pod(
        "sandbox-empty",
        "thread-empty",
        user_id="owner-empty",
        include_legacy_skills=True,
        accepted_skills_only=True,
    )

    assert pod.metadata.labels["hartmesh.io/accepted-skill-profile"] == "empty_only"
    assert pod.spec.init_containers is None
    assert [container.name for container in pod.spec.containers] == ["sandbox"]
    sandbox_mounts = {mount.mount_path: mount for mount in pod.spec.containers[0].volume_mounts}
    assert sandbox_mounts["/mnt/skills/.accepted"].read_only is True
    assert not any(
        path in sandbox_mounts
        for path in (
            "/mnt/skills",
            "/mnt/skills/public",
            "/mnt/skills/custom",
            "/mnt/skills/legacy",
        )
    )


def test_provisioner_rejects_an_extra_mount_alias_to_snapshot_storage(
    provisioner_module,
) -> None:
    evidence = _projection_evidence()
    provisioner_module.USERDATA_PVC_NAME = "home-rwx"
    provisioner_module.ACCEPTED_SKILL_PROJECTION_PROFILE = "rwx_verified_copy_v1"
    provisioner_module.ACCEPTED_SKILL_RUNTIME_IMAGE = "registry.example/provisioner@sha256:" + ("a" * 64)
    provisioner_module.SANDBOX_IMAGE = "registry.example/aio@sha256:" + ("b" * 64)
    projection = provisioner_module.AcceptedSkillProjectionV1(
        profile="rwx_verified_copy_v1",
        snapshot_id=evidence.snapshot_id,
        content_digest=evidence.content_digest,
        run_id="run-1",
        generation=7,
        projections=[item.to_json() for item in evidence.projections],
        file_count=evidence.file_count,
        total_bytes=evidence.total_bytes,
    )

    with pytest.raises(
        provisioner_module.HTTPException,
        match="accepted skill source alias",
    ):
        provisioner_module._build_pod(
            "sandbox-alias",
            "thread-1",
            user_id="owner-1",
            extra_mounts=[
                provisioner_module.ExtraMount(
                    host_path="/.deer-flow/runtime",
                    container_path="/mnt/integrations/lark-cli/data",
                    read_only=False,
                )
            ],
            accepted_skill_projection=projection,
            attempt_capability="A" * 43,
        )


def test_provisioner_rejects_verified_copy_without_rwx_source_or_pinned_runtime(
    provisioner_module,
) -> None:
    evidence = _projection_evidence()
    projection = provisioner_module.AcceptedSkillProjectionV1(
        profile="rwx_verified_copy_v1",
        snapshot_id=evidence.snapshot_id,
        content_digest=evidence.content_digest,
        run_id="run-1",
        generation=1,
        projections=[item.to_json() for item in evidence.projections],
        file_count=1,
        total_bytes=evidence.total_bytes,
    )
    provisioner_module.ACCEPTED_SKILL_PROJECTION_PROFILE = "rwx_verified_copy_v1"
    provisioner_module.USERDATA_PVC_NAME = ""
    provisioner_module.ACCEPTED_SKILL_RUNTIME_IMAGE = "registry.example/provisioner@sha256:" + ("a" * 64)
    with pytest.raises(provisioner_module.HTTPException, match="RWX home PVC"):
        provisioner_module._build_pod(
            "sandbox-1",
            "thread-1",
            accepted_skill_projection=projection,
            attempt_capability="A" * 43,
        )

    provisioner_module.USERDATA_PVC_NAME = "home-rwx"
    provisioner_module.ACCEPTED_SKILL_RUNTIME_IMAGE = "registry.example/provisioner:latest"
    with pytest.raises(provisioner_module.HTTPException, match="digest-pinned"):
        provisioner_module._build_pod(
            "sandbox-1",
            "thread-1",
            accepted_skill_projection=projection,
            attempt_capability="A" * 43,
        )


def test_provisioner_readiness_requires_the_configured_claim_to_be_rwx(
    provisioner_module,
) -> None:
    class Core:
        def __init__(self, access_modes: list[str]) -> None:
            self._access_modes = access_modes

        def read_namespaced_persistent_volume_claim(
            self,
            name: str,
            namespace: str,
        ):
            assert name == "home-rwx"
            assert namespace == provisioner_module.K8S_NAMESPACE
            return type(
                "PersistentVolumeClaim",
                (),
                {
                    "spec": type(
                        "Spec",
                        (),
                        {"access_modes": self._access_modes},
                    )(),
                    "status": type("Status", (), {"phase": "Bound"})(),
                },
            )()

    provisioner_module.ACCEPTED_SKILL_PROJECTION_PROFILE = "rwx_verified_copy_v1"
    provisioner_module.ACCEPTED_SKILL_RUNTIME_IMAGE = "registry.example/provisioner@sha256:" + ("a" * 64)
    provisioner_module.SANDBOX_IMAGE = "registry.example/aio@sha256:" + ("b" * 64)
    provisioner_module.USERDATA_PVC_NAME = "home-rwx"
    provisioner_module.PROVISIONER_API_KEY = "test-management-key"
    provisioner_module.networking_v1 = object()
    provisioner_module.coordination_v1 = object()

    provisioner_module.core_v1 = Core(["ReadWriteOnce"])
    with pytest.raises(provisioner_module.HTTPException) as rejected:
        provisioner_module.readiness()
    assert rejected.value.status_code == 503
    assert rejected.value.detail == "accepted_skill_pvc_not_rwx"

    provisioner_module.core_v1 = Core(["ReadWriteMany"])
    assert provisioner_module.readiness() == {"status": "ready"}


def test_provisioner_readiness_requires_management_auth_for_all_profiles(
    provisioner_module,
) -> None:
    provisioner_module.core_v1 = object()
    provisioner_module.networking_v1 = object()
    provisioner_module.coordination_v1 = object()
    provisioner_module.ACCEPTED_SKILL_PROJECTION_PROFILE = "disabled"
    provisioner_module.PROVISIONER_API_KEY = ""
    provisioner_module.PROVISIONER_AUTH_AUDIENCE = ""
    provisioner_module.PROVISIONER_GATEWAY_NAMESPACE = ""
    provisioner_module.PROVISIONER_GATEWAY_SERVICE_ACCOUNT = ""

    with pytest.raises(provisioner_module.HTTPException) as rejected:
        provisioner_module.readiness()

    assert rejected.value.status_code == 503
    assert rejected.value.detail == "provisioner_management_auth_unavailable"


def test_remote_aio_advertises_immutable_only_for_an_exact_verified_receipt() -> None:
    from deerflow.community.aio_sandbox.aio_sandbox_provider import (
        AioSandboxProvider,
    )

    evidence = _projection_evidence()
    provider = AioSandboxProvider.__new__(AioSandboxProvider)
    provider._backend = RemoteSandboxBackend("http://provisioner:8002")
    provider._lock = threading.Lock()
    provider._accepted_only_sandbox_ids = {"sandbox-1"}
    provider._active_sandbox_identity = {"sandbox-1": ("owner-1", "thread-1")}
    provider._thread_sandboxes = {("owner-1", "thread-1"): "sandbox-1"}
    provider._sandbox_infos = {
        "sandbox-1": SandboxInfo(
            sandbox_id="sandbox-1",
            sandbox_url="http://10.0.0.8:8081",
        )
    }

    assert provider.accepted_skill_material_capability("sandbox-1") is AcceptedSkillMaterialCapability.EMPTY_ONLY

    provider._sandbox_infos["sandbox-1"].accepted_skill_material = AcceptedSkillMaterialReceiptV1(
        profile="rwx_verified_copy_v1",
        attempt_id="sandbox-sandbox-1-accepted-attempt",
        snapshot_id=evidence.snapshot_id,
        content_digest=evidence.content_digest,
        run_id="run-1",
        generation=7,
        pod_uid="pod-uid-1",
        lease_uid="lease-uid-1",
        runtime_image_ids_digest="d" * 64,
        verifier_receipt_digest="e" * 64,
        materialization_evidence_digest="c" * 64,
    )
    binding = AcceptedSkillSandboxBindingV1(
        snapshot_id=evidence.snapshot_id,
        run_id="run-1",
        generation=7,
        evidence=evidence,
    )

    provider.bind_accepted_skill_snapshot(
        "sandbox-1",
        thread_id="thread-1",
        user_id="owner-1",
        binding=binding,
    )
    assert provider.accepted_skill_material_capability("sandbox-1") is AcceptedSkillMaterialCapability.IMMUTABLE_READ_ONLY

    with pytest.raises(
        AcceptedSkillSandboxBindingError,
        match="accepted_skill_snapshot_receipt_mismatch",
    ):
        provider.bind_accepted_skill_snapshot(
            "sandbox-1",
            thread_id="thread-1",
            user_id="owner-1",
            binding=AcceptedSkillSandboxBindingV1(
                snapshot_id=evidence.snapshot_id,
                run_id="another-run",
                generation=7,
                evidence=evidence,
            ),
        )


def test_accepted_attempt_lease_is_the_owner_root_and_is_idempotent(
    provisioner_module,
) -> None:
    evidence = _projection_evidence()
    projection = provisioner_module.AcceptedSkillProjectionV1(
        profile="rwx_verified_copy_v1",
        snapshot_id=evidence.snapshot_id,
        content_digest=evidence.content_digest,
        run_id="run-1",
        generation=7,
        projections=[item.to_json() for item in evidence.projections],
        file_count=evidence.file_count,
        total_bytes=evidence.total_bytes,
    )
    provisioner_module.USERDATA_PVC_NAME = "home-rwx"
    provisioner_module.ACCEPTED_SKILL_PROJECTION_PROFILE = "rwx_verified_copy_v1"
    provisioner_module.ACCEPTED_SKILL_RUNTIME_IMAGE = "registry.example/provisioner@sha256:" + ("a" * 64)
    provisioner_module.SANDBOX_IMAGE = "registry.example/aio@sha256:" + ("b" * 64)
    created: list[object] = []

    class Coordination:
        def create_namespaced_lease(self, namespace: str, lease):
            assert namespace == provisioner_module.K8S_NAMESPACE
            if created:
                raise provisioner_module.ApiException(status=409)
            lease.metadata.uid = "lease-uid-1"
            lease.metadata.resource_version = "1"
            created.append(lease)
            return lease

        def read_namespaced_lease(self, _name: str, _namespace: str):
            return created[0]

    provisioner_module.coordination_v1 = Coordination()
    lease = provisioner_module._claim_accepted_attempt(
        "sandbox-1",
        projection,
        "A" * 43,
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )
    replayed = provisioner_module._claim_accepted_attempt(
        "sandbox-1",
        projection,
        "A" * 43,
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert replayed.metadata.uid == lease.metadata.uid == "lease-uid-1"
    assert len(created) == 1
    owner = provisioner_module._accepted_attempt_owner_reference(lease)
    assert owner.kind == "Lease"
    assert owner.uid == "lease-uid-1"
    pod = provisioner_module._build_pod(
        "sandbox-1",
        "thread-1",
        accepted_skill_projection=projection,
        attempt_capability="A" * 43,
        accepted_attempt_owner=owner,
    )
    assert pod.metadata.owner_references == [owner]


def test_expired_accepted_attempt_is_reconciled_by_exact_lease_uid(
    provisioner_module,
) -> None:
    now = datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
    expired = SimpleNamespace(
        metadata=SimpleNamespace(
            name="sandbox-old-accepted-attempt",
            uid="lease-old-uid",
            labels={"hartmesh.io/accepted-skill-attempt": "true"},
        ),
        spec=SimpleNamespace(
            renew_time=now - timedelta(minutes=5),
            acquire_time=now - timedelta(minutes=5),
            lease_duration_seconds=60,
        ),
    )
    live = SimpleNamespace(
        metadata=SimpleNamespace(
            name="sandbox-live-accepted-attempt",
            uid="lease-live-uid",
            labels={"hartmesh.io/accepted-skill-attempt": "true"},
        ),
        spec=SimpleNamespace(
            renew_time=now - timedelta(seconds=10),
            acquire_time=now - timedelta(seconds=10),
            lease_duration_seconds=60,
        ),
    )
    deleted: list[tuple[str, str]] = []

    class Coordination:
        def list_namespaced_lease(self, namespace: str, **kwargs):
            assert namespace == provisioner_module.K8S_NAMESPACE
            assert kwargs["limit"] == provisioner_module.ACCEPTED_ATTEMPT_RECONCILE_LIMIT
            return SimpleNamespace(items=[expired, live], metadata=SimpleNamespace(_continue=None))

        def delete_namespaced_lease(self, name: str, namespace: str, body):
            assert body.preconditions.uid == "lease-old-uid"
            deleted.append((name, namespace))

    provisioner_module.coordination_v1 = Coordination()
    assert provisioner_module._reconcile_expired_accepted_attempts(now=now) == 1
    assert deleted == [("sandbox-old-accepted-attempt", provisioner_module.K8S_NAMESPACE)]


def test_accepted_attempt_reconciliation_advances_across_bounded_pages(
    provisioner_module,
) -> None:
    now = datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
    live = SimpleNamespace(
        metadata=SimpleNamespace(name="live", uid="live-uid"),
        spec=SimpleNamespace(
            renew_time=now,
            acquire_time=now,
            lease_duration_seconds=60,
        ),
    )
    expired = SimpleNamespace(
        metadata=SimpleNamespace(name="expired", uid="expired-uid"),
        spec=SimpleNamespace(
            renew_time=now - timedelta(minutes=5),
            acquire_time=now - timedelta(minutes=5),
            lease_duration_seconds=60,
        ),
    )
    continuations: list[str | None] = []
    deleted: list[str] = []

    class Coordination:
        def list_namespaced_lease(self, _namespace: str, **kwargs):
            cursor = kwargs.get("_continue")
            continuations.append(cursor)
            if cursor is None:
                return SimpleNamespace(
                    items=[live],
                    metadata=SimpleNamespace(_continue="page-2"),
                )
            assert cursor == "page-2"
            return SimpleNamespace(
                items=[expired],
                metadata=SimpleNamespace(_continue=None),
            )

        def delete_namespaced_lease(
            self,
            name: str,
            _namespace: str,
            *,
            body,
        ):
            assert body.preconditions.uid == "expired-uid"
            deleted.append(name)

    provisioner_module.coordination_v1 = Coordination()
    provisioner_module._accepted_reconcile_continue = None

    assert provisioner_module._reconcile_expired_accepted_attempts(now=now) == 0
    assert provisioner_module._reconcile_expired_accepted_attempts(now=now) == 1
    assert continuations == [None, "page-2"]
    assert deleted == ["expired"]


def test_unready_accepted_attempt_cannot_bypass_destroy_fence(
    provisioner_module,
) -> None:
    pod = SimpleNamespace(
        metadata=SimpleNamespace(
            uid="pod-uid-1",
            labels={
                "hartmesh.io/accepted-skill-profile": "rwx_verified_copy_v1",
            },
            annotations={},
            owner_references=[
                SimpleNamespace(
                    kind="Lease",
                    name="sandbox-sandbox-1-accepted-attempt",
                    uid="lease-uid-1",
                )
            ],
        ),
        status=SimpleNamespace(
            phase="Pending",
            pod_ip=None,
            container_statuses=[],
            init_container_statuses=[],
        ),
    )
    deletes: list[str] = []

    class Core:
        def read_namespaced_pod(self, _name: str, _namespace: str):
            return pod

        def delete_namespaced_service(self, *_args, **_kwargs):
            deletes.append("service")

        def delete_namespaced_pod(self, *_args, **_kwargs):
            deletes.append("pod")

        def delete_namespaced_secret(self, *_args, **_kwargs):
            deletes.append("secret")

    provisioner_module.core_v1 = Core()
    provisioner_module.networking_v1 = None

    with pytest.raises(provisioner_module.HTTPException) as rejected:
        provisioner_module.destroy_sandbox("sandbox-1")

    assert rejected.value.status_code == 409
    assert rejected.value.detail == "accepted_attempt_fence_mismatch"
    assert deletes == []


def test_accepted_pod_receipt_binds_pod_lease_and_runtime_image_ids(
    provisioner_module,
) -> None:
    provisioner_module.ACCEPTED_SKILL_PROJECTION_PROFILE = "rwx_verified_copy_v1"
    provisioner_module.USERDATA_PVC_NAME = "shared-rwx"
    evidence = _projection_evidence()
    projection = provisioner_module.AcceptedSkillProjectionV1(
        profile="rwx_verified_copy_v1",
        snapshot_id=evidence.snapshot_id,
        content_digest=evidence.content_digest,
        run_id="run-1",
        generation=7,
        projections=[item.to_json() for item in evidence.projections],
        file_count=evidence.file_count,
        total_bytes=evidence.total_bytes,
    )
    provisioner_module.ACCEPTED_SKILL_RUNTIME_IMAGE = "registry.example/provisioner@sha256:" + ("a" * 64)
    provisioner_module.SANDBOX_IMAGE = "registry.example/aio@sha256:" + ("b" * 64)
    lease = provisioner_module._build_accepted_attempt_lease(
        "sandbox-1",
        projection,
        "A" * 43,
    )
    lease.metadata.uid = "lease-uid-1"
    owner = provisioner_module._accepted_attempt_owner_reference(lease)
    pod = provisioner_module._build_pod(
        "sandbox-1",
        "thread-1",
        user_id="owner-1",
        accepted_skill_projection=projection,
        attempt_capability="A" * 43,
        accepted_attempt_owner=owner,
    )
    lease.metadata.annotations["hartmesh.io/accepted-isolation-digest"] = pod.metadata.annotations["hartmesh.io/accepted-isolation-digest"]
    lease.metadata.annotations["hartmesh.io/accepted-attempt-state"] = "pod_creation_started"
    lease.metadata.annotations["hartmesh.io/accepted-pod-uid"] = "pod-uid-1"
    pod.metadata.uid = "pod-uid-1"
    pod.status = SimpleNamespace(
        phase="Pending",
        pod_ip="10.0.0.8",
        container_statuses=[],
        init_container_statuses=[],
    )
    pod.status = SimpleNamespace(
        phase="Running",
        pod_ip="10.0.0.8",
        container_statuses=[
            SimpleNamespace(
                name="sandbox",
                image_id="containerd://registry.example/aio@sha256:" + ("b" * 64),
                ready=True,
            ),
            SimpleNamespace(
                name="accepted-skill-gate",
                image_id="containerd://registry.example/provisioner@sha256:" + ("a" * 64),
                ready=True,
            ),
        ],
        init_container_statuses=[
            SimpleNamespace(
                name="accepted-skill-verifier",
                image_id="containerd://registry.example/provisioner@sha256:" + ("a" * 64),
                state=SimpleNamespace(
                    terminated=SimpleNamespace(exit_code=0),
                ),
            )
        ],
    )
    response = provisioner_module._accepted_pod_response(
        "sandbox-1",
        expected=projection,
        expected_capability="A" * 43,
        expected_lease_uid="lease-uid-1",
        attempt_lease=lease,
        pod=pod,
        verifier_receipt={
            "version": 1,
            "profile": "rwx_verified_copy_v1",
            "snapshot_id": evidence.snapshot_id,
            "content_digest": evidence.content_digest,
            "file_count": evidence.file_count,
            "total_bytes": evidence.total_bytes,
        },
    )

    assert response is not None
    receipt = response.accepted_skill_material
    assert receipt is not None
    assert receipt["pod_uid"] == "pod-uid-1"
    assert receipt["lease_uid"] == "lease-uid-1"
    assert (
        receipt["verifier_receipt_digest"]
        == hashlib.sha256(
            json.dumps(
                {
                    "version": 1,
                    "profile": "rwx_verified_copy_v1",
                    "snapshot_id": evidence.snapshot_id,
                    "content_digest": evidence.content_digest,
                    "file_count": evidence.file_count,
                    "total_bytes": evidence.total_bytes,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
    )
    assert (
        receipt["runtime_image_ids_digest"]
        == hashlib.sha256(
            json.dumps(
                {
                    "accepted-skill-gate": pod.status.container_statuses[1].image_id,
                    "accepted-skill-verifier": pod.status.init_container_statuses[0].image_id,
                    "sandbox": pod.status.container_statuses[0].image_id,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
    )
    materialization = dict(receipt)
    materialization_digest = materialization.pop("materialization_evidence_digest")
    assert (
        materialization_digest
        == hashlib.sha256(
            json.dumps(
                materialization,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
    )

    pod.status.container_statuses[0].image_id += "-untrusted-suffix"
    with pytest.raises(provisioner_module.HTTPException) as rejected:
        provisioner_module._accepted_pod_response(
            "sandbox-1",
            expected=projection,
            expected_capability="A" * 43,
            expected_lease_uid="lease-uid-1",
            attempt_lease=lease,
            pod=pod,
            verifier_receipt={
                "version": 1,
                "profile": "rwx_verified_copy_v1",
                "snapshot_id": evidence.snapshot_id,
                "content_digest": evidence.content_digest,
                "file_count": evidence.file_count,
                "total_bytes": evidence.total_bytes,
            },
        )
    assert rejected.value.detail == "accepted_attempt_image_identity_mismatch"


def test_accepted_pod_response_rejects_mutated_isolation_spec(
    provisioner_module,
) -> None:
    provisioner_module.ACCEPTED_SKILL_PROJECTION_PROFILE = "rwx_verified_copy_v1"
    provisioner_module.USERDATA_PVC_NAME = "shared-rwx"
    provisioner_module.ACCEPTED_SKILL_RUNTIME_IMAGE = "registry.example/provisioner@sha256:" + ("a" * 64)
    provisioner_module.SANDBOX_IMAGE = "registry.example/aio@sha256:" + ("b" * 64)
    evidence = _projection_evidence()
    projection = provisioner_module.AcceptedSkillProjectionV1(
        profile="rwx_verified_copy_v1",
        snapshot_id=evidence.snapshot_id,
        content_digest=evidence.content_digest,
        run_id="run-1",
        generation=7,
        projections=[item.to_json() for item in evidence.projections],
        file_count=evidence.file_count,
        total_bytes=evidence.total_bytes,
    )
    lease = provisioner_module._build_accepted_attempt_lease(
        "sandbox-1",
        projection,
        "A" * 43,
    )
    lease.metadata.uid = "lease-uid-1"
    pod = provisioner_module._build_pod(
        "sandbox-1",
        "thread-1",
        user_id="owner-1",
        accepted_skill_projection=projection,
        attempt_capability="A" * 43,
        accepted_attempt_owner=provisioner_module._accepted_attempt_owner_reference(lease),
    )
    isolation_digest = pod.metadata.annotations["hartmesh.io/accepted-isolation-digest"]
    lease.metadata.annotations["hartmesh.io/accepted-isolation-digest"] = isolation_digest
    lease.metadata.annotations["hartmesh.io/accepted-attempt-state"] = "pod_creation_started"
    lease.metadata.annotations["hartmesh.io/accepted-pod-uid"] = "pod-uid-1"
    pod.metadata.uid = "pod-uid-1"
    pod.status = SimpleNamespace(
        phase="Pending",
        pod_ip="10.0.0.8",
        container_statuses=[],
        init_container_statuses=[],
    )
    accepted_mount = next(mount for mount in pod.spec.containers[0].volume_mounts if mount.mount_path == "/mnt/skills/.accepted")
    accepted_mount.read_only = False

    with pytest.raises(provisioner_module.HTTPException) as rejected:
        provisioner_module._accepted_pod_response(
            "sandbox-1",
            expected=projection,
            expected_capability="A" * 43,
            expected_lease_uid="lease-uid-1",
            attempt_lease=lease,
            pod=pod,
        )

    assert rejected.value.status_code == 409
    assert rejected.value.detail == "accepted_attempt_pod_spec_mismatch"


def test_accepted_pod_url_brackets_ipv6(provisioner_module) -> None:
    assert provisioner_module._accepted_pod_url("2001:db8::8") == "http://[2001:db8::8]:8081"


def test_accepted_attempt_replay_with_another_capability_fails_closed(
    provisioner_module,
) -> None:
    evidence = _projection_evidence()
    projection = provisioner_module.AcceptedSkillProjectionV1(
        profile="rwx_verified_copy_v1",
        snapshot_id=evidence.snapshot_id,
        content_digest=evidence.content_digest,
        run_id="run-1",
        generation=7,
        projections=[item.to_json() for item in evidence.projections],
        file_count=evidence.file_count,
        total_bytes=evidence.total_bytes,
    )
    lease = provisioner_module._build_accepted_attempt_lease(
        "sandbox-1",
        projection,
        "A" * 43,
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )
    lease.metadata.uid = "lease-uid-1"
    lease.metadata.resource_version = "1"

    class Coordination:
        def create_namespaced_lease(self, _namespace: str, _lease):
            raise provisioner_module.ApiException(status=409)

        def read_namespaced_lease(self, _name: str, _namespace: str):
            return lease

    provisioner_module.coordination_v1 = Coordination()
    with pytest.raises(provisioner_module.HTTPException) as conflict:
        provisioner_module._claim_accepted_attempt(
            "sandbox-1",
            projection,
            "B" * 43,
            now=datetime(2026, 1, 1, tzinfo=UTC),
        )
    assert conflict.value.status_code == 409
    assert conflict.value.detail == "accepted_attempt_identity_conflict"


def test_accepted_attempt_allows_exactly_one_pod_creation_transition(
    provisioner_module,
) -> None:
    evidence = _projection_evidence()
    projection = provisioner_module.AcceptedSkillProjectionV1(
        profile="rwx_verified_copy_v1",
        snapshot_id=evidence.snapshot_id,
        content_digest=evidence.content_digest,
        run_id="run-1",
        generation=7,
        projections=[item.to_json() for item in evidence.projections],
        file_count=evidence.file_count,
        total_bytes=evidence.total_bytes,
    )
    lease = provisioner_module._build_accepted_attempt_lease(
        "sandbox-1",
        projection,
        "A" * 43,
    )
    lease.metadata.uid = "lease-uid-1"

    class Coordination:
        def replace_namespaced_lease(self, _name: str, _namespace: str, updated):
            updated.metadata.resource_version = "2"
            return updated

    provisioner_module.coordination_v1 = Coordination()
    started, should_create = provisioner_module._prepare_accepted_pod_creation(
        lease,
    )
    replayed, should_recreate = provisioner_module._prepare_accepted_pod_creation(
        started,
    )

    assert should_create is True
    assert should_recreate is False
    assert replayed.metadata.annotations["hartmesh.io/accepted-attempt-state"] == ("pod_creation_started")


def test_accepted_attempt_renewal_is_fenced_by_exact_materialization(
    provisioner_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 1, 1, tzinfo=UTC)
    materialization_digest = "d" * 64
    receipt = {
        "attempt_id": "sandbox-sandbox-1-accepted-attempt",
        "pod_uid": "pod-uid-1",
        "lease_uid": "lease-uid-1",
        "materialization_evidence_digest": materialization_digest,
    }
    monkeypatch.setattr(
        provisioner_module,
        "_accepted_pod_response",
        lambda *_args, **_kwargs: provisioner_module.SandboxResponse(
            sandbox_id="sandbox-1",
            sandbox_url="http://10.0.0.8:8081",
            status="Running",
            accepted_skill_material=receipt,
        ),
    )
    lease = SimpleNamespace(
        metadata=SimpleNamespace(
            name=receipt["attempt_id"],
            uid="lease-uid-1",
            resource_version="4",
        ),
        spec=SimpleNamespace(
            acquire_time=observed_at - timedelta(seconds=10),
            renew_time=observed_at - timedelta(seconds=10),
            lease_duration_seconds=120,
        ),
    )
    replaced: list[object] = []

    class Coordination:
        def read_namespaced_lease(self, name: str, _namespace: str):
            assert name == receipt["attempt_id"]
            return lease

        def replace_namespaced_lease(
            self,
            name: str,
            namespace: str,
            updated,
        ):
            assert name == receipt["attempt_id"]
            assert namespace == provisioner_module.K8S_NAMESPACE
            replaced.append(updated)

    provisioner_module.coordination_v1 = Coordination()
    request = provisioner_module.RenewAcceptedAttemptRequest(
        pod_uid="pod-uid-1",
        lease_uid="lease-uid-1",
        materialization_evidence_digest=materialization_digest,
    )

    provisioner_module._renew_accepted_attempt(
        "sandbox-1",
        request,
        now=observed_at,
    )

    assert replaced == [lease]
    assert lease.spec.renew_time == observed_at

    with pytest.raises(provisioner_module.HTTPException) as stale:
        provisioner_module._renew_accepted_attempt(
            "sandbox-1",
            request.model_copy(update={"pod_uid": "replaced-pod"}),
            now=observed_at,
        )
    assert stale.value.status_code == 409
    assert stale.value.detail == "accepted_attempt_fence_mismatch"


def test_accepted_secret_cleanup_never_deletes_another_lease_owner(
    provisioner_module,
) -> None:
    resources = {
        provisioner_module._accepted_evidence_secret_name("sandbox-1"): (
            "secret-evidence-uid",
            "lease-uid-1",
        ),
        provisioner_module._accepted_capability_secret_name("sandbox-1"): (
            "secret-capability-uid",
            "replacement-lease-uid",
        ),
    }
    deleted: list[tuple[str, str]] = []

    class Core:
        def read_namespaced_secret(self, name: str, _namespace: str):
            secret_uid, owner_uid = resources[name]
            return SimpleNamespace(
                metadata=SimpleNamespace(
                    uid=secret_uid,
                    owner_references=[
                        SimpleNamespace(kind="Lease", uid=owner_uid),
                    ],
                )
            )

        def delete_namespaced_secret(self, name: str, _namespace: str, body):
            deleted.append((name, body.preconditions.uid))

    provisioner_module.core_v1 = Core()

    provisioner_module._delete_accepted_secrets(
        "sandbox-1",
        expected_owner_uid="lease-uid-1",
    )

    assert deleted == [
        (
            provisioner_module._accepted_evidence_secret_name("sandbox-1"),
            "secret-evidence-uid",
        )
    ]


def test_capability_gate_rejects_wrong_identity_and_proxies_once(
    tmp_path: Path,
) -> None:
    verifier = _load_verifier_module()
    upstream_calls: list[tuple[str, bytes, str | None]] = []

    class Upstream(verifier.http.server.BaseHTTPRequestHandler):
        def log_message(self, _format: str, *_args: object) -> None:
            return None

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            upstream_calls.append(
                (
                    self.path,
                    self.rfile.read(length),
                    self.headers.get("Authorization"),
                )
            )
            payload = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    upstream = verifier.http.server.ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
    upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    upstream_thread.start()
    capability_file = tmp_path / "capability"
    capability_file.write_text("A" * 43, encoding="utf-8")
    receipt_file = tmp_path / "receipt.json"
    receipt = {
        "version": 1,
        "profile": "rwx_verified_copy_v1",
        "snapshot_id": "a" * 64,
        "content_digest": "a" * 64,
        "file_count": 1,
        "total_bytes": 10,
    }
    verifier.write_materialization_receipt(receipt_file, receipt)
    gate_port_socket = socket.socket()
    gate_port_socket.bind(("127.0.0.1", 0))
    gate_port = gate_port_socket.getsockname()[1]
    gate_port_socket.close()
    gate_thread = threading.Thread(
        target=verifier.serve_gate,
        kwargs={
            "listen_host": "127.0.0.1",
            "listen_port": gate_port,
            "upstream": f"http://127.0.0.1:{upstream.server_port}",
            "capability_file": capability_file,
            "receipt_file": receipt_file,
        },
        daemon=True,
    )
    gate_thread.start()

    wrong = urllib.request.Request(
        f"http://127.0.0.1:{gate_port}/v1/test",
        data=b"payload",
        headers={"Authorization": "Bearer " + ("B" * 43)},
        method="POST",
    )
    with pytest.raises(urllib.error.HTTPError) as rejected:
        urllib.request.urlopen(wrong, timeout=2)
    assert rejected.value.code == 403
    assert upstream_calls == []

    allowed = urllib.request.Request(
        f"http://127.0.0.1:{gate_port}/v1/test",
        data=b"payload",
        headers={"Authorization": "Bearer " + ("A" * 43)},
        method="POST",
    )
    with urllib.request.urlopen(allowed, timeout=2) as response:
        assert response.read() == b'{"ok":true}'
    receipt_request = urllib.request.Request(
        f"http://127.0.0.1:{gate_port}/__hartmesh/accepted-material/v1",
        headers={"Authorization": "Bearer " + ("A" * 43)},
        method="GET",
    )
    with urllib.request.urlopen(receipt_request, timeout=2) as response:
        assert json.loads(response.read()) == receipt
    assert upstream_calls == [("/v1/test", b"payload", None)]
    upstream.shutdown()


@pytest.mark.asyncio
async def test_started_transition_atomically_binds_materialization_evidence() -> None:
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create_or_reject("thread-evidence")
    evidence = AcceptedSkillExecutionEvidenceV1(
        profile="rwx_verified_copy_v1",
        attempt_id="sandbox-run-1-accepted-attempt",
        snapshot_id="a" * 64,
        run_id=record.run_id,
        generation=4,
        pod_uid="pod-uid-1",
        lease_uid="lease-uid-1",
        runtime_image_ids_digest="c" * 64,
        verifier_receipt_digest="d" * 64,
        materialization_evidence_digest="b" * 64,
    )

    assert await manager.try_start(record.run_id, execution_evidence=evidence) is RunStartOutcome.started
    row = await store.get(record.run_id)
    events = await store.list_lifecycle_events()

    assert row["status"] == "running"
    assert row["execution_evidence_json"] == evidence.to_persisted()
    assert row["execution_evidence_digest"] == evidence.digest
    assert events[-1]["lifecycle_type"] == "started"
    assert events[-1]["payload"]["execution_evidence_digest"] == evidence.digest


@pytest.mark.asyncio
async def test_started_transition_rejects_evidence_for_another_run() -> None:
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create_or_reject("thread-evidence")
    evidence = AcceptedSkillExecutionEvidenceV1(
        profile="rwx_verified_copy_v1",
        attempt_id="sandbox-other-accepted-attempt",
        snapshot_id="a" * 64,
        run_id="another-run",
        generation=4,
        pod_uid="pod-uid-1",
        lease_uid="lease-uid-1",
        runtime_image_ids_digest="c" * 64,
        verifier_receipt_digest="d" * 64,
        materialization_evidence_digest="b" * 64,
    )

    with pytest.raises(RunStartupError, match="different run"):
        await manager.try_start(record.run_id, execution_evidence=evidence)

    row = await store.get(record.run_id)
    assert row["status"] == "pending"
    assert row["execution_evidence_json"] is None
    assert not any(event["lifecycle_type"] == "started" for event in await store.list_lifecycle_events())


@pytest.mark.asyncio
async def test_store_rejects_cross_run_execution_evidence_before_transition() -> None:
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create_or_reject("thread-evidence")
    evidence = AcceptedSkillExecutionEvidenceV1(
        profile="rwx_verified_copy_v1",
        attempt_id="sandbox-other-accepted-attempt",
        snapshot_id="a" * 64,
        run_id="another-run",
        generation=4,
        pod_uid="pod-uid-1",
        lease_uid="lease-uid-1",
        runtime_image_ids_digest="c" * 64,
        verifier_receipt_digest="d" * 64,
        materialization_evidence_digest="b" * 64,
    )

    with pytest.raises(ValueError, match="different run"):
        await store.start_run(
            record.run_id,
            execution_evidence_json=evidence.to_persisted(),
            execution_evidence_digest=evidence.digest,
        )

    assert (await store.get(record.run_id))["status"] == "pending"


@pytest.mark.asyncio
async def test_accepted_attempt_renews_only_after_authoritative_run_lease() -> None:
    store = MemoryRunStore()
    manager = RunManager(
        store=store,
        run_ownership_config=RunOwnershipConfig(heartbeat_enabled=True),
    )
    record = await manager.create_or_reject("thread-evidence")
    evidence = AcceptedSkillExecutionEvidenceV1(
        profile="rwx_verified_copy_v1",
        attempt_id="sandbox-attempt",
        snapshot_id="a" * 64,
        run_id=record.run_id,
        generation=4,
        pod_uid="pod-uid-1",
        lease_uid="lease-uid-1",
        runtime_image_ids_digest="c" * 64,
        verifier_receipt_digest="d" * 64,
        materialization_evidence_digest="b" * 64,
    )
    assert (
        await manager.try_start(
            record.run_id,
            execution_evidence=evidence,
        )
        is RunStartOutcome.started
    )
    renew = AsyncMock(return_value=True)
    await manager.set_execution_lease_renewal(record.run_id, renew)

    await manager._renew_leases()

    renew.assert_awaited_once_with()
    assert record.ownership_lost is False


@pytest.mark.asyncio
async def test_rejected_accepted_attempt_renewal_fences_the_running_worker() -> None:
    store = MemoryRunStore()
    manager = RunManager(
        store=store,
        run_ownership_config=RunOwnershipConfig(heartbeat_enabled=True),
    )
    record = await manager.create_or_reject("thread-evidence")
    assert await manager.try_start(record.run_id) is RunStartOutcome.started
    renew = AsyncMock(return_value=False)
    await manager.set_execution_lease_renewal(record.run_id, renew)

    await manager._renew_leases()

    renew.assert_awaited_once_with()
    assert record.ownership_lost is True
    assert record.abort_event.is_set()
