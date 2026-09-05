"""Tests for AioSandboxProvider mount helpers."""

import asyncio
import contextlib
import hashlib
import importlib
import stat
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from deerflow.config.paths import Paths, join_host_path
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.sandbox.accepted_material import (
    AcceptedSkillSandboxBindingError,
    AcceptedSkillSandboxBindingV1,
)
from deerflow.sandbox.acquire_serialization import AcquireSerializer


def _accepted_runtime_topology():
    from deerflow.qualification_evidence import (
        AcceptedSandboxPersistentVolumeTopologyV1,
        AcceptedSandboxRuntimeTopologyV1,
    )

    return AcceptedSandboxRuntimeTopologyV1(
        provider_kind="aio_kubernetes",
        profile="rwx_verified_copy_v2",
        sandbox_image_digest="c" * 64,
        verifier_image_digest="b" * 64,
        namespace_uid="namespace-uid",
        pod_security_enforce=None,
        pod_security_warn=None,
        pod_security_audit=None,
        runtime_class=None,
        gateway_namespace="runtime",
        gateway_service_account="gateway",
        token_review_audience="hartmesh-provisioner",
        accepted_attempt_lease_seconds=120,
        accepted_attempt_reconcile_interval_seconds=30,
        accepted_attempt_reconcile_limit=100,
        volumes=(
            AcceptedSandboxPersistentVolumeTopologyV1(
                role="skills",
                uid="skills-uid",
                volume_name="skills-volume",
                storage_class="rwx-storage",
                access_modes=("ReadWriteMany",),
            ),
            AcceptedSandboxPersistentVolumeTopologyV1(
                role="userdata",
                uid="userdata-uid",
                volume_name="userdata-volume",
                storage_class="rwx-storage",
                access_modes=("ReadWriteMany",),
            ),
        ),
    )


def test_batch_attempt_resource_scopes_do_not_alias_parent_or_siblings() -> None:
    aio_mod = importlib.import_module(
        "deerflow.community.aio_sandbox.aio_sandbox_provider",
    )

    parent = aio_mod.AioSandboxProvider._accepted_resource_thread_id(
        "thread-1",
        None,
    )
    first = aio_mod.AioSandboxProvider._accepted_resource_thread_id(
        "thread-1",
        "batch-child-first",
    )
    second = aio_mod.AioSandboxProvider._accepted_resource_thread_id(
        "thread-1",
        "batch-child-second",
    )

    assert parent == "thread-1"
    assert len({parent, first, second}) == 3
    assert "batch-child" not in first


@pytest.mark.asyncio
async def test_cancelled_bound_acquire_waits_for_created_sandbox_cleanup() -> None:
    aio_mod = importlib.import_module(
        "deerflow.community.aio_sandbox.aio_sandbox_provider",
    )
    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)
    resource_created = threading.Event()
    allow_return = threading.Event()
    destroy_started = threading.Event()
    allow_destroy = threading.Event()
    destroyed: list[str] = []

    def acquire(*_args) -> str:
        resource_created.set()
        assert allow_return.wait(timeout=2)
        return "created-before-cancellation"

    provider._provision_accepted_skills_with_claim = acquire

    def destroy(sandbox_id: str) -> None:
        destroy_started.set()
        assert allow_destroy.wait(timeout=2)
        destroyed.append(sandbox_id)

    provider.destroy = destroy
    binding = AcceptedSkillSandboxBindingV1(
        snapshot_id="a" * 64,
        run_id="run-cancelled-acquire",
        generation=1,
    )
    task = asyncio.create_task(
        provider.provision_accepted_skills_async(
            "thread-1",
            user_id="user-1",
            binding=binding,
        ),
    )
    assert await asyncio.to_thread(resource_created.wait, 2)

    task.cancel("cancel after backend creation")
    allow_return.set()
    assert await asyncio.to_thread(destroy_started.wait, 2)
    task.cancel("second cancellation during cleanup")
    allow_destroy.set()

    with pytest.raises(asyncio.CancelledError, match="cancel after backend creation"):
        await task
    assert destroyed == ["created-before-cancellation"]


def test_accepted_material_qualification_config_requires_a_pinned_pair() -> None:
    provider = "deerflow.community.aio_sandbox:AioSandboxProvider"

    with pytest.raises(ValidationError, match="qualification evidence and digest"):
        SandboxConfig(
            use=provider,
            accepted_material_qualification_evidence="/qualification/evidence.json",
        )
    with pytest.raises(ValidationError, match="qualification evidence and digest"):
        SandboxConfig(
            use=provider,
            accepted_material_qualification_digest="sha256:" + ("a" * 64),
        )

    config = SandboxConfig(
        use=provider,
        accepted_material_qualification_evidence="/qualification/evidence.json",
        accepted_material_qualification_digest="sha256:" + ("a" * 64),
        accepted_material_qualification_max_age_seconds=86400,
    )
    assert config.accepted_material_qualification_max_age_seconds == 86400


def test_aio_provider_loads_the_qualification_tuple(monkeypatch) -> None:
    aio_mod = importlib.import_module(
        "deerflow.community.aio_sandbox.aio_sandbox_provider",
    )
    sandbox_config = SandboxConfig(
        use="deerflow.community.aio_sandbox:AioSandboxProvider",
        accepted_material_qualification_evidence="/qualification/evidence.json",
        accepted_material_qualification_digest="sha256:" + ("a" * 64),
        accepted_material_qualification_max_age_seconds=86400,
    )
    monkeypatch.setattr(
        aio_mod,
        "get_app_config",
        lambda: SimpleNamespace(
            sandbox=sandbox_config,
            stream_bridge=None,
            skills=SimpleNamespace(container_path="/mnt/skills"),
        ),
    )
    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)

    loaded = provider._load_config()

    assert loaded["accepted_material_qualification_evidence"] == ("/qualification/evidence.json")
    assert loaded["accepted_material_qualification_digest"] == ("sha256:" + ("a" * 64))
    assert loaded["accepted_material_qualification_max_age_seconds"] == 86400


@pytest.mark.asyncio
async def test_aio_qualification_rejects_an_unpinned_or_replaced_artifact(
    tmp_path,
    monkeypatch,
) -> None:
    from deerflow.qualification_evidence import (
        ACCEPTED_SANDBOX_OPERATION_FENCING_MODE_V1,
        ACCEPTED_SKILL_QUALIFICATION_SCENARIOS_V2,
        AcceptedSandboxPersistentVolumeTopologyV1,
        AcceptedSandboxQualificationArtifactV1,
        AcceptedSandboxRaceEvidenceV1,
        AcceptedSandboxRuntimeTopologyV1,
        AcceptedSkillMaterialEvidenceV2,
        AcceptedSkillQualificationEnvironmentV2,
        AcceptedSkillScenarioEvidenceV2,
        KubernetesAcceptedSkillQualificationEvidenceV2,
        qualification_evidence_digest,
    )
    from deerflow.sandbox.accepted_material import AcceptedMaterialError

    aio_mod = importlib.import_module(
        "deerflow.community.aio_sandbox.aio_sandbox_provider",
    )
    now = datetime.now(UTC)
    subordinate = KubernetesAcceptedSkillQualificationEvidenceV2(
        qualification_id="accepted-sandbox-current",
        gateway_image_reference="registry.example/gateway@sha256:" + ("a" * 64),
        gateway_image_digest="sha256:" + ("a" * 64),
        provisioner_image_reference="registry.example/provisioner@sha256:" + ("b" * 64),
        provisioner_image_digest="sha256:" + ("b" * 64),
        verifier_image_reference="registry.example/provisioner@sha256:" + ("b" * 64),
        verifier_image_digest="sha256:" + ("b" * 64),
        sandbox_image_reference="registry.example/sandbox@sha256:" + ("c" * 64),
        sandbox_image_digest="sha256:" + ("c" * 64),
        chart_version="2.1.0",
        chart_digest="sha256:" + ("d" * 64),
        configuration_digest="sha256:" + ("e" * 64),
        migration_head="0032_test",
        environment=AcceptedSkillQualificationEnvironmentV2(
            kubernetes_server_version="v1.33.1",
            cluster_context="qualification-context",
            cluster_driver="kind",
            namespace="hartmesh-qualification-a1b2c3",
            schedulable_nodes=("worker-a", "worker-b"),
            gateway_node="worker-a",
            sandbox_node="worker-b",
            rwx_storage_class="rwx-storage",
            rwx_volume_uid="pvc-uid",
            token_review_authenticated=True,
            gateway_service_account="hartmesh-gateway",
            lease_uid="lease-uid",
            lease_renewals=2,
        ),
        material=AcceptedSkillMaterialEvidenceV2(
            skill_name="qualification-skill",
            snapshot_digest="sha256:" + ("1" * 64),
            skill_tree_digest="sha256:" + ("2" * 64),
            allowed_tool_policy_digest="sha256:" + ("3" * 64),
            file_count=2,
            total_bytes=192,
            materialization_digest="sha256:" + ("4" * 64),
            receipt_digest="sha256:" + ("5" * 64),
        ),
        scenarios=tuple(
            AcceptedSkillScenarioEvidenceV2(
                name=name,
                run_id=f"run-{index}",
                result_digest="sha256:" + (f"{index + 6:x}" * 64),
                replacement_observed=name in {"gateway_replacement_cleanup", "process_loss_cleanup"},
                cleanup_outcome="deleted",
            )
            for index, name in enumerate(
                ACCEPTED_SKILL_QUALIFICATION_SCENARIOS_V2,
            )
        ),
        completed_at=now - timedelta(minutes=1),
    )
    profile = aio_mod.AioSandboxProvider.accepted_sandbox_capability_profile()
    topology = AcceptedSandboxRuntimeTopologyV1(
        provider_kind="aio_kubernetes",
        profile="rwx_verified_copy_v2",
        sandbox_image_digest="c" * 64,
        verifier_image_digest="b" * 64,
        namespace_uid="namespace-uid",
        pod_security_enforce=None,
        pod_security_warn=None,
        pod_security_audit=None,
        runtime_class=None,
        gateway_namespace="runtime",
        gateway_service_account="gateway",
        token_review_audience="hartmesh-provisioner",
        accepted_attempt_lease_seconds=120,
        accepted_attempt_reconcile_interval_seconds=30,
        accepted_attempt_reconcile_limit=100,
        volumes=(
            AcceptedSandboxPersistentVolumeTopologyV1(
                role="skills",
                uid="skills-uid",
                volume_name="skills-volume",
                storage_class="rwx-storage",
                access_modes=("ReadWriteMany",),
            ),
            AcceptedSandboxPersistentVolumeTopologyV1(
                role="userdata",
                uid="userdata-uid",
                volume_name="userdata-volume",
                storage_class="rwx-storage",
                access_modes=("ReadWriteMany",),
            ),
        ),
    )
    evidence = AcceptedSandboxQualificationArtifactV1(
        qualification_id=subordinate.qualification_id,
        accepted_skill_evidence=subordinate,
        accepted_skill_evidence_digest=qualification_evidence_digest(
            subordinate.canonical_bytes(),
        ),
        provider_kind="aio_kubernetes",
        capability_profile_version=profile.version,
        capability_profile_digest=profile.digest,
        operation_fencing_mode=ACCEPTED_SANDBOX_OPERATION_FENCING_MODE_V1,
        topology_policy_digest=topology.qualification_policy_digest,
        race=AcceptedSandboxRaceEvidenceV1(
            session_validation_passes=1,
            raced_provider_calls=1,
            post_loss_rejections=1,
            stale_terminal_rejected=True,
            cleanup_outcome="deleted",
        ),
        completed_at=subordinate.completed_at,
    )
    artifact = tmp_path / "qualification.json"
    payload = evidence.canonical_bytes()
    artifact.write_bytes(payload)
    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)
    provider._config = {
        "accepted_material_qualification_evidence": str(artifact),
        "accepted_material_qualification_digest": qualification_evidence_digest(
            payload,
        ),
        "accepted_material_qualification_max_age_seconds": 86400,
    }
    live_topology = replace(
        topology,
        namespace_uid="production-namespace-uid",
        gateway_namespace="production",
        gateway_service_account="production-gateway",
        volumes=tuple(
            replace(
                volume,
                uid=f"production-{volume.role}-uid",
                volume_name=f"production-{volume.role}-volume",
            )
            for volume in topology.volumes
        ),
    )
    provider._backend = SimpleNamespace(
        accepted_material_runtime_qualification_subjects=lambda: (
            "c" * 64,
            "b" * 64,
            live_topology,
        ),
    )
    monkeypatch.setenv("DEER_FLOW_IMAGE_DIGEST", "sha256:" + ("a" * 64))

    qualification = await provider._accepted_sandbox_qualification(
        profile=profile,
        runtime_image_digest="c" * 64,
    )
    assert qualification.is_current(now)
    assert qualification.topology_digest == live_topology.digest
    assert profile.recoverable_resource_lookup is False

    mismatched_topology = replace(evidence, topology_policy_digest="0" * 64)
    mismatched_payload = mismatched_topology.canonical_bytes()
    artifact.write_bytes(mismatched_payload)
    provider._config["accepted_material_qualification_digest"] = qualification_evidence_digest(mismatched_payload)
    with pytest.raises(AcceptedMaterialError, match="sandbox_provider_unqualified"):
        await provider._accepted_sandbox_qualification(
            profile=profile,
            runtime_image_digest="c" * 64,
        )

    artifact.write_bytes(payload)
    provider._config["accepted_material_qualification_digest"] = qualification_evidence_digest(payload)

    mismatched_profile = replace(evidence, capability_profile_digest="0" * 64)
    mismatched_payload = mismatched_profile.canonical_bytes()
    artifact.write_bytes(mismatched_payload)
    provider._config["accepted_material_qualification_digest"] = qualification_evidence_digest(mismatched_payload)
    with pytest.raises(AcceptedMaterialError, match="sandbox_provider_unqualified"):
        await provider._accepted_sandbox_qualification(
            profile=profile,
            runtime_image_digest="c" * 64,
        )

    artifact.write_bytes(payload)
    provider._config["accepted_material_qualification_digest"] = qualification_evidence_digest(payload)

    monkeypatch.setenv("DEER_FLOW_IMAGE_DIGEST", "sha256:" + ("9" * 64))
    with pytest.raises(AcceptedMaterialError, match="sandbox_provider_unqualified"):
        await provider._accepted_sandbox_qualification(
            profile=profile,
            runtime_image_digest="c" * 64,
        )
    monkeypatch.setenv("DEER_FLOW_IMAGE_DIGEST", "sha256:" + ("a" * 64))
    provider._backend = SimpleNamespace(
        accepted_material_runtime_qualification_subjects=lambda: (
            "c" * 64,
            "8" * 64,
            topology,
        ),
    )
    with pytest.raises(AcceptedMaterialError, match="sandbox_provider_unqualified"):
        await provider._accepted_sandbox_qualification(
            profile=profile,
            runtime_image_digest="c" * 64,
        )

    provider._backend = SimpleNamespace(
        accepted_material_runtime_qualification_subjects=lambda: (
            "c" * 64,
            "b" * 64,
            live_topology,
        ),
    )
    provider._config["accepted_material_qualification_digest"] = "sha256:" + ("f" * 64)
    with pytest.raises(AcceptedMaterialError, match="sandbox_provider_unqualified"):
        await provider._accepted_sandbox_qualification(
            profile=profile,
            runtime_image_digest="c" * 64,
        )


@pytest.mark.asyncio
async def test_aio_candidate_is_explicit_short_lived_and_never_passing(
    monkeypatch,
) -> None:
    from deerflow.sandbox.accepted_material import AcceptedMaterialError

    aio_mod = importlib.import_module(
        "deerflow.community.aio_sandbox.aio_sandbox_provider",
    )
    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)
    provider._config = {}
    topology = _accepted_runtime_topology()
    provider._backend = SimpleNamespace(
        accepted_material_runtime_qualification_subjects=lambda: (
            "c" * 64,
            "b" * 64,
            topology,
        ),
    )
    profile = provider.accepted_sandbox_capability_profile()
    monkeypatch.setenv("DEER_FLOW_IMAGE_DIGEST", "sha256:" + ("a" * 64))

    with pytest.raises(AcceptedMaterialError, match="sandbox_provider_unqualified"):
        await provider._accepted_sandbox_qualification(
            profile=profile,
            runtime_image_digest="c" * 64,
        )

    monkeypatch.setenv("DEER_FLOW_QUALIFICATION_CANDIDATE", "1")
    monkeypatch.setenv("DEER_FLOW_QUALIFICATION_CANDIDATE_ID", "qual-1")
    monkeypatch.setenv(
        "DEER_FLOW_QUALIFICATION_NAMESPACE",
        "hartmesh-qualification-qual-1",
    )
    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_RUNTIME", "1")
    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_FAULT_INJECTION", "1")
    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID", "qual-1")

    qualification = await provider._accepted_sandbox_qualification(
        profile=profile,
        runtime_image_digest="c" * 64,
    )
    now = datetime.now(UTC)
    assert qualification.status == "candidate"
    assert qualification.is_current(now) is False
    assert qualification.is_candidate_current(now) is True
    assert qualification.expires_at - qualification.verified_at <= timedelta(
        minutes=15,
    )


_LEGACY_COLLIDING_IDENTITIES = (
    ("user-9721", "thread-9721"),
    ("user-94361", "thread-94361"),
)

# ── thread-data mount configuration ─────────────────────────────────────────


@pytest.mark.parametrize(
    ("sandbox_overrides", "expected"),
    [
        ({}, None),
        ({"thread_data_mounts": True}, True),
        ({"thread_data_mounts": False}, False),
    ],
)
def test_load_config_preserves_thread_data_mounts_override(sandbox_overrides, expected, monkeypatch):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    sandbox_config = SandboxConfig(
        use="deerflow.community.aio_sandbox:AioSandboxProvider",
        **sandbox_overrides,
    )
    app_config = SimpleNamespace(sandbox=sandbox_config, stream_bridge=None)
    monkeypatch.setattr(aio_mod, "get_app_config", lambda: app_config)
    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)

    assert provider._load_config()["thread_data_mounts"] is expected
    assert provider._load_config()["skills_container_path"] == "/mnt/skills"


def test_load_config_snapshots_custom_skills_container_path(monkeypatch):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    sandbox_config = SandboxConfig(
        use="deerflow.community.aio_sandbox:AioSandboxProvider",
    )
    app_config = SimpleNamespace(
        sandbox=sandbox_config,
        stream_bridge=None,
        skills=SimpleNamespace(container_path="/custom-skills"),
    )
    monkeypatch.setattr(aio_mod, "get_app_config", lambda: app_config)
    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)

    assert provider._load_config()["skills_container_path"] == "/custom-skills"


@pytest.mark.parametrize(
    ("backend_is_local", "override", "expected"),
    [
        (True, None, True),
        (False, None, False),
        (True, False, False),
        (False, True, True),
    ],
)
def test_thread_data_mounts_override_precedes_backend_detection(backend_is_local, override, expected):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)
    provider._config = {} if override is None else {"thread_data_mounts": override}
    provider._backend = object.__new__(aio_mod.LocalContainerBackend) if backend_is_local else object()

    assert provider.uses_thread_data_mounts is expected


# ── ensure_thread_dirs ───────────────────────────────────────────────────────


def test_ensure_thread_dirs_creates_acp_workspace(tmp_path):
    """ACP workspace directory must be created alongside user-data dirs."""
    paths = Paths(base_dir=tmp_path)
    paths.ensure_thread_dirs("thread-1")

    assert (tmp_path / "threads" / "thread-1" / "user-data" / "workspace").exists()
    assert (tmp_path / "threads" / "thread-1" / "user-data" / "uploads").exists()
    assert (tmp_path / "threads" / "thread-1" / "user-data" / "outputs").exists()
    assert (tmp_path / "threads" / "thread-1" / "acp-workspace").exists()


def test_ensure_thread_dirs_acp_workspace_is_world_writable(tmp_path):
    """ACP workspace must be chmod 0o777 so the ACP subprocess can write into it."""
    paths = Paths(base_dir=tmp_path)
    paths.ensure_thread_dirs("thread-2")

    acp_dir = tmp_path / "threads" / "thread-2" / "acp-workspace"
    mode = oct(acp_dir.stat().st_mode & 0o777)
    assert mode == oct(0o777)


def test_host_thread_dir_rejects_invalid_thread_id(tmp_path):
    paths = Paths(base_dir=tmp_path)

    with pytest.raises(ValueError, match="Invalid thread_id"):
        paths.host_thread_dir("../escape")


# ── _get_thread_mounts ───────────────────────────────────────────────────────


def _make_provider(tmp_path):
    """Build a minimal AioSandboxProvider instance without starting the idle checker.

    ``tmp_path`` is accepted and ignored: ownership no longer lives on disk. Each
    provider gets its own in-process ownership store, so it owns every sandbox it
    tracks — cross-instance behaviour is covered in
    ``test_sandbox_orphan_reconciliation.py`` (shared store) and
    ``test_sandbox_ownership_store.py`` (store contract).
    """
    from deerflow.community.aio_sandbox.ownership.memory import MemoryOwnershipStore
    from deerflow.config.sandbox_config import SandboxOwnershipConfig

    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    with patch.object(aio_mod.AioSandboxProvider, "_start_idle_checker"):
        provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)
        provider._config = {"idle_timeout": 600, "replicas": 3}
        provider._sandboxes = {}
        provider._sandbox_infos = {}
        provider._thread_sandboxes = {}
        provider._warm_pool = {}
        provider._active_sandbox_identity = {}
        provider._warm_pool_identity = {}
        provider._last_activity = {}
        provider._local_teardown = set()
        provider._acquire_epoch = {}
        provider._acquire_epoch_counter = 0
        provider._acquire_inflight = {}
        provider._acquire_serializer = AcquireSerializer(thread_name_prefix="aio-sandbox-lock-wait")
        provider._lock = MagicMock()
        provider._idle_checker_stop = MagicMock()
        provider._idle_checker_thread = None
        provider._renewal_stop = MagicMock()
        provider._renewal_thread = None
        provider._shutdown_called = False
        provider._owner_id = "test-worker"
        provider._ownership_config = SandboxOwnershipConfig()
        provider._ownership = MemoryOwnershipStore(owner_id="test-worker", ttl_seconds=600)
    return provider


def test_get_thread_mounts_includes_acp_workspace(tmp_path, monkeypatch):
    """_get_thread_mounts must include /mnt/acp-workspace (read-only) for docker sandbox."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=tmp_path))
    monkeypatch.setattr(aio_mod, "get_effective_user_id", lambda: None)

    mounts = aio_mod.AioSandboxProvider._get_thread_mounts("thread-3")

    container_paths = {m[1]: (m[0], m[2]) for m in mounts}

    assert "/mnt/acp-workspace" in container_paths, "ACP workspace mount is missing"
    expected_host = str(tmp_path / "threads" / "thread-3" / "acp-workspace")
    actual_host, read_only = container_paths["/mnt/acp-workspace"]
    assert actual_host == expected_host
    assert read_only is True, "ACP workspace should be read-only inside the sandbox"


def test_get_thread_mounts_includes_user_data_dirs(tmp_path, monkeypatch):
    """Baseline: user-data mounts must still be present after the ACP workspace change."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=tmp_path))

    mounts = aio_mod.AioSandboxProvider._get_thread_mounts("thread-4")
    container_paths = {m[1] for m in mounts}

    assert "/mnt/user-data/workspace" in container_paths
    assert "/mnt/user-data/uploads" in container_paths
    assert "/mnt/user-data/outputs" in container_paths


def test_get_thread_mounts_uses_explicit_user_id(tmp_path, monkeypatch):
    """Channel runs must mount the same user bucket used for artifact delivery."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=tmp_path))
    monkeypatch.setattr(aio_mod, "get_effective_user_id", lambda: "default")

    mounts = aio_mod.AioSandboxProvider._get_thread_mounts("thread-4", user_id="ou-user")
    container_paths = {container_path: host_path for host_path, container_path, _ in mounts}

    assert container_paths["/mnt/user-data/workspace"] == str(tmp_path / "users" / "ou-user" / "threads" / "thread-4" / "user-data" / "workspace")
    assert container_paths["/mnt/user-data/uploads"] == str(tmp_path / "users" / "ou-user" / "threads" / "thread-4" / "user-data" / "uploads")
    assert container_paths["/mnt/user-data/outputs"] == str(tmp_path / "users" / "ou-user" / "threads" / "thread-4" / "user-data" / "outputs")


def test_get_lark_cli_runtime_mounts_uses_user_auth_dirs(tmp_path, monkeypatch):
    """Sandbox lark-cli commands must read the same auth dirs as Settings."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    lark_cli = importlib.import_module("deerflow.integrations.lark_cli")
    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=tmp_path))
    monkeypatch.setattr(aio_mod, "get_effective_user_id", lambda: "default")
    runtime_dir = tmp_path / "integrations" / "lark-cli" / "sandbox-cli"
    runtime_dir.mkdir(parents=True)

    mounts = aio_mod.AioSandboxProvider._get_lark_cli_runtime_mounts(user_id="alice")
    mount_order = [container_path for _host_path, container_path, _read_only in mounts]
    container_paths = {container_path: (host_path, read_only) for host_path, container_path, read_only in mounts}

    assert container_paths[lark_cli.LARK_CLI_SANDBOX_CONFIG_DIR] == (
        str(tmp_path / "users" / "alice" / "integrations" / "lark-cli" / "config"),
        True,
    )
    assert container_paths[f"{lark_cli.LARK_CLI_SANDBOX_CONFIG_DIR}/locks"] == (
        str(tmp_path / "users" / "alice" / "integrations" / "lark-cli" / "config" / "locks"),
        False,
    )
    assert mount_order.index(lark_cli.LARK_CLI_SANDBOX_CONFIG_DIR) < mount_order.index(lark_cli.LARK_CLI_SANDBOX_LOCKS_DIR)
    assert container_paths[lark_cli.LARK_CLI_SANDBOX_DATA_DIR] == (
        str(tmp_path / "users" / "alice" / "integrations" / "lark-cli" / "data"),
        False,
    )
    assert stat.S_IMODE((tmp_path / "users" / "alice" / "integrations" / "lark-cli" / "config").stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "users" / "alice" / "integrations" / "lark-cli" / "config" / "locks").stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "users" / "alice" / "integrations" / "lark-cli" / "data").stat().st_mode) == 0o700
    assert container_paths["/mnt/integrations/lark-cli/runtime"] == (
        str(runtime_dir),
        True,
    )


def test_get_user_skill_mounts_mounts_only_global_integrations(tmp_path, monkeypatch):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    skills_root = tmp_path / "skills"
    (skills_root / "public").mkdir(parents=True)
    config = SimpleNamespace(
        skills=SimpleNamespace(
            get_skills_path=lambda: skills_root,
            container_path="/mnt/skills",
        )
    )
    monkeypatch.setattr(aio_mod, "get_app_config", lambda: config)
    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=tmp_path / "home"))

    alice = {container: host for host, container, _read_only in aio_mod.AioSandboxProvider._get_user_skill_mounts(user_id="alice")}
    bob = {container: host for host, container, _read_only in aio_mod.AioSandboxProvider._get_user_skill_mounts(user_id="bob")}

    assert set(alice) == {"/mnt/skills/integrations"}
    assert set(bob) == {"/mnt/skills/integrations"}
    assert alice["/mnt/skills/integrations"] != bob["/mnt/skills/integrations"]
    assert alice["/mnt/skills/integrations"] == str(tmp_path / "home" / "users" / "alice" / "skills_view" / "integrations")
    assert bob["/mnt/skills/integrations"] == str(tmp_path / "home" / "users" / "bob" / "skills_view" / "integrations")


def test_get_extra_mounts_provisioner_payload_has_unique_container_paths(tmp_path, monkeypatch, provisioner_module):
    """Full AIO mount composition must not send duplicate paths to provisioner."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    lark_cli = importlib.import_module("deerflow.integrations.lark_cli")
    remote_backend = importlib.import_module("deerflow.community.aio_sandbox.remote_backend")
    skills_root = tmp_path / "skills"
    (skills_root / "public").mkdir(parents=True)
    home = tmp_path / "home"
    config = SimpleNamespace(
        skills=SimpleNamespace(
            get_skills_path=lambda: skills_root,
            container_path="/mnt/skills",
        )
    )
    runtime_dir = home / "integrations" / "lark-cli" / "sandbox-cli"
    runtime_dir.mkdir(parents=True)

    monkeypatch.setattr(aio_mod, "get_app_config", lambda: config)
    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=home))
    monkeypatch.setattr(aio_mod, "get_effective_user_id", lambda: "default")
    monkeypatch.setattr(remote_backend, "user_should_see_legacy_skills", lambda *_args, **_kwargs: False)

    provider = _make_provider(tmp_path)
    provider._config["skills_container_path"] = config.skills.container_path
    mounts = provider._get_extra_mounts("thread-1", user_id="alice")
    container_paths = [container for _host, container, _read_only in mounts]

    assert len(container_paths) == len(set(container_paths))
    assert "/mnt/skills/.accepted" not in container_paths
    assert "/mnt/skills/custom" in container_paths
    assert "/mnt/skills/integrations" in container_paths
    assert lark_cli.LARK_CLI_SANDBOX_CONFIG_DIR in container_paths
    assert lark_cli.LARK_CLI_SANDBOX_LOCKS_DIR in container_paths
    assert lark_cli.LARK_CLI_SANDBOX_DATA_DIR in container_paths
    assert lark_cli.LARK_CLI_SANDBOX_RUNTIME_DIR in container_paths

    payload = remote_backend._provisioner_extra_mounts_payload(mounts)
    payload_paths = [str(item["container_path"]) for item in payload]
    assert len(payload_paths) == len(set(payload_paths))
    assert payload_paths.index(lark_cli.LARK_CLI_SANDBOX_CONFIG_DIR) < payload_paths.index(lark_cli.LARK_CLI_SANDBOX_LOCKS_DIR)

    provisioner_module.DEER_FLOW_HOST_BASE_DIR = str(home)
    validated = provisioner_module._validated_extra_mounts([provisioner_module.ExtraMount(**item) for item in payload])
    validated_paths = [mount.container_path for mount in validated]

    assert len(validated_paths) == len(set(validated_paths))
    assert set(validated_paths) == {
        "/mnt/acp-workspace",
        "/mnt/skills/public",
        "/mnt/skills/custom",
        "/mnt/skills/legacy",
        "/mnt/skills/integrations",
        lark_cli.LARK_CLI_SANDBOX_CONFIG_DIR,
        lark_cli.LARK_CLI_SANDBOX_LOCKS_DIR,
        lark_cli.LARK_CLI_SANDBOX_DATA_DIR,
        lark_cli.LARK_CLI_SANDBOX_RUNTIME_DIR,
    }


def test_accepted_extra_mounts_omit_every_mutable_skill_projection(tmp_path, monkeypatch):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    home = tmp_path / "home"
    config = SimpleNamespace(skills=SimpleNamespace(container_path="/mnt/skills"))
    monkeypatch.setattr(aio_mod, "get_app_config", lambda: config)
    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=home))
    provider = _make_provider(tmp_path)
    monkeypatch.setattr(
        provider,
        "_get_skills_mounts",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("live skill mounts must not be consulted")),
    )
    monkeypatch.setattr(
        provider,
        "_get_user_skill_mounts",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("live integration mounts must not be consulted")),
    )
    monkeypatch.setattr(provider, "_get_lark_cli_runtime_mounts", lambda **_kwargs: [])

    mounts = provider._get_extra_mounts(
        "thread-accepted",
        user_id="owner-accepted",
        accepted_skills_only=True,
    )
    skill_paths = {container for _host, container, _read_only in mounts if container == "/mnt/skills" or container.startswith("/mnt/skills/")}

    assert skill_paths == {"/mnt/skills/.accepted"}


def test_accepted_aio_acquisition_rejects_writable_host_alias_of_active_material(
    tmp_path,
    monkeypatch,
):
    aio_mod = importlib.import_module(
        "deerflow.community.aio_sandbox.aio_sandbox_provider",
    )
    paths = Paths(base_dir=tmp_path / "home")
    active_view = paths.skill_snapshot_active_view_dir(
        "owner-alias",
        "thread-alias",
    )
    config = SimpleNamespace(skills=SimpleNamespace(container_path="/mnt/skills"))
    monkeypatch.setattr(aio_mod, "get_app_config", lambda: config)
    monkeypatch.setattr(aio_mod, "get_paths", lambda: paths)
    provider = _make_provider(tmp_path)
    provider._config["mounts"] = [
        SimpleNamespace(
            host_path=str(active_view.parent),
            container_path="/mnt/alias",
            read_only=False,
        ),
    ]

    with pytest.raises(
        AcceptedSkillSandboxBindingError,
        match="accepted_skill_snapshot_writable_alias",
    ):
        provider.provision_accepted_skills(
            "thread-alias",
            user_id="owner-alias",
            binding=AcceptedSkillSandboxBindingV1(snapshot_id=None),
        )


def test_accepted_snapshot_binding_tracks_cached_sandbox_identity(
    tmp_path,
    monkeypatch,
):
    """AIO cache hits must project the selected digest, including empty."""
    snapshot_module = importlib.import_module("deerflow.runtime.skill_snapshot")
    provider = _make_provider(tmp_path)
    provider._lock = threading.Lock()
    provider._thread_sandboxes = {("alice", "thread-1"): "sandbox-1"}
    projected: list[tuple[str | None, str, str | None]] = []
    monkeypatch.setattr(
        snapshot_module,
        "bind_skill_snapshot_active_view",
        lambda *, user_id, thread_id, snapshot_id, **_kwargs: projected.append((user_id, thread_id, snapshot_id)),
    )

    provider.bind_accepted_skill_snapshot(
        "sandbox-1",
        thread_id="thread-1",
        user_id="alice",
        binding=AcceptedSkillSandboxBindingV1(snapshot_id="a" * 64),
    )
    provider.bind_accepted_skill_snapshot(
        "sandbox-1",
        thread_id="thread-1",
        user_id="alice",
        binding=AcceptedSkillSandboxBindingV1(snapshot_id=None),
    )

    assert projected == [
        ("alice", "thread-1", "a" * 64),
        ("alice", "thread-1", None),
    ]


@pytest.mark.anyio
async def test_aio_nonempty_accepted_snapshot_passes_pre_model_middleware(
    tmp_path,
    monkeypatch,
) -> None:
    from pathlib import Path

    from langgraph.runtime import Runtime

    from deerflow.runtime.accepted_invocation import ResolvedAgentMaterialV1
    from deerflow.runtime.agent_revision import RESOLVED_AGENT_MATERIAL_CONTEXT_KEY
    from deerflow.runtime.skill_projection import (
        SKILL_PROJECTION_TOKEN_CONTEXT_KEY,
    )
    from deerflow.runtime.skill_snapshot import snapshot_effective_skills
    from deerflow.sandbox.accepted_projection import release_accepted_skill_consumer
    from deerflow.sandbox.middleware import SandboxMiddleware
    from deerflow.sandbox.sandbox_provider import (
        reset_sandbox_provider,
        set_sandbox_provider,
    )
    from deerflow.skills.parser import parse_skill_file
    from deerflow.skills.types import SkillCategory

    source = tmp_path / "source" / "aio-skill"
    source.mkdir(parents=True)
    skill_file = source / "SKILL.md"
    skill_file.write_text(
        "---\nname: aio-skill\ndescription: immutable\n---\naccepted bytes\n",
        encoding="utf-8",
    )
    skill = parse_skill_file(
        skill_file,
        SkillCategory.CUSTOM,
        relative_path=Path("aio-skill"),
    )
    assert skill is not None
    paths = Paths(base_dir=tmp_path / "state")
    monkeypatch.setattr("deerflow.runtime.skill_snapshot.get_paths", lambda: paths)
    snapshot = snapshot_effective_skills((skill,), user_id="aio-owner")
    assert snapshot is not None
    material = ResolvedAgentMaterialV1(
        agent_id="lead-agent",
        storage_source="test",
        storage_version="1",
        agent_config=None,
        soul="",
        model_profile={},
        skill_snapshot=snapshot,
        enabled_skill_objects=snapshot.skills,
        all_skill_objects=snapshot.skills,
    )
    provider, _sandbox, _aio_mod = _make_provider_with_active_sandbox(
        tmp_path,
        "sandbox-aio-accepted",
    )
    identity = ("aio-owner", "aio-thread")
    provider._thread_sandboxes[identity] = "sandbox-aio-accepted"
    provider._active_sandbox_identity["sandbox-aio-accepted"] = identity
    provider._accepted_only_sandbox_ids = {"sandbox-aio-accepted"}
    projected: list[str | None] = []
    monkeypatch.setattr(
        "deerflow.runtime.skill_snapshot.bind_skill_snapshot_active_view",
        lambda *, snapshot_id, **_kwargs: projected.append(snapshot_id),
    )
    monkeypatch.setattr(provider, "clear_accepted_skill_snapshot", lambda _clear: True)
    runtime = Runtime(
        context={
            "thread_id": identity[1],
            "run_id": "aio-run",
            "user_id": identity[0],
            RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: material,
        }
    )
    set_sandbox_provider(provider)
    try:
        await SandboxMiddleware(lazy_init=True).abefore_agent(
            {"sandbox": {"sandbox_id": "sandbox-aio-accepted"}},
            runtime,
        )
        assert projected == [snapshot.snapshot_id]
    finally:
        token = runtime.context.pop(SKILL_PROJECTION_TOKEN_CONTEXT_KEY, None)
        if token is not None:
            assert release_accepted_skill_consumer(token)
        reset_sandbox_provider()
        snapshot.release()


def test_aio_proves_failed_prepublication_view_is_empty(
    tmp_path,
    monkeypatch,
) -> None:
    from deerflow.config.paths import Paths
    from deerflow.runtime.skill_projection import SkillProjectionClear

    paths = Paths(base_dir=tmp_path / "state")
    monkeypatch.setattr("deerflow.runtime.skill_snapshot.get_paths", lambda: paths)
    provider, _sandbox, _aio_mod = _make_provider_with_active_sandbox(
        tmp_path,
        "sandbox-aio-unpublished",
    )
    identity = ("aio-unpublished-owner", "aio-unpublished-thread")
    provider._thread_sandboxes[identity] = "sandbox-aio-unpublished"
    provider._active_sandbox_identity["sandbox-aio-unpublished"] = identity
    provider._accepted_only_sandbox_ids = {"sandbox-aio-unpublished"}
    clear = SkillProjectionClear(
        user_id=identity[0],
        thread_id=identity[1],
        sandbox_id="sandbox-aio-unpublished",
        run_id="aio-unpublished-run",
        generation=1,
        snapshot_id=None,
    )

    assert provider.clear_accepted_skill_snapshot(clear) is False
    assert provider.ensure_accepted_skill_snapshot_absent(clear)


def test_thread_skill_projection_mounts_all_categories(
    tmp_path,
    monkeypatch,
):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    paths = Paths(base_dir=tmp_path / "home")
    projection_root = paths.thread_skills_view_dir(
        "thread-policy",
        user_id="alice",
    )
    for category in ("public", "custom", "legacy", "integrations"):
        (projection_root / category).mkdir(parents=True, exist_ok=True)
    config = SimpleNamespace(skills=SimpleNamespace(container_path="/mnt/skills"))
    monkeypatch.setattr(aio_mod, "get_app_config", lambda: config)
    monkeypatch.setattr(aio_mod, "get_paths", lambda: paths)

    mounts = aio_mod.AioSandboxProvider._get_skills_mounts(
        "thread-policy",
        user_id="alice",
    )

    assert {container_path: host_path for host_path, container_path, _ in mounts} == {f"/mnt/skills/{category}": str(projection_root / category) for category in ("public", "custom", "legacy", "integrations")}
    assert all(read_only for _host, _container, read_only in mounts)


def test_thread_skill_projection_uses_distinct_sandbox_identity(
    tmp_path,
    monkeypatch,
):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    paths = Paths(base_dir=tmp_path / "home")
    monkeypatch.setattr(aio_mod, "get_paths", lambda: paths)
    monkeypatch.setattr(
        aio_mod,
        "get_app_config",
        lambda: SimpleNamespace(skills=SimpleNamespace(container_path="/mnt/skills")),
    )
    provider = _make_provider(tmp_path)

    shared_id = provider._sandbox_id_for_thread("thread-policy", "alice")
    paths.thread_skills_view_dir(
        "thread-policy",
        user_id="alice",
    ).mkdir(parents=True)
    policy_id = provider._sandbox_id_for_thread("thread-policy", "alice")

    assert policy_id != shared_id
    assert policy_id == provider._sandbox_id_for_thread("thread-policy", "alice")
    assert policy_id != provider._deterministic_sandbox_id(
        "thread-policy:agent-skills-v1",
        "alice",
    )


def test_policy_scoped_sandbox_identity_changes_with_skills_container_root(
    tmp_path,
    monkeypatch,
):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    paths = Paths(base_dir=tmp_path / "home")
    paths.thread_skills_view_dir(
        "thread-policy",
        user_id="alice",
    ).mkdir(parents=True)
    monkeypatch.setattr(aio_mod, "get_paths", lambda: paths)
    config = SimpleNamespace(skills=SimpleNamespace(container_path="/mnt/skills"))
    monkeypatch.setattr(aio_mod, "get_app_config", lambda: config)
    provider = _make_provider(tmp_path)

    default_root_id = provider._sandbox_id_for_thread(
        "thread-policy",
        "alice",
    )
    config.skills.container_path = "/custom-skills"
    provider._config["skills_container_path"] = config.skills.container_path
    custom_root_id = provider._sandbox_id_for_thread(
        "thread-policy",
        "alice",
    )

    assert custom_root_id != default_root_id


def test_shared_sandbox_identity_changes_when_custom_skills_root_changes(
    tmp_path,
    monkeypatch,
):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    paths = Paths(base_dir=tmp_path / "home")
    monkeypatch.setattr(aio_mod, "get_paths", lambda: paths)
    config = SimpleNamespace(skills=SimpleNamespace(container_path="/custom-skills-a"))
    monkeypatch.setattr(aio_mod, "get_app_config", lambda: config)
    provider = _make_provider(tmp_path)
    provider._config["skills_container_path"] = config.skills.container_path

    first_root_id = provider._sandbox_id_for_thread(
        "thread-shared",
        "alice",
    )
    config.skills.container_path = "/custom-skills-b"
    provider._config["skills_container_path"] = config.skills.container_path
    second_root_id = provider._sandbox_id_for_thread(
        "thread-shared",
        "alice",
    )

    assert second_root_id != first_root_id


def test_cached_sandbox_is_replaced_when_expected_identity_changes(
    tmp_path,
    monkeypatch,
):
    provider = _make_provider(tmp_path)
    provider._config["skills_container_path"] = "/custom-skills"
    provider._thread_sandboxes = {("alice", "thread-shared"): "stale-root-id"}
    provider._sandboxes = {"stale-root-id": object()}
    provider._sandbox_infos = {}
    monkeypatch.setattr(
        provider,
        "_sandbox_id_for_thread",
        lambda *_args, **_kwargs: "new-root-id",
    )
    destroy = MagicMock()
    monkeypatch.setattr(provider, "destroy", destroy)

    assert (
        provider._reuse_in_process_sandbox(
            "thread-shared",
            user_id="alice",
        )
        is None
    )
    destroy.assert_called_once_with("stale-root-id")


def test_policy_scoped_create_excludes_local_config_mounts_below_skills_root(
    tmp_path,
    monkeypatch,
):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    paths = Paths(base_dir=tmp_path / "home")
    paths.thread_skills_view_dir(
        "thread-policy",
        user_id="alice",
    ).mkdir(parents=True)
    monkeypatch.setattr(aio_mod, "get_paths", lambda: paths)
    monkeypatch.setattr(
        aio_mod,
        "get_app_config",
        lambda: SimpleNamespace(skills=SimpleNamespace(container_path="/mnt/skills")),
    )

    provider = _make_provider(tmp_path)
    provider._config = {
        "replicas": 3,
        "skills_container_path": "/mnt/skills",
    }
    provider._thread_locks = {}
    provider._warm_pool = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._last_activity = {}
    provider._lock = aio_mod.threading.Lock()
    captured: dict = {}

    backend = object.__new__(aio_mod.LocalContainerBackend)

    def _create(thread_id, sandbox_id, **kwargs):
        captured.update(kwargs)
        return aio_mod.SandboxInfo(
            sandbox_id=sandbox_id,
            sandbox_url="http://sandbox",
        )

    backend.create = _create
    provider._backend = backend
    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready", lambda *_a, **_k: True)
    monkeypatch.setattr(provider, "_get_extra_mounts", lambda *_a, **_k: [])
    monkeypatch.setattr(
        aio_mod.AioSandboxProvider,
        "_lark_integration_active",
        staticmethod(lambda user_id=None: False),
    )
    monkeypatch.setattr(
        aio_mod.AioSandboxProvider,
        "_lark_broker_active",
        staticmethod(lambda user_id=None: False),
    )
    monkeypatch.setattr(
        provider,
        "_register_created_sandbox",
        lambda *a, **k: "sandbox-policy",
    )

    provider._create_sandbox(
        "thread-policy",
        "sandbox-policy",
        user_id="alice",
    )

    assert captured["config_mount_exclusion_root"] == "/mnt/skills"


def test_remote_create_forwards_configured_skills_container_path(
    tmp_path,
    monkeypatch,
):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._config = {
        "replicas": 3,
        "skills_container_path": "/custom-skills",
    }
    provider._thread_locks = {}
    provider._warm_pool = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._last_activity = {}
    provider._lock = aio_mod.threading.Lock()
    captured: dict = {}

    backend = aio_mod.RemoteSandboxBackend("http://provisioner:8002")

    def _create(thread_id, sandbox_id, **kwargs):
        captured.update(kwargs)
        return aio_mod.SandboxInfo(
            sandbox_id=sandbox_id,
            sandbox_url="http://sandbox",
        )

    backend.create = _create
    provider._backend = backend
    monkeypatch.setattr(
        aio_mod,
        "get_app_config",
        lambda: SimpleNamespace(skills=SimpleNamespace(container_path="/custom-skills")),
    )
    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready", lambda *_a, **_k: True)
    monkeypatch.setattr(provider, "_get_extra_mounts", lambda *_a, **_k: [])
    monkeypatch.setattr(
        aio_mod.AioSandboxProvider,
        "_lark_integration_active",
        staticmethod(lambda user_id=None: False),
    )
    monkeypatch.setattr(
        aio_mod.AioSandboxProvider,
        "_lark_broker_active",
        staticmethod(lambda user_id=None: False),
    )
    monkeypatch.setattr(
        provider,
        "_register_created_sandbox",
        lambda *a, **k: "sandbox-custom-root",
    )

    provider._create_sandbox(
        "thread-custom-root",
        "sandbox-custom-root",
        user_id="alice",
    )

    assert captured["skills_container_path"] == "/custom-skills"


@pytest.mark.anyio
async def test_remote_create_async_forwards_configured_skills_container_path(
    tmp_path,
    monkeypatch,
):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._config = {
        "replicas": 3,
        "skills_container_path": "/custom-skills",
    }
    provider._thread_locks = {}
    provider._warm_pool = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._last_activity = {}
    provider._lock = aio_mod.threading.Lock()
    captured: dict = {}

    backend = aio_mod.RemoteSandboxBackend("http://provisioner:8002")

    def _create(thread_id, sandbox_id, **kwargs):
        captured.update(kwargs)
        return aio_mod.SandboxInfo(
            sandbox_id=sandbox_id,
            sandbox_url="http://sandbox",
        )

    backend.create = _create
    provider._backend = backend
    monkeypatch.setattr(
        aio_mod,
        "get_app_config",
        lambda: SimpleNamespace(skills=SimpleNamespace(container_path="/custom-skills")),
    )

    async def _ready(*_args, **_kwargs):
        return True

    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready_async", _ready)
    monkeypatch.setattr(provider, "_get_extra_mounts", lambda *_a, **_k: [])
    monkeypatch.setattr(
        aio_mod.AioSandboxProvider,
        "_lark_integration_active",
        staticmethod(lambda user_id=None: False),
    )
    monkeypatch.setattr(
        aio_mod.AioSandboxProvider,
        "_lark_broker_active",
        staticmethod(lambda user_id=None: False),
    )
    monkeypatch.setattr(
        provider,
        "_register_created_sandbox",
        lambda *a, **k: "sandbox-custom-root",
    )

    await provider._create_sandbox_async(
        "thread-custom-root",
        "sandbox-custom-root",
        user_id="alice",
    )

    assert captured["skills_container_path"] == "/custom-skills"


def test_join_host_path_preserves_windows_drive_letter_style():
    base = r"C:\Users\demo\deer-flow\backend\.deer-flow"

    joined = join_host_path(base, "threads", "thread-9", "user-data", "outputs")

    assert joined == r"C:\Users\demo\deer-flow\backend\.deer-flow\threads\thread-9\user-data\outputs"


def test_get_thread_mounts_preserves_windows_host_path_style(tmp_path, monkeypatch):
    """Docker bind mount sources must keep Windows-style paths intact."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    monkeypatch.setenv("DEER_FLOW_HOST_BASE_DIR", r"C:\Users\demo\deer-flow\backend\.deer-flow")
    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=tmp_path))
    monkeypatch.setattr(aio_mod, "get_effective_user_id", lambda: None)

    mounts = aio_mod.AioSandboxProvider._get_thread_mounts("thread-10")

    container_paths = {container_path: host_path for host_path, container_path, _ in mounts}

    assert container_paths["/mnt/user-data/workspace"] == r"C:\Users\demo\deer-flow\backend\.deer-flow\threads\thread-10\user-data\workspace"
    assert container_paths["/mnt/user-data/uploads"] == r"C:\Users\demo\deer-flow\backend\.deer-flow\threads\thread-10\user-data\uploads"
    assert container_paths["/mnt/user-data/outputs"] == r"C:\Users\demo\deer-flow\backend\.deer-flow\threads\thread-10\user-data\outputs"
    assert container_paths["/mnt/acp-workspace"] == r"C:\Users\demo\deer-flow\backend\.deer-flow\threads\thread-10\acp-workspace"


def test_discover_or_create_only_unlocks_when_lock_succeeds(tmp_path, monkeypatch):
    """Unlock should not run if exclusive locking itself fails."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._discover_or_create_with_lock = aio_mod.AioSandboxProvider._discover_or_create_with_lock.__get__(
        provider,
        aio_mod.AioSandboxProvider,
    )

    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=tmp_path))
    monkeypatch.setattr(
        aio_mod,
        "_lock_file_exclusive",
        lambda _lock_file: (_ for _ in ()).throw(RuntimeError("lock failed")),
    )

    unlock_calls: list[object] = []
    monkeypatch.setattr(
        aio_mod,
        "_unlock_file",
        lambda lock_file: unlock_calls.append(lock_file),
    )

    with patch.object(provider, "_create_sandbox", return_value="sandbox-id"):
        with pytest.raises(RuntimeError, match="lock failed"):
            provider._discover_or_create_with_lock("thread-5", "sandbox-5")

    assert unlock_calls == []


@pytest.mark.anyio
async def test_acquire_async_uses_async_readiness_polling(monkeypatch):
    """AioSandboxProvider async creation must not use sync readiness polling."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(None)
    provider._config = {"replicas": 3}
    provider._warm_pool = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._last_activity = {}
    provider._lock = aio_mod.threading.Lock()
    provider._backend = SimpleNamespace(
        create=MagicMock(return_value=aio_mod.SandboxInfo(sandbox_id="sandbox-async", sandbox_url="http://sandbox")),
        destroy=MagicMock(),
        discover=MagicMock(return_value=None),
    )

    async_readiness_calls: list[tuple[str, int]] = []

    async def fake_wait_for_sandbox_ready_async(sandbox_url: str, timeout: int = 30, poll_interval: float = 1.0) -> bool:
        async_readiness_calls.append((sandbox_url, timeout))
        return True

    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready_async", fake_wait_for_sandbox_ready_async)
    monkeypatch.setattr(
        aio_mod,
        "wait_for_sandbox_ready",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("sync readiness should not be used")),
    )

    sandbox_id = await provider._create_sandbox_async("thread-async", "sandbox-async", user_id="user-async")

    assert sandbox_id == "sandbox-async"
    assert async_readiness_calls == [("http://sandbox", 60)]
    assert provider._backend.destroy.call_count == 0
    assert provider._thread_sandboxes[("user-async", "thread-async")] == "sandbox-async"


@pytest.mark.anyio
async def test_discover_or_create_with_lock_async_offloads_lock_file_open_and_close(tmp_path, monkeypatch):
    """Async lock path must not open or close lock files on the event loop."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._discover_or_create_with_lock_async = aio_mod.AioSandboxProvider._discover_or_create_with_lock_async.__get__(
        provider,
        aio_mod.AioSandboxProvider,
    )
    provider._warm_pool = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {("default", "thread-async-lock"): "sandbox-async-lock"}
    provider._sandboxes = {"sandbox-async-lock": aio_mod.AioSandbox(id="sandbox-async-lock", base_url="http://sandbox")}
    provider._last_activity = {}
    provider._lock = aio_mod.threading.Lock()
    provider._backend = SimpleNamespace(discover=MagicMock(return_value=None))

    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=tmp_path))

    to_thread_calls: list[object] = []

    async def fake_to_thread(func, /, *args, **kwargs):
        to_thread_calls.append(func)
        return func(*args, **kwargs)

    monkeypatch.setattr(aio_mod.asyncio, "to_thread", fake_to_thread)

    sandbox_id = await provider._discover_or_create_with_lock_async("thread-async-lock", "sandbox-async-lock", user_id="default")

    assert sandbox_id == "sandbox-async-lock"
    assert aio_mod._open_lock_file in to_thread_calls
    assert any(getattr(func, "__name__", "") == "close" for func in to_thread_calls)


@pytest.mark.anyio
async def test_acquire_async_lock_wait_uses_dedicated_executor(tmp_path, monkeypatch):
    """Per-thread lock waits should not consume the default asyncio.to_thread pool."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)

    async def fail_to_thread(*_args, **_kwargs):
        raise AssertionError("thread-lock acquisition must not use asyncio.to_thread")

    monkeypatch.setattr(aio_mod.asyncio, "to_thread", fail_to_thread)

    async def fake_acquire_internal_async(thread_id: str | None, *, user_id: str) -> str:
        await asyncio.sleep(0)
        return "sandbox-lock-wait"

    monkeypatch.setattr(provider, "_acquire_internal_async", fake_acquire_internal_async)

    thread_id = "thread-lock-wait"
    hold = provider._acquire_serializer.hold(provider._thread_key(thread_id, "default"))
    hold.__enter__()
    try:
        waiter = asyncio.create_task(provider.acquire_async(thread_id, user_id="default"))
        await asyncio.sleep(0.05)
        assert not waiter.done()
    finally:
        hold.__exit__(None, None, None)

    assert await asyncio.wait_for(waiter, timeout=1) == "sandbox-lock-wait"


@pytest.mark.anyio
async def test_acquire_async_cancellation_does_not_leak_thread_lock(tmp_path):
    """Cancelled async lock waiters must not leave the per-thread lock held."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._warm_pool = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._last_activity = {}
    provider._lock = aio_mod.threading.Lock()

    thread_id = "thread-cancel-lock"
    key = provider._thread_key(thread_id, "default")
    hold = provider._acquire_serializer.hold(key)
    hold.__enter__()

    task = asyncio.create_task(provider.acquire_async(thread_id, user_id="default"))
    await asyncio.sleep(0.05)
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    hold.__exit__(None, None, None)
    deadline = asyncio.get_running_loop().time() + 1
    while asyncio.get_running_loop().time() < deadline:
        if key not in provider._acquire_serializer._table:
            return
        await asyncio.sleep(0.01)

    pytest.fail("provider thread lock was leaked after cancelling acquire_async")


@pytest.mark.anyio
async def test_acquire_async_cancelled_waiter_does_not_block_successor(tmp_path, monkeypatch):
    """A cancelled waiter must not prevent the next live waiter from acquiring."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._warm_pool = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._last_activity = {}
    provider._lock = aio_mod.threading.Lock()

    async def fake_acquire_internal_async(thread_id: str | None, *, user_id: str) -> str:
        assert thread_id == "thread-successor-lock"
        assert user_id == "default"
        await asyncio.sleep(0)
        return "sandbox-successor"

    monkeypatch.setattr(provider, "_acquire_internal_async", fake_acquire_internal_async)

    thread_id = "thread-successor-lock"
    key = provider._thread_key(thread_id, "default")
    hold = provider._acquire_serializer.hold(key)
    hold.__enter__()

    cancelled_waiter = asyncio.create_task(provider.acquire_async(thread_id, user_id="default"))
    await asyncio.sleep(0.05)
    cancelled_waiter.cancel()
    try:
        await cancelled_waiter
    except asyncio.CancelledError:
        pass

    live_waiter = asyncio.create_task(provider.acquire_async(thread_id, user_id="default"))
    hold.__exit__(None, None, None)

    assert await asyncio.wait_for(live_waiter, timeout=1) == "sandbox-successor"

    deadline = asyncio.get_running_loop().time() + 1
    while asyncio.get_running_loop().time() < deadline:
        if key not in provider._acquire_serializer._table:
            return
        await asyncio.sleep(0.01)

    pytest.fail("provider thread lock was not released after successor acquire_async")


@pytest.mark.anyio
async def test_acquire_internal_async_offloads_cached_reuse_health_check(tmp_path, monkeypatch):
    """Async cached reuse must keep backend health checks off the event loop."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider, _sandbox, _ = _make_provider_with_active_sandbox(tmp_path, "sandbox-cached-async")
    provider._thread_sandboxes = {("default", "thread-cached-async"): "sandbox-cached-async"}
    provider._backend.is_alive = MagicMock(return_value=True)

    to_thread_calls: list[tuple[object, tuple[object, ...]]] = []

    async def fake_to_thread(func, /, *args, **kwargs):
        to_thread_calls.append((func, args))
        return func(*args, **kwargs)

    monkeypatch.setattr(aio_mod.asyncio, "to_thread", fake_to_thread)

    sandbox_id = await provider._acquire_internal_async("thread-cached-async", user_id="default")

    assert sandbox_id == "sandbox-cached-async"
    assert to_thread_calls == [
        (provider._ensure_skills_projection, ("default",)),
        (provider._reuse_in_process_sandbox, ("thread-cached-async",)),
    ]


def test_remote_backend_create_forwards_effective_user_id(monkeypatch):
    """Provisioner mode must receive user_id so PVC subPath matches user isolation."""
    remote_mod = importlib.import_module("deerflow.community.aio_sandbox.remote_backend")
    backend = remote_mod.RemoteSandboxBackend("http://provisioner:8002")
    token = set_current_user(SimpleNamespace(id="user-7"))
    posted: dict = {}

    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"sandbox_url": "http://sandbox.local"}

    def _post(url, json, timeout, headers=None):  # noqa: A002 - mirrors requests.post kwarg
        posted.update({"url": url, "json": json, "timeout": timeout})
        return _Response()

    monkeypatch.setattr(remote_mod.requests, "post", _post)
    monkeypatch.setattr(remote_mod, "user_should_see_legacy_skills", lambda _user_id: True)

    try:
        backend.create("thread-42", "sandbox-42")
    finally:
        reset_current_user(token)

    assert posted["url"] == "http://provisioner:8002/api/sandboxes"
    assert posted["json"] == {
        "sandbox_id": "sandbox-42",
        "thread_id": "thread-42",
        "user_id": "user-7",
        "include_legacy_skills": True,
        "skills_container_path": "/mnt/skills",
        "provision_lark_cli_runtime": False,
        "provision_lark_cli_broker": False,
    }


def test_remote_backend_create_prefers_explicit_user_id(monkeypatch):
    """Provisioner mode must not fall back to the ambient default for channel runs."""
    remote_mod = importlib.import_module("deerflow.community.aio_sandbox.remote_backend")
    backend = remote_mod.RemoteSandboxBackend("http://provisioner:8002")
    posted: dict = {}

    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"sandbox_url": "http://sandbox.local"}

    def _post(url, json, timeout, headers=None):  # noqa: A002 - mirrors requests.post kwarg
        posted.update({"url": url, "json": json, "timeout": timeout})
        return _Response()

    monkeypatch.setattr(remote_mod.requests, "post", _post)
    monkeypatch.setattr(remote_mod, "get_effective_user_id", lambda: "default")
    monkeypatch.setattr(remote_mod, "user_should_see_legacy_skills", lambda _user_id: False)

    backend.create("thread-42", "sandbox-42", user_id="ou-user")

    assert posted["json"]["user_id"] == "ou-user"
    assert posted["json"]["include_legacy_skills"] is False


def test_create_sandbox_requests_runtime_when_lark_installed(tmp_path, monkeypatch):
    """The provider must request lark-cli runtime provisioning when Lark is installed."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._config = {"replicas": 3}
    provider._warm_pool = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._last_activity = {}
    provider._lock = aio_mod.threading.Lock()

    captured: dict = {}

    def _create(thread_id, sandbox_id, *, extra_mounts=None, user_id=None, provision_lark_cli_runtime=False, provision_lark_cli_broker=False):
        captured["provision_lark_cli_runtime"] = provision_lark_cli_runtime
        captured["provision_lark_cli_broker"] = provision_lark_cli_broker
        return aio_mod.SandboxInfo(sandbox_id=sandbox_id, sandbox_url="http://sandbox")

    provider._backend = SimpleNamespace(create=_create, destroy=MagicMock(), discover=MagicMock(return_value=None))
    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready", lambda *_a, **_k: True)
    monkeypatch.setattr(provider, "_get_extra_mounts", lambda *_a, **_k: [])
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_lark_integration_active", staticmethod(lambda user_id=None: True))
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_lark_broker_active", staticmethod(lambda user_id=None: False))
    monkeypatch.setattr(provider, "_register_created_sandbox", lambda *a, **k: "sandbox-lark")

    provider._create_sandbox("thread-lark", "sandbox-lark", user_id="alice")
    assert captured["provision_lark_cli_runtime"] is True
    assert captured["provision_lark_cli_broker"] is False


def test_create_sandbox_requests_broker_when_active(tmp_path, monkeypatch):
    """Broker mode (Pattern B) is requested when the provisioner reports it."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._config = {"replicas": 3}
    provider._warm_pool = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._last_activity = {}
    provider._lock = aio_mod.threading.Lock()

    captured: dict = {}

    def _create(thread_id, sandbox_id, *, extra_mounts=None, user_id=None, provision_lark_cli_runtime=False, provision_lark_cli_broker=False):
        captured["provision_lark_cli_runtime"] = provision_lark_cli_runtime
        captured["provision_lark_cli_broker"] = provision_lark_cli_broker
        return aio_mod.SandboxInfo(sandbox_id=sandbox_id, sandbox_url="http://sandbox")

    provider._backend = SimpleNamespace(create=_create, destroy=MagicMock(), discover=MagicMock(return_value=None))
    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready", lambda *_a, **_k: True)
    monkeypatch.setattr(provider, "_get_extra_mounts", lambda *_a, **_k: [])
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_lark_integration_active", staticmethod(lambda user_id=None: True))
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_lark_broker_active", staticmethod(lambda user_id=None: True))
    monkeypatch.setattr(provider, "_register_created_sandbox", lambda *a, **k: "sandbox-broker")

    provider._create_sandbox("thread-broker", "sandbox-broker", user_id="alice")
    assert captured["provision_lark_cli_runtime"] is True
    assert captured["provision_lark_cli_broker"] is True


def test_create_sandbox_skips_runtime_when_lark_absent(tmp_path, monkeypatch):
    """No runtime provisioning request when the Lark skill pack is not installed."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._config = {"replicas": 3}
    provider._warm_pool = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._last_activity = {}
    provider._lock = aio_mod.threading.Lock()

    captured: dict = {}

    def _create(thread_id, sandbox_id, *, extra_mounts=None, user_id=None, provision_lark_cli_runtime=False, provision_lark_cli_broker=False):
        captured["provision_lark_cli_runtime"] = provision_lark_cli_runtime
        captured["provision_lark_cli_broker"] = provision_lark_cli_broker
        return aio_mod.SandboxInfo(sandbox_id=sandbox_id, sandbox_url="http://sandbox")

    provider._backend = SimpleNamespace(create=_create, destroy=MagicMock(), discover=MagicMock(return_value=None))
    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready", lambda *_a, **_k: True)
    monkeypatch.setattr(provider, "_get_extra_mounts", lambda *_a, **_k: [])
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_lark_integration_active", staticmethod(lambda user_id=None: False))
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_lark_broker_active", staticmethod(lambda user_id=None: False))
    monkeypatch.setattr(provider, "_register_created_sandbox", lambda *a, **k: "sandbox-nolark")

    provider._create_sandbox("thread-nolark", "sandbox-nolark", user_id="alice")
    assert captured["provision_lark_cli_runtime"] is False
    assert captured["provision_lark_cli_broker"] is False


# ── Sandbox client teardown (#2872) ──────────────────────────────────────────


def _make_provider_with_active_sandbox(tmp_path, sandbox_id: str):
    """Build a provider with one active sandbox suitable for release/destroy/shutdown tests."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._lock = aio_mod.threading.Lock()
    provider._warm_pool = {}
    provider._sandbox_infos = {
        sandbox_id: aio_mod.SandboxInfo(sandbox_id=sandbox_id, sandbox_url="http://sandbox-host"),
    }
    provider._thread_sandboxes = {}
    provider._last_activity = {sandbox_id: 0.0}
    provider._local_teardown = set()
    provider._acquire_epoch = {}
    provider._acquire_epoch_counter = 0
    provider._acquire_inflight = {}
    provider._shutdown_called = False
    provider._idle_checker_thread = None
    provider._backend = SimpleNamespace(destroy=MagicMock())

    sandbox = MagicMock()
    sandbox.id = sandbox_id
    sandbox.close = MagicMock()
    provider._sandboxes = {sandbox_id: sandbox}
    return provider, sandbox, aio_mod


def test_release_closes_cached_sandbox_client(tmp_path):
    """release() must close the host-side client owned by the cached AioSandbox (#2872)."""
    provider, sandbox, _ = _make_provider_with_active_sandbox(tmp_path, "sandbox-rel")

    provider.release("sandbox-rel")

    sandbox.close.assert_called_once_with()
    # And the sandbox is parked in the warm pool (container still running).
    assert "sandbox-rel" in provider._warm_pool
    assert "sandbox-rel" not in provider._sandboxes


def test_destroy_closes_cached_sandbox_client(tmp_path):
    """destroy() must close the host-side client before backend container teardown (#2872)."""
    provider, sandbox, _ = _make_provider_with_active_sandbox(tmp_path, "sandbox-destroy")
    backend_destroy = provider._backend.destroy

    provider.destroy("sandbox-destroy")

    sandbox.close.assert_called_once_with()
    backend_destroy.assert_called_once()
    assert "sandbox-destroy" not in provider._sandboxes
    assert "sandbox-destroy" not in provider._sandbox_infos


def test_shutdown_closes_all_active_sandbox_clients(tmp_path):
    """shutdown() must close every cached AioSandbox client during teardown (#2872)."""
    provider, sandbox, _ = _make_provider_with_active_sandbox(tmp_path, "sandbox-shut")

    provider.shutdown()

    sandbox.close.assert_called_once_with()
    provider._backend.destroy.assert_called_once()
    assert provider._sandboxes == {}


@pytest.mark.parametrize("teardown", ["reset", "shutdown"])
def test_teardown_refuses_to_destroy_an_invocation_owned_projection(
    tmp_path,
    teardown,
):
    from deerflow.runtime.skill_projection import get_skill_projection_coordinator

    provider, sandbox, _ = _make_provider_with_active_sandbox(
        tmp_path,
        "sandbox-owned",
    )
    identity = ("shutdown-owner", "shutdown-thread")
    provider._thread_sandboxes[identity] = "sandbox-owned"
    provider._active_sandbox_identity["sandbox-owned"] = identity
    coordinator = get_skill_projection_coordinator()
    reservation = coordinator.reserve_admission(
        user_id=identity[0],
        thread_id=identity[1],
        reservation_id="aio-shutdown-reservation",
        snapshot_id=None,
    )
    coordinator.promote_admission(reservation, run_id="aio-shutdown-run")
    lead = coordinator.activate(
        user_id=identity[0],
        thread_id=identity[1],
        sandbox_id="sandbox-owned",
        run_id="aio-shutdown-run",
        snapshot_id=None,
        consumer_id="lead",
    )
    child = coordinator.retain(lead, consumer_id="subagent:still-running")
    assert coordinator.release(lead) is None

    try:
        with pytest.raises(
            AcceptedSkillSandboxBindingError,
            match="accepted_skill_snapshot_projection_in_use",
        ):
            getattr(provider, teardown)()

        assert provider._shutdown_called is False
        assert provider._sandboxes == {"sandbox-owned": sandbox}
        sandbox.close.assert_not_called()
        provider._backend.destroy.assert_not_called()
    finally:
        clear = coordinator.release(child)
        assert clear is not None
        assert coordinator.finalize_release(clear)

    getattr(provider, teardown)()
    sandbox.close.assert_called_once_with()
    provider._backend.destroy.assert_called_once()


@pytest.mark.asyncio
async def test_reset_closes_acquire_serializer_executor(tmp_path):
    provider = _make_provider(tmp_path)
    async with provider._acquire_serializer.hold_async(("alice", "thread-reset")):
        pass
    worker_threads = tuple(provider._acquire_serializer.executor._threads)
    assert worker_threads

    provider.reset()

    for thread in worker_threads:
        thread.join(timeout=2)
    assert all(not thread.is_alive() for thread in worker_threads)
    with pytest.raises(RuntimeError, match="closed"):
        async with provider._acquire_serializer.hold_async(("alice", "thread-after-reset")):
            pass


def test_release_swallows_close_errors(tmp_path, caplog):
    """A failure inside sandbox.close() must not break provider release()."""
    provider, sandbox, _ = _make_provider_with_active_sandbox(tmp_path, "sandbox-rel-err")
    sandbox.close.side_effect = RuntimeError("boom")

    with caplog.at_level("WARNING"):
        provider.release("sandbox-rel-err")

    assert "Error closing sandbox sandbox-rel-err during release" in caplog.text
    # Still moved to warm pool: client teardown failure must not block lifecycle.
    assert "sandbox-rel-err" in provider._warm_pool


def test_get_uses_in_memory_registry_only(tmp_path):
    """get() must stay event-loop safe by avoiding backend health checks."""
    provider, sandbox, _ = _make_provider_with_active_sandbox(tmp_path, "sandbox-dead")
    provider._backend.is_alive = MagicMock(side_effect=AssertionError("get must not call backend health checks"))

    assert provider.get("sandbox-dead") is sandbox


def test_acquire_drops_dead_cached_sandbox(tmp_path, monkeypatch):
    """acquire() must replace a stale active cache entry after its container dies."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider, sandbox, _ = _make_provider_with_active_sandbox(tmp_path, "sandbox-dead")
    provider._thread_sandboxes = {("default", "thread-dead"): "sandbox-dead"}
    provider._config = {"replicas": 3}
    provider._backend.is_alive = MagicMock(return_value=False)
    provider._backend.discover = MagicMock(return_value=None)
    provider._backend.create = MagicMock(
        return_value=aio_mod.SandboxInfo(
            sandbox_id="sandbox-dead",
            sandbox_url="http://fresh-sandbox",
            container_name="deer-flow-sandbox-sandbox-dead",
        )
    )

    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_sandbox_id_for_thread", lambda _self, _thread_id, _user_id: "sandbox-dead")
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_get_extra_mounts", lambda _self, _thread_id, *, user_id=None: [])
    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=tmp_path))
    monkeypatch.setattr(aio_mod, "get_effective_user_id", lambda: None)
    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready", lambda _url, timeout=60: True)

    sandbox_id = provider.acquire("thread-dead", user_id="default")

    assert sandbox_id == "sandbox-dead"
    sandbox.close.assert_called_once_with()
    provider._backend.destroy.assert_called_once()
    provider._backend.create.assert_called_once()
    assert provider._thread_sandboxes[("default", "thread-dead")] == "sandbox-dead"
    assert provider._sandboxes["sandbox-dead"].base_url == "http://fresh-sandbox"


def test_acquire_keeps_cached_sandbox_when_health_check_errors(tmp_path):
    """Transient backend health-check errors must not destroy a tracked sandbox."""
    provider, sandbox, _ = _make_provider_with_active_sandbox(tmp_path, "sandbox-transient")
    provider._thread_sandboxes = {("default", "thread-transient"): "sandbox-transient"}
    provider._backend.is_alive = MagicMock(side_effect=OSError("docker daemon busy"))

    sandbox_id = provider.acquire("thread-transient", user_id="default")

    assert sandbox_id == "sandbox-transient"
    sandbox.close.assert_not_called()
    provider._backend.destroy.assert_not_called()
    assert provider._sandboxes["sandbox-transient"] is sandbox


def test_drop_unhealthy_sandbox_skips_recreated_entry(tmp_path):
    """A stale health-check result must not delete a newly registered sandbox."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._lock = aio_mod.threading.Lock()
    provider._warm_pool = {}
    provider._last_activity = {"sandbox-toctou": 1.0}
    provider._thread_sandboxes = {("default", "thread-toctou"): "sandbox-toctou"}
    old_info = aio_mod.SandboxInfo(sandbox_id="sandbox-toctou", sandbox_url="http://old-sandbox")
    new_info = aio_mod.SandboxInfo(sandbox_id="sandbox-toctou", sandbox_url="http://new-sandbox")
    new_sandbox = MagicMock()
    provider._sandbox_infos = {"sandbox-toctou": new_info}
    provider._sandboxes = {"sandbox-toctou": new_sandbox}
    provider._backend = SimpleNamespace(destroy=MagicMock())

    provider._drop_unhealthy_sandbox("sandbox-toctou", "stale health check", expected_info=old_info)

    new_sandbox.close.assert_not_called()
    provider._backend.destroy.assert_not_called()
    assert provider._sandbox_infos["sandbox-toctou"] is new_info
    assert provider._sandboxes["sandbox-toctou"] is new_sandbox
    assert provider._thread_sandboxes == {("default", "thread-toctou"): "sandbox-toctou"}


def test_acquire_skips_dead_warm_pool_sandbox(tmp_path, monkeypatch):
    """acquire() must create a fresh sandbox when the warm-pool entry died."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._lock = aio_mod.threading.Lock()
    provider._sandboxes = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._last_activity = {}
    provider._warm_pool = {
        "sandbox-warm-dead": (
            aio_mod.SandboxInfo(
                sandbox_id="sandbox-warm-dead",
                sandbox_url="http://stale-sandbox",
                container_name="deer-flow-sandbox-sandbox-warm-dead",
            ),
            0.0,
        )
    }
    provider._config = {"replicas": 3}
    provider._backend = SimpleNamespace(
        is_alive=MagicMock(return_value=False),
        destroy=MagicMock(),
        discover=MagicMock(return_value=None),
        create=MagicMock(
            return_value=aio_mod.SandboxInfo(
                sandbox_id="sandbox-warm-dead",
                sandbox_url="http://fresh-sandbox",
                container_name="deer-flow-sandbox-sandbox-warm-dead",
            )
        ),
    )

    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_sandbox_id_for_thread", lambda _self, _thread_id, _user_id: "sandbox-warm-dead")
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_get_extra_mounts", lambda _self, _thread_id, *, user_id=None: [])
    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=tmp_path))
    monkeypatch.setattr(aio_mod, "get_effective_user_id", lambda: None)
    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready", lambda _url, timeout=60: True)

    sandbox_id = provider.acquire("thread-warm-dead", user_id="default")

    assert sandbox_id == "sandbox-warm-dead"
    provider._backend.destroy.assert_called_once()
    provider._backend.create.assert_called_once()
    assert provider._warm_pool == {}
    assert provider._thread_sandboxes[("default", "thread-warm-dead")] == "sandbox-warm-dead"
    assert provider._sandboxes["sandbox-warm-dead"].base_url == "http://fresh-sandbox"


def test_destroy_swallows_close_errors_and_still_destroys_backend(tmp_path, caplog):
    """A failure in sandbox.close() must not skip backend container destruction."""
    provider, sandbox, _ = _make_provider_with_active_sandbox(tmp_path, "sandbox-dest-err")
    sandbox.close.side_effect = RuntimeError("boom")

    with caplog.at_level("WARNING"):
        provider.destroy("sandbox-dest-err")

    assert "Error closing sandbox sandbox-dest-err during destroy" in caplog.text
    provider._backend.destroy.assert_called_once()


def test_cleanup_idle_sandboxes_keeps_active_cleanup_and_delegates_warm_expiry(tmp_path):
    """AIO active-idle cleanup must remain local while warm expiry uses the shared lifecycle."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._lock = aio_mod.threading.Lock()
    provider._sandboxes = {"active-old": MagicMock()}
    provider._sandbox_infos = {
        "active-old": aio_mod.SandboxInfo(sandbox_id="active-old", sandbox_url="http://active-old"),
    }
    provider._thread_sandboxes = {("default", "thread-old"): "active-old"}
    provider._last_activity = {"active-old": 0.0}
    provider._warm_pool = {
        "warm-old": (
            aio_mod.SandboxInfo(sandbox_id="warm-old", sandbox_url="http://warm-old"),
            0.0,
        )
    }

    calls = []
    # The idle path destroys through `_destroy_tracked`, not `destroy()`: its
    # "still idle?" re-check has to run in the same critical section that
    # reserves the teardown, so it is passed down as a predicate. Asserting on
    # `destroy` here would pass vacuously — it is no longer on this path.
    provider._destroy_tracked = MagicMock(side_effect=lambda _sandbox_id, **_kw: calls.append("active"))
    provider._reap_expired_warm = MagicMock(side_effect=lambda _idle_timeout: calls.append("warm"))

    provider._cleanup_idle_sandboxes(1.0)

    assert provider._destroy_tracked.call_count == 1
    assert provider._destroy_tracked.call_args.args == ("active-old",)
    # The gate must actually be a live predicate, not a constant-true placeholder.
    assert provider._destroy_tracked.call_args.kwargs["still_reapable"]() is True
    provider._reap_expired_warm.assert_called_once_with(1.0)
    assert calls == ["active", "warm"]


def test_cleanup_idle_sandboxes_preserves_invocation_owned_projection(
    tmp_path,
    monkeypatch,
):
    """Idle reaping cannot tear down a sandbox retained by a background child."""
    from deerflow.runtime.skill_projection import SkillProjectionCoordinator

    provider, _, aio_mod = _make_provider_with_active_sandbox(
        tmp_path,
        "sandbox-owned",
    )
    provider._thread_sandboxes = {
        ("alice", "thread-owned"): "sandbox-owned",
    }
    provider._active_sandbox_identity = {
        "sandbox-owned": ("alice", "thread-owned"),
    }
    provider._destroy_tracked = MagicMock()
    provider._reap_expired_warm = MagicMock()
    coordinator = SkillProjectionCoordinator()
    coordinator.claim_committed_run(
        user_id="alice",
        thread_id="thread-owned",
        run_id="run-owned",
        snapshot_id=None,
    )
    token = coordinator.activate(
        user_id="alice",
        thread_id="thread-owned",
        sandbox_id="sandbox-owned",
        run_id="run-owned",
        snapshot_id=None,
        consumer_id="subagent:background",
    )
    monkeypatch.setattr(
        "deerflow.runtime.skill_projection.get_skill_projection_coordinator",
        lambda: coordinator,
    )

    provider._cleanup_idle_sandboxes(1.0)

    provider._destroy_tracked.assert_not_called()
    assert "sandbox-owned" in provider._sandboxes
    clear = coordinator.release(token)
    assert clear is not None
    assert coordinator.finalize_release(clear)


def test_create_sandbox_evicts_oldest_warm_replica_via_shared_lifecycle(tmp_path, monkeypatch):
    """Replica enforcement must destroy the oldest warm SandboxInfo before creating another."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._lock = aio_mod.threading.Lock()
    provider._config = {"replicas": 2}
    provider._sandboxes = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._last_activity = {}

    oldest_info = aio_mod.SandboxInfo(sandbox_id="warm-oldest", sandbox_url="http://warm-oldest")
    newest_info = aio_mod.SandboxInfo(sandbox_id="warm-newest", sandbox_url="http://warm-newest")
    created_info = aio_mod.SandboxInfo(sandbox_id="created", sandbox_url="http://created")
    provider._warm_pool = {
        "warm-newest": (newest_info, 20.0),
        "warm-oldest": (oldest_info, 10.0),
    }
    provider._backend = SimpleNamespace(
        create=MagicMock(return_value=created_info),
        destroy=MagicMock(),
    )
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_get_extra_mounts", lambda _self, _thread_id, *, user_id=None: [])
    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready", lambda _url, *, timeout=60: True)

    sandbox_id = provider._create_sandbox(None, "created", user_id="default")

    assert sandbox_id == "created"
    provider._backend.destroy.assert_called_once_with(oldest_info)
    assert "warm-oldest" not in provider._warm_pool
    assert provider._warm_pool == {"warm-newest": (newest_info, 20.0)}
    assert provider._sandbox_infos["created"] is created_info


def _make_tenant_isolation_provider(tmp_path, monkeypatch):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._lock = aio_mod.threading.Lock()
    provider._sandboxes = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._last_activity = {}
    provider._warm_pool = {}
    provider._active_sandbox_identity = {}
    provider._warm_pool_identity = {}
    provider._shutdown_called = False
    provider._config = {"replicas": 3, "idle_timeout": 0}

    create_calls = []

    def _create(thread_id, sandbox_id, **kwargs):
        create_calls.append((thread_id, sandbox_id, kwargs.get("user_id")))
        return aio_mod.SandboxInfo(
            sandbox_id=sandbox_id,
            sandbox_url=f"http://sandbox-{len(create_calls)}.local",
            container_name=f"deer-flow-sandbox-{sandbox_id}",
        )

    provider._backend = SimpleNamespace(
        create=MagicMock(side_effect=_create),
        destroy=MagicMock(),
        discover=MagicMock(return_value=None),
        is_alive=MagicMock(return_value=True),
        list_running=MagicMock(return_value=[]),
    )
    provider._claim_ownership = MagicMock(return_value=True)
    provider._held_teardown_lease = lambda _sandbox_id: contextlib.nullcontext()

    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=tmp_path))
    monkeypatch.setattr(
        aio_mod.AioSandboxProvider,
        "_get_extra_mounts",
        lambda self, thread_id, *, user_id=None: [],
    )
    monkeypatch.setattr(
        aio_mod,
        "wait_for_sandbox_ready",
        lambda _url, timeout=60: True,
    )
    return provider, create_calls, aio_mod


def test_aio_wider_id_separates_known_legacy_collision():
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    identity_a, identity_b = _LEGACY_COLLIDING_IDENTITIES
    user_a, thread_a = identity_a
    user_b, thread_b = identity_b

    old_a = hashlib.sha256(f"{user_a}:{thread_a}".encode()).hexdigest()[:8]
    old_b = hashlib.sha256(f"{user_b}:{thread_b}".encode()).hexdigest()[:8]

    assert old_a == old_b
    assert aio_mod.AioSandboxProvider._deterministic_sandbox_id(
        thread_a,
        user_a,
    ) != aio_mod.AioSandboxProvider._deterministic_sandbox_id(
        thread_b,
        user_b,
    )


def test_aio_forced_collision_never_overwrites_active_tenant(
    tmp_path,
    monkeypatch,
):
    provider, create_calls, aio_mod = _make_tenant_isolation_provider(
        tmp_path,
        monkeypatch,
    )
    monkeypatch.setattr(
        aio_mod.AioSandboxProvider,
        "_deterministic_sandbox_id",
        staticmethod(lambda thread_id, user_id: "deadbeefdeadbeef"),
    )

    sandbox_id = provider.acquire("thread-a", user_id="user-a")
    info_a = provider._sandbox_infos[sandbox_id]
    provider.release(sandbox_id)

    assert sandbox_id in provider._warm_pool

    with pytest.raises(aio_mod.SandboxIdentityCollisionError):
        provider.acquire("thread-b", user_id="user-b")

    assert provider._warm_pool[sandbox_id][0] is info_a
    provider._backend.destroy.assert_not_called()
    assert len(create_calls) == 1
    assert provider.acquire("thread-a", user_id="user-a") == sandbox_id
    assert provider._sandbox_infos[sandbox_id] is info_a


# --- #4248 regression: readiness-timeout destroy ownership ---


def _make_unready_destroy_provider(tmp_path, *, sandbox_id, base_url, monkeypatch, aio_mod):
    """Provider wired so ``_create_sandbox`` reaches the readiness-timeout branch.

    ``wait_for_sandbox_ready`` always returns False; the backend records what the
    destroy path did. Mirrors the fixtures used by the warm-replica eviction
    test, minus the warm pool.
    """
    provider = _make_provider(tmp_path)
    provider._lock = aio_mod.threading.Lock()
    provider._config = {"replicas": 3}
    provider._warm_pool = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._last_activity = {}
    provider._active_sandbox_identity = {}
    provider._warm_pool_identity = {}
    unready_info = aio_mod.SandboxInfo(sandbox_id=sandbox_id, sandbox_url=base_url)
    provider._backend = SimpleNamespace(
        create=MagicMock(return_value=unready_info),
        destroy=MagicMock(),
    )
    monkeypatch.setattr(
        aio_mod.AioSandboxProvider,
        "_get_extra_mounts",
        lambda _self, _thread_id, *, user_id=None: [],
    )
    return provider, unready_info


def test_create_sandbox_claims_ownership_before_readiness_timeout_destroy(tmp_path, monkeypatch):
    """#4248: a readiness-timeout destroy must run under a `del:` teardown lease.

    Before #4248 the unready container was reaped with a bare ``destroy`` call.
    Ownership is published by ``_register_created_sandbox`` only after the
    readiness gate, so for up to 60s the container ran unowned and a peer could
    adopt it; the subsequent stop landed on whatever turn the peer had handed it.
    """
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider, unready_info = _make_unready_destroy_provider(
        tmp_path,
        sandbox_id="unready",
        base_url="http://unready",
        monkeypatch=monkeypatch,
        aio_mod=aio_mod,
    )
    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready", lambda _url, *, timeout=60: False)

    # The heartbeat releases the teardown lease on exit, so the destroy call is
    # the only place we can observe the `del:` state. Snapshot the lease at
    # the instant destroy runs.
    destroy_snapshots: list = []

    def destroy_spy(info):
        destroy_snapshots.append(provider._ownership._leases.get(info.sandbox_id))

    provider._backend.destroy.side_effect = destroy_spy

    with pytest.raises(RuntimeError, match="failed to become ready"):
        provider._create_sandbox("thread-4248", "unready", user_id="user-4248")

    provider._backend.destroy.assert_called_once_with(unready_info)
    assert destroy_snapshots, "destroy must run inside the held teardown lease"
    lease = destroy_snapshots[0]
    assert lease is not None, "teardown lease must be held while destroy runs"
    assert lease.owner_id == provider._owner_id
    assert lease.destroying is True, "destroy must run under a `del:` teardown lease"


@pytest.mark.anyio
async def test_create_sandbox_async_claims_ownership_before_readiness_timeout_destroy(tmp_path, monkeypatch):
    """#4248 (async path): same teardown-lease guard on the async readiness branch."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider, unready_info = _make_unready_destroy_provider(
        tmp_path,
        sandbox_id="unready-async",
        base_url="http://unready-async",
        monkeypatch=monkeypatch,
        aio_mod=aio_mod,
    )

    async def fake_wait_async(_url, *, timeout=60, poll_interval=1.0):
        return False

    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready_async", fake_wait_async)
    monkeypatch.setattr(
        aio_mod,
        "wait_for_sandbox_ready",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("sync readiness should not be used")),
    )

    destroy_snapshots: list = []

    def destroy_spy(info):
        destroy_snapshots.append(provider._ownership._leases.get(info.sandbox_id))

    provider._backend.destroy.side_effect = destroy_spy

    with pytest.raises(RuntimeError, match="failed to become ready"):
        await provider._create_sandbox_async("thread-4248-async", "unready-async", user_id="user-4248-async")

    provider._backend.destroy.assert_called_once_with(unready_info)
    assert destroy_snapshots, "destroy must run inside the held teardown lease"
    lease = destroy_snapshots[0]
    assert lease is not None
    assert lease.owner_id == provider._owner_id
    assert lease.destroying is True, "destroy must run under a `del:` teardown lease"


def test_create_sandbox_skips_destroy_when_unready_sandbox_owned_by_peer(tmp_path, monkeypatch):
    """#4248 fail-closed: if a peer already owns the unready container, do not stop it.

    The lease refuses our teardown claim, so the container is left for the peer
    to reap via its own reconciliation. Stopping it anyway would be the
    cross-instance kill this guard exists to prevent.
    """
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider, unready_info = _make_unready_destroy_provider(
        tmp_path,
        sandbox_id="peer-owned",
        base_url="http://peer-owned",
        monkeypatch=monkeypatch,
        aio_mod=aio_mod,
    )
    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready", lambda _url, *, timeout=60: False)

    # Every claim refuses: peer holds the lease (or the store cannot answer).
    provider._ownership.claim = lambda _sid, *, for_destroy=False: False

    with pytest.raises(RuntimeError, match="failed to become ready"):
        provider._create_sandbox("thread-peer", "peer-owned", user_id="user-peer")

    provider._backend.destroy.assert_not_called()


def test_reconcile_does_not_adopt_a_container_whose_unready_teardown_is_reserved(tmp_path, monkeypatch):
    """#4248 follow-up: the readiness-timeout destroy must hold the local
    reservation, not just the cross-instance claim.

    The claim succeeds against our own lease by design, so without
    ``_reserve_local_teardown`` there is a window — readiness failed, claim not
    yet written — in which the idle checker's ``_reconcile_orphans`` sees the
    container running, untracked, and past its recovery grace, and adopts it
    into ``_warm_pool``. The claim then still succeeds (the lease is ours) and
    the stop lands on an entry this instance has just adopted, leaving a dead
    warm entry for the next reclaim to hand out. This is the same interleaving
    shape as ``test_reconcile_does_not_adopt_a_container_this_instance_is_tearing_down``
    in ``test_sandbox_orphan_reconciliation.py``.
    """
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider, unready_info = _make_unready_destroy_provider(
        tmp_path,
        sandbox_id="unready-race",
        base_url="http://unready-race",
        monkeypatch=monkeypatch,
        aio_mod=aio_mod,
    )
    provider._unowned_since = {}
    provider._backend.list_running = MagicMock(return_value=[unready_info])
    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready", lambda _url, *, timeout=60: False)

    # Park the destroy thread after it has reserved the local teardown but
    # before the `del:` claim lands — the exact window reconcile would adopt in.
    at_claim, let_claim = threading.Event(), threading.Event()
    real_claim = provider._claim_ownership

    def gated_claim(sandbox_id, *, for_destroy=False):
        if for_destroy:
            at_claim.set()
            assert let_claim.wait(timeout=5)
        return real_claim(sandbox_id, for_destroy=for_destroy)

    provider._claim_ownership = gated_claim
    reaper = threading.Thread(
        target=lambda: provider._destroy_unready_sandbox("unready-race", unready_info),
        daemon=True,
    )
    reaper.start()
    try:
        assert at_claim.wait(timeout=5), "the unready destroy never reached its claim"
        # Reserved locally, still running, untracked, and the `del:` marker is
        # not written yet — exactly the shape reconcile would have adopted
        # before the reservation wrapped this path.
        provider._reconcile_orphans()
        assert "unready-race" not in provider._warm_pool, "reconcile adopted a container this instance is tearing down"
    finally:
        let_claim.set()
        reaper.join(timeout=5)

    # The reservation is released once the stop returns, and the destroy did run.
    provider._backend.destroy.assert_called_once_with(unready_info)
    assert provider._local_teardown == set(), "a teardown reservation outlived the stop it guarded"


def test_reconcile_adopts_unready_container_when_no_teardown_is_in_flight(tmp_path, monkeypatch):
    """Mirror of the interleaving test: with no destroy running, the same
    not-yet-registered container *is* adoptable, so the guard above cannot
    over-block legitimate reconciliation of a container whose creator crashed
    before the readiness gate.
    """
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider, unready_info = _make_unready_destroy_provider(
        tmp_path,
        sandbox_id="adoptable",
        base_url="http://adoptable",
        monkeypatch=monkeypatch,
        aio_mod=aio_mod,
    )
    provider._unowned_since = {}
    provider._backend.list_running = MagicMock(return_value=[unready_info])

    provider._reconcile_orphans()

    assert "adoptable" in provider._warm_pool, "reconcile must still adopt a genuinely unowned container"


def test_deterministic_sandbox_id_matches_shared_identity():
    from deerflow.sandbox.identity import derive_sandbox_scope_token

    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    assert aio_mod.AioSandboxProvider._deterministic_sandbox_id("t-1", "u-1") == derive_sandbox_scope_token(user_id="u-1", thread_id="t-1")


# --- per-sandbox request headers take upstream's shape (session provider phase 5) ---


def _headers_provider(tmp_path, aio_mod, monkeypatch, infos: dict):
    provider = _make_provider(tmp_path)
    provider._config = {"replicas": 3}
    provider._lock = aio_mod.threading.Lock()
    provider._backend = SimpleNamespace(
        create=lambda _thread_id, sandbox_id, **_kwargs: infos[sandbox_id],
        destroy=MagicMock(),
        discover=MagicMock(return_value=None),
    )
    monkeypatch.setattr(provider, "_get_extra_mounts", lambda *_a, **_k: [])
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_lark_integration_active", staticmethod(lambda user_id=None: False))
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_lark_broker_active", staticmethod(lambda user_id=None: False))
    monkeypatch.setattr(provider, "_register_created_sandbox", lambda _thread_id, sandbox_id, _info, **_k: sandbox_id)
    return provider


def test_register_created_sandbox_hands_request_headers_to_the_sandbox(tmp_path, monkeypatch):
    """The provider passes ``info.request_headers`` straight through, empty or
    not: one call shape, no conditional construction, exactly as upstream."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._lock = aio_mod.threading.Lock()
    constructed: list[dict] = []

    class _FakeSandbox:
        def __init__(self, **kwargs):
            constructed.append(kwargs)
            self.id = kwargs["id"]

    monkeypatch.setattr(aio_mod, "AioSandbox", _FakeSandbox)
    headers = {"Authorization": "Bearer attempt"}

    provider._register_created_sandbox("thread-h", "sb-h", aio_mod.SandboxInfo(sandbox_id="sb-h", sandbox_url="http://sb", request_headers=headers), user_id="alice")
    provider._register_created_sandbox("thread-p", "sb-p", aio_mod.SandboxInfo(sandbox_id="sb-p", sandbox_url="http://sb"), user_id="alice")

    assert constructed == [
        {"id": "sb-h", "base_url": "http://sb", "request_headers": headers},
        {"id": "sb-p", "base_url": "http://sb", "request_headers": {}},
    ]


def test_create_sandbox_passes_request_headers_to_readiness_only_when_present(tmp_path, monkeypatch):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    headers = {"Authorization": "Bearer attempt"}
    infos = {
        "sb-h": aio_mod.SandboxInfo(sandbox_id="sb-h", sandbox_url="http://sb-h", request_headers=headers),
        "sb-p": aio_mod.SandboxInfo(sandbox_id="sb-p", sandbox_url="http://sb-p"),
    }
    provider = _headers_provider(tmp_path, aio_mod, monkeypatch, infos)
    readiness_calls: list[tuple[str, dict]] = []

    def fake_ready(url, **kwargs):
        readiness_calls.append((url, kwargs))
        return True

    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready", fake_ready)

    assert provider._create_sandbox("thread-h", "sb-h", user_id="alice") == "sb-h"
    assert provider._create_sandbox("thread-p", "sb-p", user_id="alice") == "sb-p"

    assert readiness_calls == [
        ("http://sb-h", {"timeout": aio_mod.SANDBOX_LOCAL_PROVIDER_READY_TIMEOUT, "headers": headers}),
        ("http://sb-p", {"timeout": aio_mod.SANDBOX_LOCAL_PROVIDER_READY_TIMEOUT}),
    ]


@pytest.mark.anyio
async def test_create_sandbox_async_passes_request_headers_to_readiness_only_when_present(tmp_path, monkeypatch):
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    headers = {"Authorization": "Bearer attempt"}
    infos = {
        "sb-h": aio_mod.SandboxInfo(sandbox_id="sb-h", sandbox_url="http://sb-h", request_headers=headers),
        "sb-p": aio_mod.SandboxInfo(sandbox_id="sb-p", sandbox_url="http://sb-p"),
    }
    provider = _headers_provider(tmp_path, aio_mod, monkeypatch, infos)
    readiness_calls: list[tuple[str, dict]] = []

    async def fake_ready_async(url, **kwargs):
        readiness_calls.append((url, kwargs))
        return True

    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready_async", fake_ready_async)
    monkeypatch.setattr(aio_mod, "wait_for_sandbox_ready", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("sync readiness should not be used")))

    assert await provider._create_sandbox_async("thread-h", "sb-h", user_id="alice") == "sb-h"
    assert await provider._create_sandbox_async("thread-p", "sb-p", user_id="alice") == "sb-p"

    assert readiness_calls == [
        ("http://sb-h", {"timeout": aio_mod.SANDBOX_LOCAL_PROVIDER_READY_TIMEOUT, "headers": headers}),
        ("http://sb-p", {"timeout": aio_mod.SANDBOX_LOCAL_PROVIDER_READY_TIMEOUT}),
    ]
