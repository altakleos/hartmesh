from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest
from deerflow_extension_api import TenantReferenceV1

from deerflow.runtime.runs.store.base import (
    LifecycleTransition,
    LifecycleType,
    build_lifecycle_payload,
)
from deerflow.sandbox.accepted_material import (
    AcceptedExecutionEvidenceV1,
    AcceptedFileType,
    AcceptedFileV1,
    AcceptedMaterialCapability,
    AcceptedMaterialError,
    AcceptedMaterializerSelection,
    AcceptedMaterialLeaseV1,
    AcceptedMaterialRequestV1,
    AcceptedSkillSandboxBindingV1,
    InMemoryAcceptedMaterializer,
    InMemoryAcceptedMaterialState,
    capture_accepted_file_manifest,
    resolve_accepted_materializer,
)
from deerflow.sandbox.sandbox_provider import AcceptedSkillMaterialCapability


def _tenant() -> TenantReferenceV1:
    digest = "1" * 64
    return TenantReferenceV1(
        version=1,
        public_ref=f"tenant-{digest[:16]}",
        digest=digest,
    )


def _regular_file(path: str, content: bytes = b"content") -> AcceptedFileV1:
    return AcceptedFileV1(
        version=1,
        path=path,
        file_type=AcceptedFileType.REGULAR,
        size=len(content),
        mode=0o444,
        digest=hashlib.sha256(content).hexdigest(),
    )


def test_accepted_material_request_has_one_canonical_persisted_form() -> None:
    content = b"# demo\n"
    accepted_file = AcceptedFileV1(
        version=1,
        path="SKILL.md",
        file_type=AcceptedFileType.REGULAR,
        size=len(content),
        mode=0o444,
        digest=hashlib.sha256(content).hexdigest(),
    )

    request = AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=_tenant(),
        user_ref="user-abc123",
        thread_ref="thread-def456",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(accepted_file,),
        runtime_image_digest="5" * 64,
        lease_expires_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
    )

    assert request.to_persisted() == {
        "version": 1,
        "run_id": "run-1",
        "attempt_id": "attempt-1",
        "tenant": {
            "version": 1,
            "public_ref": "tenant-1111111111111111",
            "digest": "1" * 64,
        },
        "user_ref": "user-abc123",
        "thread_ref": "thread-def456",
        "agent_revision_digest": "2" * 64,
        "skill_snapshot_digest": "3" * 64,
        "skill_scope_digest": "4" * 64,
        "file_manifest": [
            {
                "version": 1,
                "path": "SKILL.md",
                "file_type": "regular",
                "size": 7,
                "mode": 0o444,
                "digest": "bc70e26f40b8816eb177813dda1f5f529a27a4641d45aa19cae2348a8c6a5fe9",
                "link_target": None,
            }
        ],
        "runtime_image_digest": "5" * 64,
        "lease_expires_at": "2026-08-31T12:00:00Z",
        "digest": request.digest,
    }
    assert AcceptedMaterialRequestV1.from_persisted(request.to_persisted()) == request


@pytest.mark.parametrize("mode", [0o644, 0o2444, 0o4444, 0o1444])
def test_accepted_material_file_rejects_write_and_special_mode_bits(mode: int) -> None:
    content = b"content"

    with pytest.raises(ValueError, match="read-only without special bits"):
        AcceptedFileV1(
            version=1,
            path="SKILL.md",
            file_type=AcceptedFileType.REGULAR,
            size=len(content),
            mode=mode,
            digest=hashlib.sha256(content).hexdigest(),
        )


@pytest.mark.parametrize(
    "path",
    [
        "../SKILL.md",
        "/SKILL.md",
        "public//SKILL.md",
        "public\\SKILL.md",
        "public/e\u0301.md",
        "/".join(["deep"] * 33),
    ],
)
def test_accepted_material_file_rejects_ambiguous_or_traversing_paths(
    path: str,
) -> None:
    with pytest.raises(ValueError, match="path"):
        _regular_file(path)


@pytest.mark.parametrize("target", ["../outside", "/outside", "safe//target"])
def test_accepted_material_symlink_rejects_escaping_or_ambiguous_target(
    target: str,
) -> None:
    with pytest.raises(ValueError, match="link_target"):
        AcceptedFileV1(
            version=1,
            path="public/link",
            file_type=AcceptedFileType.SYMLINK,
            size=len(target.encode("utf-8")),
            mode=0,
            digest=hashlib.sha256(target.encode("utf-8")).hexdigest(),
            link_target=target,
        )


@pytest.mark.parametrize(
    "manifest",
    [
        (_regular_file("public/Skill.md"), _regular_file("public/skill.md")),
        (_regular_file("public"), _regular_file("public/SKILL.md")),
    ],
)
def test_accepted_material_request_rejects_manifest_aliases_and_non_directory_parents(
    manifest: tuple[AcceptedFileV1, ...],
) -> None:
    with pytest.raises(ValueError, match="duplicate|non-directory parent"):
        AcceptedMaterialRequestV1.build(
            run_id="run-1",
            attempt_id="attempt-1",
            tenant=_tenant(),
            user_ref="user-ref",
            thread_ref="thread-ref",
            agent_revision_digest="2" * 64,
            skill_snapshot_digest="3" * 64,
            skill_scope_digest="4" * 64,
            file_manifest=manifest,
            runtime_image_digest="5" * 64,
            lease_expires_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
        )


def test_accepted_material_request_rejects_missing_parents_and_dangling_links() -> None:
    target = "missing.md"
    dangling = AcceptedFileV1(
        version=1,
        path="public/link",
        file_type=AcceptedFileType.SYMLINK,
        size=len(target),
        mode=0,
        digest=hashlib.sha256(target.encode()).hexdigest(),
        link_target=target,
    )
    directory = AcceptedFileV1(
        version=1,
        path="public",
        file_type=AcceptedFileType.DIRECTORY,
        size=0,
        mode=0o555,
        digest=None,
    )

    with pytest.raises(ValueError, match="dangling symlink"):
        AcceptedMaterialRequestV1.build(
            run_id="run-1",
            attempt_id="attempt-1",
            tenant=_tenant(),
            user_ref="user-ref",
            thread_ref="thread-ref",
            agent_revision_digest="2" * 64,
            skill_snapshot_digest="3" * 64,
            skill_scope_digest="4" * 64,
            file_manifest=(directory, dangling),
            runtime_image_digest="5" * 64,
            lease_expires_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
        )


def test_accepted_material_request_accepts_a_closed_safe_symlink() -> None:
    target = "target.md"
    directory = AcceptedFileV1(
        version=1,
        path="public",
        file_type=AcceptedFileType.DIRECTORY,
        size=0,
        mode=0o555,
        digest=None,
    )
    link = AcceptedFileV1(
        version=1,
        path="public/link",
        file_type=AcceptedFileType.SYMLINK,
        size=len(target),
        mode=0,
        digest=hashlib.sha256(target.encode()).hexdigest(),
        link_target=target,
    )

    request = AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=_tenant(),
        user_ref="user-ref",
        thread_ref="thread-ref",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(directory, link, _regular_file("public/target.md")),
        runtime_image_digest="5" * 64,
        lease_expires_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
    )

    assert request.file_manifest[1] == link


def test_capture_manifest_binds_directories_modes_and_exact_bytes(tmp_path) -> None:
    nested = tmp_path / "public" / "demo"
    nested.mkdir(parents=True)
    skill = nested / "SKILL.md"
    skill.write_bytes(b"# demo\n")
    skill.chmod(0o400)
    nested.chmod(0o500)
    nested.parent.chmod(0o500)

    try:
        manifest = capture_accepted_file_manifest(tmp_path)

        assert [entry.path for entry in manifest] == [
            "public",
            "public/demo",
            "public/demo/SKILL.md",
        ]
        assert manifest[-1].digest == hashlib.sha256(b"# demo\n").hexdigest()
    finally:
        skill.chmod(0o600)
        nested.chmod(0o700)
        nested.parent.chmod(0o700)


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        (
            lambda: AcceptedFileV1(
                version=True,
                path="SKILL.md",
                file_type=AcceptedFileType.REGULAR,
                size=0,
                mode=0o444,
                digest=hashlib.sha256(b"").hexdigest(),
            ),
            "file version",
        ),
    ],
)
def test_accepted_material_versions_reject_booleans(factory, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        factory()


def test_accepted_material_request_rejects_boolean_nested_tenant_version() -> None:
    tenant = TenantReferenceV1(
        version=True,  # type: ignore[arg-type]
        public_ref="tenant-1111111111111111",
        digest="1" * 64,
    )

    with pytest.raises(ValueError, match="tenant version"):
        AcceptedMaterialRequestV1.build(
            run_id="run-1",
            attempt_id="attempt-1",
            tenant=tenant,
            user_ref="user-ref",
            thread_ref="thread-ref",
            agent_revision_digest="2" * 64,
            skill_snapshot_digest="3" * 64,
            skill_scope_digest="4" * 64,
            file_manifest=(),
            runtime_image_digest="5" * 64,
            lease_expires_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
        )


def test_execution_evidence_binds_request_and_lease_without_persisting_handle() -> None:
    request = AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=_tenant(),
        user_ref="user-abc123",
        thread_ref="thread-def456",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
    )
    renewal_handle = object()
    lease = AcceptedMaterialLeaseV1(
        version=1,
        provider_kind="aio",
        provider_instance_ref="sandbox-1",
        ownership_epoch=7,
        lease_expires_at=request.lease_expires_at,
        opaque_renewal_handle=renewal_handle,
    )
    evidence = AcceptedExecutionEvidenceV1.build(
        run_id=request.run_id,
        attempt_id=request.attempt_id,
        tenant=request.tenant,
        provider_kind=lease.provider_kind,
        provider_instance_ref=lease.provider_instance_ref,
        ownership_epoch=lease.ownership_epoch,
        runtime_image_digest=request.runtime_image_digest,
        skill_snapshot_digest=request.skill_snapshot_digest,
        skill_scope_digest=request.skill_scope_digest,
        materialization_digest="6" * 64,
        verifier_image_digest="7" * 64,
        verifier_contract_version="rwx_verified_copy_v2",
        read_only_proof_digest="8" * 64,
        qualification_scope="durable_one_replica_rwx_verified_copy_v2_nonempty_skill",
    )

    assert lease.to_persisted() == {
        "version": 1,
        "provider_kind": "aio",
        "provider_instance_ref": "sandbox-1",
        "ownership_epoch": 7,
        "lease_expires_at": "2026-08-31T12:00:00Z",
    }
    assert renewal_handle not in lease.to_persisted().values()
    assert evidence.binds(request, lease)
    assert AcceptedExecutionEvidenceV1.from_persisted(evidence.to_persisted()) == evidence
    tampered = deepcopy(evidence.to_persisted())
    tampered["ownership_epoch"] = 8
    with pytest.raises(ValueError, match="digest"):
        AcceptedExecutionEvidenceV1.from_persisted(tampered)
    lifecycle = build_lifecycle_payload(
        LifecycleTransition(
            lifecycle_type=LifecycleType.started,
            status="running",
            execution_evidence_json=evidence.to_persisted(),
            execution_evidence_digest=evidence.digest,
        ),
    )
    assert lifecycle["execution_evidence_digest"] == evidence.digest


def test_legacy_provider_capability_name_is_the_neutral_contract() -> None:
    assert AcceptedSkillMaterialCapability is AcceptedMaterialCapability


@pytest.mark.asyncio
async def test_materializer_resolution_uses_only_an_opt_in_provider_hook() -> None:
    adapter = InMemoryAcceptedMaterializer(owner="worker-a")
    binding = AcceptedSkillSandboxBindingV1(
        snapshot_id="3" * 64,
        run_id="run-1",
        generation=1,
    )
    observed: dict[str, object] = {}

    class ProviderWithAdapter:
        async def accepted_materializer_selection(self, **kwargs):
            observed.update(kwargs)
            return AcceptedMaterializerSelection(
                materializer=adapter,
                runtime_image_digest="5" * 64,
                lease_duration=timedelta(minutes=5),
            )

    selection = await resolve_accepted_materializer(
        ProviderWithAdapter(),
        binding=binding,
        thread_id="thread-1",
        user_id="user-1",
    )

    assert selection is not None
    assert selection.materializer is adapter
    assert selection.runtime_image_digest == "5" * 64
    assert observed == {
        "binding": binding,
        "thread_id": "thread-1",
        "user_id": "user-1",
    }
    assert (
        await resolve_accepted_materializer(
            object(),
            binding=binding,
            thread_id="thread-1",
            user_id="user-1",
        )
        is None
    )


@pytest.mark.asyncio
async def test_in_memory_adapter_models_idempotency_and_epoch_fences() -> None:
    now = datetime(2026, 8, 31, 12, tzinfo=UTC)

    def clock() -> datetime:
        return now

    request = AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=_tenant(),
        user_ref="user-abc123",
        thread_ref="thread-def456",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=now + timedelta(seconds=30),
    )
    state = InMemoryAcceptedMaterialState()
    first = InMemoryAcceptedMaterializer(
        owner="worker-a",
        state=state,
        clock=clock,
        lease_duration=timedelta(seconds=30),
    )
    second = InMemoryAcceptedMaterializer(
        owner="worker-b",
        state=state,
        clock=clock,
        lease_duration=timedelta(seconds=30),
    )

    sandbox, lease, evidence = await first.acquire_and_materialize(request)
    replay_sandbox, replay_lease, replay_evidence = await first.acquire_and_materialize(
        request,
    )
    assert replay_sandbox is sandbox
    assert replay_lease is lease
    assert replay_evidence is evidence
    assert await first.validate(lease, evidence)

    conflicting = AcceptedMaterialRequestV1.build(
        run_id=request.run_id,
        attempt_id=request.attempt_id,
        tenant=request.tenant,
        user_ref=request.user_ref,
        thread_ref=request.thread_ref,
        agent_revision_digest=request.agent_revision_digest,
        skill_snapshot_digest=request.skill_snapshot_digest,
        skill_scope_digest="9" * 64,
        file_manifest=request.file_manifest,
        runtime_image_digest=request.runtime_image_digest,
        lease_expires_at=request.lease_expires_at,
    )
    with pytest.raises(
        AcceptedMaterialError,
        match="accepted_material_request_conflict",
    ):
        await first.acquire_and_materialize(conflicting)

    with pytest.raises(AcceptedMaterialError, match="accepted_material_claim_conflict"):
        await second.acquire_and_materialize(request)

    now += timedelta(seconds=31)
    recovered_sandbox, recovered_lease, recovered_evidence = await second.acquire_and_materialize(request)
    assert recovered_sandbox is sandbox
    assert recovered_lease.ownership_epoch == lease.ownership_epoch + 1
    assert recovered_evidence.ownership_epoch == recovered_lease.ownership_epoch
    assert not await first.validate(lease, evidence)
    with pytest.raises(AcceptedMaterialError, match="accepted_material_lease_lost"):
        await first.renew(lease)

    await first.release(lease)
    assert await second.validate(recovered_lease, recovered_evidence)
    await second.release(recovered_lease)
    assert not await second.validate(recovered_lease, recovered_evidence)
