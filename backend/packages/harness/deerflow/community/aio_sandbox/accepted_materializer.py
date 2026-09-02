"""AIO adapter for the provider-neutral accepted-material contract."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

from deerflow.qualification_evidence import ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2
from deerflow.sandbox.accepted_material import (
    AcceptedExecutionEvidenceV1,
    AcceptedMaterialCapability,
    AcceptedMaterialError,
    AcceptedMaterialExecutionClaimV1,
    AcceptedMaterialLeaseV1,
    AcceptedMaterialRequestV1,
    AcceptedSkillExecutionEvidenceV2,
    AcceptedSkillSandboxBindingV1,
)
from deerflow.sandbox.sandbox import Sandbox

if TYPE_CHECKING:
    from deerflow.community.aio_sandbox.aio_sandbox_provider import (
        AioSandboxProvider,
    )


def _canonical_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"),
    ).hexdigest()


@dataclass(slots=True)
class _AioRenewalHandle:
    request: AcceptedMaterialRequestV1
    legacy_evidence: AcceptedSkillExecutionEvidenceV2
    neutral_evidence: AcceptedExecutionEvidenceV1
    current_lease: AcceptedMaterialLeaseV1 | None = None
    active: bool = True


@dataclass(slots=True)
class _AioMaterialization:
    request: AcceptedMaterialRequestV1
    sandbox: Sandbox
    lease: AcceptedMaterialLeaseV1
    evidence: AcceptedExecutionEvidenceV1


class AioAcceptedMaterializer:
    """Translate the qualified AIO v2 tuple without weakening its checks."""

    def __init__(
        self,
        *,
        provider: AioSandboxProvider,
        binding_resolver: Callable[
            [AcceptedMaterialRequestV1],
            AcceptedSkillSandboxBindingV1,
        ],
        scope_resolver: Callable[
            [AcceptedMaterialRequestV1],
            tuple[str, str],
        ]
        | None = None,
        clock: Callable[[], datetime] | None = None,
        lease_duration: timedelta = timedelta(minutes=5),
    ) -> None:
        if not callable(binding_resolver):
            raise TypeError("binding_resolver must be callable")
        if not isinstance(lease_duration, timedelta) or lease_duration <= timedelta(0) or lease_duration > timedelta(hours=1):
            raise ValueError("lease_duration must be between zero and one hour")
        self._provider = provider
        self._binding_resolver = binding_resolver
        self._scope_resolver = scope_resolver or (lambda request: (request.thread_ref, request.user_ref))
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lease_duration = lease_duration
        self._locks: dict[tuple[str, str, str], asyncio.Lock] = {}
        self._active: dict[tuple[str, str, str], _AioMaterialization] = {}

    def capability(self) -> AcceptedMaterialCapability:
        return AcceptedMaterialCapability.IMMUTABLE_READ_ONLY

    @staticmethod
    def _key(
        request: AcceptedMaterialRequestV1,
    ) -> tuple[str, str, str]:
        return request.tenant.digest, request.run_id, request.attempt_id

    def _lock_for(self, key: tuple[str, str, str]) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    @staticmethod
    def _neutral_evidence(
        request: AcceptedMaterialRequestV1,
        *,
        sandbox_id: str,
        legacy: AcceptedSkillExecutionEvidenceV2,
        stable_execution_claim: bool = False,
    ) -> AcceptedExecutionEvidenceV1:
        if legacy.profile != "rwx_verified_copy_v2" or legacy.snapshot_id != request.skill_snapshot_digest or legacy.run_id != request.run_id:
            raise AcceptedMaterialError("accepted_material_evidence_mismatch")
        if legacy.sandbox_image_digest != request.runtime_image_digest:
            raise AcceptedMaterialError("accepted_material_image_digest_mismatch")
        if stable_execution_claim:
            request_commitment = {
                "accepted_material_identity_digest": _canonical_digest(
                    {key: value for key, value in request.to_persisted().items() if key not in {"digest", "lease_expires_at"}},
                ),
            }
            verifier_contract_version = f"{legacy.profile}:accepted_execution_claim_v1"
        else:
            # Preserve the original persisted V1 proof contract exactly.
            request_commitment = {
                "accepted_material_request_digest": request.digest,
            }
            verifier_contract_version = legacy.profile
        read_only_proof_digest = _canonical_digest(
            {
                "version": 1,
                **request_commitment,
                "profile": legacy.profile,
                "provider_attempt_id": legacy.attempt_id,
                "pod_isolation_digest": legacy.pod_isolation_digest,
                "network_policy_spec_digest": legacy.network_policy_spec_digest,
                "capability_secret_digest": legacy.capability_secret_digest,
                "verifier_receipt_digest": legacy.verifier_receipt_digest,
                "materialization_evidence_digest": (legacy.materialization_evidence_digest),
            },
        )
        return AcceptedExecutionEvidenceV1.build(
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            tenant=request.tenant,
            provider_kind="aio_kubernetes",
            provider_instance_ref=sandbox_id,
            ownership_epoch=legacy.generation,
            runtime_image_digest=legacy.sandbox_image_digest,
            skill_snapshot_digest=legacy.snapshot_id,
            skill_scope_digest=request.skill_scope_digest,
            materialization_digest=legacy.materialization_evidence_digest,
            verifier_image_digest=legacy.accepted_skill_runtime_image_digest,
            verifier_contract_version=verifier_contract_version,
            read_only_proof_digest=read_only_proof_digest,
            qualification_scope=ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2,
        )

    async def acquire_and_materialize(
        self,
        request: AcceptedMaterialRequestV1,
        *,
        execution_claim: AcceptedMaterialExecutionClaimV1 | None = None,
    ) -> tuple[Sandbox, AcceptedMaterialLeaseV1, AcceptedExecutionEvidenceV1]:
        if not isinstance(request, AcceptedMaterialRequestV1):
            raise TypeError("request must be AcceptedMaterialRequestV1")
        if request.lease_expires_at <= self._clock():
            raise AcceptedMaterialError("accepted_material_lease_expired")
        if execution_claim is not None and (not isinstance(execution_claim, AcceptedMaterialExecutionClaimV1) or not execution_claim.binds(request)):
            raise AcceptedMaterialError("accepted_material_execution_claim_mismatch")
        if execution_claim is not None and execution_claim.execution_takeover:
            # A Kubernetes Secret update is visible to the API server before
            # kubelet projects it into the gate sidecar.  Until AIO has a
            # per-request linearizable owner/epoch authority, accepting a
            # takeover would leave the prior credential usable and would let
            # stale cleanup destroy the unchanged Pod.  Keep the provider
            # seam explicit and fail before any remote or local mutation.
            raise AcceptedMaterialError(
                "accepted_material_execution_takeover_unavailable",
            )
        key = self._key(request)
        async with self._lock_for(key):
            current = self._active.get(key)
            if current is not None:
                if current.request.digest != request.digest:
                    raise AcceptedMaterialError("accepted_material_request_conflict")
                if await self.validate(current.lease, current.evidence):
                    return current.sandbox, current.lease, current.evidence
                self._active.pop(key, None)

            binding = self._binding_resolver(request)
            if not isinstance(binding, AcceptedSkillSandboxBindingV1):
                raise TypeError(
                    "binding_resolver must return AcceptedSkillSandboxBindingV1",
                )
            if binding.snapshot_id != request.skill_snapshot_digest or binding.run_id != request.run_id:
                raise AcceptedMaterialError("accepted_material_binding_mismatch")
            thread_id, user_id = self._scope_resolver(request)
            if not isinstance(thread_id, str) or not thread_id or not isinstance(user_id, str) or not user_id:
                raise AcceptedMaterialError("accepted_material_scope_unavailable")
            if execution_claim is not None and execution_claim.execution_takeover:
                recover = getattr(
                    self._provider,
                    "recover_bound_accepted_skills_async",
                    None,
                )
                if not callable(recover):
                    raise AcceptedMaterialError(
                        "accepted_material_execution_takeover_unsupported",
                    )
                sandbox_id = await recover(
                    thread_id,
                    user_id=user_id,
                    binding=binding,
                    execution_claim=execution_claim,
                )
            elif execution_claim is None:
                sandbox_id = await self._provider.acquire_bound_accepted_skills_async(
                    thread_id,
                    user_id=user_id,
                    binding=binding,
                )
            else:
                sandbox_id = await self._provider.acquire_bound_accepted_skills_async(
                    thread_id,
                    user_id=user_id,
                    binding=binding,
                    execution_claim=execution_claim,
                )
            try:
                sandbox = self._provider.get(sandbox_id)
                legacy = self._provider.accepted_skill_execution_evidence(sandbox_id)
                if sandbox is None or not isinstance(
                    legacy,
                    AcceptedSkillExecutionEvidenceV2,
                ):
                    raise AcceptedMaterialError(
                        "accepted_material_evidence_unavailable",
                    )
                evidence = self._neutral_evidence(
                    request,
                    sandbox_id=sandbox_id,
                    legacy=legacy,
                    stable_execution_claim=execution_claim is not None,
                )
            except Exception:
                try:
                    await asyncio.to_thread(self._provider.destroy, sandbox_id)
                except Exception:
                    raise AcceptedMaterialError(
                        "accepted_material_cleanup_failed",
                    ) from None
                raise
            handle = _AioRenewalHandle(
                request=request,
                legacy_evidence=legacy,
                neutral_evidence=evidence,
            )
            lease = AcceptedMaterialLeaseV1(
                version=1,
                provider_kind=evidence.provider_kind,
                provider_instance_ref=sandbox_id,
                ownership_epoch=evidence.ownership_epoch,
                lease_expires_at=request.lease_expires_at,
                opaque_renewal_handle=handle,
            )
            handle.current_lease = lease
            materialization = _AioMaterialization(
                request=request,
                sandbox=cast(Sandbox, sandbox),
                lease=lease,
                evidence=evidence,
            )
            self._active[key] = materialization
            return materialization.sandbox, lease, evidence

    @staticmethod
    def _handle(
        lease: AcceptedMaterialLeaseV1,
    ) -> _AioRenewalHandle | None:
        handle = lease.opaque_renewal_handle
        if not isinstance(handle, _AioRenewalHandle):
            return None
        if not handle.active or handle.current_lease is not lease:
            return None
        return handle

    async def validate(
        self,
        lease: AcceptedMaterialLeaseV1,
        evidence: AcceptedExecutionEvidenceV1,
    ) -> bool:
        if not isinstance(lease, AcceptedMaterialLeaseV1) or not isinstance(
            evidence,
            AcceptedExecutionEvidenceV1,
        ):
            return False
        handle = self._handle(lease)
        if handle is None or handle.neutral_evidence != evidence or not evidence.binds(handle.request, lease) or lease.lease_expires_at <= self._clock():
            return False
        return await self._provider.validate_accepted_skill_execution_async(
            lease.provider_instance_ref,
            handle.legacy_evidence,
        )

    async def renew(
        self,
        lease: AcceptedMaterialLeaseV1,
    ) -> AcceptedMaterialLeaseV1:
        handle = self._handle(lease)
        if handle is None or not await self.validate(
            lease,
            handle.neutral_evidence,
        ):
            raise AcceptedMaterialError("accepted_material_lease_lost")
        if not await self._provider.renew_accepted_skill_execution_async(
            lease.provider_instance_ref,
            handle.legacy_evidence,
        ):
            raise AcceptedMaterialError("accepted_material_lease_lost")
        renewed = AcceptedMaterialLeaseV1(
            version=1,
            provider_kind=lease.provider_kind,
            provider_instance_ref=lease.provider_instance_ref,
            ownership_epoch=lease.ownership_epoch,
            lease_expires_at=self._clock() + self._lease_duration,
            opaque_renewal_handle=handle,
        )
        handle.current_lease = renewed
        key = self._key(handle.request)
        current = self._active.get(key)
        if current is not None and current.lease is lease:
            current.lease = renewed
        return renewed

    async def release(self, lease: AcceptedMaterialLeaseV1) -> None:
        if not isinstance(lease, AcceptedMaterialLeaseV1):
            raise TypeError("lease must be AcceptedMaterialLeaseV1")
        handle = self._handle(lease)
        if handle is None:
            return
        key = self._key(handle.request)
        try:
            current = self._provider.accepted_skill_execution_evidence(
                lease.provider_instance_ref,
            )
            if current == handle.legacy_evidence:
                await asyncio.to_thread(
                    self._provider.destroy,
                    lease.provider_instance_ref,
                )
        finally:
            handle.active = False
            materialization = self._active.get(key)
            if materialization is not None and materialization.lease is lease:
                self._active.pop(key, None)


__all__ = ["AioAcceptedMaterializer"]
