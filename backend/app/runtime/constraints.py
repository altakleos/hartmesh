"""Application adapter for restrictive invocation constraints."""

from __future__ import annotations

import asyncio
from collections.abc import Collection
from typing import Any, Protocol

from deerflow_extension_api import (
    INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2,
    ConstraintIndeterminate,
    ConstraintProjectionRequestV1,
    ConstraintProjectionRequestV2,
    ConstraintProjectionV1,
    ConstraintProjectionV2,
    ConstraintRejected,
    SafeContextReferenceV1,
    SealedOriginV1,
)

from app.runtime.invocation import (
    InternalConstraintDecision,
    PreparedLaunch,
)
from deerflow.runtime.accepted_invocation import AcceptedInvocation


class ConstraintsHost(Protocol):
    capability_api_version: str | None
    required_capability_id: str | None

    async def project(
        self,
        request: ConstraintProjectionRequestV1 | ConstraintProjectionRequestV2,
        *,
        host_max_total_subagents: int | None = None,
        runtime_enforceable: bool = True,
    ) -> ConstraintProjectionV1 | ConstraintProjectionV2 | ConstraintRejected | ConstraintIndeterminate | None: ...


class ConstraintsHealth(Protocol):
    async def health_for(
        self,
        capability_ids: Collection[str],
        *,
        refresh: bool = True,
    ) -> Collection[Any]: ...


def _host_subagent_ceiling(accepted: AcceptedInvocation) -> int | None:
    material = accepted.agent_revision.material
    if material is not None:
        if material.runtime_defaults.get("subagent_enabled") is False:
            return 0
        value = material.runtime_defaults.get("max_total_subagents")
        if type(value) is int and value > 0:
            return value
    value = accepted.context_references.get("max_total_subagents")
    return value if type(value) is int and value > 0 else None


class ProviderInvocationConstraints:
    """Bind one Capability Host projection to normalized admission facts."""

    def __init__(
        self,
        host: ConstraintsHost | None,
        health: ConstraintsHealth | None = None,
    ) -> None:
        self._host = host
        self._health = health

    async def _required_capability_is_healthy(
        self,
        host: ConstraintsHost,
        *,
        expected_generation: int,
    ) -> bool:
        required_id = getattr(host, "required_capability_id", None)
        if required_id is None:
            return True
        health = self._health
        if health is None:
            return False
        try:
            snapshots = tuple(
                await health.health_for(
                    {required_id},
                    refresh=True,
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return False
        return len(snapshots) == 1 and getattr(snapshots[0], "capability_id", None) == required_id and getattr(snapshots[0], "status", None) == "healthy" and getattr(snapshots[0], "extension_generation", None) == expected_generation

    async def project(self, launch: PreparedLaunch) -> InternalConstraintDecision:
        host = self._host
        if host is None:
            return InternalConstraintDecision.absent()
        accepted = launch.accepted_invocation
        if not isinstance(accepted, AcceptedInvocation):
            return InternalConstraintDecision.indeterminate()
        if not await self._required_capability_is_healthy(
            host,
            expected_generation=accepted.extension_generation,
        ):
            return InternalConstraintDecision.indeterminate()
        request_digest = launch.request_digest
        if not isinstance(request_digest, str):
            return InternalConstraintDecision.indeterminate()
        try:
            if getattr(host, "capability_api_version", None) == INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2:
                trusted_context = accepted.trusted_context
                manifest_digest = accepted.extension_manifest_digest
                host_ceiling = _host_subagent_ceiling(accepted)
                if trusted_context is None or manifest_digest is None or host_ceiling is None:
                    return InternalConstraintDecision.indeterminate()
                request = ConstraintProjectionRequestV2(
                    identity=trusted_context.identity,
                    origin=trusted_context.origin,
                    policy_lookup_references=tuple(reference for reference in trusted_context.persistable_references if reference.reference.purpose == "correlation"),
                    thread_id=trusted_context.thread_id,
                    external_key_reference=trusted_context.external_key_reference,
                    agent_revision=trusted_context.agent_revision,
                    profile_revision=trusted_context.profile_revision,
                    request_digest=request_digest,
                    trusted_context_digest=trusted_context.digest,
                    extension_manifest_digest=manifest_digest,
                    extension_generation=accepted.extension_generation,
                    host_max_total_subagents=host_ceiling,
                )
                result = await host.project(request, runtime_enforceable=True)
            else:
                result = await host.project(
                    ConstraintProjectionRequestV1(
                        request_digest=request_digest,
                        agent_revision_digest=accepted.agent_revision.digest,
                        identity=accepted.principal.identity,
                        origin=(
                            accepted.trusted_context.origin
                            if accepted.trusted_context is not None
                            else SealedOriginV1(
                                source_kind=accepted.origin.source_kind,
                                references=tuple(
                                    SafeContextReferenceV1(
                                        key=key,
                                        value=value,
                                        storage_class="persistable",
                                        purpose="correlation",
                                    )
                                    for key, value in sorted(accepted.origin.references.items())
                                ),
                                digest=accepted.base_origin_digest,
                            )
                        ),
                        trusted_context=accepted.trusted_context,
                    ),
                    host_max_total_subagents=_host_subagent_ceiling(accepted),
                    runtime_enforceable=True,
                )
        except Exception:
            return InternalConstraintDecision.indeterminate()
        if result is None:
            return InternalConstraintDecision.absent()
        if type(result) is ConstraintRejected:
            return InternalConstraintDecision.denied()
        if type(result) is ConstraintIndeterminate:
            return InternalConstraintDecision.indeterminate()
        if type(result) in (ConstraintProjectionV1, ConstraintProjectionV2):
            return InternalConstraintDecision.projected(result)
        return InternalConstraintDecision.indeterminate()


__all__ = ["ProviderInvocationConstraints"]
