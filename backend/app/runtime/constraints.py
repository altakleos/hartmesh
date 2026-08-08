"""Application adapter for restrictive invocation constraints."""

from __future__ import annotations

from typing import Protocol

from deerflow_extension_api import (
    ConstraintIndeterminate,
    ConstraintProjectionRequestV1,
    ConstraintProjectionV1,
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
    async def project(
        self,
        request: ConstraintProjectionRequestV1,
        *,
        host_max_total_subagents: int | None,
        runtime_enforceable: bool = True,
    ) -> ConstraintProjectionV1 | ConstraintRejected | ConstraintIndeterminate | None: ...


def _host_subagent_ceiling(accepted: AcceptedInvocation) -> int | None:
    material = accepted.agent_revision.material
    if material is not None:
        value = material.runtime_defaults.get("max_total_subagents")
        if type(value) is int and value > 0:
            return value
    value = accepted.context_references.get("max_total_subagents")
    return value if type(value) is int and value > 0 else None


class ProviderInvocationConstraints:
    """Bind one Capability Host projection to normalized admission facts."""

    def __init__(self, host: ConstraintsHost | None) -> None:
        self._host = host

    async def project(self, launch: PreparedLaunch) -> InternalConstraintDecision:
        host = self._host
        if host is None:
            return InternalConstraintDecision.absent()
        accepted = launch.accepted_invocation
        if not isinstance(accepted, AcceptedInvocation):
            return InternalConstraintDecision.indeterminate()
        request_digest = launch.request_digest
        if not isinstance(request_digest, str):
            return InternalConstraintDecision.indeterminate()
        try:
            result = await host.project(
                ConstraintProjectionRequestV1(
                    request_digest=request_digest,
                    agent_revision_digest=accepted.agent_revision.digest,
                    identity=accepted.principal.identity,
                    origin=SealedOriginV1(
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
                    ),
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
        if type(result) is ConstraintProjectionV1:
            return InternalConstraintDecision.projected(result)
        return InternalConstraintDecision.indeterminate()


__all__ = ["ProviderInvocationConstraints"]
