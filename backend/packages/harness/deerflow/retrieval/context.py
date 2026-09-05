"""Host-owned per-call handoff from retrieval service to receipt middleware."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

from deerflow.retrieval.contracts import (
    AcceptedRetrievalRequest,
    ResolvedRetrievalCredentialV1,
    RetrievalEvidenceError,
    RetrievalObservationDraftV1,
    RetrievalObservationV1,
    RetrievalPolicyDenied,
    RetrievalPolicyV1,
    RetrievalRequestConstraintsV1,
    _bounded_reference,
    _domain_digest,
    _identifier,
)

RETRIEVAL_TOOL_METADATA_KEY = "deerflow_retrieval_v1"


@dataclass(frozen=True, slots=True)
class RetrievalToolDeclarationV1:
    provider_id: str
    tool_kind: str
    adapter_capability_version: str
    protected_argument_fields: tuple[str, ...] = ("query",)
    mcp_evidence_ref: str | None = None
    version: int = 1

    def __post_init__(self) -> None:
        if self.version != 1:
            raise RetrievalEvidenceError("retrieval_tool_declaration_invalid")
        object.__setattr__(self, "provider_id", _identifier(self.provider_id, field_name="provider_id"))
        object.__setattr__(self, "tool_kind", _identifier(self.tool_kind, field_name="tool_kind"))
        _bounded_reference(self.adapter_capability_version, field_name="adapter_capability_version", max_bytes=64)
        fields = tuple(sorted({_identifier(item, field_name="protected_argument_field") for item in self.protected_argument_fields}))
        if not fields or len(fields) > 8:
            raise RetrievalEvidenceError("retrieval_protected_arguments_invalid")
        object.__setattr__(self, "protected_argument_fields", fields)
        if self.mcp_evidence_ref is not None:
            _bounded_reference(self.mcp_evidence_ref, field_name="mcp_evidence_ref", max_bytes=256)

    def to_metadata(self) -> dict[str, object]:
        return {
            "version": self.version,
            "provider_id": self.provider_id,
            "tool_kind": self.tool_kind,
            "adapter_capability_version": self.adapter_capability_version,
            "protected_argument_fields": list(self.protected_argument_fields),
            **({"mcp_evidence_ref": self.mcp_evidence_ref} if self.mcp_evidence_ref is not None else {}),
        }


def retrieval_tool_declaration(tool: object) -> RetrievalToolDeclarationV1 | None:
    """Read an explicit host tool declaration; never infer from tool names."""

    metadata = getattr(tool, "metadata", None)
    if not isinstance(metadata, Mapping) or RETRIEVAL_TOOL_METADATA_KEY not in metadata:
        return None
    value = metadata[RETRIEVAL_TOOL_METADATA_KEY]
    required = {
        "version",
        "provider_id",
        "tool_kind",
        "adapter_capability_version",
        "protected_argument_fields",
    }
    if not isinstance(value, Mapping) or not required.issubset(value) or set(value) - (required | {"mcp_evidence_ref"}) or not isinstance(value.get("protected_argument_fields"), (list, tuple)):
        raise RetrievalEvidenceError("retrieval_tool_declaration_invalid")
    return RetrievalToolDeclarationV1(
        version=value["version"],  # type: ignore[arg-type]
        provider_id=value["provider_id"],  # type: ignore[arg-type]
        tool_kind=value["tool_kind"],  # type: ignore[arg-type]
        adapter_capability_version=value["adapter_capability_version"],  # type: ignore[arg-type]
        protected_argument_fields=tuple(value["protected_argument_fields"]),  # type: ignore[arg-type]
        mcp_evidence_ref=value.get("mcp_evidence_ref"),  # type: ignore[arg-type]
    )


def protect_retrieval_request_projection(
    projection: Mapping[str, object],
    declaration: RetrievalToolDeclarationV1,
) -> dict[str, object]:
    """Replace protected argument shapes with a query-independent marker."""

    arguments = projection.get("arguments")
    if not isinstance(arguments, Mapping):
        raise RetrievalEvidenceError("retrieval_request_projection_invalid")
    missing = set(declaration.protected_argument_fields) - set(arguments)
    if missing:
        raise RetrievalEvidenceError("retrieval_protected_argument_missing")
    protected = {str(name): ({"classification": "protected", "type": "redacted"} if name in declaration.protected_argument_fields else value) for name, value in arguments.items()}
    return {
        **dict(projection),
        "arguments": protected,
    }


@dataclass(slots=True)
class RetrievalDraftHandoffV1:
    receipt: object
    declaration: RetrievalToolDeclarationV1
    runtime_context: Mapping[str, object]
    draft: RetrievalObservationDraftV1 | None = None
    active: bool = True

    def publish(self, draft: RetrievalObservationDraftV1) -> None:
        from deerflow.runtime.tool_evidence import DurableToolReceiptV1

        if not self.active or not isinstance(self.receipt, DurableToolReceiptV1):
            raise RetrievalEvidenceError("retrieval_draft_context_inactive")
        if self.draft is not None:
            raise RetrievalEvidenceError("retrieval_draft_duplicate")
        if (
            not isinstance(draft, RetrievalObservationDraftV1)
            or draft.receipt_id != self.receipt.receipt_id
            or draft.run_id != self.receipt.context.run_id
            or draft.attempt != self.receipt.context.attempt
            or draft.provider_id != self.declaration.provider_id
            or draft.tool_kind != self.declaration.tool_kind
            or draft.adapter_capability_version != self.declaration.adapter_capability_version
        ):
            raise RetrievalEvidenceError("retrieval_draft_mismatch")
        self.draft = draft

    def make_terminal_draft(
        self,
        *,
        provider_status: str,
        safe_reason: str,
        policy_digest: str | None = None,
        safe_constraints: Mapping[str, object] | None = None,
        provider_finished_at: datetime | None = None,
    ) -> RetrievalObservationDraftV1:
        """Build bounded evidence when an inner layer never reached a provider.

        The fallback commits only a static policy-state marker. It deliberately
        has no access to tool arguments, so it cannot accidentally derive a
        query identifier while covering authorization, configuration, and
        cancellation terminals.
        """

        from deerflow_extension_api import TenantReferenceV1

        from deerflow.runtime.tool_evidence import DurableToolReceiptV1
        from deerflow.sandbox.accepted_material import (
            current_accepted_sandbox_bridge,
        )

        # Middleware deliberately finalizes after leaving the context manager,
        # when publication is closed but its retained host-owned handoff is
        # still authoritative for a synthetic terminal draft.
        if not isinstance(self.receipt, DurableToolReceiptV1):
            raise RetrievalEvidenceError("retrieval_draft_context_inactive")
        tenant = self.receipt.context.tenant
        if not isinstance(tenant, TenantReferenceV1):
            raise RetrievalEvidenceError("retrieval_tenant_context_unavailable")
        tool_plane = self.runtime_context.get("accepted_tool_plane_revision")
        required_digests = (
            "base_revision_digest",
            "user_overlay_digest",
            "projection_digest",
            "effective_digest",
        )
        if not isinstance(tool_plane, Mapping) or any(not isinstance(tool_plane.get(name), str) for name in required_digests):
            raise RetrievalEvidenceError("retrieval_tool_plane_context_unavailable")
        marker_digest = policy_digest or _domain_digest(
            "retrieval-policy-state",
            {
                "provider_id": self.declaration.provider_id,
                "state": "not_evaluated",
            },
        )
        constraints = safe_constraints or {
            "version": 1,
            "provider_id": self.declaration.provider_id,
            "policy_status": "not_evaluated",
        }
        sandbox_bridge = current_accepted_sandbox_bridge()
        finished_at = provider_finished_at or datetime.now(UTC)
        if finished_at < self.receipt.occurred_at:
            finished_at = self.receipt.occurred_at
        return RetrievalObservationDraftV1(
            tenant_ref=tenant.public_ref,
            tenant_digest=tenant.digest,
            run_id=self.receipt.context.run_id,
            receipt_id=self.receipt.receipt_id,
            attempt=self.receipt.context.attempt,
            provider_id=self.declaration.provider_id,
            tool_kind=self.declaration.tool_kind,
            adapter_capability_version=self.declaration.adapter_capability_version,
            policy_digest=marker_digest,
            safe_constraints=constraints,
            started_at=self.receipt.occurred_at,
            provider_finished_at=finished_at,
            provider_status=provider_status,  # type: ignore[arg-type]
            safe_reason=safe_reason,
            result_count=0,
            source_count=0,
            source_references=(),
            truncated=False,
            partial=False,
            safe_provider_request_ref=None,
            tool_plane_base_revision_digest=tool_plane["base_revision_digest"],  # type: ignore[arg-type]
            tool_plane_user_overlay_digest=tool_plane["user_overlay_digest"],  # type: ignore[arg-type]
            tool_plane_projection_digest=tool_plane["projection_digest"],  # type: ignore[arg-type]
            tool_plane_effective_digest=tool_plane["effective_digest"],  # type: ignore[arg-type]
            accepted_execution_evidence_ref=(sandbox_bridge.execution_evidence_reference if sandbox_bridge is not None else None),
            accepted_sandbox_operation_ref=None,
            mcp_evidence_ref=self.declaration.mcp_evidence_ref,
        )


_ACTIVE_RETRIEVAL_HANDOFF: ContextVar[RetrievalDraftHandoffV1 | None] = ContextVar(
    "deerflow_active_retrieval_handoff",
    default=None,
)


@contextmanager
def active_retrieval_draft_context(
    receipt: object,
    declaration: RetrievalToolDeclarationV1,
    runtime_context: Mapping[str, object],
) -> Iterator[RetrievalDraftHandoffV1]:
    handoff = RetrievalDraftHandoffV1(
        receipt=receipt,
        declaration=declaration,
        runtime_context=runtime_context,
    )
    token = _ACTIVE_RETRIEVAL_HANDOFF.set(handoff)
    try:
        yield handoff
    finally:
        handoff.active = False
        _ACTIVE_RETRIEVAL_HANDOFF.reset(token)


def get_active_retrieval_handoff() -> RetrievalDraftHandoffV1 | None:
    handoff = _ACTIVE_RETRIEVAL_HANDOFF.get()
    return handoff if handoff is not None and handoff.active else None


def accepted_retrieval_app_config_from_active() -> object:
    """Return the immutable application config captured at run admission."""

    from deerflow.runtime.accepted_invocation import ResolvedAgentMaterialV1
    from deerflow.runtime.agent_revision import (
        RESOLVED_AGENT_MATERIAL_CONTEXT_KEY,
    )

    handoff = get_active_retrieval_handoff()
    if handoff is None:
        raise RetrievalEvidenceError("retrieval_active_context_required")
    material = handoff.runtime_context.get(
        RESOLVED_AGENT_MATERIAL_CONTEXT_KEY,
    )
    app_config = getattr(material, "app_config", None)
    if not isinstance(material, ResolvedAgentMaterialV1) or app_config is None:
        raise RetrievalEvidenceError("retrieval_accepted_config_unavailable")
    return app_config


def publish_retrieval_observation_draft(draft: RetrievalObservationDraftV1) -> None:
    handoff = get_active_retrieval_handoff()
    if handoff is None:
        raise RetrievalEvidenceError("retrieval_draft_context_unavailable")
    handoff.publish(draft)


def publish_retrieval_observation_draft_if_active(
    draft: RetrievalObservationDraftV1,
) -> bool:
    handoff = get_active_retrieval_handoff()
    if handoff is None:
        return False
    handoff.publish(draft)
    return True


def accepted_retrieval_request_from_active(
    *,
    query: str,
    credential: ResolvedRetrievalCredentialV1,
    policy: RetrievalPolicyV1,
    requested_constraints: RetrievalRequestConstraintsV1,
    accepted_sandbox_operation_ref: str | None = None,
) -> AcceptedRetrievalRequest:
    """Build a request exclusively from the active host-sealed run context."""

    from deerflow_extension_api import TrustedRunContextV1

    from deerflow.runtime.accepted_invocation import TRUSTED_RUN_CONTEXT_KEY
    from deerflow.runtime.tool_evidence import DurableToolReceiptV1
    from deerflow.sandbox.accepted_material import (
        current_accepted_sandbox_bridge,
    )

    handoff = get_active_retrieval_handoff()
    if handoff is None or not isinstance(handoff.receipt, DurableToolReceiptV1):
        raise RetrievalEvidenceError("retrieval_active_context_required")
    declaration = handoff.declaration
    if credential.provider_id != declaration.provider_id or requested_constraints.provider_id != declaration.provider_id:
        raise RetrievalEvidenceError("retrieval_provider_context_mismatch")
    trusted = handoff.runtime_context.get(TRUSTED_RUN_CONTEXT_KEY)
    if not isinstance(trusted, TrustedRunContextV1):
        raise RetrievalEvidenceError("retrieval_trusted_context_unavailable")
    verified_actor = trusted.verified_actor
    if verified_actor is None or trusted.tenant is None:
        raise RetrievalEvidenceError("retrieval_actor_context_unavailable")
    if trusted.run_id != handoff.receipt.context.run_id:
        raise RetrievalEvidenceError("retrieval_run_context_mismatch")
    tool_plane = handoff.runtime_context.get("accepted_tool_plane_revision")
    required_digests = {
        "base_revision_digest",
        "user_overlay_digest",
        "projection_digest",
        "effective_digest",
    }
    if not isinstance(tool_plane, Mapping) or any(not isinstance(tool_plane.get(name), str) for name in required_digests):
        raise RetrievalEvidenceError("retrieval_tool_plane_context_unavailable")
    sandbox_bridge = current_accepted_sandbox_bridge()
    try:
        return AcceptedRetrievalRequest(
            thread_id=trusted.thread_id,
            tenant=trusted.tenant,
            receipt=handoff.receipt,
            actor_ref=verified_actor.digest,
            provider_id=declaration.provider_id,
            tool_kind=declaration.tool_kind,
            adapter_capability_version=declaration.adapter_capability_version,
            query=query,
            credential=credential,
            policy=policy,
            requested_constraints=requested_constraints,
            tool_plane_base_revision_digest=tool_plane["base_revision_digest"],  # type: ignore[arg-type]
            tool_plane_user_overlay_digest=tool_plane["user_overlay_digest"],  # type: ignore[arg-type]
            tool_plane_projection_digest=tool_plane["projection_digest"],  # type: ignore[arg-type]
            tool_plane_effective_digest=tool_plane["effective_digest"],  # type: ignore[arg-type]
            accepted_execution_evidence_ref=(sandbox_bridge.execution_evidence_reference if sandbox_bridge is not None else None),
            accepted_sandbox_operation_ref=accepted_sandbox_operation_ref,
            mcp_evidence_ref=declaration.mcp_evidence_ref,
        )
    except RetrievalPolicyDenied:
        handoff.publish(
            handoff.make_terminal_draft(
                provider_status="policy_denied",
                safe_reason="policy_denied",
                policy_digest=policy.digest,
                safe_constraints={
                    "version": 1,
                    "provider_id": declaration.provider_id,
                    "policy_status": "denied",
                },
            )
        )
        raise


@runtime_checkable
class RetrievalObservationFinalizer(Protocol):
    async def record_with_receipt_outcome(
        self,
        receipt: object,
        draft: RetrievalObservationDraftV1,
    ) -> RetrievalObservationV1: ...
