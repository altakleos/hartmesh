"""Gateway-owned authorization of durable invocation operations."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
from collections.abc import Callable
from typing import Any, Protocol

from deerflow_extension_api import (
    AuthorizationProvider,
    AuthzDecision,
    AuthzReason,
    AuthzRequest,
    Principal,
)

from app.runtime.invocation import (
    InternalAuthorizationDecision,
    InvocationPrincipal,
    PreparedLaunch,
)
from deerflow.config.authorization_config import AuthorizationConfig, InvocationOperationsAuthorizationConfig
from deerflow.runtime import RunRecord
from deerflow.runtime.accepted_invocation import AcceptedInvocation, PrincipalProjection, canonical_digest

logger = logging.getLogger(__name__)

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@-]*$")
_MAX_POLICY_ID_BYTES = 128
_MAX_REASON_CODES = 16
_MAX_REASON_CODE_BYTES = 64
_MAX_REASON_MESSAGE_BYTES = 1024
_MAX_METADATA_BYTES = 8192


class AuthorizationResolution(Protocol):
    generation: int
    provider: AuthorizationProvider | None


ResolutionSupplier = Callable[[], AuthorizationResolution]


def validate_invocation_authorization_startup(
    config: AuthorizationConfig,
    resolution: AuthorizationResolution,
) -> None:
    """Fail startup when an enabled operation has no coherent provider."""
    settings = config.invocation_operations
    if not (settings.start_enabled or settings.observe_enabled or settings.cancel_enabled):
        return
    if config.enabled is not True:
        raise ValueError("invocation operation authorization requires authorization.enabled=true")
    if resolution.provider is None:
        raise ValueError("invocation operation authorization requires one initialized authorization provider")


def _projection_principal(projection: PrincipalProjection) -> Principal:
    return Principal(
        user_id=projection.user_id,
        role=projection.role,
        oauth_provider=projection.oauth_provider,
        oauth_id=projection.oauth_id,
        channel_user_id=projection.channel_user_id,
        is_internal=projection.is_internal,
        identity=projection.identity,
    )


def _invocation_principal(principal: InvocationPrincipal) -> Principal:
    return Principal(
        user_id=principal.user_id,
        role=principal.role,
        oauth_provider=principal.oauth_provider,
        oauth_id=principal.oauth_id,
        channel_user_id=principal.channel_user_id,
        is_internal=principal.is_internal,
        identity=principal.identity,
    )


def _validate_identifier(value: str, *, field_name: str, max_bytes: int) -> str:
    if not isinstance(value, str) or len(value.encode("utf-8")) > max_bytes or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"authorization {field_name} is invalid")
    return value


def _validate_json_value(value: Any, *, depth: int = 0) -> Any:
    if depth > 8:
        raise ValueError("authorization metadata is too deeply nested")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("authorization metadata contains a non-finite number")
        return value
    if isinstance(value, list):
        return [_validate_json_value(item, depth=depth + 1) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise ValueError("authorization metadata keys must be strings")
        return {key: _validate_json_value(item, depth=depth + 1) for key, item in value.items()}
    raise ValueError("authorization metadata must be JSON-safe")


def _validate_decision(decision: Any) -> AuthzDecision:
    if not isinstance(decision, AuthzDecision):
        raise TypeError("AuthorizationProvider.aauthorize must return AuthzDecision")
    if type(decision.allow) is not bool:
        raise TypeError("authorization decision allow must be a boolean")
    if not isinstance(decision.reasons, list) or len(decision.reasons) > _MAX_REASON_CODES:
        raise ValueError("authorization decision has invalid reasons")
    for reason in decision.reasons:
        if not isinstance(reason, AuthzReason):
            raise TypeError("authorization decision reasons must be AuthzReason values")
        _validate_identifier(reason.code, field_name="reason code", max_bytes=_MAX_REASON_CODE_BYTES)
        if not isinstance(reason.message, str) or len(reason.message.encode("utf-8")) > _MAX_REASON_MESSAGE_BYTES:
            raise ValueError("authorization reason message is too large")
    if decision.policy_id is not None:
        _validate_identifier(decision.policy_id, field_name="policy ID", max_bytes=_MAX_POLICY_ID_BYTES)
    if not isinstance(decision.metadata, dict):
        raise TypeError("authorization decision metadata must be an object")
    safe_metadata = _validate_json_value(decision.metadata)
    encoded = json.dumps(
        safe_metadata,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > _MAX_METADATA_BYTES:
        raise ValueError("authorization decision metadata is too large")
    return decision


def _decision_evidence(decision: AuthzDecision, *, generation: int) -> dict[str, Any]:
    reason_codes = [reason.code for reason in decision.reasons]
    evidence_digest = canonical_digest(
        {
            "version": 1,
            "allow": decision.allow,
            "policy_id": decision.policy_id,
            "reasons": [{"code": reason.code, "message": reason.message} for reason in decision.reasons],
            "metadata": decision.metadata,
        }
    )
    return {
        "version": 1,
        "decisions": [
            {
                "authorization_generation": generation,
                "policy_id": decision.policy_id,
                "reason_codes": reason_codes,
                "evidence_digest": evidence_digest,
            }
        ],
    }


def _accepted_record_context(record: RunRecord) -> dict[str, Any]:
    accepted = record.accepted_invocation
    if not isinstance(accepted, AcceptedInvocation):
        return {
            "principal": None,
            "bound_context": {"thread_id": record.thread_id},
            "status": record.status.value,
            "state_version": record.state_version,
            "accepted_agent": None,
            "accepted_origin": None,
        }
    return {
        "principal": accepted.principal.to_json(),
        "bound_context": {
            "thread_id": accepted.thread_id,
            "accepted_context_digest": accepted.accepted_context_digest,
        },
        "status": record.status.value,
        "state_version": record.state_version,
        "accepted_agent": accepted.agent_revision.to_json(),
        "accepted_origin": accepted.origin.to_json(),
    }


class ProviderInvocationAuthorization:
    """Apply one resolver-owned provider to enabled invocation operations."""

    def __init__(
        self,
        settings: InvocationOperationsAuthorizationConfig,
        resolve: ResolutionSupplier,
    ) -> None:
        # These flags are an operator startup contract. Keep a private copy so
        # API-writable/live configuration cannot enable a new policy boundary.
        self._settings = settings.model_copy(deep=True)
        self._resolve = resolve

    async def _authorize(
        self,
        request: AuthzRequest,
        *,
        persist_evidence: bool = False,
    ) -> InternalAuthorizationDecision:
        try:
            snapshot = self._resolve()
            provider = snapshot.provider
            if provider is None:
                raise RuntimeError("authorization provider is unavailable")
            async with asyncio.timeout(self._settings.timeout_seconds):
                decision = _validate_decision(await provider.aauthorize(request))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "Invocation authorization was indeterminate for %s %s",
                request.action,
                request.target,
                exc_info=True,
            )
            return InternalAuthorizationDecision.indeterminate()

        if decision.allow is not True:
            return InternalAuthorizationDecision.denied()
        evidence = _decision_evidence(decision, generation=snapshot.generation) if persist_evidence else None
        return InternalAuthorizationDecision.allowed(evidence=evidence)

    async def authorize_start(
        self,
        launch: PreparedLaunch,
    ) -> InternalAuthorizationDecision:
        if self._settings.start_enabled is not True:
            return InternalAuthorizationDecision.allowed()
        accepted = launch.accepted_invocation
        if not isinstance(accepted, AcceptedInvocation):
            return InternalAuthorizationDecision.indeterminate()
        request = AuthzRequest(
            principal=_projection_principal(accepted.principal),
            resource="invocation",
            action="start",
            target=f"agent:{accepted.agent_revision.agent_id}@sha256:{accepted.agent_revision.digest}",
            context={
                "principal": accepted.principal.to_json(),
                "origin": accepted.origin.to_json(),
                "bound_context": {
                    "thread_id": accepted.thread_id,
                    "references": dict(accepted.context_references),
                },
                "external_key": {
                    "scope": launch.external_scope,
                    "key": launch.external_key,
                },
                "request_digest": launch.request_digest,
                "request_digest_version": launch.request_digest_version,
                "revision": accepted.agent_revision.to_json(),
                "safe_source_evidence": dict(accepted.origin.references),
            },
        )
        return await self._authorize(request, persist_evidence=True)

    async def authorize_observe(
        self,
        record: RunRecord,
        principal: InvocationPrincipal,
        *,
        target_kind: str = "run",
    ) -> InternalAuthorizationDecision:
        if self._settings.observe_enabled is not True:
            return InternalAuthorizationDecision.allowed()
        target = f"run:{record.run_id}" if target_kind == "run" else f"context:{record.thread_id}"
        return await self._authorize(
            AuthzRequest(
                principal=_invocation_principal(principal),
                resource="invocation",
                action="observe",
                target=target,
                context=_accepted_record_context(record),
            )
        )

    async def authorize_context_observe(
        self,
        thread_id: str,
        principal: InvocationPrincipal,
    ) -> InternalAuthorizationDecision:
        if self._settings.observe_enabled is not True:
            return InternalAuthorizationDecision.allowed()
        return await self._authorize(
            AuthzRequest(
                principal=_invocation_principal(principal),
                resource="invocation",
                action="observe",
                target=f"context:{thread_id}",
                context={
                    "principal": {
                        "version": 1,
                        "user_id": principal.user_id,
                        "role": principal.role,
                        "oauth_provider": principal.oauth_provider,
                        "oauth_id": principal.oauth_id,
                        "channel_user_id": principal.channel_user_id,
                        "is_internal": principal.is_internal,
                    },
                    "bound_context": {"thread_id": thread_id},
                },
            )
        )

    async def authorize_cancel(
        self,
        record: RunRecord,
        principal: InvocationPrincipal,
    ) -> InternalAuthorizationDecision:
        if self._settings.cancel_enabled is not True:
            return InternalAuthorizationDecision.allowed()
        return await self._authorize(
            AuthzRequest(
                principal=_invocation_principal(principal),
                resource="invocation",
                action="cancel",
                target=f"run:{record.run_id}",
                context=_accepted_record_context(record),
            )
        )


__all__ = [
    "ProviderInvocationAuthorization",
    "validate_invocation_authorization_startup",
]
