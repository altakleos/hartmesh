"""Capability Host execution for trusted invocation contributors."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Literal

from deerflow_extension_api import (
    OriginContributionRequestV1,
    OriginContributionV1,
    OriginContributor,
    RunContextContributionRequestV1,
    RunContextContributionV1,
    RunContextContributor,
    SafeContextReferenceV1,
)

from deerflow.extensions.registry import LoadedExtensions

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 2.0
_MAX_AGGREGATE_REFERENCES = 32
_MAX_AGGREGATE_CANONICAL_BYTES = 8192
_SUPPORTED_REQUIRED_KINDS = frozenset({"origin_contributor", "run_context_contributor"})
_PASSTHROUGH_REQUIRED_CAPABILITIES = frozenset({"invocation_constraints.v1"})


def _is_passthrough_required_capability(capability_id: str) -> bool:
    if capability_id in _PASSTHROUGH_REQUIRED_CAPABILITIES:
        return True
    kind, separator, contribution_id = capability_id.partition(":")
    return separator == ":" and kind == "mcp_interceptor" and bool(contribution_id)


class RequiredCapabilityError(RuntimeError):
    """A configured required trusted capability cannot be supplied."""


class ContributorIndeterminateError(RuntimeError):
    """A required contributor could not produce a valid invocation result."""


@dataclass(frozen=True)
class ContributorDiagnostic:
    capability_id: str
    contribution_id: str
    diagnostic_code: str
    error_class: str
    correlation_id: str

    @property
    def message(self) -> str:
        """Backward-compatible bounded diagnostic summary."""

        return self.error_class


@dataclass(frozen=True)
class NamespacedSafeReference:
    contribution_id: str
    namespace: str
    reference: SafeContextReferenceV1


@dataclass(frozen=True)
class ComposedContributions:
    persistable: tuple[NamespacedSafeReference, ...] = ()
    runtime_only: tuple[NamespacedSafeReference, ...] = ()
    secret_handles: tuple[NamespacedSafeReference, ...] = ()
    execution_digest: str = ""
    diagnostics: tuple[ContributorDiagnostic, ...] = ()


@dataclass(frozen=True)
class _Initialized:
    contribution_id: str
    capability_id: str
    contributor: OriginContributor | RunContextContributor
    required: bool


def _bounded_message(exc: BaseException) -> str:
    # Contributor exception text is untrusted and may contain credentials.  A
    # bounded exception class is enough for an operator diagnostic without
    # reflecting plugin-controlled values into logs or startup state.
    name = type(exc).__name__
    safe = "".join(character if character.isascii() and (character.isalnum() or character == "_") else "_" for character in name)
    return (safe or "Exception")[:128]


def _diagnostic(
    *,
    capability_id: str,
    contribution_id: str,
    diagnostic_code: str,
    failure: BaseException,
) -> ContributorDiagnostic:
    return ContributorDiagnostic(
        capability_id=capability_id,
        contribution_id=contribution_id,
        diagnostic_code=diagnostic_code,
        error_class=_bounded_message(failure),
        correlation_id=uuid.uuid4().hex,
    )


def _log_required_diagnostic(diagnostic: ContributorDiagnostic) -> None:
    logger.error(
        "Required invocation contributor failed capability_id=%s contribution_id=%s diagnostic_code=%s error_class=%s correlation_id=%s",
        diagnostic.capability_id,
        diagnostic.contribution_id,
        diagnostic.diagnostic_code,
        diagnostic.error_class,
        diagnostic.correlation_id,
    )


def _digest(payload: object) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _approved_reference_product(reference: SafeContextReferenceV1) -> Literal["persistable", "runtime_only", "secret_handle"]:
    """Apply the host-owned storage/consumer policy to one safe reference.

    Contributor storage labels are requests, not authority. Persistable safe
    values and stable handle identifiers are accepted as evidence; an
    ephemeral value must affect execution. Runtime-only correlation has no
    approved consumer and therefore fails closed instead of floating through
    an untyped context path.
    """

    if reference.purpose == "secret_handle":
        return "secret_handle"
    if reference.storage_class == "persistable":
        return "persistable"
    if reference.purpose == "execution":
        return "runtime_only"
    raise ContributorIndeterminateError("unsupported_reference_policy")


class ContributorHost:
    """Startup-initialized, immutable contributor runtime for one app."""

    def __init__(
        self,
        extensions: LoadedExtensions,
        *,
        required_capabilities: tuple[str, ...] | list[str] = (),
    ) -> None:
        required = tuple(required_capabilities)
        if len(required) != len(set(required)):
            raise RequiredCapabilityError("required_capabilities contains a duplicate capability ID")
        for capability_id in required:
            if _is_passthrough_required_capability(capability_id):
                continue
            kind, separator, contribution_id = capability_id.partition(":")
            if separator != ":" or kind not in _SUPPORTED_REQUIRED_KINDS or not contribution_id:
                raise RequiredCapabilityError(f"unsupported required capability {capability_id!r}")
        required_set = frozenset(required)
        contributor_required = frozenset(capability_id for capability_id in required_set if not _is_passthrough_required_capability(capability_id))
        available = {
            *(f"origin_contributor:{item.contribution_id}" for item in extensions.origin_contributor_factories),
            *(f"run_context_contributor:{item.contribution_id}" for item in extensions.run_context_contributor_factories),
        }
        missing = sorted(contributor_required - available)
        if missing:
            raise RequiredCapabilityError(f"required capability is not registered: {missing[0]}")

        diagnostics: list[ContributorDiagnostic] = []
        self._origin = self._initialize(
            "origin_contributor",
            extensions.origin_contributor_factories,
            contributor_required,
            diagnostics,
        )
        self._run_context = self._initialize(
            "run_context_contributor",
            extensions.run_context_contributor_factories,
            contributor_required,
            diagnostics,
        )
        self.startup_diagnostics = tuple(diagnostics)

    @property
    def initialized_capability_ids(self) -> frozenset[str]:
        """Capability IDs whose factories produced valid contributor objects."""

        return frozenset(item.capability_id for item in (*self._origin, *self._run_context))

    @staticmethod
    def _initialize(
        kind: Literal["origin_contributor", "run_context_contributor"],
        registrations: tuple[Any, ...],
        required: frozenset[str],
        diagnostics: list[ContributorDiagnostic],
    ) -> tuple[_Initialized, ...]:
        initialized: list[_Initialized] = []
        for registration in sorted(registrations, key=lambda item: item.contribution_id):
            capability_id = f"{kind}:{registration.contribution_id}"
            is_required = capability_id in required
            try:
                contributor = registration.factory()
                protocol = OriginContributor if kind == "origin_contributor" else RunContextContributor
                if not isinstance(contributor, protocol):
                    raise TypeError(f"factory returned an object that does not implement {protocol.__name__}")
            except Exception as exc:
                diagnostic = _diagnostic(
                    capability_id=capability_id,
                    contribution_id=registration.contribution_id,
                    diagnostic_code="initialization_failed",
                    failure=exc,
                )
                if is_required:
                    _log_required_diagnostic(diagnostic)
                    raise RequiredCapabilityError(f"required capability {capability_id} failed to initialize: {_bounded_message(exc)}") from None
                diagnostics.append(diagnostic)
                continue
            initialized.append(
                _Initialized(
                    contribution_id=registration.contribution_id,
                    capability_id=capability_id,
                    contributor=contributor,
                    required=is_required,
                )
            )
        return tuple(initialized)

    async def contribute_origin(self, request: OriginContributionRequestV1) -> ComposedContributions:
        return await self._compose(self._origin, request, OriginContributionV1)

    async def contribute_run_context(self, request: RunContextContributionRequestV1) -> ComposedContributions:
        return await self._compose(self._run_context, request, RunContextContributionV1)

    async def _compose(
        self,
        registrations: tuple[_Initialized, ...],
        request: OriginContributionRequestV1 | RunContextContributionRequestV1,
        expected_type: type[OriginContributionV1] | type[RunContextContributionV1],
    ) -> ComposedContributions:
        async def _call(registration: _Initialized) -> object:
            return await asyncio.wait_for(
                registration.contributor.contribute(request),  # type: ignore[arg-type]
                timeout=_TIMEOUT_SECONDS,
            )

        results = await asyncio.gather(
            *(_call(registration) for registration in registrations),
            return_exceptions=True,
        )
        namespaces: set[str] = set()
        fully_qualified_keys: set[str] = set()
        persistable: list[NamespacedSafeReference] = []
        runtime_only: list[NamespacedSafeReference] = []
        secret_handles: list[NamespacedSafeReference] = []
        aggregate_projection: list[dict[str, Any]] = []
        execution_values: list[dict[str, Any]] = []
        diagnostics: list[ContributorDiagnostic] = []
        for registration, result in zip(registrations, results, strict=True):
            failure: BaseException | None = None
            if isinstance(result, BaseException):
                failure = result
            elif result is None:
                continue
            elif not isinstance(result, expected_type):
                failure = TypeError(f"contributor returned {type(result).__name__}; expected {expected_type.__name__} or None")
            if failure is not None:
                diagnostic = _diagnostic(
                    capability_id=registration.capability_id,
                    contribution_id=registration.contribution_id,
                    diagnostic_code=("contribution_timeout" if isinstance(failure, TimeoutError) else "contribution_failed"),
                    failure=failure,
                )
                if registration.required:
                    _log_required_diagnostic(diagnostic)
                    raise ContributorIndeterminateError(f"required capability {registration.capability_id} failed: {_bounded_message(failure)}") from None
                diagnostics.append(diagnostic)
                continue

            assert isinstance(result, (OriginContributionV1, RunContextContributionV1))
            duplicate_key = any(f"{result.namespace}.{reference.key}" in fully_qualified_keys for reference in result.references)
            if duplicate_key:
                raise ContributorIndeterminateError("duplicate_fully_qualified_key")
            if result.namespace in namespaces:
                raise ContributorIndeterminateError("duplicate_namespace")
            namespaces.add(result.namespace)
            for reference in result.references:
                fully_qualified_keys.add(f"{result.namespace}.{reference.key}")
                item = NamespacedSafeReference(
                    contribution_id=registration.contribution_id,
                    namespace=result.namespace,
                    reference=reference,
                )
                approved_product = _approved_reference_product(reference)
                if approved_product == "secret_handle":
                    secret_handles.append(item)
                elif approved_product == "persistable":
                    persistable.append(item)
                else:
                    runtime_only.append(item)
                aggregate_projection.append(
                    {
                        "contribution_id": registration.contribution_id,
                        "namespace": result.namespace,
                        "key": reference.key,
                        "purpose": reference.purpose,
                        "storage_class": reference.storage_class,
                        "value": reference.value,
                    }
                )
                if reference.purpose in {"execution", "secret_handle"}:
                    execution_values.append(
                        {
                            "contribution_id": registration.contribution_id,
                            "namespace": result.namespace,
                            "key": reference.key,
                            "purpose": reference.purpose,
                            "value": reference.value,
                        }
                    )
        if len(aggregate_projection) > _MAX_AGGREGATE_REFERENCES:
            raise ContributorIndeterminateError("aggregate_reference_limit")
        aggregate_canonical = json.dumps(
            {"version": 1, "references": aggregate_projection},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(aggregate_canonical) > _MAX_AGGREGATE_CANONICAL_BYTES:
            raise ContributorIndeterminateError("aggregate_canonical_size_limit")
        return ComposedContributions(
            persistable=tuple(persistable),
            runtime_only=tuple(runtime_only),
            secret_handles=tuple(secret_handles),
            execution_digest=_digest({"version": 1, "execution": execution_values}),
            diagnostics=tuple(diagnostics),
        )
