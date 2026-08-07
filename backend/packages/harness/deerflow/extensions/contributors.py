"""Capability Host execution for trusted invocation contributors."""

from __future__ import annotations

import asyncio
import hashlib
import json
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

_TIMEOUT_SECONDS = 2.0
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
    message: str


@dataclass(frozen=True)
class NamespacedSafeReference:
    contribution_id: str
    namespace: str
    reference: SafeContextReferenceV1


@dataclass(frozen=True)
class ComposedContributions:
    persistable: tuple[NamespacedSafeReference, ...] = ()
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
    return type(exc).__name__[:128]


def _digest(payload: object) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


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
                if is_required:
                    raise RequiredCapabilityError(f"required capability {capability_id} failed to initialize: {_bounded_message(exc)}") from exc
                diagnostics.append(ContributorDiagnostic(capability_id, _bounded_message(exc)))
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
        persistable: list[NamespacedSafeReference] = []
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
            elif result.namespace in namespaces:
                failure = ValueError(f"duplicate contributor namespace {result.namespace!r}")
            if failure is not None:
                if registration.required:
                    raise ContributorIndeterminateError(f"required capability {registration.capability_id} failed: {_bounded_message(failure)}") from failure
                diagnostics.append(ContributorDiagnostic(registration.capability_id, _bounded_message(failure)))
                continue

            assert isinstance(result, (OriginContributionV1, RunContextContributionV1))
            namespaces.add(result.namespace)
            for reference in result.references:
                item = NamespacedSafeReference(
                    contribution_id=registration.contribution_id,
                    namespace=result.namespace,
                    reference=reference,
                )
                if reference.storage_class == "persistable":
                    persistable.append(item)
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
        return ComposedContributions(
            persistable=tuple(persistable),
            execution_digest=_digest({"version": 1, "execution": execution_values}),
            diagnostics=tuple(diagnostics),
        )
