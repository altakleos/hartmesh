"""Provider-neutral contracts for evidence-bearing external retrieval.

The portable projections in this module intentionally contain no query or
credential-derived value. Operational selectors live only on the in-memory
policy/request objects and are replaced by bounded public facts before an
observation is persisted or returned by an API.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal
from urllib.parse import urlsplit

_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$", re.ASCII)
_PUBLIC_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$", re.ASCII)
_DOMAIN_RE = re.compile(r"^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)(?:\.(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?))*$", re.ASCII)
_RAGFLOW_SOURCE_RE = re.compile(
    r"^ragflow-doc:([A-Za-z0-9_.-]{1,128}):([0-9a-f]{64})$",
    re.ASCII,
)
_MAX_SOURCE_REFERENCE_BYTES = 512
_MAX_RESULTS = 100
_MAX_ITEM_BYTES = 1024 * 1024
_MAX_AGGREGATE_BYTES = 8 * 1024 * 1024
_MAX_TIMEOUT_MS = 120_000
_MAX_RECENCY_DAYS = 36_500


class RetrievalEvidenceError(ValueError):
    """A bounded machine-readable retrieval evidence failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class RetrievalPolicyDenied(RetrievalEvidenceError):
    """A caller-requested constraint would broaden server-owned policy."""


class RetrievalProviderError(RetrievalEvidenceError):
    """A provider failure reduced to a finite safe category."""

    _STATUSES = frozenset(
        {
            "provider_unavailable",
            "timeout",
            "rate_limited",
            "authentication_failed",
            "configuration_error",
            "unsafe_response",
            "oversized_response",
        }
    )

    def __init__(self, status: str) -> None:
        if status not in self._STATUSES:
            raise ValueError("unsupported retrieval provider failure category")
        self.status = status
        super().__init__(f"retrieval_{status}")


def _canonical_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RetrievalEvidenceError("retrieval_projection_invalid") from exc


def _domain_digest(domain: str, value: object) -> str:
    return hashlib.sha256(b"hartmesh/" + domain.encode("ascii") + b"/v1\0" + _canonical_bytes(value)).hexdigest()


def _identifier(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER_RE.fullmatch(value) is None:
        raise RetrievalEvidenceError(f"retrieval_{field_name}_invalid")
    return value


def _domain(value: object) -> str:
    if not isinstance(value, str):
        raise RetrievalEvidenceError("retrieval_domain_invalid")
    normalized = value.strip().rstrip(".").lower()
    try:
        normalized = normalized.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise RetrievalEvidenceError("retrieval_domain_invalid") from exc
    if len(normalized) > 253 or _DOMAIN_RE.fullmatch(normalized) is None:
        raise RetrievalEvidenceError("retrieval_domain_invalid")
    return normalized


def _domains(values: tuple[str, ...], *, field_name: str) -> tuple[str, ...]:
    normalized = tuple(sorted({_domain(value) for value in values}))
    if len(normalized) > 64:
        raise RetrievalEvidenceError(f"retrieval_{field_name}_too_large")
    return normalized


def _domain_within(candidate: str, boundary: str) -> bool:
    return candidate == boundary or candidate.endswith(f".{boundary}")


def _normalized_origin(value: str) -> str:
    try:
        parsed = urlsplit(value)
        scheme = parsed.scheme.lower()
        hostname = parsed.hostname
        port = parsed.port
    except (TypeError, ValueError) as exc:
        raise RetrievalEvidenceError("retrieval_endpoint_invalid") from exc
    if scheme not in {"http", "https"} or hostname is None:
        raise RetrievalEvidenceError("retrieval_endpoint_invalid")
    host = _domain(hostname)
    if port is not None and port != (443 if scheme == "https" else 80):
        host = f"{host}:{port}"
    return f"{scheme}://{host}"


@dataclass(frozen=True, slots=True)
class RetrievalRequestConstraintsV1:
    """Caller-requested constraints, before intersection with server policy."""

    provider_id: str
    endpoint: str
    collections: tuple[str, ...] = ()
    domains: tuple[str, ...] = ()
    recency_days: int | None = None
    max_results: int | None = None
    max_item_bytes: int | None = None
    max_aggregate_bytes: int | None = None
    timeout_ms: int | None = None
    allow_redirects: bool | None = None
    accept_partial: bool | None = None
    source_schemes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider_id", _identifier(self.provider_id, field_name="provider_id"))
        if not isinstance(self.endpoint, str) or not self.endpoint:
            raise RetrievalEvidenceError("retrieval_endpoint_invalid")
        if len(self.collections) > 64 or any(not isinstance(item, str) or not item or len(item.encode("utf-8")) > 256 for item in self.collections):
            raise RetrievalEvidenceError("retrieval_collections_invalid")
        object.__setattr__(self, "collections", tuple(dict.fromkeys(self.collections)))
        object.__setattr__(self, "domains", _domains(self.domains, field_name="domains"))
        for name in ("recency_days", "max_results", "max_item_bytes", "max_aggregate_bytes", "timeout_ms"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 1):
                raise RetrievalEvidenceError(f"retrieval_{name}_invalid")
        if self.recency_days is not None and self.recency_days > _MAX_RECENCY_DAYS:
            raise RetrievalEvidenceError("retrieval_recency_days_invalid")
        for name, ceiling in (
            ("max_results", _MAX_RESULTS),
            ("max_item_bytes", _MAX_ITEM_BYTES),
            ("max_aggregate_bytes", _MAX_AGGREGATE_BYTES),
            ("timeout_ms", _MAX_TIMEOUT_MS),
        ):
            value = getattr(self, name)
            if value is not None and value > ceiling:
                raise RetrievalEvidenceError(f"retrieval_{name}_invalid")
        for name in ("allow_redirects", "accept_partial"):
            value = getattr(self, name)
            if value is not None and type(value) is not bool:
                raise RetrievalEvidenceError(f"retrieval_{name}_invalid")
        schemes = tuple(sorted(set(self.source_schemes)))
        if any(item not in {"http", "https", "ragflow-doc"} for item in schemes):
            raise RetrievalEvidenceError("retrieval_source_scheme_invalid")
        object.__setattr__(self, "source_schemes", schemes)


@dataclass(frozen=True, slots=True)
class EffectiveRetrievalConstraintsV1:
    """The accepted, narrowed constraint set used for one provider call."""

    provider_id: str
    endpoint_origin: str
    collections: tuple[str, ...]
    collection_public_refs: tuple[str, ...]
    domains: tuple[str, ...]
    domain_scope: Literal["provider_default", "restricted"]
    recency_days: int | None
    max_results: int
    max_item_bytes: int
    max_aggregate_bytes: int
    timeout_ms: int
    allow_redirects: bool
    accept_partial: bool
    source_schemes: tuple[str, ...]
    policy_digest: str

    def to_safe_projection(self) -> dict[str, object]:
        """Return only operator-approved constraints, never private selectors."""

        return {
            "version": 1,
            "provider_id": self.provider_id,
            "collection_public_refs": list(self.collection_public_refs),
            "domain_scope": self.domain_scope,
            "recency_days": self.recency_days,
            "max_results": self.max_results,
            "max_item_bytes": self.max_item_bytes,
            "max_aggregate_bytes": self.max_aggregate_bytes,
            "timeout_ms": self.timeout_ms,
            "allow_redirects": self.allow_redirects,
            "accept_partial": self.accept_partial,
            "source_schemes": list(self.source_schemes),
            "policy_digest": self.policy_digest,
        }


@dataclass(frozen=True, slots=True)
class RetrievalPolicyV1:
    """Immutable server-owned ceiling for one retrieval capability."""

    allowed_providers: tuple[str, ...]
    allowed_endpoint_origins: tuple[str, ...]
    allowed_collections: tuple[str, ...] = ()
    collection_public_refs: tuple[str, ...] = ()
    web_domain_allowlist: tuple[str, ...] = ()
    web_domain_denylist: tuple[str, ...] = ()
    max_recency_days: int | None = None
    max_results: int = 10
    max_item_bytes: int = 16 * 1024
    max_aggregate_bytes: int = 64 * 1024
    timeout_ms: int = 30_000
    allow_redirects: bool = False
    source_schemes: tuple[Literal["http", "https", "ragflow-doc"], ...] = ("https",)
    accept_partial: bool = False
    version: Literal[1] = field(default=1, init=False)

    def __post_init__(self) -> None:
        providers = tuple(sorted({_identifier(item, field_name="provider_id") for item in self.allowed_providers}))
        if not providers or len(providers) > 16:
            raise RetrievalEvidenceError("retrieval_allowed_providers_invalid")
        object.__setattr__(self, "allowed_providers", providers)
        origins = tuple(sorted({_normalized_origin(item) for item in self.allowed_endpoint_origins}))
        if not origins or len(origins) > 16:
            raise RetrievalEvidenceError("retrieval_allowed_endpoints_invalid")
        object.__setattr__(self, "allowed_endpoint_origins", origins)
        if len(self.allowed_collections) > 64 or any(not isinstance(item, str) or not item or len(item.encode("utf-8")) > 256 for item in self.allowed_collections):
            raise RetrievalEvidenceError("retrieval_allowed_collections_invalid")
        collections = tuple(dict.fromkeys(self.allowed_collections))
        object.__setattr__(self, "allowed_collections", collections)
        refs = tuple(self.collection_public_refs)
        if refs and len(refs) != len(collections):
            raise RetrievalEvidenceError("retrieval_collection_refs_invalid")
        if any(not isinstance(item, str) or _PUBLIC_REF_RE.fullmatch(item) is None for item in refs):
            raise RetrievalEvidenceError("retrieval_collection_refs_invalid")
        object.__setattr__(self, "collection_public_refs", refs)
        object.__setattr__(self, "web_domain_allowlist", _domains(self.web_domain_allowlist, field_name="domain_allowlist"))
        object.__setattr__(self, "web_domain_denylist", _domains(self.web_domain_denylist, field_name="domain_denylist"))
        if any(any(_domain_within(allowed, denied) for denied in self.web_domain_denylist) for allowed in self.web_domain_allowlist):
            raise RetrievalEvidenceError("retrieval_domain_policy_conflict")
        for name in ("max_results", "max_item_bytes", "max_aggregate_bytes", "timeout_ms"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise RetrievalEvidenceError(f"retrieval_{name}_invalid")
        for name, ceiling in (
            ("max_results", _MAX_RESULTS),
            ("max_item_bytes", _MAX_ITEM_BYTES),
            ("max_aggregate_bytes", _MAX_AGGREGATE_BYTES),
            ("timeout_ms", _MAX_TIMEOUT_MS),
        ):
            if getattr(self, name) > ceiling:
                raise RetrievalEvidenceError(f"retrieval_{name}_invalid")
        if self.max_item_bytes > self.max_aggregate_bytes:
            raise RetrievalEvidenceError("retrieval_size_policy_invalid")
        if self.max_recency_days is not None and (type(self.max_recency_days) is not int or self.max_recency_days < 1):
            raise RetrievalEvidenceError("retrieval_max_recency_days_invalid")
        if self.max_recency_days is not None and self.max_recency_days > _MAX_RECENCY_DAYS:
            raise RetrievalEvidenceError("retrieval_max_recency_days_invalid")
        if type(self.allow_redirects) is not bool or type(self.accept_partial) is not bool:
            raise RetrievalEvidenceError("retrieval_boolean_policy_invalid")
        schemes = tuple(sorted(set(self.source_schemes)))
        if not schemes or any(item not in {"http", "https", "ragflow-doc"} for item in schemes):
            raise RetrievalEvidenceError("retrieval_source_scheme_invalid")
        object.__setattr__(self, "source_schemes", schemes)

    def _digest_projection(self) -> dict[str, object]:
        # Portable policy identity is intentionally computed from the safe
        # policy projection, not private endpoint/collection selectors. The
        # accepted tool-plane digests bind exact deployment material without
        # turning this digest into a low-entropy selector oracle.
        return {
            "version": self.version,
            "allowed_providers": list(self.allowed_providers),
            "collection_public_refs": list(self.collection_public_refs),
            "domain_scope": ("restricted" if self.web_domain_allowlist or self.web_domain_denylist else "provider_default"),
            "max_recency_days": self.max_recency_days,
            "max_results": self.max_results,
            "max_item_bytes": self.max_item_bytes,
            "max_aggregate_bytes": self.max_aggregate_bytes,
            "timeout_ms": self.timeout_ms,
            "allow_redirects": self.allow_redirects,
            "source_schemes": list(self.source_schemes),
            "accept_partial": self.accept_partial,
        }

    @property
    def digest(self) -> str:
        return _domain_digest("retrieval-policy", self._digest_projection())

    def narrow(self, requested: RetrievalRequestConstraintsV1) -> EffectiveRetrievalConstraintsV1:
        """Intersect caller constraints with this policy or fail before I/O."""

        if not isinstance(requested, RetrievalRequestConstraintsV1):
            raise TypeError("requested must be RetrievalRequestConstraintsV1")
        if requested.provider_id not in self.allowed_providers:
            raise RetrievalPolicyDenied("retrieval_policy_provider_denied")
        endpoint_origin = _normalized_origin(requested.endpoint)
        if endpoint_origin not in self.allowed_endpoint_origins:
            raise RetrievalPolicyDenied("retrieval_policy_endpoint_denied")

        allowed_collections = set(self.allowed_collections)
        collections = requested.collections or self.allowed_collections
        if any(item not in allowed_collections for item in collections):
            raise RetrievalPolicyDenied("retrieval_policy_collection_denied")
        ref_by_collection = dict(zip(self.allowed_collections, self.collection_public_refs, strict=False))
        public_refs = tuple(ref_by_collection[item] for item in collections if item in ref_by_collection)

        domains = requested.domains or self.web_domain_allowlist
        if self.web_domain_allowlist and any(not any(_domain_within(item, boundary) for boundary in self.web_domain_allowlist) for item in domains):
            raise RetrievalPolicyDenied("retrieval_policy_domain_denied")
        if any(any(_domain_within(item, denied) for denied in self.web_domain_denylist) for item in domains):
            raise RetrievalPolicyDenied("retrieval_policy_domain_denied")

        if requested.recency_days is not None and self.max_recency_days is not None and requested.recency_days > self.max_recency_days:
            raise RetrievalPolicyDenied("retrieval_policy_recency_denied")
        recency = requested.recency_days if requested.recency_days is not None else self.max_recency_days

        def narrowed_int(name: str) -> int:
            ceiling = getattr(self, name)
            value = getattr(requested, name)
            if value is not None and value > ceiling:
                raise RetrievalPolicyDenied(f"retrieval_policy_{name}_denied")
            return ceiling if value is None else value

        allow_redirects = self.allow_redirects if requested.allow_redirects is None else requested.allow_redirects
        if allow_redirects and not self.allow_redirects:
            raise RetrievalPolicyDenied("retrieval_policy_redirect_denied")
        accept_partial = self.accept_partial if requested.accept_partial is None else requested.accept_partial
        if accept_partial and not self.accept_partial:
            raise RetrievalPolicyDenied("retrieval_policy_partial_denied")
        schemes = requested.source_schemes or self.source_schemes
        if any(item not in self.source_schemes for item in schemes):
            raise RetrievalPolicyDenied("retrieval_policy_source_scheme_denied")

        return EffectiveRetrievalConstraintsV1(
            provider_id=requested.provider_id,
            endpoint_origin=endpoint_origin,
            collections=tuple(collections),
            collection_public_refs=public_refs,
            domains=tuple(domains),
            domain_scope=("restricted" if self.web_domain_allowlist or self.web_domain_denylist else "provider_default"),
            recency_days=recency,
            max_results=narrowed_int("max_results"),
            max_item_bytes=narrowed_int("max_item_bytes"),
            max_aggregate_bytes=narrowed_int("max_aggregate_bytes"),
            timeout_ms=narrowed_int("timeout_ms"),
            allow_redirects=allow_redirects,
            accept_partial=accept_partial,
            source_schemes=tuple(schemes),
            policy_digest=self.digest,
        )


def normalize_web_source_reference(
    value: object,
    *,
    allowed_schemes: tuple[str, ...] = ("https",),
    allowed_domains: tuple[str, ...] = (),
    denied_domains: tuple[str, ...] = (),
) -> str:
    """Reduce a public web locator to its approved origin.

    Paths, query parameters, and fragments are intentionally removed
    wholesale. Any of them can reflect search terms, tokens, document
    identifiers, or user data. The returned origin is therefore a coarse
    source reference, not a page-level locator.
    """

    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 8_192:
        raise RetrievalEvidenceError("retrieval_source_invalid")
    try:
        parsed = urlsplit(value)
        scheme = parsed.scheme.lower()
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise RetrievalEvidenceError("retrieval_source_invalid") from exc
    if scheme not in allowed_schemes or scheme not in {"http", "https"} or hostname is None:
        raise RetrievalEvidenceError("retrieval_source_scheme_denied")
    host = _domain(hostname)
    allowed = tuple(_domain(item) for item in allowed_domains)
    denied = tuple(_domain(item) for item in denied_domains)
    if allowed and not any(_domain_within(host, item) for item in allowed):
        raise RetrievalPolicyDenied("retrieval_source_domain_denied")
    if any(_domain_within(host, item) for item in denied):
        raise RetrievalPolicyDenied("retrieval_source_domain_denied")
    authority = host
    if port is not None and port != (443 if scheme == "https" else 80):
        authority = f"{host}:{port}"
    if re.search(r"%(?![0-9A-Fa-f]{2})", parsed.path):
        raise RetrievalEvidenceError("retrieval_source_invalid")
    normalized = f"{scheme}://{authority}"
    if len(normalized.encode("utf-8")) > _MAX_SOURCE_REFERENCE_BYTES:
        raise RetrievalEvidenceError("retrieval_source_too_large")
    return normalized


def _digest(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise RetrievalEvidenceError(f"retrieval_{field_name}_invalid")
    return value


def _bounded_reference(value: object, *, field_name: str, max_bytes: int = 128) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > max_bytes or any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise RetrievalEvidenceError(f"retrieval_{field_name}_invalid")
    return value


@dataclass(frozen=True, slots=True)
class ResolvedRetrievalCredentialV1:
    """A server-resolved credential handle that can never be serialized.

    ``selector_ref`` is operational configuration identity. It and ``secret``
    are deliberately absent from every portable projection and digest.
    """

    provider_id: str
    selector_ref: str
    secret: object = field(repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider_id", _identifier(self.provider_id, field_name="provider_id"))
        _bounded_reference(self.selector_ref, field_name="credential_selector")

    @property
    def available(self) -> bool:
        return self.secret is not None and not (isinstance(self.secret, str) and not self.secret)


@dataclass(frozen=True, slots=True)
class AcceptedRetrievalRequest:
    """One host-accepted request bound to an active durable tool attempt."""

    thread_id: str
    tenant: object
    receipt: object
    actor_ref: str
    provider_id: str
    tool_kind: str
    adapter_capability_version: str
    query: str = field(repr=False)
    credential: ResolvedRetrievalCredentialV1 = field(repr=False)
    policy: RetrievalPolicyV1
    requested_constraints: RetrievalRequestConstraintsV1
    tool_plane_base_revision_digest: str
    tool_plane_user_overlay_digest: str
    tool_plane_projection_digest: str
    tool_plane_effective_digest: str
    accepted_execution_evidence_ref: str | None = None
    accepted_sandbox_operation_ref: str | None = None
    mcp_evidence_ref: str | None = None
    effective_constraints: EffectiveRetrievalConstraintsV1 = field(init=False)

    def __post_init__(self) -> None:
        # Local imports avoid turning the provider-neutral package into a
        # constructor for tenant or receipt models owned by earlier projects.
        from deerflow_extension_api import TenantReferenceV1

        from deerflow.runtime.tool_evidence import DurableToolReceiptV1

        _bounded_reference(self.thread_id, field_name="thread_id", max_bytes=64)
        if not isinstance(self.tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1")
        if not isinstance(self.receipt, DurableToolReceiptV1) or self.receipt.phase != "started":
            raise RetrievalEvidenceError("retrieval_active_receipt_required")
        if self.receipt.context.tenant != self.tenant:
            raise RetrievalEvidenceError("retrieval_tenant_mismatch")
        _bounded_reference(self.actor_ref, field_name="actor_ref", max_bytes=128)
        provider_id = _identifier(self.provider_id, field_name="provider_id")
        object.__setattr__(self, "provider_id", provider_id)
        object.__setattr__(self, "tool_kind", _identifier(self.tool_kind, field_name="tool_kind"))
        _bounded_reference(self.adapter_capability_version, field_name="adapter_capability_version", max_bytes=64)
        if not isinstance(self.query, str) or not self.query.strip() or len(self.query.encode("utf-8")) > 16 * 1024:
            raise RetrievalEvidenceError("retrieval_query_invalid")
        if not isinstance(self.credential, ResolvedRetrievalCredentialV1) or self.credential.provider_id != provider_id:
            raise RetrievalEvidenceError("retrieval_credential_mismatch")
        if not self.credential.available:
            raise RetrievalEvidenceError("retrieval_credential_unavailable")
        if not isinstance(self.policy, RetrievalPolicyV1):
            raise TypeError("policy must be RetrievalPolicyV1")
        if not isinstance(self.requested_constraints, RetrievalRequestConstraintsV1) or self.requested_constraints.provider_id != provider_id:
            raise RetrievalEvidenceError("retrieval_constraints_mismatch")
        object.__setattr__(self, "effective_constraints", self.policy.narrow(self.requested_constraints))
        for name in (
            "tool_plane_base_revision_digest",
            "tool_plane_user_overlay_digest",
            "tool_plane_projection_digest",
            "tool_plane_effective_digest",
        ):
            _digest(getattr(self, name), field_name=name)
        for name in (
            "accepted_execution_evidence_ref",
            "accepted_sandbox_operation_ref",
            "mcp_evidence_ref",
        ):
            value = getattr(self, name)
            if value is not None:
                _bounded_reference(value, field_name=name, max_bytes=256)


@dataclass(frozen=True, slots=True)
class ProviderRetrievalRequest:
    """Protected adapter input. This object must never be logged or persisted."""

    query: str = field(repr=False)
    credential: ResolvedRetrievalCredentialV1 = field(repr=False)
    endpoint: str
    constraints: EffectiveRetrievalConstraintsV1


@dataclass(frozen=True, slots=True)
class ProviderRetrievalItem:
    """Protected provider item plus the locator facts needed for evidence."""

    source_locator: str | None = None
    content: object = field(default="", repr=False)
    collection_selector: str | None = field(default=None, repr=False)
    document_selector: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.source_locator is None and self.document_selector is None:
            raise RetrievalEvidenceError("retrieval_item_source_missing")
        if self.source_locator is not None and (not isinstance(self.source_locator, str) or not self.source_locator):
            raise RetrievalEvidenceError("retrieval_item_source_invalid")
        for name in ("collection_selector", "document_selector"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value or len(value.encode("utf-8")) > 512):
                raise RetrievalEvidenceError(f"retrieval_item_{name}_invalid")


@dataclass(frozen=True, slots=True)
class ProviderRetrievalResponse:
    """Narrow normalized response returned by a provider adapter."""

    candidate_result: object = field(repr=False)
    items: tuple[ProviderRetrievalItem, ...]
    content_type: str = "application/json"
    result_count: int | None = None
    safe_request_ref: str | None = None
    partial: bool = False
    truncated: bool = False

    def __post_init__(self) -> None:
        items = tuple(self.items)
        if any(not isinstance(item, ProviderRetrievalItem) for item in items):
            raise RetrievalEvidenceError("retrieval_provider_items_invalid")
        object.__setattr__(self, "items", items)
        _bounded_reference(
            self.content_type,
            field_name="provider_content_type",
            max_bytes=128,
        )
        if self.result_count is not None and (type(self.result_count) is not int or self.result_count < 0):
            raise RetrievalEvidenceError("retrieval_provider_result_count_invalid")
        if self.safe_request_ref is not None:
            _bounded_reference(self.safe_request_ref, field_name="provider_request_ref", max_bytes=128)
        if type(self.partial) is not bool or type(self.truncated) is not bool:
            raise RetrievalEvidenceError("retrieval_provider_flags_invalid")


def _safe_constraints_projection(value: Mapping[str, object]) -> dict[str, object]:
    """Detach and validate the closed, query-free constraint vocabulary."""

    encoded = _canonical_bytes(value)
    if len(encoded) > 4 * 1024:
        raise RetrievalEvidenceError("retrieval_constraints_too_large")
    detached = json.loads(encoded)
    if not isinstance(detached, dict):
        raise RetrievalEvidenceError("retrieval_constraints_invalid")
    keys = set(detached)
    sentinel_keys = {"version", "provider_id", "policy_status"}
    full_keys = {
        "version",
        "provider_id",
        "collection_public_refs",
        "domain_scope",
        "recency_days",
        "max_results",
        "max_item_bytes",
        "max_aggregate_bytes",
        "timeout_ms",
        "allow_redirects",
        "accept_partial",
        "source_schemes",
        "policy_digest",
    }
    if detached.get("version") != 1:
        raise RetrievalEvidenceError("retrieval_constraints_invalid")
    _identifier(detached.get("provider_id"), field_name="provider_id")
    if keys == sentinel_keys:
        if detached.get("policy_status") not in {"not_evaluated", "denied"}:
            raise RetrievalEvidenceError("retrieval_constraints_invalid")
        return detached
    if keys != full_keys:
        raise RetrievalEvidenceError("retrieval_constraints_invalid")

    refs = detached.get("collection_public_refs")
    schemes = detached.get("source_schemes")
    if not isinstance(refs, list) or len(refs) > 64 or any(not isinstance(item, str) or _PUBLIC_REF_RE.fullmatch(item) is None for item in refs) or len(set(refs)) != len(refs):
        raise RetrievalEvidenceError("retrieval_constraints_invalid")
    if detached.get("domain_scope") not in {"provider_default", "restricted"}:
        raise RetrievalEvidenceError("retrieval_constraints_invalid")
    recency = detached.get("recency_days")
    if recency is not None and (type(recency) is not int or not 1 <= recency <= _MAX_RECENCY_DAYS):
        raise RetrievalEvidenceError("retrieval_constraints_invalid")
    for name, ceiling in (
        ("max_results", _MAX_RESULTS),
        ("max_item_bytes", _MAX_ITEM_BYTES),
        ("max_aggregate_bytes", _MAX_AGGREGATE_BYTES),
        ("timeout_ms", _MAX_TIMEOUT_MS),
    ):
        item = detached.get(name)
        if type(item) is not int or not 1 <= item <= ceiling:
            raise RetrievalEvidenceError("retrieval_constraints_invalid")
    if detached["max_item_bytes"] > detached["max_aggregate_bytes"]:
        raise RetrievalEvidenceError("retrieval_constraints_invalid")
    if type(detached.get("allow_redirects")) is not bool or type(detached.get("accept_partial")) is not bool:
        raise RetrievalEvidenceError("retrieval_constraints_invalid")
    if not isinstance(schemes, list) or not schemes or any(item not in {"http", "https", "ragflow-doc"} for item in schemes) or len(set(schemes)) != len(schemes):
        raise RetrievalEvidenceError("retrieval_constraints_invalid")
    _digest(detached.get("policy_digest"), field_name="policy_digest")
    return detached


def _validate_safe_source_reference(
    reference: str,
    constraints: Mapping[str, object],
) -> None:
    schemes = constraints.get("source_schemes")
    if not isinstance(schemes, list):
        raise RetrievalEvidenceError("retrieval_source_invalid")
    if reference.startswith("ragflow-doc:"):
        match = _RAGFLOW_SOURCE_RE.fullmatch(reference)
        public_refs = constraints.get("collection_public_refs")
        if match is None or "ragflow-doc" not in schemes or not isinstance(public_refs, list) or match.group(1) not in public_refs:
            raise RetrievalEvidenceError("retrieval_source_invalid")
        return
    normalized = normalize_web_source_reference(
        reference,
        allowed_schemes=tuple(schemes),
    )
    if normalized != reference:
        raise RetrievalEvidenceError("retrieval_source_invalid")


@dataclass(frozen=True, slots=True)
class RetrievalObservationDraftV1:
    """Host-created safe facts awaiting the outer receipt's final digest."""

    tenant_ref: str
    tenant_digest: str
    run_id: str
    receipt_id: str
    attempt: int
    provider_id: str
    tool_kind: str
    adapter_capability_version: str
    policy_digest: str
    safe_constraints: Mapping[str, object]
    started_at: datetime
    provider_finished_at: datetime
    provider_status: Literal[
        "success",
        "empty",
        "partial",
        "policy_denied",
        "provider_unavailable",
        "timeout",
        "rate_limited",
        "authentication_failed",
        "configuration_error",
        "unsafe_response",
        "oversized_response",
        "internal_error",
        "cancelled",
    ]
    safe_reason: str | None
    result_count: int
    source_count: int
    source_references: tuple[str, ...]
    truncated: bool
    partial: bool
    safe_provider_request_ref: str | None
    tool_plane_base_revision_digest: str
    tool_plane_user_overlay_digest: str
    tool_plane_projection_digest: str
    tool_plane_effective_digest: str
    accepted_execution_evidence_ref: str | None = None
    accepted_sandbox_operation_ref: str | None = None
    mcp_evidence_ref: str | None = None
    version: Literal[1] = field(default=1, init=False)

    def __post_init__(self) -> None:
        if type(self.attempt) is not int or self.attempt < 1:
            raise RetrievalEvidenceError("retrieval_attempt_invalid")
        for name in ("tenant_digest", "policy_digest", "tool_plane_base_revision_digest", "tool_plane_user_overlay_digest", "tool_plane_projection_digest", "tool_plane_effective_digest"):
            _digest(getattr(self, name), field_name=name)
        for name, limit in (
            ("tenant_ref", 64),
            ("run_id", 64),
            ("receipt_id", 128),
            ("provider_id", 64),
            ("tool_kind", 64),
            ("adapter_capability_version", 64),
        ):
            _bounded_reference(getattr(self, name), field_name=name, max_bytes=limit)
        if self.started_at.tzinfo is None or self.provider_finished_at.tzinfo is None:
            raise RetrievalEvidenceError("retrieval_timestamp_invalid")
        if self.provider_finished_at < self.started_at:
            raise RetrievalEvidenceError("retrieval_timestamp_invalid")
        if self.provider_status not in {
            "success",
            "empty",
            "partial",
            "policy_denied",
            "provider_unavailable",
            "timeout",
            "rate_limited",
            "authentication_failed",
            "configuration_error",
            "unsafe_response",
            "oversized_response",
            "internal_error",
            "cancelled",
        }:
            raise RetrievalEvidenceError("retrieval_provider_status_invalid")
        if type(self.result_count) is not int or self.result_count < 0 or type(self.source_count) is not int or self.source_count < 0:
            raise RetrievalEvidenceError("retrieval_count_invalid")
        refs = tuple(self.source_references)
        if len(refs) > 64 or self.source_count != len(refs) or self.source_count > self.result_count or (self.provider_status in {"success", "partial"} and self.result_count > 0 and self.source_count == 0) or len(set(refs)) != len(refs):
            raise RetrievalEvidenceError("retrieval_source_count_invalid")
        if any(not isinstance(item, str) or not item or len(item.encode("utf-8")) > _MAX_SOURCE_REFERENCE_BYTES for item in refs):
            raise RetrievalEvidenceError("retrieval_source_invalid")
        object.__setattr__(self, "source_references", refs)
        if type(self.truncated) is not bool or type(self.partial) is not bool:
            raise RetrievalEvidenceError("retrieval_observation_flags_invalid")
        if self.safe_reason is not None:
            _identifier(self.safe_reason, field_name="safe_reason")
        if self.safe_provider_request_ref is not None:
            _bounded_reference(self.safe_provider_request_ref, field_name="provider_request_ref")
        constraints = _safe_constraints_projection(self.safe_constraints)
        if constraints["provider_id"] != self.provider_id:
            raise RetrievalEvidenceError("retrieval_constraints_invalid")
        if "policy_digest" in constraints and constraints["policy_digest"] != self.policy_digest:
            raise RetrievalEvidenceError("retrieval_constraints_invalid")
        if "policy_status" in constraints:
            if self.result_count or refs:
                raise RetrievalEvidenceError("retrieval_source_invalid")
        else:
            max_results = constraints["max_results"]
            if type(max_results) is not int or self.result_count > max_results:
                raise RetrievalEvidenceError("retrieval_count_invalid")
            for reference in refs:
                _validate_safe_source_reference(reference, constraints)
        successful_statuses = {"success", "empty", "partial"}
        if self.provider_status == "success" and self.result_count == 0:
            raise RetrievalEvidenceError("retrieval_provider_status_invalid")
        if self.provider_status == "empty" and (self.result_count != 0 or refs):
            raise RetrievalEvidenceError("retrieval_provider_status_invalid")
        if self.partial != (self.provider_status == "partial"):
            raise RetrievalEvidenceError("retrieval_provider_status_invalid")
        if self.provider_status not in successful_statuses and (self.result_count != 0 or refs or self.truncated or self.partial or self.safe_reason is None):
            raise RetrievalEvidenceError("retrieval_provider_status_invalid")
        object.__setattr__(self, "safe_constraints", constraints)

    def to_event_projection(self) -> dict[str, object]:
        duration_ms = max(
            0,
            min(
                86_400_000,
                int((self.provider_finished_at - self.started_at).total_seconds() * 1_000),
            ),
        )
        return {
            "version": self.version,
            "canonicalization": "hartmesh-retrieval-v1",
            "tenant_ref": self.tenant_ref,
            "tenant_digest": self.tenant_digest,
            "run_id": self.run_id,
            "receipt_id": self.receipt_id,
            "attempt": self.attempt,
            "provider_id": self.provider_id,
            "tool_kind": self.tool_kind,
            "adapter_capability_version": self.adapter_capability_version,
            "policy_digest": self.policy_digest,
            "safe_constraints": dict(self.safe_constraints),
            "started_at": self.started_at.astimezone(UTC).isoformat(),
            "provider_finished_at": self.provider_finished_at.astimezone(UTC).isoformat(),
            "duration_ms": duration_ms,
            "provider_status": self.provider_status,
            "safe_reason": self.safe_reason,
            "result_count": self.result_count,
            "source_count": self.source_count,
            "source_references": list(self.source_references),
            "truncated": self.truncated,
            "partial": self.partial,
            "safe_provider_request_ref": self.safe_provider_request_ref,
            "tool_plane": {
                "base_revision_digest": self.tool_plane_base_revision_digest,
                "user_overlay_digest": self.tool_plane_user_overlay_digest,
                "projection_digest": self.tool_plane_projection_digest,
                "effective_digest": self.tool_plane_effective_digest,
            },
            "accepted_execution_evidence_ref": self.accepted_execution_evidence_ref,
            "accepted_sandbox_operation_ref": self.accepted_sandbox_operation_ref,
            "mcp_evidence_ref": self.mcp_evidence_ref,
        }

    @property
    def draft_digest(self) -> str:
        return hashlib.sha256(
            _canonical_bytes(
                {
                    "domain": "hartmesh/retrieval-draft/v1",
                    "projection": self.to_event_projection(),
                }
            )
        ).hexdigest()

    @classmethod
    def from_event_projection(cls, value: object) -> RetrievalObservationDraftV1:
        expected = {
            "version",
            "canonicalization",
            "tenant_ref",
            "tenant_digest",
            "run_id",
            "receipt_id",
            "attempt",
            "provider_id",
            "tool_kind",
            "adapter_capability_version",
            "policy_digest",
            "safe_constraints",
            "started_at",
            "provider_finished_at",
            "duration_ms",
            "provider_status",
            "safe_reason",
            "result_count",
            "source_count",
            "source_references",
            "truncated",
            "partial",
            "safe_provider_request_ref",
            "tool_plane",
            "accepted_execution_evidence_ref",
            "accepted_sandbox_operation_ref",
            "mcp_evidence_ref",
        }
        if not isinstance(value, Mapping) or set(value) != expected or value.get("version") != 1 or value.get("canonicalization") != "hartmesh-retrieval-v1":
            raise RetrievalEvidenceError("retrieval_draft_projection_invalid")
        tool_plane = value.get("tool_plane")
        if not isinstance(tool_plane, Mapping) or set(tool_plane) != {
            "base_revision_digest",
            "user_overlay_digest",
            "projection_digest",
            "effective_digest",
        }:
            raise RetrievalEvidenceError("retrieval_tool_plane_invalid")
        refs = value.get("source_references")
        if not isinstance(refs, list):
            raise RetrievalEvidenceError("retrieval_source_invalid")
        constraints = value.get("safe_constraints")
        if not isinstance(constraints, Mapping):
            raise RetrievalEvidenceError("retrieval_constraints_invalid")
        try:
            started_at = datetime.fromisoformat(str(value["started_at"]))
            finished_at = datetime.fromisoformat(str(value["provider_finished_at"]))
        except (TypeError, ValueError) as exc:
            raise RetrievalEvidenceError("retrieval_timestamp_invalid") from exc
        draft = cls(
            tenant_ref=value["tenant_ref"],  # type: ignore[arg-type]
            tenant_digest=value["tenant_digest"],  # type: ignore[arg-type]
            run_id=value["run_id"],  # type: ignore[arg-type]
            receipt_id=value["receipt_id"],  # type: ignore[arg-type]
            attempt=value["attempt"],  # type: ignore[arg-type]
            provider_id=value["provider_id"],  # type: ignore[arg-type]
            tool_kind=value["tool_kind"],  # type: ignore[arg-type]
            adapter_capability_version=value["adapter_capability_version"],  # type: ignore[arg-type]
            policy_digest=value["policy_digest"],  # type: ignore[arg-type]
            safe_constraints=constraints,
            started_at=started_at,
            provider_finished_at=finished_at,
            provider_status=value["provider_status"],  # type: ignore[arg-type]
            safe_reason=value["safe_reason"],  # type: ignore[arg-type]
            result_count=value["result_count"],  # type: ignore[arg-type]
            source_count=value["source_count"],  # type: ignore[arg-type]
            source_references=tuple(refs),  # type: ignore[arg-type]
            truncated=value["truncated"],  # type: ignore[arg-type]
            partial=value["partial"],  # type: ignore[arg-type]
            safe_provider_request_ref=value["safe_provider_request_ref"],  # type: ignore[arg-type]
            tool_plane_base_revision_digest=tool_plane["base_revision_digest"],  # type: ignore[arg-type]
            tool_plane_user_overlay_digest=tool_plane["user_overlay_digest"],  # type: ignore[arg-type]
            tool_plane_projection_digest=tool_plane["projection_digest"],  # type: ignore[arg-type]
            tool_plane_effective_digest=tool_plane["effective_digest"],  # type: ignore[arg-type]
            accepted_execution_evidence_ref=value["accepted_execution_evidence_ref"],  # type: ignore[arg-type]
            accepted_sandbox_operation_ref=value["accepted_sandbox_operation_ref"],  # type: ignore[arg-type]
            mcp_evidence_ref=value["mcp_evidence_ref"],  # type: ignore[arg-type]
        )
        if value.get("duration_ms") != draft.to_event_projection()["duration_ms"]:
            raise RetrievalEvidenceError("retrieval_duration_invalid")
        return draft


@dataclass(frozen=True, slots=True)
class RetrievalObservationV1:
    """Terminal portable observation joined to exactly one receipt outcome."""

    draft: RetrievalObservationDraftV1
    receipt_phase: Literal["succeeded", "failed", "denied", "cancelled"]
    result_projection_digest: str | None
    result_kind: str | None
    safe_terminal_reason: str | None
    terminal_at: datetime
    observation_digest: str = field(init=False)
    observation_id: str = field(init=False)
    version: Literal[1] = field(default=1, init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.draft, RetrievalObservationDraftV1):
            raise TypeError("draft must be RetrievalObservationDraftV1")
        if self.receipt_phase not in {"succeeded", "failed", "denied", "cancelled"}:
            raise RetrievalEvidenceError("retrieval_receipt_phase_invalid")
        if self.result_projection_digest is not None:
            _digest(self.result_projection_digest, field_name="result_projection_digest")
        if (self.result_projection_digest is None) != (self.result_kind is None):
            raise RetrievalEvidenceError("retrieval_result_projection_invalid")
        if self.result_kind is not None:
            _bounded_reference(self.result_kind, field_name="result_kind", max_bytes=32)
        if self.receipt_phase == "succeeded" and self.result_projection_digest is None:
            raise RetrievalEvidenceError("retrieval_success_result_missing")
        provider_status = self.draft.provider_status
        if self.receipt_phase == "succeeded" and provider_status not in {
            "success",
            "empty",
            "partial",
        }:
            raise RetrievalEvidenceError("retrieval_terminal_status_mismatch")
        if provider_status == "policy_denied" and self.receipt_phase != "denied":
            raise RetrievalEvidenceError("retrieval_terminal_status_mismatch")
        if provider_status == "cancelled" and self.receipt_phase != "cancelled":
            raise RetrievalEvidenceError("retrieval_terminal_status_mismatch")
        if (
            provider_status
            in {
                "provider_unavailable",
                "timeout",
                "rate_limited",
                "authentication_failed",
                "configuration_error",
                "unsafe_response",
                "oversized_response",
                "internal_error",
            }
            and self.receipt_phase != "failed"
        ):
            raise RetrievalEvidenceError("retrieval_terminal_status_mismatch")
        if self.safe_terminal_reason is not None:
            _identifier(self.safe_terminal_reason, field_name="safe_terminal_reason")
        if self.terminal_at.tzinfo is None or self.terminal_at < self.draft.provider_finished_at:
            raise RetrievalEvidenceError("retrieval_terminal_timestamp_invalid")
        digest = hashlib.sha256(_canonical_bytes(self._digest_projection())).hexdigest()
        object.__setattr__(self, "observation_digest", digest)
        object.__setattr__(self, "observation_id", f"ro_{digest}")
        if len(_canonical_bytes(self.to_event_body())) > 12 * 1024:
            raise RetrievalEvidenceError("retrieval_observation_too_large")

    @property
    def receipt_id(self) -> str:
        return self.draft.receipt_id

    @property
    def attempt(self) -> int:
        return self.draft.attempt

    @property
    def idempotency_key(self) -> str:
        return f"{self.receipt_id}:retrieval"

    @property
    def final_result_status(self) -> str:
        return "success" if self.receipt_phase == "succeeded" else "error"

    def _digest_projection(self) -> dict[str, object]:
        return {
            "domain": "hartmesh/retrieval-observation/v1",
            "draft": self.draft.to_event_projection(),
            "draft_digest": self.draft.draft_digest,
            "receipt_phase": self.receipt_phase,
            "result_projection_digest": self.result_projection_digest,
            "result_kind": self.result_kind,
            "final_result_status": self.final_result_status,
            "safe_terminal_reason": self.safe_terminal_reason,
            "terminal_at": self.terminal_at.astimezone(UTC).isoformat(),
        }

    def to_event_body(self) -> dict[str, object]:
        return {
            "version": self.version,
            "observation_id": self.observation_id,
            "idempotency_key": self.idempotency_key,
            "observation_digest": self.observation_digest,
            **self._digest_projection(),
        }

    def to_public_projection(self) -> dict[str, object]:
        """Return the authorized bounded API shape (never query/result text)."""

        draft = self.draft
        return {
            "version": self.version,
            "observation_id": self.observation_id,
            "observation_digest": self.observation_digest,
            "receipt_id": draft.receipt_id,
            "attempt": draft.attempt,
            "provider_id": draft.provider_id,
            "tool_kind": draft.tool_kind,
            "adapter_capability_version": draft.adapter_capability_version,
            "policy_digest": draft.policy_digest,
            "safe_constraints": dict(draft.safe_constraints),
            "started_at": draft.started_at.astimezone(UTC).isoformat(),
            "provider_finished_at": draft.provider_finished_at.astimezone(UTC).isoformat(),
            "duration_ms": draft.to_event_projection()["duration_ms"],
            "terminal_at": self.terminal_at.astimezone(UTC).isoformat(),
            "status": self.receipt_phase,
            "provider_status": draft.provider_status,
            "final_result_status": self.final_result_status,
            "safe_terminal_reason": self.safe_terminal_reason,
            "result_projection_digest": self.result_projection_digest,
            "result_kind": self.result_kind,
            "result_count": draft.result_count,
            "source_count": draft.source_count,
            "source_references": list(draft.source_references),
            "truncated": draft.truncated,
            "partial": draft.partial,
            "safe_provider_request_ref": draft.safe_provider_request_ref,
            "tool_plane": {
                "base_revision_digest": draft.tool_plane_base_revision_digest,
                "user_overlay_digest": draft.tool_plane_user_overlay_digest,
                "projection_digest": draft.tool_plane_projection_digest,
                "effective_digest": draft.tool_plane_effective_digest,
            },
            "accepted_execution_evidence_ref": draft.accepted_execution_evidence_ref,
            "accepted_sandbox_operation_ref": draft.accepted_sandbox_operation_ref,
            "mcp_evidence_ref": draft.mcp_evidence_ref,
        }

    @classmethod
    def finalize(cls, receipt: object, draft: RetrievalObservationDraftV1) -> RetrievalObservationV1:
        from deerflow.runtime.tool_evidence import DurableToolReceiptV1

        if not isinstance(receipt, DurableToolReceiptV1) or receipt.phase == "started":
            raise RetrievalEvidenceError("retrieval_terminal_receipt_required")
        if not isinstance(draft, RetrievalObservationDraftV1):
            raise TypeError("draft must be RetrievalObservationDraftV1")
        if draft.receipt_id != receipt.receipt_id or draft.run_id != receipt.context.run_id or draft.attempt != receipt.context.attempt:
            raise RetrievalEvidenceError("retrieval_receipt_mismatch")
        tenant = receipt.context.tenant
        if tenant is None or draft.tenant_ref != tenant.public_ref or draft.tenant_digest != tenant.digest:
            raise RetrievalEvidenceError("retrieval_tenant_mismatch")
        return cls(
            draft=draft,
            receipt_phase=receipt.phase,
            result_projection_digest=receipt.result_projection_digest,
            result_kind=receipt.result_kind,
            safe_terminal_reason=receipt.safe_error_code or draft.safe_reason,
            terminal_at=receipt.occurred_at,
        )

    @classmethod
    def from_event_body(cls, value: object) -> RetrievalObservationV1:
        expected = {
            "version",
            "observation_id",
            "idempotency_key",
            "observation_digest",
            "domain",
            "draft",
            "draft_digest",
            "receipt_phase",
            "result_projection_digest",
            "result_kind",
            "final_result_status",
            "safe_terminal_reason",
            "terminal_at",
        }
        if not isinstance(value, Mapping) or set(value) != expected or value.get("version") != 1 or value.get("domain") != "hartmesh/retrieval-observation/v1":
            raise RetrievalEvidenceError("retrieval_observation_invalid")
        draft = RetrievalObservationDraftV1.from_event_projection(value.get("draft"))
        if value.get("draft_digest") != draft.draft_digest:
            raise RetrievalEvidenceError("retrieval_draft_digest_mismatch")
        try:
            terminal_at = datetime.fromisoformat(str(value["terminal_at"]))
        except (TypeError, ValueError) as exc:
            raise RetrievalEvidenceError("retrieval_terminal_timestamp_invalid") from exc
        observation = cls(
            draft=draft,
            receipt_phase=value["receipt_phase"],  # type: ignore[arg-type]
            result_projection_digest=value["result_projection_digest"],  # type: ignore[arg-type]
            result_kind=value["result_kind"],  # type: ignore[arg-type]
            safe_terminal_reason=value["safe_terminal_reason"],  # type: ignore[arg-type]
            terminal_at=terminal_at,
        )
        if (
            value.get("idempotency_key") != observation.idempotency_key
            or value.get("final_result_status") != observation.final_result_status
            or value.get("observation_id") != observation.observation_id
            or value.get("observation_digest") != observation.observation_digest
        ):
            raise RetrievalEvidenceError("retrieval_observation_digest_mismatch")
        return observation


def validate_retrieval_pair(
    receipt_body: Mapping[str, object],
    observation_body: Mapping[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    """Validate and detach an exact terminal receipt/observation pair."""

    from deerflow.runtime.tool_evidence import DurableToolReceiptV1

    try:
        receipt_detached = json.loads(_canonical_bytes(receipt_body))
        observation_detached = json.loads(_canonical_bytes(observation_body))
    except (TypeError, ValueError) as exc:
        raise RetrievalEvidenceError("retrieval_pair_invalid") from exc
    if not isinstance(receipt_detached, dict) or not isinstance(observation_detached, dict):
        raise RetrievalEvidenceError("retrieval_pair_invalid")
    receipt = DurableToolReceiptV1.from_event_body(
        receipt_detached,
        occurred_at=datetime.now(UTC),
    )
    observation = RetrievalObservationV1.from_event_body(observation_detached)
    if receipt.phase == "started":
        raise RetrievalEvidenceError("retrieval_terminal_receipt_required")
    tenant = receipt.context.tenant
    if (
        tenant is None
        or observation.draft.run_id != receipt.context.run_id
        or observation.draft.tenant_ref != tenant.public_ref
        or observation.draft.tenant_digest != tenant.digest
        or observation.receipt_id != receipt.receipt_id
        or observation.attempt != receipt.context.attempt
        or observation.receipt_phase != receipt.phase
        or observation.result_projection_digest != receipt.result_projection_digest
        or observation.result_kind != receipt.result_kind
        or observation.safe_terminal_reason != (receipt.safe_error_code or observation.draft.safe_reason)
    ):
        raise RetrievalEvidenceError("retrieval_pair_mismatch")
    return receipt.to_event_body(), observation.to_event_body()


def retrieval_observation_event_metadata(
    observation: RetrievalObservationV1,
    *,
    task_id: str,
    writer_fence_digest: str,
) -> dict[str, object]:
    return {
        "receipt_id": observation.receipt_id,
        "task_id": task_id,
        "attempt": observation.attempt,
        "provider_id": observation.draft.provider_id,
        "observation_id": observation.observation_id,
        "writer_fence_digest": writer_fence_digest,
        "content_is_json": True,
        "content_is_dict": True,
    }


@dataclass(frozen=True, slots=True)
class RetrievalCandidate:
    """Provider candidate plus its non-terminal safe observation draft."""

    result: object = field(repr=False)
    draft: RetrievalObservationDraftV1
