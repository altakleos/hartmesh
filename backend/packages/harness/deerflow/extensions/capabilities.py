"""Safe operational projections for authoritative extension capabilities."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Collection
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any, Literal

from deerflow_extension_api import (
    API_VERSION,
    INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2,
    INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY,
    INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,
    MCP_INTERCEPTOR_KIND,
    ORIGIN_CONTRIBUTOR_KIND,
    RUN_CONTEXT_CONTRIBUTOR_KIND,
    CapabilityHealthResult,
)

from deerflow.diagnostics import bounded_diagnostic, log_bounded_failure
from deerflow.extensions.registry import LoadedExtensions

logger = logging.getLogger(__name__)

RequiredCapabilityOwner = Literal["contributors", "constraints", "mcp"]
_MAX_REQUIRED_CAPABILITY_BYTES = 160

# This is the canonical ownership classification used by Gateway composition.
# Exact versioned IDs come from the public contract; dynamic contribution IDs
# are routed by the public descriptor kind. A new constraints version becomes
# routable by extending this host-owned set from the public version constants,
# without teaching unrelated contributor or MCP hosts about that version.
_INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITIES = frozenset(
    {
        INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY,
        INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,
    }
)
REQUIRED_CAPABILITY_ID_OWNERS = MappingProxyType(
    {capability_id: "constraints" for capability_id in _INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITIES},
)
REQUIRED_CAPABILITY_KIND_OWNERS = MappingProxyType(
    {
        ORIGIN_CONTRIBUTOR_KIND: "contributors",
        RUN_CONTEXT_CONTRIBUTOR_KIND: "contributors",
        MCP_INTERCEPTOR_KIND: "mcp",
    }
)


class RequiredCapabilityRoutingError(RuntimeError):
    """Operator-required capabilities cannot be routed to one owning host."""


@dataclass(frozen=True)
class RequiredCapabilityRoutes:
    """One deterministic partition of startup-only required capabilities."""

    contributors: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()
    mcp: tuple[str, ...] = ()


def _safe_capability_label(value: object) -> str:
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"
    encoded = value.encode("utf-8")
    if len(encoded) > _MAX_REQUIRED_CAPABILITY_BYTES or any(ord(character) < 32 or ord(character) == 127 for character in value):
        return "<invalid>"
    return repr(value)


def route_required_capabilities(
    required_capabilities: Collection[str],
) -> RequiredCapabilityRoutes:
    """Validate and route every required capability to exactly one host."""

    required = tuple(required_capabilities)
    if any(not isinstance(capability_id, str) for capability_id in required):
        invalid = next(capability_id for capability_id in required if not isinstance(capability_id, str))
        raise RequiredCapabilityRoutingError(f"unsupported required capability {_safe_capability_label(invalid)}")
    if len(required) != len(set(required)):
        raise RequiredCapabilityRoutingError("required_capabilities contains a duplicate capability ID")
    routed: dict[RequiredCapabilityOwner, list[str]] = {
        "contributors": [],
        "constraints": [],
        "mcp": [],
    }
    for capability_id in required:
        owner = REQUIRED_CAPABILITY_ID_OWNERS.get(capability_id)
        if owner is None and isinstance(capability_id, str):
            kind, separator, contribution_id = capability_id.partition(":")
            if separator == ":" and contribution_id:
                owner = REQUIRED_CAPABILITY_KIND_OWNERS.get(kind)
        invalid = len(capability_id.encode("utf-8")) > _MAX_REQUIRED_CAPABILITY_BYTES
        invalid = invalid or any(ord(character) < 32 or ord(character) == 127 for character in capability_id)
        if owner is None or invalid:
            raise RequiredCapabilityRoutingError(f"unsupported required capability {_safe_capability_label(capability_id)}")
        routed[owner].append(capability_id)
    return RequiredCapabilityRoutes(
        contributors=tuple(routed["contributors"]),
        constraints=tuple(routed["constraints"]),
        mcp=tuple(routed["mcp"]),
    )


@dataclass(frozen=True)
class CapabilityPluginManifestEntry:
    """Bounded package identity for one plugin in a capability manifest."""

    package_name: str | None
    package_version: str | None
    load_required: bool
    source_entry_digest: str | None


@dataclass(frozen=True)
class CapabilityContributionManifestEntry:
    """Safe source attribution for every registered extension contribution."""

    contribution_id: str | None
    contribution_type: str
    package_name: str | None
    package_version: str | None
    source_entry_digest: str | None


@dataclass(frozen=True)
class CapabilityManifestEntry:
    """Immutable initialization evidence for one contributed capability."""

    contribution_id: str
    capability_id: str
    capability_type: str
    capability_api_version: str
    package_name: str | None
    package_version: str | None
    source_entry_digest: str | None
    operator_required: bool
    initialization_status: Literal["initialized", "failed", "missing"]
    diagnostic_code: str


@dataclass(frozen=True)
class CapabilityManifest:
    """Canonical startup-frozen capability generation and its digest."""

    extension_api_version: str
    extension_generation: int
    artifact_manifest_digest: str | None
    extension_configuration_digest: str | None
    plugins: tuple[CapabilityPluginManifestEntry, ...]
    contributions: tuple[CapabilityContributionManifestEntry, ...]
    capabilities: tuple[CapabilityManifestEntry, ...]
    digest: str


@dataclass(frozen=True)
class CapabilityHealthSnapshot:
    """One time-bounded health result for a contributed capability."""

    contribution_id: str
    capability_id: str
    status: Literal["healthy", "unhealthy", "unknown"]
    diagnostic_code: str
    checked_at: datetime
    expires_at: datetime
    extension_generation: int = 0
    last_healthy_at: datetime | None = None
    correlation_id: str | None = None


@dataclass(frozen=True)
class CapabilityReadinessSnapshot:
    """Aggregate readiness and bounded capability-health evidence."""

    status: Literal["ready", "not_ready"]
    health: tuple[CapabilityHealthSnapshot, ...]


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def capability_manifest_to_dict(manifest: CapabilityManifest) -> dict[str, Any]:
    """Return the exact safe public projection of an immutable manifest."""

    return {
        "version": 2,
        "extension_api_version": manifest.extension_api_version,
        "extension_generation": manifest.extension_generation,
        "artifact_manifest_digest": manifest.artifact_manifest_digest,
        "extension_configuration_digest": manifest.extension_configuration_digest,
        "manifest_digest": manifest.digest,
        "plugins": [asdict(item) for item in manifest.plugins],
        "contributions": [asdict(item) for item in manifest.contributions],
        "capabilities": [asdict(item) for item in manifest.capabilities],
    }


def capability_health_to_dict(
    snapshots: Collection[CapabilityHealthSnapshot],
) -> dict[str, Any]:
    """Return mutable health separately from manifest identity."""

    return {
        "version": 1,
        "snapshots": [
            {
                "contribution_id": item.contribution_id,
                "capability_id": item.capability_id,
                "status": item.status,
                "diagnostic_code": item.diagnostic_code,
                "checked_at": _timestamp(item.checked_at),
                "expires_at": _timestamp(item.expires_at),
                "extension_generation": item.extension_generation,
                "last_healthy_at": (_timestamp(item.last_healthy_at) if item.last_healthy_at is not None else None),
                "correlation_id": item.correlation_id,
            }
            for item in snapshots
        ],
    }


def _constraint_capability_id(capability_api_version: str) -> str:
    if capability_api_version == INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2:
        return INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2
    return INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY


def _manifest_digest(payload: dict[str, object]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _registration_manifest_entry(
    registration: object,
    *,
    capability_id: str,
    capability_type: str,
    operator_required: bool,
    initialized_capability_ids: frozenset[str],
) -> CapabilityManifestEntry:
    initialized = capability_id in initialized_capability_ids
    return CapabilityManifestEntry(
        contribution_id=str(getattr(registration, "contribution_id")),
        capability_id=capability_id,
        capability_type=capability_type,
        capability_api_version=str(getattr(registration, "capability_api_version")),
        package_name=getattr(registration, "package_name", None),
        package_version=getattr(registration, "package_version", None),
        source_entry_digest=getattr(registration, "source_entry_digest", None),
        operator_required=operator_required,
        initialization_status="initialized" if initialized else "failed",
        diagnostic_code="initialized" if initialized else "initialization_failed",
    )


def build_capability_manifest(
    extensions: LoadedExtensions,
    *,
    required_capabilities: Collection[str] = (),
    authorization_required: bool = False,
    legacy_authorization_initialized: bool = False,
    initialized_capability_ids: Collection[str] = (),
) -> CapabilityManifest:
    """Build the deterministic safe projection for one immutable generation."""

    required = frozenset(required_capabilities)
    initialized = frozenset(initialized_capability_ids)
    plugin_requirements: dict[tuple[str | None, str | None, str | None], bool] = {}
    for item in extensions.loaded_plugins:
        key = (item.package_name, item.package_version, item.source_entry_digest)
        plugin_requirements[key] = plugin_requirements.get(key, False) or item.required
    plugins = tuple(
        CapabilityPluginManifestEntry(
            package_name=name,
            package_version=version,
            load_required=plugin_requirements[(name, version, source_entry_digest)],
            source_entry_digest=source_entry_digest,
        )
        for name, version, source_entry_digest in sorted(
            plugin_requirements,
            key=lambda item: (item[0] or "", item[1] or "", item[2] or ""),
        )
    )
    contributions = tuple(
        CapabilityContributionManifestEntry(
            contribution_id=item.contribution_id,
            contribution_type=item.contribution_type,
            package_name=item.package_name,
            package_version=item.package_version,
            source_entry_digest=item.source_entry_digest,
        )
        for item in extensions.contributions
    )
    entries: list[CapabilityManifestEntry] = []
    for registration in extensions.authorization_provider_factories:
        capability_id = f"authorization_provider:{registration.contribution_id}"
        entries.append(
            _registration_manifest_entry(
                registration,
                capability_id=capability_id,
                capability_type="authorization_provider",
                operator_required=authorization_required,
                initialized_capability_ids=initialized,
            )
        )
    if legacy_authorization_initialized:
        capability_id = "authorization_provider:legacy"
        entries.append(
            CapabilityManifestEntry(
                contribution_id="legacy",
                capability_id=capability_id,
                capability_type="authorization_provider",
                capability_api_version="1.0",
                package_name=None,
                package_version=None,
                source_entry_digest=None,
                operator_required=authorization_required,
                initialization_status="initialized",
                diagnostic_code="initialized",
            )
        )
    if authorization_required and not any(entry.capability_type == "authorization_provider" for entry in entries):
        entries.append(
            CapabilityManifestEntry(
                contribution_id="missing",
                capability_id="authorization_provider:missing",
                capability_type="authorization_provider",
                capability_api_version="1.0",
                package_name=None,
                package_version=None,
                source_entry_digest=None,
                operator_required=True,
                initialization_status="missing",
                diagnostic_code="not_registered",
            )
        )
    for capability_type, registrations in (
        ("origin_contributor", extensions.origin_contributor_factories),
        ("run_context_contributor", extensions.run_context_contributor_factories),
    ):
        for registration in registrations:
            capability_id = f"{capability_type}:{registration.contribution_id}"
            entries.append(
                _registration_manifest_entry(
                    registration,
                    capability_id=capability_id,
                    capability_type=capability_type,
                    operator_required=capability_id in required,
                    initialized_capability_ids=initialized,
                )
            )
    for registration in extensions.invocation_constraints_provider_factories:
        capability_id = _constraint_capability_id(registration.capability_api_version)
        entries.append(
            _registration_manifest_entry(
                registration,
                capability_id=capability_id,
                capability_type="invocation_constraints",
                operator_required=capability_id in required,
                initialized_capability_ids=initialized,
            )
        )
    for registration in extensions.mcp_interceptor_descriptors:
        capability_id = f"mcp_interceptor:{registration.contribution_id}"
        if registration.contribution_id in extensions.mcp_interceptor_conflicts:
            entries.append(
                CapabilityManifestEntry(
                    contribution_id=registration.contribution_id,
                    capability_id=capability_id,
                    capability_type="mcp_interceptor",
                    capability_api_version=registration.capability_api_version,
                    package_name=registration.package_name,
                    package_version=registration.package_version,
                    source_entry_digest=registration.source_entry_digest,
                    operator_required=capability_id in required,
                    initialization_status="failed",
                    diagnostic_code="duplicate_registration",
                )
            )
        else:
            entries.append(
                _registration_manifest_entry(
                    registration,
                    capability_id=capability_id,
                    capability_type="mcp_interceptor",
                    operator_required=capability_id in required,
                    initialized_capability_ids=initialized,
                )
            )

    registered_ids = {item.capability_id for item in entries}
    for missing_id in sorted(required - registered_ids):
        capability_type, separator, contribution_id = missing_id.partition(":")
        if not separator:
            capability_type = (
                "invocation_constraints"
                if missing_id
                in {
                    INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY,
                    INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,
                }
                else missing_id
            )
            contribution_id = missing_id
        entries.append(
            CapabilityManifestEntry(
                contribution_id=contribution_id,
                capability_id=missing_id,
                capability_type=capability_type,
                capability_api_version=(INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2 if missing_id == INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2 else "1.0"),
                package_name=None,
                package_version=None,
                source_entry_digest=None,
                operator_required=True,
                initialization_status="missing",
                diagnostic_code="not_registered",
            )
        )
    capabilities = tuple(
        sorted(
            entries,
            key=lambda item: (
                item.capability_type,
                item.contribution_id,
                item.package_name or "",
            ),
        )
    )
    payload: dict[str, object] = {
        "version": 2,
        "extension_api_version": API_VERSION,
        "extension_generation": extensions.generation,
        "artifact_manifest_digest": extensions.artifact_manifest_digest,
        "extension_configuration_digest": extensions.extension_configuration_digest,
        "plugins": [asdict(item) for item in plugins],
        "contributions": [asdict(item) for item in contributions],
        "capabilities": [asdict(item) for item in capabilities],
    }
    return CapabilityManifest(
        extension_api_version=API_VERSION,
        extension_generation=extensions.generation,
        artifact_manifest_digest=extensions.artifact_manifest_digest,
        extension_configuration_digest=extensions.extension_configuration_digest,
        plugins=plugins,
        contributions=contributions,
        capabilities=capabilities,
        digest=_manifest_digest(payload),
    )


class CapabilityHealthMonitor:
    """Cache and single-flight health probes for one immutable manifest."""

    def __init__(
        self,
        manifest: CapabilityManifest,
        extensions: LoadedExtensions,
        *,
        clock: Any | None = None,
        cache_seconds: float = 10.0,
        timeout_seconds: float = 2.0,
        stale_seconds: float = 30.0,
        admission_max_age_seconds: float = 10.0,
    ) -> None:
        if cache_seconds <= 0 or timeout_seconds <= 0 or stale_seconds <= 0 or admission_max_age_seconds <= 0:
            raise ValueError("capability health timing values must be positive")
        self._manifest = manifest
        self._clock = clock or (lambda: datetime.now(UTC))
        self._cache_ttl = timedelta(seconds=cache_seconds)
        self._timeout_seconds = timeout_seconds
        self._stale_after = timedelta(seconds=stale_seconds)
        self._admission_max_age = timedelta(seconds=admission_max_age_seconds)
        self._probes: dict[str, Any] = {}
        for registration in (
            *extensions.authorization_provider_factories,
            *extensions.origin_contributor_factories,
            *extensions.run_context_contributor_factories,
        ):
            self._probes[f"{registration.kind}:{registration.contribution_id}"] = registration.health_probe
        for registration in extensions.invocation_constraints_provider_factories:
            self._probes[_constraint_capability_id(registration.capability_api_version)] = registration.health_probe
        for registration in extensions.mcp_interceptor_descriptors:
            self._probes[f"mcp_interceptor:{registration.contribution_id}"] = registration.health_probe
        self._cache: dict[str, CapabilityHealthSnapshot] = {}
        self._last_healthy: dict[str, datetime] = {}
        self._inflight: dict[str, asyncio.Task[CapabilityHealthSnapshot]] = {}
        self._lock = asyncio.Lock()

    def _now(self) -> datetime:
        now = self._clock()
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("capability health clock must return an aware datetime")
        return now

    async def _run_probe(
        self,
        entry: CapabilityManifestEntry,
    ) -> CapabilityHealthSnapshot:
        now = self._now()
        correlation_id: str | None = None
        if entry.initialization_status != "initialized":
            snapshot = CapabilityHealthSnapshot(
                contribution_id=entry.contribution_id,
                capability_id=entry.capability_id,
                status="unhealthy",
                diagnostic_code=entry.diagnostic_code,
                checked_at=now,
                expires_at=now + self._cache_ttl,
                extension_generation=self._manifest.extension_generation,
            )
        else:
            probe = self._probes.get(entry.capability_id)
            if probe is None:
                result = CapabilityHealthResult(status="healthy")
            else:
                try:
                    async with asyncio.timeout(self._timeout_seconds):
                        result = await probe()
                    if type(result) is not CapabilityHealthResult:
                        raise TypeError("health probe returned an invalid result")
                except asyncio.CancelledError:
                    raise
                except TimeoutError as exc:
                    diagnostic = bounded_diagnostic(
                        code="probe_timeout",
                        operation="health_probe",
                        error=exc,
                        capability_id=entry.capability_id,
                        contribution_id=entry.contribution_id,
                    )
                    correlation_id = diagnostic.correlation_id
                    log_bounded_failure(logger, diagnostic)
                    result = CapabilityHealthResult(
                        status="unhealthy",
                        diagnostic_code="probe_timeout",
                    )
                except Exception as exc:
                    diagnostic = bounded_diagnostic(
                        code="probe_failed",
                        operation="health_probe",
                        error=exc,
                        capability_id=entry.capability_id,
                        contribution_id=entry.contribution_id,
                    )
                    correlation_id = diagnostic.correlation_id
                    log_bounded_failure(logger, diagnostic)
                    result = CapabilityHealthResult(
                        status="unhealthy",
                        diagnostic_code="probe_failed",
                    )
            checked_at = self._now()
            if result.status == "healthy":
                self._last_healthy[entry.capability_id] = checked_at
            snapshot = CapabilityHealthSnapshot(
                contribution_id=entry.contribution_id,
                capability_id=entry.capability_id,
                status=result.status,
                diagnostic_code=result.diagnostic_code or ("healthy" if result.status == "healthy" else "reported_unhealthy"),
                checked_at=checked_at,
                expires_at=checked_at + self._cache_ttl,
                extension_generation=self._manifest.extension_generation,
                last_healthy_at=self._last_healthy.get(entry.capability_id),
                correlation_id=correlation_id,
            )
        async with self._lock:
            self._cache[entry.capability_id] = snapshot
            current = self._inflight.get(entry.capability_id)
            if current is asyncio.current_task():
                self._inflight.pop(entry.capability_id, None)
        return snapshot

    async def _snapshot_for(
        self,
        entry: CapabilityManifestEntry,
        *,
        refresh: bool,
    ) -> CapabilityHealthSnapshot:
        now = self._now()
        async with self._lock:
            cached = self._cache.get(entry.capability_id)
            if cached is not None and (not refresh or cached.expires_at > now):
                return cached
            task = self._inflight.get(entry.capability_id)
            if task is not None:
                return CapabilityHealthSnapshot(
                    contribution_id=entry.contribution_id,
                    capability_id=entry.capability_id,
                    status="unknown",
                    diagnostic_code="refresh_in_progress",
                    checked_at=now,
                    expires_at=now,
                    extension_generation=self._manifest.extension_generation,
                    last_healthy_at=(cached.last_healthy_at if cached is not None else None),
                )
            task = asyncio.create_task(self._run_probe(entry))
            self._inflight[entry.capability_id] = task
        return await asyncio.shield(task)

    async def health(
        self,
        *,
        refresh: bool = True,
    ) -> tuple[CapabilityHealthSnapshot, ...]:
        return tuple(await asyncio.gather(*(self._snapshot_for(entry, refresh=refresh) for entry in self._manifest.capabilities)))

    async def health_for(
        self,
        capability_ids: Collection[str],
        *,
        refresh: bool = True,
    ) -> tuple[CapabilityHealthSnapshot, ...]:
        """Return bounded, fresh snapshots for an exact capability set."""

        requested = frozenset(capability_ids)
        entries = tuple(entry for entry in self._manifest.capabilities if entry.capability_id in requested)
        snapshots = list(await asyncio.gather(*(self._snapshot_for(entry, refresh=refresh) for entry in entries)))
        now = self._now()
        for index, snapshot in enumerate(snapshots):
            if now - snapshot.checked_at <= self._stale_after:
                continue
            snapshots[index] = CapabilityHealthSnapshot(
                contribution_id=snapshot.contribution_id,
                capability_id=snapshot.capability_id,
                status="unknown",
                diagnostic_code="snapshot_stale",
                checked_at=snapshot.checked_at,
                expires_at=snapshot.expires_at,
                extension_generation=snapshot.extension_generation,
                last_healthy_at=snapshot.last_healthy_at,
                correlation_id=snapshot.correlation_id,
            )
        return tuple(snapshots)

    async def admission_readiness(
        self,
        *,
        expected_generation: int,
    ) -> CapabilityReadinessSnapshot:
        """Prove fresh health for every operator-required capability."""

        required = {entry.capability_id for entry in self._manifest.capabilities if entry.operator_required}
        snapshots = list(await self.health_for(required, refresh=True))
        now = self._now()
        ready = {item.capability_id for item in snapshots} == required
        for index, snapshot in enumerate(snapshots):
            if snapshot.extension_generation != expected_generation:
                snapshot = replace(
                    snapshot,
                    status="unknown",
                    diagnostic_code="generation_mismatch",
                )
            elif snapshot.status == "healthy" and (snapshot.last_healthy_at is None or now - snapshot.last_healthy_at > self._admission_max_age):
                snapshot = replace(
                    snapshot,
                    status="unknown",
                    diagnostic_code="snapshot_stale",
                )
            snapshots[index] = snapshot
            if snapshot.status != "healthy":
                ready = False
        return CapabilityReadinessSnapshot(
            status="ready" if ready else "not_ready",
            health=tuple(snapshots),
        )

    async def readiness(
        self,
        *,
        refresh: bool = True,
        lifecycle_ready: bool = True,
    ) -> CapabilityReadinessSnapshot:
        snapshots = list(await self.health(refresh=refresh))
        required = {entry.capability_id for entry in self._manifest.capabilities if entry.operator_required}
        now = self._now()
        ready = lifecycle_ready
        for index, snapshot in enumerate(snapshots):
            if snapshot.capability_id not in required:
                continue
            if now - snapshot.checked_at > self._stale_after:
                snapshot = replace(
                    snapshot,
                    status="unknown",
                    diagnostic_code="snapshot_stale",
                )
                snapshots[index] = snapshot
            if snapshot.status != "healthy":
                ready = False
        return CapabilityReadinessSnapshot(
            status="ready" if ready else "not_ready",
            health=tuple(snapshots),
        )


__all__ = [
    "CapabilityManifest",
    "CapabilityManifestEntry",
    "CapabilityHealthMonitor",
    "CapabilityHealthSnapshot",
    "CapabilityPluginManifestEntry",
    "CapabilityReadinessSnapshot",
    "REQUIRED_CAPABILITY_ID_OWNERS",
    "REQUIRED_CAPABILITY_KIND_OWNERS",
    "RequiredCapabilityRoutes",
    "RequiredCapabilityRoutingError",
    "build_capability_manifest",
    "capability_health_to_dict",
    "capability_manifest_to_dict",
    "route_required_capabilities",
]
