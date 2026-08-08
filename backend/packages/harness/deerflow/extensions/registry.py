"""Registration-phase registry and its immutable runtime product.

Extensions only ever see the write-only public ``ExtensionRegistry`` contract.
The concrete host type additionally owns attribution, rollback, and immutable
runtime projection.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Literal

from deerflow_extension_api import (
    AuthorizationProvider,
    AuthorizationProviderFactory,
    CapabilityHealthProbe,
    ExtensionData,
    InvocationConstraintsProvider,
    InvocationConstraintsProviderFactory,
    InvocationConstraintsProviderV2,
    McpInterceptor,
    McpInterceptorDescriptor,
    MiddlewareContributor,
    OriginContributor,
    OriginContributorFactory,
    RunContextContributor,
    RunContextContributorFactory,
)
from deerflow_extension_api import ExtensionRegistry as ExtensionRegistryContract

_Entry = tuple[str, Any]


class DuplicateAuthorizationProviderFactoryError(ValueError):
    """Raised when more than one authoritative provider factory is registered."""


class DuplicateInvocationConstraintsProviderFactoryError(ValueError):
    """Raised when more than one restrictive constraints factory is registered."""


@dataclass(frozen=True)
class RegisteredAuthorizationProviderFactory:
    """Host-owned provider registration with loader-stamped provenance."""

    contribution_id: str
    capability_api_version: str
    factory: Callable[[], AuthorizationProvider]
    kind: Literal["authorization_provider"]
    source: str
    package_name: str | None
    package_version: str | None
    health_probe: CapabilityHealthProbe | None = None


@dataclass(frozen=True)
class RegisteredOriginContributorFactory:
    contribution_id: str
    capability_api_version: str
    factory: Callable[[], OriginContributor]
    kind: Literal["origin_contributor"]
    source: str
    package_name: str | None
    package_version: str | None
    health_probe: CapabilityHealthProbe | None = None


@dataclass(frozen=True)
class RegisteredRunContextContributorFactory:
    contribution_id: str
    capability_api_version: str
    factory: Callable[[], RunContextContributor]
    kind: Literal["run_context_contributor"]
    source: str
    package_name: str | None
    package_version: str | None
    health_probe: CapabilityHealthProbe | None = None


@dataclass(frozen=True)
class RegisteredInvocationConstraintsProviderFactory:
    contribution_id: str
    capability_api_version: str
    factory: Callable[[], InvocationConstraintsProvider | InvocationConstraintsProviderV2]
    kind: Literal["invocation_constraints"]
    source: str
    package_name: str | None
    package_version: str | None
    health_probe: CapabilityHealthProbe | None = None


@dataclass(frozen=True)
class RegisteredMcpInterceptorDescriptor:
    contribution_id: str
    capability_api_version: str
    factory: Callable[[], McpInterceptor]
    kind: Literal["mcp_interceptor"]
    source: str
    package_name: str | None
    package_version: str | None
    health_probe: CapabilityHealthProbe | None = None


@dataclass(frozen=True)
class LoadedPluginRegistration:
    """Loader-owned provenance for one successfully installed plugin spec."""

    package_name: str | None
    package_version: str | None
    required: bool


@dataclass(frozen=True)
class _RegistryMark:
    loaded_plugin_count: int
    middleware_count: int
    authorization_provider_count: int
    origin_contributor_count: int
    run_context_contributor_count: int
    invocation_constraints_provider_count: int
    mcp_interceptor_count: int


@dataclass(frozen=True)
class LoadedExtensions:
    """Immutable view consumed at runtime.

    Every entry carries its source string so diagnostics, provenance and
    ordering errors can name the extension responsible.
    """

    app_store: ExtensionData
    generation: int = 0
    loaded_plugins: tuple[LoadedPluginRegistration, ...] = ()
    middleware_contributors: tuple[tuple[str, MiddlewareContributor], ...] = ()
    authorization_provider_factories: tuple[RegisteredAuthorizationProviderFactory, ...] = ()
    origin_contributor_factories: tuple[RegisteredOriginContributorFactory, ...] = ()
    run_context_contributor_factories: tuple[RegisteredRunContextContributorFactory, ...] = ()
    invocation_constraints_provider_factories: tuple[RegisteredInvocationConstraintsProviderFactory, ...] = ()
    invocation_constraints_provider_conflict: bool = False
    mcp_interceptor_descriptors: tuple[RegisteredMcpInterceptorDescriptor, ...] = ()
    mcp_interceptor_conflicts: frozenset[str] = frozenset()

    # Precomputed attributes, not methods: hook sites read one attribute to
    # short-circuit, so the zero-extension path constructs nothing.
    has_middleware_contributors: bool = False
    needs_task_store: bool = False

    @property
    def authorization_provider_factory(self) -> RegisteredAuthorizationProviderFactory | None:
        return self.authorization_provider_factories[0] if self.authorization_provider_factories else None

    @property
    def invocation_constraints_provider_factory(self) -> RegisteredInvocationConstraintsProviderFactory | None:
        return self.invocation_constraints_provider_factories[0] if self.invocation_constraints_provider_factories else None


class ExtensionRegistry(ExtensionRegistryContract):
    """Mutable, registration-phase only.

    Subclasses the public contract Protocol so the host implementation is
    type-checked against what extensions annotate; the host-only machinery
    below (attribution, discard, mark/rollback_to, build) stays out of the
    contract on purpose.
    """

    def __init__(self) -> None:
        self._loaded_plugins: list[LoadedPluginRegistration] = []
        self._middlewares: list[_Entry] = []
        self._authorization_providers: list[RegisteredAuthorizationProviderFactory] = []
        self._origin_contributors: list[RegisteredOriginContributorFactory] = []
        self._run_context_contributors: list[RegisteredRunContextContributorFactory] = []
        self._invocation_constraints_providers: list[RegisteredInvocationConstraintsProviderFactory] = []
        self._invocation_constraints_provider_conflict = False
        self._mcp_interceptors: list[RegisteredMcpInterceptorDescriptor] = []
        self._mcp_interceptor_conflicts: set[str] = set()
        self._current_source: str | None = None
        self._current_package_name: str | None = None
        self._current_package_version: str | None = None

    @contextmanager
    def attributed_to(
        self,
        source: str,
        *,
        package_name: str | None = None,
        package_version: str | None = None,
    ) -> Iterator[None]:
        """Attribute everything registered inside the block to ``source``."""
        previous = self._current_source
        previous_package_name = self._current_package_name
        previous_package_version = self._current_package_version
        self._current_source = source
        self._current_package_name = package_name
        self._current_package_version = package_version
        try:
            yield
        finally:
            self._current_source = previous
            self._current_package_name = previous_package_name
            self._current_package_version = previous_package_version

    def _source(self) -> str:
        if self._current_source is None:
            raise RuntimeError("registration must happen inside ExtensionRegistry.attributed_to(...)")
        return self._current_source

    def middlewares(self, contributor: MiddlewareContributor) -> None:
        self._middlewares.append((self._source(), contributor))

    def record_loaded_plugin(
        self,
        *,
        package_name: str | None,
        package_version: str | None,
        required: bool,
    ) -> None:
        """Record successful install provenance after install() returns."""

        self._loaded_plugins.append(
            LoadedPluginRegistration(
                package_name=package_name,
                package_version=package_version,
                required=required,
            )
        )

    def authorization_provider(self, contribution: AuthorizationProviderFactory) -> None:
        if not isinstance(contribution, AuthorizationProviderFactory):
            raise TypeError("authorization_provider requires AuthorizationProviderFactory")
        if self._authorization_providers:
            existing = self._authorization_providers[0]
            raise DuplicateAuthorizationProviderFactoryError(f"authorization provider factory already registered by {existing.source} ({existing.contribution_id}); duplicate {contribution.contribution_id} is not allowed")
        self._authorization_providers.append(
            RegisteredAuthorizationProviderFactory(
                contribution_id=contribution.contribution_id,
                capability_api_version=contribution.capability_api_version,
                factory=contribution.factory,
                kind=contribution.kind,
                source=self._source(),
                package_name=self._current_package_name,
                package_version=self._current_package_version,
                health_probe=contribution.health_probe,
            )
        )

    def origin_contributor(self, contribution: OriginContributorFactory) -> None:
        if not isinstance(contribution, OriginContributorFactory):
            raise TypeError("origin_contributor requires OriginContributorFactory")
        if any(item.contribution_id == contribution.contribution_id for item in self._origin_contributors):
            raise ValueError(f"duplicate origin contributor contribution_id {contribution.contribution_id!r}")
        self._origin_contributors.append(
            RegisteredOriginContributorFactory(
                contribution_id=contribution.contribution_id,
                capability_api_version=contribution.capability_api_version,
                factory=contribution.factory,
                kind=contribution.kind,
                source=self._source(),
                package_name=self._current_package_name,
                package_version=self._current_package_version,
                health_probe=contribution.health_probe,
            )
        )

    def run_context_contributor(self, contribution: RunContextContributorFactory) -> None:
        if not isinstance(contribution, RunContextContributorFactory):
            raise TypeError("run_context_contributor requires RunContextContributorFactory")
        if any(item.contribution_id == contribution.contribution_id for item in self._run_context_contributors):
            raise ValueError(f"duplicate run-context contributor contribution_id {contribution.contribution_id!r}")
        self._run_context_contributors.append(
            RegisteredRunContextContributorFactory(
                contribution_id=contribution.contribution_id,
                capability_api_version=contribution.capability_api_version,
                factory=contribution.factory,
                kind=contribution.kind,
                source=self._source(),
                package_name=self._current_package_name,
                package_version=self._current_package_version,
                health_probe=contribution.health_probe,
            )
        )

    def invocation_constraints(self, contribution: InvocationConstraintsProviderFactory) -> None:
        if not isinstance(contribution, InvocationConstraintsProviderFactory):
            raise TypeError("invocation_constraints requires InvocationConstraintsProviderFactory")
        if self._invocation_constraints_providers:
            existing = self._invocation_constraints_providers[0]
            self._invocation_constraints_provider_conflict = True
            raise DuplicateInvocationConstraintsProviderFactoryError(f"invocation constraints provider factory already registered by {existing.source} ({existing.contribution_id}); duplicate {contribution.contribution_id} is not allowed")
        self._invocation_constraints_providers.append(
            RegisteredInvocationConstraintsProviderFactory(
                contribution_id=contribution.contribution_id,
                capability_api_version=contribution.capability_api_version,
                factory=contribution.factory,
                kind=contribution.kind,
                source=self._source(),
                package_name=self._current_package_name,
                package_version=self._current_package_version,
                health_probe=contribution.health_probe,
            )
        )

    def mcp_interceptor(self, contribution: McpInterceptorDescriptor) -> None:
        if not isinstance(contribution, McpInterceptorDescriptor):
            raise TypeError("mcp_interceptor requires McpInterceptorDescriptor")
        if any(item.contribution_id == contribution.contribution_id for item in self._mcp_interceptors):
            # Keep this host-owned ambiguity marker even when the loader rolls
            # back the failing plugin's positional registrations. Otherwise an
            # earlier descriptor could silently satisfy a required capability
            # after a duplicate trusted declaration was rejected.
            self._mcp_interceptor_conflicts.add(contribution.contribution_id)
            raise ValueError(f"duplicate MCP interceptor contribution_id {contribution.contribution_id!r}")
        self._mcp_interceptors.append(
            RegisteredMcpInterceptorDescriptor(
                contribution_id=contribution.contribution_id,
                capability_api_version=contribution.capability_api_version,
                factory=contribution.factory,
                kind=contribution.kind,
                source=self._source(),
                package_name=self._current_package_name,
                package_version=self._current_package_version,
                health_probe=contribution.health_probe,
            )
        )

    def discard(self, source: str) -> None:
        """Remove every entry registered by ``source``.

        Called when install() raises partway through. A half-registered
        extension is more dangerous than an absent one because the data it
        produces looks complete.

        Note: this matches by source string, so it is unsafe when two specs
        share the same ``use`` with different config — it would remove a
        different, successfully-installed instance's entries too. Callers
        that process one install() at a time should prefer
        ``mark()``/``rollback_to()`` instead.
        """
        self._middlewares[:] = [entry for entry in self._middlewares if entry[0] != source]
        self._authorization_providers[:] = [entry for entry in self._authorization_providers if entry.source != source]
        self._origin_contributors[:] = [entry for entry in self._origin_contributors if entry.source != source]
        self._run_context_contributors[:] = [entry for entry in self._run_context_contributors if entry.source != source]
        self._invocation_constraints_providers[:] = [entry for entry in self._invocation_constraints_providers if entry.source != source]
        self._mcp_interceptors[:] = [entry for entry in self._mcp_interceptors if entry.source != source]

    def mark(self) -> _RegistryMark:
        """Snapshot bucket lengths so one install() can be undone positionally."""
        return _RegistryMark(
            len(self._loaded_plugins),
            len(self._middlewares),
            len(self._authorization_providers),
            len(self._origin_contributors),
            len(self._run_context_contributors),
            len(self._invocation_constraints_providers),
            len(self._mcp_interceptors),
        )

    def rollback_to(self, mark: _RegistryMark) -> None:
        """Undo every registration made since ``mark``.

        Positional rather than source-keyed: two specs may legitimately share
        a ``use`` string with different config, and deleting by source would
        take the other instance's successful registrations with it.
        """
        del self._loaded_plugins[mark.loaded_plugin_count :]
        del self._middlewares[mark.middleware_count :]
        del self._authorization_providers[mark.authorization_provider_count :]
        del self._origin_contributors[mark.origin_contributor_count :]
        del self._run_context_contributors[mark.run_context_contributor_count :]
        del self._invocation_constraints_providers[mark.invocation_constraints_provider_count :]
        del self._mcp_interceptors[mark.mcp_interceptor_count :]

    def build(self, *, generation: int = 0) -> LoadedExtensions:
        return LoadedExtensions(
            app_store=ExtensionData("app"),
            generation=generation,
            loaded_plugins=tuple(self._loaded_plugins),
            middleware_contributors=tuple(self._middlewares),
            authorization_provider_factories=tuple(self._authorization_providers),
            origin_contributor_factories=tuple(self._origin_contributors),
            run_context_contributor_factories=tuple(self._run_context_contributors),
            invocation_constraints_provider_factories=tuple(self._invocation_constraints_providers),
            invocation_constraints_provider_conflict=self._invocation_constraints_provider_conflict,
            mcp_interceptor_descriptors=tuple(self._mcp_interceptors),
            mcp_interceptor_conflicts=frozenset(self._mcp_interceptor_conflicts),
            has_middleware_contributors=bool(self._middlewares),
            needs_task_store=bool(self._middlewares),
        )


#: Shared empty instance for hosts that load no extensions.
EMPTY_EXTENSIONS = ExtensionRegistry().build()
