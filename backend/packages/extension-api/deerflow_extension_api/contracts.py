"""The extension contracts and their data types.

Compatibility rules enforced throughout this module:
  * every Protocol method carries a default implementation, so adding a method
    later stays additive for already-released extensions;
  * every optional dataclass field carries a default, so adding a field stays
    additive.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

from deerflow_extension_api.state import ExtensionData

if TYPE_CHECKING:  # pragma: no cover - typing only
    from deerflow_extension_api.authorization import AuthorizationProviderFactory
    from deerflow_extension_api.constraints import InvocationConstraintsProviderFactory
    from deerflow_extension_api.contributors import OriginContributorFactory, RunContextContributorFactory
    from deerflow_extension_api.placement import AgentBuildContext, MiddlewarePlacement

F = TypeVar("F", bound=Callable[..., Any])


# --- Host projections -------------------------------------------------------


@dataclass(frozen=True)
class HostPolicySnapshot:
    """The limits the host actually enforces, projected for extensions.

    A narrow projection instead of the host's AppConfig: exposing AppConfig
    would pin every extension to the harness release cadence. Every field has
    a default so widening this stays additive.
    """

    token_budget_enabled: bool = False
    max_input_tokens: int | None = None
    max_output_tokens: int | None = None
    max_total_tokens: int | None = None
    budget_warn_fraction: float | None = None
    budget_hard_fraction: float | None = None
    max_subagents_per_run: int | None = None


# --- Middleware -------------------------------------------------------------


class MiddlewareContributor(Protocol):
    def contribute_middlewares(
        self,
        app_store: ExtensionData,
        ctx: AgentBuildContext,
    ) -> Sequence[MiddlewarePlacement]:
        return ()


# --- Registration surface ---------------------------------------------------


@runtime_checkable
class ExtensionRegistry(Protocol):
    """The write-only registration surface handed to ``install()``.

    Structural and minimal on purpose. It exposes typed middleware and
    authorization-provider contributions; later capabilities add their own
    defaulted methods rather than an untyped generic registry. The host's
    concrete registry additionally carries host-only machinery (attribution,
    positional rollback, build) that is deliberately absent here.
    """

    def middlewares(self, contributor: MiddlewareContributor) -> None:
        return None

    def authorization_provider(self, contribution: AuthorizationProviderFactory) -> None:
        """Register the process's single authoritative authorization factory."""
        return None

    def origin_contributor(self, contribution: OriginContributorFactory) -> None:
        """Register one trusted Origin contributor factory."""
        return None

    def run_context_contributor(self, contribution: RunContextContributorFactory) -> None:
        """Register one trusted accepted-run-context contributor factory."""
        return None

    def invocation_constraints(self, contribution: InvocationConstraintsProviderFactory) -> None:
        """Register the process's single restrictive constraints factory."""
        return None


#: The install() entry point signature every extension exposes.
ExtensionInstall = Callable[[ExtensionRegistry, Mapping[str, Any]], None]


# --- Declaration decorator --------------------------------------------------


def extension(*, api: str, name: str | None = None) -> Callable[[F], F]:
    """Stamp an install function with the API version it was written against.

    Optional. pip's dependency resolution is the primary compatibility
    mechanism; this covers `--no-deps` installs and editable monorepo checkouts
    where versions can skew, and turns a deep AttributeError into an
    actionable startup diagnostic.
    """

    def _decorate(func: F) -> F:
        func.__deerflow_api__ = api  # type: ignore[attr-defined]
        func.__deerflow_name__ = name  # type: ignore[attr-defined]
        return func

    return _decorate
