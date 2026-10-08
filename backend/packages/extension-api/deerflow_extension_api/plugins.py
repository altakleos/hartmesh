"""Unified, optional browser/backend contributions from one trusted package.

Backend actions run in the Gateway process. The host supplies current settings
and an authenticated principal for each admitted call; this is not a sandbox.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from deerflow_extension_api.auth import ExtensionPrincipal
from deerflow_extension_api.settings import FrontendBinding, SettingsContribution, SettingsField, SettingValue
from deerflow_extension_api.storage import ResourceStorage, StorageActor, StorageController, StorageResource


@dataclass(frozen=True)
class ActionContext:
    principal: ExtensionPrincipal | None
    settings: Mapping[str, SettingValue]
    actor: StorageActor | None = field(default=None, kw_only=True)
    storage: ResourceStorage | None = field(default=None, kw_only=True)
    resource: StorageResource | None = field(default=None, kw_only=True)


@dataclass(frozen=True)
class BackendAction:
    name: str
    handler: Callable[[Mapping[str, Any], ActionContext], Awaitable[Any]]
    purpose: Literal["business", "management"] = "business"


@dataclass(frozen=True)
class ToolContext(ActionContext):
    """Host-bound identity for a model tool call.

    Legacy plugins use principal.user_id. Contract v4 uses actor and the
    host-bound storage capability; nonhuman callers have no human principal.
    """

    thread_id: str | None


@dataclass(frozen=True)
class ModelTool:
    name: str
    description: str
    input_schema: Mapping[str, Any]
    handler: Callable[[Mapping[str, Any], ToolContext], Awaitable[Any]]
    group: str = "extensions"
    purpose: Literal["business", "management"] = "business"


@dataclass(frozen=True)
class BrowserModule:
    """Self-contained browser module; use BrowserAssets for relative resources."""

    module: str
    code: str
    public_fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "public_fields", tuple(self.public_fields))


@dataclass(frozen=True)
class BrowserAssets:
    """Versioned manifest and static files inside a trusted installed package.

    The host validates and snapshots the allowlisted files during registration.
    Relative paths in the manifest are resolved against root, never a request.
    """

    module: str
    root: str | Path
    manifest: str = "ui_manifest.json"
    public_fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "public_fields", tuple(self.public_fields))


@dataclass(frozen=True)
class ArtifactPresentation:
    """One generic preview capability owned by an installed trusted plugin.

    The host authorizes and snapshots the source before invoking ``project``
    off-loop. The callback receives immutable bytes, never a caller path or
    filesystem handle. Browser handlers use the same id in the plugin module.
    Compatibility query names are installed declarations, never code selectors.
    """

    id: str
    suffixes: tuple[str, ...]
    source_max_bytes: int = 16 * 1024 * 1024
    preview_max_bytes: int = 1024 * 1024
    project: Callable[[bytes], bytes] | None = None
    projection_marker: str | None = None
    compat_queries: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "suffixes", tuple(self.suffixes))
        object.__setattr__(self, "compat_queries", tuple(self.compat_queries))


@dataclass(frozen=True)
class PluginContribution:
    """One identity, one enabled switch, optional settings and implementations.

    The host owns the boolean ``enabled`` field. Other fields are non-secret
    settings, private to the backend unless explicitly projected by its browser declaration.
    Supply at least one browser module, backend action, or model tool. Backend implementations
    are installed through the existing operator-controlled Python loader.
    """

    namespace: str
    title: str
    description: str = ""
    enabled: bool = False
    fields: tuple[SettingsField, ...] = ()
    frontend: BrowserModule | BrowserAssets | None = None
    backend: tuple[BackendAction, ...] = ()
    api_version: int = 1
    tools: tuple[ModelTool, ...] = ()
    artifacts: tuple[ArtifactPresentation, ...] = ()
    storage_api_version: int | None = None
    actor_kinds: tuple[str, ...] = ("human",)
    storage_controller: StorageController | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "fields", tuple(self.fields))
        object.__setattr__(self, "backend", tuple(self.backend))
        object.__setattr__(self, "tools", tuple(self.tools))
        object.__setattr__(self, "artifacts", tuple(self.artifacts))
        object.__setattr__(self, "actor_kinds", tuple(self.actor_kinds))

    def settings_contribution(self) -> SettingsContribution:
        return SettingsContribution(
            namespace=self.namespace,
            title=self.title,
            description=self.description,
            fields=(SettingsField("enabled", "启用 / Enabled", "boolean", self.enabled), *self.fields),
            applies="request-and-page-load" if (self.backend or self.tools) and self.frontend else "next-request" if self.backend or self.tools else "page-load",
            frontend=FrontendBinding(self.frontend.module, ("enabled", *self.frontend.public_fields)) if self.frontend else None,
        )
