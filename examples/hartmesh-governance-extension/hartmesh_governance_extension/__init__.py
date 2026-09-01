"""Public entry point for the standalone HartMesh governance template."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from deerflow_extension_api import ExtensionInstall, ExtensionRegistry, extension

from hartmesh_governance_extension.governance import register_governance

__all__ = ["install", "register_governance"]


@extension(api="0.13.0", name="hartmesh-governance")
def install(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    """Register the reference policy and audit contributions."""

    register_governance(registry, config)


_entry_point: ExtensionInstall = install
