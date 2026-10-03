"""Library guidance backed by an explicit, verified sandbox image declaration.

``docker/sandbox/Dockerfile`` asserts the HartMesh imports at build time. Other
environments make no such promise. Rendering reads the initialized provider's
startup snapshot when available, without starting a provider or probing imports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from deerflow.sandbox.sandbox_provider import get_initialized_sandbox_provider

if TYPE_CHECKING:
    from deerflow.config.sandbox_config import SandboxConfig

__all__ = ["GUARANTEED_IMPORTS", "preinstalled_libraries_section"]

#: Import names the sandbox image asserts at build time, in the Dockerfile's
#: own order. Import names, not distribution names -- what a model would type.
GUARANTEED_IMPORTS: tuple[str, ...] = (
    "docx",
    "duckdb",
    "xlrd",
    "weasyprint",
    "xlsxwriter",
    "matplotlib",
    "pandas",
    "openpyxl",
)

#: The distribution behind an import name, where they differ and the difference
#: is the kind of thing a model gets wrong.
_DISTRIBUTION_NOTES = {"docx": "python-docx"}


def preinstalled_libraries_section(sandbox_config: SandboxConfig | None = None, *, bash_available: bool = True) -> str:
    """Only promise imports for the configured or already initialized profile."""
    if not bash_available:
        return ""
    provider = get_initialized_sandbox_provider()
    profile = getattr(provider, "python_libraries_profile", None) if provider is not None else getattr(sandbox_config, "python_libraries_profile", None)
    if profile != "hartmesh":
        return "- Python library availability depends on the configured environment. Use imports needed for the task and handle a missing dependency; no preinstalled library set is guaranteed."
    names = ", ".join(f"{name} ({_DISTRIBUTION_NOTES[name]})" if name in _DISTRIBUTION_NOTES else name for name in GUARANTEED_IMPORTS)
    return (
        f"- Always importable in the sandbox: {names}. "
        "Do not run commands to check whether these exist — the image guarantees them. "
        "For anything else, import it and handle the failure if it is absent; a probe costs a sandbox round trip and a model call to read its answer."
    )
