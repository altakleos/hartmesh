"""The compose profile's own renderer and catalog, as the Gateway reaches them.

Provider keys are managed in the product only where the deployment's
``config.yaml`` is rendered from a provider catalog -- the compose profile,
whose ``compose.yaml`` names its directory in ``HARTMESH_PROFILE_DIR``.
Everywhere else (the chart, ``make dev``, a hand-written ``config.yaml``)
there is no catalog to manage keys for, and nothing here runs.

The renderer is the profile's ``gateway/render_config.py``, loaded from the
profile directory rather than reimplemented: it alone decides which catalog
fragment a variable includes and what a render refuses, so a key set in the
product selects exactly what the same key in the environment would.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Literal

PROFILE_DIR_ENV = "HARTMESH_PROFILE_DIR"
_MODULE_NAME = "hartmesh_profile_render_config"
_ORDER_PREFIX = re.compile(r"\A\d+-")


class RenderRefused(ValueError):
    """The profile's renderer refused; its message names the cause, never a value."""


@dataclass(frozen=True)
class CatalogProvider:
    # The fragment's file name without its order prefix: `providers/models/20-anthropic.yaml` is `anthropic`.
    id: str
    variable: str
    kind: Literal["models", "tools"]


def _load_renderer(path: Path) -> ModuleType:
    module = sys.modules.get(_MODULE_NAME)
    if module is not None and getattr(module, "__file__", None) == str(path):
        return module
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, path)
    if spec is None or spec.loader is None:
        raise RenderRefused(f"the profile renderer at {path} cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


class ProfileRenderer:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self._renderer = _load_renderer(directory / "gateway" / "render_config.py")
        self._catalog = self._renderer.load_catalog(directory / "providers")
        self._template = (directory / "config.yaml").read_text(encoding="utf-8")
        providers: list[CatalogProvider] = []
        for fragment in self._catalog:
            stem = Path(fragment.source).stem
            providers.append(CatalogProvider(id=_ORDER_PREFIX.sub("", stem), variable=fragment.env, kind="models" if fragment.models else "tools"))
        self.providers: tuple[CatalogProvider, ...] = tuple(providers)

    @classmethod
    def from_environ(cls, environ: Mapping[str, str]) -> ProfileRenderer | None:
        raw = environ.get(PROFILE_DIR_ENV, "").strip()
        return cls(Path(raw)) if raw else None

    def operator_models_file(self, environ: Mapping[str, str]) -> str | None:
        """The operator model file the environment names, if any."""
        return environ.get(self._renderer.MODELS_ENV, "").strip() or None

    @property
    def operator_models_variable(self) -> str:
        return self._renderer.MODELS_ENV

    def render(self, environ: Mapping[str, str]) -> str:
        """The rendered config.yaml text for *environ*, exactly as gateway/run.sh would write it."""
        try:
            rendered, _ = self._renderer.render_text(self._template, self._catalog, environ)
        except self._renderer.RenderError as exc:
            raise RenderRefused(str(exc)) from None
        return rendered
