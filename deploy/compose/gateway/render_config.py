#!/usr/bin/env python3
"""Render the tenant's effective config.yaml for the HartMesh compose profile.

Run by gateway/run.sh at every Gateway start with the backend's own Python
(PyYAML is a harness dependency). Inputs:

* ``--template``: deploy/compose/config.yaml, the static part of the config.
* ``--catalog``: deploy/compose/providers, one YAML fragment per provider key
  (``env: NAME`` plus ``models:`` and/or ``tools:``).
* the process environment: the tenant ``.env`` plus the Gateway's own
  variables; only variable *names* are inspected, never logged.

A fragment is included only when its ``env`` variable is present and
non-empty. Fragments write ``api_key: $NAME`` (the reference, never the value)
so the Gateway still expands the secret itself and nothing secret lands on
disk. Fragment ``models`` are appended in catalog file order; fragment
``tools`` replace the template entry with the same name, the first fragment
in file order winning, so keyless defaults survive when no key is present.

``sandbox.network`` is selected by ``SANDBOX_EGRESS``: ``allowlist`` (or
absent) keeps the template block, ``open`` reduces it to ``mode: open``, any
other value refuses to render. The rendered document is checked so that no
``$NAME`` reference remains for a variable that is absent or empty.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

EGRESS_ENV = "SANDBOX_EGRESS"
EGRESS_MODES = ("allowlist", "open")
_VARIABLE = re.compile(r"\A\$([A-Za-z_][A-Za-z0-9_]*)\Z")


class RenderError(ValueError):
    """A refusal to render; the message names the cause, never a value."""


@dataclass(frozen=True)
class Fragment:
    """One provider catalog fragment."""

    source: str
    env: str
    models: tuple[Mapping[str, Any], ...]
    tools: tuple[Mapping[str, Any], ...]


def _mapping(value: object, what: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RenderError(f"{what} must be a mapping")
    return value


def _entries(value: object, what: str) -> tuple[Mapping[str, Any], ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or any(not isinstance(item, Mapping) or not isinstance(item.get("name"), str) for item in value):
        raise RenderError(f"{what} must be a list of named entries")
    return tuple(value)


def load_fragment(path: Path, *, root: Path) -> Fragment:
    """Parse one catalog fragment, refusing anything but the documented shape."""

    source = path.relative_to(root).as_posix()
    document = _mapping(yaml.safe_load(path.read_text(encoding="utf-8")), f"catalog fragment {source}")
    unknown = set(document) - {"env", "models", "tools"}
    if unknown:
        raise RenderError(f"catalog fragment {source} has unknown keys: {sorted(unknown)}")
    env = document.get("env")
    if not isinstance(env, str) or _VARIABLE.fullmatch(f"${env}") is None:
        raise RenderError(f"catalog fragment {source} needs an environment variable name in `env`")
    models = _entries(document.get("models"), f"catalog fragment {source} models")
    tools = _entries(document.get("tools"), f"catalog fragment {source} tools")
    if not models and not tools:
        raise RenderError(f"catalog fragment {source} declares neither models nor tools")
    return Fragment(source=source, env=env, models=models, tools=tools)


def load_catalog(root: Path) -> tuple[Fragment, ...]:
    """Load every ``*.yaml`` fragment under ``root`` in sorted path order."""

    paths = sorted(path for path in root.rglob("*.yaml") if path.is_file())
    return tuple(load_fragment(path, root=root) for path in paths)


def _present(environ: Mapping[str, str], name: str) -> bool:
    return bool(environ.get(name, "").strip())


def _references(value: object, found: set[str]) -> None:
    if isinstance(value, str):
        match = _VARIABLE.fullmatch(value)
        if match is not None:
            found.add(match.group(1))
    elif isinstance(value, Mapping):
        for item in value.values():
            _references(item, found)
    elif isinstance(value, list):
        for item in value:
            _references(item, found)


def select_egress(environ: Mapping[str, str]) -> str:
    """Return the sandbox egress mode the contract selected."""

    raw = environ.get(EGRESS_ENV, "").strip()
    mode = raw or "allowlist"
    if mode not in EGRESS_MODES:
        raise RenderError(f"{EGRESS_ENV} must be one of {', '.join(EGRESS_MODES)} (or absent)")
    return mode


def render(
    template: Mapping[str, Any],
    fragments: tuple[Fragment, ...],
    environ: Mapping[str, str],
) -> tuple[dict[str, Any], tuple[Fragment, ...]]:
    """Return the rendered document and the fragments it included."""

    document: dict[str, Any] = dict(template)

    template_models = document.get("models") or []
    if not isinstance(template_models, list):
        raise RenderError("template `models` must be a list")
    template_tools = _entries(document.get("tools"), "template tools")

    included = tuple(fragment for fragment in fragments if _present(environ, fragment.env))

    models = list(template_models)
    for fragment in included:
        models.extend(dict(model) for model in fragment.models)
    names = [model.get("name") for model in models]
    if len(names) != len(set(names)):
        raise RenderError("rendered models carry a duplicate name")
    document["models"] = models

    tools: dict[str, Mapping[str, Any]] = {str(tool["name"]): dict(tool) for tool in template_tools}
    replaced: set[str] = set()
    for fragment in included:
        for tool in fragment.tools:
            name = str(tool["name"])
            if name in replaced:
                continue
            tools[name] = dict(tool)
            replaced.add(name)
    document["tools"] = list(tools.values())

    sandbox = dict(_mapping(document.get("sandbox"), "template `sandbox`"))
    if sandbox.get("provisioner_url"):
        raise RenderError("template `sandbox.provisioner_url` must be absent: the profile uses the local Docker backend")
    network = dict(_mapping(sandbox.get("network"), "template `sandbox.network`"))
    if network.get("mode") != "allowlist":
        raise RenderError("template `sandbox.network.mode` must be allowlist; SANDBOX_EGRESS selects open at render time")
    mode = select_egress(environ)
    sandbox["network"] = {"mode": "open"} if mode == "open" else network
    document["sandbox"] = sandbox

    referenced: set[str] = set()
    _references(document, referenced)
    missing = sorted(name for name in referenced if not _present(environ, name))
    if missing:
        raise RenderError(f"rendered config references unset environment variables: {missing}")
    return document, included


def render_text(template_text: str, fragments: tuple[Fragment, ...], environ: Mapping[str, str]) -> tuple[str, tuple[Fragment, ...]]:
    """Render from template text to YAML text."""

    template = _mapping(yaml.safe_load(template_text), "template")
    document, included = render(template, fragments, environ)
    header = "# Rendered by the HartMesh compose profile at Gateway start. Do not edit:\n# the source is /opt/hartmesh/config.yaml plus /opt/hartmesh/providers.\n"
    return header + yaml.safe_dump(document, sort_keys=False, allow_unicode=True, width=200), included


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        fragments = load_catalog(args.catalog)
        rendered, included = render_text(args.template.read_text(encoding="utf-8"), fragments, os.environ)
    except (RenderError, OSError, yaml.YAMLError) as exc:
        print(f"render_config: refusing to render: {exc}", file=sys.stderr)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=args.output.parent, prefix=".config.yaml.", delete=False)
    with handle:
        handle.write(rendered)
    os.chmod(handle.name, 0o640)
    os.replace(handle.name, args.output)
    providers = ", ".join(sorted({fragment.env for fragment in included})) or "none"
    print(f"render_config: wrote {args.output} (egress={select_egress(os.environ)}; provider keys found: {providers})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
