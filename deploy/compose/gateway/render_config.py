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

``HARTMESH_MODELS_FILE`` is optional. Absent (or empty), everything above is
the whole story. Set, it names a YAML file on the tenant's own data disk --
mounted read-only at ``<HARTMESH_DATA_DIR>/operator`` -- carrying ``models:``
and nothing else, and that list becomes the whole rendered ``models:``
section: fragment models are no longer appended, whichever provider keys the
tenant carries.

Everything is validated before anything is written: the documented shape, the
Gateway's own ``ModelConfig`` on every rendered entry (its schema, not a second
copy of it, so what renders is what loads), an installed ``BaseChatModel``
class for operator entries, unique names, and credential fields that are
whole-string ``$NAME`` references the environment actually carries. Because the
output is replaced atomically, a refusal leaves the last valid rendered file in
place. ``--check`` runs the same render and writes nothing, which is how an
operator validates an edit while the Gateway still serves the previous one.

Diagnostics name the source, the entry's position, the field and the rule --
never a value, a source line or an environment content. An operator's paste or
a stray bracket can put a credential in any field, and a refusal is journalled.
"""

from __future__ import annotations

import argparse
import ipaddress
import os
import re
import sys
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml

EGRESS_ENV = "SANDBOX_EGRESS"
HOST_RESOLVER_VIEW = Path("/run/hartmesh-host-resolv.conf")
HOST_RESOLVER_DEFAULT = "/run/systemd/resolve/resolv.conf"
EGRESS_MODES = ("allowlist", "open")
MODELS_ENV = "HARTMESH_MODELS_FILE"
OPERATOR_DIRECTORY = "<HARTMESH_DATA_DIR>/operator"
# A field is credential-bearing when its name's last `_`/`-` segment is one of
# these. Suffix matching on segments, not substrings, is what keeps `max_tokens`
# and `budget_tokens` out of it while catching `api_key`, `gemini_api_key`,
# `azure_ad_token` and a bare `Authorization` header.
CREDENTIAL_SEGMENTS = frozenset({"key", "apikey", "token", "secret", "password", "credential", "credentials", "authorization"})
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


def _client_problem(use: str) -> str | None:
    """Return why ``use`` cannot name a client class, or None if it can.

    The resolver's own exceptions quote the path they were handed, and that
    path is a field an operator typed -- a paste can put a credential in it as
    easily as in ``api_key``. So the cause is classified here, from the shape
    of the failure rather than from its message, and the message is discarded.
    """

    module_path, separator, attribute = use.rpartition(":")
    if not separator or not module_path.strip() or not attribute.strip():
        return "must name a class as `module.path:ClassName`"
    try:
        import_module(module_path)
    except ModuleNotFoundError:
        return "names a module this release does not install"
    except Exception:  # noqa: BLE001 - an import can raise anything, and none of it may be printed
        return "names a module that failed to import"
    try:
        _resolve_chat_model_class(use)
    except Exception:  # noqa: BLE001 - same reason: the message quotes the path
        module = sys.modules.get(module_path)
        if module is not None and not hasattr(module, attribute):
            return "names a module that defines no such attribute"
        return "does not name a chat model client (it must be a BaseChatModel subclass)"
    return None


def _resolve_chat_model_class(use: str) -> None:
    """Refuse a client class this release cannot construct.

    Deliberately not applied to the bundled catalog: those fragments ship with
    the release, and importing every one of them at start would turn a provider
    package the image happens not to carry into a new start requirement for
    tenants who never selected that model. The operator file is where a typo or
    a class from a package this release does not install can appear, and where
    the alternative to refusing is a model that only fails on first use.

    Imported lazily, so a tenant without an operator file pays nothing.
    """

    from langchain.chat_models import BaseChatModel

    from deerflow.reflection.resolvers import resolve_class

    resolve_class(use, BaseChatModel)


def _is_credential_field(name: str) -> bool:
    return name.replace("-", "_").rsplit("_", 1)[-1].lower() in CREDENTIAL_SEGMENTS


def _positions(places: list[tuple[str, int]]) -> str:
    """Name colliding entries by file and position, one file at a time."""

    by_source: dict[str, list[int]] = {}
    for source, index in places:
        by_source.setdefault(source, []).append(index)
    return "; ".join(f"{source} {', '.join(f'models[{index}]' for index in indexes)}" for source, indexes in by_source.items())


def _anchor(path: str) -> str:
    """The structural part of a path: everything up to its last list index.

    ``models[0].default_headers.Authorization`` anchors on ``models[0]``. A key
    name is operator-typed content like any value, so a diagnostic points at
    the entry and leaves the operator to look inside it.
    """

    closed = path.rfind("]")
    return path[: closed + 1] if closed != -1 else "the document root"


def _credential_problems(value: object, path: str, problems: list[str]) -> None:
    """Collect the paths of credential fields that are not ``$NAME`` references.

    Only paths are collected. The contract this profile documents is that a
    secret exists in the tenant environment and nowhere else -- not in an
    operator file, not in the generated config.yaml, not in a refusal and not
    in the journal -- so a value that breaks the rule must not travel in the
    diagnostic that reports it either. A credential field is never descended
    into, for the same reason.
    """

    if isinstance(value, Mapping):
        for key, item in value.items():
            child = f"{path}.{key}" if path else str(key)
            if _is_credential_field(str(key)):
                # An absent credential is the documented way to configure a
                # client that needs none; null is the same statement in YAML.
                if item is not None and not (isinstance(item, str) and _VARIABLE.fullmatch(item)):
                    problems.append(_anchor(child))
                continue
            _credential_problems(item, child, problems)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _credential_problems(item, f"{path}[{index}]", problems)


def _schema_problem(error: Mapping[str, Any]) -> str:
    """One pydantic error as location plus rule, with the input left behind."""

    location = ".".join(str(part) for part in error.get("loc") or ()) or "(entry)"
    detail = str(error.get("msg") or "").strip()
    rejected = error.get("input")
    # Pydantic's own messages are generated from the rule and carry no input,
    # but a custom validator's message is an exception string. Rather than
    # guess which is which, drop any message that quotes what it rejected.
    for form in (rejected if isinstance(rejected, str) else None, repr(rejected)):
        if form and len(form) > 1 and form in detail:
            detail = ""
    category = str(error.get("type") or "invalid")
    return f"{location}: {category}" + (f" ({detail})" if detail else "")


def _validate_model_schema(entry: Mapping[str, Any], where: str) -> None:
    """Refuse a model the Gateway's own ``ModelConfig`` would reject.

    Reuses the backend's schema rather than restating it, because a second
    copy would drift and the whole point is that what renders is what loads.
    Deliberately not ``AppConfig.from_file``: that applies a dozen unrelated
    process-wide singletons (see tests/_config_singleton_guard.py), which a
    validation step has no business doing.
    """

    from pydantic import ValidationError

    from deerflow.config.model_config import ModelConfig

    try:
        ModelConfig.model_validate(dict(entry))
    except ValidationError as exc:
        problems = sorted({_schema_problem(error) for error in exc.errors()})
        raise RenderError(f"{where} is not a model the Gateway will load: {'; '.join(problems)}") from None
    except (TypeError, ValueError):
        raise RenderError(f"{where} is not a model the Gateway will load: the entry is not a mapping of fields") from None


def load_operator_models(environ: Mapping[str, str]) -> tuple[Mapping[str, Any], ...] | None:
    """Return the operator's authoritative model list, or None when unset.

    ``None`` (the key absent or empty) and ``()`` (an explicit ``models: []``)
    are different answers: the first keeps the bundled catalog, the second is a
    tenant the operator configured no models for. Every other outcome -- a path
    that does not exist, a file that does not parse, a document carrying
    anything but ``models:`` -- is a refusal, never a quiet fall back to the
    bundled defaults the operator was overriding.
    """

    raw = environ.get(MODELS_ENV, "").strip()
    if not raw:
        return None
    try:
        text = Path(raw).read_text(encoding="utf-8")
    except OSError as exc:
        raise RenderError(
            f"{MODELS_ENV} names {raw}, which cannot be read ({exc.strerror}). The file must be readable by uid 1000 under {OPERATOR_DIRECTORY}, which the profile mounts read-only; unset {MODELS_ENV} for the bundled catalog"
        ) from exc
    try:
        loaded = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        # The parser's message quotes the line it choked on, which is exactly
        # the line an operator may have just pasted a credential into. Report
        # where it is, not what it says, and break the exception chain so no
        # traceback can put the excerpt back.
        started = getattr(exc, "context_mark", None)
        stopped = getattr(exc, "problem_mark", None)
        # The context mark is where the construct the parser could not finish
        # began, which is the line the operator has to look at; the problem
        # mark is often just the end of the file.
        where = started or stopped
        position = f" at line {where.line + 1}, column {where.column + 1}" if where is not None else ""
        if started is not None and stopped is not None and stopped.line != started.line:
            position += f" (the parser gave up at line {stopped.line + 1})"
        raise RenderError(f"operator model file {raw} is not valid YAML{position}; the parser's message is withheld because it quotes the source") from None
    if loaded is None:
        raise RenderError(f"operator model file {raw} is empty. Write `models: []` to configure no models, or unset {MODELS_ENV} to use the bundled catalog")
    document = _mapping(loaded, f"operator model file {raw}")
    unknown = set(document) - {"models"}
    if unknown:
        # Counted rather than named: a key is as much operator-typed content as
        # a value, and a stray paste lands at the top level readily enough.
        raise RenderError(f"operator model file {raw} declares {len(unknown)} top-level key(s) other than `models:`. It is a model list, not a second copy of config.yaml; every other setting stays with the profile")
    if document.get("models") is None:
        # `models:` with nothing after it is YAML null, which is neither of the
        # two documented answers; an operator who means "no models" writes the
        # empty list, and one mid-edit gets told so rather than silently
        # emptying the tenant's model list.
        raise RenderError(f"operator model file {raw} declares no `models:` list. Write `models: []` to configure no models")
    return _entries(document["models"], f"operator model file {raw} models")


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


def open_runsc_resolver_mount(environ: Mapping[str, str]) -> dict[str, object]:
    """Validate the host resolver view before handing its source to Docker.

    Docker's custom-bridge DNS is on host loopback, outside runsc's netstack.
    This profile supplies the VM's upstream resolver file as a read-only bind.
    The Gateway sees that exact source at HOST_RESOLVER_VIEW; the sandbox bind
    source is interpreted by the VM's Docker daemon, not by the Gateway.
    """
    source = environ.get("HARTMESH_SANDBOX_RESOLV_CONF", "").strip() or HOST_RESOLVER_DEFAULT
    if not Path(source).is_absolute() or ".." in Path(source).parts or any(c in source for c in ("\0", "\n", ",")):
        raise RenderError("sandbox resolver source must be an absolute Docker host file path")
    try:
        contents = HOST_RESOLVER_VIEW.read_text(encoding="utf-8")
    except OSError as exc:
        raise RenderError("sandbox resolver file is absent or unreadable at the Gateway's read-only host mount") from exc
    nameservers = 0
    for line in contents.splitlines():
        fields = re.split(r"[#;]", line, maxsplit=1)[0].split()
        if not fields or fields[0] != "nameserver":
            continue
        if len(fields) != 2:
            raise RenderError("sandbox resolver has a malformed nameserver line")
        try:
            address = ipaddress.ip_address(fields[1])
        except ValueError as exc:
            raise RenderError("sandbox resolver nameservers must be IP addresses") from exc
        if address.is_loopback or address.is_unspecified or address.is_link_local or address.is_multicast or address.is_reserved:
            raise RenderError("sandbox resolver needs upstream nameservers reachable outside loopback")
        nameservers += 1
    if not nameservers:
        raise RenderError("sandbox resolver declares no upstream nameserver")
    return {"host_path": source, "container_path": "/etc/resolv.conf", "read_only": True}


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

    operator_models = load_operator_models(environ)
    # (entry, the file it came from, its position in that file, whether the
    # client class is this deployment's to get wrong rather than the release's)
    sourced: list[tuple[dict[str, Any], str, int, bool]] = [(dict(model), "template", index, False) for index, model in enumerate(template_models)]
    if operator_models is None:
        for fragment in included:
            sourced.extend((dict(model), f"catalog fragment {fragment.source}", index, False) for index, model in enumerate(fragment.models))
    else:
        # Authoritative: a present provider key buys tools, never models.
        operator_source = f"operator model file {environ.get(MODELS_ENV, '').strip()}"
        sourced.extend((dict(model), operator_source, index, True) for index, model in enumerate(operator_models))

    for entry, source, index, check_client in sourced:
        where = f"{source} models[{index}]"
        _validate_model_schema(entry, where)
        if check_client:
            problem = _client_problem(str(entry["use"]))
            if problem is not None:
                raise RenderError(f"{where} field `use` {problem}")

    collisions: dict[str, list[tuple[str, int]]] = {}
    for entry, source, index, _ in sourced:
        collisions.setdefault(str(entry.get("name")), []).append((source, index))
    shared = sorted(places for places in collisions.values() if len(places) > 1)
    if shared:
        # By position, never by the name itself: `name` is a field an operator
        # types, and what these entries have in common is precisely its value.
        groups = "; ".join(_positions(places) for places in shared)
        raise RenderError(f"rendered models must carry distinct names, and these entries share one: {groups}")

    document["models"] = [entry for entry, _, _, _ in sourced]

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
    if mode == "open" and environ.get("DEER_FLOW_SANDBOX_RUNTIME") == "runsc":
        mounts = list(sandbox.get("mounts") or [])
        if any(mount.get("container_path") == "/etc/resolv.conf" for mount in mounts):
            raise RenderError("the profile owns the open-runsc resolver mount; remove the conflicting template mount")
        sandbox["mounts"] = [*mounts, open_runsc_resolver_mount(environ)]
    document["sandbox"] = sandbox

    problems: list[str] = []
    _credential_problems(document, "", problems)
    if problems:
        counted = ", ".join(f"{anchor}: {problems.count(anchor)}" for anchor in sorted(set(problems)))
        raise RenderError(
            f"credential fields must be environment references of the whole-string form $NAME, which the Gateway expands at start; a literal would be written into the generated config.yaml instead. Offending fields by entry: {counted}"
        )

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
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", action="store_true", help="validate and write nothing")
    args = parser.parse_args(argv)
    if args.output is None and not args.check:
        parser.error("--output is required unless --check is given")
    try:
        fragments = load_catalog(args.catalog)
        rendered, included = render_text(args.template.read_text(encoding="utf-8"), fragments, os.environ)
    except (RenderError, OSError, yaml.YAMLError) as exc:
        print(f"render_config: refusing to render: {exc}", file=sys.stderr)
        return 1
    providers = ", ".join(sorted({fragment.env for fragment in included})) or "none"
    source = f"operator file {os.environ[MODELS_ENV].strip()}" if os.environ.get(MODELS_ENV, "").strip() else "bundled catalog"
    summary = f"models from {source}; egress={select_egress(os.environ)}; provider keys found: {providers}"
    if args.check:
        print(f"render_config: {args.template} renders ({summary})")
        return 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=args.output.parent, prefix=".config.yaml.", delete=False)
    with handle:
        handle.write(rendered)
    os.chmod(handle.name, 0o640)
    os.replace(handle.name, args.output)
    print(f"render_config: wrote {args.output} ({summary})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
