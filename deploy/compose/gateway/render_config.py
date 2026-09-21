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
other value refuses to render. ``sandbox.ready_timeout`` is the template's
value unless the optional ``SANDBOX_READY_TIMEOUT`` names a whole number of
seconds from 60 to 600; anything else refuses to render, so no value can
disable the cold-start deadline. The rendered document is checked so that no
``$NAME`` reference remains for a variable that is absent or empty.

The sign-in mode is selected by the tenant ``.env`` and nothing is assumed:
the three sign-on keys (``HARTMESH_SIGN_ON_ISSUER``, ``_CLIENT_ID``,
``_CLIENT_SECRET``) render the identity provider as the one way in --
``auth.local.enabled: false``, registration off, one provider named ``sso``
whose callback is ``https://<HARTMESH_PUBLIC_HOST>/api/v1/auth/callback/sso``
-- while ``HARTMESH_LOCAL_PASSWORDS=allowed`` copies the template's ``auth``
through unchanged, which is local passwords exactly as before. Neither, both,
or a half-set sign-on group refuses to render and names every key involved
(README: "Sign-in"). The client secret reaches the Gateway as the reference
``$HARTMESH_SIGN_ON_CLIENT_SECRET`` and is never written to disk. The optional
``HARTMESH_SIGN_ON_ACCESS_CLAIM`` / ``_ACCESS_VALUES`` pair makes admission
follow one claim of the token, and ``HARTMESH_SIGN_ON_ROLES`` makes the role
follow it too (README: "Membership follows the claim").

``HARTMESH_MODELS_FILE`` is optional. Absent (or empty), everything above is
the whole story. Set, it names a YAML file on the tenant's own data disk --
mounted read-only at ``<HARTMESH_DATA_DIR>/operator`` -- carrying ``models:``
and nothing else, and that list becomes the whole rendered ``models:``
section: fragment models are no longer appended, whichever provider keys the
tenant carries.

The rendered ``tenant_bundle.path`` names the directory the operator writes
the company's brand, starter list and report profiles into. The render reads
it through the Gateway's own loader and reports one summary line -- whether a
company name and a logo are set, how many starters and report profiles there
are -- plus one ``warning:`` line per problem the loader names. A problem
never refuses the render: a brand typo is not a reason to deny the tenant a
Gateway, and the summary is how an operator sees it first.

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
READY_TIMEOUT_ENV = "SANDBOX_READY_TIMEOUT"
# Whole seconds, inclusive. The floor is the harness default (a smaller budget
# has never been useful); the ceiling keeps one cold start inside a single
# nginx proxy_read_timeout window (600 s) so a waiting turn is not cut off by
# the front door before the sandbox answers.
READY_TIMEOUT_RANGE = (60, 600)
_WHOLE_SECONDS = re.compile(r"\A[0-9]+\Z")
MODELS_ENV = "HARTMESH_MODELS_FILE"
# ── Sign-in mode ────────────────────────────────────────────────────────────
SIGN_ON_ISSUER_ENV = "HARTMESH_SIGN_ON_ISSUER"
SIGN_ON_CLIENT_ID_ENV = "HARTMESH_SIGN_ON_CLIENT_ID"
SIGN_ON_CLIENT_SECRET_ENV = "HARTMESH_SIGN_ON_CLIENT_SECRET"
SIGN_ON_KEYS = (SIGN_ON_ISSUER_ENV, SIGN_ON_CLIENT_ID_ENV, SIGN_ON_CLIENT_SECRET_ENV)
LOCAL_PASSWORDS_ENV = "HARTMESH_LOCAL_PASSWORDS"
LOCAL_PASSWORDS_VALUE = "allowed"
SIGN_ON_ADMINS_ENV = "HARTMESH_SIGN_ON_ADMINS"
SIGN_ON_SCOPES_ENV = "HARTMESH_SIGN_ON_SCOPES"
SIGN_ON_CLIENT_AUTH_ENV = "HARTMESH_SIGN_ON_CLIENT_AUTH"
SIGN_ON_NAME_ENV = "HARTMESH_SIGN_ON_NAME"
SIGN_ON_ACCESS_CLAIM_ENV = "HARTMESH_SIGN_ON_ACCESS_CLAIM"
SIGN_ON_ACCESS_VALUES_ENV = "HARTMESH_SIGN_ON_ACCESS_VALUES"
SIGN_ON_ROLES_ENV = "HARTMESH_SIGN_ON_ROLES"
SIGN_ON_OPTIONAL_KEYS = (SIGN_ON_ADMINS_ENV, SIGN_ON_SCOPES_ENV, SIGN_ON_CLIENT_AUTH_ENV, SIGN_ON_NAME_ENV, SIGN_ON_ACCESS_CLAIM_ENV, SIGN_ON_ACCESS_VALUES_ENV, SIGN_ON_ROLES_ENV)
SIGN_ON_ROLE_NAMES = ("admin", "user")
PUBLIC_HOST_ENV = "HARTMESH_PUBLIC_HOST"
TOKEN_EXPIRY_DAYS_ENV = "AUTH_TOKEN_EXPIRY_DAYS"
TOKEN_EXPIRY_DAYS_RANGE = (1, 30)
SIGN_IN_MODES = ("sign_on_only", "local")
# The provider's name is fixed so the callback a deployer registers is
# computable from the public host alone, before the VM exists.
SIGN_ON_PROVIDER_ID = "sso"
SIGN_ON_DEFAULT_NAME = "Single sign-on"
SIGN_ON_DEFAULT_SCOPES = ("openid", "email", "profile")
SIGN_ON_CLIENT_AUTH_METHODS = ("client_secret_post", "client_secret_basic")
_HOSTNAME = re.compile(r"\A[A-Za-z0-9.-]+\Z")
_SCOPE = re.compile(r"\A[A-Za-z0-9_.:/-]+\Z")
_EMAIL = re.compile(r"\A[^\s@]+@[^\s@]+\.[^\s@]+\Z")
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


def _safe_location(path: tuple[object, ...]) -> str:
    """The structural head of a path, with operator-typed keys left out.

    A path is kept as components rather than as text, because text cannot tell
    a generated index from a bracket someone typed, nor a schema field from a
    key someone pasted. Only two kinds of component are safe to print: a list
    index, which this renderer generated, and the document's own top-level key,
    which comes from the template. Everything from the first operator-typed key
    onwards is dropped -- including any index below it, which means nothing
    without the key above it.

    ``("models", 0, "api_key")`` and ``("models", 0, "x_options", 0, "token")``
    both locate ``models[0]``, and the operator looks inside that entry.
    """

    rendered: list[str] = []
    for component in path:
        if isinstance(component, int):
            rendered.append(f"[{component}]")
        elif not rendered:
            # The top level of the rendered document is the template's, and the
            # operator file cannot add a key to it (`load_operator_models`).
            rendered.append(str(component))
        else:
            break
    return "".join(rendered) or "the document root"


def _credential_problems(value: object, path: tuple[object, ...], problems: list[str]) -> None:
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
            child = (*path, key)
            if _is_credential_field(str(key)):
                # An absent credential is the documented way to configure a
                # client that needs none; null is the same statement in YAML.
                if item is not None and not (isinstance(item, str) and _VARIABLE.fullmatch(item)):
                    problems.append(_safe_location(child))
                continue
            _credential_problems(item, child, problems)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _credential_problems(item, (*path, index), problems)


def _schema_location(loc: tuple[object, ...], declared: frozenset[str]) -> str:
    """A pydantic ``loc`` with only the schema's own vocabulary printed.

    ``loc`` is not a trusted source of text: an ``invalid_key`` error carries
    the rejected key itself, and that key is operator-typed content. A field the
    schema declares is the schema's word, not the operator's, so it stays;
    anything else becomes ``(key)`` and ends the location.
    """

    rendered: list[str] = []
    for component in loc:
        if isinstance(component, int):
            rendered.append(f"[{component}]")
        elif isinstance(component, str) and component in declared:
            rendered.append(f".{component}" if rendered else component)
        else:
            rendered.append(".(key)" if rendered else "(key)")
            break
    return "".join(rendered) or "(entry)"


def _schema_problem(error: Mapping[str, Any], declared: frozenset[str]) -> str:
    """One pydantic error as location plus rule, with the input left behind."""

    location = _schema_location(tuple(error.get("loc") or ()), declared)
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

    declared = frozenset(ModelConfig.model_fields)
    try:
        ModelConfig.model_validate(dict(entry))
    except ValidationError as exc:
        problems = sorted({_schema_problem(error, declared) for error in exc.errors()})
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


def select_ready_timeout(environ: Mapping[str, str]) -> int | None:
    """Return the operator's cold-start readiness budget override, or None when unset."""

    raw = environ.get(READY_TIMEOUT_ENV, "").strip()
    if not raw:
        return None
    low, high = READY_TIMEOUT_RANGE
    if not _WHOLE_SECONDS.match(raw) or not low <= int(raw) <= high:
        raise RenderError(f"{READY_TIMEOUT_ENV} must be a whole number of seconds from {low} to {high} (or absent)")
    return int(raw)


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


def select_sign_in(environ: Mapping[str, str]) -> str:
    """Return the sign-in mode the contract selected, or refuse.

    Three outcomes and no fourth: every sign-on key present is
    ``sign_on_only``; ``HARTMESH_LOCAL_PASSWORDS=allowed`` alone is ``local``;
    neither, both, or a half-set sign-on group is a refusal that names every
    key involved. Nothing falls back to local passwords: a forgotten key is a
    tenant nobody can enter, which is noticed at once, where an open one on
    the internet is not.
    """

    present = [name for name in SIGN_ON_KEYS if _present(environ, name)]
    local = environ.get(LOCAL_PASSWORDS_ENV, "").strip()
    if local and local != LOCAL_PASSWORDS_VALUE:
        raise RenderError(f"{LOCAL_PASSWORDS_ENV} must be exactly `{LOCAL_PASSWORDS_VALUE}` (or absent)")
    if local and present:
        raise RenderError(f"{LOCAL_PASSWORDS_ENV} and the sign-on keys ({', '.join(present)}) are both set; a tenant signs in one way. Remove one side")
    if local:
        stray = [name for name in SIGN_ON_OPTIONAL_KEYS if _present(environ, name)]
        if stray:
            raise RenderError(f"{LOCAL_PASSWORDS_ENV}={LOCAL_PASSWORDS_VALUE} selects local passwords, but the sign-on options {', '.join(stray)} are set and would be ignored. Remove them, or select sign-on with the three sign-on keys")
        return "local"
    if not present:
        raise RenderError(f"no sign-in mode is selected: set the sign-on keys {', '.join(SIGN_ON_KEYS)} for the identity provider, or {LOCAL_PASSWORDS_ENV}={LOCAL_PASSWORDS_VALUE} for local passwords. Nothing is assumed")
    missing = [name for name in SIGN_ON_KEYS if name not in present]
    if missing:
        raise RenderError(f"the sign-on keys are incomplete: missing {', '.join(missing)} (present: {', '.join(present)}). Nothing falls back to local passwords")
    return "sign_on_only"


def select_token_expiry_days(environ: Mapping[str, str]) -> int | None:
    """Validate the optional session lifetime the Gateway reads from its environment."""

    raw = environ.get(TOKEN_EXPIRY_DAYS_ENV, "").strip()
    if not raw:
        return None
    low, high = TOKEN_EXPIRY_DAYS_RANGE
    if not _WHOLE_SECONDS.match(raw) or not low <= int(raw) <= high:
        raise RenderError(f"{TOKEN_EXPIRY_DAYS_ENV} must be a whole number of days from {low} to {high} (or absent)")
    return int(raw)


def _split_list(raw: str) -> list[str]:
    return [item for item in re.split(r"[,\s]+", raw.strip()) if item]


def sign_on_access(environ: Mapping[str, str]) -> dict[str, Any]:
    """The provider's admission rule from the optional access keys, or ``{}``.

    ``HARTMESH_SIGN_ON_ACCESS_CLAIM`` names one claim literally (a URN with
    colons and dots is one name) and ``HARTMESH_SIGN_ON_ACCESS_VALUES`` the
    values of it that admit; ``HARTMESH_SIGN_ON_ROLES`` optionally maps each
    admitting value to ``admin`` or ``user``. A half-set pair, a mapping with
    no claim, a mapping beside the administrators' list, an admitting value
    with no role or a role for a value that does not admit each refuse and
    name the keys and positions involved, never the values.
    """

    claim = environ.get(SIGN_ON_ACCESS_CLAIM_ENV, "").strip()
    values = _split_list(environ.get(SIGN_ON_ACCESS_VALUES_ENV, ""))
    roles_raw = environ.get(SIGN_ON_ROLES_ENV, "").strip()
    if claim and not values:
        raise RenderError(f"{SIGN_ON_ACCESS_CLAIM_ENV} is set but {SIGN_ON_ACCESS_VALUES_ENV} is empty: no value would admit anyone. Set the admitting values, or unset the claim")
    if values and not claim:
        raise RenderError(f"{SIGN_ON_ACCESS_VALUES_ENV} is set but {SIGN_ON_ACCESS_CLAIM_ENV} is not: there is no claim to look the values up in. Set the claim, or unset the values")
    if len(set(values)) != len(values):
        raise RenderError(f"{SIGN_ON_ACCESS_VALUES_ENV} repeats a value; each admitting value once, separated by commas")
    access: dict[str, Any] = {"access_claim": claim, "access_values": values} if claim else {}
    if not roles_raw:
        return access
    if not claim:
        raise RenderError(f"{SIGN_ON_ROLES_ENV} is set but {SIGN_ON_ACCESS_CLAIM_ENV} is not: a role mapping needs the admission claim it maps. Set {SIGN_ON_ACCESS_CLAIM_ENV} and {SIGN_ON_ACCESS_VALUES_ENV}, or unset the mapping")
    if _present(environ, SIGN_ON_ADMINS_ENV):
        raise RenderError(f"{SIGN_ON_ROLES_ENV} and {SIGN_ON_ADMINS_ENV} are both set: roles come from the claim or from the email list, not both. Unset one")
    mapping: dict[str, str] = {}
    for index, entry in enumerate(_split_list(roles_raw)):
        value, separator, role = entry.rpartition("=")
        if not separator or not value or role not in SIGN_ON_ROLE_NAMES:
            raise RenderError(f"{SIGN_ON_ROLES_ENV} must be entries of the form <value>=admin or <value>=user separated by commas; the entry at position {index} is not")
        if value in mapping:
            raise RenderError(f"{SIGN_ON_ROLES_ENV} maps the value at position {index} twice")
        mapping[value] = role
    unmapped = [str(index) for index, value in enumerate(values) if value not in mapping]
    if unmapped:
        raise RenderError(f"{SIGN_ON_ROLES_ENV} gives no role to the admitting value(s) at position {', '.join(unmapped)} of {SIGN_ON_ACCESS_VALUES_ENV}: every value that admits must carry a role")
    stray = [str(index) for index, value in enumerate(mapping) if value not in values]
    if stray:
        raise RenderError(f"{SIGN_ON_ROLES_ENV} maps value(s) at position {', '.join(stray)} that are not in {SIGN_ON_ACCESS_VALUES_ENV}: a role can only follow a value that admits")
    access["access_roles"] = mapping
    return access


def sign_on_auth(template_auth: Mapping[str, Any], environ: Mapping[str, str]) -> dict[str, Any]:
    """The rendered ``auth`` block for sign-on-only mode.

    Diagnostics name the key and the rule, never the value: the issuer,
    the client id and every optional value are operator-typed, and a paste
    can put the secret in any of them.
    """

    host = environ.get(PUBLIC_HOST_ENV, "").strip()
    if not host or _HOSTNAME.fullmatch(host) is None:
        raise RenderError(f"{PUBLIC_HOST_ENV} must be a hostname (letters, digits, dots, hyphens): the sign-on callback is derived from it")
    issuer = environ[SIGN_ON_ISSUER_ENV].strip()
    if not issuer.startswith("https://") or any(character.isspace() for character in issuer) or len(issuer) <= len("https://"):
        raise RenderError(f"{SIGN_ON_ISSUER_ENV} must be the provider's issuer URL, starting with https:// (the tenant is published on the internet, and the ID token's `iss` must equal it)")
    client_id = environ[SIGN_ON_CLIENT_ID_ENV].strip()
    if any(character.isspace() for character in client_id):
        raise RenderError(f"{SIGN_ON_CLIENT_ID_ENV} must not contain whitespace")

    admins = _split_list(environ.get(SIGN_ON_ADMINS_ENV, ""))
    bad = [str(index) for index, email in enumerate(admins) if _EMAIL.fullmatch(email) is None]
    if bad:
        raise RenderError(f"{SIGN_ON_ADMINS_ENV} must be a comma-separated list of email addresses; entries at position {', '.join(bad)} are not")

    scopes = list(SIGN_ON_DEFAULT_SCOPES)
    for scope in _split_list(environ.get(SIGN_ON_SCOPES_ENV, "")):
        if _SCOPE.fullmatch(scope) is None:
            raise RenderError(f"{SIGN_ON_SCOPES_ENV} must be a list of scope tokens separated by spaces or commas")
        if scope not in scopes:
            scopes.append(scope)

    method = environ.get(SIGN_ON_CLIENT_AUTH_ENV, "").strip() or SIGN_ON_CLIENT_AUTH_METHODS[0]
    if method not in SIGN_ON_CLIENT_AUTH_METHODS:
        raise RenderError(f"{SIGN_ON_CLIENT_AUTH_ENV} must be one of {', '.join(SIGN_ON_CLIENT_AUTH_METHODS)} (or absent)")

    name = environ.get(SIGN_ON_NAME_ENV, "").strip() or SIGN_ON_DEFAULT_NAME
    if len(name) > 64 or not name.isprintable():
        raise RenderError(f"{SIGN_ON_NAME_ENV} must be printable text of at most 64 characters")
    access = sign_on_access(environ)

    local = dict(_mapping(template_auth.get("local", {}), "template `auth.local`"))
    if "enabled" in local or "allow_registration" in local:
        raise RenderError("template `auth.local.enabled` and `auth.local.allow_registration` must be absent; the sign-in keys select them")
    if "oidc" in template_auth:
        raise RenderError("template `auth.oidc` must be absent; the sign-on keys render it")
    local.update({"enabled": False, "allow_registration": False})
    return {
        **template_auth,
        "local": local,
        "oidc": {
            "enabled": True,
            "frontend_base_url": f"https://{host}",
            "providers": {
                SIGN_ON_PROVIDER_ID: {
                    "display_name": name,
                    "issuer": issuer,
                    "client_id": client_id,
                    # The reference, never the value: the Gateway expands it.
                    "client_secret": f"${SIGN_ON_CLIENT_SECRET_ENV}",
                    "redirect_uri": f"https://{host}/api/v1/auth/callback/{SIGN_ON_PROVIDER_ID}",
                    "scopes": scopes,
                    "token_endpoint_auth_method": method,
                    "admin_emails": admins,
                    **access,
                }
            },
        },
    }


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
    low, high = READY_TIMEOUT_RANGE
    budget = sandbox.get("ready_timeout")
    if isinstance(budget, bool) or not isinstance(budget, int) or not low <= budget <= high:
        raise RenderError(f"template `sandbox.ready_timeout` must be a whole number of seconds from {low} to {high}")
    override = select_ready_timeout(environ)
    if override is not None:
        sandbox["ready_timeout"] = override
    if mode == "open" and environ.get("DEER_FLOW_SANDBOX_RUNTIME") == "runsc":
        mounts = list(sandbox.get("mounts") or [])
        if any(mount.get("container_path") == "/etc/resolv.conf" for mount in mounts):
            raise RenderError("the profile owns the open-runsc resolver mount; remove the conflicting template mount")
        sandbox["mounts"] = [*mounts, open_runsc_resolver_mount(environ)]
    document["sandbox"] = sandbox

    template_auth = _mapping(document.get("auth", {}), "template `auth`")
    if "oidc" in template_auth or "enabled" in _mapping(template_auth.get("local", {}), "template `auth.local`"):
        raise RenderError("template `auth.oidc` and `auth.local.enabled` must be absent; the sign-in keys select them")
    select_token_expiry_days(environ)
    if select_sign_in(environ) == "sign_on_only":
        document["auth"] = sign_on_auth(template_auth, environ)

    problems: list[str] = []
    _credential_problems(document, (), problems)
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


def bundle_report(document: Mapping[str, Any]) -> tuple[str, tuple[str, ...]]:
    """One summary line for the tenant bundle the rendered config names, and the problems the loader found.

    Counts and states only: what the operator wrote is theirs, and this line is
    journalled at every start.
    """

    section = document.get("tenant_bundle")
    path = section.get("path") if isinstance(section, Mapping) else None
    if not path:
        return "tenant bundle: none configured", ()
    # The Gateway's own loader, so what --check reports is what the Gateway reads.
    bundle = import_module("deerflow.config.tenant_bundle").load_tenant_bundle(path)
    if not bundle.present:
        return f"tenant bundle at {path}: unusable", bundle.problems
    starters = "none" if bundle.starters is None else str(len(bundle.starters))
    summary = f"tenant bundle at {path}: company_name {'set' if bundle.company_name else 'unset'}; logo {'present' if bundle.logo else 'absent'}; starters {starters}; report profiles {len(bundle.report_profiles)}"
    return summary, bundle.problems


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
    document = yaml.safe_load(rendered)
    budget = document["sandbox"]["ready_timeout"]
    bundle_line, bundle_problems = bundle_report(document)
    for problem in bundle_problems:
        print(f"render_config: warning: tenant bundle: {problem}", file=sys.stderr)
    sign_in = select_sign_in(os.environ)
    sign_in_line = f"sign-in={sign_in}"
    if sign_in == "sign_on_only":
        access = sign_on_access(os.environ)
        membership = (", admission by claim" if access.get("access_claim") else "") + (", roles from claim" if access.get("access_roles") else "")
        sign_in_line += f" (provider {SIGN_ON_PROVIDER_ID}, callback https://{os.environ[PUBLIC_HOST_ENV].strip()}/api/v1/auth/callback/{SIGN_ON_PROVIDER_ID}{membership})"
    summary = f"models from {source}; egress={select_egress(os.environ)}; {sign_in_line}; provider keys found: {providers}; sandbox ready_timeout={budget}s; {bundle_line}"
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
