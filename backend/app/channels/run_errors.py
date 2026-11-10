"""Opaque exact-run host error facts; native diagnostic text is never retained."""

import re
from dataclasses import asdict, dataclass

from app.channels.execution import ChannelExecutionProtocolError, ChannelExecutionScope

_ISSUER = object()
_CODE = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,127}\Z")


def error_facts(value):
    """Only structured host codes; an absent/malformed shape proves nothing."""
    if type(value) is not dict:
        return None
    result = {key: value.get(key) for key in ("name", "reason", "cause", "status")}
    if type(result["name"]) is not str or not _CODE.fullmatch(result["name"]):
        return None
    if any(result[key] is not None and (type(result[key]) is not str or not _CODE.fullmatch(result[key])) for key in ("reason", "cause")):
        return None
    if result["status"] is not None and (type(result["status"]) is not int or not 100 <= result["status"] <= 599):
        return None
    return result


@dataclass(frozen=True, init=False)
class RunErrorEvidence:
    _issuer: object
    _scope: ChannelExecutionScope
    _run_id: str
    _facts: tuple

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


def _issue_run_error(scope, run_id, facts):
    """Trusted facade only, after scoped replay and terminal-row revalidation."""
    if type(scope) is not ChannelExecutionScope or type(facts) is not dict or error_facts(facts) != facts:
        raise ChannelExecutionProtocolError()
    result = object.__new__(RunErrorEvidence)
    for key, value in {"_issuer": _ISSUER, "_scope": scope, "_run_id": run_id, "_facts": tuple(facts.items())}.items():
        object.__setattr__(result, key, value)
    return result


def validate_run_error(value, scope, run_id):
    if type(value) is not RunErrorEvidence or getattr(value, "_issuer", None) is not _ISSUER or value._scope != scope or value._run_id != run_id:
        raise ChannelExecutionProtocolError()
    return dict(value._facts)


def saved_run_error(value, scope, run_id):
    return {"version": 1, "scope": asdict(scope), "run_id": run_id, "facts": validate_run_error(value, scope, run_id)}


def validate_saved_run_error(value, scope, run_id):
    """Read authenticated saved host data; this never issues a live capability."""
    if (
        type(value) is not dict
        or set(value) != {"version", "scope", "run_id", "facts"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or value["scope"] != asdict(scope)
        or value["run_id"] != run_id
        or type(value["facts"]) is not dict
        or set(value["facts"]) != {"name", "reason", "cause", "status"}
        or error_facts(value["facts"]) != value["facts"]
    ):
        raise ChannelExecutionProtocolError()
    return dict(value["facts"])
