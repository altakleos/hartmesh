"""Opaque canonical outcome identity, independent of recipient redaction."""

import hashlib
import json


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()


def result_identity(work):
    return {
        "version": 1,
        "outcome": digest([work.id, work.instance_id, work.assignment_revision, work.outcome_id, work.outcome]),
        "sources": [digest(source) for source in work.outcome["sources"]],
    }


def qualify_identity(identity, source):
    if type(identity) is not dict or set(identity) != {"version", "outcome", "sources"} or type(identity["version"]) is not int or identity["version"] != 1 or type(identity["sources"]) is not list:
        raise ValueError("invalid-work-result-identity")
    refs = source.work["outcome"]["sources"]
    if len(identity["sources"]) != len(refs):
        raise ValueError("invalid-work-result-identity")
    for value in [identity["outcome"], *identity["sources"]]:
        if type(value) is not str or len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError("invalid-work-result-identity")
    for reference, expected in zip(refs, identity["sources"], strict=True):
        if reference == {"kind": "unavailable", "current_contents": "not_checked"}:
            continue
        if digest({key: value for key, value in reference.items() if key != "current_contents"}) != expected:
            raise ValueError("invalid-work-result-source-identity")
