"""Reporting-owned bounded preferences and cooperative, revision-checked mutations.

The stable sidecar lock coordinates these writers only. Native edits can bypass it.
Receipts are ordinary data, never authenticated authority. No platform imports.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

MAX_BYTES = 65536
MAX_DEPTH = 8
FIELDS = {"brand", "exclusions", "summary_length", "comparisons", "charts", "currency"}


class SettingsError(ValueError):
    pass


def _plain(value, maximum=256):
    return isinstance(value, str) and len(value) <= maximum and not any(ord(c) < 32 or ord(c) == 127 for c in value)


def _encode(value):
    try:
        raw = json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError) as error:
        raise SettingsError("Invalid preferences JSON.") from error
    if len(raw) > MAX_BYTES:
        raise SettingsError("Preferences exceed 64 KiB.")
    return raw


def _decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise SettingsError("Duplicate preferences key.")
            result[key] = value
        return result

    def constant(_):
        raise SettingsError("Non-finite preferences value.")

    if len(raw) > MAX_BYTES:
        raise SettingsError("Preferences exceed 64 KiB.")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)

        def depth(item, level=0):
            if level > MAX_DEPTH:
                raise SettingsError("Preferences exceed eight nesting levels.")
            if isinstance(item, dict):
                for child in item.values():
                    depth(child, level + 1)
            elif isinstance(item, list):
                for child in item:
                    depth(child, level + 1)

        depth(value)
        _encode(value)
        return value
    except (ValueError, UnicodeError, RecursionError) as error:
        raise SettingsError("Invalid preferences JSON: " + str(error)[:160]) from error


def validate(value):
    """Validate the whole v1 payload before applying even one setting."""
    _encode(value)
    if not isinstance(value, dict) or type(value.get("version")) is not int or value["version"] != 1 or set(value) - FIELDS - {"version"}:
        raise SettingsError("Expected version 1 preferences with supported fields only.")
    for key, item in value.items():
        valid = True
        if key == "brand":
            valid = isinstance(item, dict) and not set(item) - {"primary", "secondary"} and all(isinstance(v, str) and re.fullmatch(r"#[0-9a-fA-F]{6}", v) for v in item.values())
        elif key == "exclusions":
            valid = isinstance(item, list) and len(item) <= 64
            if valid:
                for exclusion in item:
                    if isinstance(exclusion, str):
                        valid = _plain(exclusion, 1024) and "=" in exclusion and bool(exclusion.partition("=")[0].strip())
                    else:
                        valid = isinstance(exclusion, dict) and set(exclusion) in ({"role", "equals"}, {"column", "equals"}) and all(_plain(v, 1024) for v in exclusion.values())
                        if valid:
                            valid = bool(exclusion.get("role", exclusion.get("column", "")).strip())
                    if not valid:
                        break
        elif key == "summary_length":
            valid = item in ("short", "standard")
        elif key == "comparisons":
            valid = isinstance(item, list) and len(item) <= 2 and all(v in ("previous_period", "same_period_last_year") for v in item) and len(set(item)) == len(item)
        elif key == "charts":
            valid = isinstance(item, list) and len(item) <= 32 and all(isinstance(v, str) and v in {"revenue_by_period", "revenue_by_category", "jobs_by_person"} for v in item)
        elif key == "currency":
            valid = isinstance(item, str) and bool(re.fullmatch(r"[A-Z]{3}", item))
        if not valid:
            raise SettingsError(f"Invalid preferences field: {key}.")
    return copy.deepcopy(value)


def _load(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError:
        return None
    with os.fdopen(fd, "rb") as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise SettingsError("Preferences must be a regular file.")
        return source.read(MAX_BYTES + 1)


def _snapshot(path):
    raw = _load(path)
    if raw is None:
        return {"preferences": {"version": 1}, "revision": "missing", "sha256": None}, None
    document = _decode(raw)
    if not isinstance(document, dict):
        raise SettingsError("Preferences must be an object.")
    document = dict(document)
    receipt = document.pop("_mutation", None)
    preferences = validate(document)
    digest = hashlib.sha256(raw).hexdigest()
    revision = "legacy:" + digest
    if receipt is not None:
        if not isinstance(receipt, dict) or set(receipt) != {"revision", "operation", "request_sha256"}:
            raise SettingsError("Invalid preferences mutation receipt.")
        if (
            not all(isinstance(receipt[k], str) and re.fullmatch(r"[0-9a-f]{64}", receipt[k]) for k in ("revision", "request_sha256"))
            or not isinstance(receipt["operation"], str)
            or not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", receipt["operation"])
        ):
            raise SettingsError("Invalid preferences mutation receipt.")
        revision = receipt["revision"]
    return {"preferences": preferences, "revision": revision, "sha256": digest}, receipt


def read(path):
    """Atomic-replacement snapshot; never creates or rewrites a file."""
    try:
        return _snapshot(Path(path))[0]
    except OSError as error:
        raise SettingsError("Cannot read preferences: " + str(error)) from error


@contextmanager
def _lock(path):
    try:
        import fcntl
    except ImportError as error:
        raise SettingsError("Preferences mutations require filesystem locking.") from error
    path.parent.mkdir(parents=True, exist_ok=True)
    # A retry may encounter directories left by an interrupted first creation.
    # Flush physical ancestry too; existence alone is not a durability receipt.
    directory = path.parent.resolve()
    while True:
        _flush_directory(directory)
        parent = directory.parent
        if parent == directory or parent.stat().st_dev != directory.stat().st_dev:
            break  # Existing mount provisioning belongs to the storage owner.
        directory = parent
    fd = os.open(path.with_name(path.name + ".lock"), os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "a+b") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise SettingsError("Preferences lock must be a regular file.")
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield


def _flush_directory(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def mutate(path, action, payload, *, expected, operation):
    if action not in ("save", "patch", "reset") or not isinstance(payload, dict):
        raise SettingsError("Invalid settings operation.")
    if not isinstance(operation, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", operation):
        raise SettingsError("Supply a stable operation ID (1–80 letters, digits, underscore or hyphen).")
    if not isinstance(expected, str) or not (expected == "missing" or re.fullmatch(r"(?:legacy:)?[0-9a-f]{64}", expected)):
        raise SettingsError("Supply the exact revision returned by read.")
    if action == "reset" and payload:
        raise SettingsError("Reset takes no preferences.")
    request = {"action": action, "payload": payload, "expected": expected, "operation": operation}
    fingerprint = hashlib.sha256(_encode(request)).hexdigest()
    path = Path(path)
    published = False
    temporary = None
    try:
        with _lock(path):
            current, receipt = _snapshot(path)
            if receipt and receipt["operation"] == operation:
                if receipt["request_sha256"] != fingerprint:
                    raise SettingsError("Operation ID conflicts with a different request.")
                published = True
                # A prior replacement may have succeeded without its directory flush.
                _flush_directory(path.parent)
                return current
            if current["revision"] != expected:
                raise SettingsError("Preferences revision conflict; reload and reconcile the original operation before retrying.")
            if action == "save":
                preferences = validate(payload)
            elif action == "reset":
                preferences = {"version": 1}
            else:
                if set(payload) - FIELDS:
                    raise SettingsError("Patch supports preference fields only.")
                preferences = dict(current["preferences"])
                for key, value in payload.items():
                    if value is None:
                        preferences.pop(key, None)
                    else:
                        preferences[key] = value
                preferences = validate(preferences)
            revision = hashlib.sha256(os.urandom(32)).hexdigest()
            data = _encode({**preferences, "_mutation": {"revision": revision, "operation": operation, "request_sha256": fingerprint}}) + b"\n"
            if len(data) > MAX_BYTES:
                raise SettingsError("Managed preferences exceed 64 KiB.")
            fd, temporary = tempfile.mkstemp(prefix=".preferences-", dir=path.parent)
            with os.fdopen(fd, "wb") as target:
                target.write(data)
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary, path)
            published = True
            _flush_directory(path.parent)
            return _snapshot(path)[0]
    except OSError as error:
        phase = "outcome uncertain; reconcile with the exact same operation and request" if published else "failed before publication"
        raise SettingsError(f"Preferences {phase}: {error}") from error
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("read", "validate", "save", "patch", "reset"))
    parser.add_argument("path")
    parser.add_argument("--from", dest="source", help="Bounded JSON payload file for save/patch")
    parser.add_argument("--expected", help="Exact revision returned by read; missing for first creation")
    parser.add_argument("--operation", help="Keep the same ID and request across uncertain retries")
    args = parser.parse_args(argv)
    try:
        if args.action in ("read", "validate"):
            result = read(args.path)
            if args.action == "validate" and result["revision"] == "missing":
                raise SettingsError("Preferences file is missing.")
        else:
            raw = _load(Path(args.source)) if args.source else None
            if args.action in ("save", "patch") and raw is None:
                raise SettingsError("Supply --from for save/patch.")
            result = mutate(args.path, args.action, _decode(raw) if raw is not None else {}, expected=args.expected, operation=args.operation)
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
        return 0
    except (SettingsError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
