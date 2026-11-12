"""Immutable media proof for source-aware review refreshes.

Historical receipts acknowledge previous effects only. They grant no authority
to read a file or send current content, and retain their actual protection flag.
"""

from copy import deepcopy
from types import SimpleNamespace

from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_result_files import result_files, result_media, result_options
from app.telegram_bots.work_result_identity import qualify_identity
from app.telegram_bots.work_result_ledger import qualify_result_parts

_WORK = {"id", "instance_id", "assignment_revision", "outcome_id", "outcome"}
_HIDDEN = {"kind": "unavailable", "current_contents": "not_checked"}


def _recipes(proof):
    files = result_files(proof["work"], locale=proof["locale"])
    return [
        {
            **part,
            "source_ref": {"key": "result.presentation", "values": {"work_id": proof["work"]["id"], "part": index}}
            if proof["version"] == 2
            else {"key": "work.request.presentation", "values": {"request_id": proof["request_id"], "part": index}},
        }
        for index, part in enumerate(result_media(files, protected=proof["protected"], **result_options(proof["options"])))
    ]


def archive_media(item, source, *, locale, options, earlier=None):
    """Capture before retiring unfinished parts; callers own source/claim locks."""
    version = item.payload["presentation_version"]
    parts = [deepcopy(part) for part in item.parts if part["presentation_version"] == version and ("files" in part or "result_file_warning" in part)]
    ledger = deepcopy(item.payload["work_result_ledger"])
    identities = {part["part_id"] for part in parts}
    ledger["roots"] = [identity for identity in ledger["roots"] if identity in identities]
    proof = {
        "version": 2 if "result_notice_basis" in item.payload else 1,
        "presentation_version": version,
        "request_id": item.request_id if "result_notice_basis" in item.payload else source.request["id"],
        "work": {key: deepcopy(source.work[key]) for key in _WORK},
        "locale": locale,
        "protected": source.protected,
        "options": deepcopy(options),
        "ledger": ledger,
        "parts": parts,
        **({"navigation": deepcopy(item.payload["work_result_navigation"])} if "work_result_navigation" in item.payload else {}),
    }
    read_media_history(item, proof, source, earlier=earlier)
    return proof


def read_media_history(item, proof, source, *, earlier=None):
    """Return source-index references and byte identities, never invented ACKs."""
    try:
        if type(proof) is not dict or set(proof) != {"version", "presentation_version", "request_id", "work", "locale", "protected", "options", "ledger", "parts"} | ({"navigation"} if "navigation" in proof else set()):
            raise ValueError()
        if (
            type(proof["version"]) is not int
            or proof["version"] != (2 if "result_notice_basis" in item.payload else 1)
            or type(proof["protected"]) is not bool
            or proof["request_id"] != (item.request_id if "result_notice_basis" in item.payload else source.request["id"])
            or set(proof["work"]) != _WORK
        ):
            raise ValueError()
        old, current = proof["work"], source.work
        qualify_identity(item.payload.get("work_result_identity"), SimpleNamespace(work=old))
        if any(not same_body(old[key], current[key]) for key in _WORK - {"outcome"}):
            raise ValueError()
        if not same_body({key: value for key, value in old["outcome"].items() if key != "sources"}, {key: value for key, value in current["outcome"].items() if key != "sources"}):
            raise ValueError()
        result_files(current, locale=proof["locale"])
        previous, latest = old["outcome"]["sources"], current["outcome"]["sources"]
        if len(previous) != len(latest) or any(left != right and left != _HIDDEN and right != _HIDDEN for left, right in zip(previous, latest, strict=True)):
            raise ValueError()
        snapshot = SimpleNamespace(parts=proof["parts"], payload={"presentation_version": proof["presentation_version"], "work_result_ledger": proof["ledger"], "work_result_navigation": proof.get("navigation")})
        recipes = _recipes(proof)
        qualify_result_parts(snapshot, recipes, carried=earlier)
        carried, hashes = {}, {}
        for event in proof["ledger"]["events"]:
            if event["reason"] == "carry":
                for receipt in event["receipts"]:
                    carried[receipt["source_index"]] = deepcopy(receipt)
        unfinished = False
        for part in proof["parts"]:
            matches = [actual for actual in item.parts if actual["part_id"] == part["part_id"]]
            if len(matches) != 1:
                raise ValueError()
            actual = matches[0]
            if not same_body(actual, part) and not (part["state"] in {"pending", "sending"} and actual["state"] == "retired" and same_body({**actual, "state": part["state"]}, part)):
                raise ValueError()
            for index, digest in part.get("result_file_hashes", {}).items():
                if hashes.setdefault(int(index), digest) != digest:
                    raise ValueError()
            if part["state"] != "sent":
                unfinished |= part["state"] not in {"retired", "unavailable"}
                continue
            if unfinished:
                raise ValueError()
            members = part.get("files") or [part["result_file_warning"]["file"]]
            for file in members:
                index = file["source_index"]
                if index in carried:
                    raise ValueError()
                carried[index] = {"part_id": part["part_id"], "source_index": index}
        return carried, hashes
    except (KeyError, TypeError, ValueError, IndexError, AttributeError):
        raise ValueError("invalid-work-result-media-proof") from None


def qualified_history(item, source):
    history = item.payload.get("work_result_history", [])
    if type(history) is not list:
        raise ValueError("invalid-work-result-media-proof")
    carried, hashes, version = {}, {}, -1
    for proof in history:
        number = proof.get("presentation_version") if type(proof) is dict else None
        if type(number) is not int or not version < number < item.payload["presentation_version"]:
            raise ValueError("invalid-work-result-media-proof")
        known, remembered = read_media_history(item, proof, source, earlier=carried)
        for index, receipt in known.items():
            if carried.setdefault(index, receipt) != receipt:
                raise ValueError("conflicting-work-result-receipt")
        for index, digest in remembered.items():
            if hashes.setdefault(index, digest) != digest:
                raise ValueError("conflicting-work-result-bytes")
        version = number
    for part in item.parts:
        if part["presentation_version"] >= item.payload["presentation_version"] or not ("files" in part or "result_file_warning" in part):
            continue
        for index, digest in part.get("result_file_hashes", {}).items():
            if hashes.get(int(index)) != digest:
                raise ValueError("unqualified-work-result-bytes")
        if part["state"] == "sent":
            for file in part.get("files") or [part["result_file_warning"]["file"]]:
                index = file["source_index"]
                if carried.get(index) != {"part_id": part["part_id"], "source_index": index}:
                    raise ValueError("unqualified-work-result-receipt")
    return carried, hashes
