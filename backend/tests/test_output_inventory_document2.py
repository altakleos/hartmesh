"""Closed format2 shapes; these seeded documents do not attest live routing."""

import hashlib
import json
from copy import deepcopy

import pytest
from test_output_inventory import inventory_document, output_receipt

from deerflow.runtime.runs.output import OutputEvidenceError
from deerflow.runtime.runs.output_inventory import (
    build_event_inventory_evidence,
    canonical_inventory_document_bytes,
    inventory_document_to_dict,
    validate_inventory_document,
    validate_inventory_pair,
)


def document2(*, kind="no_admitted_output_view", coverage="complete", **changes):
    value = inventory_document(version=2)
    scan = value.pop("scan")
    value["output_view"] = dict(kind="managed_outputs", scan=scan) if kind == "managed_outputs" else dict(kind=kind, scope=dict(version=1, kind="managed_output_routing", virtual_root="/mnt/user-data/outputs"))
    if coverage != "complete":
        value.update(coverage=coverage, reason="scan_incomplete" if coverage == "incomplete" else "unsupported_profile", observed_items=value.pop("items"))
    value.update(changes)
    return value


@pytest.mark.parametrize("kind", ["managed_outputs", "no_admitted_output_view"])
def test_document2_exact_canonical_bytes_bind_existing_receipt_pair(kind):
    value = document2(kind=kind)
    parsed = validate_inventory_document(value)
    expected = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    assert canonical_inventory_document_bytes(parsed) == expected
    evidence = build_event_inventory_evidence(output_receipt(), status="success", inventory_event_seq=8, document=parsed)
    assert evidence.inventory_sha256 == hashlib.sha256(expected).hexdigest()
    assert validate_inventory_pair(output_receipt(), evidence, status="success") == (output_receipt(), evidence)


@pytest.mark.parametrize("coverage", ["incomplete", "unsupported"])
def test_document2_unknown_view_remains_explicit_subset(coverage):
    value = document2(coverage=coverage, output_view=None)
    parsed = validate_inventory_document(value)
    assert parsed.coverage == coverage and parsed.observed_items == ()
    assert inventory_document_to_dict(parsed) == value


def test_viewless_document_retains_independent_current_file_offer():
    value = document2()
    value["items"] = [dict(kind="file", path="/mnt/user-data/outputs/old.txt", surfaces=["presentation_reference"], witnesses=[dict(kind="root_snapshot", root_epoch=value["root_epoch"], message_id="answer")])]
    parsed = validate_inventory_document(value)
    assert parsed.items[0].path == value["items"][0]["path"]
    assert parsed.items[0].surfaces == ("presentation_reference",)


def test_document1_canonical_bytes_remain_unchanged():
    value = inventory_document()
    expected = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    assert canonical_inventory_document_bytes(validate_inventory_document(value)) == expected


@pytest.mark.parametrize("delta", [{"output_view": None}, {"scan": {}}, {"version": True}, {"version": 2.0}, {"version": "2"}, {"profile": None}, {"root_epoch": None}])
def test_document2_complete_requires_closed_positive_envelope(delta):
    with pytest.raises(OutputEvidenceError):
        validate_inventory_document(document2(**delta))


@pytest.mark.parametrize("field,value", [("before_root", "opened"), ("produced_paths", []), ("scan", {}), ("scope", {"version": 1, "kind": "managed_output_routing", "virtual_root": "/elsewhere"})])
def test_viewless_never_claims_scan_or_other_routing(field, value):
    doc = document2()
    doc["output_view"][field] = value
    with pytest.raises(OutputEvidenceError):
        validate_inventory_document(doc)


@pytest.mark.parametrize("scan_surface", [True, False])
def test_viewless_forbids_scan_dependent_file_witnesses(scan_surface):
    doc = document2()
    doc["items"] = [dict(kind="file", path="/mnt/user-data/outputs/a.txt", surfaces=["output_change" if scan_surface else "presentation_reference"], witnesses=[dict(kind="output_scan")])]
    with pytest.raises(OutputEvidenceError):
        validate_inventory_document(doc)


def test_format2_managed_delta_keeps_exact_scan_join():
    doc = document2(kind="managed_outputs")
    doc["output_view"]["scan"]["produced_paths"] = ["/mnt/user-data/outputs/a.txt"]
    with pytest.raises(OutputEvidenceError):
        validate_inventory_document(doc)
    doc["items"] = [dict(kind="file", path="/mnt/user-data/outputs/a.txt", surfaces=["output_change"], witnesses=[dict(kind="output_scan")])]
    parsed = validate_inventory_document(doc)
    assert parsed.output_view.scan.produced_paths == ("/mnt/user-data/outputs/a.txt",)
    crossed = deepcopy(doc)
    crossed["items"][0]["path"] = "/mnt/user-data/outputs/b.txt"
    with pytest.raises(OutputEvidenceError):
        validate_inventory_document(crossed)
