"""Closed inventory proof tests; seeded documents attest parsing, not live provenance."""

import hashlib
import importlib
import json
from copy import deepcopy
from types import MappingProxyType

import pytest

from deerflow.runtime.runs.output import OutputEvidenceError, TextOutputReceipt

ROOT_EPOCH = "44067627-570c-4c90-9503-48f14b80627f"


def inventory_api():
    return importlib.import_module("deerflow.runtime.runs.output_inventory")


def output_receipt(**changes):
    return TextOutputReceipt(
        version=1,
        kind="text",
        owner_user_id="owner",
        thread_id="thread",
        run_id="run",
        input_message_id="human",
        assistant_message_id="sdk-opaque-" + "a" * 96,
        event_seq=3,
        text_sha256=hashlib.sha256(b"answer").hexdigest(),
        **changes,
    )


def inventory_document(**changes):
    api = inventory_api()
    value = dict(
        version=1,
        profile="terminal-presentation-v1",
        owner_user_id="owner",
        thread_id="thread",
        run_id="run",
        input_message_id="human",
        output_receipt_sha256=api.output_receipt_digest(output_receipt()),
        root_epoch=ROOT_EPOCH,
        coverage="complete",
        reason=None,
        items=[],
        scan=dict(
            state="complete",
            source="managed_outputs",
            scope=dict(version=1, kind="managed_outputs_regular_changes", virtual_root="/mnt/user-data/outputs", excluded_dir_names=[".git", "tool_output"]),
            before_root="opened",
            after_root="opened",
            produced_paths=[],
        ),
    )
    value.update(changes)
    return value


def test_complete_empty_digest_uses_entire_exact_document_and_long_sdk_id():
    api = inventory_api()
    value = inventory_document()
    result = api.validate_inventory_document(value)
    expected = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    assert api.canonical_inventory_document_bytes(result) == expected
    assert api.inventory_document_digest(result) == hashlib.sha256(expected).hexdigest()
    evidence = api.build_event_inventory_evidence(output_receipt(), status="success", inventory_event_seq=4, document=result)
    assert api.validate_inventory_pair(output_receipt(), evidence, status="success") == (output_receipt(), evidence)
    assert len(output_receipt().assistant_message_id) > 64
    assert result.items == ()


@pytest.mark.parametrize("value", [True, "1", 1.0, 2])
def test_nonliteral_inventory_version_refuses(value):
    doc = inventory_document(version=value)
    with pytest.raises(OutputEvidenceError):
        inventory_api().validate_inventory_document(doc)


@pytest.mark.parametrize("delta", [{"observed_items": []}, {"profile": None}, {"root_epoch": None}, {"reason": "scan_incomplete"}])
def test_complete_requires_qualified_closed_positive_shape(delta):
    with pytest.raises(OutputEvidenceError):
        inventory_api().validate_inventory_document(inventory_document(**delta))


@pytest.mark.parametrize("root", ["missing", "unavailable", None])
def test_empty_map_does_not_substitute_for_opened_root(root):
    doc = inventory_document()
    doc["scan"]["after_root"] = root
    with pytest.raises(OutputEvidenceError):
        inventory_api().validate_inventory_document(doc)


def test_incomplete_observed_empty_is_distinct_and_detached():
    api = inventory_api()
    doc = inventory_document(coverage="incomplete", reason="scan_incomplete", observed_items=[])
    del doc["items"]
    doc["scan"] = dict(state="incomplete", source="managed_outputs", scope=None, before_root=None, after_root="missing", reason="unavailable")
    result = api.validate_inventory_document(MappingProxyType(doc))
    assert result.coverage == "incomplete" and result.observed_items == ()
    doc["observed_items"].append({"private": "later mutation"})
    assert result.observed_items == ()
    with pytest.raises(OutputEvidenceError):
        api.validate_inventory_document({**api.inventory_document_to_dict(result), "items": []})


@pytest.mark.parametrize("kwargs", [None, [], {"inventory_evidence": None}, {"inventory_evidence": {}}, {"inventory_evidence": True}])
def test_absence_is_only_missing_mapping_key(kwargs):
    with pytest.raises(OutputEvidenceError):
        inventory_api().inventory_evidence_from_kwargs(kwargs)


def test_absent_evidence_and_mappingproxy_pair_roundtrip():
    api = inventory_api()
    assert api.inventory_evidence_from_kwargs(MappingProxyType({})) is None
    evidence = api.build_event_inventory_evidence(output_receipt(), status="success", inventory_event_seq=4, document=inventory_document())
    saved = MappingProxyType({"inventory_evidence": MappingProxyType(api.inventory_evidence_to_dict(evidence))})
    assert api.inventory_evidence_from_kwargs(saved) == evidence


@pytest.mark.parametrize(
    "reason,output_reason,status",
    [
        ("prestart_not_enumerated", "failed", "error"),
        ("prestart_not_enumerated", "cancelled", "interrupted"),
        ("lease_takeover_not_enumerated", "failed", "error"),
        ("replacement_not_enumerated", "failed", "error"),
        ("cancellation_after_inventory_not_enumerated", "cancelled", "interrupted"),
        ("cancellation_after_inventory_not_enumerated", "cancelled", "error"),
    ],
)
def test_pure_unavailable_pairs_are_not_host_provenance(reason, output_reason, status):
    from deerflow.runtime.runs.output import UnavailableOutputReceipt

    api = inventory_api()
    receipt = UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id="owner", thread_id="thread", run_id="run", input_message_id=None, reason=output_reason)
    evidence = api.build_unavailable_inventory_evidence(receipt, status=status, reason=reason)
    assert api.validate_inventory_pair(receipt, evidence, status=status) == (receipt, evidence)
    assert set(api.inventory_evidence_to_dict(evidence)) == {"version", "kind", "owner_user_id", "thread_id", "run_id", "input_message_id", "output_receipt_sha256", "reason"}


@pytest.mark.parametrize("reason", ["prestart_not_enumerated", "lease_takeover_not_enumerated", "replacement_not_enumerated", "cancellation_after_inventory_not_enumerated"])
def test_unavailable_cannot_certify_success_text(reason):
    with pytest.raises(OutputEvidenceError):
        inventory_api().build_unavailable_inventory_evidence(output_receipt(), status="success", reason=reason)


@pytest.mark.parametrize("field,value", [("owner_user_id", "other"), ("input_message_id", None), ("output_receipt_sha256", "0" * 64)])
def test_exact_pair_join_rejects_changed_identity_or_digest(field, value):
    api = inventory_api()
    evidence = api.inventory_evidence_to_dict(api.build_event_inventory_evidence(output_receipt(), status="success", inventory_event_seq=4, document=inventory_document()))
    evidence[field] = value
    with pytest.raises(OutputEvidenceError):
        api.validate_inventory_pair(output_receipt(), evidence, status="success")


def test_all_witness_bytes_count_in_whole_document_bound():
    api = inventory_api()
    doc = inventory_document()
    doc["items"] = [{"kind": "structured", "carrier": "message_artifact", "value_sha256": "a" * 64, "witness": {"kind": "message_event", "event_seq": 2, "message_id": "x" * 262144}}]
    with pytest.raises(api.InventoryLimitExceeded):
        api.validate_inventory_document(doc)


def test_descriptor_count_bound_and_fragment_depth_nodes():
    api = inventory_api()
    item = {"kind": "structured", "carrier": "message_artifact", "value_sha256": "a" * 64, "witness": {"kind": "message_event", "event_seq": 2, "message_id": "tool"}}
    with pytest.raises(api.InventoryLimitExceeded):
        api.validate_inventory_document(inventory_document(items=[{**item, "value_sha256": f"{i:064x}"} for i in range(257)]))
    fragment = "x"
    for _ in range(12):
        fragment = [fragment]
    api.presentation_fragment_digest(fragment)
    with pytest.raises(api.InventoryLimitExceeded):
        api.presentation_fragment_digest([fragment])
    with pytest.raises(api.InventoryLimitExceeded):
        api.presentation_fragment_digest([0] * 8192)


def test_fragment_types_unicode_and_inert_metadata_have_no_coercion():
    api = inventory_api()
    assert api.presentation_fragment_digest({"v": True}) != api.presentation_fragment_digest({"v": 1})
    assert api.presentation_fragment_digest("é") != api.presentation_fragment_digest("e\u0301")
    for value in ({1: "bad"}, float("nan"), object(), "\ud800"):
        with pytest.raises(OutputEvidenceError):
            api.presentation_fragment_digest(value)


def test_constructed_nested_model_is_revalidated():
    api = inventory_api()
    doc = api.validate_inventory_document(inventory_document())
    forged = doc.model_copy(update={"scan": doc.scan.model_copy(update={"after_root": "missing"})})
    with pytest.raises(OutputEvidenceError):
        api.validate_inventory_document(forged)
    assert api.inventory_document_to_dict(doc) == deepcopy(inventory_document())


def test_complete_cannot_omit_observed_regular_output_delta():
    doc = inventory_document()
    doc["scan"]["produced_paths"] = ["/mnt/user-data/outputs/actual.txt"]
    with pytest.raises(OutputEvidenceError):
        inventory_api().validate_inventory_document(doc)


def test_fragment_bytes_refuse_before_full_encoder(monkeypatch):
    api = inventory_api()
    original = api._encode
    encoded = []

    def encode(value):
        encoded.append(True)
        return original(value)

    monkeypatch.setattr(api, "_encode", encode)
    with pytest.raises(api.InventoryLimitExceeded):
        api.presentation_fragment_digest("x" * (api.MAX_INVENTORY_DOCUMENT_BYTES + 1))
    assert not encoded


def test_fragment_cumulative_and_escaping_bytes_are_bounded():
    api = inventory_api()
    for fragment in (["x" * 131072, "y" * 131072], "\x00" * 45000, "🐍" * 70000):
        with pytest.raises(api.InventoryLimitExceeded):
            api.presentation_fragment_digest(fragment)
    assert api.presentation_fragment_digest("x" * (api.MAX_INVENTORY_DOCUMENT_BYTES - 2))


def test_constructed_invalid_nested_model_does_not_emit_private_warning():
    import warnings

    api = inventory_api()
    doc = api.validate_inventory_document(inventory_document())
    forged = doc.model_copy(update={"scan": doc.scan.model_copy(update={"after_root": {"private": "synthetic-private-proof-body"}})})
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        with pytest.raises(OutputEvidenceError):
            api.validate_inventory_document(forged)
    assert not recorded


def test_overbound_required_envelope_refuses_before_document_encoder(monkeypatch):
    api = inventory_api()
    doc = inventory_document(input_message_id="x" * api.MAX_INVENTORY_DOCUMENT_BYTES)
    encoded = []
    original = api._encode

    def encode(value):
        encoded.append(True)
        return original(value)

    monkeypatch.setattr(api, "_encode", encode)
    with pytest.raises(api.InventoryLimitExceeded):
        api.validate_inventory_document(doc)
    assert not encoded
