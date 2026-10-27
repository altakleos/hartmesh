"""Canonical inventory selector requests; no host qualification is inferred."""

from copy import deepcopy
from types import MappingProxyType

import pytest

from deerflow.runtime.runs import output


def canonical(value):
    return output.capture_intent_to_dict(value)


@pytest.mark.parametrize("version", [1, 2])
def test_old_intent_instances_and_copies_keep_exact_old_encoding(version):
    intent = output.OutputCaptureIntent(version=version)
    for value in (intent, deepcopy(intent), intent.model_copy(deep=True), output.OutputCaptureIntent.model_validate(intent), {"version": version}, MappingProxyType({"version": version})):
        assert canonical(value) == {"version": version}
        assert output.OutputCaptureIntent.model_validate(value).document_version == 1


def test_explicit_document2_round_trips_without_changing_receipt_versions():
    requested = {"version": 2, "inventory_document_version": 2}
    intent = output.OutputCaptureIntent.model_validate(requested)
    assert intent.document_version == 2
    assert canonical(intent) == requested
    assert canonical(intent.model_copy(deep=True)) == requested
    assert canonical(output.capture_from_kwargs({"output_capture": MappingProxyType(requested)})) == requested
    assert requested == {"version": 2, "inventory_document_version": 2}
    with pytest.raises(ValueError):
        output.UnavailableOutputReceipt(version=2, kind="unavailable", owner_user_id="alice", thread_id="thread", run_id="run", input_message_id=None, reason="failed")


@pytest.mark.parametrize("selector", [None, True, False, 1, 2.0, "2", 3, {}, []])
def test_present_selector_accepts_only_literal_integer2(selector):
    with pytest.raises(ValueError):
        output.OutputCaptureIntent.model_validate({"version": 2, "inventory_document_version": selector})


def test_selector_cannot_be_attached_to_capture1_or_unknown_claims():
    for value in ({"version": 1, "inventory_document_version": 2}, {"version": 2, "inventory_document_version": 2, "no_user": True}):
        with pytest.raises(ValueError):
            output.OutputCaptureIntent.model_validate(value)


@pytest.mark.parametrize("update", [{"version": True}, {"version": 3}, {"inventory_document_version": None}, {"inventory_document_version": 1}, {"inventory_document_version": True}, {"inventory_document_version": 2.0}, {"no_user": True}])
def test_encoder_revalidates_model_copy_bypasses(update):
    intent = output.OutputCaptureIntent(version=2).model_copy(update=update)
    with pytest.raises(output.OutputEvidenceError):
        canonical(intent)


def test_encoder_rejects_malformed_direct_values_with_fixed_error():
    for value in (None, [], {}, {"version": 2, "inventory_document_version": None}, {"version": 2, "inventory_document_version": 2, "proof": "forged"}):
        with pytest.raises(output.OutputEvidenceError, match="^Run output evidence is unavailable$"):
            canonical(value)


def test_request_schema_advertises_only_the_optional_literal2_selector():
    schema = output.OutputCaptureIntent.model_json_schema()
    assert schema["required"] == ["version"]
    selector = schema["properties"]["inventory_document_version"]
    assert selector["type"] == "integer" and type(selector["const"]) is int and selector["const"] == 2
    assert "anyOf" not in selector and "default" not in selector
