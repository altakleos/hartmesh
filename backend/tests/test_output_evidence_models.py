"""Strict, detached data for generic output evidence; no channel policy."""

import hashlib
import importlib
from copy import deepcopy

import pytest

from deerflow.runtime.runs.schemas import RunStatus


@pytest.fixture
def output():
    return importlib.import_module("deerflow.runtime.runs.output")


def text_receipt(**changes):
    return {
        "version": 1,
        "kind": "text",
        "owner_user_id": "human:opaque/owner",
        "thread_id": "original-thread",
        "run_id": "original-run",
        "input_message_id": None,
        "assistant_message_id": "actual-model-message",
        "event_seq": 3,
        "text_sha256": hashlib.sha256(b"Actual answer").hexdigest(),
        **changes,
    }


def test_receipt_is_scoped_and_frozen_without_a_fabricated_human(output):
    receipt = output.validate_receipt(text_receipt())
    assert receipt.owner_user_id == "human:opaque/owner" and receipt.input_message_id is None
    assert receipt.model_dump() == text_receipt()
    with pytest.raises(Exception):
        receipt.run_id = "different-run"


@pytest.mark.parametrize(
    "changes",
    [
        {"version": True},
        {"version": 1.0},
        {"version": "1"},
        {"version": 2},
        {"event_seq": True},
        {"event_seq": 0},
        {"event_seq": "3"},
        {"assistant_message_id": ""},
        {"text_sha256": "A" * 64},
        {"kind": "unknown"},
        {"publication_id": "a" * 32},
    ],
)
def test_receipts_refuse_coercion_unknown_versions_or_channel_fields(output, changes):
    with pytest.raises(output.OutputEvidenceError, match="^Run output evidence is unavailable$"):
        output.validate_receipt(text_receipt(**changes))


@pytest.mark.parametrize("value", [None, [], "", 0, False])
def test_present_malformed_receipt_is_not_legacy_absence(output, value):
    with pytest.raises(output.OutputEvidenceError):
        output.receipt_from_kwargs({"output_evidence": value})
    assert output.receipt_from_kwargs({}) is None


@pytest.mark.parametrize("value", [None, {}, {"version": True}, {"version": 1, "extra": "not-allowed"}])
def test_present_capture_intent_is_strict_and_cannot_be_guessed(output, value):
    with pytest.raises(output.OutputEvidenceError):
        output.capture_from_kwargs({"output_capture": value})
    assert output.capture_from_kwargs({}) is None
    assert output.capture_from_kwargs({"output_capture": {"version": 1}}).model_dump() == {"version": 1}


def test_closed_unavailable_receipt_has_no_synthetic_output_identity(output):
    data = {key: value for key, value in text_receipt(kind="unavailable", reason="cancelled").items() if key not in {"assistant_message_id", "event_seq", "text_sha256"}}
    receipt = output.validate_receipt(data)
    assert receipt.model_dump() == data
    with pytest.raises(output.OutputEvidenceError):
        output.validate_receipt({**data, "assistant_message_id": "made-up"})


def test_snapshot_cannot_mutate_the_store_or_be_mutated_through_nested_data(output):
    source = {"output_capture": {"version": 1}, "input": {"messages": [{"content": "original"}]}}
    before = deepcopy(source)
    snapshot = output.RunOutputSnapshot(run_id="original-run", thread_id="original-thread", owner_user_id="human:opaque/owner", assistant_id=" CUSTOM_AGENT ", status=RunStatus.running, kwargs=source, local_finalizing=True)
    source["input"]["messages"][0]["content"] = "mutated"
    assert snapshot.kwargs["input"]["messages"][0]["content"] == before["input"]["messages"][0]["content"]
    with pytest.raises(TypeError):
        snapshot.kwargs["output_capture"]["version"] = 2
    assert snapshot.assistant_id == " CUSTOM_AGENT "


def test_text_digest_preserves_exact_whitespace_and_unicode(output):
    text = "  A\ne\u0301  "
    assert output.canonical_text_digest(text) == hashlib.sha256(text.encode("utf-8")).hexdigest()
    assert output.canonical_text_digest(text) != output.canonical_text_digest(text.strip())


@pytest.mark.parametrize("status", [RunStatus.success, "success"])
def test_text_receipt_requires_success(output, status):
    assert output.validate_receipt_status(text_receipt(), status).kind == "text"


@pytest.mark.parametrize("status", ["pending", "running", "error", "timeout", "interrupted", "unknown", None])
def test_text_receipt_rejects_contradictory_status(output, status):
    with pytest.raises(output.OutputEvidenceError):
        output.validate_receipt_status(text_receipt(), status)


@pytest.mark.parametrize(
    "reason,allowed",
    [
        ("failed", {"error", "timeout"}),
        ("cancelled", {"interrupted", "error"}),
        ("unsupported_input", {"success"}),
        ("unsupported_output", {"success"}),
        ("human_input_required", {"success"}),
        ("ambiguous_source", {"success"}),
        ("missing_message_identity", {"success"}),
    ],
)
def test_unavailable_reason_is_bound_to_actual_terminal_status(output, reason, allowed):
    data = {key: value for key, value in text_receipt(kind="unavailable", reason=reason).items() if key not in {"assistant_message_id", "event_seq", "text_sha256"}}
    for status in ("pending", "running", "success", "error", "timeout", "interrupted"):
        if status in allowed:
            assert output.validate_receipt_status(data, status).reason == reason
        else:
            with pytest.raises(output.OutputEvidenceError):
                output.validate_receipt_status(data, status)


def test_opaque_raw_identity_round_trips_without_normalizing_it(output):
    data = text_receipt(owner_user_id=" owner/raw ", assistant_message_id="provider-id ", input_message_id="human-id\n")
    assert output.receipt_to_dict(data) == data
    snapshot = output.RunOutputSnapshot(run_id="run/raw ", thread_id="thread/raw ", owner_user_id=" owner/raw ", assistant_id=None, status=RunStatus.running, kwargs={}, local_finalizing=False)
    assert snapshot.owner_user_id == data["owner_user_id"]


def test_receipt_and_capture_helpers_accept_the_public_frozen_snapshot(output):
    snapshot = output.RunOutputSnapshot(
        run_id="original-run",
        thread_id="original-thread",
        owner_user_id="human:opaque/owner",
        assistant_id=None,
        status=RunStatus.success,
        kwargs={"output_capture": {"version": 1}, "output_evidence": text_receipt()},
        local_finalizing=False,
    )
    assert output.capture_from_kwargs(snapshot.kwargs).version == 1
    assert output.receipt_to_dict(output.receipt_from_kwargs(snapshot.kwargs)) == text_receipt()
