"""Inventory is a separate opt-in, never a receipt-version or metadata upgrade."""

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from langchain_core.messages import HumanMessage
from pydantic import ValidationError

from app.gateway import services
from app.gateway.run_models import RunCreateRequest
from deerflow.runtime.runs.output import OutputCaptureIntent, OutputEvidenceError, capture_from_kwargs, validate_receipt


def test_creating_intent_two_preserves_the_original_intent_one():
    for version in (1, 2):
        body = RunCreateRequest.model_validate({"output_capture": {"version": version}})
        assert body.output_capture.model_dump() == {"version": version}
        assert capture_from_kwargs({"output_capture": {"version": version}}) == body.output_capture


@pytest.mark.parametrize("version", [True, False, 2.0, "2", None, 0, 3])
def test_inventory_intent_requires_an_exact_supported_integer(version):
    with pytest.raises(ValidationError):
        RunCreateRequest.model_validate({"output_capture": {"version": version}})


@pytest.mark.parametrize("version", [2, True, 1.0, "1"])
def test_accepting_inventory_intent_does_not_widen_receipt_one(version):
    with pytest.raises(OutputEvidenceError):
        validate_receipt(dict(version=version, kind="unavailable", owner_user_id="owner", thread_id="thread", run_id="run", input_message_id=None, reason="failed"))


@pytest.mark.parametrize("run_capability,event_capability", [(False, False), (True, False), (False, True), (1, True), (True, 1)])
def test_inventory_admission_requires_both_actual_literal_capabilities(run_capability, event_capability):
    manager = SimpleNamespace(supports_output_receipts=True, supports_output_inventory=run_capability)
    events = SimpleNamespace(supports_durable_output_evidence=True, supports_durable_output_inventory=event_capability)
    with pytest.raises(HTTPException) as failure:
        services._output_capture_preflight({"version": 2}, run_manager=manager, event_store=events)
    assert failure.value.status_code == 503


def test_inventory_admission_accepts_the_same_retained_qualified_authorities():
    manager = SimpleNamespace(supports_output_receipts=True, supports_output_inventory=True)
    events = SimpleNamespace(supports_durable_output_evidence=True, supports_durable_output_inventory=True)
    assert services._output_capture_preflight({"version": 2}, run_manager=manager, event_store=events).version == 2
    # Existing text-only callers do not require the new capability.
    manager.supports_output_inventory = events.supports_durable_output_inventory = False
    assert services._output_capture_preflight({"version": 1}, run_manager=manager, event_store=events).version == 1


@pytest.mark.parametrize("stored,requested", [(1, 2), (2, 1), (None, 2), (2, None)])
def test_idempotency_never_adopts_or_downgrades_capture_intent(stored, requested):
    kwargs = {} if stored is None else {"output_capture": {"version": stored}}
    intent = None if requested is None else OutputCaptureIntent(version=requested)
    with pytest.raises(HTTPException) as failure:
        services._require_same_output_capture(kwargs, intent)
    assert failure.value.status_code == 409


def test_caller_inventory_proof_claims_do_not_reach_live_run_context():
    forged = {"inventory_evidence": {"kind": "unavailable"}, "output_evidence": {"kind": "text"}, "output_inventory_profile": "terminal-presentation-v1"}
    config = services.build_run_config(thread_id="thread", request_config={"context": {**forged, "legitimate": 7}}, metadata=None)
    assert config["context"].get("legitimate") == 7
    assert not (forged.keys() & config["context"].keys())
    assert not (forged.keys() & config["configurable"].keys())


@pytest.mark.parametrize("trusted", [False, True])
def test_even_internal_input_cannot_supply_inventory_proof_or_profile(trusted):
    claims = {"inventory_evidence": {"kind": "unavailable"}, "output_evidence": {"kind": "text"}, "output_inventory_profile": "terminal-presentation-v1"}
    message = HumanMessage("original", additional_kwargs={**claims, "legitimate": ["kept"]})
    normalized = services._normalize_input_messages([message], location="input.messages", trusted_internal=trusted)[0]
    assert normalized.content == "original"
    assert normalized.additional_kwargs == {"legitimate": ["kept"]}
    assert message.additional_kwargs == {**claims, "legitimate": ["kept"]}
