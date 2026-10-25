"""The versioned creating body and server receipt are a closed wire contract."""

import importlib

import pytest
from pydantic import ValidationError

from app.gateway.run_models import RunCreateRequest

PUBLICATION = "a" * 32


def test_creating_request_accepts_typed_v1_publication_without_changing_legacy_defaults():
    body = RunCreateRequest.model_validate({"channel_execution": {"version": 1, "publication_id": PUBLICATION}})
    assert body.channel_execution.model_dump(mode="json") == {"version": 1, "publication_id": PUBLICATION}
    assert RunCreateRequest().channel_execution is None
    assert RunCreateRequest().on_disconnect == "cancel"


@pytest.mark.parametrize("version", [True, False, "1", 1.0, 0, 2, None])
def test_creating_version_never_coerces_an_unsupported_value(version):
    with pytest.raises(ValidationError):
        RunCreateRequest.model_validate({"channel_execution": {"version": version, "publication_id": PUBLICATION}})


@pytest.mark.parametrize("publication", [None, 123, "", "a" * 31, "a" * 33, "A" * 32, "g" * 32, " " + PUBLICATION])
def test_creating_publication_is_a_strict_lowercase_hex_string(publication):
    with pytest.raises(ValidationError):
        RunCreateRequest.model_validate({"channel_execution": {"version": 1, "publication_id": publication}})


def test_creating_protocol_refuses_unknown_fields():
    with pytest.raises(ValidationError):
        RunCreateRequest.model_validate({"channel_execution": {"version": 1, "publication_id": PUBLICATION, "owner_user_id": "caller-owned"}})


@pytest.fixture
def wire():
    return importlib.import_module("app.gateway.channel_execution")


def receipt_document(**changes):
    return {"version": 1, "publication_id": PUBLICATION, "owner_user_id": "real-human", "thread_id": "original-thread", "assistant_id": "lead_agent", "agent_name": None, **changes}


def test_receipt_is_frozen_and_retains_exact_identifiers(wire):
    value = wire.ChannelExecutionReceipt.model_validate(receipt_document(agent_name="actual-agent"))
    assert value.model_dump(mode="json") == receipt_document(agent_name="actual-agent")
    with pytest.raises(ValidationError):
        value.owner_user_id = "changed"


def test_only_absent_receipt_key_is_a_legacy_nonmatch(wire):
    assert wire.receipt_from_run_kwargs({"input": {}}) is None
    assert wire.receipt_from_run_kwargs({"channel_execution": receipt_document()}).model_dump(mode="json") == receipt_document()


@pytest.mark.parametrize("value", [None, [], "not-a-record", {"channel_execution": None}, {"channel_execution": {}}, {"channel_execution": receipt_document(version=True)}, {"channel_execution": receipt_document(extra="unexpected")}])
def test_malformed_present_proof_or_nonmapping_kwargs_is_never_absence(wire, value):
    with pytest.raises(wire.ChannelExecutionProofError):
        wire.receipt_from_run_kwargs(value)


@pytest.mark.parametrize("field,value", [("owner_user_id", ""), ("thread_id", None), ("assistant_id", 123), ("agent_name", ""), ("agent_name", 12)])
def test_receipt_scope_fields_never_coerce_or_default(wire, field, value):
    with pytest.raises(ValidationError):
        wire.ChannelExecutionReceipt.model_validate(receipt_document(**{field: value}))


def test_wire_helper_has_no_gateway_runtime_or_channel_dependencies(wire):
    import ast
    from pathlib import Path

    source = ast.parse(Path(wire.__file__).read_text(encoding="utf-8"))
    imports = [node.module or "" for node in ast.walk(source) if isinstance(node, ast.ImportFrom)]
    assert not any(name.startswith(("app.channels", "app.telegram_bots", "app.gateway.services", "app.gateway.routers", "deerflow.runtime")) for name in imports)
