"""Strict source consistency controls over explicit protocol seeds."""

import importlib
from copy import deepcopy

import pytest
from _telegram_human_input_test_support import record, request, source_basis


def api():
    return importlib.import_module("app.telegram_bots.human_input_source")


def test_actual_root_snapshot_witness_shape_can_qualify_a_current_request():
    assert api().validate_request_basis(record(), source_basis()) == request()


@pytest.mark.parametrize("change", ["artifact", "tool_call", "caller", "run", "witness", "extra_message"])
def test_changed_unpaired_or_foreign_source_cannot_gain_controls(change):
    basis = source_basis()
    if change == "artifact":
        basis["messages"][-1]["content"]["artifact"]["human_input"]["question"] = "Changed question"
    elif change == "tool_call":
        basis["messages"][0]["content"]["tool_calls"][0]["id"] = "different"
    elif change == "caller":
        basis["messages"][0]["metadata"]["caller"] = "subagent"
    elif change == "run":
        basis["messages"][-1]["run_id"] = "other"
    elif change == "witness":
        basis["projection"]["inventory"]["document"]["items"][0]["witness"]["message_id"] = "other"
    else:
        last = deepcopy(basis["messages"][0])
        last["seq"] = 4
        basis["messages"].append(last)
    with pytest.raises(ValueError):
        api().validate_request_basis(record(), basis)


def test_decoder_keeps_all_canonical_choice_text_without_button_truncation():
    value = request(options=[{"id": str(i), "label": "Label " + str(i), "value": "Canonical " + str(i)} for i in range(60)])
    assert api().decode_request(value) == value


@pytest.mark.parametrize("details", [None, {"host": "example.test", "port": True, "ttl_seconds": 300}, {"host": "example.test", "port": 443, "ttl_seconds": 0}])
def test_network_decoder_refuses_malformed_details_without_guessing_from_prose(details):
    with pytest.raises(ValueError):
        api().decode_request(request(source="sandbox_network", input_mode="single_choice", details=details))


def test_auxiliary_delivery_identity_cannot_be_mistaken_for_a_bot_token():
    import re
    from dataclasses import replace

    from app.telegram_bots.token_pattern import TOKEN_BODY

    source = replace(record(), event_id="event:5", publication_id="845d6aa86bd76d7b56a3925d76115582")
    key = api().human_input_step_key(source, "guard-order")
    assert len(key) <= 96
    assert re.search(TOKEN_BODY, key) is None
