"""Closed output projections join separate admission and generic output proof."""

from copy import deepcopy
from hashlib import sha256

import pytest


def projection():
    text = "  Exact café\n"
    return {
        "version": 1,
        "owner_user_id": "human",
        "thread_id": "thread",
        "run_id": "run",
        "run_status": "success",
        "state": "complete",
        "channel_execution": {"version": 1, "publication_id": "a" * 32, "owner_user_id": "human", "thread_id": "thread", "assistant_id": "lead_agent", "agent_name": None},
        "output_evidence": {
            "version": 1,
            "kind": "text",
            "owner_user_id": "human",
            "thread_id": "thread",
            "run_id": "run",
            "input_message_id": "actual-human",
            "assistant_message_id": "actual-ai",
            "event_seq": 4,
            "text_sha256": sha256(text.encode("utf-8")).hexdigest(),
        },
        "source": {"text": text},
        "reason": None,
    }


def test_complete_projection_is_frozen_exact_and_detached():
    from app.gateway.run_output import RunOutputProjection

    body = projection()
    value = RunOutputProjection.model_validate(body)
    body["source"]["text"] = "changed"
    assert value.source.text == "  Exact café\n"
    with pytest.raises(Exception):
        value.source.text = "replacement"


@pytest.mark.parametrize(
    "path,value",
    [
        ((), {"extra": True}),
        (("version",), True),
        (("channel_execution", "owner_user_id"), "other"),
        (("channel_execution", "thread_id"), "other"),
        (("channel_execution", "assistant_id"), "other"),
        (("output_evidence", "run_id"), "other"),
        (("output_evidence", "input_message_id"), None),
        (("run_status",), "running"),
        (("source", "text"), "Exact café"),
        (("reason",), "source_missing"),
        (("source", "extra"), True),
    ],
)
def test_projection_refuses_scope_source_and_state_contradictions(path, value):
    from app.gateway.run_output import RunOutputProjection

    body = deepcopy(projection())
    if not path:
        body.update(value)
    else:
        target = body
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value
    with pytest.raises(ValueError):
        RunOutputProjection.model_validate(body)


@pytest.mark.parametrize("state,run_status,reason", [("pending", "pending", None), ("pending", "running", None), ("unavailable", "success", "source_missing"), ("unavailable", "success", "source_changed")])
def test_pending_and_source_loss_preserve_the_closed_shape(state, run_status, reason):
    from app.gateway.run_output import RunOutputProjection

    body = projection()
    body.update(state=state, run_status=run_status, source=None, reason=reason)
    if state == "pending":
        body["output_evidence"] = None
    assert RunOutputProjection.model_validate(body).state == state


def test_failed_before_input_has_no_fabricated_human_or_source():
    from app.gateway.run_output import RunOutputProjection

    body = projection()
    receipt = {key: value for key, value in body["output_evidence"].items() if key not in {"assistant_message_id", "event_seq", "text_sha256"}}
    receipt.update(kind="unavailable", reason="failed", input_message_id=None)
    body.update(run_status="error", state="unavailable", source=None, reason="failed", output_evidence=receipt)
    assert RunOutputProjection.model_validate(body).output_evidence.input_message_id is None


def test_projection_revalidates_a_constructed_admission_model():
    from app.gateway.channel_execution import ChannelExecutionReceipt
    from app.gateway.run_output import RunOutputProjection

    body = projection()
    body["channel_execution"] = ChannelExecutionReceipt.model_construct(**{**body["channel_execution"], "version": True})
    with pytest.raises(ValueError):
        RunOutputProjection.model_validate(body)


def test_invalid_utf8_source_is_a_validation_failure():
    from app.gateway.run_output import RunOutputProjection

    body = projection()
    body["source"]["text"] = "\ud800"
    with pytest.raises(ValueError):
        RunOutputProjection.model_validate(body)
