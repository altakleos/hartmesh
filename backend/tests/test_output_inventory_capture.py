"""The host publishes once and audits the actual immutable singleton it wrote."""

from types import SimpleNamespace

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store

from deerflow.runtime.runs.output import OutputEvidenceError, UnavailableOutputReceipt
from deerflow.runtime.runs.output_inventory import output_receipt_digest
from deerflow.runtime.runs.output_inventory_capture import OutputInventoryCapture, persist_output_inventory
from deerflow.runtime.user_context import reset_current_user, set_current_user


def receipt():
    return UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id="inventory-owner", thread_id="inventory-thread", run_id="inventory-run", input_message_id="human", reason="failed")


def unsupported_document():
    value = receipt()
    return dict(
        version=1,
        profile=None,
        owner_user_id=value.owner_user_id,
        thread_id=value.thread_id,
        run_id=value.run_id,
        input_message_id=value.input_message_id,
        output_receipt_sha256=output_receipt_digest(value),
        root_epoch=None,
        coverage="unsupported",
        reason="unsupported_profile",
        observed_items=[],
        scan=None,
    )


def test_unqualified_worker_cannot_claim_an_empty_supported_inventory():
    capture = OutputInventoryCapture()
    document = capture.document(receipt(), status="error", before=None, after=None, runtime_presented_files=None)
    assert document.coverage == "unsupported" and document.reason == "unsupported_profile"
    assert document.root_epoch is None and document.profile is None
    assert not hasattr(document, "items") and document.observed_items == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_single_actual_append_with_exact_readonly_recovery(capture_environment, monkeypatch, lost_reply):
    store = capture_environment.event_store
    actual = store.put_if_absent
    writes = []

    async def write(**kwargs):
        writes.append(kwargs)
        result = await actual(**kwargs)
        if lost_reply:
            raise OSError("Actual commit reply lost")
        return result

    monkeypatch.setattr(store, "put_if_absent", write)
    token = set_current_user(SimpleNamespace(id=receipt().owner_user_id))
    try:
        proof = await persist_output_inventory(store, output_receipt=receipt(), status="error", document=unsupported_document())
    finally:
        reset_current_user(token)
    assert len(writes) == 1
    rows = await store.list_events(receipt().thread_id, receipt().run_id, event_types=["run.output_inventory"], limit=2, user_id=receipt().owner_user_id)
    assert len(rows) == 1 and rows[0]["seq"] == proof.inventory_event_seq
    from test_run_event_stream_contract import _assert_fixed_event_valid

    _assert_fixed_event_valid(rows[0], persisted=True)
    assert proof.output_receipt_sha256 == output_receipt_digest(receipt())


@pytest.mark.asyncio
async def test_preexisting_different_singleton_is_never_adopted_or_replaced(capture_environment):
    store = capture_environment.event_store
    token = set_current_user(SimpleNamespace(id=receipt().owner_user_id))
    try:
        changed = {**unsupported_document(), "reason": "unsupported_surface"}
        original = await store.put(thread_id=receipt().thread_id, run_id=receipt().run_id, event_type="run.output_inventory", category="outputs", content=changed)
        with pytest.raises(OutputEvidenceError):
            await persist_output_inventory(store, output_receipt=receipt(), status="error", document=unsupported_document())
    finally:
        reset_current_user(token)
    rows = await store.list_events(receipt().thread_id, receipt().run_id, event_types=["run.output_inventory"], limit=2, user_id=receipt().owner_user_id)
    assert rows == [original]


@pytest.mark.asyncio
async def test_unknown_append_and_failed_recovery_never_retry_the_append(capture_environment, monkeypatch):
    writes = []

    async def unknown(**kwargs):
        writes.append(kwargs)
        raise OSError("Unknown append")

    async def unavailable(*args, **kwargs):
        raise OSError("No exact readback")

    store = capture_environment.event_store
    monkeypatch.setattr(store, "put_if_absent", unknown)
    monkeypatch.setattr(store, "list_events", unavailable)
    with pytest.raises(OutputEvidenceError):
        await persist_output_inventory(store, output_receipt=receipt(), status="error", document=unsupported_document())
    assert len(writes) == 1
