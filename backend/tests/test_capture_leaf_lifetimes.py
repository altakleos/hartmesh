"""Each source page is a fresh admission under the actual host owner."""

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _run_output_capture_test_support import put_messages
from _run_output_store_test_support import RAW_OWNER
from langchain_core.messages import AIMessage, HumanMessage

from deerflow.runtime.runs.capture_io import CaptureIOClosed, CaptureIOOwner, capture_io_scope
from deerflow.runtime.runs.output import TextOutputReceipt, canonical_text_digest
from deerflow.runtime.runs.output_source import read_durable_output_source


@pytest.mark.asyncio
@pytest.mark.parametrize("retire_at", ["before", "after_page", "open"])
async def test_source_pages_refuse_retired_admission_without_fabricating_absence(capture_environment, monkeypatch, retire_at):
    e = capture_environment
    await put_messages(e, [HumanMessage(id="source-human", content="input").model_dump()])
    await put_messages(e, [AIMessage(id=f"earlier-{i}", content="earlier").model_dump() for i in range(260)])
    rows = await put_messages(e, [AIMessage(id="source-answer", content="exact source").model_dump()])
    receipt = TextOutputReceipt(
        version=1, kind="text", owner_user_id=RAW_OWNER, thread_id="thread", run_id="run", input_message_id="source-human", assistant_message_id="source-answer", event_seq=rows[-1]["seq"], text_sha256=canonical_text_digest("exact source")
    )
    owner, calls = CaptureIOOwner(), []
    original = e.event_store.list_messages_by_run

    async def observed(*args, **kwargs):
        result = await original(*args, **kwargs)
        calls.append(kwargs["after_seq"])
        if retire_at == "after_page" and len(calls) == 1:
            assert len(result) == 256
            owner.retire()
        return result

    monkeypatch.setattr(e.event_store, "list_messages_by_run", observed)
    if retire_at == "before":
        owner.retire()
    result, failure = None, None
    try:
        with capture_io_scope(owner):
            try:
                result = await read_durable_output_source(e.event_store, receipt=receipt)
            except Exception as error:
                failure = error
        expected = [] if retire_at == "before" else [0] if retire_at == "after_page" else [0, 256]
        assert calls == expected, "A later source page reached SQL after its host owner retired"
        if retire_at == "open":
            assert failure is None and result.state == "matched" and result.text == "exact source"
        else:
            assert isinstance(failure, CaptureIOClosed) and result is None
    finally:
        owner.retire()
        await owner.drain()
