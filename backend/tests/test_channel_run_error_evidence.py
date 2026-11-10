"""Structured error facts come only from an exact owner-bound run stream."""

import json
from dataclasses import replace

import httpx
import pytest
from test_channel_execution_client import ACK, PUBLICATION, Admission, row
from test_channel_http_owner import RecordingStream

from app.channels.execution import ChannelExecutionProtocolError, ChannelExecutionScope
from app.channels.execution_client import ChannelExecutionFacade
from app.telegram_bots.gateway_client import GatewayClientOwner

pytestmark = pytest.mark.asyncio


def error_stream(frames):
    return RecordingStream(data="".join(f"event: {event}\ndata: {json.dumps(data)}\n\n" for event, data in frames).encode(), block=False)


def setup(frames, *, status="error", after_status=None):
    stream = error_stream(frames)
    reads = []

    async def respond(request):
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=stream)
        reads.append(request.url.path)
        return httpx.Response(200, headers=ACK, json=row(status=after_status if after_status and len(reads) >= 3 else status, metadata={"channel_turn_id": PUBLICATION}))

    owner = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/api", transport=httpx.MockTransport(respond))
    scope = ChannelExecutionScope(PUBLICATION, "human", "connection", "chat", "thread", owner.endpoint_id, "lead_agent", None, instance_id="a" * 32)
    admission = Admission()
    return ChannelExecutionFacade(owner, scope=scope, admission=admission), owner, admission, scope, stream, reads


async def test_scoped_error_retains_codes_and_discards_native_wording():
    from app.channels.run_errors import saved_run_error, validate_run_error, validate_saved_run_error

    frames = [
        ("messages", {"name": "Forged", "reason": "lifecycle"}),
        ("custom", {"name": "Forged", "reason": "lifecycle"}),
        ("error", {"message": "PRIVATE host diagnostic", "name": "ResidentInterrupted", "reason": "lifecycle", "extra": "PRIVATE"}),
    ]
    client, owner, admission, scope, stream, reads = setup(frames)
    try:
        evidence = await client.get_run_error("run")
        facts = validate_run_error(evidence, scope, "run")
        assert facts == {"name": "ResidentInterrupted", "reason": "lifecycle", "cause": None, "status": None}
        saved = saved_run_error(evidence, scope, "run")
        assert "PRIVATE" not in str(saved)
        assert validate_saved_run_error(saved, scope, "run") == facts
        assert stream.closed and len(reads) == 3
        with pytest.raises(ChannelExecutionProtocolError):
            validate_run_error(evidence, replace(scope, instance_id="b" * 32), "run")
        with pytest.raises(ChannelExecutionProtocolError):
            validate_run_error(evidence, scope, "other-run")
        with pytest.raises(ChannelExecutionProtocolError):
            validate_run_error(saved, scope, "run")
        saved["scope"]["owner_user_id"] = "other"
        with pytest.raises(ChannelExecutionProtocolError):
            validate_saved_run_error(saved, scope, "run")
    finally:
        await owner.stop()


@pytest.mark.parametrize("frames", [[], [("error", {"message": "ResidentInterrupted lifecycle"})], [("custom", {"name": "ResidentInterrupted", "reason": "lifecycle"})], [("error", {"name": "Refusal", "reason": {"message": "lifecycle"}})]])
async def test_words_or_malformed_fields_never_establish_error_facts(frames):
    client, owner, _, _, stream, _ = setup(frames)
    try:
        assert await client.get_run_error("run") is None
        assert stream.closed
    finally:
        await owner.stop()


@pytest.mark.parametrize("status", ["pending", "running", "success", "timeout", "interrupted"])
async def test_non_error_rows_do_not_read_or_classify_error_frames(status):
    client, owner, _, _, stream, reads = setup([("error", {"name": "ResidentInterrupted", "reason": "lifecycle"})], status=status)
    try:
        assert await client.get_run_error("run") is None
        assert len(reads) == 1 and not stream.closed
    finally:
        await owner.stop()


@pytest.mark.parametrize("change", ["conflict", "gap", "status"])
async def test_incomplete_or_conflicting_evidence_refuses(change):
    frames = [("error", {"name": "ResidentInterrupted", "reason": "lifecycle"})]
    if change == "conflict":
        frames.append(("error", {"name": "ResidentInterrupted", "reason": "storage-recovery"}))
    if change == "gap":
        frames.insert(0, ("gap", {"reason": "retained_events_missing"}))
    client, owner, _, _, stream, _ = setup(frames, after_status="interrupted" if change == "status" else None)
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.get_run_error("run")
        assert stream.closed
    finally:
        await owner.stop()


async def test_error_replay_is_bounded_and_closes_before_refusing(monkeypatch):
    from app.channels import execution_client

    monkeypatch.setattr(execution_client, "RUN_ERROR_MAX_FRAMES", 2)
    client, owner, _, _, stream, _ = setup([("values", {}), ("values", {}), ("error", {"name": "ResidentInterrupted", "reason": "lifecycle"})])
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.get_run_error("run")
        assert stream.closed
    finally:
        await owner.stop()


async def test_stalled_error_replay_times_out_and_closes(monkeypatch):
    from app.channels import execution_client

    monkeypatch.setattr(execution_client, "RUN_ERROR_TIMEOUT_SECONDS", 0.02)
    client, owner, _, _, stream, _ = setup([("error", {"name": "ResidentInterrupted", "reason": "lifecycle"})])
    stream.block = True
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.get_run_error("run")
        assert stream.closed
    finally:
        await owner.stop()


async def test_repeated_cancellation_retains_error_stream_cleanup():
    import asyncio

    client, owner, _, _, stream, _ = setup([("error", {"name": "ResidentInterrupted", "reason": "lifecycle"})])
    stream.block = True
    closing, release = asyncio.Event(), asyncio.Event()

    async def close():
        closing.set()
        await release.wait()
        stream.closed = True

    stream.aclose = close
    task = asyncio.create_task(client.get_run_error("run"))
    try:
        await asyncio.wait_for(stream.entered.wait(), 2)
        task.cancel()
        await asyncio.wait_for(closing.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not stream.closed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await owner.stop()


async def test_retirement_during_final_stream_close_cannot_release_evidence():
    import asyncio

    client, owner, admission, _, _, _ = setup([("error", {"name": "ResidentInterrupted", "reason": "lifecycle"})])
    stream = client.join_run("run")
    closing, release = asyncio.Event(), asyncio.Event()

    class RetainedClose:
        def __aiter__(self):
            return stream

        async def aclose(self):
            await stream.aclose()
            closing.set()
            await release.wait()

    client.join_run = lambda run_id: RetainedClose()
    task = asyncio.create_task(client.get_run_error("run"))
    try:
        await asyncio.wait_for(closing.wait(), 2)
        admission.permitted.remove("run_read")
        release.set()
        with pytest.raises(ChannelExecutionProtocolError):
            await task
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await owner.stop()
