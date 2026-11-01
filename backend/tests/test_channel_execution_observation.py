"""Exact publication reads/cancellation over the existing recorded HTTP owner."""

import asyncio
import json

import httpx
import pytest
from test_channel_execution_client import ACK, PUBLICATION, facade, row

from app.channels.execution import ChannelExecutionProtocolError

pytestmark = pytest.mark.asyncio


def run_row(**changes):
    return row(status="running", metadata={"channel_turn_id": PUBLICATION}, **changes)


async def test_cancel_borrows_original_owner_and_needs_no_create_permission():
    calls = []

    def respond(request):
        calls.append(request)
        if request.method == "GET":
            return httpx.Response(200, json=run_row(), headers=ACK)
        return httpx.Response(202)

    client, owner, admission, _ = facade(respond)
    admission.permitted.discard("run_create")
    admission.permitted.add("run_cancel")
    try:
        assert await client.cancel_run("run") == "requested"
        assert [(call.method, call.url.path) for call in calls] == [("GET", "/prefix/api/threads/thread/runs/run"), ("POST", "/prefix/api/threads/thread/runs/run/cancel")]
        assert dict(calls[-1].url.params) == {"wait": "false", "action": "interrupt"}
        assert calls[-1].content == b""
        assert "run_create" not in admission.calls and "run_cancel" in admission.calls
    finally:
        await owner.stop()


@pytest.mark.parametrize("stop_reason, outcome", [(None, "requested"), ("gateway_stopped", "terminal")])
async def test_cancel_of_a_run_already_interrupted_says_whether_it_was_cancelled_and_posts_nothing(stop_reason, outcome):
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, json={**run_row(), "status": "interrupted", "stop_reason": stop_reason}, headers=ACK)

    client, owner, admission, _ = facade(respond)
    admission.permitted.discard("run_create")
    admission.permitted.add("run_cancel")
    try:
        assert await client.cancel_run("run") == outcome
        assert not any(call.method == "POST" for call in calls)
    finally:
        await owner.stop()


@pytest.mark.parametrize("change", ["publication", "metadata", "missing_ack", "cancel_permission", "read_permission"])
async def test_cancel_refuses_unproved_or_unauthorized_run_without_post(change):
    calls = []

    def respond(request):
        calls.append(request)
        data = run_row()
        if change == "publication":
            data["kwargs"]["channel_execution"]["publication_id"] = "b" * 32
        elif change == "metadata":
            data["metadata"]["channel_turn_id"] = "b" * 32
        return httpx.Response(200, json=data, headers={} if change == "missing_ack" else ACK)

    client, owner, admission, _ = facade(respond)
    if change != "cancel_permission":
        admission.permitted.add("run_cancel")
    if change == "read_permission":
        admission.permitted.discard("run_read")
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.cancel_run("run")
        assert not any(call.method == "POST" for call in calls)
    finally:
        await owner.stop()


async def test_join_is_get_and_resumes_only_original_run_without_creating():
    calls = []

    def respond(request):
        calls.append(request)
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=b'id: 2-0\nevent: custom\ndata: {"value":"advisory"}\n\n')
        return httpx.Response(200, json=run_row(), headers=ACK)

    client, owner, admission, _ = facade(respond)
    admission.permitted.discard("run_create")
    stream = client.join_run("run", after_event_id="1-0")
    try:
        part = await anext(stream)
        assert part.event == "custom" and part.id == "2-0"
        assert part.data == {"value": "advisory"}
        assert all(call.method == "GET" for call in calls)
        assert calls[-1].url.path == "/prefix/api/threads/thread/runs/run/join"
        assert calls[-1].headers["Last-Event-ID"] == "1-0"
        assert "run_create" not in admission.calls
    finally:
        await stream.aclose()
        await owner.stop()


async def test_join_drops_a_queued_part_after_original_read_permission_retires():
    def respond(request):
        if request.url.path.endswith("/join"):
            data = "".join(f"id: {number}-0\nevent: custom\ndata: {json.dumps({'number': number})}\n\n" for number in range(1, 4))
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=data.encode())
        return httpx.Response(200, json=run_row(), headers=ACK)

    client, owner, admission, _ = facade(respond)
    stream = client.join_run("run")
    try:
        assert (await anext(stream)).data == {"number": 1}
        await asyncio.sleep(0)
        admission.permitted.discard("run_read")
        with pytest.raises(ChannelExecutionProtocolError):
            await anext(stream)
    finally:
        await stream.aclose()
        await owner.stop()


@pytest.mark.parametrize("result", ["matched", "absent", "foreign", "metadata", "missing_ack", "unsupported", "legacy_miss"])
async def test_indexed_lookup_is_exact_read_only_and_never_pages_or_creates(result):
    calls = []

    def respond(request):
        calls.append(request)
        data = run_row()
        if result == "foreign":
            data["kwargs"]["channel_execution"]["publication_id"] = "b" * 32
        if result == "metadata":
            data["metadata"]["channel_turn_id"] = "b" * 32
        if result == "absent":
            data = {"detail": "Run not found", "lookup": "idempotency-key"}
        elif result == "legacy_miss":
            data = {"detail": "Run not found"}
        return httpx.Response(404 if result in {"absent", "legacy_miss"} else 503 if result == "unsupported" else 200, json=data, headers={} if result in {"missing_ack", "unsupported"} else ACK)

    client, owner, admission, _ = facade(respond)
    admission.permitted.discard("run_create")
    try:
        if result in {"matched", "absent"}:
            found = await client.lookup_run()
            assert (found is None) == (result == "absent")
        else:
            with pytest.raises(ChannelExecutionProtocolError):
                await client.lookup_run()
        assert len(calls) == 1 and calls[0].method == "GET"
        assert calls[0].url.path == "/prefix/api/threads/thread/runs/by-idempotency-key"
        assert calls[0].headers["Idempotency-Key"] == PUBLICATION
        assert calls[0].headers["X-DeerFlow-Require-Channel-Execution"] == "1"
        assert "run_create" not in admission.calls
    finally:
        await owner.stop()


async def test_read_only_join_closes_actual_response_on_cancel_and_keeps_borrowed_owner():
    from test_channel_http_owner import RecordingStream

    body = RecordingStream()

    def respond(request):
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=body)
        return httpx.Response(200, headers=ACK, json=run_row())

    client, owner, admission, _ = facade(respond)
    admission.permitted.discard("run_create")
    owner.close_run_admission()
    stream = client.join_run("run")
    try:
        assert (await anext(stream)).event == "values"
        await asyncio.wait_for(body.entered.wait(), 3)
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert body.closed and not owner.closed
        assert not owner._responses and not owner._streams
        assert (await client.get_run("run")).row["run_id"] == "run"
    finally:
        await stream.aclose()
        await owner.stop()
