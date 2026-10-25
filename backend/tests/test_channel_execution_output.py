"""Opt-in output capture and strict output reads over the actual HTTP owner."""

import asyncio
import json
from hashlib import sha256

import httpx
import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_channel_execution_client import PUBLICATION, Admission, creating
from test_channel_http_owner import RecordingStream

from app.channels.execution import ChannelExecutionProtocolError, ChannelExecutionScope
from app.channels.execution_client import ChannelExecutionFacade
from app.telegram_bots.gateway_client import GatewayClientOwner

pytestmark = pytest.mark.asyncio
_DEFAULT = object()
ACK = {"X-DeerFlow-Channel-Execution-Version": "1"}
TEXT = "Exact UTF-8: café\n  preserve spaces  "


def client_for(respond, *, capture_output=_DEFAULT):
    owner = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/prefix/api", transport=httpx.MockTransport(respond))
    scope = ChannelExecutionScope(PUBLICATION, "human", "connection", "chat", "thread", owner.endpoint_id, "lead_agent", None)
    admission = Admission()
    options = {} if capture_output is _DEFAULT else {"capture_output": capture_output}
    client = ChannelExecutionFacade(owner, scope=scope, admission=admission, **options)
    return client, owner, admission, scope


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("capture_output", [pytest.param(_DEFAULT, id="default"), False, True])
async def test_host_capture_option_controls_the_actual_detached_post(streaming, capture_output):
    calls = []
    response = RecordingStream(block=False)

    def respond(request):
        calls.append(request)
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=response) if streaming else httpx.Response(200, json={})

    client, owner, _, _ = client_for(respond, capture_output=capture_output)
    try:
        if streaming:
            stream = client.runs.stream("thread", "lead_agent", **creating())
            try:
                await anext(stream)
            finally:
                await stream.aclose()
        else:
            await client.runs.wait("thread", "lead_agent", **creating())
        assert len(calls) == 1
        wire = json.loads(calls[0].content)
        if capture_output is True:
            assert wire["output_capture"] == {"version": 1}
        else:
            assert "output_capture" not in wire
    finally:
        await owner.stop()


async def test_provider_metadata_cannot_activate_default_capture():
    calls = []
    client, owner, _, _ = client_for(lambda request: calls.append(request) or httpx.Response(200, json={}))
    payload = creating()
    payload["metadata"]["output_capture"] = {"version": 1}
    payload["context"] = {"capture_output": True, "output_capture": {"version": 1}}
    try:
        await client.runs.wait("thread", "lead_agent", **payload)
        assert "output_capture" not in json.loads(calls[0].content)
    finally:
        await owner.stop()


@pytest.mark.parametrize("value", [None, 0, 1, "true", {}, []])
async def test_capture_option_requires_a_literal_bool_before_transport(value):
    calls = []
    with pytest.raises(ChannelExecutionProtocolError):
        client_for(lambda request: calls.append(request) or httpx.Response(200, json={}), capture_output=value)
    assert not calls


async def test_capture_option_is_readonly_and_provider_metadata_cannot_disable_it():
    calls = []
    client, owner, _, _ = client_for(lambda request: calls.append(request) or httpx.Response(200, json={}), capture_output=True)
    try:
        with pytest.raises(AttributeError):
            client.capture_output = False
        assert client.capture_output is True
        payload = creating()
        payload["config"] = {"output_capture": None, "context": {"capture_output": False}}
        await client.runs.wait("thread", "lead_agent", **payload)
        assert json.loads(calls[0].content)["output_capture"] == {"version": 1}
    finally:
        await owner.stop()


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("status", [422, 503])
async def test_failed_capture_negotiation_never_retries_an_ordinary_creating_post(streaming, status):
    calls = []
    client, owner, _, _ = client_for(lambda request: calls.append(request) or httpx.Response(status, json={"detail": "Synthetic unsupported capture"}), capture_output=True)
    try:
        with pytest.raises(ChannelExecutionProtocolError, match="^Channel execution protocol refused$"):
            if streaming:
                stream = client.runs.stream("thread", "lead_agent", **creating())
                try:
                    await anext(stream)
                finally:
                    await stream.aclose()
            else:
                await client.runs.wait("thread", "lead_agent", **creating())
        assert len(calls) == 1 and calls[0].method == "POST"
        assert json.loads(calls[0].content)["output_capture"] == {"version": 1}
    finally:
        await owner.stop()


def projection(*, state="complete"):
    channel = {"version": 1, "publication_id": PUBLICATION, "owner_user_id": "human", "thread_id": "thread", "assistant_id": "lead_agent", "agent_name": None}
    receipt = {
        "version": 1,
        "kind": "text",
        "owner_user_id": "human",
        "thread_id": "thread",
        "run_id": "run",
        "input_message_id": "actual-human",
        "assistant_message_id": "actual-assistant",
        "event_seq": 9,
        "text_sha256": sha256(TEXT.encode("utf-8")).hexdigest(),
    }
    return {
        "version": 1,
        "thread_id": "thread",
        "run_id": "run",
        "owner_user_id": "human",
        "run_status": "running" if state == "pending" else "success",
        "state": state,
        "channel_execution": channel,
        "output_evidence": None if state == "pending" else receipt,
        "source": {"text": TEXT} if state == "complete" else None,
        "reason": "source_missing" if state == "unavailable" else None,
    }


@pytest.mark.parametrize("capture_output", [False, True])
@pytest.mark.parametrize("state", ["pending", "complete", "unavailable"])
async def test_output_read_is_an_exact_original_route_and_authorized_projection(state, capture_output):
    from app.gateway.run_output import RunOutputProjection

    calls = []
    client, owner, admission, _ = client_for(lambda request: calls.append(request) or httpx.Response(200, headers=ACK, json=projection(state=state)), capture_output=capture_output)
    try:
        owner.close_run_admission()
        admission.permitted.remove("run_create")
        output = await client.get_run_output("run")
        assert type(output) is RunOutputProjection
        assert output.state == state
        assert output.source.text == TEXT if state == "complete" else output.source is None
        assert admission.calls == ["source_read", "source_read"]
        assert len(calls) == 1 and calls[0].method == "GET"
        assert str(calls[0].url) == "https://runtime.example/prefix/api/threads/thread/runs/run/output"
        assert calls[0].headers["X-DeerFlow-Owner-User-Id"] == "human"
        assert calls[0].headers["X-DeerFlow-Require-Channel-Execution"] == "1"
        with pytest.raises((AttributeError, ValueError)):
            output.run_id = "replacement"
    finally:
        await owner.stop()


@pytest.mark.parametrize("status,reason", [("error", "failed"), ("timeout", "failed"), ("interrupted", "cancelled"), ("error", "cancelled"), ("success", "human_input_required")])
async def test_a_sealed_unavailable_outcome_retains_its_real_closed_reason(status, reason):
    body = projection(state="unavailable")
    body.update(run_status=status, reason=reason)
    body["output_evidence"] = {"version": 1, "kind": "unavailable", "owner_user_id": "human", "thread_id": "thread", "run_id": "run", "input_message_id": None, "reason": reason}
    client, owner, _, _ = client_for(lambda request: httpx.Response(200, headers=ACK, json=body))
    try:
        result = await client.get_run_output("run")
        assert result.state == "unavailable" and result.reason == reason
        assert result.source is None and result.output_evidence.input_message_id is None
        assert not hasattr(result.output_evidence, "assistant_message_id")
    finally:
        await owner.stop()


@pytest.mark.parametrize(
    "status,ack,body,absent", [(404, True, {"detail": "Run not found"}, True), (404, False, {}, False), (503, False, {"detail": "unavailable"}, False), (200, False, projection(), False), (200, True, None, False), (204, True, None, False)]
)
async def test_output_absence_requires_the_supported_exact_read_ack(status, ack, body, absent):
    calls = []
    client, owner, _, _ = client_for(lambda request: calls.append(request) or httpx.Response(status, headers=ACK if ack else {}, json=body))
    try:
        if absent:
            assert await client.get_run_output("run") is None
        else:
            with pytest.raises(ChannelExecutionProtocolError, match="^Channel execution protocol refused$"):
                await client.get_run_output("run")
        assert len(calls) == 1 and calls[0].method == "GET"
    finally:
        await owner.stop()


@pytest.mark.parametrize(
    "path,value",
    [
        (("owner_user_id",), "other"),
        (("thread_id",), "other"),
        (("run_id",), "other"),
        (("channel_execution", "publication_id"), "b" * 32),
        (("channel_execution", "assistant_id"), "other"),
        (("channel_execution", "agent_name"), "other"),
        (("output_evidence", "run_id"), "other"),
        (("output_evidence", "owner_user_id"), "other"),
        (("output_evidence", "assistant_message_id"), ""),
        (("output_evidence", "event_seq"), True),
        (("output_evidence", "text_sha256"), "0" * 64),
        (("source", "text"), TEXT.strip()),
        (("version",), True),
    ],
)
async def test_output_scope_and_exact_source_cannot_be_replaced(path, value):
    body = projection()
    target = body
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    calls = []
    client, owner, _, _ = client_for(lambda request: calls.append(request) or httpx.Response(200, headers=ACK, json=body))
    try:
        with pytest.raises(ChannelExecutionProtocolError, match="^Channel execution protocol refused$"):
            await client.get_run_output("run")
        assert len(calls) == 1
    finally:
        await owner.stop()


async def test_output_read_withdraws_source_access_during_the_response():
    entered, release = asyncio.Event(), asyncio.Event()

    async def respond(request):
        entered.set()
        await release.wait()
        return httpx.Response(200, headers=ACK, json=projection())

    client, owner, admission, _ = client_for(respond)
    reading = asyncio.create_task(client.get_run_output("run"))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        admission.permitted.remove("source_read")
        release.set()
        with pytest.raises(ChannelExecutionProtocolError):
            await reading
    finally:
        release.set()
        await asyncio.gather(reading, return_exceptions=True)
        await owner.stop()


async def test_output_post_response_sql_remains_owned_through_repeated_stop_cancellation(db):
    entered, release, sql_closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    client, owner, admission, _ = client_for(lambda request: httpx.Response(200, headers=ACK, json=projection()))
    actual = admission.authorize
    calls = 0

    async def authorize(operation):
        nonlocal calls
        await actual(operation)
        calls += 1
        if calls == 2:
            async with db.sessions() as session:
                await session.execute(select(1))
                entered.set()
                await release.wait()
            sql_closed.set()

    admission.authorize = authorize
    reading = asyncio.create_task(client.get_run_output("run"))
    closing = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        closing = asyncio.create_task(owner.stop())
        closing.cancel()
        await asyncio.gather(closing, return_exceptions=True)
        closing = asyncio.create_task(owner.stop())
        await asyncio.sleep(0)
        closing.cancel()
        await asyncio.gather(closing, return_exceptions=True)
        closing = asyncio.create_task(owner.stop())
        done, _ = await asyncio.wait({closing}, timeout=0.1)
        assert not done and not sql_closed.is_set()
        release.set()
        await asyncio.wait_for(closing, 3)
        assert sql_closed.is_set()
        (result,) = await asyncio.gather(reading, return_exceptions=True)
        assert isinstance(result, (asyncio.CancelledError, ChannelExecutionProtocolError))
    finally:
        release.set()
        if closing is not None:
            await asyncio.gather(closing, return_exceptions=True)
        await asyncio.gather(reading, return_exceptions=True)
        await owner.stop()
