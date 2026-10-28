"""Known creating refusal crosses the host bus without trusting model wording."""

import copy
import importlib
from dataclasses import replace
from unittest.mock import AsyncMock

import httpx
import pytest
from test_channel_execution_manager import setup as setup

from app.channels.execution import ChannelExecutionBusyError, ChannelExecutionContext, ChannelExecutionProtocolError
from app.channels.execution_client import CHANNEL_EXECUTION_BUSY_CODE, ChannelExecutionFacade
from app.channels.manager import THREAD_BUSY_MESSAGE
from app.telegram_bots.gateway_client import GatewayClientOwner


def api():
    return importlib.import_module("app.channels.outcomes")


def test_refusal_is_scoped_host_evidence_and_cannot_be_rebuilt_from_data(setup):
    e, module = setup, api()
    proof = module.capture_creation_refusal(e.context, ChannelExecutionBusyError())
    assert module.validate_creation_refusal(proof, e.context.scope) == "thread_busy"
    assert copy.copy(proof) is proof and copy.deepcopy(proof) is proof
    for fake in ({"scope": e.context.scope, "code": "thread_busy"}, ChannelExecutionBusyError()):
        with pytest.raises(ChannelExecutionProtocolError):
            module.validate_creation_refusal(fake, e.context.scope)
    with pytest.raises(TypeError):
        module.CreationRefusal(scope=e.context.scope, code="thread_busy")
    with pytest.raises(ChannelExecutionProtocolError):
        module.validate_creation_refusal(proof, replace(e.context.scope, publication_id="b" * 32))


def test_wording_attribute_impostors_and_retired_attempts_cannot_issue_refusals(setup):
    e, module = setup, api()
    with pytest.raises(ChannelExecutionProtocolError):
        module.capture_creation_refusal(e.context, RuntimeError(THREAD_BUSY_MESSAGE))
    e.admission.allowed = False
    with pytest.raises(ChannelExecutionProtocolError):
        module.capture_creation_refusal(e.context, ChannelExecutionBusyError())


@pytest.mark.asyncio
@pytest.mark.parametrize("streaming", [False, True])
async def test_actual_facade_busy_keeps_original_scope_and_emits_one_host_refusal(setup, monkeypatch, streaming):
    e = setup
    calls = []

    def respond(request):
        calls.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"thread_id": "original-thread", "owner_user_id": "owner", "agent_instance": None, "metadata": {"channel_agent_name": "lead_agent"}})
        return httpx.Response(409, json={"detail": {"code": CHANNEL_EXECUTION_BUSY_CODE}})

    owner = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/prefix/api", transport=httpx.MockTransport(respond))
    scope = replace(e.context.scope, endpoint_id=owner.endpoint_id)
    e.message.execution_context = ChannelExecutionContext(scope, ChannelExecutionFacade(owner, scope=scope, admission=e.admission), e.admission)
    monkeypatch.setattr(e.manager, "_channel_supports_streaming", lambda _channel: streaming)
    lookup = AsyncMock(return_value="a-later-conversation")
    monkeypatch.setattr(e.manager, "_lookup_thread_id", lookup)
    try:
        await e.manager._handle_chat(e.message)
        assert len(e.outbounds) == 1
        result = e.outbounds[0]
        assert result.thread_id == scope.thread_id, "A creating refusal must not follow a newer conversation mapping"
        assert api().validate_creation_refusal(result.creation_refusal, scope) == "thread_busy"
        lookup.assert_not_awaited()
        assert [call.method for call in calls] == ["GET", "POST"]
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_model_quoting_busy_sentence_does_not_issue_creation_refusal(setup):
    e = setup
    e.message.metadata["creation_refusal"] = {"code": "thread_busy"}
    e.client.runs.wait.return_value = {"messages": [{"type": "ai", "content": THREAD_BUSY_MESSAGE}]}
    await e.manager._handle_chat(e.message)
    assert len(e.outbounds) == 1 and e.outbounds[0].text == THREAD_BUSY_MESSAGE
    assert not isinstance(e.outbounds[0], api().CreationRefusedMessage)
