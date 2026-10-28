"""The real creating service stamps only a validated human publication."""

import asyncio
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from _channel_execution_test_support import gateway_environment as gateway_environment
from fastapi import HTTPException
from langchain_core.messages import HumanMessage

from app.gateway import services
from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
from app.gateway.run_models import RunCreateRequest

PUBLICATION = "a" * 32


def body(**changes):
    values = {"assistant_id": "lead_agent", "input": {"messages": [{"role": "human", "content": "Synthetic question"}]}, "channel_execution": {"version": 1, "publication_id": PUBLICATION}, "on_disconnect": "continue"}
    return RunCreateRequest.model_validate({**values, **changes})


async def launched(e, request_body, *, key="publication-key"):
    record = await services.start_run(request_body, e.thread_id, e.request, idempotency_key=key)
    if record.task is not None:
        await record.task
    return record


@pytest.mark.asyncio
async def test_receipt_and_actual_human_marker_derive_from_server_scope(gateway_environment):
    e = gateway_environment
    record = await launched(e, body())
    assert record.kwargs["channel_execution"] == {"version": 1, "publication_id": PUBLICATION, "owner_user_id": e.owner_id, "thread_id": e.thread_id, "assistant_id": "lead_agent", "agent_name": None}
    assert record.metadata["channel_turn_id"] == PUBLICATION
    actual = e.worker_inputs[0]["graph_input"]["messages"][0]
    assert actual.additional_kwargs["channel_turn_id"] == PUBLICATION
    assert record.kwargs["input"]["messages"][0]["additional_kwargs"]["channel_turn_id"] == PUBLICATION
    assert record.user_id == e.owner_id


def input_response(**changes):
    return {"version": 1, "kind": "human_input_response", "source": "ask_clarification", "request_id": "request-choice", "response_kind": "option", "option_id": "first", "value": "First option", **changes}


@pytest.mark.asyncio
async def test_canonical_response_reaches_worker_with_the_new_publication(gateway_environment):
    e = gateway_environment
    response = input_response()
    record = await launched(e, body(input={"messages": [{"role": "human", "content": response["value"], "additional_kwargs": {"human_input_response": response}}]}))
    actual = e.worker_inputs[0]["graph_input"]["messages"][0]
    assert actual.additional_kwargs == {"human_input_response": response, "channel_turn_id": PUBLICATION}
    assert record.user_id == e.owner_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response,hidden,extra_human",
    [(input_response(version=True), False, False), (input_response(unexpected="extra"), False, False), (input_response(option_id=""), False, False), (input_response(), True, False), (input_response(), False, True)],
)
async def test_noncanonical_or_hidden_response_is_refused_before_worker(gateway_environment, response, hidden, extra_human):
    e = gateway_environment
    messages = [{"role": "human", "content": "First option", "additional_kwargs": {"human_input_response": response, **({"hide_from_ui": True} if hidden else {})}}]
    if extra_human:
        messages.append({"role": "human", "content": "Unrelated input"})
    with pytest.raises(HTTPException) as failure:
        await launched(e, body(input={"messages": messages}))
    assert failure.value.status_code == 422
    assert not e.worker_inputs


@pytest.mark.asyncio
async def test_supplied_markers_are_scrubbed_and_only_admitted_human_is_stamped(gateway_environment):
    e = gateway_environment
    supplied = {"messages": [{"role": "human", "content": "Synthetic question", "additional_kwargs": {"channel_turn_id": "f" * 32, "original_user_content": "Original question", "files": [{"path": "uploads/fixture.txt"}]}}]}
    request_body = body(input=supplied, metadata={"channel_turn_id": "e" * 32, "kept": "metadata"}, config={"metadata": {"channel_turn_id": "d" * 32, "kept_config": "yes"}})
    before = deepcopy(request_body.model_dump(mode="json"))
    record = await launched(e, request_body)
    actual = e.worker_inputs[0]["graph_input"]["messages"][0]
    assert actual.additional_kwargs == {"channel_turn_id": PUBLICATION, "original_user_content": "Original question", "files": [{"path": "uploads/fixture.txt"}]}
    assert record.metadata["channel_turn_id"] == PUBLICATION and record.metadata["kept"] == "metadata"
    assert "channel_turn_id" not in record.kwargs["config"]["metadata"]
    assert record.kwargs["config"]["metadata"]["kept_config"] == "yes"
    assert e.worker_inputs[0]["config"]["metadata"]["channel_turn_id"] == PUBLICATION
    assert request_body.model_dump(mode="json") == before


@pytest.mark.asyncio
async def test_ordinary_internal_request_cannot_persist_a_publication_identity(gateway_environment):
    e = gateway_environment
    request_body = RunCreateRequest(input={"messages": [{"role": "human", "content": "ordinary"}]}, metadata={"channel_turn_id": PUBLICATION}, config={"metadata": {"channel_turn_id": PUBLICATION}})
    record = await launched(e, request_body)
    assert "channel_turn_id" not in record.metadata
    assert "channel_turn_id" not in record.kwargs["config"]["metadata"]
    assert "channel_execution" not in record.kwargs


@pytest.mark.asyncio
async def test_noninternal_creating_proof_is_refused_before_worker(gateway_environment):
    e = gateway_environment
    e.request.state.user = e.owner
    e.request.state.auth_source = AUTH_SOURCE_SESSION
    with pytest.raises(HTTPException) as failure:
        await launched(e, body())
    assert failure.value.status_code == 403
    assert not e.worker_inputs


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"command": {"resume": "answer"}},
        {"checkpoint_id": "older-checkpoint"},
        {"input": None},
        {"input": {"messages": []}},
        {"input": {"messages": [{"role": "human", "content": "hidden", "additional_kwargs": {"hide_from_ui": True}}]}},
        {"input": {"messages": [{"role": "human", "name": "summary", "content": "framework"}]}},
        {"input": {"messages": [{"role": "human", "content": "reply", "additional_kwargs": {"human_input_response": None}}]}},
        {"input": {"messages": [{"role": "human", "content": "one"}, {"role": "human", "content": "two"}]}},
        {"on_disconnect": "cancel"},
        {"context": {"is_bootstrap": True}},
    ],
)
async def test_unsupported_initial_meanings_are_refused_before_instance_preparation(gateway_environment, monkeypatch, changes):
    from app.gateway import agent_conversations

    e = gateway_environment
    calls = []

    async def no_preparation(*args, **kwargs):
        calls.append(True)
        raise AssertionError("Unsupported v1 must be refused before instance readiness/preparation")

    monkeypatch.setattr(agent_conversations, "execution_for_run", no_preparation)
    with pytest.raises(HTTPException) as failure:
        await launched(e, body(**changes))
    assert failure.value.status_code == 422
    assert not calls and not e.worker_inputs


@pytest.mark.asyncio
async def test_instance_binding_is_refused_without_execution_or_readiness_effects(gateway_environment, monkeypatch):
    from app.gateway import agent_conversations

    e = gateway_environment
    calls = []

    async def bound(_thread):
        return "captured-instance"

    async def no_preparation(*args, **kwargs):
        calls.append(True)
        raise AssertionError("Creating v1 does not prepare instance execution")

    monkeypatch.setattr(e.bindings, "binding", bound)
    monkeypatch.setattr(agent_conversations, "execution_for_run", no_preparation)
    with pytest.raises(HTTPException) as failure:
        await launched(e, body())
    assert failure.value.status_code == 409
    assert not calls and not e.worker_inputs


@pytest.mark.asyncio
async def test_missing_binding_authority_is_not_proof_of_an_ordinary_thread(gateway_environment):
    e = gateway_environment
    e.request.app.state.agent_conversations = None
    with pytest.raises(HTTPException) as failure:
        await launched(e, body())
    assert failure.value.status_code == 503
    assert not e.worker_inputs


@pytest.mark.asyncio
async def test_exact_replay_reuses_one_run_and_rejects_changed_publication_or_input(gateway_environment):
    e = gateway_environment
    first = await launched(e, body())
    same = await launched(e, body())
    assert first.run_id == same.run_id and len(e.worker_inputs) == 1
    for changed in [body(channel_execution={"version": 1, "publication_id": "b" * 32}), body(input={"messages": [{"role": "human", "content": "different"}]})]:
        with pytest.raises(HTTPException) as failure:
            await launched(e, changed)
        assert failure.value.status_code == 409
    assert len(e.worker_inputs) == 1


@pytest.mark.asyncio
async def test_stamping_never_mutates_a_caller_owned_message_object(gateway_environment):
    message = HumanMessage(content="Synthetic question", additional_kwargs={"kept": "yes"})
    record = await launched(gateway_environment, body(input={"messages": [message]}))
    assert message.additional_kwargs == {"kept": "yes"}
    assert record.kwargs["input"]["messages"][0]["additional_kwargs"]["channel_turn_id"] == PUBLICATION


@pytest.mark.asyncio
async def test_concurrent_admissions_sharing_a_message_keep_their_own_anchor(gateway_environment, monkeypatch):
    e = gateway_environment
    second_thread = e.thread_id + "-second"
    await e.thread_store.create(second_thread, user_id=e.owner_id)
    first_loading, second_loading = asyncio.Event(), asyncio.Event()
    count = 0

    async def controlled_load(**kwargs):
        nonlocal count
        count += 1
        if count == 1:
            first_loading.set()
            await second_loading.wait()
        else:
            second_loading.set()
        return None

    monkeypatch.setattr(services, "_load_scope_agent_config", controlled_load)
    message = HumanMessage(content="Shared synthetic question")
    first = asyncio.create_task(launched(e, body(input={"messages": [message]}), key="first-publication"))
    try:
        await asyncio.wait_for(first_loading.wait(), 2)
        second = await services.start_run(body(input={"messages": [message]}, channel_execution={"version": 1, "publication_id": "b" * 32}), second_thread, e.request, idempotency_key="second-publication")
        if second.task is not None:
            await second.task
        one = await asyncio.wait_for(first, 2)
    finally:
        second_loading.set()
        if not first.done():
            first.cancel()
        await asyncio.gather(first, return_exceptions=True)
    assert one.kwargs["input"]["messages"][0]["additional_kwargs"]["channel_turn_id"] == PUBLICATION
    assert second.kwargs["input"]["messages"][0]["additional_kwargs"]["channel_turn_id"] == "b" * 32
    assert message.additional_kwargs == {}


@pytest.mark.asyncio
async def test_v1_private_snapshot_survives_caller_mutation_during_owner_lookup(gateway_environment, monkeypatch):
    e = gateway_environment
    entered, release = asyncio.Event(), asyncio.Event()
    get_user = e.provider.get_user

    async def controlled_owner(owner):
        entered.set()
        await release.wait()
        return await get_user(owner)

    monkeypatch.setattr(e.provider, "get_user", controlled_owner)
    message = HumanMessage(content=[{"type": "text", "text": "Original question"}])
    request_body = body(input={"messages": [message]}, config={"metadata": {"retained": ["original"]}})
    task = asyncio.create_task(launched(e, request_body))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        message.content[0]["text"] = "Mutated question"
        message.additional_kwargs.update(hide_from_ui=True, human_input_response=None)
        request_body.config["metadata"]["retained"].append("mutated")
        release.set()
        record = await asyncio.wait_for(task, 2)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    actual = e.worker_inputs[0]["graph_input"]["messages"][0]
    assert actual.content == [{"type": "text", "text": "Original question"}]
    assert actual.additional_kwargs == {"channel_turn_id": PUBLICATION}
    assert record.kwargs["config"]["metadata"]["retained"] == ["original"]
    assert record.kwargs["input"]["messages"][0]["content"] == actual.content


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["account", "thread", "owner", "other-owner"])
async def test_admission_requires_a_live_human_and_exact_existing_thread_owner(gateway_environment, missing):
    e = gateway_environment
    if missing == "account":
        e.provider.user = None
    elif missing == "thread":
        await e.thread_store.delete(e.thread_id, user_id=None)
    else:
        # Replace the actual thread metadata rather than weakening its accessor.
        await e.thread_store.delete(e.thread_id, user_id=None)
        await e.thread_store.create(e.thread_id, user_id=None if missing == "owner" else "another-owner")
    with pytest.raises(HTTPException) as failure:
        await launched(e, body())
    assert failure.value.status_code in {403, 404}
    assert not e.worker_inputs


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["disabled", "released", "inert", "provider-error", "binding-error"])
async def test_unavailable_owner_or_binding_authority_never_launches(gateway_environment, monkeypatch, reason):
    e = gateway_environment
    if reason in {"disabled", "released"}:
        e.provider.user = e.owner.model_copy(update={reason + "_at": datetime.now(UTC)})
    elif reason == "inert":
        e.config.auth.local.enabled = False
    else:

        async def failed(*args, **kwargs):
            raise RuntimeError("private-backend-detail")

        monkeypatch.setattr(e.provider if reason == "provider-error" else e.bindings, "get_user" if reason == "provider-error" else "binding", failed)
    with pytest.raises(HTTPException) as failure:
        await launched(e, body())
    assert failure.value.status_code == (503 if reason.endswith("error") else 403)
    assert "private-backend-detail" not in str(failure.value.detail)
    assert not e.worker_inputs


@pytest.mark.asyncio
@pytest.mark.parametrize("assistant", [None, "lead_agent", " CUSTOM_AGENT "])
async def test_receipt_uses_actual_loaded_agent_name_and_canonical_factory(gateway_environment, monkeypatch, assistant):
    e = gateway_environment
    calls = []

    async def loaded(*, assistant_id, user_id):
        calls.append((assistant_id, user_id))
        return SimpleNamespace(name="custom-agent", knowledge_scope=None)

    monkeypatch.setattr(services, "_load_scope_agent_config", loaded)
    record = await launched(e, body(assistant_id=assistant, context={"agent_name": "custom-agent"}))
    assert calls == [("custom-agent", e.owner_id)]
    assert record.assistant_id == assistant
    assert record.kwargs["channel_execution"]["assistant_id"] == "lead_agent"
    assert record.kwargs["channel_execution"]["agent_name"] == "custom-agent"


@pytest.mark.asyncio
async def test_custom_alias_cannot_disagree_with_actual_loaded_agent(gateway_environment, monkeypatch):
    async def loaded(**kwargs):
        return SimpleNamespace(name="different-agent", knowledge_scope=None)

    monkeypatch.setattr(services, "_load_scope_agent_config", loaded)
    with pytest.raises(HTTPException) as failure:
        await launched(gateway_environment, body(assistant_id="custom-agent"))
    assert failure.value.status_code == 422
    assert not gateway_environment.worker_inputs


@pytest.mark.asyncio
@pytest.mark.parametrize("carrier", ["context", "config-context", "configurable"])
async def test_canonical_receipt_target_is_pinned_into_actual_worker_config(gateway_environment, monkeypatch, carrier):
    e = gateway_environment

    async def loaded(**kwargs):
        return SimpleNamespace(name="custom-agent", knowledge_scope=None)

    monkeypatch.setattr(services, "_load_scope_agent_config", loaded)
    changes = {"context": {"agent_name": " CUSTOM_AGENT "}} if carrier == "context" else {"config": {"context" if carrier == "config-context" else "configurable": {"agent_name": " CUSTOM_AGENT "}}}
    record = await launched(e, body(**changes))
    assert record.kwargs["channel_execution"]["agent_name"] == "custom-agent"
    for name in ("context", "configurable"):
        assert e.worker_inputs[0]["config"][name]["agent_name"] == "custom-agent"


@pytest.mark.asyncio
async def test_only_creating_v1_busy_conflict_uses_the_closed_wire_code(gateway_environment):
    e = gateway_environment
    await e.manager.create(e.thread_id, user_id=e.owner_id)
    with pytest.raises(services.BusyThreadConflict) as failure:
        await launched(e, body())
    assert failure.value.status_code == 409
    assert failure.value.detail == {"code": "thread_busy"}
    with pytest.raises(services.BusyThreadConflict) as legacy:
        await launched(e, RunCreateRequest(assistant_id="lead_agent", input={"messages": [{"role": "human", "content": "ordinary"}]}), key="ordinary-key")
    assert isinstance(legacy.value.detail, str)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["receipt", "owner", "disconnect"])
async def test_idempotent_reuse_cannot_bypass_receipt_owner_or_disconnect(gateway_environment, field):
    e = gateway_environment
    first = await launched(e, body())
    if field == "receipt":
        first.kwargs.pop("channel_execution")
        e.store._runs[first.run_id]["kwargs"].pop("channel_execution", None)
    elif field == "owner":
        first.user_id = "different-owner"
        e.store._runs[first.run_id]["user_id"] = "different-owner"
    else:
        from deerflow.runtime import DisconnectMode

        first.on_disconnect = DisconnectMode.cancel
        e.store._runs[first.run_id]["on_disconnect"] = "cancel"
    with pytest.raises(HTTPException) as failure:
        await launched(e, body())
    assert failure.value.status_code == (503 if field == "owner" else 409)
    assert len(e.worker_inputs) == 1


@pytest.mark.asyncio
async def test_legacy_request_cannot_reuse_a_typed_run_via_raw_input_compatibility(gateway_environment):
    e = gateway_environment
    first = await launched(e, body())
    legacy = RunCreateRequest(assistant_id="lead_agent", input=deepcopy(first.kwargs["input"]), on_disconnect="cancel")
    with pytest.raises(HTTPException) as failure:
        await launched(e, legacy)
    assert failure.value.status_code == 409
    assert len(e.worker_inputs) == 1


@pytest.mark.asyncio
async def test_ordinary_ownership_refusal_still_precedes_invalid_input(gateway_environment):
    e = gateway_environment
    await e.thread_store.delete(e.thread_id, user_id=None)
    await e.thread_store.create(e.thread_id, user_id="different-owner")
    request_body = RunCreateRequest(input={"messages": [{"role": "not-a-role", "content": "invalid"}]})
    with pytest.raises(HTTPException) as failure:
        await launched(e, request_body)
    assert failure.value.status_code == 404
    assert not e.worker_inputs
