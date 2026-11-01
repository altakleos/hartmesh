"""Fixed host classifications never derive retry authority from displayed text."""

import ast
import importlib
import inspect
import textwrap
from dataclasses import replace
from types import MappingProxyType, SimpleNamespace

import pytest

from app.channels import manager
from app.channels.execution import ChannelExecutionBusyError, ChannelExecutionContext, ChannelExecutionProtocolError, ChannelExecutionScope
from app.channels.execution_client import ValidatedRun
from app.channels.outcomes import capture_creation_refusal
from app.gateway.channel_execution import ChannelExecutionReceipt
from app.telegram_bots.texts import catalog


def errors():
    return importlib.import_module("app.telegram_bots.errors")


def scoped_run(*, status="error", reason=None, custom=False):
    scope = ChannelExecutionScope(
        publication_id="a" * 32, owner_user_id="human", connection_id="connection", chat_id="7001", thread_id="original-thread", endpoint_id="endpoint", assistant_id="lead_agent", agent_name="custom-agent" if custom else None
    )
    receipt = ChannelExecutionReceipt(version=1, publication_id=scope.publication_id, owner_user_id=scope.owner_user_id, thread_id=scope.thread_id, assistant_id=scope.assistant_id, agent_name=scope.agent_name)
    row = {
        "run_id": "actual-run",
        "thread_id": scope.thread_id,
        "assistant_id": "lead_agent",
        "status": status,
        "stop_reason": reason,
        "metadata": {"channel_turn_id": scope.publication_id},
        "kwargs": {"channel_execution": receipt.model_dump()},
    }
    return scope, ValidatedRun(MappingProxyType(row), receipt)


@pytest.mark.parametrize("custom", [False, True])
@pytest.mark.parametrize(
    "reason,key,retry",
    [
        ("sandbox_capacity_exceeded", "busy.queued", True),
        ("files-full", "chat.files_full", False),
        ("orphan_recovered", "refused.interrupted", False),
        ("gateway_stopped", "chat.restarted", False),
        ("model_refused", "error.model_refused", False),
        (None, "error.generic", False),
        ("unknown-host-code", "error.generic", False),
        ("busy", "error.generic", False),
    ],
)
def test_only_the_current_ordinary_run_reason_selects_a_fixed_failure(custom, reason, key, retry):
    scope, run = scoped_run(reason=reason, custom=custom)
    result = errors().classify_run(run, scope)
    assert (result.text_key, result.retry_later) == (key, retry)


@pytest.mark.parametrize("status", ["success", "interrupted", "timeout"])
def test_capacity_reason_cannot_retry_a_success_or_an_external_interruption(status):
    scope, run = scoped_run(status=status, reason="sandbox_capacity_exceeded")
    result = errors().classify_run(run, scope)
    if status == "success":
        assert result is None
    else:
        assert (result.text_key, result.retry_later) == ("error.generic", False)


@pytest.mark.parametrize("wording", [manager.THREAD_BUSY_MESSAGE, "sandbox_capacity_exceeded", "SandboxCapacityExceededError", "busy", "files-full"])
@pytest.mark.parametrize("status", ["success", "error"])
def test_model_or_error_wording_never_confers_a_retry(wording, status):
    scope, run = scoped_run(status=status)
    run.row["kwargs"]["input"] = {"messages": [{"role": "assistant", "content": wording}]}
    row = dict(run.row, error=wording, last_ai_message=wording, text=wording, run_stop_reason="sandbox_capacity_exceeded")
    result = errors().classify_run(ValidatedRun(MappingProxyType(row), run.receipt), scope)
    assert result is None if status == "success" else (result.text_key, result.retry_later) == ("error.generic", False)


@pytest.mark.parametrize("change", ["plain-dict", "no-proof", "different-proof", "changed-stored-proof", "missing-marker", "wrong-thread", "pending", "running", "resident-target", "bad-reason", "missing-run-id"])
def test_a_noncanonical_or_uncorrelated_run_never_selects_a_failure(change):
    scope, run = scoped_run(reason="sandbox_capacity_exceeded")
    row, proof = dict(run.row), run.receipt
    if change == "plain-dict":
        run = row
    else:
        if change == "no-proof":
            proof = None
        elif change == "different-proof":
            proof = proof.model_copy(update={"publication_id": "b" * 32})
        elif change == "changed-stored-proof":
            row["kwargs"] = {"channel_execution": proof.model_dump() | {"publication_id": "b" * 32}}
        elif change == "missing-marker":
            row["metadata"] = {}
        elif change == "wrong-thread":
            row["thread_id"] = "another-thread"
        elif change in {"pending", "running"}:
            row["status"] = change
        elif change == "resident-target":
            scope = replace(scope, assistant_id="resident-instance")
        elif change == "bad-reason":
            row["stop_reason"] = ["sandbox_capacity_exceeded"]
        elif change == "missing-run-id":
            row.pop("run_id")
        run = ValidatedRun(MappingProxyType(row), proof)
    with pytest.raises(ChannelExecutionProtocolError):
        errors().classify_run(run, scope)


def test_only_the_original_opaque_creating_refusal_selects_thread_waiting():
    scope, _run = scoped_run()
    context = ChannelExecutionContext(scope, SimpleNamespace(), SimpleNamespace(current=lambda operation: True))
    proof = capture_creation_refusal(context, ChannelExecutionBusyError())
    result = errors().classify_creation_refusal(proof, scope)
    assert (result.text_key, result.retry_later) == ("busy.thread", True)
    for fake in [{"code": "thread_busy"}, manager.THREAD_BUSY_MESSAGE, ChannelExecutionBusyError()]:
        with pytest.raises(ChannelExecutionProtocolError):
            errors().classify_creation_refusal(fake, scope)
    with pytest.raises(ChannelExecutionProtocolError):
        errors().classify_creation_refusal(proof, replace(scope, publication_id="b" * 32))


def test_every_selected_fixed_key_exists_in_both_catalogs_and_is_not_a_startup_claim():
    module = errors()
    keys = set(module.RUN_STOP_REASONS.values()) | set(module.MANAGER_SENTENCES.values()) | {"error.generic", "busy.thread"}
    for locale in ["en", "zh-CN"]:
        assert keys <= catalog(locale).keys()
    assert module.RUN_STOP_REASONS["orphan_recovered"] == "refused.interrupted"
    assert catalog("en")["refused.interrupted"] == "{name}'s workspace was restarted, which stopped this answer. Your message is kept."
    assert "{name}" in catalog("zh-CN")["refused.interrupted"]
    with pytest.raises(TypeError):
        module.RUN_STOP_REASONS["model-provided-retry"] = "busy.queued"


def test_upstream_literals_unchanged():
    expected = {
        "THREAD_BUSY_MESSAGE": "This conversation is already processing another request. Please wait for it to finish and try again.",
        "BOUND_IDENTITY_REQUIRED_MESSAGE": "Connect this channel from Settings in the web app, complete the in-channel connect step, then send your message again.",
        "BOUND_IDENTITY_UNAVAILABLE_MESSAGE": "Channel connection verification is temporarily unavailable. Please try again later or contact your administrator.",
    }
    assert {name: getattr(manager, name) for name in expected} == expected
    inline = {
        "_handle_message": ["An internal error occurred. Please try again.", "Channel execution protocol refused"],
        "_load_thread_agent": ["This conversation has an invalid stored agent selection. Use /agent use <name> to start a valid conversation."],
        "_publish_execution_result": ["(No response from agent)"],
        "_handle_chat_on_thread": ["(No response from agent)"],
        "_handle_streaming_chat": ["An error occurred while processing your request. Please try again.", "(No response from agent)"],
    }
    for name, sentences in inline.items():
        tree = ast.parse(textwrap.dedent(inspect.getsource(getattr(manager.ChannelManager, name))))
        strings = [node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and type(node.value) is str]
        assert all(strings.count(sentence) == 1 for sentence in sentences), name
    assert set(errors().MANAGER_SENTENCES) == set(expected.values()) | {sentence for sentences in inline.values() for sentence in sentences}
    assert manager._format_artifact_text(["/mnt/user-data/outputs/one.txt"]) == "Created File: 📎 one.txt"
    assert manager._format_artifact_text(["/mnt/user-data/outputs/one.txt", "/mnt/user-data/outputs/two.txt"]) == "Created Files: 📎 one.txt、two.txt"
