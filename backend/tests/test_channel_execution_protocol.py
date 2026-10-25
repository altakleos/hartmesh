"""The channel-neutral host contract stays lightweight and detached."""

import ast
import importlib
import sys
from dataclasses import FrozenInstanceError, fields
from pathlib import Path

import pytest


@pytest.fixture
def protocol():
    return importlib.import_module("app.channels.execution")


def test_scope_and_context_are_frozen_and_expose_the_original_primitives(protocol):
    values = dict(publication_id="a" * 32, owner_user_id="owner", connection_id="connection", chat_id="chat", thread_id="thread", endpoint_id="endpoint", assistant_id="lead_agent", agent_name=None)
    scope = protocol.ChannelExecutionScope(**values)
    context = protocol.ChannelExecutionContext(scope, object(), object())
    for key, value in values.items():
        assert getattr(context, key) == value
    assert [field.name for field in fields(context)] == ["scope", "client", "admission"]
    with pytest.raises(FrozenInstanceError):
        scope.thread_id = "another"
    with pytest.raises(FrozenInstanceError):
        context.scope = scope


def test_transport_response_and_error_are_detached_and_diagnostics_are_fixed(protocol):
    body = {"detail": {"code": "private-value"}}
    headers = {"content-type": "application/json"}
    response = protocol.ChannelJsonResponse(409, headers, body)
    body["detail"]["code"] = "changed"
    headers.clear()
    assert response.body == {"detail": {"code": "private-value"}}
    assert dict(response.headers) == {"content-type": "application/json"}
    failure = protocol.ChannelHttpError(response)
    response.body["detail"]["code"] = "changed-again"
    assert failure.response.body == {"detail": {"code": "private-value"}}
    with pytest.raises(AttributeError):
        failure.response = response
    assert str(failure) == "Channel execution HTTP request failed"
    assert str(protocol.ChannelExecutionProtocolError()) == "Channel execution protocol refused"
    assert str(protocol.ChannelExecutionBusyError()) == "Channel execution thread busy"
    with pytest.raises(TypeError):
        protocol.ChannelExecutionProtocolError("private-value")


def test_protocol_import_surface_is_standard_library_only(protocol):
    tree = ast.parse(Path(protocol.__file__).read_text(encoding="utf-8"))
    imports = {name.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for name in node.names}
    imports |= {node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    assert imports <= sys.stdlib_module_names


@pytest.mark.parametrize(
    "field,value",
    [
        ("publication_id", True),
        ("publication_id", "A" * 32),
        ("publication_id", "short"),
        ("owner_user_id", None),
        ("chat_id", ""),
        ("thread_id", "bad\nthread"),
        ("endpoint_id", " endpoint"),
        ("assistant_id", 1),
        ("agent_name", ""),
        ("connection_id", True),
    ],
)
def test_scope_rejects_nonprimitive_or_malformed_host_facts_with_fixed_diagnostics(protocol, field, value):
    values = dict(publication_id="a" * 32, owner_user_id="owner", connection_id="connection", chat_id="chat", thread_id="thread", endpoint_id="endpoint", assistant_id="lead_agent", agent_name=None)
    with pytest.raises(protocol.ChannelExecutionProtocolError, match="^Channel execution protocol refused$"):
        protocol.ChannelExecutionScope(**(values | {field: value}))
