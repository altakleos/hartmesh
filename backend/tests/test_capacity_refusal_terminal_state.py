"""A capacity-refused turn, and Stop during the capacity wait, end once and durably.

Before this change, on the single-Gateway profile:

- a turn whose bash call was refused for capacity finished as ``success`` with
  the reason ``sandbox_capacity_exceeded``, a code the lifecycle journal did
  not know. The rejected terminal write read as a lost cancellation race, so
  the worker was fenced: its stream never sent ``end``, its durable row stayed
  ``running``, and the chat refused the person's next turn with 409;
- Stop during the capacity wait ended the turn as an opaque ``RuntimeFailure``:
  the wait answered the Stop with its own exception, the turn went on to a tool
  result whose receipt the Stop had fenced, and the worker staged that error
  over the Stop.

The refused-turn and Stop tests fail on the release before it. They drive the
production route as a browser does:
the real Gateway runs API, ``run_agent``, the real lead-agent graph and
middleware chain with the real ``bash`` tool, the real ``RunManager`` over the
SQLite run store and its lifecycle journal (``run_events: db``, as the tenant
profile runs it), and an accepted business-report snapshot. The sandbox is the
real AIO provider's admission over a fake container backend (``_FakeBackend``)
whose two slots are held by another person's sets, so no Docker is needed.
Only the model is scripted.
"""

from __future__ import annotations

import shutil
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from _agent_e2e_helpers import FakeToolCallingModel
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from test_accepted_capacity_outcomes import _live, _saturate, _waiting_signal
from test_runtime_lifecycle_e2e import (
    _create_thread,
    _drain_stream,
    _parse_sse,
    _preserve_process_config_singletons,
    _register_user,
    _reset_process_singletons,
    _run_body,
    _run_id_from_response,
    _wait_for_status,
)
from test_sandbox_warm_reuse_latency import _make_provider

pytestmark = pytest.mark.no_auto_user

REPO_ROOT = Path(__file__).resolve().parents[2]

_CONFIG_YAML = """\
log_level: info
models:
  - name: fake-test-model
    display_name: Fake Test Model
    use: langchain_openai:ChatOpenAI
    model: gpt-4o-mini
    api_key: $OPENAI_API_KEY
    base_url: $OPENAI_API_BASE
sandbox:
  use: _seeded_skill_sandbox_provider:ProjectionProvider
deployment:
  profile: local_development
tool_groups:
  - name: bash
tools:
  - name: bash
    group: bash
    use: deerflow.sandbox.tools:bash_tool
title:
  enabled: false
memory:
  enabled: false
database:
  backend: sqlite
run_events:
  backend: db
"""


class _BashThenAnswer(FakeToolCallingModel):
    """Asks for one bash command per turn, then answers from its result."""

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        del stop, run_manager, kwargs
        if isinstance(messages[-1], ToolMessage):
            reply = AIMessage(content="The workspace is busy right now; try again in a moment.")
        else:
            turn = sum(1 for message in messages if isinstance(message, AIMessage))
            reply = AIMessage(
                content="",
                tool_calls=[{"name": "bash", "args": {"description": "list", "command": "ls /mnt/user-data"}, "id": f"bash-{turn}", "type": "tool_call"}],
            )
        return ChatResult(generations=[ChatGeneration(message=reply)])


@pytest.fixture
def capacity_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    home = tmp_path / "deer-flow-home"
    home.mkdir()
    monkeypatch.setenv("DEER_FLOW_HOME", str(home))
    monkeypatch.setenv("OPENAI_API_KEY", "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY")
    monkeypatch.setenv("OPENAI_API_BASE", "https://example.invalid")
    skills = tmp_path / "skills"
    shutil.copytree(REPO_ROOT / "skills" / "public" / "business-report", skills / "public" / "business-report")
    monkeypatch.setenv("DEER_FLOW_SKILLS_PATH", str(skills))
    config = tmp_path / "config.yaml"
    config.write_text(_CONFIG_YAML, encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(config))
    extensions = tmp_path / "extensions_config.json"
    extensions.write_text('{"mcpServers": {}, "skills": {}}', encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(extensions))

    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    held = _saturate(provider, "active")
    for target in (
        "deerflow.sandbox.get_sandbox_provider",
        "deerflow.sandbox.sandbox_provider.get_sandbox_provider",
        "deerflow.sandbox.tools.get_sandbox_provider",
        "deerflow.sandbox.middleware.get_sandbox_provider",
    ):
        monkeypatch.setattr(target, lambda: provider)

    _preserve_process_config_singletons(monkeypatch)
    _reset_process_singletons(monkeypatch)
    from deerflow.config import app_config as app_config_module

    app_config_module.get_app_config().database.sqlite_dir = str(home / "db")
    from app.gateway.app import create_app

    return create_app(), provider, backend, held


def _durable_row(run_id: str) -> dict[str, Any]:
    """The run as the durable store holds it, not as this process remembers it."""
    import asyncio

    from deerflow.persistence.engine import get_session_factory
    from deerflow.persistence.run.sql import RunRepository

    return asyncio.run(RunRepository(get_session_factory()).get(run_id, user_id=None))


def _start_turn(client, thread_id: str, csrf_token: str) -> tuple[threading.Thread, dict[str, Any]]:
    """Send one chat turn as the browser does, in the background.

    The test client returns a streamed response only once the stream has
    ended, so a stream that never ends shows up as a turn still in flight.
    """
    streamed: dict[str, Any] = {}

    def _turn() -> None:
        try:
            body = _run_body(input={"messages": [{"role": "user", "content": "List what is in my workspace."}]}, config={"recursion_limit": 100})
            with client.stream("POST", f"/api/threads/{thread_id}/runs/stream", json=body, headers={"X-CSRF-Token": csrf_token}) as response:
                streamed["status_code"] = response.status_code
                streamed["run_id"] = _run_id_from_response(response) if response.status_code == 200 else None
                streamed["events"] = _parse_sse(_drain_stream(response, timeout=5))
        except BaseException as exc:  # reported in the test's own thread
            streamed["error"] = exc

    turn = threading.Thread(target=_turn, daemon=True)
    turn.start()
    return turn, streamed


def _ended(client, thread_id: str, turn: threading.Thread, streamed: dict[str, Any], *, timeout: float = 30.0) -> tuple[str, list[dict]]:
    turn.join(timeout)
    if turn.is_alive():
        # Failed already. End the stuck streams so the test client can close.
        stuck = [run["run_id"] for run in client.get(f"/api/threads/{thread_id}/runs").json()]
        for run_id in stuck:
            client.portal.call(client.app.state.stream_bridge.publish_end, run_id)
        turn.join(10)
        raise AssertionError(f"the stream never ended; durable rows: {[_durable_row(run_id) for run_id in stuck]}")
    assert "error" not in streamed, streamed.get("error")
    assert streamed["status_code"] == 200, streamed
    return streamed["run_id"], streamed["events"]


def _stream_turn(client, thread_id: str, csrf_token: str) -> tuple[str, list[dict]]:
    return _ended(client, thread_id, *_start_turn(client, thread_id, csrf_token))


def test_a_capacity_refused_turn_ends_once_and_the_chat_takes_the_next_turn(capacity_app):
    from starlette.testclient import TestClient

    app, provider, backend, held = capacity_app
    provider._config["capacity_wait_timeout"] = 0

    with patch("deerflow.agents.lead_agent.agent.create_chat_model", new=lambda *args, **kwargs: _BashThenAnswer(responses=[AIMessage(content="unused")])), TestClient(app) as client:
        csrf_token = _register_user(client, email="capacity-e2e@example.com")
        thread_id = _create_thread(client, csrf_token)

        run_id, events = _stream_turn(client, thread_id, csrf_token)

        # The stream ended, and nothing on it reads like a crash.
        assert [event["event"] for event in events][-1] == "end", events
        assert "error" not in [event["event"] for event in events], events
        # One terminal state, the same in this process and in the durable store.
        run = _wait_for_status(client, thread_id, run_id, "success")
        assert run["stop_reason"] == "sandbox_capacity_exceeded", run
        row = _durable_row(run_id)
        assert (row["status"], row["stop_reason"]) == ("success", "sandbox_capacity_exceeded"), row
        assert _live(backend) == sorted(held), "no third set"

        # Once a slot frees, the person's next turn in the same chat is accepted and runs.
        provider.release(held[0])
        second_run_id, second_events = _stream_turn(client, thread_id, csrf_token)
        assert [event["event"] for event in second_events][-1] == "end", second_events
        second = _wait_for_status(client, thread_id, second_run_id, "success")
        assert second["stop_reason"] is None, second
        assert _durable_row(second_run_id)["status"] == "success"


@pytest.mark.parametrize("attempt", range(3))
def test_stop_during_the_capacity_wait_ends_the_turn_interrupted(capacity_app, monkeypatch, attempt):
    from starlette.testclient import TestClient

    del attempt
    app, provider, backend, held = capacity_app
    provider._config["capacity_wait_timeout"] = 5
    waiting = _waiting_signal(monkeypatch)

    with patch("deerflow.agents.lead_agent.agent.create_chat_model", new=lambda *args, **kwargs: _BashThenAnswer(responses=[AIMessage(content="unused")])), TestClient(app) as client:
        csrf_token = _register_user(client, email="capacity-stop-e2e@example.com")
        thread_id = _create_thread(client, csrf_token)

        turn, streamed = _start_turn(client, thread_id, csrf_token)
        assert waiting.wait(20), "the turn never reached the capacity wait"
        runs = client.get(f"/api/threads/{thread_id}/runs").json()
        [run_id] = [run["run_id"] for run in runs]

        stopped = time.monotonic()
        cancelled = client.post(f"/api/threads/{thread_id}/runs/{run_id}/cancel?wait=true&action=interrupt", headers={"X-CSRF-Token": csrf_token})
        elapsed = time.monotonic() - stopped
        _, events = _ended(client, thread_id, turn, streamed)

        assert cancelled.status_code == 204, cancelled.text
        assert elapsed < 2.0, f"Stop took {elapsed:.2f}s"
        assert [event["event"] for event in events][-1] == "end", events
        assert "error" not in [event["event"] for event in events], events
        _wait_for_status(client, thread_id, run_id, "interrupted")
        assert _durable_row(run_id)["status"] == "interrupted"
        assert _live(backend) == sorted(held), "no set was created after Stop"
        assert provider._starting == set(), "no reservation left behind"

        # The chat takes the person's next turn.
        provider.release(held[0])
        next_run_id, next_events = _stream_turn(client, thread_id, csrf_token)
        assert [event["event"] for event in next_events][-1] == "end", next_events
        _wait_for_status(client, thread_id, next_run_id, "success")


def test_a_stop_that_a_tool_answers_with_an_error_still_ends_the_turn_interrupted(capacity_app, monkeypatch):
    """Whatever a tool makes of the Stop, the Stop decides how the turn ends.

    The capacity wait once answered the Stop with its own exception, so the
    turn went on to a tool result whose receipt the Stop had already fenced,
    and ended as ``RuntimeFailure``. That acquisition is repaired; this
    injects the same shape so any tool that does it is covered.
    """
    import asyncio

    from starlette.testclient import TestClient

    app, provider, backend, held = capacity_app
    provider._config["capacity_wait_timeout"] = 5
    waiting = _waiting_signal(monkeypatch)
    acquire = provider.provision_accepted_skills_async

    async def _answers_stop_with_an_error(*args, **kwargs):
        try:
            return await acquire(*args, **kwargs)
        except asyncio.CancelledError:
            raise RuntimeError("the acquisition was stopped") from None

    monkeypatch.setattr(provider, "provision_accepted_skills_async", _answers_stop_with_an_error)

    with patch("deerflow.agents.lead_agent.agent.create_chat_model", new=lambda *args, **kwargs: _BashThenAnswer(responses=[AIMessage(content="unused")])), TestClient(app) as client:
        csrf_token = _register_user(client, email="capacity-stop-error-e2e@example.com")
        thread_id = _create_thread(client, csrf_token)

        turn, streamed = _start_turn(client, thread_id, csrf_token)
        assert waiting.wait(20), "the turn never reached the capacity wait"
        [run_id] = [run["run_id"] for run in client.get(f"/api/threads/{thread_id}/runs").json()]
        cancelled = client.post(f"/api/threads/{thread_id}/runs/{run_id}/cancel?wait=true&action=interrupt", headers={"X-CSRF-Token": csrf_token})
        _, events = _ended(client, thread_id, turn, streamed)

        assert cancelled.status_code == 204, cancelled.text
        assert [event["event"] for event in events][-1] == "end", events
        assert "error" not in [event["event"] for event in events], events
        _wait_for_status(client, thread_id, run_id, "interrupted")
        assert _durable_row(run_id)["status"] == "interrupted"
        assert _live(backend) == sorted(held), "no set was created after Stop"


def test_a_refusal_reason_that_is_not_a_host_code_is_dropped_not_fenced(capacity_app, monkeypatch):
    """A tool's refusal reason of the wrong shape costs the reason, not the run."""
    from starlette.testclient import TestClient

    from deerflow.sandbox.exceptions import SandboxCapacityExceededError

    app, provider, backend, held = capacity_app
    provider._config["capacity_wait_timeout"] = 0
    monkeypatch.setattr(SandboxCapacityExceededError, "run_stop_reason", "Sandbox Capacity!")

    with patch("deerflow.agents.lead_agent.agent.create_chat_model", new=lambda *args, **kwargs: _BashThenAnswer(responses=[AIMessage(content="unused")])), TestClient(app) as client:
        csrf_token = _register_user(client, email="capacity-bad-reason-e2e@example.com")
        thread_id = _create_thread(client, csrf_token)

        run_id, events = _stream_turn(client, thread_id, csrf_token)

        assert [event["event"] for event in events][-1] == "end", events
        run = _wait_for_status(client, thread_id, run_id, "success")
        assert run["stop_reason"] is None, run
        row = _durable_row(run_id)
        assert (row["status"], row["stop_reason"]) == ("success", None), row
