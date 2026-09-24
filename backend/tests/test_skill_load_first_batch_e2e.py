"""A report chat's first batch, through the real Gateway and lead agent.

Released-profile report chats read business-report's ``SKILL.md`` and, in the
same assistant message, inspected the upload (openpyxl sheet and sample rows,
or ``ls -la``). The skill says to build first, but a call chosen beside the
read was selected before that text could reach the model. This drives the
observed first batch through the production route: the Gateway runs API,
``run_agent``, the real lead-agent graph, middleware chain and ``read_file`` /
``bash`` tools, with the real ``business-report`` package admitted as the
run's accepted snapshot on the tenant profile. The sandbox is the host-local
test provider that declares immutable accepted material
(``_seeded_skill_sandbox_provider``), so this needs no Docker. Only the model is scripted, and it takes the skill's
location from the system prompt it is given, as the real model does.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from _agent_e2e_helpers import FakeToolCallingModel
from langchain_core.messages import AIMessage, BaseMessage, SystemMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
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
  allow_host_bash: true
deployment:
  profile: local_development
tool_groups:
  - name: file:read
  - name: bash
tools:
  - name: read_file
    group: file:read
    use: deerflow.sandbox.tools:read_file_tool
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
  backend: memory
"""

_PROMPT = "Use the business-report skill to create the August 2026 business review from the uploaded workbook as PDF, Word and Excel. Generate all three files for download. Keep the input unchanged."
_INSPECTION = "python3 -c \"import zipfile; print(zipfile.ZipFile('/mnt/user-data/uploads/input.xlsx').namelist())\""
_BUILD_STAND_IN = "echo build-ran"
_REQUESTS: list[list[BaseMessage]] = []


def _text(message: BaseMessage) -> str:
    return message.content if isinstance(message.content, str) else str(message.content)


class _ReportChatModel(FakeToolCallingModel):
    """Plays the first released-profile sample's opening, then builds and answers.

    Turn 1 reads the skill at the ``<location>`` its system prompt names and,
    in the same message, inspects the upload. Turn 2 runs a stand-in for the
    build; turn 3 answers. Every request it receives is kept in ``_REQUESTS``.
    """

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        del stop, run_manager, kwargs
        _REQUESTS.append(list(messages))
        turn = sum(1 for message in messages if isinstance(message, AIMessage))
        if turn == 0:
            prompt = "\n".join(_text(message) for message in messages if isinstance(message, SystemMessage))
            match = re.search(r"<name>business-report</name>\s*<description>.*?</description>\s*<location>([^<]+)</location>", prompt, re.DOTALL)
            assert match, "the system prompt names no business-report location"
            reply = AIMessage(
                content="",
                tool_calls=[
                    {"name": "read_file", "args": {"description": "Read the report skill", "path": match.group(1)}, "id": "read-skill", "type": "tool_call"},
                    {"name": "bash", "args": {"description": "Inspect the workbook", "command": _INSPECTION}, "id": "inspect-upload", "type": "tool_call"},
                ],
            )
        elif turn == 1:
            reply = AIMessage(content="", tool_calls=[{"name": "bash", "args": {"description": "Build the report", "command": _BUILD_STAND_IN}, "id": "build", "type": "tool_call"}])
        else:
            reply = AIMessage(content="The August 2026 business review is ready.")
        return ChatResult(generations=[ChatGeneration(message=reply)])


@pytest.fixture
def report_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
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

    _preserve_process_config_singletons(monkeypatch)
    _reset_process_singletons(monkeypatch)
    from deerflow.config import app_config as app_config_module

    app_config_module.get_app_config().database.sqlite_dir = str(home / "db")
    from app.gateway.app import create_app

    return create_app()


def _run_report_chat(app) -> tuple[list[list[BaseMessage]], dict[str, Any]]:
    from starlette.testclient import TestClient

    _REQUESTS.clear()
    model = _ReportChatModel(responses=[AIMessage(content="unused")])

    def fake_create_chat_model(*args: Any, **kwargs: Any) -> _ReportChatModel:
        del args, kwargs
        return model

    with patch("deerflow.agents.lead_agent.agent.create_chat_model", new=fake_create_chat_model), TestClient(app) as client:
        csrf_token = _register_user(client, email="report-e2e@example.com")
        user_id = client.get("/api/v1/auth/me").json()["id"]
        thread_id = _create_thread(client, csrf_token)

        from deerflow.config.paths import get_paths

        uploads = get_paths().sandbox_uploads_dir(thread_id, user_id=user_id)
        uploads.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO_ROOT / "backend" / "tests" / "skills" / "business_report" / "fixtures" / "example_services_export.xlsx", uploads / "input.xlsx")

        body = _run_body(
            input={"messages": [{"role": "user", "content": _PROMPT}]},
            context={"thinking_enabled": False, "is_plan_mode": False, "subagent_enabled": False},
        )
        with client.stream("POST", f"/api/threads/{thread_id}/runs/stream", json=body, headers={"X-CSRF-Token": csrf_token}) as response:
            assert response.status_code == 200, response.read().decode()
            run_id = _run_id_from_response(response)
            transcript = _drain_stream(response, timeout=30.0)
        run = _wait_for_status(client, thread_id, run_id, "success", timeout=10.0)
    assert "error" not in [event["event"] for event in _parse_sse(transcript)], transcript
    return list(_REQUESTS), run


def _tool_results(request: list[BaseMessage]) -> dict[str, ToolMessage]:
    return {message.tool_call_id: message for message in request if isinstance(message, ToolMessage)}


def test_an_inspection_chosen_beside_the_skill_read_is_not_run(report_app):
    requests, _run = _run_report_chat(report_app)

    assert len(requests) == 3
    after_first_batch = _tool_results(requests[1])

    # The read ran and delivered the released skill text.
    assert "# Business Report Skill" in _text(after_first_batch["read-skill"])
    # The inspection chosen beside it did not run: nothing listed the
    # workbook, and the result names the instructions it was chosen without.
    refused = after_first_batch["inspect-upload"]
    assert refused.status == "error"
    assert _text(refused).startswith("Not run:"), _text(refused)
    assert "business-report skill's instructions" in _text(refused)
    assert "xl/workbook.xml" not in _text(refused)

    # The next call, chosen with the instructions in hand, runs.
    assert "build-ran" in _text(_tool_results(requests[2])["build"])
