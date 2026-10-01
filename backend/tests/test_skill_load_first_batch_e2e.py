"""A report chat's first sandbox work, through the real Gateway and lead agent.

Released-profile report chats (v2.1.0+hartmesh.34) read business-report's
``SKILL.md`` and, in the same assistant message, inspected the upload; two of
four then ran more workbook inspections after the read returned, before
building. This drives those choices through the production route: the Gateway
runs API, ``run_agent``, the real lead-agent graph, middleware chain and
``read_file`` / ``bash`` tools, with the real ``business-report`` package
admitted as the run's accepted snapshot on the tenant profile. The sandbox is
the host-local test provider that declares immutable accepted material
(``_seeded_skill_sandbox_provider``), so this needs no Docker, and the build
really runs, on this test's own Python. Only the model is scripted: it takes
the skill's location from the system prompt it is given, as the real model
does, and it chooses to inspect again after every refusal, as the released
model did, before it builds.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import sys
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
  use: deerflow.sandbox.local:LocalSandboxProvider
  allow_host_bash: true
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
# The observed inspections: workbook structure beside the read, then the same
# again and the month distribution once the read had returned.
_STRUCTURE = "cd /mnt/user-data && python3 -c \"import zipfile; print(zipfile.ZipFile('/mnt/user-data/uploads/input.xlsx').namelist())\""
_MONTHS = "cd /mnt/user-data/uploads && python3 -c \"import zipfile; print(len(zipfile.ZipFile('input.xlsx').namelist()), 'parts')\""
_REQUESTS: list[list[BaseMessage]] = []


def _text(message: BaseMessage) -> str:
    return message.content if isinstance(message.content, str) else str(message.content)


def _bash(command: str, call_id: str, **extra: Any) -> dict:
    return {"name": "bash", "args": {"description": call_id, "command": command, **extra}, "id": call_id, "type": "tool_call"}


def _build(directory: str, period: str = "2026-08") -> dict:
    """The build as the skill's Step 1 example writes it, with ``present`` beside ``command``.

    Without the example's ``${SKILL_DIR:?…}`` guard, which the host-local
    provider's path check reads as an absolute ``/scripts`` path; the sandbox
    image runs the guarded form, and the recogniser's tests cover it.
    """
    out = f"/mnt/user-data/outputs/reports/{period}-business-review"
    command = f'SKILL_DIR="{directory}"; python "$SKILL_DIR/scripts/report.py" build /mnt/user-data/uploads/input.xlsx --period {period} --out {out} --render pdf,docx,xlsx'
    present = [f"{out}/{period}-business-review.{suffix}" for suffix in ("report.json", "pdf", "docx", "xlsx")]
    return _bash(command, "build", present=present)


class _ReportChatModel(FakeToolCallingModel):
    """A model that inspects whenever it is allowed a choice, then builds and answers.

    Turn 1 reads the skill at the ``<location>`` its system prompt names and,
    in the same message, inspects the workbook. Turn 2 inspects it again and
    turn 3 asks for the months in it, as the released model did after the
    read returned, and turn 4 reads the upload with ``read_file``; only
    then does it build (turn 5) and answer. With ``after_build`` it inspects
    once more after the build. Every request it receives is kept in
    ``_REQUESTS``.
    """

    after_build: bool = False

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        del stop, run_manager, kwargs
        _REQUESTS.append(list(messages))
        prompt = "\n".join(_text(message) for message in messages if isinstance(message, SystemMessage))
        match = re.search(r"<name>business-report</name>\s*<description>.*?</description>\s*<location>([^<]+)</location>", prompt, re.DOTALL)
        assert match, "the system prompt names no business-report location"
        location = match.group(1)
        turns = [
            [{"name": "read_file", "args": {"description": "Read the report skill", "path": location}, "id": "read-skill", "type": "tool_call"}, _bash(_STRUCTURE, "inspect-beside-read")],
            [_bash(_STRUCTURE, "inspect-again")],
            [_bash(_MONTHS, "inspect-months")],
            [{"name": "read_file", "args": {"description": "Read the upload", "path": "/mnt/user-data/uploads/input.xlsx"}, "id": "read-upload", "type": "tool_call"}],
            [_build(location.rsplit("/", 1)[0])],
            *([[_bash(_MONTHS, "inspect-after-build")]] if self.after_build else []),
        ]
        turn = sum(1 for message in messages if isinstance(message, AIMessage))
        reply = AIMessage(content="", tool_calls=turns[turn]) if turn < len(turns) else AIMessage(content="The August 2026 business review is ready.")
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
    # The host-local sandbox runs `python` from PATH; this interpreter has the
    # libraries the sandbox image ships for the report skill.
    monkeypatch.setenv("PATH", f"{Path(sys.executable).parent}{os.pathsep}{os.environ.get('PATH', '')}")

    _preserve_process_config_singletons(monkeypatch)
    _reset_process_singletons(monkeypatch)
    from deerflow.config import app_config as app_config_module

    app_config_module.get_app_config().database.sqlite_dir = str(home / "db")
    from app.gateway.app import create_app

    return create_app()


def _run_report_chat(app, **model_fields: Any) -> tuple[list[list[BaseMessage]], dict[str, Any], dict[str, Any]]:
    from starlette.testclient import TestClient

    _REQUESTS.clear()
    model = _ReportChatModel(responses=[AIMessage(content="unused")], **model_fields)

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
        upload = uploads / "input.xlsx"
        shutil.copyfile(REPO_ROOT / "backend" / "tests" / "skills" / "business_report" / "fixtures" / "example_services_export.xlsx", upload)
        before = hashlib.sha256(upload.read_bytes()).hexdigest()

        body = _run_body(
            input={"messages": [{"role": "user", "content": _PROMPT}]},
            context={"thinking_enabled": False, "is_plan_mode": False, "subagent_enabled": False},
            # Every model turn passes through each middleware node; six turns need more steps than 50.
            config={"recursion_limit": 200},
        )
        with client.stream("POST", f"/api/threads/{thread_id}/runs/stream", json=body, headers={"X-CSRF-Token": csrf_token}) as response:
            assert response.status_code == 200, response.read().decode()
            run_id = _run_id_from_response(response)
            transcript = _drain_stream(response, timeout=60.0)
        run = _wait_for_status(client, thread_id, run_id, "success", timeout=10.0)
        state = client.get(f"/api/threads/{thread_id}/state").json()
        assert hashlib.sha256(upload.read_bytes()).hexdigest() == before, "the upload changed"
    assert "error" not in [event["event"] for event in _parse_sse(transcript)], transcript
    return list(_REQUESTS), run, state


def _tool_results(request: list[BaseMessage]) -> dict[str, ToolMessage]:
    return {message.tool_call_id: message for message in request if isinstance(message, ToolMessage)}


def test_the_first_thing_the_sandbox_runs_for_a_report_is_the_build(report_app):
    requests, _run, state = _run_report_chat(report_app)

    assert len(requests) == 6

    # The read ran, and the inspection chosen beside it did not. Its result
    # names the instructions it was chosen without and the command they start with.
    after_first_batch = _tool_results(requests[1])
    assert "# Business Report Skill" in _text(after_first_batch["read-skill"])
    beside = after_first_batch["inspect-beside-read"]
    assert beside.status == "error"
    assert _text(beside).startswith("Not run: this call was chosen in the same message that loads the business-report skill's instructions"), _text(beside)
    assert "`scripts/report.py build`" in _text(beside)
    assert "xl/workbook.xml" not in _text(beside)

    # Each inspection chosen after the read returned is refused, however often
    # the model asks and whichever sandbox tool it uses, and none of them
    # reads the workbook.
    for request, call_id in ((requests[2], "inspect-again"), (requests[3], "inspect-months"), (requests[4], "read-upload")):
        refused = _tool_results(request)[call_id]
        assert refused.status == "error"
        assert _text(refused).startswith("Not run: The business-report skill's work starts with `scripts/report.py build`"), _text(refused)
        assert "xl/workbook.xml" not in _text(refused)
        assert "parts" not in _text(refused)
        assert "[Content_Types]" not in _text(refused)
        assert refused.additional_kwargs["deerflow_tool_meta"]["error_type"] == "not_run"

    # The build then runs, as the first thing the sandbox does in this chat.
    built = _tool_results(requests[5])["build"]
    assert built.status != "error", _text(built)
    assert _text(built).startswith("Built draft 1: August 2026 Business Review"), _text(built)
    sandbox_calls = [call["id"] for message in state["values"]["messages"] if message["type"] == "ai" for call in message["tool_calls"] if call["name"] == "bash" or call["id"] == "read-upload"]
    ran = [call_id for call_id in sandbox_calls if not _text_of(state, call_id).startswith("Not run:")]
    assert ran == ["build"]


def _text_of(state: dict[str, Any], call_id: str) -> str:
    message = next(message for message in state["values"]["messages"] if message["type"] == "tool" and message["tool_call_id"] == call_id)
    return message["content"] if isinstance(message["content"], str) else str(message["content"])


def test_after_the_build_the_model_may_inspect(report_app):
    """Once the build has run, the model's next choice is its own again."""
    requests, _run, _state = _run_report_chat(report_app, after_build=True)

    assert len(requests) == 7
    assert _text(_tool_results(requests[5])["build"]).startswith("Built draft 1:")
    inspected = _tool_results(requests[6])["inspect-after-build"]
    assert "parts" in _text(inspected), _text(inspected)
