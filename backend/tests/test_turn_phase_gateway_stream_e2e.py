"""Model-to-HTTP-stream timing through the real Gateway, worker, graph and route.

What runs for real here
-----------------------
* ``uvicorn`` serving ``app.gateway.app.create_app()`` on a loopback socket,
  with its lifespan (runtime, stores, run manager) started as in production;
* the authenticated ``POST /api/threads/{id}/runs/stream`` route, reached over
  a real HTTP connection by ``httpx`` with the same body shape the frontend
  sends, after a real registration and thread creation;
* the real worker (``run_agent``), the real ``lead_agent`` graph built by the
  real model factory from ``config.yaml``, and the real SSE consumer;
* callback invocation by LangChain/LangGraph on a ``BaseChatModel`` the
  factory instantiated through the ordinary ``models[].use`` path
  (``tests/_turn_phase_probe_model.py``): the phase handler sees the same
  ``on_chat_model_start`` / ``on_llm_new_token`` / ``on_llm_end`` a provider
  model produces, not calls made by the test.

What is synthetic
-----------------
Inference only: the probe model streams a scripted answer with controlled
delays (a hidden reasoning block, a pause, text chunks, a tail). The "slow
cleanup" after the model finishes is a delay injected in-process into the
stream bridge's ``publish_end``, so terminal completion is measurably later
than model completion. No sandbox is acquired (no tool runs), so the sandbox
phases are absent here by design; they are covered by the fake-backed
lifecycle suites.

The optional last test drives the released ``docker/nginx/nginx.conf`` in an
``nginx:alpine`` container published on ``127.0.0.1`` only, with a relay
container aliased ``gateway`` forwarding to a plain TCP forwarder that exists
only for the duration of that test, bound on the Docker bridge's host address
(reachable by any container on this host while it runs) and copying bytes to
the loopback Gateway. The Gateway itself listens on loopback only. The test
skips, and says so, whenever Docker, the bridge address, the network, the
image or the containers are unavailable.

What remains unobserved
-----------------------
Browser first paint (needs a browser through public ingress). Cross-process
and cross-replica delivery (the journal is captured in this process). A
post-terminal ``Last-Event-ID`` replay (never requested here). Tool and
heartbeat frames never appear on the wire in these runs (the probe emits no
tool calls and every run finishes inside the heartbeat interval), so their
exclusion from "first text" is proven only by the unit suite.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

FIRST_TEXT_DELAY_S = 0.6
TAIL_DELAY_S = 0.3
SLOW_CLEANUP_S = 0.5
HANG_DELAY_S = 30.0
# Scheduling noise on a loaded CI runner; the delays are chosen to dwarf it.
TOLERANCE = 0.85

_MINIMAL_CONFIG_YAML = """\
log_level: info
models:
  - name: turn-phase-probe
    display_name: Turn Phase Probe
    use: _turn_phase_probe_model:ProbeStreamingChatModel
    model: probe
sandbox:
  use: deerflow.sandbox.local:LocalSandboxProvider
agents_api:
  enabled: true
database:
  backend: sqlite
"""


# ── Journal capture ──────────────────────────────────────────────────────


class _JournalSink(logging.Handler):
    """Collects the one record each turn emits, by run id: fields and message."""

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self._lock = threading.Lock()
        self._by_run: dict[str, dict[str, Any]] = {}
        self._messages: dict[str, str] = {}

    def emit(self, record: logging.LogRecord) -> None:
        wire = getattr(record, "turn_phases", None)
        if isinstance(wire, dict) and isinstance(wire.get("run_id"), str):
            with self._lock:
                self._by_run[wire["run_id"]] = wire
                self._messages[wire["run_id"]] = record.getMessage()

    def message_for(self, run_id: str) -> str:
        """The line a deployment's formatter prints for *run_id*."""
        with self._lock:
            message = self._messages.get(run_id)
        assert message is not None, f"no turn-phase message was emitted for run {run_id}"
        return message

    def wait_for(self, run_id: str, *, timeout: float = 10.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                wire = self._by_run.get(run_id)
            if wire is not None:
                return wire
            time.sleep(0.02)
        raise AssertionError(f"no turn-phase journal was emitted for run {run_id}")


def _phase_at(wire: dict[str, Any], phase: str) -> float | None:
    for record in wire["phases"]:
        if record["phase"] == phase:
            return float(record["at_ms"])
    return None


# ── The Gateway under test ───────────────────────────────────────────────


def _reset_process_singletons(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.gateway import deps as deps_module
    from deerflow.config import app_config as app_config_module
    from deerflow.config import paths as paths_module
    from deerflow.persistence import engine as engine_module
    from deerflow.sandbox.sandbox_provider import shutdown_sandbox_provider

    # The provider singleton outlives a served Gateway; the next one must
    # build its own from its own config. So does the users-table provider
    # cached in deps: left alone, a second served Gateway would keep reading
    # the first one's accounts.
    shutdown_sandbox_provider()
    for module, attr in (
        (app_config_module, "_app_config"),
        (app_config_module, "_app_config_path"),
        (app_config_module, "_app_config_mtime"),
        (paths_module, "_paths_singleton"),
        (engine_module, "_engine"),
        (engine_module, "_session_factory"),
        (deps_module, "_cached_local_provider"),
        (deps_module, "_cached_repo"),
    ):
        monkeypatch.setattr(module, attr, None, raising=False)


@dataclass
class _Gateway:
    loopback_url: str
    loopback_port: int
    journals: _JournalSink
    tmp_home: Path


@dataclass
class _StreamObservation:
    run_id: str
    t_request: float
    t_first_byte: float | None = None
    t_first_text: float | None = None
    t_end: float | None = None
    events: list[str] = field(default_factory=list)
    # Every dispatched frame as ``(event, payload)``. ``events`` stays the
    # order-only view most timing assertions read; suites that assert on what a
    # control frame carried (the delivery-failure suite) read this.
    frames: list[tuple[str, Any]] = field(default_factory=list)
    text_frames: int = 0
    reasoning_frames: int = 0


def _frame_carries_text(data: Any) -> tuple[bool, bool]:
    """(has answer text, has hidden reasoning) for one ``messages`` frame, judged independently of the server's classifier."""
    message = data[0] if isinstance(data, list) and data else data
    if not isinstance(message, dict) or message.get("type") not in {"ai", "AIMessage", "AIMessageChunk"}:
        return False, False
    content = message.get("content")
    if isinstance(content, str):
        return bool(content.strip()), False
    text = reasoning = False
    for block in content or ():
        if isinstance(block, dict):
            if block.get("type") == "text" and str(block.get("text", "")).strip():
                text = True
            elif block.get("type") in {"reasoning", "thinking"}:
                reasoning = True
    return text, reasoning


@contextlib.contextmanager
def serve_gateway(home: Path, *, config_yaml: str = _MINIMAL_CONFIG_YAML) -> Iterator[_Gateway]:
    """Serve the real Gateway over ``home`` on a loopback socket.

    ``home/skills`` is the skill library the Gateway sees (a caller seeds
    ``public/<name>/SKILL.md`` there before entering); everything else under
    ``home`` is written here. Shared with the seeded-skill suite.
    """
    monkeypatch = pytest.MonkeyPatch()
    (home / "skills").mkdir(exist_ok=True)
    (home / "config.yaml").write_text(config_yaml, encoding="utf-8")
    (home / "extensions_config.json").write_text('{"mcpServers": {}, "skills": {}}', encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_HOME", str(home / "deer-flow-home"))
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(home / "config.yaml"))
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(home / "extensions_config.json"))
    monkeypatch.setenv("DEER_FLOW_SKILLS_PATH", str(home / "skills"))
    monkeypatch.setenv("HARTMESH_PROBE_FIRST_TEXT_DELAY_S", str(FIRST_TEXT_DELAY_S))
    monkeypatch.setenv("HARTMESH_PROBE_TAIL_DELAY_S", str(TAIL_DELAY_S))
    monkeypatch.setenv("HARTMESH_PROBE_HANG_DELAY_S", str(HANG_DELAY_S))
    if str(Path(__file__).parent) not in sys.path:
        sys.path.insert(0, str(Path(__file__).parent))
    _reset_process_singletons(monkeypatch)

    # No second model call for a thread title; the probe is the only model.
    from deerflow.config.memory_config import MemoryConfig
    from deerflow.config.summarization_config import SummarizationConfig
    from deerflow.config.title_config import TitleConfig

    # The conftest restores the title singleton after every test, so patch
    # the accessor rather than the singleton: it must hold for all four runs.
    monkeypatch.setattr("deerflow.config.title_config.get_title_config", lambda: TitleConfig(enabled=False))
    monkeypatch.setattr("deerflow.agents.middlewares.memory_middleware.get_memory_config", lambda: MemoryConfig(enabled=False))
    monkeypatch.setattr("deerflow.agents.memory.manager.get_memory_config", lambda: MemoryConfig(enabled=False))
    monkeypatch.setattr("deerflow.config.summarization_config._summarization_config", SummarizationConfig(enabled=False))

    # Slow cleanup, injected after the model is done and before the run is
    # terminal, so the two are distinguishable on both sides of the wire.
    import asyncio

    from deerflow.runtime.stream_bridge import MemoryStreamBridge

    original_publish_end = MemoryStreamBridge.publish_end

    async def _slow_publish_end(self, run_id: str, *args: Any, **kwargs: Any):
        await asyncio.sleep(SLOW_CLEANUP_S)
        return await original_publish_end(self, run_id, *args, **kwargs)

    monkeypatch.setattr(MemoryStreamBridge, "publish_end", _slow_publish_end)

    sink = _JournalSink()
    journal_logger = logging.getLogger("deerflow.runtime.turn_phases")
    previous_level = journal_logger.level
    journal_logger.setLevel(logging.INFO)
    journal_logger.addHandler(sink)

    from deerflow.config import app_config as app_config_module

    # A config the Gateway refuses at load is a result some suites assert on
    # (a validation error); the environment set above must not outlive it
    # either, or every later test in the worker reads this Gateway's config.
    try:
        cfg = app_config_module.get_app_config()
    except Exception:
        journal_logger.removeHandler(sink)
        journal_logger.setLevel(previous_level)
        monkeypatch.undo()
        raise
    cfg.database.sqlite_dir = str(home / "deer-flow-home" / "db")

    import uvicorn

    from app.gateway.app import create_app

    loopback = socket.socket()
    loopback.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    loopback.bind(("127.0.0.1", 0))
    loopback.listen()
    loopback_port = loopback.getsockname()[1]
    loopback_url = f"http://127.0.0.1:{loopback_port}"

    server = uvicorn.Server(uvicorn.Config(create_app(), log_level="warning", lifespan="on"))
    thread = threading.Thread(target=lambda: asyncio.run(server.serve(sockets=[loopback])), name="turn-phase-e2e-gateway", daemon=True)
    thread.start()

    def _tear_down() -> None:
        server.should_exit = True
        thread.join(timeout=30)
        with contextlib.suppress(OSError):
            loopback.close()
        journal_logger.removeHandler(sink)
        journal_logger.setLevel(previous_level)
        from deerflow.sandbox.sandbox_provider import shutdown_sandbox_provider

        shutdown_sandbox_provider()
        monkeypatch.undo()

    # A Gateway that refuses to start is a result some suites assert on; the
    # environment and singletons this function set must not outlive it either,
    # or every later test in the worker reads this Gateway's config.
    try:
        deadline = time.monotonic() + 60
        while not server.started:
            if not thread.is_alive():
                raise RuntimeError("the test Gateway exited before it started")
            if time.monotonic() > deadline:
                raise TimeoutError("the test Gateway did not start in time")
            time.sleep(0.05)
    except BaseException:
        _tear_down()
        raise

    try:
        yield _Gateway(loopback_url=loopback_url, loopback_port=loopback_port, journals=sink, tmp_home=home)
    finally:
        _tear_down()


@pytest.fixture(scope="module")
def gateway(tmp_path_factory: pytest.TempPathFactory) -> Iterator[_Gateway]:
    with serve_gateway(tmp_path_factory.mktemp("turn-phase-e2e")) as served:
        yield served


# ── Driving the route ────────────────────────────────────────────────────


def _register_and_create_thread(client: httpx.Client, base: str, *, langgraph_prefix: str = "/api") -> tuple[str, str]:
    email = f"turn-phase-{uuid.uuid4().hex[:10]}@example.com"
    registered = client.post(f"{base}/api/v1/auth/register", json={"email": email, "password": "very-strong-password-123"})
    assert registered.status_code == 201, registered.text
    csrf = client.cookies.get("csrf_token")
    assert csrf, "register must set the CSRF cookie"
    thread_id = str(uuid.uuid4())
    created = client.post(f"{base}{langgraph_prefix}/threads", json={"thread_id": thread_id, "metadata": {}}, headers={"X-CSRF-Token": csrf})
    assert created.status_code == 200, created.text
    return csrf, thread_id


def _run_body(text: str, *, recursion_limit: int = 25) -> dict[str, Any]:
    return {
        "assistant_id": "lead_agent",
        "input": {"messages": [{"role": "user", "content": text}]},
        "config": {"recursion_limit": recursion_limit},
        "context": {"thinking_enabled": False, "subagent_enabled": False},
        "stream_mode": ["messages-tuple"],
    }


def _observe_stream(
    client: httpx.Client,
    base: str,
    thread_id: str,
    csrf: str,
    text: str,
    *,
    langgraph_prefix: str = "/api",
    on_frame: Any = None,
    timeout: float = 60.0,
    recursion_limit: int = 25,
) -> _StreamObservation:
    """POST the run and time what the client can see, frame by frame.

    ``on_frame`` is called after every dispatched frame with the observation
    so far, so a caller can act (cancel, say) once a specific frame has been
    seen rather than on a timer.
    """
    t_request = time.monotonic()
    with client.stream(
        "POST",
        f"{base}{langgraph_prefix}/threads/{thread_id}/runs/stream",
        json=_run_body(text, recursion_limit=recursion_limit),
        headers={"X-CSRF-Token": csrf, "Accept": "text/event-stream"},
        timeout=timeout,
    ) as response:
        assert response.status_code == 200, response.read().decode()
        assert response.headers.get("content-type", "").startswith("text/event-stream")
        run_id = response.headers["content-location"].rsplit("/", 1)[-1]
        observation = _StreamObservation(run_id=run_id, t_request=t_request)
        event: str | None = None
        data_lines: list[str] = []
        for line in response.iter_lines():
            now = time.monotonic()
            if observation.t_first_byte is None:
                observation.t_first_byte = now
            if line.startswith(":"):
                continue
            if line.startswith("event:"):
                event = line[len("event:") :].strip()
                continue
            if line.startswith("data:"):
                data_lines.append(line[len("data:") :].strip())
                continue
            if line != "":
                continue
            if event is None:
                data_lines = []
                continue
            observation.events.append(event)
            payload: Any = None
            if data_lines:
                with contextlib.suppress(ValueError):
                    payload = json.loads("\n".join(data_lines))
            observation.frames.append((event, payload))
            if event == "messages":
                has_text, has_reasoning = _frame_carries_text(payload)
                if has_reasoning:
                    observation.reasoning_frames += 1
                if has_text:
                    observation.text_frames += 1
                    if observation.t_first_text is None:
                        observation.t_first_text = now
            if on_frame is not None:
                on_frame(observation)
            event = None
            data_lines = []
            if observation.events[-1] == "end":
                observation.t_end = now
                break
    return observation


# ── Tests over loopback ──────────────────────────────────────────────────
