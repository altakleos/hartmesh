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
    from deerflow.config import app_config as app_config_module
    from deerflow.config import paths as paths_module
    from deerflow.persistence import engine as engine_module
    from deerflow.sandbox.sandbox_provider import shutdown_sandbox_provider

    # The provider singleton outlives a served Gateway; the next one must
    # build its own from its own config.
    shutdown_sandbox_provider()
    for module, attr in (
        (app_config_module, "_app_config"),
        (app_config_module, "_app_config_path"),
        (app_config_module, "_app_config_mtime"),
        (paths_module, "_paths_singleton"),
        (engine_module, "_engine"),
        (engine_module, "_session_factory"),
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

    cfg = app_config_module.get_app_config()
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
    deadline = time.monotonic() + 60
    while not server.started:
        if not thread.is_alive():
            raise RuntimeError("the test Gateway exited before it started")
        if time.monotonic() > deadline:
            raise TimeoutError("the test Gateway did not start in time")
        time.sleep(0.05)

    try:
        yield _Gateway(loopback_url=loopback_url, loopback_port=loopback_port, journals=sink, tmp_home=home)
    finally:
        server.should_exit = True
        thread.join(timeout=30)
        with contextlib.suppress(OSError):
            loopback.close()
        journal_logger.removeHandler(sink)
        journal_logger.setLevel(previous_level)
        from deerflow.sandbox.sandbox_provider import shutdown_sandbox_provider

        shutdown_sandbox_provider()
        monkeypatch.undo()


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


def _run_body(text: str) -> dict[str, Any]:
    return {
        "assistant_id": "lead_agent",
        "input": {"messages": [{"role": "user", "content": text}]},
        "config": {"recursion_limit": 25},
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
        json=_run_body(text),
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


def test_text_is_delivered_incrementally_and_the_phases_line_up(gateway: _Gateway) -> None:
    """Model request, first provider text, first outgoing text, completion, terminal.

    The client must see the first answer text well before the end frame (a
    buffered response would collapse the three timestamps together), and the
    journal must place the same events in the same order on its own clock.
    """
    with httpx.Client() as client:
        csrf, thread_id = _register_and_create_thread(client, gateway.loopback_url)
        observed = _observe_stream(client, gateway.loopback_url, thread_id, csrf, "probe:text please")

    assert observed.t_first_byte is not None and observed.t_first_text is not None and observed.t_end is not None
    assert observed.text_frames >= 1 and observed.reasoning_frames >= 1
    assert observed.events[-1] == "end"
    client_first_text = observed.t_first_text - observed.t_first_byte
    client_tail = observed.t_end - observed.t_first_text
    assert client_first_text >= FIRST_TEXT_DELAY_S * TOLERANCE, f"first text arrived {client_first_text:.3f}s after the first byte: the body was buffered or reasoning counted as text"
    assert client_tail >= (TAIL_DELAY_S + SLOW_CLEANUP_S) * TOLERANCE, f"end arrived {client_tail:.3f}s after first text: streaming did not precede completion"

    wire = gateway.journals.wait_for(observed.run_id)
    model_request = _phase_at(wire, "model_request")
    provider_text = _phase_at(wire, "first_provider_text")
    stream_text = _phase_at(wire, "first_stream_text")
    completion = _phase_at(wire, "model_completion")
    terminal = _phase_at(wire, "terminal")
    admission = _phase_at(wire, "admission")
    assert None not in (admission, model_request, provider_text, stream_text, completion, terminal), wire
    assert admission <= model_request < provider_text <= stream_text < completion < terminal, wire
    assert provider_text - model_request >= FIRST_TEXT_DELAY_S * 1000 * TOLERANCE, "the provider's first text was credited to the reasoning chunk"
    assert stream_text - provider_text < FIRST_TEXT_DELAY_S * 1000, "the outgoing text mark drifted away from the provider mark"
    assert completion - stream_text >= TAIL_DELAY_S * 1000 * TOLERANCE
    assert terminal - completion >= SLOW_CLEANUP_S * 1000 * TOLERANCE, "cleanup after the model was not separated from completion"
    assert wire["outcome"] == "success"
    assert wire["acquisition_source"] is None, "no tool ran, so no sandbox was acquired"
    unobservable = {entry["phase"] for entry in wire["unobservable"]}
    assert "browser_first_text" in unobservable
    assert "first_stream_text" not in unobservable
    print(
        "turn-phase e2e (loopback, uvicorn+httpx, probe model): "
        f"client first_text={client_first_text:.3f}s tail={client_tail:.3f}s; "
        f"journal model_request={model_request:.0f}ms provider_text={provider_text:.0f}ms stream_text={stream_text:.0f}ms "
        f"completion={completion:.0f}ms terminal={terminal:.0f}ms total={wire['total_ms']:.0f}ms"
    )


def test_the_emitted_line_carries_the_turn_timing_a_deployment_can_read(gateway: _Gateway) -> None:
    """One line, one turn: the offsets an operator needs without a second source.

    A deployment prints ``%(message)s``. Reading a turn's timing from a
    released Gateway therefore has to be possible from the message alone --
    including the outgoing-text offset, which no SSE body timestamps and no
    other log line records.
    """
    with httpx.Client() as client:
        csrf, thread_id = _register_and_create_thread(client, gateway.loopback_url)
        observed = _observe_stream(client, gateway.loopback_url, thread_id, csrf, "probe:text please")

    wire = gateway.journals.wait_for(observed.run_id)
    message = gateway.journals.message_for(observed.run_id)

    assert message.startswith("turn phase timings")
    assert f"run={observed.run_id}" in message
    assert "outcome=success" in message
    for phase in ("admission", "model_request", "first_provider_text", "first_stream_text", "model_completion", "terminal"):
        assert f"{phase}@" in message, message
    stream_text_ms = _phase_at(wire, "first_stream_text")
    assert f"first_stream_text@{round(stream_text_ms)}ms" in message
    assert "unobservable=browser_first_text(" in message
    # The route's own interval -- request received to worker admission -- is
    # the part of the ≤2 s acknowledgement target a server can measure, and it
    # reads from the same line, before the phases it precedes.
    assert "launch=" in message and message.index("launch=") < message.index("phases="), message
    launch = wire["launch"]
    assert launch is not None and launch["total_ms"] > 0, wire
    assert [step["step"] for step in launch["steps"]] == ["identify", "permit", "seal", "authorize", "constrain", "prepare", "persist"], launch
    assert launch["handoff_ms"] >= 0, launch
    print(f"turn-phase e2e (released log line): {message}")


def test_the_time_before_the_model_request_is_accounted_for(gateway: _Gateway) -> None:
    """Admission to model request, with no unnamed gap in between.

    Tenant-class .15 read 2.6 to 3.4 s of every turn between the sandbox
    lookup ending and the binding starting with no phase to attribute it to.
    The three pre-model phases have to sit in the window and in order, so the
    next run can say which of building the graph, loading the thread's state
    or the graph's own start owns the time.
    """
    with httpx.Client() as client:
        csrf, thread_id = _register_and_create_thread(client, gateway.loopback_url)
        observed = _observe_stream(client, gateway.loopback_url, thread_id, csrf, "probe:text please")

    wire = gateway.journals.wait_for(observed.run_id)
    admission = _phase_at(wire, "admission")
    assembly = _phase_at(wire, "assembly")
    agent_build = _phase_at(wire, "agent_build")
    preflight = _phase_at(wire, "checkpoint_preflight")
    graph_start = _phase_at(wire, "graph_start")
    model_request = _phase_at(wire, "model_request")
    assert None not in (admission, assembly, agent_build, preflight, graph_start, model_request), wire
    assert admission <= assembly <= agent_build <= preflight <= graph_start <= model_request, wire
    assert not any(record["phase"] == "skill_materialization" for record in wire["phases"]), "an ordinary turn projects no accepted snapshot, so that phase must be absent rather than zero"
    build = next(record for record in wire["phases"] if record["phase"] == "agent_build")
    assert "duration_ms" in build, build  # a span, not a bare mark
    build_ms = build["duration_ms"]
    message = gateway.journals.message_for(observed.run_id)
    for phase in ("agent_build@", "checkpoint_preflight@", "graph_start@"):
        assert phase in message, message
    print(
        "turn-phase e2e (pre-model window): "
        f"admission={admission:.0f}ms assembly={assembly:.0f}ms agent_build={agent_build:.0f}ms(+{build_ms:.0f}ms) "
        f"checkpoint_preflight={preflight:.0f}ms graph_start={graph_start:.0f}ms model_request={model_request:.0f}ms"
    )


def test_a_silent_turn_manufactures_no_text_timestamps(gateway: _Gateway) -> None:
    with httpx.Client() as client:
        csrf, thread_id = _register_and_create_thread(client, gateway.loopback_url)
        observed = _observe_stream(client, gateway.loopback_url, thread_id, csrf, "probe:silent")

    assert observed.t_end is not None
    assert observed.text_frames == 0
    assert observed.reasoning_frames >= 1, "the hidden reasoning did reach the wire"
    wire = gateway.journals.wait_for(observed.run_id)
    assert _phase_at(wire, "model_request") is not None
    assert _phase_at(wire, "model_completion") is not None
    assert _phase_at(wire, "first_provider_text") is None
    assert _phase_at(wire, "first_stream_text") is None
    assert "first_stream_text" not in {entry["phase"] for entry in wire["unobservable"]}, "nothing to observe is not a missed observation"
    assert wire["outcome"] == "success"


def test_a_cancelled_turn_manufactures_no_text_timestamps(gateway: _Gateway) -> None:
    cancelled_at: dict[str, float] = {}

    with httpx.Client() as client:
        csrf, thread_id = _register_and_create_thread(client, gateway.loopback_url)

        def _cancel(observation: _StreamObservation) -> None:
            # Cancel once the model is provably running: its hidden reasoning
            # chunk has reached the wire. Earlier, and there would be no model
            # request to assert on.
            if "t" in cancelled_at or observation.reasoning_frames == 0:
                return
            response = client.post(
                f"{gateway.loopback_url}/api/threads/{thread_id}/runs/{observation.run_id}/cancel",
                headers={"X-CSRF-Token": csrf},
            )
            assert response.status_code in (202, 204), response.text
            cancelled_at["t"] = time.monotonic()

        observed = _observe_stream(client, gateway.loopback_url, thread_id, csrf, "probe:hang", on_frame=_cancel, timeout=20.0)

    assert observed.t_end is not None, "the stream must end after the cancel"
    assert observed.t_end - cancelled_at["t"] < HANG_DELAY_S / 2, "the hang was interrupted, not waited out"
    assert observed.text_frames == 0
    wire = gateway.journals.wait_for(observed.run_id)
    assert _phase_at(wire, "model_request") is not None
    assert _phase_at(wire, "first_provider_text") is None
    assert _phase_at(wire, "first_stream_text") is None
    assert _phase_at(wire, "terminal") is not None
    assert wire["outcome"] != "success"


# ── The released nginx stream path ───────────────────────────────────────


_RELAY_CONF = """\
events {{}}
http {{
    server {{
        listen 8001;
        location / {{
            proxy_pass {upstream};
            proxy_http_version 1.1;
            proxy_buffering off;
            proxy_set_header Host $http_host;
            proxy_set_header Connection '';
        }}
    }}
}}
"""


class _TcpForwarder:
    """A byte-for-byte TCP forwarder on the Docker bridge's host address.

    Exists only while the nginx test runs. It is the one thing a container
    can reach, and it copies bytes to the loopback Gateway with no buffering
    beyond the kernel's, so it cannot mask or manufacture incremental delivery.
    """

    def __init__(self, bind_ip: str, target_port: int) -> None:
        self._target_port = target_port
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind((bind_ip, 0))
        self._listener.listen()
        self._listener.settimeout(0.2)
        self.port = self._listener.getsockname()[1]
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._accept_thread = threading.Thread(target=self._accept_loop, name="turn-phase-e2e-forwarder", daemon=True)
        self._accept_thread.start()

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                client, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            try:
                upstream = socket.create_connection(("127.0.0.1", self._target_port), timeout=5)
            except OSError:
                client.close()
                continue
            for source, sink in ((client, upstream), (upstream, client)):
                thread = threading.Thread(target=self._pump, args=(source, sink), daemon=True)
                thread.start()
                self._threads.append(thread)

    @staticmethod
    def _pump(source: socket.socket, sink: socket.socket) -> None:
        try:
            while True:
                chunk = source.recv(65536)
                if not chunk:
                    break
                sink.sendall(chunk)
        except OSError:
            pass
        finally:
            with contextlib.suppress(OSError):
                sink.shutdown(socket.SHUT_WR)

    def close(self) -> None:
        self._stop.set()
        with contextlib.suppress(OSError):
            self._listener.close()
        self._accept_thread.join(timeout=2)


def _docker(*args: str, timeout: float = 60) -> subprocess.CompletedProcess[str]:
    """Run one docker command; any failure to run it at all becomes a skip."""
    try:
        return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        pytest.skip(f"docker {args[0]} unavailable ({type(exc).__name__}): the released nginx stream path stays untested here")


@contextlib.contextmanager
def _released_nginx(gateway: _Gateway, tmp_path: Path) -> Iterator[str]:
    """The released nginx config in ``nginx:alpine``, published on loopback only.

    ``docker-compose.yaml`` mounts ``docker/nginx/nginx.conf`` as a template
    and copies it into place before starting nginx; this does the same. The
    config resolves ``gateway`` through Docker's embedded DNS, so a relay
    container carries that alias and forwards to a test-scoped forwarder on
    the bridge's host address, which copies bytes to the loopback Gateway.
    Every unavailable prerequisite skips; only the released nginx answering
    on loopback turns the assertions hard.
    """
    if _docker("info", timeout=5).returncode != 0:
        pytest.skip("Docker is not available: the released nginx stream path stays untested here")
    suffix = uuid.uuid4().hex[:6]
    network = f"hm-turn-phase-{os.getpid()}-{suffix}"
    relay_name = f"hm-turn-phase-relay-{suffix}"
    nginx_name = f"hm-turn-phase-nginx-{suffix}"
    started: list[str] = []
    forwarder: _TcpForwarder | None = None
    network_created = False
    try:
        if _docker("network", "create", network, timeout=30).returncode != 0:
            pytest.skip("could not create a Docker network: the released nginx stream path stays untested here")
        network_created = True
        bridge_ip = _docker("network", "inspect", network, "-f", "{{(index .IPAM.Config 0).Gateway}}", timeout=30).stdout.strip()
        try:
            forwarder = _TcpForwarder(bridge_ip, gateway.loopback_port)
        except OSError as exc:
            pytest.skip(f"cannot bind the Docker bridge address {bridge_ip!r} ({exc}): the released nginx stream path stays untested here")
        relay_conf = tmp_path / "relay.conf"
        relay_conf.write_text(_RELAY_CONF.format(upstream=f"http://{bridge_ip}:{forwarder.port}"), encoding="utf-8")
        for name, args in (
            (
                relay_name,
                ["--network-alias", "gateway", "-v", f"{relay_conf}:/etc/nginx/nginx.conf:ro", "nginx:alpine"],
            ),
            (
                nginx_name,
                [
                    "-p",
                    "127.0.0.1:0:2026",
                    "-v",
                    f"{REPO_ROOT / 'docker' / 'nginx' / 'nginx.conf'}:/etc/nginx/nginx.conf.template:ro",
                    "nginx:alpine",
                    "sh",
                    "-c",
                    "cp /etc/nginx/nginx.conf.template /etc/nginx/nginx.conf && nginx -g 'daemon off;'",
                ],
            ),
        ):
            result = _docker("run", "-d", "--rm", "--name", name, "--network", network, *args, timeout=180)
            if result.returncode != 0:
                pytest.skip(f"could not start {name} ({result.stderr.strip()[-200:]}): the released nginx stream path stays untested here")
            started.append(name)
        published = _docker("port", nginx_name, "2026", timeout=30).stdout.strip().splitlines()
        if not published or not published[0].startswith("127.0.0.1:"):
            pytest.skip(f"released nginx was not published on loopback ({published}): the released nginx stream path stays untested here")
        base = f"http://{published[0]}"
        deadline = time.monotonic() + 30
        while True:
            try:
                httpx.get(f"{base}/api/langgraph/threads", timeout=2.0)
                break
            except httpx.HTTPError:
                if time.monotonic() > deadline:
                    logs = _docker("logs", nginx_name, timeout=30)
                    pytest.skip(f"released nginx did not answer on {base} ({logs.stderr[-300:]!r}): the released nginx stream path stays untested here")
                time.sleep(0.2)
        yield base
    finally:
        for name in started:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=60)
        if forwarder is not None:
            forwarder.close()
        if network_created:
            subprocess.run(["docker", "network", "rm", network], capture_output=True, timeout=30)


def test_the_released_nginx_stream_path_delivers_text_incrementally(gateway: _Gateway, tmp_path: Path) -> None:
    with _released_nginx(gateway, tmp_path) as base, httpx.Client() as client:
        csrf, thread_id = _register_and_create_thread(client, base, langgraph_prefix="/api/langgraph")
        observed = _observe_stream(client, base, thread_id, csrf, "probe:text through nginx", langgraph_prefix="/api/langgraph")

    assert observed.t_first_byte is not None and observed.t_first_text is not None and observed.t_end is not None
    client_first_text = observed.t_first_text - observed.t_first_byte
    client_tail = observed.t_end - observed.t_first_text
    assert client_first_text >= FIRST_TEXT_DELAY_S * TOLERANCE, f"through nginx, first text arrived {client_first_text:.3f}s after the first byte: the proxy buffered the stream"
    assert client_tail >= (TAIL_DELAY_S + SLOW_CLEANUP_S) * TOLERANCE
    wire = gateway.journals.wait_for(observed.run_id)
    assert _phase_at(wire, "first_stream_text") is not None
    assert wire["outcome"] == "success"
    print(f"turn-phase e2e (released nginx.conf in nginx:alpine on 127.0.0.1, relay alias gateway): client first_text={client_first_text:.3f}s tail={client_tail:.3f}s")
