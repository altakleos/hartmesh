"""What a person gets from an accepted-skill turn while every sandbox slot is busy.

Everything here runs the production route of the released tenant profile: a
real ``run_agent`` over an accepted admission with a nonempty skill snapshot,
the real accepted materialization, a real ``create_agent`` graph with the real
``SandboxMiddleware``, ``bash`` tool and ``ToolErrorHandlingMiddleware``, and
the real AIO provider's admission over a fake container backend holding both
slots. Only the model is scripted. A refusal raised from a substitute agent
skips the boundary that lost the capacity type on ``.33``, so it is not used
here.

Saturation is built both ways a saturated deployment presents: two sets executing
(``active=2``) and two sets still being created (``starting=2``).
"""

from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _agent_e2e_helpers import FakeToolCallingModel
from langchain_core.messages import AIMessage
from test_accepted_skill_snapshots import (  # noqa: F401 - the fixture is used by name
    _TEST_TENANT,
    _accepted,
    _parsed_skill,
    _resolve_revision,
    _write_skill,
    snapshot_paths,
)
from test_sandbox_warm_reuse_latency import _aio_mod, _FakeBackend, _make_provider

from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.worker import SANDBOX_CAPACITY_STOP_REASON, RunContext, run_agent

OTHER_USER = "capacity-holder"


def _live(backend: _FakeBackend) -> list[str]:
    return sorted(sandbox_id for sandbox_id, alive in backend.alive.items() if alive)


def _saturate(provider, how: str) -> list[str]:
    """Hold both slots for other threads, executing or still being created."""
    if how == "active":
        return [provider.acquire(f"holder-{index}", user_id=OTHER_USER) for index in range(2)]
    provider._starting.update({"starting-a", "starting-b"})
    return []


def _waiting_signal(monkeypatch) -> threading.Event:
    """Set when an acquisition starts waiting for a slot, by the provider's own count."""
    aio = _aio_mod()
    waiting = threading.Event()
    counted = aio.record_capacity_wait

    def _count_and_signal() -> None:
        counted()
        waiting.set()

    monkeypatch.setattr(aio, "record_capacity_wait", _count_and_signal)
    return waiting


def _install_provider(monkeypatch, provider) -> None:
    """Install the provider, on the released tenant profile, stated here.

    The worker picks projection or durable materialization from the
    deployment profile; left to the machine's own ``config.yaml`` the result
    would depend on where the suite runs.
    """
    from deerflow.config.app_config import AppConfig
    from deerflow.config.deployment_config import DeploymentConfig
    from deerflow.config.sandbox_config import SandboxConfig

    tenant_profile = AppConfig(sandbox=SandboxConfig(use="test"), deployment=DeploymentConfig(profile="local_development"))

    async def _tenant_profile():
        return tenant_profile

    monkeypatch.setattr("deerflow.authz.sandbox_authz.safe_app_config_async", _tenant_profile)
    for target in (
        "deerflow.sandbox.get_sandbox_provider",
        "deerflow.sandbox.sandbox_provider.get_sandbox_provider",
        "deerflow.sandbox.tools.get_sandbox_provider",
        "deerflow.sandbox.middleware.get_sandbox_provider",
    ):
        monkeypatch.setattr(target, lambda: provider)


def _agent_factory(model):
    from langchain.agents import create_agent
    from langchain_core.tools import tool

    from deerflow.agents.middlewares.tool_error_handling_middleware import ToolErrorHandlingMiddleware
    from deerflow.sandbox.middleware import SandboxMiddleware
    from deerflow.sandbox.tools import bash_tool

    @tool
    def opening_hours(day: str) -> str:
        """Return the shop's opening hours for a day."""
        return f"{day}: 9 to 17"

    def factory(*, config):
        del config
        return create_agent(
            model=model,
            tools=[bash_tool, opening_hours],
            middleware=[SandboxMiddleware(), ToolErrorHandlingMiddleware()],
        )

    return factory


class _CountingModel(FakeToolCallingModel):
    calls: int = 0

    def _generate(self, *args, **kwargs):
        object.__setattr__(self, "calls", self.calls + 1)
        return super()._generate(*args, **kwargs)


def _bash_turn_model() -> _CountingModel:
    return _CountingModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[{"name": "bash", "args": {"description": "list", "command": "ls /mnt/user-data"}, "id": "call-1", "type": "tool_call"}],
            ),
            AIMessage(content="The workspace is busy right now."),
        ]
    )


async def _admitted_run(monkeypatch, tmp_path):
    skill_file = _write_skill(tmp_path, body="Capacity outcome", allowed_tools="bash")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    manager = RunManager(tenant=_TEST_TENANT)
    # Admitted for the signed-in user the accepted invocation names, as the
    # Gateway admits it: the record, the run context and the tool path must
    # all resolve the same person.
    record = await manager.create_or_reject("thread-1", user_id="user-1", accepted_invocation=_accepted(revision))
    return manager, record


def _bridge():
    return SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())


def _published(bridge, event: str) -> list[dict]:
    return [call.args[2] for call in bridge.publish.await_args_list if call.args[1] == event]


async def _run(bridge, manager, record, model):
    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=None, tenant=_TEST_TENANT),
        agent_factory=_agent_factory(model),
        graph_input={"messages": [{"role": "user", "content": "hello"}]},
        # The Gateway injects the signed-in user into the run context; the
        # worker and the tool path must resolve the same one.
        config={"recursion_limit": 1000, "context": {"user_id": "user-1"}},
        stream_modes=["values"],
    )


# ── An eligible no-tool turn during saturation ───────────────────────────


@pytest.mark.anyio
@pytest.mark.parametrize("how", ["active", "starting"])
async def test_a_turn_that_uses_no_tool_answers_while_every_slot_is_busy(tmp_path, monkeypatch, snapshot_paths, how):  # noqa: F811
    """The eligibility contract: a turn that calls no sandbox tool never needs one.

    On ``.33`` the accepted snapshot was materialized into a sandbox before the
    first model call, so this turn waited the whole capacity budget and ended
    as ``Runtime operation failed`` without the model ever being asked.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    held = _saturate(provider, how)
    _install_provider(monkeypatch, provider)
    manager, record = await _admitted_run(monkeypatch, tmp_path)
    model = _CountingModel(responses=[AIMessage(content="Plain chat works.")])
    bridge = _bridge()

    started = time.monotonic()
    await _run(bridge, manager, record, model)
    elapsed = time.monotonic() - started

    assert record.status is RunStatus.success, _published(bridge, "error")
    assert model.calls == 1
    assert elapsed < 2.0, f"the turn waited for a slot it never needed ({elapsed:.2f}s)"
    assert _live(backend) == sorted(held), "no set was created, and none was evicted"
    assert backend.destroyed == []
    assert provider._starting == ({"starting-a", "starting-b"} if how == "starting" else set())


@pytest.mark.anyio
async def test_a_turn_that_calls_only_a_non_sandbox_tool_answers_while_every_slot_is_busy(tmp_path, monkeypatch, snapshot_paths):  # noqa: F811
    """The contract is about sandbox-backed tools, not tools in general."""
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    held = _saturate(provider, "active")
    _install_provider(monkeypatch, provider)
    manager, record = await _admitted_run(monkeypatch, tmp_path)
    model = _CountingModel(
        responses=[
            AIMessage(content="", tool_calls=[{"name": "opening_hours", "args": {"day": "Monday"}, "id": "call-1", "type": "tool_call"}]),
            AIMessage(content="Monday: 9 to 17."),
        ]
    )
    bridge = _bridge()

    started = time.monotonic()
    await _run(bridge, manager, record, model)
    elapsed = time.monotonic() - started

    assert record.status is RunStatus.success, _published(bridge, "error")
    assert record.stop_reason is None
    assert model.calls == 2
    assert elapsed < 2.0, f"the turn waited for a slot it never needed ({elapsed:.2f}s)"
    assert _live(backend) == sorted(held), "no set was created, and none was evicted"
    assert backend.destroyed == []


@pytest.mark.anyio
async def test_a_tool_turn_with_a_free_slot_acquires_one_sandbox_at_its_first_tool_call(tmp_path, monkeypatch, snapshot_paths):  # noqa: F811
    """The ordinary case: one set, parked at the end, its preparation timed after the model."""
    from deerflow.runtime.turn_phases import TurnPhase, TurnPhaseJournal

    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    _install_provider(monkeypatch, provider)
    manager, record = await _admitted_run(monkeypatch, tmp_path)
    snapshots = []
    emit = TurnPhaseJournal.emit

    def _captured(self, *args, **kwargs):
        snapshot = emit(self, *args, **kwargs)
        snapshots.append(snapshot)
        return snapshot

    monkeypatch.setattr(TurnPhaseJournal, "emit", _captured)
    bridge = _bridge()

    await _run(bridge, manager, record, _bash_turn_model())

    assert record.status is RunStatus.success, _published(bridge, "error")
    assert record.stop_reason is None
    assert len(backend.created) == 1
    assert provider._sandboxes == {}, "parked when the run ended"
    assert list(provider._warm_pool) == backend.created
    [journal] = snapshots
    model_at = journal.phase_at_ms(TurnPhase.MODEL_REQUEST)
    for phase in (TurnPhase.SKILL_PROJECTION, TurnPhase.SKILL_SNAPSHOT_BIND):
        at = journal.phase_at_ms(phase)
        assert at is not None and at > model_at, (phase, at, model_at)


# ── A sandbox-requiring turn during saturation ───────────────────────────


@pytest.mark.anyio
@pytest.mark.parametrize("how", ["active", "starting"])
async def test_a_tool_turn_at_capacity_ends_with_the_capacity_state_not_a_crash(tmp_path, monkeypatch, snapshot_paths, how):  # noqa: F811
    """The refusal keeps its type all the way to the person and the run record.

    On ``.33`` the accepted materialization boundary replaced
    ``SandboxCapacityExceededError`` with an opaque binding error, and the
    person read ``Runtime operation failed (reference: ...)``.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 0
    held = _saturate(provider, how)
    _install_provider(monkeypatch, provider)
    manager, record = await _admitted_run(monkeypatch, tmp_path)
    model = _bash_turn_model()
    bridge = _bridge()

    await _run(bridge, manager, record, model)

    errors = _published(bridge, "error")
    assert all("Runtime operation failed" not in str(error) for error in errors), errors
    assert record.stop_reason == SANDBOX_CAPACITY_STOP_REASON, (record.status, record.error, errors)
    assert model.calls <= 2, "the model may say the workspace is busy; it is never asked to try again"
    assert _live(backend) == sorted(held), "no third set"
    assert backend.destroyed == [], "no live turn evicted"
    assert provider._starting == ({"starting-a", "starting-b"} if how == "starting" else set()), "no reservation left behind"


@pytest.mark.anyio
async def test_a_turn_that_gets_its_sandbox_after_a_refusal_is_not_recorded_as_refused(tmp_path, monkeypatch, snapshot_paths):  # noqa: F811
    """The capacity reason says the turn's sandbox work never ran; here it did.

    A scheduled task reads that reason as a failed occurrence, so a turn whose
    retry found a slot must not carry it.
    """
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 0
    held = _saturate(provider, "active")
    _install_provider(monkeypatch, provider)
    manager, record = await _admitted_run(monkeypatch, tmp_path)

    class _RetryingModel(_CountingModel):
        def _generate(self, *args, **kwargs):
            if self.calls == 1:
                provider.release(held[0])  # the other thread finished between the calls
            return super()._generate(*args, **kwargs)

    bash = {"name": "bash", "args": {"description": "list", "command": "ls /mnt/user-data"}, "type": "tool_call"}
    model = _RetryingModel(
        responses=[
            AIMessage(content="", tool_calls=[{**bash, "id": "call-1"}]),
            AIMessage(content="", tool_calls=[{**bash, "id": "call-2"}]),
            AIMessage(content="Done."),
        ]
    )
    bridge = _bridge()

    await _run(bridge, manager, record, model)

    assert record.status is RunStatus.success, _published(bridge, "error")
    assert model.calls == 3
    assert len(backend.created) == len(held) + 1, "the retry got its sandbox"
    assert record.stop_reason is None


# ── Stop while the turn waits for a slot ─────────────────────────────────


@pytest.mark.anyio
@pytest.mark.parametrize("attempt", range(3))
async def test_stop_during_the_capacity_wait_ends_the_turn_as_cancelled_promptly(tmp_path, monkeypatch, snapshot_paths, attempt):  # noqa: F811
    """Stop is acknowledged at once and the turn ends cancelled, not as an error.

    On ``.33`` the accepted acquisition waited in a worker thread nothing could
    interrupt: Stop returned after about 4.6 s and the turn still ended as a
    generic error when the wait ran out.
    """
    del attempt
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 5
    held = _saturate(provider, "active")
    waiting = _waiting_signal(monkeypatch)
    _install_provider(monkeypatch, provider)
    manager, record = await _admitted_run(monkeypatch, tmp_path)
    model = _bash_turn_model()
    bridge = _bridge()
    task = asyncio.create_task(_run(bridge, manager, record, model))
    record.task = task

    assert await asyncio.to_thread(waiting.wait, 3), "the turn never reached the capacity wait"

    stopped = time.monotonic()
    await manager.cancel(record.run_id)
    await asyncio.wait_for(task, timeout=2)
    elapsed = time.monotonic() - stopped

    assert elapsed < 1.0, f"Stop took {elapsed:.2f}s to end the wait"
    assert record.status is RunStatus.interrupted, (record.status, record.error, _published(bridge, "error"))
    assert record.stop_reason != SANDBOX_CAPACITY_STOP_REASON, "a late refusal did not replace the cancellation"
    assert _live(backend) == sorted(held), "no set was created after Stop"
    assert provider._starting == set(), "no reservation left behind"
    assert not getattr(provider, "_capacity_async_waiters", None), "no queued acquisition left behind"


def test_a_stop_that_lands_during_admission_evicts_no_parked_sandbox(tmp_path, monkeypatch):
    """A stopped acquisition never costs another thread its warm sandbox.

    Stop can arrive between the admission's first check and its eviction of
    the oldest parked sandbox; that thread would then pay a cold start for a
    slot nobody takes.
    """
    aio = _aio_mod()
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    for index in range(2):
        provider.release(provider.acquire(f"parked-{index}", user_id=OTHER_USER))
    parked = dict(provider._warm_pool)
    assert len(parked) == 2
    stopped = threading.Event()

    def _stop_arrives(_sandbox_id):
        stopped.set()
        return False

    monkeypatch.setattr(provider, "_try_reserve_slot_locked", _stop_arrives)
    token = aio._ACQUISITION_CANCELLED.set(stopped)
    try:
        with pytest.raises(aio._AcquisitionCancelledError):
            provider._admit_create("stopped-turn", allow_eviction=True)
    finally:
        aio._ACQUISITION_CANCELLED.reset(token)

    assert provider._warm_pool.keys() == parked.keys()
    assert backend.destroyed == []


def _admit_in_thread(provider, stopped: threading.Event, sandbox_id: str) -> tuple[threading.Thread, list[BaseException]]:
    """Run one waiting admission in a worker thread, as the accepted projection does."""
    aio = _aio_mod()
    raised: list[BaseException] = []

    def _admit() -> None:
        aio._ACQUISITION_CANCELLED.set(stopped)
        try:
            provider._admit_create(sandbox_id, allow_eviction=True)
        except BaseException as exc:
            raised.append(exc)

    thread = threading.Thread(target=_admit, daemon=True)
    thread.start()
    return thread, raised


def test_a_stop_that_lands_as_a_slot_frees_takes_no_slot(tmp_path, monkeypatch):
    """A slot freeing at the moment of Stop goes to someone who still wants it."""
    aio = _aio_mod()
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 3
    _saturate(provider, "starting")
    waiting = _waiting_signal(monkeypatch)
    stopped = threading.Event()
    thread, raised = _admit_in_thread(provider, stopped, "stopped-turn")
    assert waiting.wait(2)

    with provider._lock:
        provider._starting.discard("starting-a")  # a slot frees, nobody told yet
    stopped.set()
    provider._wake_stopped_acquisition()
    thread.join(2)

    assert [type(exc) for exc in raised] == [aio._AcquisitionCancelledError]
    assert "stopped-turn" not in provider._starting


def test_a_stop_whose_wake_comes_before_the_wait_is_not_slept_through(tmp_path, monkeypatch):
    """The wake can fire between deciding to wait and waiting; it must still count."""
    aio = _aio_mod()
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    provider._config["capacity_wait_timeout"] = 3
    _saturate(provider, "starting")
    stopped = threading.Event()

    def _stop_before_the_wait() -> None:
        stopped.set()
        provider._wake_stopped_acquisition()

    monkeypatch.setattr(aio, "record_capacity_wait", _stop_before_the_wait)
    started = time.monotonic()
    thread, raised = _admit_in_thread(provider, stopped, "stopped-turn")
    thread.join(5)
    elapsed = time.monotonic() - started

    assert [type(exc) for exc in raised] == [aio._AcquisitionCancelledError]
    assert elapsed < 1.0, f"the stopped admission slept {elapsed:.2f}s"
