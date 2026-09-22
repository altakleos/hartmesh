"""Cancelling a run must stop the command the sandbox is running, not only the graph.

Before this, ``run_sync_lifecycle_operation`` waited for the worker thread that
``asyncio.to_thread`` cannot interrupt, so a cancelled run's bash call ran to
its natural end: on a deployment an operator turned an account off while its
``sleep 541`` was in flight and the sandbox process, and the stream, ran on for
another 537 s. The sandbox now carries an abort hook, the tool wrapper calls it
on cancellation, and the drain that follows returns as soon as the command dies.

The local cases run real processes and assert real pids are gone; the AIO cases
drive a fake client, because the abort it issues is an HTTP call into a
container this suite has no way to start.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from deerflow.sandbox.local.local_sandbox import LocalSandbox

posix_only = pytest.mark.skipif(os.name == "nt", reason="POSIX process-group semantics")
linux_proc_only = pytest.mark.skipif(not Path("/proc/self/environ").exists(), reason="requires Linux /proc environ")


def _wait_for_file(path: Path, *, timeout: float = 10.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            text = path.read_text().strip()
            if text:
                return text
        time.sleep(0.05)
    raise AssertionError(f"{path} never appeared")


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_until_gone(pid: int, *, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.05)
    return False


def _run_in_thread(sandbox: LocalSandbox, command: str, *, timeout: float = 300) -> tuple[threading.Thread, list[str]]:
    output: list[str] = []
    thread = threading.Thread(
        target=lambda: output.append(sandbox.execute_command(command, timeout=timeout)),
        daemon=True,
    )
    thread.start()
    return thread, output


# ── The local sandbox, with real processes ──────────────────────────────


@posix_only
def test_abort_kills_the_running_command_and_its_background_child(tmp_path):
    """The foreground shell and a job it backgrounded both die, and the call returns."""
    shell_pid_file = tmp_path / "shell.pid"
    child_pid_file = tmp_path / "child.pid"
    sandbox = LocalSandbox("t")
    thread, _ = _run_in_thread(
        sandbox,
        f"echo $$ > {shell_pid_file}; sleep 300 & echo $! > {child_pid_file}; wait",
    )
    shell_pid = int(_wait_for_file(shell_pid_file))
    child_pid = int(_wait_for_file(child_pid_file))

    assert sandbox.abort_running_commands() == 1

    thread.join(timeout=15)
    assert not thread.is_alive(), "the command call must return once its process is killed"
    assert _wait_until_gone(shell_pid), "the command's shell is still running"
    assert _wait_until_gone(child_pid), "a job the command backgrounded is still running"


@posix_only
@linux_proc_only
def test_abort_kills_a_child_that_left_the_process_group(tmp_path):
    """``setsid`` escapes the killed process group; the environment token does not.

    A command that detaches its work is the case the operator cares about --
    the point of ending a removed person's run is that nothing of theirs keeps
    writing files or calling out afterwards.
    """
    detached_pid_file = tmp_path / "detached.pid"
    sandbox = LocalSandbox("t")
    thread, _ = _run_in_thread(
        sandbox,
        f"setsid sh -c 'echo $$ > {detached_pid_file}; sleep 300' & sleep 300",
    )
    detached_pid = int(_wait_for_file(detached_pid_file))

    sandbox.abort_running_commands()

    thread.join(timeout=15)
    assert not thread.is_alive()
    assert _wait_until_gone(detached_pid), "a detached child outlived the abort"


@posix_only
def test_abort_with_nothing_running_reports_nothing_and_is_harmless():
    sandbox = LocalSandbox("t")
    assert sandbox.abort_running_commands() == 0
    assert "ok" in sandbox.execute_command("echo ok", timeout=10)


@posix_only
def test_a_command_started_after_an_abort_runs_normally(tmp_path):
    """The abort ends what was in flight; it does not poison the sandbox."""
    pid_file = tmp_path / "shell.pid"
    sandbox = LocalSandbox("t")
    thread, _ = _run_in_thread(sandbox, f"echo $$ > {pid_file}; sleep 300")
    _wait_for_file(pid_file)
    sandbox.abort_running_commands()
    thread.join(timeout=15)

    assert "after" in sandbox.execute_command("echo after", timeout=10)


@posix_only
def test_abort_kills_every_command_in_flight_on_one_sandbox(tmp_path):
    """One sandbox serves a lead agent and its subagents; a cancel ends all of it."""
    first = tmp_path / "first.pid"
    second = tmp_path / "second.pid"
    sandbox = LocalSandbox("t")
    thread_one, _ = _run_in_thread(sandbox, f"echo $$ > {first}; sleep 300")
    thread_two, _ = _run_in_thread(sandbox, f"echo $$ > {second}; sleep 300")
    first_pid = int(_wait_for_file(first))
    second_pid = int(_wait_for_file(second))

    assert sandbox.abort_running_commands() == 2

    for thread in (thread_one, thread_two):
        thread.join(timeout=15)
        assert not thread.is_alive()
    assert _wait_until_gone(first_pid) and _wait_until_gone(second_pid)


@posix_only
def test_a_command_that_finishes_leaves_nothing_for_a_later_abort_to_kill():
    """The registry must not hold a finished command, or an abort would kill a stranger's pid."""
    sandbox = LocalSandbox("t")
    sandbox.execute_command("echo done", timeout=10)
    assert sandbox.abort_running_commands() == 0


# ── The AIO sandbox, against a fake client ──────────────────────────────


class _FakeShell:
    """Records what the sandbox asks the container to do."""

    def __init__(self) -> None:
        self.sessions: list[str] = []
        self.cleaned: list[str] = []
        self.commands: list[tuple[str | None, str]] = []
        self.killed: list[str] = []
        self.block = threading.Event()

    def create_session(self, *, id: str | None = None, **kwargs) -> None:  # noqa: A002 - the SDK's parameter name
        self.sessions.append(id)

    def cleanup_session(self, session_id: str, **kwargs) -> None:
        self.cleaned.append(session_id)

    def kill_process(self, *, id: str, **kwargs):  # noqa: A002 - the SDK's parameter name
        self.killed.append(id)
        self.block.set()
        return SimpleNamespace(data=None)

    def exec_command(self, *, command: str, id: str | None = None, **kwargs):  # noqa: A002
        self.commands.append((id, command))
        if id in self.sessions and not command.startswith("for d in /proc/"):
            self.block.wait(timeout=30)
        return SimpleNamespace(data=SimpleNamespace(output="", exit_code=0))


def _aio_sandbox():
    """A real AioSandbox with a fake client: no HTTP, every code path its own."""
    from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox

    shell = _FakeShell()
    sandbox = AioSandbox("aio-test", base_url="http://sandbox.invalid", home_dir="/home/gem")
    sandbox._client = SimpleNamespace(shell=shell, bash=None)
    sandbox._abort_token = "abort-token-for-test"
    return sandbox, shell


def _wait_for_command(shell: _FakeShell) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if shell.commands:
            return
        time.sleep(0.01)
    raise AssertionError("no command ever reached the container")


def test_aio_marks_every_command_with_the_sandbox_abort_token():
    sandbox, shell = _aio_sandbox()
    shell.block.set()

    sandbox.execute_command("echo hello")

    _, sent = shell.commands[0]
    assert "abort-token-for-test" in sent, "a command the abort cannot recognize cannot be stopped"
    assert sent.rstrip().endswith("echo hello")


def test_aio_abort_kills_the_session_process_and_sweeps_its_descendants():
    sandbox, shell = _aio_sandbox()
    sandbox._default_shell_corrupted = True  # forces an explicit session, as a live image does after one ErrorObservation
    worker = threading.Thread(target=lambda: sandbox.execute_command("sleep 300"), daemon=True)
    worker.start()
    _wait_for_command(shell)

    stopped = sandbox.abort_running_commands()

    assert stopped == 1
    assert shell.killed, "the session's foreground process must be killed"
    sweep = next((command for _, command in shell.commands if command.startswith("for d in /proc/")), None)
    assert sweep is not None, "descendants are found by the environment they inherited"
    assert "abort-token-for-test" in sweep and "kill -9" in sweep

    worker.join(timeout=15)
    assert not worker.is_alive()


def test_aio_abort_retires_the_shell_session_it_killed():
    """The abort kills the session's shell too, so the next command needs a new one."""
    sandbox, shell = _aio_sandbox()
    shell.block.set()
    sandbox._default_shell_corrupted = True
    sandbox.execute_command("echo one")
    first_session = sandbox._recovery_session_id
    assert first_session is not None

    sandbox._inflight_commands = 1  # the abort only acts while something is running
    sandbox.abort_running_commands()
    sandbox._inflight_commands = 0
    sandbox.execute_command("echo two")

    assert sandbox._recovery_session_id != first_session, "a killed shell session must not be reused"


def test_aio_abort_with_nothing_running_does_not_touch_the_container():
    sandbox, shell = _aio_sandbox()
    assert sandbox.abort_running_commands() == 0
    assert shell.commands == [] and shell.killed == []


def test_aio_abort_never_waits_on_the_command_lock():
    """The abort runs while a command holds the serialization lock; it must not queue behind it."""
    sandbox, shell = _aio_sandbox()
    sandbox._default_shell_corrupted = True
    worker = threading.Thread(target=lambda: sandbox.execute_command("sleep 300"), daemon=True)
    worker.start()
    _wait_for_command(shell)

    started = time.monotonic()
    sandbox.abort_running_commands()
    assert time.monotonic() - started < 5, "the abort waited for the command it was meant to stop"

    worker.join(timeout=15)


def test_aio_abort_bounds_every_request_it_makes():
    """An abort that cannot be delivered must give up, not hang the drain behind it."""
    sandbox, shell = _aio_sandbox()
    seen: list[dict] = []
    shell.kill_process = lambda *, id, **kwargs: seen.append(kwargs.get("request_options") or {})  # noqa: A002
    original_exec = shell.exec_command

    def recording_exec(*, command, id=None, **kwargs):  # noqa: A002
        seen.append(kwargs.get("request_options") or {})
        return original_exec(command=command, id=id, **kwargs)

    shell.exec_command = recording_exec
    sandbox._inflight_commands = 1
    sandbox._inflight_sessions = {"session-1": 1}

    sandbox.abort_running_commands()

    assert seen, "the abort made no request"
    assert all(options.get("timeout_in_seconds") for options in seen), "an abort request with no timeout can hang forever"


# ── The tool wrapper: a cancelled tool call aborts the command ──────────


class _AbortRecordingSandbox:
    def __init__(self) -> None:
        self.released = threading.Event()
        self.aborted = threading.Event()

    def abort_running_commands(self) -> int:
        self.aborted.set()
        self.released.set()
        return 1

    def run_long_command(self) -> str:
        self.released.wait(timeout=30)
        return "done"


@pytest.mark.anyio
async def test_cancelling_a_sandbox_tool_call_aborts_the_command():
    from deerflow.sandbox.lease import run_sync_sandbox_command

    sandbox = _AbortRecordingSandbox()
    task = asyncio.create_task(run_sync_sandbox_command(sandbox, sandbox.run_long_command))
    await asyncio.sleep(0.05)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=15)

    assert sandbox.aborted.is_set(), "cancellation must reach the sandbox command"


@pytest.mark.anyio
async def test_an_uncancelled_sandbox_tool_call_never_aborts_anything():
    from deerflow.sandbox.lease import run_sync_sandbox_command

    sandbox = _AbortRecordingSandbox()
    sandbox.released.set()

    assert await run_sync_sandbox_command(sandbox, sandbox.run_long_command) == "done"
    assert not sandbox.aborted.is_set()


@pytest.mark.anyio
async def test_a_sandbox_without_an_abort_hook_still_cancels_the_old_way():
    """Custom providers are loaded by class path; the hook is additive, never required."""
    from deerflow.sandbox.lease import run_sync_sandbox_command

    released = threading.Event()

    class _PlainSandbox:
        def work(self) -> str:
            released.wait(timeout=30)
            return "done"

    sandbox = _PlainSandbox()
    task = asyncio.create_task(run_sync_sandbox_command(sandbox, sandbox.work))
    await asyncio.sleep(0.05)
    task.cancel()
    released.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=15)
