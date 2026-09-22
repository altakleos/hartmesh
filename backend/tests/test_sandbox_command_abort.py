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
import shlex
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from deerflow.sandbox.local.local_sandbox import LocalSandbox
from deerflow.sandbox.sandbox import ABORT_TOKEN_ENV

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


@posix_only
@linux_proc_only
def test_the_token_sweep_never_kills_the_gateway_itself(monkeypatch):
    """One line stands between the sweep and SIGKILLing the process it runs in.

    The Gateway's own environment can carry the variable -- it is inherited by
    anything the deployment was started from, and a test process is proof the
    shape is reachable -- so the sweep skips its own pid explicitly.
    """
    sandbox = LocalSandbox("t")
    token = "df-sweep-must-not-kill-its-own-process"
    monkeypatch.setitem(os.environ, ABORT_TOKEN_ENV, token)

    sandbox._kill_processes_carrying_the_abort_tokens({token})

    assert _alive(os.getpid()), "the sweep killed the process it was running in"


# ── The AIO sandbox, against a fake client ──────────────────────────────


class _FakeShell:
    """Records what the sandbox asks the container to do, and how it bounds it."""

    def __init__(self) -> None:
        self.sessions: list[str] = []
        self.cleaned: list[str] = []
        self.commands: list[tuple[str | None, str]] = []
        self.killed: list[str] = []
        #: The ``request_options`` of every call, in order. An abort request
        #: with no timeout can hang the cancelled call's drain for the client's
        #: whole 600 s command budget, so every one of them is recorded.
        self.request_options: list[dict | None] = []
        self.block = threading.Event()

    def create_session(self, *, id: str | None = None, request_options=None, **kwargs) -> None:  # noqa: A002 - the SDK's parameter name
        self.sessions.append(id)
        self.request_options.append(request_options)

    def cleanup_session(self, session_id: str, request_options=None, **kwargs) -> None:
        self.cleaned.append(session_id)
        self.request_options.append(request_options)

    def kill_process(self, *, id: str, request_options=None, **kwargs):  # noqa: A002 - the SDK's parameter name
        self.killed.append(id)
        self.request_options.append(request_options)
        self.block.set()
        return SimpleNamespace(data=None)

    def exec_command(self, *, command: str, id: str | None = None, request_options=None, **kwargs):  # noqa: A002
        self.commands.append((id, command))
        self.request_options.append(request_options)
        if id in self.sessions and not command.startswith("for d in /proc/"):
            self.block.wait(timeout=30)
        return SimpleNamespace(data=SimpleNamespace(output="", exit_code=0))


class _FakeBash:
    """The env-bearing path: `bash.exec` on a session the container creates."""

    def __init__(self, shell: _FakeShell) -> None:
        self._shell = shell
        self.calls: list[dict] = []

    def exec(self, *, command: str, env=None, **kwargs):
        self.calls.append({"command": command, "env": env or {}})
        self._shell.block.wait(timeout=30)
        return SimpleNamespace(data=SimpleNamespace(stdout="", stderr=None, exit_code=0))


def _aio_sandbox():
    """A real AioSandbox with a fake client: no HTTP, every code path its own."""
    from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox

    shell = _FakeShell()
    bash = _FakeBash(shell)
    sandbox = AioSandbox("aio-test", base_url="http://sandbox.invalid", home_dir="/home/gem")
    sandbox._client = SimpleNamespace(shell=shell, bash=bash)
    return sandbox, shell, bash


def _run_in_call(sandbox, command: str, call_id: str, *, env=None) -> threading.Thread:
    """Run one command in the context of one tool call, as the wrapper does."""
    from deerflow.sandbox.sandbox import SANDBOX_COMMAND_CALL

    def body() -> None:
        SANDBOX_COMMAND_CALL.set(call_id)
        sandbox.execute_command(command, env=env)

    thread = threading.Thread(target=body, daemon=True)
    thread.start()
    return thread


def _wait_for(predicate, *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("the container was never asked to run anything")


def test_aio_marks_every_command_with_a_token_unique_to_that_command():
    sandbox, shell, _ = _aio_sandbox()
    shell.block.set()

    sandbox.execute_command("echo one")
    sandbox.execute_command("echo two")

    first, second = (command for _, command in shell.commands)
    assert first.startswith(f"export {ABORT_TOKEN_ENV}=df-") and first.endswith("echo one")
    assert second.endswith("echo two")
    assert first.split(";")[0] != second.split(";")[0], "one token for two commands cannot tell them apart"


def test_aio_abort_kills_the_session_process_and_sweeps_its_descendants():
    sandbox, shell, _ = _aio_sandbox()
    sandbox._default_shell_corrupted = True  # forces an explicit session, as a live image does after one ErrorObservation
    worker = _run_in_call(sandbox, "sleep 300", "call-1")
    _wait_for(lambda: bool(shell.commands))

    stopped = sandbox.abort_running_commands("call-1")

    assert stopped == 1
    assert shell.killed, "the session's foreground process must be killed"
    sweep = next((command for _, command in shell.commands if command.startswith("for d in /proc/")), None)
    assert sweep is not None, "descendants are found by the environment they inherited"
    assert "kill -9" in sweep
    marked = next(command for _, command in shell.commands if command.endswith("sleep 300"))
    assert marked.split(";")[0].removeprefix(f"export {ABORT_TOKEN_ENV}=") in sweep, "the sweep must look for this command's own token"

    worker.join(timeout=15)
    assert not worker.is_alive()


def test_aio_abort_reaches_a_command_carrying_injected_secrets():
    """Every skill that declares a required secret runs through `bash.exec`.

    That path creates its own session and never appears in `shell.exec_command`,
    so a sandbox that only counted the shell path reported nothing to abort and
    left the command running with its credentials for its whole 600 s budget --
    on the provider a tenant actually runs.
    """
    sandbox, shell, bash = _aio_sandbox()
    worker = _run_in_call(sandbox, "gh pr create", "call-1", env={"GH_TOKEN": "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY"})
    _wait_for(lambda: bool(bash.calls))

    stopped = sandbox.abort_running_commands("call-1")

    assert stopped == 1, "an env-bearing command must be abortable"
    sweep = next((command for _, command in shell.commands if command.startswith("for d in /proc/")), None)
    assert sweep is not None
    assert bash.calls[0]["env"][ABORT_TOKEN_ENV] in sweep, "the sweep must look for the token that command carried"

    shell.block.set()
    worker.join(timeout=15)


def test_aio_abort_leaves_another_call_s_command_running():
    """One sandbox serves the lead agent and its subagents; a subagent's own
    timeout must not kill the lead's command."""
    sandbox, shell, _ = _aio_sandbox()
    sandbox._default_shell_corrupted = True
    other = _run_in_call(sandbox, "sleep 300", "call-other")
    _wait_for(lambda: bool(shell.commands))

    assert sandbox.abort_running_commands("call-mine") == 0
    assert shell.killed == [] and not any(command.startswith("for d in /proc/") for _, command in shell.commands)

    shell.block.set()
    other.join(timeout=15)


def test_aio_abort_with_no_call_ends_every_command_in_the_sandbox():
    sandbox, shell, _ = _aio_sandbox()
    sandbox._default_shell_corrupted = True
    worker = _run_in_call(sandbox, "sleep 300", "call-1")
    _wait_for(lambda: bool(shell.commands))

    assert sandbox.abort_running_commands() == 1

    shell.block.set()
    worker.join(timeout=15)


def test_aio_abort_with_nothing_running_does_not_touch_the_container():
    sandbox, shell, _ = _aio_sandbox()
    assert sandbox.abort_running_commands("call-1") == 0
    assert shell.commands == [] and shell.killed == [] and shell.sessions == []


def test_aio_abort_never_waits_on_the_command_lock():
    """The abort runs while a command holds the serialization lock; it must not queue behind it."""
    sandbox, shell, _ = _aio_sandbox()
    sandbox._default_shell_corrupted = True
    worker = _run_in_call(sandbox, "sleep 300", "call-1")
    _wait_for(lambda: bool(shell.commands))

    started = time.monotonic()
    sandbox.abort_running_commands("call-1")
    assert time.monotonic() - started < 5, "the abort waited for the command it was meant to stop"

    worker.join(timeout=15)


def test_aio_abort_bounds_every_request_it_makes():
    """An abort that cannot be delivered must give up, not hang the drain behind it.

    All four calls count: `create_session` and `cleanup_session` inherit the
    client's 600 s command timeout unless the abort overrides it, and the drain
    that follows deliberately cannot be interrupted.
    """
    sandbox, shell, _ = _aio_sandbox()
    sandbox._default_shell_corrupted = True
    worker = _run_in_call(sandbox, "sleep 300", "call-1")
    _wait_for(lambda: bool(shell.commands))
    shell.request_options.clear()

    sandbox.abort_running_commands("call-1")

    assert len(shell.request_options) == 4, shell.request_options
    assert all(options and options.get("timeout_in_seconds") for options in shell.request_options), shell.request_options

    worker.join(timeout=15)


def test_aio_does_not_re_run_a_command_its_own_abort_killed():
    """A killed shell reports corruption; rotating would restart the work just ended."""
    sandbox, shell, _ = _aio_sandbox()
    sandbox._default_shell_corrupted = True
    shell.block.set()
    from deerflow.community.aio_sandbox.aio_sandbox import _ERROR_OBSERVATION_SIGNATURE
    from deerflow.sandbox.sandbox import SANDBOX_COMMAND_CALL

    def corrupted(*, command, id=None, request_options=None, **kwargs):  # noqa: A002
        shell.commands.append((id, command))
        return SimpleNamespace(data=SimpleNamespace(output=_ERROR_OBSERVATION_SIGNATURE, exit_code=None))

    shell.exec_command = corrupted
    SANDBOX_COMMAND_CALL.set("call-1")
    sandbox._aborted_calls["call-1"] = None

    sandbox.execute_command("rm -rf /mnt/user-data/outputs/report")

    assert len([command for _, command in shell.commands if "report" in command]) == 1, "the killed command was run again"


# ── The tool wrapper: a cancelled tool call aborts the command ──────────


class _AbortRecordingSandbox:
    def __init__(self) -> None:
        self.released = threading.Event()
        self.aborted: list[str | None] = []

    def abort_running_commands(self, call_id: str | None = None) -> int:
        self.aborted.append(call_id)
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

    assert sandbox.aborted, "cancellation must reach the sandbox command"
    # Scoped to this call, never to the sandbox: the lead agent and its
    # subagents share one, and a subagent's own timeout arrives here too.
    assert sandbox.aborted[0] is not None


@pytest.mark.anyio
async def test_an_uncancelled_sandbox_tool_call_never_aborts_anything():
    from deerflow.sandbox.lease import run_sync_sandbox_command

    sandbox = _AbortRecordingSandbox()
    sandbox.released.set()

    assert await run_sync_sandbox_command(sandbox, sandbox.run_long_command) == "done"
    assert sandbox.aborted == []


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


@pytest.mark.skipif(shutil.which("bash") is None or not Path("/proc/self/environ").exists(), reason="the sweep is a bash loop over /proc")
def test_the_aio_sweep_kills_the_marked_process_and_leaves_its_shell_running():
    """The sweep runs as one command in a persistent shell session.

    It once ended with ``exit 0``, which closed that shell: the API never saw
    the command finish and every abort waited out its own 20 s request
    timeout, measured against the released image under gVisor, holding the
    cancelled tool call that long after its command was already dead. Here the
    sweep runs in a shell reading commands the way a session does, and the
    line after it has to run.
    """
    from deerflow.community.aio_sandbox.aio_sandbox import _ABORT_SWEEP

    token = f"df-{uuid.uuid4().hex}"
    marked = subprocess.Popen(["sleep", "60"], env={**os.environ, ABORT_TOKEN_ENV: token})
    unmarked = subprocess.Popen(["sleep", "60"])
    try:
        command = _ABORT_SWEEP.format(marker=shlex.quote(f"{ABORT_TOKEN_ENV}={token}"))
        shell = subprocess.run(["bash"], input=f"{command}\necho the-session-is-still-here\n", capture_output=True, text=True, timeout=30)

        assert "the-session-is-still-here" in shell.stdout, shell
        # Nothing but the answer: an environ the sandbox user cannot read (a
        # root process) is skipped quietly rather than printed per process
        # into the result the abort waits for.
        assert shell.stderr == "", shell.stderr
        assert marked.wait(timeout=10) == -9
        assert unmarked.poll() is None
    finally:
        for process in (marked, unmarked):
            if process.poll() is None:
                process.kill()
                process.wait()
