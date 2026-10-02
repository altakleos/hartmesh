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
import contextlib
import os
import selectors
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


def _is_sweep(command: str) -> bool:
    return "for d in /proc/[0-9]*" in command


class _FakeShell:
    """Records what the sandbox asks the container to do, and how it bounds it."""

    def __init__(self) -> None:
        self.sessions: list[str] = []
        self.cleaned: list[str] = []
        self.commands: list[tuple[str | None, str]] = []
        self.killed: list[str] = []
        self.events: list[tuple[str, str]] = []
        #: The ``request_options`` of every call, in order. An abort request
        #: with no timeout can hang the cancelled call's drain for the client's
        #: whole 600 s command budget, so every one of them is recorded.
        self.request_options: list[dict | None] = []
        self.block = threading.Event()
        #: The id the image's implicit session answers with.
        self.implicit_session_id = "implicit-1"

    def create_session(self, *, id: str | None = None, request_options=None, **kwargs) -> None:  # noqa: A002 - the SDK's parameter name
        self.sessions.append(id)
        self.request_options.append(request_options)

    def cleanup_session(self, session_id: str, request_options=None, **kwargs) -> None:
        self.cleaned.append(session_id)
        self.request_options.append(request_options)
        self.block.set()

    def update_session(self, *, id, request_options=None, **kwargs):  # noqa: A002
        self.request_options.append(request_options)

    def kill_process(self, *, id: str, request_options=None, **kwargs):  # noqa: A002 - the SDK's parameter name
        self.killed.append(id)
        self.events.append(("kill", id))
        self.request_options.append(request_options)
        self.block.set()
        return SimpleNamespace(data=None)

    def exec_command(self, *, command: str, id: str | None = None, request_options=None, **kwargs):  # noqa: A002
        self.commands.append((id, command))
        self.events.append(("exec", command))
        self.request_options.append(request_options)
        if id in self.sessions and not _is_sweep(command) and "read -r p expected" not in command:
            self.block.wait(timeout=30)
        return SimpleNamespace(data=SimpleNamespace(output="", exit_code=0, session_id=id or self.implicit_session_id))


class _FakeBash:
    """The env-bearing path: `bash.exec` on a session the container creates."""

    def __init__(self, shell: _FakeShell) -> None:
        self._shell = shell
        self.calls: list[dict] = []
        self.sessions: list[str] = []

    def create_session(self, *, session_id: str, request_options=None, **kwargs) -> None:
        self.sessions.append(session_id)

    def close_session(self, session_id: str, **kwargs) -> None:
        pass

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
    sandbox._snapshot_existing_abort_processes = lambda *_a: frozenset()
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


class _ProcessShell:
    """A small shell transport using real Bash, including its kill semantics.

    An omitted id creates a new session, as the AIO API does. Killing only a
    foreground child lets Bash execute the rest of the submitted command.
    """

    def __init__(self, child_file: Path):
        self.processes = {}
        self.commands = []
        self.child_file = child_file

    def create_session(self, *, id=None, **kwargs):  # noqa: A002
        session_id = id or str(uuid.uuid4())
        self.processes[session_id] = subprocess.Popen(
            ["bash", "--noprofile", "--norc"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        return SimpleNamespace(data=SimpleNamespace(session_id=session_id))

    def exec_command(self, *, command, id=None, **kwargs):  # noqa: A002
        from agent_sandbox.core.api_error import ApiError

        if id is None:
            id = self.create_session().data.session_id  # noqa: A001
        self.commands.append((id, command))
        process = self.processes[id]
        delimiter = uuid.uuid4().hex
        try:
            process.stdin.write(f"{command}\nprintf '\\n{delimiter}\\n'\n".encode())
            process.stdin.flush()
            output = b""
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    if not selector.select(timeout=0.1):
                        continue
                    chunk = os.read(process.stdout.fileno(), 65536)
                    if not chunk:
                        break
                    output += chunk
                    if delimiter.encode() in output:
                        return SimpleNamespace(
                            data=SimpleNamespace(
                                output=output.decode().split(delimiter)[0],
                                exit_code=0,
                                session_id=id,
                                status="completed",
                            )
                        )
        except BrokenPipeError:
            pass
        raise ApiError(status_code=404, body={"message": "Shell session not found"})

    def update_session(self, **kwargs):
        pass

    def kill_process(self, *, id, **kwargs):  # noqa: A002
        # AIO kills the foreground process rather than the interactive shell.
        if self.processes[id].poll() is None and self.child_file.exists():
            with contextlib.suppress(ProcessLookupError):
                os.kill(int(self.child_file.read_text()), 9)

    def cleanup_session(self, session_id, **kwargs):
        process = self.processes[session_id]
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)


@linux_proc_only
@posix_only
def test_aio_stop_prevents_trailing_statements_preserves_old_jobs_and_recovers(
    tmp_path,
):
    """Exercise the production abort with actual shell processes, not kill mocks."""
    if shutil.which("bash") is None:
        pytest.skip("requires Bash")
    from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox

    child_file = tmp_path / "current.pid"
    old_file = tmp_path / "older.pid"
    trailing = tmp_path / "should-not-exist"
    shell = _ProcessShell(child_file)
    sandbox = AioSandbox("process-test", base_url="http://sandbox.invalid", home_dir="/home/gem")
    sandbox._client = SimpleNamespace(shell=shell)
    older_pid = None
    try:
        sandbox.execute_command(f"sleep 300 >/dev/null 2>&1 & echo $! > {old_file}; export REMEMBERED=ok; cd {tmp_path}")
        older_pid = int(_wait_for_file(old_file))
        assert "ok" in sandbox.execute_command('printf "%s %s" "$REMEMBERED" "$PWD"')
        assert sum(p.poll() is None for p in shell.processes.values()) == 1, "default calls must reuse one explicitly named shell"
        # Warm-pool release closes host sockets, while the next turn reconnects
        # to the same server-side shell rather than killing its older jobs.
        state = sandbox.detach_default_shell()
        sandbox.close()
        sandbox = AioSandbox("process-test", base_url="http://sandbox.invalid", home_dir="/home/gem")
        sandbox._client = SimpleNamespace(shell=shell)
        sandbox.restore_default_shell(state)
        assert "ok" in sandbox.execute_command('printf "%s %s" "$REMEMBERED" "$PWD"')
        assert sum(p.poll() is None for p in shell.processes.values()) == 1
        worker = _run_in_call(
            sandbox,
            f"sleep 300 & echo $! > {child_file}; wait $!; echo bad > {trailing}",
            "stop-call",
        )
        current_pid = int(_wait_for_file(child_file))
        before = time.monotonic()
        assert sandbox.abort_running_commands("stop-call") == 1
        worker.join(timeout=8)
        assert not worker.is_alive(), "the cancelled command did not drain"
        assert time.monotonic() - before < 8
        assert not trailing.exists(), "Bash continued executing after Stop"
        assert _alive(older_pid), "Stop killed a job from an earlier command"
        assert not _alive(current_pid) or Path(f"/proc/{current_pid}/stat").read_text().split(") ")[-1].startswith("Z")
        assert "after" in sandbox.execute_command("echo after")
        assert len([command for _, command in shell.commands if str(trailing) in command]) == 1, "404 recovery replayed the cancelled command"
    finally:
        for process in shell.processes.values():
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        if older_pid is not None:
            with contextlib.suppress(ProcessLookupError):
                os.kill(older_pid, 9)


def test_aio_marks_a_shell_session_once_and_sends_later_commands_as_written():
    """The shell is persistent: a prefix on every command would reset ``$?``
    and ``PIPESTATUS`` between calls. One export on the session's first command
    is inherited by everything the session starts afterwards."""
    sandbox, shell, _ = _aio_sandbox()
    shell.block.set()

    sandbox.execute_command("echo one")
    sandbox.execute_command("echo two")
    sandbox.execute_command("")

    first, second, third = (command for _, command in shell.commands)
    assert first.startswith(f"export {ABORT_TOKEN_ENV}=df-") and first.endswith("; echo one")
    assert second == "echo two"
    assert third == ""


def test_aio_gives_each_shell_session_its_own_token():
    """A subagent's session must not share a token with the lead's, or ending
    one's command would sweep the other's processes."""
    sandbox, shell, _ = _aio_sandbox()
    shell.block.set()

    sandbox.execute_command("echo lead")
    sandbox.execute_command_in_scope("echo sub", scope_id="subagent-a")

    lead, sub = (command for _, command in shell.commands)
    assert lead.startswith(f"export {ABORT_TOKEN_ENV}=df-") and sub.startswith(f"export {ABORT_TOKEN_ENV}=df-")
    assert lead.split(";")[0] != sub.split(";")[0]


def test_aio_reuses_the_explicit_default_session():
    """A response from a different session cannot silently redirect later calls."""
    sandbox, shell, _ = _aio_sandbox()
    shell.block.set()

    sandbox.execute_command("echo one")
    shell.implicit_session_id = "implicit-2"
    sandbox.execute_command("echo two")
    sandbox.execute_command("echo three")
    sandbox.execute_command("echo four")

    commands = [command for _, command in shell.commands]
    assert commands[1] == "echo two", "the change is only known once the new session has answered"
    assert commands[2] == "echo three"
    assert commands[3] == "echo four"
    assert len(shell.sessions) == 1
    assert all(session_id == shell.sessions[0] for session_id, _ in shell.commands)


def test_aio_abort_targets_the_explicit_default_session():
    """Cancellation knows the session id before even its first command returns."""
    sandbox, shell, _ = _aio_sandbox()
    shell.block.set()
    sandbox.execute_command("echo warm-up")
    shell.block.clear()
    worker = _run_in_call(sandbox, "sleep 300", "call-1")
    _wait_for(lambda: len(shell.commands) == 2)

    assert sandbox.abort_running_commands("call-1") == 1

    assert shell.sessions[0] in shell.cleaned
    sweep = next(command for _, command in shell.commands if _is_sweep(command))
    token = shell.commands[0][1].split(";")[0].removeprefix(f"export {ABORT_TOKEN_ENV}=")
    assert token in sweep, "the sweep must look for the token that session exports"
    worker.join(timeout=15)
    assert not worker.is_alive()


def test_aio_sweep_of_a_session_token_spares_what_older_commands_started():
    """A session's token is on everything it ever started, so the sweep is told
    how long ago the command began; a per-command token needs no such limit."""
    sandbox, shell, bash = _aio_sandbox()
    sandbox._default_shell_corrupted = True
    worker = _run_in_call(sandbox, "sleep 300", "call-1")
    _wait_for(lambda: bool(shell.commands))
    sandbox.abort_running_commands("call-1")
    worker.join(timeout=15)
    session_sweep = next(command for _, command in shell.commands if _is_sweep(command))
    assert "/proc/self/stat" not in session_sweep, "the shell sweep uses exact prior-job identities, not a rounded age"

    shell.commands.clear()
    shell.block.clear()
    worker = _run_in_call(sandbox, "gh pr create", "call-2", env={"GH_TOKEN": "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY"})
    _wait_for(lambda: bool(bash.calls))
    sandbox.abort_running_commands("call-2")
    shell.block.set()
    worker.join(timeout=15)
    command_sweep = next(command for _, command in shell.commands if _is_sweep(command))
    assert "/proc/self/stat" not in command_sweep


def test_aio_abort_kills_the_session_process_and_sweeps_its_descendants():
    sandbox, shell, _ = _aio_sandbox()
    sandbox._default_shell_corrupted = True  # forces an explicit session, as a live image does after one ErrorObservation
    worker = _run_in_call(sandbox, "sleep 300", "call-1")
    _wait_for(lambda: bool(shell.commands))

    stopped = sandbox.abort_running_commands("call-1")

    assert stopped == 1
    assert shell.sessions[0] in shell.cleaned, "the cancelled session must be discarded"
    parent_index = next(i for i, event in enumerate(shell.events) if event[0] == "exec" and "read -r p expected" in event[1])
    wake_index = next(i for i, event in enumerate(shell.events) if event[0] == "kill")
    sweep_index = next(i for i, event in enumerate(shell.events) if event[0] == "exec" and _is_sweep(event[1]))
    assert parent_index < wake_index < sweep_index, "the PTY must wake after stopping the parent and before sweeping its children"
    sweep = next((command for _, command in shell.commands if _is_sweep(command)), None)
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
    sweep = next((command for _, command in shell.commands if _is_sweep(command)), None)
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
    assert shell.killed == [] and not any(_is_sweep(command) for _, command in shell.commands)

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

    assert len(shell.request_options) == 7, shell.request_options
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

    assert len([command for _, command in shell.commands if "report" in command]) == 0, "an already cancelled call started a command"


# ── The tool wrapper: a cancelled tool call aborts the command ──────────


@linux_proc_only
@posix_only
def test_aio_shell_identity_guard_spares_a_reused_pid():
    if shutil.which("bash") is None:
        pytest.skip("requires Bash")
    from deerflow.community.aio_sandbox.aio_sandbox import _abort_shell_command, _shell_identity_path

    token = f"df-{uuid.uuid4().hex}"
    process = subprocess.Popen(["sleep", "60"])
    marker = Path(_shell_identity_path(token))
    try:
        ticks = int(Path(f"/proc/{process.pid}/stat").read_text().rsplit(") ", 1)[1].split()[19])
        marker.write_text(f"{process.pid} {ticks + 1}\n")
        subprocess.run(["bash", "-c", _abort_shell_command(token)], check=True, timeout=5)
        assert process.poll() is None, "a stale shell identity killed a different process"
        assert not marker.exists()
    finally:
        marker.unlink(missing_ok=True)
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)


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


sweep_runs_here = pytest.mark.skipif(
    shutil.which("bash") is None or not Path("/proc/self/environ").exists(),
    reason="the sweep is a shell loop over /proc",
)


@sweep_runs_here
@pytest.mark.parametrize("started_within_seconds", [None, 30])
def test_the_aio_sweep_kills_the_marked_process_and_leaves_its_shell_running(started_within_seconds):
    """The sweep runs as one command in a persistent shell session.

    It once ended with ``exit 0``, which closed that shell: the API never saw
    the command finish and every abort waited out its own 20 s request
    timeout, measured against the released image under gVisor, holding the
    cancelled tool call that long after its command was already dead. Here the
    sweep runs in a shell reading commands the way a session does, and the
    line after it has to run.
    """
    from deerflow.community.aio_sandbox.aio_sandbox import _abort_sweep_command

    token = f"df-{uuid.uuid4().hex}"
    marked = subprocess.Popen(["sleep", "60"], env={**os.environ, ABORT_TOKEN_ENV: token})
    unmarked = subprocess.Popen(["sleep", "60"])
    try:
        command = _abort_sweep_command(token, started_within_seconds=started_within_seconds)
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


@sweep_runs_here
@pytest.mark.parametrize("shell_name", ["sh", "bash"])
def test_the_aio_sweep_spares_a_marked_process_older_than_the_command(shell_name):
    """A server an earlier command left in the background carries the same
    session token as the command being ended, and has to survive it."""
    from deerflow.community.aio_sandbox.aio_sandbox import _abort_sweep_command

    token = f"df-{uuid.uuid4().hex}"
    env = {**os.environ, ABORT_TOKEN_ENV: token}
    # A name with a space and a parenthesis: the stat line is cut after the
    # last ")" so such a name cannot shift the field the age is read from.
    earlier = subprocess.Popen(["bash", "-c", "exec -a 'dev server) 1' sleep 60"], env=env)
    time.sleep(4.2)
    current = subprocess.Popen(["sleep", "60"], env=env)
    try:
        command = _abort_sweep_command(token, started_within_seconds=2)
        shell = subprocess.run([shell_name], input=f"{command}\necho the-session-is-still-here\n", capture_output=True, text=True, timeout=30)

        assert "the-session-is-still-here" in shell.stdout, shell
        assert shell.stderr == "", shell.stderr
        assert current.wait(timeout=10) == -9
        assert earlier.poll() is None, "a process older than the command was killed"
    finally:
        for process in (earlier, current):
            if process.poll() is None:
                process.kill()
                process.wait()
