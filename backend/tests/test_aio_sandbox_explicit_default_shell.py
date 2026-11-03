"""A command that HartMesh's per-call abort stopped leaves the container reusable.

HartMesh gives the default shell an explicit session id, so a session ended by
the abort is cleaned up by that id and the next command opens a fresh one.
Upstream fences a corrupted default shell for recycle because its implicit
shell has no id to clean up. Resident Work reads that fence after a run: with
it set, every stopped attempt settled "outcome-uncertain" and its workspace
was contained instead of the attempt being "cancelled".

Every other corruption keeps upstream's fence.
"""

from __future__ import annotations

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import pytest


@pytest.fixture()
def sandbox():
    with patch("deerflow.community.aio_sandbox.aio_sandbox.AioSandboxClient"):
        from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox

        instance = AioSandbox(id="explicit-default-shell", base_url="http://localhost:8080")
        instance._snapshot_existing_abort_processes = MagicMock(return_value=frozenset())
        instance._sweep_processes_carrying_the_abort_tokens = MagicMock()
        return instance


def _sessions(sandbox):
    created: list[str] = []
    sandbox._client.shell.create_session.side_effect = lambda id, **_kwargs: created.append(id) or SimpleNamespace(data=SimpleNamespace(session_id=id))
    return created


def _stop_a_running_command(sandbox) -> str:
    """Run ``sleep 300`` in the default shell and stop it with the per-call abort."""
    polling, release = threading.Event(), threading.Event()
    result: list[str] = []

    def wait(**_kwargs):
        polling.set()
        assert release.wait(timeout=3)

    sandbox._client.shell.exec_command.return_value = SimpleNamespace(data=SimpleNamespace(status="running"))
    sandbox._client.shell.wait_for_process.side_effect = wait
    worker = threading.Thread(target=lambda: result.append(sandbox.execute_command("sleep 300")))
    worker.start()
    try:
        assert polling.wait(timeout=3)
        assert sandbox.abort_running_commands() == 1
    finally:
        release.set()
        worker.join(timeout=5)
    assert not worker.is_alive() and len(result) == 1
    sandbox._client.shell.wait_for_process.side_effect = None
    return result[0]


def test_a_stopped_command_replaces_its_session_without_fencing_the_container(sandbox):
    created = _sessions(sandbox)

    output = _stop_a_running_command(sandbox)

    assert "cancelled" in output.lower() or "terminated" in output.lower()
    assert sandbox._default_shell_corrupted is False
    assert sandbox.requires_container_recycle is False
    assert sandbox.execution_outcome_uncertain is False
    assert created[0] in [call.args[0] for call in sandbox._client.shell.cleanup_session.call_args_list]

    sandbox._client.shell.exec_command.return_value = SimpleNamespace(data=SimpleNamespace(output="next-ok", exit_code=0, status="completed"))
    assert sandbox.execute_command("echo next") == "next-ok"
    assert sandbox._client.shell.exec_command.call_args.kwargs["id"] == created[1] != created[0]


def test_a_stopped_command_whose_session_cleanup_fails_still_fences(sandbox):
    _sessions(sandbox)
    sandbox._client.shell.cleanup_session.side_effect = RuntimeError("cleanup failed")

    _stop_a_running_command(sandbox)

    assert sandbox.requires_container_recycle is True
    assert sandbox.execution_outcome_uncertain is True


def test_a_session_the_server_ended_still_fences_as_upstream_does(sandbox):
    _sessions(sandbox)
    sandbox._client.shell.exec_command.return_value = SimpleNamespace(data=SimpleNamespace(output="partial", exit_code=None, status="terminated"))

    sandbox.execute_command("unsafe-to-repeat", timeout=3)

    assert sandbox.requires_container_recycle is True


def test_a_command_with_an_unknown_outcome_still_fences(sandbox):
    _sessions(sandbox)
    sandbox._client.shell.exec_command.side_effect = httpx.ReadTimeout("command response timed out")

    sandbox.execute_command("uncertain-command", timeout=3)

    assert sandbox.requires_container_recycle is True
    assert sandbox.execution_outcome_uncertain is True
