"""HartMesh keeps upstream's run-scoped command cancellation off.

Upstream #7772 kills every process a run's owner started, including jobs it
backgrounded earlier, when the run is cancelled, fails or hits a hard stop.
HartMesh stops commands per tool call instead (``abort_running_commands``,
decision e of the upstream sync plan). With ``supports_command_cancellation``
False no command scope binds, so the worker's
``cancel_sandbox_execution_lease_async`` reaches ``cancel_command_scope`` as a
no-op and a job the person backgrounded keeps running.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from deerflow.sandbox.command_scope import command_owner, current_command_scope
from deerflow.sandbox.local.local_sandbox import LocalSandbox


@pytest.fixture()
def aio():
    with patch("deerflow.community.aio_sandbox.aio_sandbox.AioSandboxClient"):
        from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox

        return AioSandbox(id="run-cancel-off", base_url="http://localhost:8080")


def test_local_and_aio_sandboxes_ship_with_run_scoped_cancellation_off(aio):
    assert LocalSandbox.supports_command_cancellation is False
    assert type(aio).supports_command_cancellation is False


@pytest.mark.parametrize("kind", ["local", "aio"])
def test_no_command_scope_binds_and_the_run_cancel_hook_stops_nothing(aio, kind):
    sandbox = LocalSandbox("local:run-cancel-off") if kind == "local" else aio
    sandbox.abort_running_commands = MagicMock()

    with command_owner(sandbox, "run-owner"):
        assert current_command_scope(sandbox) is None

    sandbox.request_command_cancel("run-owner")
    sandbox.cancel_command_scope("run-owner")

    assert sandbox._command_scopes.find("run-owner") is None
    sandbox.abort_running_commands.assert_not_called()
