"""Offline operator evidence distinguishes storage from resident readiness."""

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from app.gateway import storage_spaces as ss
from deerflow.spaces.provision import KINDS


def test_bootstrap_diagnostics_bound_chains_and_ignore_values():
    failures = [RuntimeError("private-value" * 1000) for _ in range(8)]
    for current, cause in zip(failures, failures[1:]):
        current.__cause__ = cause
    failures[-1].__cause__ = failures[0]
    result = ss._bootstrap_error_locations(failures[0])
    assert result.count("RuntimeError") == 3
    assert "private-value" not in result and len(result) < 2000
    failures[0].__cause__ = failures[0]
    assert ss._bootstrap_error_locations(failures[0]) == "RuntimeError@unknown"


@pytest.mark.asyncio
async def test_failed_bootstrap_logs_sources_and_state_without_exception_values(caplog):
    secret = "private-tenant-content-and-credentials"
    try:
        try:
            raise ValueError(secret)
        except ValueError as cause:
            raise RuntimeError(secret) from cause
    except RuntimeError as error:
        failure = error
    coordinator = SimpleNamespace(task=None, result=None, last_error=failure, poll=AsyncMock(return_value=None))
    runtime = SimpleNamespace(accounting_retry=False, coordinator=coordinator, workspace_qualified=False)
    app = SimpleNamespace(state=SimpleNamespace(resident_runtime=runtime, agent_execution=SimpleNamespace(pending_reconcile=[])))
    caplog.set_level(logging.WARNING, logger=ss.__name__)
    assert await ss._resident_step(app, timeout=0) is False
    assert "bootstrap unavailable" in caplog.text
    assert "RuntimeError" in caplog.text and "ValueError" in caplog.text and "test_resident_startup_diagnostics.py" in caplog.text
    assert "workspace_qualified=False" in caplog.text and "pending=0" in caplog.text
    assert secret not in caplog.text and len(caplog.text) < 2000


@pytest.mark.asyncio
async def test_unfinished_bootstrap_does_not_log_a_failure(caplog):
    coordinator = SimpleNamespace(task=object(), result=None, last_error=None, poll=AsyncMock(return_value=None))
    runtime = SimpleNamespace(accounting_retry=False, coordinator=coordinator, workspace_qualified=False)
    app = SimpleNamespace(state=SimpleNamespace(resident_runtime=runtime))
    assert await ss._resident_step(app, timeout=0) is False
    assert "bootstrap unavailable" not in caplog.text


@pytest.mark.asyncio
async def test_failed_accounting_retry_is_logged_before_invalidation(caplog):
    coordinator = SimpleNamespace(task=object(), result=None, last_error=None, invalidate=Mock())
    runtime = SimpleNamespace(accounting_retry=False, coordinator=coordinator, workspace_qualified=False)

    async def poll(*, timeout):
        coordinator.task = None
        coordinator.last_error = RuntimeError("private-value")
        runtime.accounting_retry = True

    coordinator.poll = AsyncMock(side_effect=poll)
    app = SimpleNamespace(state=SimpleNamespace(resident_runtime=runtime, agent_execution=SimpleNamespace(pending_reconcile=[])))
    caplog.set_level(logging.WARNING, logger=ss.__name__)
    assert await ss._resident_step(app, timeout=0) is False
    assert "bootstrap unavailable" in caplog.text and "RuntimeError" in caplog.text
    assert "private-value" not in caplog.text
    coordinator.invalidate.assert_called_once()


@pytest.mark.asyncio
async def test_unfinished_retry_does_not_report_the_previous_failure(caplog):
    coordinator = SimpleNamespace(task=object(), result=None, last_error=RuntimeError("previous-failure"), poll=AsyncMock(return_value=None))
    runtime = SimpleNamespace(accounting_retry=False, coordinator=coordinator, workspace_qualified=False)
    app = SimpleNamespace(state=SimpleNamespace(resident_runtime=runtime, agent_execution=SimpleNamespace(pending_reconcile=[])))
    caplog.set_level(logging.WARNING, logger=ss.__name__)
    assert await ss._resident_step(app, timeout=0) is False
    assert "bootstrap unavailable" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("execution, reason", [("unavailable", "storage-unavailable"), ("ready", None)])
async def test_startup_agent_line_follows_execution_readiness(monkeypatch, caplog, execution, reason):
    status = SimpleNamespace(unverified={}, fit=None, file_operations_settled=0, attachments_fenced=0, attempts_settled=0)
    app = SimpleNamespace(state=SimpleNamespace(agent_execution=status, storage_spaces=SimpleNamespace(catalog=SimpleNamespace(volumes={}))))
    monkeypatch.setattr(ss, "storage_state", AsyncMock(return_value=SimpleNamespace(state="ready")))
    monkeypatch.setattr(ss, "get_agents_api_config", lambda: SimpleNamespace(ai_assistants_per_user=1, ai_employees=1))
    monkeypatch.setattr(ss, "execution_state", AsyncMock(return_value=SimpleNamespace(execution=execution, reason=reason)))
    counts = {kind: SimpleNamespace(in_use=0, allowed=1, allowed_key=1, prepared=1) for kind in KINDS}
    caplog.set_level(logging.INFO, logger=ss.__name__)
    await ss._startup_lines(app, counts=counts)
    if execution == "unavailable":
        assert "agents: unavailable (storage-unavailable)" in caplog.text and "agents: ready" not in caplog.text
    else:
        assert "agents: ready" in caplog.text and "agents: unavailable" not in caplog.text
