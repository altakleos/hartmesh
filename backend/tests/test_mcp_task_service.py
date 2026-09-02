import asyncio
import base64
import json
import logging
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from deerflow_extension_api import EffectiveSubjectV1, InvocationIdentityV1

import app.mcp_tasks.service as service_module
from app.mcp_tasks.errors import PermanentNotificationError
from app.mcp_tasks.service import McpTaskService
from deerflow.mcp.tasks import (
    McpTaskDriverRegistry,
    McpTaskLineageBinder,
    McpTaskLineageError,
    TaskSnapshot,
    TaskStatus,
    TaskSubmission,
    TaskSubmitRequest,
)
from deerflow.mcp.tasks.ordinary import McpTaskProtocolError
from deerflow.persistence.mcp_tasks import (
    DuplicateMcpRemoteTaskError,
    DuplicateMcpTaskIdError,
    DuplicateMcpTaskLineageError,
)
from deerflow.runtime.runs.manager import ConflictError
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.tenant_identity import TenantIdentityV1
from deerflow.runtime.tool_evidence import build_request_projection

_TENANT = TenantIdentityV1.from_canonical_id("test").to_persisted_reference()


@pytest.fixture(autouse=True)
def _mcp_request_commitment_keyring(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "MCP_TASK_REPLAY_HMAC_KEYS",
        '{"test-v1":"a2tra2tra2tra2tra2tra2tra2tra2tra2tra2tra2s"}',
    )
    monkeypatch.setenv("MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID", "test-v1")


def _lineage(*, user_id: str = "user-1", arguments=None):
    arguments = dict(arguments or {})
    return McpTaskLineageBinder().for_standalone_api(
        tenant=_TENANT,
        principal_identity=InvocationIdentityV1(
            effective_subject=EffectiveSubjectV1(
                kind="human",
                subject_id=user_id,
                role="member",
            )
        ),
        extension_generation=1,
        extension_manifest_digest="a" * 64,
        accepted_origin_digest="b" * 64,
        server_name="reports",
        tool_name="submit",
        safe_request_projection=build_request_projection("submit", arguments),
        credential_selector=None,
    )


def _request(
    *,
    user_id: str = "user-1",
    thread_id: str = "thread-1",
    arguments=None,
    driver_data=None,
    local_task_id: str | None = None,
) -> TaskSubmitRequest:
    arguments = dict(arguments or {})
    return TaskSubmitRequest(
        user_id=user_id,
        thread_id=thread_id,
        lineage=_lineage(user_id=user_id, arguments=arguments),
        task_name="Generate report",
        arguments=arguments,
        driver_data=dict(driver_data or {}),
        local_task_id=local_task_id,
    )


def _repo(**values):
    return SimpleNamespace(tenant=_TENANT, **values)


class FakeRepository:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.claimed = False
        self.applied = []
        self.released = []
        self.created = []
        self.tenant = _TENANT

    async def get_by_lineage_digest(self, *_args, **_kwargs):
        return None

    async def get(self, *_args, **_kwargs):
        return None

    async def create(self, **kwargs):
        self.created.append(kwargs)
        return {
            "id": kwargs["task_id"],
            **kwargs,
            "server_name": kwargs["lineage"].mcp_server_name,
            "lineage": kwargs["lineage"].to_persisted_json(),
        }

    async def claim_due_tasks(self, **_kwargs):
        if self.claimed:
            return []
        self.claimed = True
        return [dict(row) for row in self.rows]

    async def apply_snapshot(self, task_id, **kwargs):
        self.applied.append((task_id, kwargs))
        return True

    async def release_claim(self, task_id, **kwargs):
        self.released.append((task_id, kwargs))
        return True


class StatefulSubmissionRepository(FakeRepository):
    """Minimal public repository seam for idempotent submission tests."""

    def __init__(self):
        super().__init__()
        self.persisted = None

    async def get_by_lineage_digest(self, lineage_digest, **_kwargs):
        if self.persisted is None:
            return None
        lineage = self.persisted.get("lineage")
        return self.persisted if isinstance(lineage, dict) and lineage.get("digest") == lineage_digest else None

    async def get(self, task_id, **_kwargs):
        if self.persisted is None or self.persisted.get("id") != task_id:
            return None
        return self.persisted

    async def create(self, **kwargs):
        self.persisted = await super().create(**kwargs)
        return self.persisted


class FailingApplyRepository(FakeRepository):
    async def apply_snapshot(self, task_id, **kwargs):
        if task_id == "task-1":
            raise RuntimeError("database unavailable")
        return await super().apply_snapshot(task_id, **kwargs)


class FailingCreateRepository(FakeRepository):
    async def create(self, **kwargs):
        self.created.append(kwargs)
        raise RuntimeError("database unavailable")


class BlockingCreateRepository(FakeRepository):
    def __init__(self):
        super().__init__()
        self.create_started = asyncio.Event()

    async def create(self, **kwargs):
        self.created.append(kwargs)
        self.create_started.set()
        await asyncio.Event().wait()


class DuplicateCreateRepository(FakeRepository):
    async def create(self, **kwargs):
        self.created.append(kwargs)
        raise DuplicateMcpRemoteTaskError("already tracked")


class RacyLineageRepository(FakeRepository):
    def __init__(self):
        super().__init__()
        self.persisted = None
        self.duplicate_observed = False

    async def get_by_lineage_digest(self, *_args, **_kwargs):
        return self.persisted if self.duplicate_observed else None

    async def create(self, **kwargs):
        self.created.append(kwargs)
        if self.persisted is None:
            self.persisted = {
                "id": kwargs["task_id"],
                **kwargs,
                "server_name": kwargs["lineage"].mcp_server_name,
                "lineage": kwargs["lineage"].to_persisted_json(),
            }
            return self.persisted
        self.duplicate_observed = True
        raise DuplicateMcpTaskLineageError("already tracked")


class RacyTaskIdRepository(RacyLineageRepository):
    async def create(self, **kwargs):
        self.created.append(kwargs)
        if self.persisted is None:
            self.persisted = {
                "id": kwargs["task_id"],
                **kwargs,
                "server_name": kwargs["lineage"].mcp_server_name,
                "lineage": kwargs["lineage"].to_persisted_json(),
            }
            return self.persisted
        self.duplicate_observed = True
        raise DuplicateMcpTaskIdError("already tracked")


class FakeDriver:
    def __init__(
        self,
        snapshots=None,
        *,
        submission=None,
        error: Exception | None = None,
        cancel_error: Exception | None = None,
    ):
        self.snapshots = list(snapshots or [])
        self.submission = submission
        self.error = error
        self.cancel_error = cancel_error
        self.status_calls = []
        self.submit_calls = []
        self.cancel_calls = []

    async def submit(self, request):
        self.submit_calls.append(request)
        if self.submission is None:
            raise AssertionError(f"unexpected submit: {request}")
        return self.submission

    async def get_status(self, task):
        self.status_calls.append(task)
        if self.error is not None:
            raise self.error
        return self.snapshots.pop(0)

    async def cancel(self, task):
        self.cancel_calls.append(task)
        if self.cancel_error is not None:
            raise self.cancel_error
        return TaskSnapshot(status=TaskStatus.CANCELLED)


class HangingDriver(FakeDriver):
    def __init__(self):
        super().__init__()
        self.started = asyncio.Event()
        self.cancelled = False

    async def get_status(self, task):
        self.status_calls.append(task)
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


class BlockingCancelDriver(FakeDriver):
    def __init__(self, *, submission):
        super().__init__(submission=submission)
        self.cancel_started = asyncio.Event()
        self.finish_cancel = asyncio.Event()
        self.cancel_finished = asyncio.Event()
        self.cancel_completed = False
        self.cancel_interrupted = False

    async def cancel(self, task):
        self.cancel_calls.append(task)
        self.cancel_started.set()
        try:
            await self.finish_cancel.wait()
        except asyncio.CancelledError:
            self.cancel_interrupted = True
            self.cancel_finished.set()
            raise
        self.cancel_completed = True
        self.cancel_finished.set()
        return TaskSnapshot(status=TaskStatus.CANCELLED)


def _claimed_row(*, driver_name="fake"):
    return {
        "id": "task-1",
        "user_id": "user-1",
        "thread_id": "thread-1",
        "run_id": "run-1",
        "tool_call_id": "call-1",
        "server_name": "reports",
        "driver_name": driver_name,
        "remote_task_id": "remote-1",
        "task_name": "Generate report",
        "status": "working",
        "driver_data": {"status_tool": "status"},
        "lease_owner": "ignored-by-service-fixture",
    }


@pytest.mark.asyncio
async def test_submit_persists_remote_handle_before_returning():
    now = datetime.now(UTC)
    repo = FakeRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED, poll_after_seconds=9),
            driver_data={"status_tool": "status"},
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    request = _request(
        arguments={"topic": "MCP"},
        driver_data={"submit_tool": "submit"},
    )

    created = await service.submit(driver_name="fake", request=request, now=now)

    assert created["remote_task_id"] == "remote-1"
    persisted = repo.created[0]
    assert persisted["next_poll_after_seconds"] == 9
    assert persisted["driver_data"] == {"submit_tool": "submit", "status_tool": "status"}
    assert driver.submit_calls[0].local_task_id == created["id"]


@pytest.mark.asyncio
async def test_submit_cancels_remote_task_when_persistence_fails():
    repo = FailingCreateRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
            driver_data={"status_tool": "status", "cancel_tool": "cancel"},
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    request = _request(
        arguments={"topic": "MCP"},
        driver_data={"submit_tool": "submit"},
        local_task_id="task-1",
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        await service.submit(driver_name="fake", request=request)

    assert len(driver.cancel_calls) == 1
    cancelled = driver.cancel_calls[0]
    assert cancelled.local_task_id == "task-1"
    assert cancelled.remote_task_id == "remote-1"
    assert cancelled.driver_data == {
        "submit_tool": "submit",
        "status_tool": "status",
        "cancel_tool": "cancel",
    }


@pytest.mark.asyncio
async def test_submit_cancellation_during_persistence_cancels_remote_task():
    repo = BlockingCreateRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
            driver_data={"cancel_tool": "cancel"},
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    request = _request(
        arguments={"topic": "MCP"},
        local_task_id="task-1",
    )

    submit_task = asyncio.create_task(service.submit(driver_name="fake", request=request))
    await repo.create_started.wait()
    submit_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await submit_task

    assert len(driver.cancel_calls) == 1
    cancelled = driver.cancel_calls[0]
    assert cancelled.local_task_id == "task-1"
    assert cancelled.remote_task_id == "remote-1"


@pytest.mark.asyncio
async def test_submit_repeated_cancellation_does_not_interrupt_compensation():
    repo = BlockingCreateRepository()
    driver = BlockingCancelDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
            driver_data={"cancel_tool": "cancel"},
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    request = _request(
        arguments={"topic": "MCP"},
        local_task_id="task-1",
    )

    submit_task = asyncio.create_task(service.submit(driver_name="fake", request=request))
    await repo.create_started.wait()
    submit_task.cancel()
    await driver.cancel_started.wait()

    submit_task.cancel()
    driver.finish_cancel.set()

    with pytest.raises(asyncio.CancelledError):
        await submit_task

    assert len(driver.cancel_calls) == 1
    assert driver.cancel_completed
    assert not driver.cancel_interrupted


@pytest.mark.asyncio
async def test_submit_stops_waiting_for_hung_compensation_without_cancelling_it(monkeypatch, caplog):
    monkeypatch.setattr(service_module, "_UNTRACKED_TASK_COMPENSATION_WAIT_SECONDS", 0)
    repo = BlockingCreateRepository()
    driver = BlockingCancelDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
            driver_data={"cancel_tool": "cancel"},
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    request = _request(
        arguments={"topic": "MCP"},
        local_task_id="task-1",
    )

    submit_task = asyncio.create_task(service.submit(driver_name="fake", request=request))
    await repo.create_started.wait()
    submit_task.cancel()

    with caplog.at_level(logging.WARNING), pytest.raises(asyncio.CancelledError):
        await submit_task

    assert "cancellation continues in the background" in caplog.text
    await driver.cancel_started.wait()
    assert not driver.cancel_interrupted
    assert not driver.cancel_completed

    driver.finish_cancel.set()
    await driver.cancel_finished.wait()

    assert len(driver.cancel_calls) == 1
    assert driver.cancel_completed
    assert not driver.cancel_interrupted


@pytest.mark.asyncio
async def test_submit_cancellation_preserves_cancelled_error_when_compensation_fails(caplog):
    repo = BlockingCreateRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
        ),
        cancel_error=RuntimeError("cancel unavailable"),
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    request = _request(
        local_task_id="task-1",
    )

    submit_task = asyncio.create_task(service.submit(driver_name="fake", request=request))
    await repo.create_started.wait()
    submit_task.cancel()

    with caplog.at_level(logging.ERROR), pytest.raises(asyncio.CancelledError):
        await submit_task

    assert len(driver.cancel_calls) == 1
    assert "Failed to cancel untracked MCP task" in caplog.text
    assert "mcp_task_compensation_failed" in caplog.text
    assert "cancel unavailable" not in caplog.text


@pytest.mark.asyncio
async def test_submit_cancels_remote_task_when_its_id_exceeds_storage_limit():
    repo = FakeRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="r" * 256,
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
            driver_data={"cancel_tool": "cancel"},
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    with pytest.raises(McpTaskProtocolError, match="remote_task_id.*255"):
        await service.submit(
            driver_name="fake",
            request=_request(
                local_task_id="task-1",
            ),
        )

    assert repo.created == []
    assert len(driver.cancel_calls) == 1
    assert driver.cancel_calls[0].remote_task_id == "r" * 256


@pytest.mark.asyncio
async def test_duplicate_remote_handle_is_rejected_without_cancelling_existing_task():
    repo = DuplicateCreateRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
            driver_data={"cancel_tool": "cancel"},
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    with pytest.raises(DuplicateMcpRemoteTaskError, match="already tracked"):
        await service.submit(
            driver_name="fake",
            request=_request(
                thread_id="thread-2",
            ),
        )

    assert driver.cancel_calls == []


@pytest.mark.asyncio
async def test_racing_identical_lineage_replay_returns_the_committed_task() -> None:
    repo = RacyLineageRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="same-idempotent-remote",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    request = _request(arguments={"topic": "MCP"})

    first = await service.submit(driver_name="fake", request=request)
    second = await service.submit(driver_name="fake", request=request)

    assert second == first
    assert len(driver.submit_calls) == 2
    assert driver.cancel_calls == []


@pytest.mark.asyncio
async def test_racing_identical_local_id_replay_returns_the_committed_task() -> None:
    repo = RacyTaskIdRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="same-idempotent-remote",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    request = _request(arguments={"topic": "MCP"})

    first = await service.submit(driver_name="fake", request=request)
    second = await service.submit(driver_name="fake", request=request)

    assert second == first
    assert len(driver.submit_calls) == 2
    assert driver.cancel_calls == []


@pytest.mark.asyncio
async def test_conflicting_idempotency_replay_fails_before_remote_submit() -> None:
    original = _request(
        arguments={"topic": "first"},
        local_task_id="mcp-task-stable-key",
    )
    conflicting = _request(
        arguments={"topic": "different"},
        local_task_id="mcp-task-stable-key",
    )
    repo = StatefulSubmissionRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.submit(driver_name="fake", request=original)

    with pytest.raises(McpTaskLineageError) as exc_info:
        await service.submit(driver_name="fake", request=conflicting)

    assert exc_info.value.code == "mcp_task_request_conflict"
    assert len(driver.submit_calls) == 1


@pytest.mark.asyncio
async def test_same_shape_replay_with_different_exact_arguments_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = base64.urlsafe_b64encode(b"k" * 32).decode("ascii").rstrip("=")
    monkeypatch.setenv("MCP_TASK_REPLAY_HMAC_KEYS", json.dumps({"v1": key}))
    monkeypatch.setenv("MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID", "v1")
    repo = StatefulSubmissionRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.submit(
        driver_name="fake",
        request=_request(
            arguments={"topic": "alpha"},
            local_task_id="mcp-task-stable-key",
        ),
    )

    with pytest.raises(McpTaskLineageError) as exc_info:
        await service.submit(
            driver_name="fake",
            request=_request(
                arguments={"topic": "bravo"},
                local_task_id="mcp-task-stable-key",
            ),
        )

    assert exc_info.value.code == "mcp_task_request_conflict"
    assert len(driver.submit_calls) == 1


@pytest.mark.asyncio
async def test_replay_commitment_preserves_all_attachment_fields() -> None:
    repo = StatefulSubmissionRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.submit(
        driver_name="fake",
        request=_request(
            arguments={
                "attachments": [
                    {
                        "file_id": "same-file",
                        "provider_locator": "bucket-a",
                    }
                ]
            },
            local_task_id="mcp-task-stable-key",
        ),
    )

    with pytest.raises(McpTaskLineageError) as exc_info:
        await service.submit(
            driver_name="fake",
            request=_request(
                arguments={
                    "attachments": [
                        {
                            "file_id": "same-file",
                            "provider_locator": "bucket-b",
                        }
                    ]
                },
                local_task_id="mcp-task-stable-key",
            ),
        )

    assert exc_info.value.code == "mcp_task_request_conflict"
    assert len(driver.submit_calls) == 1


@pytest.mark.asyncio
async def test_replay_fails_closed_when_the_stored_key_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_key = base64.urlsafe_b64encode(b"o" * 32).decode("ascii").rstrip("=")
    new_key = base64.urlsafe_b64encode(b"n" * 32).decode("ascii").rstrip("=")
    monkeypatch.setenv("MCP_TASK_REPLAY_HMAC_KEYS", json.dumps({"old-v1": old_key}))
    monkeypatch.setenv("MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID", "old-v1")
    repo = StatefulSubmissionRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    request = _request(
        arguments={"topic": "alpha"},
        local_task_id="mcp-task-stable-key",
    )
    original_service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    await original_service.submit(driver_name="fake", request=request)

    monkeypatch.setenv("MCP_TASK_REPLAY_HMAC_KEYS", json.dumps({"new-v2": new_key}))
    monkeypatch.setenv("MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID", "new-v2")
    restarted_service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    with pytest.raises(McpTaskLineageError) as exc_info:
        await restarted_service.submit(driver_name="fake", request=request)

    assert exc_info.value.code == "mcp_task_request_commitment_key_unavailable"
    assert len(driver.submit_calls) == 1


@pytest.mark.asyncio
async def test_replay_fails_closed_for_a_legacy_row_without_a_commitment() -> None:
    repo = StatefulSubmissionRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    request = _request(
        arguments={"topic": "alpha"},
        local_task_id="mcp-task-stable-key",
    )
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    await service.submit(driver_name="fake", request=request)
    assert repo.persisted is not None
    repo.persisted["request_commitment_version"] = None
    repo.persisted["request_commitment_key_id"] = None
    repo.persisted["request_commitment_digest"] = None

    with pytest.raises(McpTaskLineageError) as exc_info:
        await service.submit(driver_name="fake", request=request)

    assert exc_info.value.code == "mcp_task_request_commitment_legacy_unavailable"
    assert len(driver.submit_calls) == 1


@pytest.mark.asyncio
async def test_cancel_task_persists_request_without_calling_remote():
    record = {**_claimed_row(), "cancel_requested_at": datetime.now(UTC).isoformat()}
    repo = _repo(
        request_cancel=AsyncMock(return_value=record),
        claim_cancel_requests=AsyncMock(return_value=[{**record, "cancel_attempt_count": 1}]),
        apply_cancel_snapshot=AsyncMock(return_value=True),
        release_cancel_claim=AsyncMock(return_value=True),
        get=AsyncMock(return_value={**record, "status": "cancelled"}),
    )
    driver = FakeDriver()
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    result = await service.cancel_task(
        task_id="task-1",
        thread_id="thread-1",
        user_id="user-1",
        reason_code="user_api",
    )

    assert result == record
    request = repo.request_cancel.await_args
    assert request.kwargs["reason_code"] == "user_api"
    assert len(request.kwargs["actor_ref"]) == 64
    int(request.kwargs["actor_ref"], 16)
    assert driver.cancel_calls == []
    repo.claim_cancel_requests.assert_not_awaited()
    repo.apply_cancel_snapshot.assert_not_awaited()
    repo.release_cancel_claim.assert_not_awaited()
    repo.get.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_failure_schedules_retry_from_call_completion_time():
    record = {**_claimed_row(), "cancel_attempt_count": 1}
    repo = _repo(
        claim_cancel_requests=AsyncMock(return_value=[record]),
        release_cancel_claim=AsyncMock(return_value=True),
        claim_due_tasks=AsyncMock(return_value=[]),
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", FakeDriver(cancel_error=RuntimeError("cancel unavailable")))
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    scan_started_at = datetime(2000, 1, 1, tzinfo=UTC)

    await service.run_once(now=scan_started_at)

    released = repo.release_cancel_claim.await_args.kwargs
    assert released["retry_after_seconds"] == 5


@pytest.mark.asyncio
async def test_cancel_recovery_failures_are_isolated_and_later_phases_continue(caplog):
    records = [
        {**_claimed_row(), "id": "task-broken", "cancel_attempt_count": 1},
        {**_claimed_row(), "id": "task-sibling", "remote_task_id": "remote-2", "cancel_attempt_count": 1},
    ]

    async def release_cancel_claim(task_id, **_kwargs):
        if task_id == "task-broken":
            raise RuntimeError("cancel recovery store unavailable")
        return True

    repo = _repo(
        claim_cancel_requests=AsyncMock(return_value=records),
        release_cancel_claim=AsyncMock(side_effect=release_cancel_claim),
        claim_due_tasks=AsyncMock(return_value=[]),
        claim_notification_work=AsyncMock(return_value=[]),
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", FakeDriver(cancel_error=RuntimeError("cancel unavailable")))
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
        launch_notification=AsyncMock(),
        get_run=AsyncMock(),
    )

    with caplog.at_level(logging.ERROR):
        await service.run_once(now=datetime.now(UTC))

    assert repo.release_cancel_claim.await_count == 2
    repo.claim_due_tasks.assert_awaited_once()
    repo.claim_notification_work.assert_awaited_once()
    assert "task-broken" in caplog.text
    assert "mcp_task_cancel_persistence_failed" in caplog.text
    assert "cancel recovery store unavailable" not in caplog.text


@pytest.mark.asyncio
async def test_notification_delivery_waits_for_successful_agent_run():
    repo = _repo(
        mark_notification_dispatched=AsyncMock(return_value=True),
        finish_notification_run=AsyncMock(return_value=True),
        release_notification_claim=AsyncMock(return_value=True),
        defer_dispatched_notification=AsyncMock(return_value=True),
    )
    launch = AsyncMock(return_value={"run_id": "notify-run-1"})
    get_run = AsyncMock(return_value=SimpleNamespace(status=RunStatus.running))
    service = McpTaskService(
        repository=repo,
        drivers=McpTaskDriverRegistry(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
        launch_notification=launch,
        get_run=get_run,
    )
    now = datetime.now(UTC)
    claimed = {
        **_claimed_row(),
        "notification_status": "claimed",
        "dispatch_version": 2,
        "dispatch_attempt": 0,
        "dispatch_event": {"status": "completed"},
        "tenant_digest": _TENANT.digest,
        "lineage_digest": "b" * 64,
        "parent_run_id": "run-parent",
        "parent_tool_receipt_id": "tr_" + "c" * 64,
        "event_fingerprint": "d" * 64,
    }

    await service._notify_one(claimed, now=now)

    repo.mark_notification_dispatched.assert_awaited_once()
    repo.finish_notification_run.assert_not_awaited()
    source = launch.await_args.kwargs["source"]
    assert source == {
        "version": 1,
        "tenant_digest": _TENANT.digest,
        "task_id": claimed["id"],
        "task_lineage_digest": "b" * 64,
        "lineage_status": "legacy_unavailable",
        "parent_run_id": "run-parent",
        "parent_tool_receipt_id": "tr_" + "c" * 64,
        "terminal_result_version": 2,
        "notification_kind": "terminal",
        "result_digest": "d" * 64,
        "result_status": "completed",
    }
    assert "dispatch_attempt" not in launch.await_args.kwargs

    get_run.return_value = SimpleNamespace(status=RunStatus.success)
    await service._notify_one(
        {
            **claimed,
            "notification_status": "dispatched",
            "notification_run_id": "notify-run-1",
        },
        now=now,
    )
    repo.finish_notification_run.assert_awaited_once()
    assert repo.finish_notification_run.await_args.kwargs["delivered"] is True


@pytest.mark.asyncio
async def test_missing_dispatched_notification_run_retries_delivery():
    repo = _repo(
        finish_notification_run=AsyncMock(return_value=True),
        defer_dispatched_notification=AsyncMock(return_value=True),
    )
    service = McpTaskService(
        repository=repo,
        drivers=McpTaskDriverRegistry(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
        launch_notification=AsyncMock(),
        get_run=AsyncMock(return_value=None),
    )
    now = datetime.now(UTC)

    await service._notify_one(
        {
            **_claimed_row(),
            "notification_status": "dispatched",
            "notification_run_id": "missing-run",
            "dispatch_version": 2,
            "notification_attempt_count": 2,
        },
        now=now,
    )

    repo.defer_dispatched_notification.assert_not_awaited()
    repo.finish_notification_run.assert_awaited_once()
    finished = repo.finish_notification_run.await_args.kwargs
    assert finished["delivered"] is False
    assert finished["retry_after_seconds"] == 20
    assert "missing-run" in finished["error"]


@pytest.mark.asyncio
async def test_notification_failures_are_isolated_and_release_their_lease(caplog):
    records = [
        {
            **_claimed_row(),
            "id": "task-broken",
            "notification_status": "dispatched",
            "notification_run_id": "run-broken",
            "dispatch_version": 2,
        },
        {
            **_claimed_row(),
            "id": "task-success",
            "notification_status": "dispatched",
            "notification_run_id": "run-success",
            "dispatch_version": 3,
        },
    ]
    repo = _repo(
        claim_notification_work=AsyncMock(return_value=records),
        finish_notification_run=AsyncMock(return_value=True),
        defer_dispatched_notification=AsyncMock(return_value=True),
        release_notification_lease=AsyncMock(return_value=True),
    )
    get_run = AsyncMock(
        side_effect=[
            RuntimeError("run store unavailable"),
            SimpleNamespace(status=RunStatus.success),
        ]
    )
    service = McpTaskService(
        repository=repo,
        drivers=McpTaskDriverRegistry(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
        launch_notification=AsyncMock(),
        get_run=get_run,
    )
    now = datetime.now(UTC)

    with caplog.at_level(logging.ERROR):
        await service._run_notifications(now=now)

    repo.finish_notification_run.assert_awaited_once()
    assert repo.finish_notification_run.await_args.args[0] == "task-success"
    repo.release_notification_lease.assert_awaited_once()
    released = repo.release_notification_lease.await_args
    assert released.args[0] == "task-broken"
    assert released.kwargs["retry_after_seconds"] == 5
    assert released.kwargs["error"] == "mcp_task_notification_processing_failed"
    assert "run store unavailable" not in caplog.text
    assert "task-broken" in caplog.text


@pytest.mark.asyncio
async def test_notification_busy_thread_replaces_claim_with_latest_event():
    repo = _repo(
        release_notification_claim=AsyncMock(return_value=True),
    )
    service = McpTaskService(
        repository=repo,
        drivers=McpTaskDriverRegistry(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
        launch_notification=AsyncMock(side_effect=ConflictError("thread busy")),
        get_run=AsyncMock(return_value=SimpleNamespace(assistant_id="lead_agent")),
    )
    now = datetime.now(UTC)

    await service._notify_one(
        {
            **_claimed_row(),
            "notification_status": "claimed",
            "dispatch_version": 2,
            "dispatch_attempt": 0,
            "dispatch_event": {"status": "input_required"},
        },
        now=now,
    )

    released = repo.release_notification_claim.await_args.kwargs
    assert released["replace_with_latest"] is True
    assert released["retry_after_seconds"] == 5


@pytest.mark.asyncio
async def test_notification_launch_failure_backs_off_and_replaces_with_latest_event():
    repo = _repo(
        release_notification_claim=AsyncMock(return_value=True),
    )
    service = McpTaskService(
        repository=repo,
        drivers=McpTaskDriverRegistry(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
        max_poll_backoff_seconds=300,
        launch_notification=AsyncMock(side_effect=RuntimeError("run store unavailable")),
        get_run=AsyncMock(return_value=SimpleNamespace(assistant_id="lead_agent")),
    )
    now = datetime.now(UTC)

    await service._notify_one(
        {
            **_claimed_row(),
            "notification_status": "claimed",
            "dispatch_version": 2,
            "dispatch_attempt": 0,
            "notification_attempt_count": 3,
            "dispatch_event": {"status": "input_required"},
        },
        now=now,
    )

    released = repo.release_notification_claim.await_args.kwargs
    assert released["replace_with_latest"] is True
    assert released["count_failure"] is True
    assert released["retry_after_seconds"] == 40


@pytest.mark.asyncio
async def test_permanently_rejected_notification_is_dead_lettered():
    repo = _repo(
        dead_letter_notification=AsyncMock(return_value=True),
        release_notification_claim=AsyncMock(return_value=True),
    )
    service = McpTaskService(
        repository=repo,
        drivers=McpTaskDriverRegistry(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
        launch_notification=AsyncMock(side_effect=PermanentNotificationError("Thread thread-1 not found")),
        get_run=AsyncMock(return_value=SimpleNamespace(assistant_id="lead_agent")),
    )
    now = datetime.now(UTC)

    await service._notify_one(
        {
            **_claimed_row(),
            "notification_status": "claimed",
            "dispatch_version": 2,
            "dispatch_attempt": 0,
            "notification_attempt_count": 0,
            "dispatch_event": {"status": "completed"},
        },
        now=now,
    )

    repo.dead_letter_notification.assert_awaited_once()
    dead_lettered = repo.dead_letter_notification.await_args.kwargs
    assert dead_lettered["dispatch_version"] == 2
    assert dead_lettered["error"] == "mcp_task_notification_permanent_failure"
    assert dead_lettered["count_failure"] is True
    repo.release_notification_claim.assert_not_awaited()


@pytest.mark.asyncio
async def test_notification_retry_budget_dead_letters_before_creating_another_run():
    repo = _repo(
        dead_letter_notification=AsyncMock(return_value=True),
    )
    launch_notification = AsyncMock()
    get_run = AsyncMock()
    service = McpTaskService(
        repository=repo,
        drivers=McpTaskDriverRegistry(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
        launch_notification=launch_notification,
        get_run=get_run,
    )
    now = datetime.now(UTC)

    await service._notify_one(
        {
            **_claimed_row(),
            "notification_status": "retry",
            "notification_error": "Agent run failed",
            "dispatch_version": 2,
            "dispatch_attempt": 5,
            "notification_attempt_count": 5,
            "dispatch_event": {"status": "completed"},
        },
        now=now,
    )

    launch_notification.assert_not_awaited()
    get_run.assert_not_awaited()
    dead_lettered = repo.dead_letter_notification.await_args.kwargs
    assert dead_lettered["dispatch_version"] == 2
    assert dead_lettered["count_failure"] is False
    assert dead_lettered["error"] == "mcp_task_notification_retry_exhausted"


@pytest.mark.asyncio
async def test_dispatched_notification_retry_budget_dead_letters_before_hydrating_run():
    repo = _repo(
        dead_letter_notification=AsyncMock(return_value=True),
    )
    get_run = AsyncMock()
    service = McpTaskService(
        repository=repo,
        drivers=McpTaskDriverRegistry(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
        launch_notification=AsyncMock(),
        get_run=get_run,
    )
    now = datetime.now(UTC)

    await service._notify_one(
        {
            **_claimed_row(),
            "notification_status": "dispatched",
            "notification_run_id": "notify-run-1",
            "notification_error": "run store unavailable",
            "dispatch_version": 2,
            "notification_attempt_count": 5,
        },
        now=now,
    )

    get_run.assert_not_awaited()
    dead_lettered = repo.dead_letter_notification.await_args.kwargs
    assert dead_lettered["dispatch_version"] == 2
    assert dead_lettered["count_failure"] is False
    assert dead_lettered["error"] == "mcp_task_notification_retry_exhausted"


@pytest.mark.asyncio
async def test_submit_preserves_persistence_error_when_compensation_cancel_fails(caplog):
    repo = FailingCreateRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
        ),
        cancel_error=RuntimeError("cancel unavailable"),
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    request = _request(
        arguments={"topic": "MCP"},
        local_task_id="task-1",
    )

    with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError, match="database unavailable"):
        await service.submit(driver_name="fake", request=request)

    assert "Failed to cancel untracked MCP task" in caplog.text
    assert "mcp_task_compensation_failed" in caplog.text
    assert "cancel unavailable" not in caplog.text


@pytest.mark.asyncio
async def test_run_once_polls_without_an_llm_and_schedules_next_poll():
    repo = FakeRepository([_claimed_row()])
    driver = FakeDriver([TaskSnapshot(status=TaskStatus.WORKING, poll_after_seconds=12)])
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    scan_started_at = datetime(2000, 1, 1, tzinfo=UTC)

    await service.run_once(now=scan_started_at)

    assert driver.status_calls[0].remote_task_id == "remote-1"
    _, update = repo.applied[0]
    assert update["status"] == "working"
    assert update["next_poll_after_seconds"] == 12
    assert update["polled_at"] > scan_started_at


@pytest.mark.asyncio
async def test_run_once_caps_remote_poll_hint_to_one_day():
    repo = FakeRepository([_claimed_row()])
    driver = FakeDriver([TaskSnapshot(status=TaskStatus.WORKING, poll_after_seconds=1e20)])
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.run_once(now=datetime.now(UTC))

    _, update = repo.applied[0]
    assert update["next_poll_after_seconds"] == 86_400


@pytest.mark.asyncio
async def test_run_once_schedules_driver_error_retry_from_poll_completion_time():
    repo = FakeRepository([_claimed_row()])
    registry = McpTaskDriverRegistry()
    registry.register("fake", FakeDriver(error=RuntimeError("network down")))
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    scan_started_at = datetime(2000, 1, 1, tzinfo=UTC)

    await service.run_once(now=scan_started_at)

    _, released = repo.released[0]
    assert released["retry_after_seconds"] == 5


@pytest.mark.asyncio
async def test_run_once_stops_terminal_tasks_but_keeps_input_required_on_a_slow_poll():
    rows = [_claimed_row(), {**_claimed_row(), "id": "task-2", "remote_task_id": "remote-2"}]
    repo = FakeRepository(rows)
    driver = FakeDriver(
        [
            TaskSnapshot(status=TaskStatus.COMPLETED, result={"report": "ready"}),
            TaskSnapshot(status=TaskStatus.INPUT_REQUIRED, input_required={"prompt": "Approve?"}),
        ]
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.run_once(now=datetime.now(UTC))

    updates = {task_id: update for task_id, update in repo.applied}
    assert updates["task-1"]["status"] == "completed"
    assert updates["task-1"]["next_poll_after_seconds"] is None
    assert updates["task-2"]["status"] == "input_required"
    assert updates["task-2"]["input_required"] == {"prompt": "Approve?"}
    assert updates["task-2"]["next_poll_after_seconds"] == 60


@pytest.mark.asyncio
async def test_run_once_uses_exponential_backoff_and_caps_transient_errors():
    rows = [
        {**_claimed_row(), "id": "task-1", "consecutive_poll_error_count": 0},
        {**_claimed_row(), "id": "task-2", "consecutive_poll_error_count": 4},
    ]
    repo = FakeRepository(rows)
    registry = McpTaskDriverRegistry()
    registry.register("fake", FakeDriver(error=RuntimeError("network down")))
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
        max_poll_backoff_seconds=30,
    )

    await service.run_once(now=datetime.now(UTC))

    released = {task_id: update for task_id, update in repo.released}
    assert released["task-1"]["retry_after_seconds"] == 5
    assert released["task-2"]["retry_after_seconds"] == 30


@pytest.mark.asyncio
async def test_provider_failure_text_is_absent_from_logs_and_operational_state(
    caplog,
) -> None:
    repo = FakeRepository([_claimed_row()])
    registry = McpTaskDriverRegistry()
    registry.register(
        "fake",
        FakeDriver(error=RuntimeError("Bearer credential-and-provider-detail")),
    )
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    with caplog.at_level(logging.WARNING):
        await service.run_once(now=datetime.now(UTC))

    assert repo.released[0][1]["error"] == "mcp_task_remote_poll_failed"
    assert "credential-and-provider-detail" not in caplog.text
    assert "Bearer" not in caplog.text


@pytest.mark.asyncio
async def test_provider_cannot_smuggle_failure_text_through_code_attribute(
    caplog,
) -> None:
    class ProviderError(RuntimeError):
        code = "mcp_task_Bearer-secret-provider-detail"

    repo = FakeRepository([_claimed_row()])
    registry = McpTaskDriverRegistry()
    registry.register("fake", FakeDriver(error=ProviderError("also-secret")))
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    with caplog.at_level(logging.WARNING):
        await service.run_once(now=datetime.now(UTC))

    assert repo.released[0][1]["error"] == "mcp_task_remote_poll_failed"
    assert "secret-provider-detail" not in caplog.text
    assert "also-secret" not in caplog.text


@pytest.mark.asyncio
async def test_recovery_preserves_safe_credential_binding_failure_code() -> None:
    repo = FakeRepository([_claimed_row()])
    registry = McpTaskDriverRegistry()
    registry.register(
        "fake",
        FakeDriver(error=McpTaskLineageError("mcp_task_credential_binding_unavailable")),
    )
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.run_once(now=datetime.now(UTC))

    assert repo.released[0][1]["error"] == ("mcp_task_credential_binding_unavailable")


@pytest.mark.asyncio
async def test_protocol_error_terminalizes_instead_of_retrying_forever():
    repo = FakeRepository([_claimed_row()])
    registry = McpTaskDriverRegistry()
    registry.register("fake", FakeDriver(error=McpTaskProtocolError("missing structuredContent")))
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.run_once(now=datetime.now(UTC))

    assert repo.released == []
    _, applied = repo.applied[0]
    assert applied["status"] == "failed"
    assert applied["error"] == "mcp_task_remote_protocol_invalid"
    assert applied["next_poll_after_seconds"] is None


@pytest.mark.asyncio
async def test_protocol_error_message_is_bounded_before_terminal_persistence():
    oversized_error = "e" * 5_000
    repo = FakeRepository([_claimed_row()])
    registry = McpTaskDriverRegistry()
    registry.register("fake", FakeDriver(error=McpTaskProtocolError(oversized_error)))
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.run_once(now=datetime.now(UTC))

    _, applied = repo.applied[0]
    assert applied["status"] == "failed"
    assert applied["error"] == "mcp_task_remote_protocol_invalid"


@pytest.mark.asyncio
async def test_persisted_snapshot_errors_are_bounded_on_submit_and_poll():
    oversized_error = "e" * 5_000
    submit_repo = FakeRepository()
    submit_driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(status=TaskStatus.FAILED, error=oversized_error),
        )
    )
    submit_registry = McpTaskDriverRegistry()
    submit_registry.register("fake", submit_driver)
    submit_service = McpTaskService(
        repository=submit_repo,
        drivers=submit_registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await submit_service.submit(
        driver_name="fake",
        request=_request(),
    )

    assert submit_repo.created[0]["error"] == "mcp_task_remote_failed"

    poll_repo = FakeRepository([_claimed_row()])
    poll_registry = McpTaskDriverRegistry()
    poll_registry.register(
        "fake",
        FakeDriver([TaskSnapshot(status=TaskStatus.FAILED, error=oversized_error)]),
    )
    poll_service = McpTaskService(
        repository=poll_repo,
        drivers=poll_registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await poll_service.run_once(now=datetime.now(UTC))

    _, applied = poll_repo.applied[0]
    assert applied["error"] == "mcp_task_remote_failed"


@pytest.mark.asyncio
async def test_provider_snapshot_cannot_smuggle_prefixed_error_code() -> None:
    provider_error = "mcp_task_Bearer-secret-provider-detail"
    repo = FakeRepository()
    driver = FakeDriver(
        submission=TaskSubmission(
            remote_task_id="remote-1",
            snapshot=TaskSnapshot(
                status=TaskStatus.FAILED,
                error=provider_error,
            ),
        )
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.submit(driver_name="fake", request=_request())

    assert repo.created[0]["error"] == "mcp_task_remote_failed"
    assert provider_error not in str(repo.created[0])


@pytest.mark.asyncio
async def test_oversized_input_required_payload_terminalizes_without_persisting_it():
    repo = FakeRepository([_claimed_row()])
    registry = McpTaskDriverRegistry()
    registry.register(
        "fake",
        FakeDriver(
            [
                TaskSnapshot(
                    status=TaskStatus.INPUT_REQUIRED,
                    input_required={"prompt": "x" * 65_536},
                )
            ]
        ),
    )
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.run_once(now=datetime.now(UTC))

    assert repo.released == []
    _, applied = repo.applied[0]
    assert applied["status"] == "failed"
    assert applied["input_required"] is None
    assert applied["error"] == "mcp_task_remote_protocol_invalid"
    assert applied["next_poll_after_seconds"] is None


@pytest.mark.asyncio
async def test_oversized_result_stores_preview_without_invalid_truncated_json():
    repo = FakeRepository([_claimed_row()])
    registry = McpTaskDriverRegistry()
    registry.register(
        "fake",
        FakeDriver(
            [
                TaskSnapshot(
                    status=TaskStatus.COMPLETED,
                    result={"report": "x" * 200},
                    result_artifact={"uri": "s3://reports/1.json", "mime_type": "application/json"},
                )
            ]
        ),
    )
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
        max_result_bytes=64,
        result_preview_max_chars=24,
    )

    await service.run_once(now=datetime.now(UTC))

    _, applied = repo.applied[0]
    assert applied["result"] is None
    assert len(applied["result_preview"]) == 24
    assert applied["result_truncated"] is True
    assert applied["result_artifact"] == {
        "uri": "s3://reports/1.json",
        "mime_type": "application/json",
    }


@pytest.mark.asyncio
async def test_oversized_result_artifact_terminalizes_without_persisting_it():
    repo = FakeRepository([_claimed_row()])
    registry = McpTaskDriverRegistry()
    registry.register(
        "fake",
        FakeDriver(
            [
                TaskSnapshot(
                    status=TaskStatus.COMPLETED,
                    result_artifact={
                        "uri": "https://example.test/" + "x" * 65_536,
                        "mime_type": "application/json",
                    },
                )
            ]
        ),
    )
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.run_once(now=datetime.now(UTC))

    assert repo.released == []
    _, applied = repo.applied[0]
    assert applied["status"] == "failed"
    assert applied["result_artifact"] is None
    assert applied["error"] == "mcp_task_remote_protocol_invalid"


@pytest.mark.asyncio
async def test_non_json_numeric_result_is_a_permanent_protocol_failure():
    repo = FakeRepository([_claimed_row()])
    registry = McpTaskDriverRegistry()
    registry.register(
        "fake",
        FakeDriver(
            [
                TaskSnapshot(
                    status=TaskStatus.COMPLETED,
                    result={"score": float("nan")},
                )
            ]
        ),
    )
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.run_once(now=datetime.now(UTC))

    assert repo.released == []
    _, applied = repo.applied[0]
    assert applied["status"] == "failed"
    assert applied["error"] == "mcp_task_remote_protocol_invalid"


@pytest.mark.asyncio
async def test_run_once_releases_claim_when_driver_is_missing_or_fails():
    rows = [_claimed_row(driver_name="missing"), {**_claimed_row(), "id": "task-2", "remote_task_id": "remote-2", "driver_name": "broken"}]
    repo = FakeRepository(rows)
    registry = McpTaskDriverRegistry()
    registry.register("broken", FakeDriver(error=RuntimeError("network down")))
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    now = datetime.now(UTC)

    await service.run_once(now=now)

    released = {task_id: update for task_id, update in repo.released}
    assert released["task-1"]["error"] == "mcp_task_driver_unavailable"
    assert released["task-2"]["error"] == "mcp_task_remote_poll_failed"
    assert released["task-1"]["retry_after_seconds"] == 5
    assert released["task-2"]["retry_after_seconds"] == 5


@pytest.mark.asyncio
async def test_run_once_isolates_unexpected_failure_to_its_claimed_task(caplog):
    rows = [_claimed_row(), {**_claimed_row(), "id": "task-2", "remote_task_id": "remote-2"}]
    repo = FailingApplyRepository(rows)
    driver = FakeDriver(
        [
            TaskSnapshot(status=TaskStatus.COMPLETED, result={"report": "first"}),
            TaskSnapshot(status=TaskStatus.COMPLETED, result={"report": "second"}),
        ]
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    with caplog.at_level(logging.ERROR):
        await service.run_once(now=datetime.now(UTC))

    assert [task_id for task_id, _update in repo.applied] == ["task-2"]
    assert "task_id=task-1" in caplog.text
    assert "mcp_task_poll_persistence_failed" in caplog.text
    assert "database unavailable" not in caplog.text


@pytest.mark.asyncio
async def test_start_runs_recovery_poll_immediately_and_stop_is_clean():
    repo = FakeRepository([])
    service = McpTaskService(
        repository=repo,
        drivers=McpTaskDriverRegistry(),
        poll_interval_seconds=60,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.start()
    for _ in range(20):
        if repo.claimed:
            break
        await __import__("asyncio").sleep(0)
    await service.stop()

    assert repo.claimed is True


@pytest.mark.asyncio
async def test_stop_cancels_a_hung_driver_poll():
    repo = FakeRepository([_claimed_row()])
    driver = HangingDriver()
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=60,
        lease_seconds=120,
        max_concurrent_polls=3,
    )

    await service.start()
    await asyncio.wait_for(driver.started.wait(), timeout=1)
    await asyncio.wait_for(service.stop(), timeout=1)

    assert driver.cancelled is True


@pytest.mark.asyncio
async def test_stop_cancels_inflight_submit_and_waits_for_remote_compensation():
    """Shutdown owns submits that already created an unpersisted remote task."""

    repo = BlockingCreateRepository()
    driver = BlockingCancelDriver(
        submission=TaskSubmission(
            remote_task_id="remote-shutdown-race",
            snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED),
            driver_data={"cancel_tool": "cancel"},
        ),
    )
    registry = McpTaskDriverRegistry()
    registry.register("fake", driver)
    service = McpTaskService(
        repository=repo,
        drivers=registry,
        poll_interval_seconds=60,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    await service.start()

    submitting = asyncio.create_task(
        service.submit(
            driver_name="fake",
            request=_request(local_task_id="task-shutdown-race"),
        ),
    )
    await asyncio.wait_for(repo.create_started.wait(), timeout=1)
    stopping = asyncio.create_task(service.stop())
    cancel_started = asyncio.create_task(driver.cancel_started.wait())

    try:
        done, _ = await asyncio.wait(
            {stopping, cancel_started},
            timeout=1,
            return_when=asyncio.FIRST_COMPLETED,
        )
        assert cancel_started in done
        assert stopping not in done

        driver.finish_cancel.set()
        await asyncio.wait_for(stopping, timeout=1)
        with pytest.raises(asyncio.CancelledError):
            await submitting

        assert driver.cancel_completed is True
        assert driver.cancel_interrupted is False
    finally:
        driver.finish_cancel.set()
        if not submitting.done():
            submitting.cancel()
        await asyncio.gather(submitting, return_exceptions=True)
        if not cancel_started.done():
            cancel_started.cancel()
        await asyncio.gather(cancel_started, return_exceptions=True)
        if not stopping.done():
            stopping.cancel()
        await asyncio.gather(stopping, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("poller_running", [False, True])
async def test_stop_drains_submit_compensation_with_or_without_poller(
    poller_running,
):
    repo = FakeRepository([])
    service = McpTaskService(
        repository=repo,
        drivers=McpTaskDriverRegistry(),
        poll_interval_seconds=60,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    if poller_running:
        await service.start()

    compensation_started = asyncio.Event()
    finish_compensation = asyncio.Event()

    async def compensate() -> None:
        compensation_started.set()
        await finish_compensation.wait()

    compensation = asyncio.create_task(compensate())
    service._compensation_tasks.add(compensation)
    compensation.add_done_callback(service._compensation_tasks.discard)
    await compensation_started.wait()

    stopping = asyncio.create_task(service.stop())
    await asyncio.sleep(0)
    assert stopping.done() is False
    finish_compensation.set()
    await asyncio.wait_for(stopping, timeout=1)

    assert compensation.done()
    assert compensation.cancelled() is False


@pytest.mark.asyncio
async def test_stop_cancels_and_observes_compensation_past_shutdown_budget(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        service_module,
        "_UNTRACKED_TASK_COMPENSATION_WAIT_SECONDS",
        0,
    )
    service = McpTaskService(
        repository=FakeRepository([]),
        drivers=McpTaskDriverRegistry(),
        poll_interval_seconds=60,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    cancelled = asyncio.Event()

    async def compensate() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    compensation = asyncio.create_task(compensate())
    service._compensation_tasks.add(compensation)
    compensation.add_done_callback(service._compensation_tasks.discard)
    await asyncio.sleep(0)

    await asyncio.wait_for(service.stop(), timeout=1)

    assert compensation.cancelled()
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_stop_outer_cancellation_still_cancels_and_observes_compensation(
    monkeypatch: pytest.MonkeyPatch,
):
    """The Gateway phase timeout cannot abandon an untracked remote job."""

    monkeypatch.setattr(
        service_module,
        "_UNTRACKED_TASK_COMPENSATION_WAIT_SECONDS",
        60,
    )
    service = McpTaskService(
        repository=FakeRepository([]),
        drivers=McpTaskDriverRegistry(),
        poll_interval_seconds=60,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    compensation_started = asyncio.Event()
    compensation_cancelled = asyncio.Event()

    async def compensate() -> None:
        compensation_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            compensation_cancelled.set()

    compensation = asyncio.create_task(compensate())
    service._compensation_tasks.add(compensation)
    compensation.add_done_callback(service._compensation_tasks.discard)
    await compensation_started.wait()

    stopping = asyncio.create_task(service.stop())
    await asyncio.sleep(0)
    stopping.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stopping

    assert compensation.cancelled()
    assert compensation_cancelled.is_set()
    assert not service._compensation_tasks


@pytest.mark.asyncio
async def test_stop_is_bounded_when_submit_compensation_resists_cancellation(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    """A broken driver cannot make the producer-shutdown phase unbounded."""

    monkeypatch.setattr(
        service_module,
        "_UNTRACKED_TASK_COMPENSATION_WAIT_SECONDS",
        0.01,
    )
    service = McpTaskService(
        repository=FakeRepository([]),
        drivers=McpTaskDriverRegistry(),
        poll_interval_seconds=60,
        lease_seconds=120,
        max_concurrent_polls=3,
    )
    release = asyncio.Event()
    cancellation_observed = asyncio.Event()

    async def resist_cancellation() -> None:
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancellation_observed.set()

    compensation = asyncio.create_task(resist_cancellation())
    service._compensation_tasks.add(compensation)
    compensation.add_done_callback(service._compensation_tasks.discard)
    await asyncio.sleep(0)

    stopping = asyncio.create_task(service.stop())
    try:
        with caplog.at_level(logging.WARNING):
            done, _ = await asyncio.wait({stopping}, timeout=0.25)

        assert stopping in done
        with pytest.raises(
            RuntimeError,
            match="mcp_task_compensation_shutdown_incomplete",
        ):
            await stopping
        assert cancellation_observed.is_set()
        assert compensation in service._compensation_tasks
        assert not compensation.done()
        assert "mcp_task_compensation_shutdown_incomplete" in caplog.text
    finally:
        release.set()
        await asyncio.gather(compensation, return_exceptions=True)
        if not stopping.done():
            await asyncio.gather(stopping, return_exceptions=True)
