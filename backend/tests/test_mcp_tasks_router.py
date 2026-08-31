from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from deerflow_extension_api import EffectiveSubjectV1, InvocationIdentityV1
from fastapi import HTTPException

from app.gateway.app import create_app
from app.gateway.routers import mcp_tasks
from deerflow.config.extensions_config import ExtensionsConfig
from deerflow.runtime.tenant_identity import TenantIdentityV1

_TENANT_IDENTITY = TenantIdentityV1.from_canonical_id("test")


class FakeRepository:
    def __init__(self, rows):
        self.rows = rows
        self.list_calls = []
        self.get_calls = []
        self.tenant = _TENANT_IDENTITY.to_persisted_reference()

    async def list_by_thread(self, thread_id, *, user_id, limit, tenant_digest):
        assert tenant_digest == self.tenant.digest
        self.list_calls.append((thread_id, user_id, limit))
        return list(self.rows)

    async def get(self, task_id, *, user_id, tenant_digest):
        assert tenant_digest == self.tenant.digest
        self.get_calls.append((task_id, user_id))
        return next((row for row in self.rows if row["id"] == task_id and row["user_id"] == user_id), None)


class FakeRunManager:
    def __init__(self, allowed=()):
        self.allowed = frozenset(allowed)
        self.calls = []

    async def get(self, run_id, *, user_id, raise_on_store_error):
        self.calls.append((run_id, user_id, raise_on_store_error))
        return SimpleNamespace(run_id=run_id) if run_id in self.allowed else None


def _record(**overrides):
    return {
        "id": "mcp-task-1",
        "user_id": "user-1",
        "thread_id": "thread-1",
        "task_name": "report-generation",
        "status": "working",
        "created_at": "2026-08-05T00:00:00+00:00",
        "updated_at": "2026-08-05T00:00:05+00:00",
        "last_polled_at": "2026-08-05T00:00:05+00:00",
        "error": None,
        "last_poll_error": "temporary network failure",
        "consecutive_poll_error_count": 3,
        "last_cancel_error": None,
        "cancel_attempt_count": 0,
        "result": None,
        "result_preview": None,
        "result_truncated": False,
        "result_artifact": None,
        "input_required": None,
        "remote_task_id": "must-not-leak",
        "driver_data": {"status_tool": "must-not-leak"},
        "server_name": "must-not-leak",
        "lineage_status": "legacy_unavailable",
        "lineage": None,
        **overrides,
    }


def _request(repo):
    return SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                mcp_task_repo=repo,
                mcp_task_service=SimpleNamespace(tracking_degraded_after_errors=3),
            )
        )
    )


def test_gateway_mounts_thread_scoped_mcp_task_routes() -> None:
    paths = {route.path for route in create_app().routes}
    assert "/api/threads/{thread_id}/mcp-tasks" in paths
    assert "/api/threads/{thread_id}/mcp-tasks/{task_id}" in paths


@pytest.mark.asyncio
async def test_list_returns_only_safe_current_user_thread_fields(monkeypatch) -> None:
    repo = FakeRepository([_record()])
    monkeypatch.setattr(mcp_tasks, "get_current_user", AsyncMock(return_value="user-1"))

    response = await mcp_tasks.list_mcp_tasks.__wrapped__(
        thread_id="thread-1",
        request=_request(repo),
        limit=25,
    )

    assert repo.list_calls == [("thread-1", "user-1", 25)]
    assert response == [
        {
            "task_id": "mcp-task-1",
            "task_name": "report-generation",
            "status": "working",
            "created_at": "2026-08-05T00:00:00+00:00",
            "updated_at": "2026-08-05T00:00:05+00:00",
            "error": None,
            "tracking_degraded": True,
            "cancel_requested": False,
            "lineage": {"status": "legacy_unavailable"},
        }
    ]


@pytest.mark.asyncio
async def test_detail_exposes_bounded_result_but_not_remote_handle(monkeypatch) -> None:
    repo = FakeRepository(
        [
            _record(
                status="completed",
                result={"report": "ready"},
                result_artifact={"uri": "s3://reports/1.json", "mime_type": "application/json"},
                last_cancel_error="c" * 600,
                cancel_attempt_count=4,
                notification_status="retry",
                notification_error="n" * 600,
                notification_attempt_count=3,
            )
        ]
    )
    monkeypatch.setattr(mcp_tasks, "get_current_user", AsyncMock(return_value="user-1"))

    response = await mcp_tasks.get_mcp_task.__wrapped__(
        thread_id="thread-1",
        task_id="mcp-task-1",
        request=_request(repo),
    )

    assert response["result"] == {"report": "ready"}
    assert response["result_artifact"]["uri"] == "s3://reports/1.json"
    assert response["last_cancel_error"] == "c" * 500
    assert response["cancel_attempt_count"] == 4
    assert response["notification_status"] == "retry"
    assert response["notification_error"] == "n" * 500
    assert response["notification_attempt_count"] == 3
    assert "remote_task_id" not in response
    assert "driver_data" not in response
    assert "server_name" not in response
    assert response["lineage"] == {"status": "legacy_unavailable"}
    assert response["links"] == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("allowed", "expected"),
    [
        ((), {}),
        (("run-parent",), {"parent_run_id": "run-parent"}),
        (("run-notification",), {"notification_run_id": "run-notification"}),
        (
            ("run-parent", "run-notification"),
            {
                "parent_run_id": "run-parent",
                "notification_run_id": "run-notification",
            },
        ),
    ],
)
async def test_detail_links_require_independent_run_authorization(
    monkeypatch,
    allowed,
    expected,
) -> None:
    repo = FakeRepository(
        [
            _record(
                parent_run_id="run-parent",
                notification_run_id="run-notification",
            )
        ]
    )
    request = _request(repo)
    manager = FakeRunManager(allowed)
    request.app.state.run_manager = manager
    monkeypatch.setattr(mcp_tasks, "get_current_user", AsyncMock(return_value="user-1"))

    response = await mcp_tasks.get_mcp_task.__wrapped__(
        thread_id="thread-1",
        task_id="mcp-task-1",
        request=request,
    )

    assert response["links"] == expected
    assert manager.calls == [
        ("run-parent", "user-1", True),
        ("run-notification", "user-1", True),
    ]


@pytest.mark.asyncio
async def test_parent_visibility_does_not_grant_task_visibility(monkeypatch) -> None:
    repo = FakeRepository([_record(parent_run_id="run-parent")])
    request = _request(repo)
    manager = FakeRunManager(("run-parent",))
    request.app.state.run_manager = manager
    monkeypatch.setattr(mcp_tasks, "get_current_user", AsyncMock(return_value="user-2"))

    with pytest.raises(HTTPException) as exc_info:
        await mcp_tasks.get_mcp_task.__wrapped__(
            thread_id="thread-1",
            task_id="mcp-task-1",
            request=request,
        )

    assert exc_info.value.status_code == 404
    assert manager.calls == []


@pytest.mark.asyncio
async def test_standalone_create_ignores_forged_lineage_and_binds_authenticated_values(
    monkeypatch,
) -> None:
    repo = FakeRepository([])
    service = AsyncMock()
    service.tracking_degraded_after_errors = 3

    async def submit(**kwargs):
        task_request = kwargs["request"]
        return _record(
            id="mcp-task-created",
            status="submitted",
            lineage_status="verified",
            lineage=task_request.lineage.to_persisted_json(),
            parent_run_id=None,
            notification_run_id=None,
        )

    service.submit.side_effect = submit
    request = _request(repo)
    request.state = SimpleNamespace()
    request.app.state.mcp_task_service = service
    request.app.state.mcp_tasks_available = True
    request.app.state.tenant_identity = _TENANT_IDENTITY
    request.app.state.capability_manifest = SimpleNamespace(
        extension_generation=4,
        digest="a" * 64,
    )
    request.app.state.mcp_task_extensions_config = ExtensionsConfig.model_validate(
        {
            "mcpServers": {
                "reports": {
                    "type": "stdio",
                    "command": "reports-mcp",
                    "task_toolsets": [
                        {
                            "name": "report-generation",
                            "submit_tool": "submit_report",
                            "status_tool": "status_report",
                            "cancel_tool": "cancel_report",
                        }
                    ],
                }
            }
        }
    )
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(
            kind="human",
            subject_id="user-1",
            role="member",
        )
    )
    monkeypatch.setattr(mcp_tasks, "get_current_user", AsyncMock(return_value="user-1"))
    monkeypatch.setattr(
        mcp_tasks,
        "invocation_principal_from_request",
        AsyncMock(return_value=SimpleNamespace(identity=identity)),
    )
    body = mcp_tasks.CreateMcpTaskBody.model_validate(
        {
            "server_name": "reports",
            "task_name": "report-generation",
            "arguments": {"topic": "MCP"},
            "idempotency_key": "client-key-1",
            "lineage": {"kind": "agent_tool"},
            "tenant": "forged",
            "parent_run_id": "forged-run",
            "parent_tool_receipt_id": "tr_" + "f" * 64,
            "credential_selector_ref": "f" * 64,
        }
    )

    response = await mcp_tasks.create_mcp_task.__wrapped__(
        thread_id="thread-1",
        body=body,
        request=request,
    )

    submitted = service.submit.await_args.kwargs["request"]
    assert submitted.lineage.kind == "standalone_api"
    assert submitted.lineage.tenant == repo.tenant
    assert submitted.lineage.parent_run_id is None
    assert submitted.lineage.parent_tool_receipt_id is None
    assert submitted.lineage.principal_ref.startswith("principal-")
    assert submitted.local_task_id.startswith("mcp-task-")
    assert "client-key-1" not in submitted.local_task_id
    assert response["lineage"]["kind"] == "standalone_api"

    await mcp_tasks.create_mcp_task.__wrapped__(
        thread_id="thread-2",
        body=body,
        request=request,
    )
    other_thread = service.submit.await_args.kwargs["request"]
    assert other_thread.local_task_id != submitted.local_task_id
    assert other_thread.lineage.digest != submitted.lineage.digest


@pytest.mark.asyncio
async def test_detail_rejects_cross_user_and_cross_thread_access(monkeypatch) -> None:
    repo = FakeRepository([_record()])
    request = _request(repo)

    monkeypatch.setattr(mcp_tasks, "get_current_user", AsyncMock(return_value="user-2"))
    with pytest.raises(HTTPException) as cross_user:
        await mcp_tasks.get_mcp_task.__wrapped__(
            thread_id="thread-1",
            task_id="mcp-task-1",
            request=request,
        )
    assert cross_user.value.status_code == 404

    monkeypatch.setattr(mcp_tasks, "get_current_user", AsyncMock(return_value="user-1"))
    with pytest.raises(HTTPException) as cross_thread:
        await mcp_tasks.get_mcp_task.__wrapped__(
            thread_id="thread-2",
            task_id="mcp-task-1",
            request=request,
        )
    assert cross_thread.value.status_code == 404


@pytest.mark.asyncio
async def test_cancel_uses_service_with_exact_user_and_thread_scope(monkeypatch) -> None:
    repo = FakeRepository([_record()])
    service = AsyncMock()
    service.tracking_degraded_after_errors = 3
    service.cancel_task.return_value = _record(status="working", cancel_requested_at="2026-08-05T00:00:06+00:00")
    request = _request(repo)
    request.app.state.mcp_task_service = service
    request.app.state.mcp_tasks_available = True
    monkeypatch.setattr(mcp_tasks, "get_current_user", AsyncMock(return_value="user-1"))

    response = await mcp_tasks.cancel_mcp_task.__wrapped__(
        thread_id="thread-1",
        task_id="mcp-task-1",
        request=request,
    )

    service.cancel_task.assert_awaited_once_with(
        task_id="mcp-task-1",
        thread_id="thread-1",
        user_id="user-1",
        reason_code="user_api",
    )
    assert response["status"] == "working"
    assert response["cancel_requested"] is True


@pytest.mark.asyncio
async def test_cancel_rejected_when_worker_not_running(monkeypatch) -> None:
    repo = FakeRepository([_record()])
    service = AsyncMock()
    service.tracking_degraded_after_errors = 3
    request = _request(repo)
    request.app.state.mcp_task_service = service
    request.app.state.mcp_tasks_available = False
    monkeypatch.setattr(mcp_tasks, "get_current_user", AsyncMock(return_value="user-1"))

    with pytest.raises(HTTPException) as excinfo:
        await mcp_tasks.cancel_mcp_task.__wrapped__(
            thread_id="thread-1",
            task_id="mcp-task-1",
            request=request,
        )

    assert excinfo.value.status_code == 503
    service.cancel_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_rejected_when_availability_flag_missing(monkeypatch) -> None:
    repo = FakeRepository([_record()])
    service = AsyncMock()
    service.tracking_degraded_after_errors = 3
    request = _request(repo)
    request.app.state.mcp_task_service = service
    monkeypatch.setattr(mcp_tasks, "get_current_user", AsyncMock(return_value="user-1"))

    with pytest.raises(HTTPException) as excinfo:
        await mcp_tasks.cancel_mcp_task.__wrapped__(
            thread_id="thread-1",
            task_id="mcp-task-1",
            request=request,
        )

    assert excinfo.value.status_code == 503
    service.cancel_task.assert_not_awaited()
