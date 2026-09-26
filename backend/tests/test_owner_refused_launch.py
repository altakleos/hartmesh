"""A launch refused because its owner is turned off says so in its type, so queued work can end instead of retrying.

A due schedule, a task notification and a channel message all launch through
the owner's principal projection, which refuses a turned-off owner. A retry
of that work after the owner is enabled again would run something queued
before they were turned off, so each caller ends the work on this refusal
rather than treating it as a transient failure. The refusal stays a
``ValueError`` for every caller that already handles one.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.gateway import services
from app.gateway.auth_disabled import AUTH_SOURCE_INTERNAL
from app.runtime.invocation import InternalLaunchIntent, InternalSourceKind, OwnerRefusedLaunchError
from deerflow.runtime.tenant_identity import TenantIdentityV1


def _refused_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    async def resolve_owner(_request, _owner_user_id):
        return SimpleNamespace(id="owner-1", system_role="user", oauth_provider="sso", oauth_id="sub-1", needs_setup=False, disabled_at="2026-09-26T00:00:00+00:00")

    monkeypatch.setattr(services, "resolve_trusted_internal_owner_for_attribution", resolve_owner)


def _internal_request() -> SimpleNamespace:
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(tenant_identity=TenantIdentityV1.from_canonical_id("local"))),
        headers={services.INTERNAL_OWNER_USER_ID_HEADER_NAME: "owner-1"},
        state=SimpleNamespace(user=SimpleNamespace(id="internal", system_role="internal"), auth_source=AUTH_SOURCE_INTERNAL),
        cookies={},
    )


def test_the_refusal_is_its_own_type_and_still_a_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _refused_owner(monkeypatch)
    intent = InternalLaunchIntent(thread_id="thread-1", source_kind=InternalSourceKind.scheduled_task, owner_user_id="owner-1", trusted_task_id="task-1", task_run_id="occurrence-1")

    with pytest.raises(OwnerRefusedLaunchError, match="account is disabled") as refused:
        asyncio.run(services._principal_projection_for_intent(_internal_request(), intent, owner_user_id="owner-1"))

    assert isinstance(refused.value, ValueError)


def test_a_keyed_internal_launch_keeps_the_refusal_instead_of_answering_it_as_a_bad_request(monkeypatch: pytest.MonkeyPatch) -> None:
    """A task notification is a keyed HTTP-kind launch; its other refusals become a 422, this one must not."""
    _refused_owner(monkeypatch)
    normalizer = services._GatewayLaunchNormalizer(_internal_request())
    intent = InternalLaunchIntent(thread_id="thread-1", external_key="mcp-task-notification:" + "a" * 64, metadata={"mcp_task_notification": {}}, trusted_notification=True)

    with pytest.raises(OwnerRefusedLaunchError):
        asyncio.run(normalizer.identify(intent))


def test_other_keyed_refusals_are_still_answered_as_a_bad_request(monkeypatch: pytest.MonkeyPatch) -> None:
    async def no_owner(_request, _owner_user_id):
        return None

    monkeypatch.setattr(services, "resolve_trusted_internal_owner_for_attribution", no_owner)
    normalizer = services._GatewayLaunchNormalizer(_internal_request())
    intent = InternalLaunchIntent(thread_id="thread-1", external_key="mcp-task-notification:" + "a" * 64, metadata={"mcp_task_notification": {}}, trusted_notification=True)

    with pytest.raises(HTTPException) as refused:
        asyncio.run(normalizer.identify(intent))
    assert refused.value.status_code == 422


def test_an_internal_http_launch_for_a_refused_owner_is_a_403_not_a_500(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = SimpleNamespace(launch=_raises(OwnerRefusedLaunchError("trusted internal launch owner's account is disabled")))
    monkeypatch.setattr(services, "build_invocation_runtime", lambda _request: runtime)
    monkeypatch.setattr(services, "_launch_intent", lambda *args, **kwargs: SimpleNamespace())

    with pytest.raises(HTTPException) as refused:
        asyncio.run(services.start_run(SimpleNamespace(multitask_strategy="reject", metadata=None, config=None), "thread-1", _internal_request()))

    assert refused.value.status_code == 403
    assert isinstance(refused.value.__cause__, OwnerRefusedLaunchError)


def test_the_notification_launcher_hands_the_refusal_on_by_its_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """So the task loop ends the notification instead of retrying it."""

    async def _start_run(*args, **kwargs):
        raise HTTPException(status_code=403, detail="refused") from OwnerRefusedLaunchError("trusted internal launch owner's account is disabled")

    monkeypatch.setattr(services, "start_run", _start_run)
    source = {
        "version": 1,
        "tenant_digest": "a" * 64,
        "task_id": "task-1",
        "task_lineage_digest": None,
        "lineage_status": "legacy_unavailable",
        "parent_run_id": None,
        "parent_tool_receipt_id": None,
        "terminal_result_version": 1,
        "notification_kind": "terminal",
        "result_digest": "b" * 64,
        "result_status": "completed",
    }

    with pytest.raises(OwnerRefusedLaunchError):
        asyncio.run(
            services.launch_mcp_task_notification_run(
                app=SimpleNamespace(state=SimpleNamespace()),
                thread_id="thread-1",
                assistant_id="lead_agent",
                owner_user_id="owner-1",
                task_id="task-1",
                dispatch_version=1,
                source=source,
                event={"status": "completed"},
            )
        )


def _raises(exc: Exception):
    async def _launch(_intent):
        raise exc

    return _launch
