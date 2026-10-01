"""``disable``, ``enable`` and ``limit-role`` with the scheduler running.

They shipped in ``.35`` and were exercised with the scheduler off, or against a
scheduler service that could launch nothing. The tenant profile now ships the
scheduler on, so these drive the real thing: the Gateway with
``scheduler.enabled`` and the profile's scheduler values, the real
``AccountsCommand`` (what ``python -m app.gateway.auth.accounts`` runs) over the
Gateway's own database, the real ``ScheduledTaskService`` polling, and the real
``InvocationRuntime`` into ``RunManager`` and the lead-agent graph. Only the
model is scripted.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest
import yaml
from _agent_e2e_helpers import FakeToolCallingModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from test_runtime_lifecycle_e2e import _preserve_process_config_singletons, _register_user, _reset_process_singletons

from app.gateway.auth.accounts import AccountsCommand
from app.gateway.auth.models import User

pytestmark = pytest.mark.no_auto_user

REPO_ROOT = Path(__file__).resolve().parents[2]
ISSUER = "https://login.example.com/realms/tenant"

_CONFIG_YAML = """\
log_level: info
models:
  - name: fake-test-model
    display_name: Fake Test Model
    use: langchain_openai:ChatOpenAI
    model: gpt-4o-mini
    api_key: $OPENAI_API_KEY
    base_url: $OPENAI_API_BASE
sandbox:
  use: deerflow.sandbox.local:LocalSandboxProvider
deployment:
  profile: local_development
title:
  enabled: false
memory:
  enabled: false
database:
  backend: sqlite
run_events:
  backend: db
"""


def _scripted_model(hold: threading.Event | None = None) -> FakeToolCallingModel:
    """Answers at once, or holds the run open until the test lets go."""

    class _Model(FakeToolCallingModel):
        def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
            del messages, stop, run_manager, kwargs
            if hold is not None:
                hold.wait(120)
            return ChatResult(generations=[ChatGeneration(message=AIMessage(content="Done."))])

    return _Model(responses=[AIMessage(content="unused")])


def _build_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scheduler: dict[str, Any] | None = None):
    profile = yaml.safe_load((REPO_ROOT / "deploy" / "compose" / "config.yaml").read_text(encoding="utf-8"))
    # The profile's scheduler, running. The poll interval is the test's own: it polls when it wants a poll.
    blocks = {"scheduler": {**profile["scheduler"], "poll_interval_seconds": 300, **(scheduler or {})}}
    home = tmp_path / "deer-flow-home"
    home.mkdir()
    monkeypatch.setenv("DEER_FLOW_HOME", str(home))
    monkeypatch.setenv("OPENAI_API_KEY", "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY")
    monkeypatch.setenv("OPENAI_API_BASE", "https://example.invalid")
    config = tmp_path / "config.yaml"
    config.write_text(_CONFIG_YAML + yaml.safe_dump(blocks), encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(config))
    extensions = tmp_path / "extensions_config.json"
    extensions.write_text('{"mcpServers": {}, "skills": {}}', encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(extensions))
    _preserve_process_config_singletons(monkeypatch)
    _reset_process_singletons(monkeypatch)
    from deerflow.config import app_config as app_config_module

    app_config_module.get_app_config().database.sqlite_dir = str(home / "db")
    from app.gateway.app import create_app

    return create_app()


@pytest.fixture
def live_scheduler_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    return _build_app(tmp_path, monkeypatch)


@pytest.fixture
def live_scheduler_app_with(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """The same Gateway with some of the profile's scheduler values replaced."""
    return lambda **scheduler: _build_app(tmp_path, monkeypatch, scheduler)


class _Deployment:
    """The Gateway and what a deployer's command reaches: one database, one scheduler."""

    def __init__(self, client) -> None:
        self.client = client
        self.state = client.app.state

    def call(self, coroutine_function):
        return self.client.portal.call(coroutine_function)

    @property
    def service(self):
        return self.state.scheduled_task_service

    def command(self) -> AccountsCommand:
        from deerflow.persistence.engine import get_session_factory
        from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository
        from deerflow.persistence.run import RunRepository

        session_factory = get_session_factory()
        tenant = self.state.tenant_identity.to_persisted_reference()
        return AccountsCommand(
            self._users(session_factory),
            tokens=PersonalAccessTokenRepository(session_factory, tenant=tenant),
            schedules=self.state.scheduled_task_repo,
            runs=RunRepository(session_factory, tenant=tenant),
            wait_seconds=5,
        )

    @staticmethod
    def _users(session_factory):
        from app.gateway.auth.repositories.sqlite import SQLiteUserRepository

        return SQLiteUserRepository(session_factory)

    def run(self, command: str, **kwargs) -> dict[str, Any]:
        return self.call(lambda: self.command().run(command, issuer=ISSUER, subject="sub-owner", **kwargs))

    def make_account(self, *, role: str, email: str = "owner@example.com", subject: str = "sub-owner") -> str:
        from deerflow.persistence.engine import get_session_factory

        users = self._users(get_session_factory())
        account = User(email=email, password_hash=None, system_role=role, oauth_provider="sso", oauth_id=subject, oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC))
        return str(self.call(lambda: users.create_user(account)).id)

    def make_task(self, owner_id: str, *, due_in: timedelta = timedelta(days=1), prompt: str = "Send me the weekly numbers") -> str:
        task_id = f"task-{uuid4().hex}"
        repo = self.state.scheduled_task_repo
        self.call(
            lambda: repo.create(
                task_id=task_id,
                user_id=owner_id,
                thread_id=None,
                context_mode="fresh_thread_per_run",
                assistant_id="lead_agent",
                title="Weekly numbers",
                prompt=prompt,
                schedule_type="cron",
                schedule_spec={"cron": "0 9 * * 1"},
                timezone="UTC",
                next_run_at=datetime.now(UTC) + due_in,
            )
        )
        return task_id

    def make_due(self, task_id: str) -> None:
        repo = self.state.scheduled_task_repo

        async def _move() -> None:
            task = await repo.get_internal(task_id)
            await repo.update(task_id, user_id=task["user_id"], updates={"next_run_at": datetime.now(UTC) - timedelta(minutes=1)})

        self.call(_move)

    def poll(self) -> None:
        self.call(lambda: self.service.run_once(now=datetime.now(UTC)))

    def occurrences(self, task_id: str) -> list[dict[str, Any]]:
        return self.call(lambda: self.state.scheduled_task_run_repo.list_by_task(task_id))

    def task(self, task_id: str) -> dict[str, Any]:
        return self.call(lambda: self.state.scheduled_task_repo.get_internal(task_id))

    def wait_for_occurrence(self, task_id: str, status: str, *, timeout: float = 60.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            rows = self.occurrences(task_id)
            if rows and rows[0]["status"] == status:
                return rows[0]
            time.sleep(0.05)
        raise AssertionError(f"the occurrence never reached {status!r}: {self.occurrences(task_id)}")

    def runs_for(self, thread_id: str) -> list[Any]:
        from deerflow.persistence.engine import get_session_factory
        from deerflow.persistence.run import RunRepository

        tenant = self.state.tenant_identity.to_persisted_reference()
        return self.call(lambda: RunRepository(get_session_factory(), tenant=tenant).list_by_thread(thread_id, user_id=None))


def _deployment(app, *, hold: threading.Event | None = None, model: FakeToolCallingModel | None = None):
    from starlette.testclient import TestClient

    class _Context:
        def __enter__(self) -> _Deployment:
            scripted = model if model is not None else _scripted_model(hold)
            self.model_patch = patch("deerflow.agents.lead_agent.agent.create_chat_model", new=lambda *args, **kwargs: scripted)
            self.model_patch.start()
            self.client = TestClient(app)
            self.client.__enter__()
            return _Deployment(self.client)

        def __exit__(self, *exc) -> None:
            if hold is not None:
                hold.set()
            self.client.__exit__(*exc)
            self.model_patch.stop()

    return _Context()


def test_a_held_schedule_does_not_fire_at_its_next_due_time_and_stays_held_after_enable(live_scheduler_app):
    with _deployment(live_scheduler_app) as gateway:
        _register_user(gateway.client, email="observer@example.com")
        assert gateway.client.get("/api/scheduler").json()["state"] == "running"
        owner = gateway.make_account(role="user")
        held = gateway.make_task(owner)
        control = gateway.make_task(owner)

        document = gateway.run("disable")
        assert set(document["held"]["schedules"]) == {held, control}
        # Another person's schedule comes due in the same poll: the poller is running, and it claims that one.
        bystander = gateway.make_task(gateway.make_account(role="user", email="bystander@example.com", subject="sub-bystander"))
        gateway.make_due(bystander)
        gateway.make_due(held)
        gateway.poll()
        gateway.wait_for_occurrence(bystander, "success")
        assert gateway.occurrences(held) == [], "a held schedule was not claimed at its due time, in the poll that claimed another's"
        assert gateway.task(held)["status"] == "paused"

        # Enable withdraws the refusal and revives nothing: still held, still not firing.
        enabled = gateway.run("enable")
        assert enabled["held"]["schedules"] and set(enabled["restored"]["schedules"]) == set()
        gateway.poll()
        assert gateway.occurrences(held) == []
        assert gateway.task(held)["status"] == "paused"

        # The poller is not what stopped it, and the owner is not still refused: a schedule the owner makes after
        # the enable fires at its due time, and one the deployer restores fires at its next due time.
        fresh = gateway.make_task(owner)
        gateway.make_due(fresh)
        gateway.poll()
        gateway.wait_for_occurrence(fresh, "success")
        restorable = gateway.make_task(owner)
        gateway.run("disable")
        assert gateway.task(restorable)["status"] == "paused"
        gateway.run("enable", restore_held=True)
        assert gateway.task(restorable)["status"] == "enabled"
        gateway.make_due(restorable)
        gateway.poll()
        gateway.wait_for_occurrence(restorable, "success")
        assert gateway.occurrences(held) == [], "what the first disable held is still held"


def test_an_occurrence_claimed_just_before_the_disable_does_not_run(live_scheduler_app):
    with _deployment(live_scheduler_app) as gateway:
        owner = gateway.make_account(role="user")
        task_id = gateway.make_task(owner)
        gateway.make_due(task_id)
        launch = gateway.service._invocation_runtime.launch
        launched: list[Any] = []

        async def _disable_then_launch(intent):
            # The scheduler has claimed the due task and its occurrence; the deployer's command lands now.
            await gateway.command().run("disable", issuer=ISSUER, subject="sub-owner")
            launched.append(intent)
            return await launch(intent)

        gateway.service._invocation_runtime.launch = _disable_then_launch
        gateway.poll()

        [occurrence] = gateway.occurrences(task_id)
        assert launched, "the launch was attempted after the disable"
        assert occurrence["status"] == "failed", occurrence
        assert occurrence["error"] == "trusted internal launch owner's account is disabled", occurrence
        assert occurrence["run_id"] is None
        assert gateway.runs_for(occurrence["thread_id"]) == [], "no run was created, so no model call and no sandbox"
        # It ends and is not retried: the next poll launches nothing for it.
        gateway.poll()
        assert len(gateway.occurrences(task_id)) == 1


def test_an_occurrence_waiting_behind_a_running_one_when_the_owner_is_disabled_never_runs(live_scheduler_app):
    hold = threading.Event()
    with _deployment(live_scheduler_app, hold=hold) as gateway:
        owner = gateway.make_account(role="user")
        other = gateway.make_account(role="user", email="other@example.com", subject="sub-other")
        running_task, waiting_task = gateway.make_task(other), gateway.make_task(owner)
        gateway.make_due(running_task)
        gateway.poll()
        gateway.wait_for_occurrence(running_task, "running")
        gateway.make_due(waiting_task)
        gateway.poll()
        assert gateway.occurrences(waiting_task)[0]["status"] == "queued", "one run at a time: the second waits"

        gateway.run("disable")
        hold.set()
        gateway.wait_for_occurrence(running_task, "success")
        gateway.poll()

        [waiting] = gateway.occurrences(waiting_task)
        assert waiting["status"] == "interrupted", waiting
        assert waiting["run_id"] is None
        assert gateway.runs_for(waiting["thread_id"]) == []


def test_a_demoted_administrators_scheduled_task_runs_as_user_and_an_undemoted_ones_as_admin(live_scheduler_app, monkeypatch):
    import app.gateway.services as services

    seen: dict[str, set[str | None]] = {}
    original = services._principal_projection_for_intent

    async def _observe(request, intent, *, owner_user_id):
        projection = await original(request, intent, owner_user_id=owner_user_id)
        seen.setdefault(str(owner_user_id), set()).add(projection.role)
        return projection

    monkeypatch.setattr(services, "_principal_projection_for_intent", _observe)
    with _deployment(live_scheduler_app) as gateway:
        demoted = gateway.make_account(role="admin", email="demoted@example.com", subject="sub-owner")
        untouched = gateway.make_account(role="admin", email="untouched@example.com", subject="sub-untouched")
        demoted_task, untouched_task = gateway.make_task(demoted), gateway.make_task(untouched)
        limit = gateway.run("limit-role")
        assert limit["command"] == "limit-role"
        gateway.make_due(demoted_task)
        gateway.make_due(untouched_task)

        # One task is claimed per poll on this profile.
        gateway.poll()
        gateway.wait_for_occurrence(demoted_task, "success")
        gateway.make_due(untouched_task)
        gateway.poll()
        gateway.wait_for_occurrence(untouched_task, "success")

        # The control (a limit-free administrator) runs as admin, so the observation can tell the two apart.
        assert seen == {demoted: {"user"}, untouched: {"admin"}}, seen
