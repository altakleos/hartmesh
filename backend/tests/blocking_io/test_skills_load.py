"""Regression test: skill loading must remain releasable to a worker thread.

Anchors the production offload from `subagents/executor.py:_load_skills`,
where both `get_or_new_skill_storage` and the sync `storage.load_skills(...)`
method are dispatched via `asyncio.to_thread`. That fix addressed #1917,
where `os.walk` inside `load_skills` blocked the LangGraph async event loop.

This test invokes the production `_load_skills()` call path under the strict
Blockbuster context against a real `LocalSkillStorage` instance pointed at
a tmp directory. If the production `asyncio.to_thread` offload is removed,
Blockbuster raises `BlockingError` and this test fails.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.asyncio

_MISSING = object()
_EXECUTOR_IMPORT_MOCKS = (
    "deerflow.agents",
    "deerflow.agents.thread_state",
    "deerflow.models",
)


def _seed_skill(skills_root: Path) -> None:
    skill = skills_root / "public" / "demo"
    skill.mkdir(parents=True, exist_ok=True)
    (skill / "SKILL.md").write_text(
        "---\nname: demo\ndescription: regression-test skill\n---\n# demo\n",
        encoding="utf-8",
    )


@contextmanager
def _real_subagent_executor() -> Iterator[type]:
    """Import the real executor despite the suite-level circular-import mock."""
    original_modules = {name: sys.modules.get(name, _MISSING) for name in _EXECUTOR_IMPORT_MOCKS}
    original_executor = sys.modules.get("deerflow.subagents.executor", _MISSING)
    parent_module = sys.modules.get("deerflow.subagents")
    original_parent_executor = getattr(parent_module, "executor", _MISSING) if parent_module is not None else _MISSING

    sys.modules.pop("deerflow.subagents.executor", None)
    for name in _EXECUTOR_IMPORT_MOCKS:
        sys.modules[name] = MagicMock()

    try:
        executor_module = importlib.import_module("deerflow.subagents.executor")
        yield executor_module.SubagentExecutor
    finally:
        if original_executor is _MISSING:
            sys.modules.pop("deerflow.subagents.executor", None)
        else:
            sys.modules["deerflow.subagents.executor"] = original_executor

        if parent_module is not None:
            if original_parent_executor is _MISSING:
                try:
                    delattr(parent_module, "executor")
                except AttributeError:
                    pass
            else:
                parent_module.executor = original_parent_executor

        for name, module in original_modules.items():
            if module is _MISSING:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


async def test_load_skills_via_to_thread_does_not_block_event_loop(tmp_path: Path) -> None:
    from deerflow.config.skills_config import SkillsConfig
    from deerflow.subagents.config import SubagentConfig

    _seed_skill(tmp_path)

    with _real_subagent_executor() as SubagentExecutor:
        executor = SubagentExecutor(
            config=SubagentConfig(
                name="demo",
                description="Loads skills through the production async path.",
            ),
            tools=[],
            app_config=SimpleNamespace(skills=SkillsConfig(path=str(tmp_path))),
            parent_model="test-model",
        )

        skills = await executor._load_skills()

    assert isinstance(skills, list)
    assert any(s.name == "demo" for s in skills)


async def test_scheduler_submit_failure_offloads_material_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rejected background dispatch must not clean leases on the event loop."""
    from deerflow.subagents.config import SubagentConfig

    loop = asyncio.get_running_loop()
    event_loop_thread = threading.get_ident()
    cleanup_done = asyncio.Event()
    cleanup_threads: list[int] = []

    with _real_subagent_executor() as SubagentExecutor:
        executor_module = sys.modules[SubagentExecutor.__module__]
        executor = SubagentExecutor(
            config=SubagentConfig(
                name="demo",
                description="Exercises rejected background dispatch cleanup.",
            ),
            tools=[],
            app_config=SimpleNamespace(skills=SimpleNamespace(deferred_discovery=True)),
            parent_model="test-model",
        )

        def blocking_cleanup() -> None:
            (tmp_path / "material-released").write_text("released", encoding="utf-8")
            cleanup_threads.append(threading.get_ident())
            loop.call_soon_threadsafe(cleanup_done.set)

        def reject_submit(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("scheduler rejected task")

        monkeypatch.setattr(executor, "_release_owned_resolved_agent_material", blocking_cleanup)
        monkeypatch.setattr(executor_module, "_submit_to_isolated_loop_in_context", reject_submit)

        with pytest.raises(RuntimeError, match="scheduler rejected task"):
            executor.execute_async("task", task_id="scheduler-rejected")

        await asyncio.wait_for(cleanup_done.wait(), timeout=2)

        assert "scheduler-rejected" not in executor_module._background_tasks

    assert cleanup_threads and cleanup_threads[0] != event_loop_thread


async def test_scheduler_rejection_releases_byte_lease_when_projection_cleanup_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Projection cleanup failure must not strand the byte-snapshot lease."""
    from deerflow.subagents.config import SubagentConfig

    loop = asyncio.get_running_loop()
    byte_lease_released = asyncio.Event()

    class RetainedMaterial:
        def __init__(self) -> None:
            self.release_calls = 0

        def release_process_material(self) -> None:
            self.release_calls += 1
            loop.call_soon_threadsafe(byte_lease_released.set)

    with _real_subagent_executor() as SubagentExecutor:
        executor_module = sys.modules[SubagentExecutor.__module__]
        sandbox_provider = importlib.import_module("deerflow.sandbox.sandbox_provider")
        retained_material = RetainedMaterial()
        executor = SubagentExecutor(
            config=SubagentConfig(
                name="demo",
                description="Exercises independent rejected-dispatch cleanup.",
            ),
            tools=[],
            app_config=SimpleNamespace(skills=SimpleNamespace(deferred_discovery=True)),
            parent_model="test-model",
        )
        executor.resolved_agent_material = retained_material
        executor._owns_resolved_agent_material = True
        executor.skill_projection_token = object()
        executor._owns_skill_projection_token = True

        def fail_projection_cleanup(_token: object) -> None:
            raise RuntimeError("secret projection cleanup detail")

        def reject_submit(*_args, **_kwargs) -> None:
            raise RuntimeError("scheduler rejected task")

        monkeypatch.setattr(
            sandbox_provider,
            "release_accepted_skill_consumer",
            fail_projection_cleanup,
        )
        monkeypatch.setattr(executor_module, "_submit_to_isolated_loop_in_context", reject_submit)

        with caplog.at_level(logging.WARNING, logger=executor_module.__name__):
            with pytest.raises(RuntimeError, match="scheduler rejected task"):
                executor.execute_async("task", task_id="projection-cleanup-failed")

            await asyncio.wait_for(byte_lease_released.wait(), timeout=1)

            async def cleanup_failure_was_logged() -> None:
                while not any(record.message == "Subagent material cleanup failed after submission rejection" for record in caplog.records):
                    await asyncio.sleep(0)

            await asyncio.wait_for(cleanup_failure_was_logged(), timeout=1)

        assert retained_material.release_calls == 1
        assert executor._owns_resolved_agent_material is False
        assert executor._owns_skill_projection_token is False
        assert "projection-cleanup-failed" not in executor_module._background_tasks
        assert "secret projection cleanup detail" not in caplog.text
