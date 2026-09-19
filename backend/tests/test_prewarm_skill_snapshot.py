"""The snapshot a prewarm guesses is the one a default turn computes.

A prewarm runs before the person has typed, so it cannot know the run's
config. What it can know is the snapshot the ordinary first turn brings: the
default agent, no subagents, every enabled skill. This pins that the guess
and ``resolve_agent_revision`` agree byte for byte on that turn, and that a
turn shaped differently (subagents on, a named agent) is allowed to differ --
the guess then simply fails the in-place verification on the tenant, which
is the slow path the turn takes today.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from test_accepted_skill_snapshots import _parsed_skill, _write_skill

from deerflow.config.app_config import AppConfig
from deerflow.config.paths import Paths
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.runtime import agent_revision as revision_module
from deerflow.runtime.agent_revision import likely_first_turn_skill_snapshot, resolve_agent_revision
from deerflow.runtime.skill_snapshot import cleanup_abandoned_skill_snapshots

USER = "user-1"


@pytest.fixture
def snapshot_paths(monkeypatch, tmp_path: Path):
    """Keep process-local snapshot resources inside each test directory."""
    from deerflow.runtime import skill_snapshot as snapshot_module

    paths = Paths(tmp_path / "state")
    monkeypatch.setattr(snapshot_module, "get_paths", lambda: paths)
    yield paths
    cleanup_abandoned_skill_snapshots()


@pytest.fixture
def two_enabled_skills(tmp_path: Path, monkeypatch):
    first = _parsed_skill(_write_skill(tmp_path / "a", body="alpha", name="alpha"))
    second = _parsed_skill(_write_skill(tmp_path / "b", body="beta", name="beta"))
    monkeypatch.setattr(revision_module, "_skills", lambda _app_config, *, user_id: ((first, second), (first, second)))
    monkeypatch.setattr(revision_module, "load_agent_soul", lambda *_a, **_kw: "")
    return first, second


def _app_config() -> AppConfig:
    return AppConfig(sandbox=SandboxConfig(use="test"))


def test_the_guess_is_the_default_turns_snapshot(snapshot_paths, two_enabled_skills):
    guess = likely_first_turn_skill_snapshot(_app_config(), user_id=USER)
    try:
        turn = resolve_agent_revision({"configurable": {}}, app_config=_app_config(), user_id=USER)
        assert guess is not None
        assert turn.material.skill_snapshot is not None
        assert guess.snapshot_id == turn.material.skill_snapshot.snapshot_id
    finally:
        if guess is not None:
            guess.release()


def test_no_enabled_skills_guesses_nothing(snapshot_paths, monkeypatch):
    monkeypatch.setattr(revision_module, "_skills", lambda _app_config, *, user_id: ((), ()))

    assert likely_first_turn_skill_snapshot(_app_config(), user_id=USER) is None


def test_the_guess_holds_a_lease_the_caller_releases(snapshot_paths, two_enabled_skills):
    from deerflow.runtime import skill_snapshot as snapshot_module

    guess = likely_first_turn_skill_snapshot(_app_config(), user_id=USER)
    assert guess is not None
    assert snapshot_module._lease_counts.get(guess.root, 0) == 1
    guess.release()
    assert snapshot_module._lease_counts.get(guess.root, 0) == 0
