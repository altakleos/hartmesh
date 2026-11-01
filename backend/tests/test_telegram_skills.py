"""A slash word in a chat is looked up in the owner's own skills."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from app.telegram_bots import skills as module
from deerflow.config.paths import Paths


def link(kind="lead_agent", ref="lead_agent"):
    return SimpleNamespace(id="link", owner_user_id="owner-a", agent_kind=kind, agent_ref=ref)


def storage(monkeypatch, *found, failure=None):
    owners = []

    class Storage:
        def load_skills(self, *, enabled_only):
            assert enabled_only is False
            if failure:
                raise failure
            return [SimpleNamespace(name=name, enabled=enabled) for name, enabled in found]

    def read(owner):
        owners.append(owner)
        return Storage()

    monkeypatch.setattr(module, "get_or_new_user_skill_storage", read)
    return owners


def agent(monkeypatch, listed):
    calls = []

    def load(name, *, user_id):
        calls.append((name, user_id))
        return None if listed == "gone" else SimpleNamespace(skills=listed)

    monkeypatch.setattr(module, "load_agent_config", load)
    return calls


def test_an_enabled_skill_runs_from_the_owners_storage(monkeypatch):
    owners = storage(monkeypatch, ("weekly-report", True))
    assert module.slash_skill_outcome(link(), "/weekly-report for March") == "run"
    assert owners == ["owner-a"]


def test_a_skill_that_is_switched_off_says_so(monkeypatch):
    storage(monkeypatch, ("weekly-report", False))
    assert module.slash_skill_outcome(link(), "/weekly-report") == "off"


@pytest.mark.parametrize("text", ["/no-such-skill", "/help", "/memory", "/Weekly", "/weekly report@", "plain text"])
def test_a_word_that_names_no_skill_gives_nothing(monkeypatch, text):
    storage(monkeypatch, ("weekly-report", True))
    assert module.slash_skill_outcome(link(), text) is None


def test_a_custom_agent_runs_only_the_skills_it_lists(monkeypatch):
    storage(monkeypatch, ("weekly-report", True), ("other", True))
    target = link("custom_agent", "helper")
    calls = agent(monkeypatch, ["weekly-report"])
    assert module.slash_skill_outcome(target, "/weekly-report") == "run"
    assert module.slash_skill_outcome(target, "/other") == "unavailable"
    assert calls == [("helper", "owner-a"), ("helper", "owner-a")]


@pytest.mark.parametrize("listed", [None])
def test_a_custom_agent_with_no_list_has_every_skill(monkeypatch, listed):
    storage(monkeypatch, ("weekly-report", True))
    agent(monkeypatch, listed)
    assert module.slash_skill_outcome(link("custom_agent", "helper"), "/weekly-report") == "run"


def test_a_switched_off_skill_is_off_before_the_agents_list_is_read(monkeypatch):
    storage(monkeypatch, ("weekly-report", False))
    calls = agent(monkeypatch, [])
    assert module.slash_skill_outcome(link("custom_agent", "helper"), "/weekly-report") == "off"
    assert calls == []


def test_no_slash_word_is_a_skill_on_an_agents_bot(monkeypatch):
    owners = storage(monkeypatch, ("weekly-report", True))
    assert module.slash_skill_outcome(link("instance", "a" * 32), "/weekly-report") is None
    assert owners == []


def test_a_deleted_custom_agent_is_a_failed_lookup(monkeypatch):
    storage(monkeypatch, ("weekly-report", True))

    def gone(name, *, user_id):
        raise FileNotFoundError(name)

    monkeypatch.setattr(module, "load_agent_config", gone)
    assert module.slash_skill_outcome(link("custom_agent", "helper"), "/weekly-report") == "failed"


def test_the_real_catalog_knows_a_shipped_skill(monkeypatch, tmp_path):
    shipped = Path(__file__).resolve().parents[2] / "skills"
    skills = SimpleNamespace(get_skills_path=lambda: shipped, container_path="/mnt/skills", use="deerflow.skills.storage.local_skill_storage:LocalSkillStorage")
    monkeypatch.setattr("deerflow.config.get_app_config", lambda: SimpleNamespace(skills=skills))
    monkeypatch.setattr("deerflow.config.paths._paths", Paths(base_dir=tmp_path))
    assert module.slash_skill_outcome(link(), "/web-design-guidelines review this") in {"run", "off"}
    assert module.slash_skill_outcome(link(), "/no-such-skill-at-all") is None


def test_a_failed_read_says_so_and_names_no_detail(monkeypatch, caplog):
    storage(monkeypatch, failure=OSError("/secret/path"))
    assert module.slash_skill_outcome(link(), "/weekly-report") == "failed"
    assert "OSError" in caplog.text
    assert "/secret/path" not in caplog.text
