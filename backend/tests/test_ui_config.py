"""The deployment's say over what a workspace shows.

`ui.profile` decides whether the developer surfaces are offered to people who
are not administrators, and `ui.starters` is the list Home puts in front of
someone who has not typed anything yet. Both are presentation: hiding a screen
is not authorization — the routes behind them are unchanged and `authorization`
has no permission covering them; `system_role` is what limits a person.
"""

import json

import pytest
import yaml
from pydantic import ValidationError

from deerflow.config.app_config import AppConfig
from deerflow.config.ui_config import MAX_STARTER_TITLE_CHARS, MAX_STARTERS, StarterConfig, UiConfig


def test_a_deployment_that_says_nothing_keeps_every_surface() -> None:
    # The default has to be the behaviour every existing deployment already
    # has, or an upgrade would quietly take screens away from its users.
    assert UiConfig().profile == "developer"


def test_a_developer_deployment_keeps_the_home_it_already_had() -> None:
    # Adding tiles to every existing install is the upgrade-safety rule with
    # its sign flipped: a deployment that said nothing gets no grid.
    assert UiConfig().starters == []


def test_business_opens_on_something_without_being_asked_twice() -> None:
    starters = UiConfig(profile="business").starters

    assert starters, "one word should be enough to get a finished Home"
    assert len(starters) <= MAX_STARTERS
    assert len({starter.id for starter in starters}) == len(starters)
    for starter in starters:
        assert starter.title.strip()
        assert starter.prompt.strip()


def test_only_the_two_profiles_are_profiles() -> None:
    with pytest.raises(ValidationError):
        UiConfig(profile="kiosk")


def test_a_starter_needs_words_to_be_a_starter() -> None:
    for blank in ("", "   "):
        with pytest.raises(ValidationError):
            StarterConfig(id="review", title=blank, prompt="Do the thing")
        with pytest.raises(ValidationError):
            StarterConfig(id="review", title="Review", prompt=blank)


def test_a_starter_id_is_an_identifier() -> None:
    # It is a React key and a test handle; a free-form string is neither.
    for bad in ("Monthly Review", "review!", "", "-review"):
        with pytest.raises(ValidationError):
            StarterConfig(id=bad, title="Review", prompt="Do the thing")
    assert StarterConfig(id="monthly-review", title="Review", prompt="Do it").id == "monthly-review"


def test_home_refuses_a_wall_of_starters() -> None:
    too_many = [StarterConfig(id=f"s{index}", title="T", prompt="P") for index in range(MAX_STARTERS + 1)]

    with pytest.raises(ValidationError):
        UiConfig(starters=too_many)


def test_two_starters_cannot_share_an_id() -> None:
    with pytest.raises(ValidationError):
        UiConfig(
            starters=[
                StarterConfig(id="review", title="One", prompt="P"),
                StarterConfig(id="review", title="Two", prompt="P"),
            ]
        )


def test_an_operator_can_clear_the_grid() -> None:
    # An explicit empty list is a decision; it must outrank the profile default.
    assert UiConfig(profile="business", starters=[]).starters == []


def test_an_unknown_key_is_a_mistake_worth_reporting() -> None:
    with pytest.raises(ValidationError):
        StarterConfig(id="review", title="Review", prompt="P", icon="📊")


def test_a_tile_cannot_read_as_something_other_than_what_it_does() -> None:
    # A right-to-left override reorders the glyphs on the button while the
    # prompt it drops in the box says something else.
    for hidden in ("Payroll \u202egnitidua", "Export\u200bthe ledger", "tab\there"):
        with pytest.raises(ValidationError):
            StarterConfig(id="review", title=hidden, prompt="Do the thing")


def test_a_prompt_may_run_to_more_than_one_line() -> None:
    # A title is one line on a button; a prompt is what a person is about to
    # send, and paragraphs are normal there.
    assert StarterConfig(id="review", title="Review", prompt="One.\nTwo.").prompt == "One.\nTwo."


def test_a_block_scalar_does_not_spend_a_character_on_its_newline() -> None:
    # `prompt: >-` is how an operator will write a long one, and it arrives
    # with trailing whitespace the length limit should not count.
    starter = StarterConfig(id="review", title="T" * MAX_STARTER_TITLE_CHARS + "\n", prompt="P\n")

    assert len(starter.title) == MAX_STARTER_TITLE_CHARS
    assert starter.prompt == "P"


def test_the_default_starters_cannot_be_edited_through_one_deployment() -> None:
    # `DEFAULT_STARTERS` is shared by every `UiConfig` that takes it.
    with pytest.raises(ValidationError):
        UiConfig(profile="business").starters[0].title = "MUTATED"


def test_a_ui_block_in_config_yaml_reaches_the_config(tmp_path, monkeypatch) -> None:
    # The seam the endpoint actually reads: YAML on disk, through
    # `AppConfig.from_file`, to the `ui` field. Nothing else covers it.
    extensions_path = tmp_path / "extensions_config.json"
    extensions_path.write_text(json.dumps({"mcpServers": {}, "skills": {}}), encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(extensions_path))

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
                "models": [{"name": "m", "use": "langchain_openai:ChatOpenAI", "model": "gpt-test"}],
                "ui": {
                    "profile": "business",
                    "starters": [{"id": "review", "title": "Monthly review", "prompt": "Build my monthly review."}],
                },
            }
        ),
        encoding="utf-8",
    )

    ui = AppConfig.from_file(str(config_path)).ui

    assert ui.profile == "business"
    assert [(starter.id, starter.title) for starter in ui.starters] == [("review", "Monthly review")]


def test_a_config_without_a_ui_block_still_has_one(tmp_path, monkeypatch) -> None:
    extensions_path = tmp_path / "extensions_config.json"
    extensions_path.write_text(json.dumps({"mcpServers": {}, "skills": {}}), encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(extensions_path))

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
                "models": [{"name": "m", "use": "langchain_openai:ChatOpenAI", "model": "gpt-test"}],
            }
        ),
        encoding="utf-8",
    )

    ui = AppConfig.from_file(str(config_path)).ui

    assert ui.profile == "developer"
    assert ui.starters == []
