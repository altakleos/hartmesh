"""Protocol fixture exercises actual production tools and scanner decisions."""

import json
import runpy
from pathlib import Path

import pytest

PROVIDER = Path(__file__).resolve().parents[2] / "docker/acceptance/provider.py"


@pytest.fixture
def reply():
    return runpy.run_path(str(PROVIDER))["reply"]


def scanner(content):
    return {
        "messages": [
            {
                "role": "system",
                "content": "You are a security reviewer for AI agent skills.",
            },
            {
                "role": "user",
                "content": f"Location: supplier-comparison/SKILL.md\nReview this content:\n-----\n{content}\n-----",
            },
        ]
    }


def test_fixture_supplies_actual_scanner_decision_for_registered_example(reply):
    result = reply(scanner("Standalone skill-result-acceptance-fixture."))
    assert json.loads(result["content"])["decision"] == "allow"


def test_unknown_scanner_content_is_not_automatically_allowed(reply):
    result = reply(scanner("Unknown package content."))
    assert json.loads(result["content"])["decision"] == "block"


def test_skill_run_starts_with_real_canonical_skill_read(reply):
    result = reply(
        {
            "messages": [{"role": "user", "content": "acceptance:skill:supplier-comparison"}],
            "tools": [{"function": {"name": name}} for name in ("read_file", "bash", "present_files")],
        }
    )
    call = result["tool_calls"][0]["function"]
    assert call["name"] == "read_file"
    assert json.loads(call["arguments"])["path"] == "/mnt/skills/custom/supplier-comparison/SKILL.md"


def test_fixture_rejects_unknown_optional_model_prompt(reply):
    with pytest.raises(ValueError):
        reply({"messages": [{"role": "user", "content": "Unregistered fixture request"}]})


def test_skill_presentation_uses_actual_bounded_producer_manifest(reply):
    body = {
        "messages": [
            {"role": "user", "content": "acceptance:skill:procedure-summary"},
            {"role": "tool", "content": "skill-result-acceptance-fixture"},
            {"role": "tool", "content": "Inspection interval not supplied"},
            {
                "role": "tool",
                "content": json.dumps({"files": ["procedure-012345abcdef/procedure.pdf"]}),
            },
        ],
        "tools": [{"function": {"name": "present_files"}}],
    }
    result = reply(body)["tool_calls"][0]["function"]
    assert json.loads(result["arguments"])["filepaths"] == ["/mnt/user-data/outputs/skill-results/procedure-012345abcdef/procedure.pdf"]
    body["messages"][-1]["content"] = json.dumps({"files": ["../elsewhere.pdf"]})
    with pytest.raises(ValueError, match="unexpected path"):
        reply(body)
    body["messages"][-1]["content"] = "producer failed"
    with pytest.raises(ValueError, match="producer did not return"):
        reply(body)
