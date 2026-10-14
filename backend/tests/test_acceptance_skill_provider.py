"""Protocol fixture exercises actual production tools and scanner decisions."""

import hashlib
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


@pytest.mark.parametrize("package", ["supplier-comparison", "procedure-summary"])
def test_every_example_source_is_registered_with_fixture_scanner(reply, package):
    root = PROVIDER.parents[2] / "examples/skills" / package
    sources = sorted(path for path in root.rglob("*") if path.suffix in {".md", ".py"})
    assert sources
    for source in sources:
        result = reply(scanner(source.read_text(encoding="utf-8")))
        assert json.loads(result["content"])["decision"] == "allow", source.relative_to(root)


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


def work_body(results=(), *, message="Work on the explicitly activated assignment.", tools=("read_work_context", "bash", "report_work")):
    return {"messages": [{"role": "user", "content": message}, *[{"role": "tool", "content": result} for result in results]], "tools": [{"function": {"name": name}} for name in tools]}


def work_context(objective="acceptance:work:outcome"):
    return json.dumps({"work": {"id": "a" * 32, "revision": 7, "objective": objective, "attempt": {"id": "b" * 32}}})


def tool_call(result):
    call = result["tool_calls"][0]["function"]
    return call["name"], json.loads(call["arguments"])


@pytest.mark.parametrize("message", ["Work on the explicitly activated assignment.", "acceptance:file", "acceptance:skill:procedure-summary"])
def test_work_branch_is_chosen_by_the_offered_report_work_tool(reply, message):
    assert tool_call(reply(work_body(message=message))) == ("read_work_context", {})


def test_work_outcome_reads_context_writes_the_proof_and_reports_the_outcome_with_the_read_revision(reply):
    context = work_context()
    assert tool_call(reply(work_body())) == ("read_work_context", {})
    assert tool_call(reply(work_body([context]))) == ("bash", {"command": "mkdir -p /mnt/user-data/outputs && printf 'WORK-ACCEPTANCE-PROOF\\n' > /mnt/user-data/outputs/proof.txt"})
    report = tool_call(reply(work_body([context, "proof written"])))
    expected = {"report": {"operation_id": hashlib.sha256(("a" * 32 + ":" + "b" * 32).encode()).hexdigest()[:32], "expected_revision": 7, "action": "outcome", "statement": "Acceptance outcome: outputs/proof.txt written."}}
    assert report == ("report_work", expected)
    assert tool_call(reply(work_body([context, "proof written"]))) == report
    assert reply(work_body([context, "proof written", "outcome recorded"])) == {"content": "Work acceptance complete."}
    newer = json.loads(context)
    newer["work"]["revision"] = 9
    newer_report = tool_call(reply(work_body([json.dumps(newer), "proof written"])))[1]["report"]
    assert newer_report["expected_revision"] == 9 and newer_report["operation_id"] == expected["report"]["operation_id"]


def test_work_hold_reads_context_then_sleeps(reply):
    context = work_context("acceptance:work:hold")
    assert tool_call(reply(work_body())) == ("read_work_context", {})
    assert tool_call(reply(work_body([context]))) == ("bash", {"command": "sleep 300"})
    assert reply(work_body([context, "hold finished"])) == {"content": "Work hold finished."}


def test_work_branch_refuses_an_objective_without_a_marker(reply):
    with pytest.raises(ValueError, match="unrecognized acceptance work objective"):
        reply(work_body([work_context("unregistered objective")]))


@pytest.mark.parametrize("context", ["not json", "null", "[]", "{}", '{"work": {}}', '{"work": {"id": "a", "revision": true, "objective": "acceptance:work:outcome", "attempt": null}}'])
def test_work_branch_refuses_missing_or_malformed_actual_context(reply, context):
    with pytest.raises(ValueError, match="invalid acceptance work context"):
        reply(work_body([context]))


def test_work_branch_requires_the_actual_offered_tool(reply):
    with pytest.raises(ValueError, match="required real tool not offered: read_work_context"):
        reply(work_body(tools=("report_work",)))
    with pytest.raises(ValueError, match="required real tool not offered: bash"):
        reply(work_body([work_context()], tools=("read_work_context", "report_work")))
