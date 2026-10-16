"""Protocol fixture exercises actual production tools and scanner decisions."""

import hashlib
import http.client
import json
import runpy
import threading
from http.server import ThreadingHTTPServer
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
    return {
        "messages": [
            {"role": "system", "content": "- This run works on an assignment, and no person is waiting in this conversation."},
            {"role": "user", "content": message},
            *[{"role": "tool", "content": result} for result in results],
        ],
        "tools": [{"function": {"name": name}} for name in tools],
    }


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


def test_work_denied_reads_context_runs_the_blocked_command_and_reports_the_outcome(reply):
    context = work_context("acceptance:work:denied")
    assert tool_call(reply(work_body())) == ("read_work_context", {})
    assert tool_call(reply(work_body([context]))) == ("bash", {"command": "python3 -c \"import urllib.request; urllib.request.urlopen('https://example.org/', timeout=20)\"; echo exit $?"})
    report = tool_call(reply(work_body([context, "blocked"])))
    assert report == (
        "report_work",
        {"report": {"operation_id": hashlib.sha256(("a" * 32 + ":" + "b" * 32).encode()).hexdigest()[:32], "expected_revision": 7, "action": "outcome", "statement": "Acceptance outcome: blocked connection recorded."}},
    )
    assert reply(work_body([context, "blocked", "outcome recorded"])) == {"content": "Work acceptance complete."}


def test_work_ask_reads_context_then_requests_input_then_stops(reply):
    context = work_context("acceptance:work:ask")
    assert tool_call(reply(work_body())) == ("read_work_context", {})
    report = tool_call(reply(work_body([context])))
    assert report == (
        "report_work",
        {
            "report": {
                "operation_id": hashlib.sha256(("a" * 32 + ":" + "b" * 32).encode()).hexdigest()[:32],
                "expected_revision": 7,
                "action": "request_input",
                "statement": "Which input should I use for this assignment?",
                "purpose": "information",
                "reason": "The required input is unspecified.",
                "expected_response": "Identify the input to use.",
            }
        },
    )
    assert reply(work_body([context, "request recorded"])) == {"content": "Work question recorded."}
    newer = json.loads(context)
    newer["work"]["revision"] = 11
    assert tool_call(reply(work_body([json.dumps(newer)])))[1]["report"]["expected_revision"] == 11


@pytest.mark.parametrize("invalid", ["clarification", "missing", "user-only", "incomplete"])
def test_work_branch_refuses_a_request_that_offers_ask_clarification_or_lacks_the_question_rule(invalid):
    body = work_body()
    if invalid == "clarification":
        body["tools"].append({"function": {"name": "ask_clarification"}})
    elif invalid == "missing":
        body["messages"].pop(0)
    elif invalid == "user-only":
        body["messages"][0]["role"] = "user"
    else:
        body["messages"][0]["content"] = "- This run works on an assignment."
    handler = runpy.run_path(str(PROVIDER))["Handler"]
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    connection = http.client.HTTPConnection(*server.server_address, timeout=5)
    try:
        connection.request("POST", "/v1/chat/completions", json.dumps(body), {"Content-Type": "application/json"})
        response = connection.getresponse()
        response.read()
        assert response.status == 422
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
        assert not worker.is_alive()
