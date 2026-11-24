"""Qualify employee submission and manager consent on the installed two-image bundle."""

import json
import os
import re
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import yaml

if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("HARTMESH_VERIFY_CONFIGURATION_PROPOSALS") != "1":
    pytest.skip("requires the dedicated mounted employee proposal rehearsal", allow_module_level=True)

from _compose_browser_session import signed_in_browser, verify_image
from _compose_gateway_restart import start_gateway, stop_gateway
from playwright.sync_api import expect
from test_compose_instance_acceptance import await_execution_ready, eventually, required, sql
from test_compose_instance_acceptance import compose as compose
from test_compose_instance_acceptance import member as member
from test_compose_instance_acceptance import session as session

pytestmark = pytest.mark.live
INSTANCES = "/api/agent-instances"
PREFERENCES = "/api/v1/auth/preferences"
REPLIES = {
    "enable": "Submitted the enable proposal for review.",
    "disable": "Submitted the disable proposal for review.",
    "fresh-on": "Tracked assignment proposals are available.",
    "fresh-off": "Tracked assignment proposals are unavailable.",
    "package-off": "The configuration form is unavailable.",
}


def identity():
    return uuid.uuid4().hex


def turn(compose, person, instance_url, mode):
    await_execution_ready(person)
    thread = person.request("POST", instance_url + "/conversations", json={"creation_id": identity()}, expected=201).json()["thread_id"]
    with signed_in_browser(compose, person) as page:
        page.goto(compose.url + "/workspace/chats/" + thread)
        page.get_by_placeholder(re.compile("how can i assist", re.I)).fill(f"acceptance:proposal:{mode}:{identity()}")
        with page.expect_response(lambda r: r.request.method == "POST" and urlsplit(r.url).path.endswith(f"/threads/{thread}/runs/stream"), timeout=60000) as admitted:
            page.get_by_role("button", name="Submit", exact=True).click()
        assert admitted.value.ok
        location = admitted.value.headers.get("content-location", "")
        match = re.fullmatch(rf"/api/threads/{re.escape(thread)}/runs/([0-9a-f-]{{36}})", location)
        assert match
        expect(page.locator("[data-assistant-turn]").last.get_by_text(REPLIES[mode], exact=True)).to_be_visible(timeout=120000)
        page.screenshot(path=Path(required("EVIDENCE")) / f"employee-proposal-{mode}-{match[1]}.png")
    eventually(lambda: person.request("GET", "/api/langgraph/threads/" + thread + "/runs/" + match[1]).json()["status"] == "success", timeout=60, interval=1)
    return {"thread_id": thread, "run_id": match[1]}


def recovery_read(detail_requests, list_responses, instance_responses, proposal, expected):
    identity_fields = ("id", "instance_id", "base_revision", "base_generation", "candidate_digest")
    detail_url = f"{INSTANCES}/{proposal['instance_id']}/proposals/{proposal['id']}"
    assert len(detail_requests) <= 1
    if detail_requests:
        response = detail_requests[0].response()
        if response is not None and response.status == 200:
            card = response.json()
            assert all(card[key] == proposal[key] for key in identity_fields)
            assert card["status"] in ({"applying", "adopted"} if expected == "adopted" else {"declined"})
            return {"kind": "detail", "path": detail_url, "http_status": 200, "proposal_status": card["status"]}

    # A newer instance generation retires the old write's owner before its
    # deadline. Settings remounts and reads the list under the new generation.
    generations = set()
    for response in instance_responses:
        instance = response.json()
        if instance["id"] == proposal["instance_id"] and instance["generation"] > proposal["current_generation"]:
            generations.add(instance["generation"])
    assert generations, "No browser instance response advanced the proposal scope"
    for response in reversed(list_responses):
        for card in response.json()["proposals"]:
            if card["status"] == expected and card["current_generation"] in generations and all(card[key] == proposal[key] for key in identity_fields):
                return {"kind": "list", "path": urlsplit(response.url).path, "http_status": 200, "proposal_status": card["status"], "generation": card["current_generation"]}
    raise AssertionError("The new instance scope did not read the terminal proposal")


def settings(compose, manager, instance, proposal, action, *, stale=False):
    url = f"{INSTANCES}/{instance['id']}/proposals/{proposal['id']}"
    endpoint = f"{url}/{action}"
    with signed_in_browser(compose, manager) as page:
        page.goto(f"{compose.url}/workspace/instances?instance={instance['id']}&tab=settings")
        panel = page.get_by_role("region", name="Configuration proposals")
        row = panel.locator("article").filter(has=page.get_by_text("Awaiting decision", exact=True))
        expect(row).to_have_count(1, timeout=60000)
        original = "On" if proposal["base_value"] else "Off"
        proposed = "On" if proposal["proposed_value"] else "Off"
        expect(row.get_by_text("At proposal time · " + original, exact=True)).to_be_visible()
        expect(row.get_by_text("Proposed setting · " + proposed, exact=True)).to_be_visible()
        expect(row.get_by_text(proposal["base_revision"], exact=True)).to_be_visible()
        if stale:
            expect(row.get_by_text("Ask for a fresh proposal", exact=False)).to_be_visible()
            expect(row.get_by_role("button", name="Adopt change")).to_have_count(0)
        page.screenshot(path=Path(required("EVIDENCE")) / f"manager-proposal-{proposal['id']}.png")

        def is_decision(request):
            return request.method == "POST" and urlsplit(request.url).path == endpoint

        writes, reads = [], []
        list_responses, instance_responses = [], []

        def record_response(response):
            if writes and response.request.method == "GET" and response.status == 200:
                path = urlsplit(response.url).path
                if path == f"{INSTANCES}/{instance['id']}/proposals":
                    list_responses.append(response)
                elif path == f"{INSTANCES}/{instance['id']}":
                    instance_responses.append(response)

        def record_request(request):
            if is_decision(request):
                writes.append(request)
            elif request.method == "GET" and urlsplit(request.url).path == url:
                reads.append(request)

        page.on("request", record_request)
        page.on("response", record_response)
        started = time.monotonic()
        with page.expect_request(is_decision, timeout=30000) as issuance:
            row.get_by_role("button", name="Adopt change" if action == "adopt" else "Decline", exact=True).click()
        request = issuance.value
        body = {key: proposal[key] for key in ("base_revision", "base_generation", "candidate_digest")} if action == "adopt" else {}
        assert request.post_data_json == body
        expected = "adopted" if action == "adopt" else "declined"
        persisted = panel.locator("article").filter(has=page.get_by_text(expected.capitalize(), exact=True)).filter(has=page.get_by_text("Proposed setting · " + proposed, exact=True))
        terminal = persisted.get_by_text(proposal["base_revision"], exact=True)
        # Native fencing can outlast the browser write timeout. The installed
        # Settings must show the durable decision without another manager click.
        expect(terminal).to_be_visible(timeout=60000)
        confirmed_seconds = round(time.monotonic() - started, 2)
        response = request.response()
        browser_read = None
        if response is None:
            assert action == "adopt" and request.failure == "net::ERR_ABORTED", request.failure
        if response is None or response.status == 408 or response.status >= 500:
            browser_read = recovery_read(reads, list_responses, instance_responses, proposal, expected)
        else:
            assert response.status in (200, 202)
            assert not reads
        detail = manager.request("GET", url).json()
        assert detail["id"] == proposal["id"] and detail["decided_by"] == manager.user["id"] and detail["status"] == expected
        page.screenshot(path=Path(required("EVIDENCE")) / f"manager-{expected}-{proposal['id']}.png")
        page.reload()
        expect(terminal).to_be_visible(timeout=60000)
        assert len(writes) == 1
        assert manager.request("GET", url).json() == detail
        if action == "adopt":
            history = manager.request("GET", f"{INSTANCES}/{instance['id']}/lifecycle").json()["operations"]
            (committed,) = [entry for entry in history if entry["operation_id"] == detail["operation_id"]]
            assert committed["action"] == "adopt" and committed["complete"] and not committed["abandoned"]
        return {
            **detail,
            "browser_write": response.status if response else request.failure,
            "browser_read": browser_read,
            "automatic_confirmation": True,
            "confirmed_seconds": confirmed_seconds,
        }


def open_proposal(manager, url):
    proposals = manager.request("GET", url + "/proposals").json()["proposals"]
    (row,) = [entry for entry in proposals if entry["status"] == "open"]
    return row


def invariants(compose, manager, member, instance, sibling):
    home = sql(instance["home_id"])
    return {
        "source": manager.request("GET", "/api/agents/proposal-review/export").json(),
        "sibling": manager.request("GET", f"{INSTANCES}/{sibling['id']}/definition").json(),
        "grants": manager.request("GET", f"{INSTANCES}/{instance['id']}/grants").json(),
        "home": compose.psql(f"SELECT principal_kind,subject_id,permissions FROM storage_space_grants WHERE space_id={home} ORDER BY principal_kind,subject_id"),
        "connections": manager.request("GET", f"{INSTANCES}/{instance['id']}/connections").json(),
        "notifications": [p.request("GET", PREFERENCES, headers={"X-Expected-User-Id": p.user["id"]}).json()["notification_enabled"] for p in (manager, member)],
    }


def test_employee_submission_requires_an_authorized_human_decision(compose, session, member, tmp_path):
    evidence = {"revision": required("FILES_BROWSER_REVISION"), "backend": verify_image(compose, "gateway", required("BACKEND_REF")), "frontend": verify_image(compose, "frontend", required("HARTMESH_BROWSER_FRONTEND")), "model": "scripted"}
    assert await_execution_ready(session)["configuration_proposals"] is True
    for person, enabled in ((session, True), (member, False)):
        person.request("PATCH", PREFERENCES, headers={"X-Expected-User-Id": person.user["id"]}, json={"notification_enabled": enabled}, expected=204)
    session.request("POST", "/api/agents", json={"name": "proposal-review", "model": "acceptance-model", "skills": [], "mcp_plugins": [], "allowed_subagents": [], "soul": "Review tracked assignments with the manager."}, expected=201)

    def employee(name):
        return session.request("POST", INSTANCES, json={"creation_id": identity(), "custody": "company", "name": name, "definition_name": "proposal-review"}, expected=201).json()

    instance, sibling = employee("Assignment reviewer"), employee("Sibling reviewer")
    url = f"{INSTANCES}/{instance['id']}"
    grant = {"action": "grant", "generation": instance["generation"], "operation_id": identity(), "member_id": member.user["id"], "permissions": 1}
    eventually(lambda: session.request("POST", url + "/lifecycle", json=grant, expected=(200, 202)).json()["complete"], timeout=120, interval=1)
    assert member.request("GET", url).json()["permissions"] == 1
    initial = invariants(compose, session, member, instance, sibling)
    assert initial["notifications"] == [True, False]
    assert initial["home"] and initial["source"]["agent"]["name"] == "proposal-review"

    def create_work(expected):
        return session.request("POST", url + "/work", json={"operation_id": identity(), "objective": "Review the onboarding instructions", "success_criteria": "Identify any missing steps.", "review_required": True}, expected=expected)

    create_work(409)
    evidence["initial_turn"] = turn(compose, member, url, "fresh-off")
    turn(compose, member, url, "enable")
    proposal = open_proposal(session, url)
    member.request("GET", url + "/proposals", expected=(403, 404))
    member.request("POST", f"{url}/proposals/{proposal['id']}/decline", json={}, expected=(403, 404))
    assert session.request("GET", f"{INSTANCES}/{sibling['id']}/proposals").json()["proposals"] == []
    assert session.request("GET", url + "/definition").json()["revision"] == proposal["base_revision"]
    current = session.request("GET", url).json()
    session.request("PATCH", url, json={"generation": current["generation"], "name": "Assignment reviewer updated"})
    stale = open_proposal(session, url)
    assert stale["stale"] and stale["adopt_blocked_reason"] == "stale"
    evidence["stale"] = settings(compose, session, instance, stale, "decline", stale=True)
    turn(compose, member, url, "enable")
    proposal = open_proposal(session, url)
    assert proposal["base_value"] is False and proposal["current_value"] is False and proposal["proposed_value"] is True
    assert proposal["effective_work_policy"]["review_required"] is True
    create_work(409)
    evidence["enable"] = settings(compose, session, instance, proposal, "adopt")
    assert session.request("GET", url + "/work").json()["work"] == []
    evidence["fresh_turn"] = turn(compose, member, url, "fresh-on")
    work = create_work(201).json()
    assert work["definition_revision"] == proposal["candidate_digest"]
    evidence["fresh_work"] = {"id": work["id"], "definition_revision": work["definition_revision"]}
    turn(compose, member, url, "disable")
    off = open_proposal(session, url)
    assert off["base_value"] is True and off["current_value"] is True and off["proposed_value"] is False
    evidence["disable"] = settings(compose, session, instance, off, "adopt")
    create_work(409)
    evidence["disabled_turn"] = turn(compose, member, url, "fresh-off")
    retained = session.request("GET", f"{url}/work/{work['id']}").json()
    assert retained["definition_revision"] == work["definition_revision"] and retained["needs_mandate_reconciliation"] is True
    assert invariants(compose, session, member, instance, sibling) == initial
    evidence["preferences_preserved"] = True

    # A disposable operator template opts out through the installed package's
    # normal switch. Restore the exact bytes and boot the original config.
    template = Path(required("HARTMESH_BUNDLE_PROJECT_DIR")) / "config.yaml"
    saved = tmp_path / "saved.yaml"
    saved.write_bytes(template.read_bytes())
    document = yaml.safe_load(saved.read_bytes())
    (form,) = (entry for entry in document["plugins"] if entry["name"] == "work-proposals")
    assert form["enabled"] is True
    form["enabled"] = False
    disabled = tmp_path / "disabled.yaml"
    disabled.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    try:
        stop_gateway(compose)
        compose.invoke(["sudo", "install", "-m", "0644", str(disabled), str(template)])
        start_gateway(compose, session)
        assert await_execution_ready(session)["configuration_proposals"] is False
        session.request("GET", url + "/proposals", expected=404)
        with signed_in_browser(compose, session) as page:
            requests = []
            page.on("request", lambda r: requests.append(urlsplit(r.url).path))
            with page.expect_response(lambda response: response.request.method == "GET" and urlsplit(response.url).path == "/api/features", timeout=60000) as discovery:
                page.goto(f"{compose.url}/workspace/instances?instance={instance['id']}&tab=settings")
            assert discovery.value.ok and discovery.value.json()["agent_instances"]["configuration_proposals"] is False
            settings_tab = page.get_by_role("tabpanel", name="Settings", exact=True)
            expect(settings_tab.get_by_role("textbox", name="Name", exact=True)).to_have_value("Assignment reviewer updated", timeout=60000)
            expect(settings_tab.get_by_role("textbox", name="Brief", exact=True)).to_have_value("Review tracked assignments with the manager.", timeout=60000)
            expect(settings_tab.get_by_role("region", name="Configuration proposals")).to_have_count(0)
            assert url + "/proposals" not in requests
        turn(compose, member, url, "package-off")
        evidence["package_off"] = True
    finally:
        if compose.run("ps", "-q", "gateway").stdout.strip():
            stop_gateway(compose)
        compose.invoke(["sudo", "install", "-m", "0644", str(saved), str(template)])
        start_gateway(compose, session)
    assert await_execution_ready(session)["configuration_proposals"] is True
    assert session.request("GET", f"{url}/proposals/{off['id']}").json()["status"] == "adopted"
    evidence["complete"] = True
    (Path(required("EVIDENCE")) / "employee-proposals.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
