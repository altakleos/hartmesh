"""Qualify company MCP connections on the disposable mounted Compose bundle."""

import json
import os
import re
import subprocess
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote, urlsplit

import pytest

if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("HARTMESH_VERIFY_CONNECTIONS") not in {"explicit", "bundle-default"}:
    pytest.skip("requires the hosted connection qualification", allow_module_level=True)

from _compose_connection_evidence import confirmed_browser_notice, observe_initial_thread_reads
from _compose_gateway_restart import start_gateway, stop_gateway
from playwright.sync_api import expect, sync_playwright
from test_compose_instance_acceptance import Bundle, activate, await_attempt, await_execution_ready, delegate, eventually, new_conversation, required
from test_compose_instance_acceptance import compose as compose
from test_compose_instance_acceptance import session as session

pytestmark = pytest.mark.live

SERVICES = ("notices-a", "notices-b", "notices-unlisted")
REGISTRY = "/api/mcp/config"


def server(host):
    return {"type": "http", "url": f"https://{host}.example.test:8080/mcp", "enabled": True, "description": "Read a sample notice."}


def option(session, agent, name):
    path = f"/api/agent-instances/{agent['instance']['id']}/connections"
    entries = session.request("GET", path).json()["connections"]
    return next(row for row in entries if row["name"] == name)


def set_grant(session, agent, name, given):
    path = f"/api/agent-instances/{agent['instance']['id']}/connections"
    row = option(session, agent, name)
    assert row["given"] is not given
    session.request("PUT" if given else "DELETE", path + "/" + quote(row["id"], safe=""), expected=204)
    refreshed = option(session, agent, name)
    assert refreshed["given"] is given
    return refreshed


def verify_image(compose, service, image):
    container = compose.run("ps", "-q", service).stdout.strip()
    running = compose.invoke(["docker", "inspect", "--format", "{{.Config.Image}} {{.Image}}", container]).stdout.strip().split()
    expected = compose.invoke(["docker", "image", "inspect", "--format", "{{.Id}}", image]).stdout.strip()
    assert running == [image, expected]
    return {"ref": image, "image_id": expected}


@contextmanager
def notice_services(compose):
    directory = Path(required("HARTMESH_BUNDLE_PROJECT_DIR"))
    command = [*compose.command, "-f", str(directory / "compose.yaml"), "-f", str(directory / "connections.yaml")]
    try:
        compose.invoke([*command, "up", "-d", "--no-deps", *SERVICES])

        def observations():
            result = {}
            for service in SERVICES:
                reply = compose.invoke(
                    [
                        *command,
                        "exec",
                        "-T",
                        service,
                        "/app/backend/.venv/bin/python",
                        "-c",
                        "import ssl,urllib.request as u; "
                        "opener=u.build_opener(u.ProxyHandler({}),u.HTTPSHandler(context=ssl.create_default_context(cafile='/fixture/ca.pem'))); "
                        f"print(opener.open('https://{service}.example.test:8080/observations',timeout=3).read().decode())",
                    ],
                    check=False,
                )
                if reply.returncode:
                    return None
                counts = json.loads(reply.stdout)
                assert set(counts) == {"requests", "discoveries", "calls"} and all(type(v) is int and v >= 0 for v in counts.values())
                result[service] = counts
            return result

        first = eventually(observations, timeout=45, interval=1)
        assert all(all(n == 0 for n in row.values()) for row in first.values())
        yield lambda: eventually(observations, timeout=10, interval=1)
    finally:
        compose.invoke([*command, "rm", "--stop", "--force", *SERVICES])


def create_employees(session):
    name = "notice-reader"
    session.request(
        "POST", "/api/agents", json={"name": name, "model": "acceptance-model", "skills": [], "mcp_plugins": [], "allowed_subagents": ["general-purpose"], "work_policy": {"enabled": True}, "soul": "Read requested notices."}, expected=201
    )
    employees = []
    for index in (1, 2):
        instance = session.request("POST", "/api/agent-instances", json={"creation_id": uuid.uuid4().hex, "name": f"Notice reader {index}", "definition_name": name, "custody": "company"}, expected=201).json()
        employees.append(new_conversation(session, {"instance": instance, "definition": name}))
    return employees


@contextmanager
def signed_in_browser(compose, session):
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            context = browser.new_context(locale="en-US")
            context.add_cookies([{"url": compose.url, "name": c.name, "value": c.value, "httpOnly": c.has_nonstandard_attr("HttpOnly"), "secure": c.secure, "sameSite": "Lax"} for c in session.client.cookies.jar])
            assert context.request.get(compose.url + "/api/v1/auth/me").json()["id"] == session.user["id"]
            yield context.new_page()
        finally:
            browser.close()


def access_page(page, compose, agent, enabled):
    reply = page.goto(f"{compose.url}/workspace/instances?instance={agent['instance']['id']}&tab=access", wait_until="domcontentloaded")
    assert reply.ok
    expect(page.get_by_text(agent["instance"]["name"], exact=True).first).to_be_visible(timeout=30000)
    heading = page.get_by_role("heading", name="Connections", exact=True)
    if enabled:
        expect(heading).to_be_visible(timeout=30000)
    else:
        expect(heading).to_have_count(0)


def connect_from_browser(page, compose):
    page.goto(compose.url + "/workspace/chats/new?settings=tools", wait_until="domcontentloaded")
    page.get_by_role("button", name="Add connection", exact=True).click(timeout=30000)
    dialog = page.get_by_role("dialog", name="Add connection", exact=True)
    dialog.get_by_label("Name", exact=True).fill("notices")
    dialog.get_by_label("Address", exact=True).fill(server("notices-a")["url"])
    dialog.get_by_label("Description (optional)", exact=True).fill(server("notices-a")["description"])
    with page.expect_response(lambda response: response.url.endswith("/api/mcp/config/servers/notices/check") and response.request.method == "POST", timeout=30000) as checked:
        dialog.get_by_role("button", name="Save and check", exact=True).click()
    assert checked.value.ok
    assert checked.value.json() == {"status": "connected", "tool_count": 1}
    expect(page.get_by_text("Saved. Connected. 1 tool.", exact=True)).to_be_visible(timeout=30000)


def read_from_browser(page, compose, session, agent, evidence):
    thread_id = agent["thread_id"]
    evidence["thread_id"] = thread_id
    expected = "Notice from notices-a."
    with observe_initial_thread_reads(page, f"{compose.url}/api/langgraph/threads/{thread_id}", evidence):
        page.goto(f"{compose.url}/workspace/chats/{thread_id}", wait_until="domcontentloaded")
        field = page.get_by_placeholder(re.compile(r"how can i assist", re.I))
        field.fill("acceptance:notice:read")
    with page.expect_response(lambda reply: reply.request.method == "POST" and urlsplit(reply.url).path.endswith(f"/threads/{thread_id}/runs/stream"), timeout=30000) as streamed:
        page.get_by_role("button", name="Submit", exact=True).click()
    response = streamed.value
    assert response.ok
    match = re.fullmatch(rf"/api/threads/{re.escape(thread_id)}/runs/([0-9a-f-]{{36}})", response.headers.get("content-location", ""))
    assert match, "Browser stream did not identify its thread and run"
    run_id = match[1]
    evidence["run_id"] = run_id
    expect(page.locator("[data-assistant-turn]").last.get_by_text(expected, exact=True)).to_be_visible(timeout=90000)
    thread_path = f"/api/langgraph/threads/{thread_id}"
    run_path = f"{thread_path}/runs/{run_id}"

    def persisted_answer():
        run = session.request("GET", run_path).json()
        assert run["run_id"] == run_id and run["thread_id"] == thread_id
        assert run["status"] not in {"error", "timeout", "interrupted"}, run["status"]
        if run["status"] != "success":
            return None
        messages = session.request("GET", thread_path + "/state").json()["values"]["messages"]
        events = session.request("GET", run_path + "/messages").json()["data"]
        return confirmed_browser_notice(thread_id, run_id, expected, messages, events)

    evidence["message_id"] = eventually(persisted_answer, timeout=90, interval=1)


def observed_chat(session, agent, *, assigned, helpers=False):
    run = session.run(agent["thread_id"], message="acceptance:notice:delegate" if helpers else "acceptance:notice:read" if assigned else "acceptance:notice:unassigned", helpers=helpers)
    stored = session.request("GET", f"/api/langgraph/threads/{agent['thread_id']}/state").json()["values"]["messages"]
    last = next(message for message in reversed(stored) if message.get("type") == "ai")
    expected = "Helper confirmed its offered tools." if helpers else "Notice from notices-a." if assigned else "No notice connection is available."
    assert expected in str(last["content"])
    return run["run_id"]


def exercise_assignment(compose, session, agent, counts, evidence):
    context = new_conversation(session, agent)
    nonce = uuid.uuid4().hex
    work = delegate(session, context, "acceptance:work:notices:" + nonce)
    folder = Path(required("EVIDENCE"))
    edit = folder / ("armed-" + nonce + "-edit")
    revoke = folder / ("armed-" + nonce + "-revoke")
    before = counts()
    assignment = {"thread_id": context["thread_id"], "work_id": work["id"], "counts_before": before}
    evidence["assignment"] = assignment
    edit.touch()
    revoke.touch()
    try:
        activate(session, context, work)
        eventually(lambda: (folder / ("waiting-" + nonce + "-edit")).exists(), timeout=110, interval=1)
        first = counts()
        assignment["after_first_call"] = first
        assert first["notices-a"]["calls"] == before["notices-a"]["calls"] + 1
        assert first["notices-b"]["calls"] == before["notices-b"]["calls"]
        session.request("PUT", REGISTRY + "/server", json={"server_name": "notices", "server": server("notices-b")})
        after_edit = counts()
        assignment["after_edit"] = after_edit
        edit.unlink()
        eventually(lambda: (folder / ("waiting-" + nonce + "-revoke")).exists(), timeout=110, interval=1)
        second = counts()
        assignment["after_second_call"] = second
        assert second["notices-a"] == after_edit["notices-a"]
        assert second["notices-b"]["calls"] == first["notices-b"]["calls"] + 1
        set_grant(session, agent, "notices", False)
        revoked = counts()
        assignment["after_revocation"] = revoked
        revoke.unlink()
        settled = await_attempt(session, context, work["id"], {"succeeded", "failed"}, timeout=120)
        assert settled["attempt"]["status"] == "succeeded" and settled["status"] == "submitted"
        assert counts() == revoked
        run_id = settled["attempt"]["run_id"]
        assignment["run_id"] = run_id
        events = session.request("GET", f"/api/threads/{context['thread_id']}/runs/{run_id}/events", params={"event_types": "middleware:connection_call"}).json()
        assert "Notice from notices-" not in json.dumps(events) and ".example.test" not in json.dumps(events)
        calls = [entry["content"]["changes"] for entry in events]
        assert [c["outcome"] for c in calls] == ["sending", "made", "sending", "made", "refused"]
        assert calls[-1]["reason"] == "not-given"
        assert all(c["origin"] == "assignment" and c["instance_id"] == agent["instance"]["id"] and c["connection_name"] == "notices" for c in calls)
        assert all(set(c) <= {"outcome", "reason", "call_id", "connection_id", "connection_name", "instance_id", "origin", "package"} for c in calls)
        assert len({c["connection_id"] for c in calls}) == 1
        assert [calls[i]["call_id"] == calls[i + 1]["call_id"] for i in (0, 2)] == [True, True]
        assert len({calls[i]["call_id"] for i in (0, 2, 4)}) == 3
        assignment["outcomes"] = [c["outcome"] for c in calls]
        assignment["refusal"] = calls[-1]["reason"]
    finally:
        edit.unlink(missing_ok=True)
        revoke.unlink(missing_ok=True)


def replace_option(compose, mode):
    """Change only the explicit test option or the shipped bundle's one operator key."""
    if required("HARTMESH_VERIFY_CONNECTIONS") == "explicit":
        helper = Path(__file__).resolve().parents[2] / "docker/acceptance/connections/template.py"
        compose.invoke(["sudo", sys.executable, str(helper), mode])
    # The pre-activation renderer ignores this key. The final renderer reads it.
    field = Path(required("HARTMESH_BUNDLE_ENV_FILE"))
    compose.invoke(["sudo", "sed", "-i", "/^HARTMESH_AGENT_CONNECTIONS=/d", str(field)])
    subprocess.run(["sudo", "tee", "-a", str(field)], input=f"HARTMESH_AGENT_CONNECTIONS={mode}\n", stdout=subprocess.DEVNULL, text=True, check=True)


def features(session):
    return session.request("GET", "/api/features").json()["agent_instances"]


def exercise_switch(compose, session, page, agent, counts, evidence):
    choice = option(session, agent, "notices")
    assert choice["given"]
    toggle = "/api/agent-instances/" + agent["instance"]["id"] + "/connections/" + quote(choice["id"], safe="")
    for mode in ("off", "on"):
        stop_gateway(compose)
        replace_option(compose, mode)
        start_gateway(compose, session)
        await_execution_ready(session)
        assert features(session)["connections"] is (mode == "on")
        access_page(page, compose, agent, mode == "on")
        if mode == "off":
            denied = session.request("PUT", toggle, expected=(403, 404, 409)).json()
            assert "not-available" in json.dumps(denied)
            before = counts()
            run = observed_chat(session, new_conversation(session, agent), assigned=False)
            assert counts() == before
            evidence["off"] = {"run_id": run, "give": "not-available", "unchanged_counts": before}
        else:
            current = option(session, agent, "notices")
            assert current["given"] and current["id"] == choice["id"] and current["given_at"] == choice["given_at"]
            run = observed_chat(session, new_conversation(session, agent), assigned=True)
            evidence["on"] = {"run_id": run, "same_choice": True}


def rollback(compose, session, version, primary, unaffected, counts, assignment):
    directory = Path(f"/opt/hartmesh-rollback-{version}")
    refs = (directory / "images.txt").read_text().splitlines()
    backend = next(value for value in refs if "/hartmesh-backend@" in value)
    frontend = next(value for value in refs if "/hartmesh-frontend@" in value)
    old = Bundle()
    old.command = ["docker", "compose", "--project-directory", str(directory), "--env-file", required("HARTMESH_BUNDLE_ENV_FILE")]
    choice = option(session, unaffected, "bulletins")
    assert choice["given"] and option(session, primary, "notices")["given"]
    assert compose.ai("status")["file_operations_pending"] == 0
    stop_gateway(compose)
    # Rehearse the product's own offline downgrade precondition on the same data.
    checked = compose.run("run", "--rm", "--no-deps", "-T", "--entrypoint", "sh", "gateway", "/opt/hartmesh/gateway/as-user.sh", "/app/backend/.venv/bin/python", "-m", "app.gateway.agent_instances", "check-rollback")
    assert json.loads(checked.stdout.splitlines()[-1])["returncode"] == 0
    old.run("up", "-d", "--wait", "--no-deps", "frontend", timeout=300)
    start_gateway(old, session)
    old_images = {"backend": verify_image(old, "gateway", backend), "frontend": verify_image(old, "frontend", frontend)}
    try:
        await_execution_ready(session)
        assert not features(session).get("connections")
        before = counts()
        old_run = observed_chat(session, primary, assigned=False)
        seen = session.request("GET", f"/api/threads/{assignment['thread_id']}/runs/{assignment['run_id']}/events", params={"event_types": "middleware:connection_call"}).json()
        assert [e["content"]["changes"]["outcome"] for e in seen] == assignment["outcomes"]
        assert counts() == before
        # Edit an existing definition with the old product and make a fresh one
        # under the removed name. All traffic is confined to the hosted fixture.
        session.request("POST", REGISTRY + "/servers", json={"mcp_servers": {"sample": server("notices-a")}})
        session.request("PUT", REGISTRY + "/server", json={"server_name": "bulletins", "server": {**server("notices-a"), "description": "Updated notice listing."}})
        for enabled in (False, True):
            session.request("PATCH", REGISTRY, json={"server_name": "bulletins", "enabled": enabled})
        session.request("DELETE", REGISTRY + "/servers/sample")
        masked = session.request("GET", REGISTRY).json()["mcp_servers"]["notices"]
        assert masked["capability"]["instance_binding_secret"] == "***"
        session.request("DELETE", REGISTRY + "/servers/notices")
        rejected = session.request("POST", REGISTRY + "/servers", json={"mcp_servers": {"notices": masked}}, expected=400)
        assert "masked secret placeholder" in rejected.text
        session.request("POST", REGISTRY + "/servers", json={"mcp_servers": {"notices": server("notices-a")}})
    finally:
        stop_gateway(old)
        compose.run("up", "-d", "--wait", "--no-deps", "frontend", timeout=300)
        start_gateway(compose, session)
        await_execution_ready(session)
    verify_image(compose, "gateway", required("BACKEND_REF"))
    verify_image(compose, "frontend", required("HARTMESH_BROWSER_FRONTEND"))
    assert not option(session, primary, "notices")["given"]
    retained = option(session, unaffected, "bulletins")
    assert retained["given"] and retained["id"] == choice["id"] and retained["given_at"] == choice["given_at"]
    unassigned = observed_chat(session, new_conversation(session, primary), assigned=False)
    assigned = observed_chat(session, new_conversation(session, unaffected), assigned=True)
    return {"older": old_images, "older_reply": old_run, "replacement_unassigned": unassigned, "untouched_assigned": assigned}


def test_company_connection_calls_switch_and_rollback(compose, session):
    output = Path(required("EVIDENCE")) / "connections.json"
    record = {"complete": False, "activation": required("HARTMESH_VERIFY_CONNECTIONS"), "head": required("GITHUB_SHA"), "workflow_run": required("GITHUB_RUN_ID"), "attempt": required("GITHUB_RUN_ATTEMPT"), "rollback": {}}
    try:
        record["images"] = {"backend": verify_image(compose, "gateway", required("BACKEND_REF")), "frontend": verify_image(compose, "frontend", required("HARTMESH_BROWSER_FRONTEND"))}
        await_execution_ready(session)
        assert features(session)["connections"]
        with notice_services(compose) as counts, signed_in_browser(compose, session) as page:
            primary, unaffected = create_employees(session)
            access_page(page, compose, primary, True)
            refused = session.request("POST", REGISTRY + "/servers", json={"mcp_servers": {"excluded": server("notices-unlisted")}}, expected=400)
            assert refused.json() == {"detail": "MCP server 'excluded': MCP connections on this deployment must use a public HTTP(S) address."}
            assert counts()["notices-unlisted"]["requests"] == 0
            connect_from_browser(page, compose)
            baseline = counts()
            assert baseline["notices-a"]["discoveries"] > 0 and baseline["notices-a"]["calls"] == 0
            assert baseline["notices-b"]["requests"] == baseline["notices-unlisted"]["requests"] == 0
            set_grant(session, primary, "notices", True)
            record["chat"] = {"after_check": baseline}
            read_from_browser(page, compose, session, primary, record["chat"])
            after_chat = counts()
            assert after_chat["notices-a"]["calls"] == 1 and after_chat["notices-b"]["calls"] == 0
            page.screenshot(path=output.parent / "notice-conversation.png", animations="disabled")
            record["after_web_reply"] = after_chat
            page.reload(wait_until="domcontentloaded")
            expect(page.locator("[data-assistant-turn]").last.get_by_text("Notice from notices-a.", exact=True)).to_be_visible(timeout=30000)
            assert counts() == after_chat
            record["chat"].update(after_reply=after_chat, unchanged_after_history=True)
            record["helper"] = observed_chat(session, new_conversation(session, primary), assigned=True, helpers=True)
            assert counts()["notices-a"]["calls"] == after_chat["notices-a"]["calls"]
            exercise_assignment(compose, session, primary, counts, record)
            session.request("PUT", REGISTRY + "/server", json={"server_name": "notices", "server": server("notices-a")})
            set_grant(session, primary, "notices", True)
            exercise_switch(compose, session, page, primary, counts, record)
            session.request("POST", REGISTRY + "/servers", json={"mcp_servers": {"bulletins": server("notices-a")}})
            set_grant(session, unaffected, "bulletins", True)
            for version in ("66", "61", "60"):
                record["rollback"][version] = rollback(compose, session, version, primary, unaffected, counts, record["assignment"])
                if version != "60":
                    set_grant(session, primary, "notices", True)
            assert counts()["notices-unlisted"]["requests"] == 0
        record["complete"] = True
    finally:
        output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
