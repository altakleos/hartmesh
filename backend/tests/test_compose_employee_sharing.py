"""Exercise company sharing and continuing through installed Gateway routes."""

import hashlib
import json
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

import pytest

if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("HARTMESH_VERIFY_SHARED_SNAPSHOTS") != "1":
    pytest.skip("requires the mounted snapshot publication rehearsal", allow_module_level=True)

from _compose_browser_session import signed_in_browser, verify_image
from _compose_gateway_restart import start_gateway, stop_gateway
from test_compose_instance_acceptance import ARTIFACT, MARKER, Bundle, Person, await_execution_ready, company, eventually, required, sql
from test_compose_instance_acceptance import compose as compose
from test_compose_instance_acceptance import member as member
from test_compose_instance_acceptance import session as session
from test_compose_shared_settlement import cut_domain_write, host, interrupted_request, phase, snapshot

from app.gateway.transcript import transcript_stem

pytestmark = pytest.mark.live
SHARED = "/api/shared-conversations"
INSTANCES = "/api/agent-instances"


@pytest.fixture
def colleague(compose):
    person = Person(compose)
    try:
        person.request("POST", "/api/v1/auth/register", json={"email": "sharing-colleague@example.com", "password": "Acceptance-Colleague-2026!"}, expected=201)
        person.user = person.request("GET", "/api/v1/auth/me").json()
        yield person
    finally:
        person.client.close()


def grant_chat(manager, instance, person, *, snapshot_url=None):
    url = f"{INSTANCES}/{instance['id']}"
    current = manager.request("GET", url).json()
    change = {"action": "grant", "operation_id": uuid.uuid4().hex, "generation": current["generation"], "member_id": person.user["id"], "permissions": 1}
    eventually(lambda: manager.request("POST", url + "/lifecycle", json=change, expected=(200, 202)).json()["complete"], timeout=120, interval=2)
    granted = person.request("GET", url).json()
    assert granted["permissions"] == 1
    # Changing the active resident's grants contains it before the next run.
    # The lifecycle receipt confirms the grant; execution readiness is separate.
    offer = person.request("GET", snapshot_url).json()["answerer"] if snapshot_url else None
    started = time.monotonic()
    await_execution_ready(person, timeout=90)
    return {"permissions": granted["permissions"], "execution_at_grant": granted["execution"], "offer_at_grant": offer, "ready_seconds": round(time.monotonic() - started, 3), "readiness_budget_seconds": 90}


def agent_draft(manager, thread):
    draft = manager.request("GET", SHARED + "/draft", params={"thread_id": thread}).json()
    assert draft["message_count"] >= 2 and len(draft["files"]) == 1
    assert draft["files"][0]["sha256"] == hashlib.sha256(ARTIFACT).hexdigest()
    return draft, {"thread_id": thread, "snapshot": draft["snapshot"], "acknowledge": {"audience": "company", "snapshot": draft["snapshot"]}}


def cut_at_publication(manager, compose, shared, draft, body):
    file = draft["files"][0]
    path = "Conversations/" + transcript_stem(draft["title"])[:80] + " · " + draft["snapshot"][:8] + "/" + file["path"]
    with ThreadPoolExecutor(max_workers=1) as pool:
        with cut_domain_write(compose, path, removal=False) as waiting:
            request = pool.submit(interrupted_request, manager, "POST", SHARED, {"json": body})
            eventually(waiting, timeout=60, interval=0.25)
            row, facts = snapshot(compose, shared, "publish", "", source_path="/mnt/user-data/outputs/acceptance.txt")
            assert facts["publication_order"] == "fact-before-name"
            assert facts["publication"]["path"] == path and facts["publication"]["sha256"] == file["sha256"]
            assert facts["publication"]["from_thread_id"] == body["thread_id"] and row["actor"] == manager.user["id"]
            assert compose.psql("SELECT count(*) FROM shared_publications WHERE path=" + sql(path)) == "0"
            compose.run("kill", "-s", "KILL", "gateway")
        return {"operation": row["operation"], "path": path, "facts": facts, "cutoff": request.result(timeout=30)}


def browser_share(compose, manager, source, digest):
    from playwright.sync_api import expect

    with signed_in_browser(compose, manager) as page:
        page.goto(compose.url + "/workspace/chats/" + source)
        page.get_by_role("button", name="Share conversation with everyone").click(timeout=60000)
        dialog = page.get_by_role("dialog")
        submit = dialog.get_by_role("button", name="Share", exact=True)
        expect(submit).to_be_disabled()
        expect(dialog.get_by_text("files/acceptance.txt", exact=False)).to_be_visible(timeout=30000)
        dialog.get_by_role("checkbox").check()
        page.screenshot(path=Path(required("EVIDENCE")) / "agent-sharing-confirm.png")
        with page.expect_response(lambda r: r.request.method == "POST" and urlsplit(r.url).path == SHARED) as posted:
            submit.click()
        response = posted.value
        assert response.status == 201 and response.request.post_data_json == {"thread_id": source, "snapshot": digest, "acknowledge": {"audience": "company", "snapshot": digest}}
        return response.json()["publication_id"]


def browser_continue(compose, colleague, publication, instance, nonce):
    from playwright.sync_api import expect

    with signed_in_browser(compose, colleague) as page:
        page.goto(compose.url + "/workspace/files/conversations/" + publication)
        button = page.get_by_role("button", name="Continue with " + instance["name"], exact=True)
        expect(button).to_be_visible(timeout=60000)
        expect(page.get_by_text("This access continues after removal from Company shared.", exact=False)).to_be_visible()
        page.screenshot(path=Path(required("EVIDENCE")) / "agent-sharing-continue.png")
        with page.expect_response(lambda r: r.request.method == "POST" and urlsplit(r.url).path == SHARED + "/" + publication + "/continue") as posted:
            button.click()
        response = posted.value
        assert response.ok and response.request.post_data_json["answerer"] == {"kind": "agent", "id": instance["id"]}
        result = response.json()
        page.wait_for_url("**/workspace/chats/" + result["thread_id"], timeout=30000)
        thread = result["thread_id"]
        (copy,) = result["files"]
        assert copy["status"] == "copied"
        attachment = copy["attachment"]
        expect(page.get_by_text(attachment["filename"], exact=True)).to_be_visible()
        expect(page.get_by_text(instance["name"] + " answers from here.", exact=True)).to_be_visible()
        page.get_by_placeholder(re.compile("how can i assist", re.I)).fill("acceptance:continued-report:" + nonce)
        with page.expect_response(lambda r: r.request.method == "POST" and urlsplit(r.url).path.endswith(f"/threads/{thread}/runs/stream"), timeout=30000) as streamed:
            page.get_by_role("button", name="Submit", exact=True).click()
        run = streamed.value
        assert run.ok
        match = re.fullmatch(rf"/api/threads/{re.escape(thread)}/runs/([0-9a-f-]{{36}})", run.headers.get("content-location", ""))
        assert match, "The streamed admission must identify this thread and run"
        chips = run.request.post_data_json["input"]["messages"][-1]["additional_kwargs"]["files"]
        assert chips == [attachment_chip(attachment)]
        expect(page.locator("[data-assistant-turn]").last.get_by_text("I read the shared report in my files.", exact=True)).to_be_visible(timeout=90000)
        run_path = f"/api/langgraph/threads/{thread}/runs/{match[1]}"
        eventually(lambda: colleague.request("GET", run_path).json()["status"] == "success", timeout=60, interval=1)
        return response.request.post_data_json, result, {"run_id": match[1], "file_path": chips[0]["path"], "via": "browser"}


def attachment_chip(attachment):
    return {"filename": attachment["filename"], "path": attachment["virtual_path"], "size": attachment["size"], "status": "uploaded"}


def test_manager_shares_a_colleagues_words_and_home_output(compose, session, member, colleague):
    result = {"backend": verify_image(compose, "gateway", required("BACKEND_REF"))}
    definition = session.request(
        "POST", "/api/agents", json={"name": "sharing-review", "model": "acceptance-model", "skills": [], "mcp_plugins": [], "allowed_subagents": [], "soul": "Use the available file tools for the requested report."}, expected=201
    ).json()
    instance = session.request("POST", INSTANCES, json={"creation_id": uuid.uuid4().hex, "name": "Report assistant", "definition_name": definition["name"], "custody": "company"}, expected=201).json()
    url = f"{INSTANCES}/{instance['id']}"
    grant_chat(session, instance, member)
    home = "/api/spaces/" + instance["home_id"]
    space = session.request("GET", home).json()
    session.request("PUT", home + "/content", params={"path": "uploads/input.txt", "generation": space["generation"], "operation_id": uuid.uuid4().hex, "create": True}, content=MARKER)
    sibling = b"Pre-existing Home file. Keep its original bytes."
    session.request("PUT", home + "/content", params={"path": "uploads/acceptance.txt", "generation": space["generation"], "operation_id": uuid.uuid4().hex, "create": True}, content=sibling)
    source = member.request("POST", url + "/conversations", json={"creation_id": uuid.uuid4().hex}, expected=201).json()["thread_id"]
    nonce = uuid.uuid4().hex
    assert member.run(source, message="acceptance:file:shared-" + nonce)["status"] == "success"
    member.request("GET", SHARED + "/draft", params={"thread_id": source}, expected=(403, 404))
    draft, body = agent_draft(session, source)
    assert draft["with"] == {"kind": "employee", "id": instance["id"], "name": instance["name"]}
    session.request("POST", SHARED, json={"thread_id": source, "snapshot": draft["snapshot"]}, expected=(403, 409, 422))
    signed = required("SNAPSHOT_QUALIFICATION_MODE") == "signed"
    if signed:
        result["frontend"] = verify_image(compose, "frontend", required("SNAPSHOT_BROWSER_IMAGE"))
        publication = browser_share(compose, session, source, draft["snapshot"])
    else:
        publication = session.request("POST", SHARED, json=body, expected=201).json()["publication_id"]
    entry = SHARED + "/" + publication
    before = colleague.request("GET", entry).json()
    assert before["with"] == draft["with"] and before["answerer"] == {"kind": "assistant", "reason": "no_access"}
    grant = grant_chat(session, instance, colleague, snapshot_url=entry)
    detail = colleague.request("GET", entry).json()
    expected = {"kind": "agent", "id": instance["id"], "name": instance["name"]}
    assert detail["answerer"] == expected and grant["offer_at_grant"] in (expected, {"kind": "assistant", "reason": "unavailable"})
    result["grant_readiness"] = {**grant, "offer_before_grant": before["answerer"], "qualified_offer": detail["answerer"]}

    old_offer = {"request_id": str(uuid.uuid4()), "answerer": {"kind": "assistant"}}
    previous_threads = compose.psql("SELECT count(*) FROM threads_meta")
    changed = colleague.request("POST", entry + "/continue", json=old_offer, expected=409).json()
    assert changed["detail"]["code"] == "answerer_changed" and changed["detail"]["answerer"]["id"] == instance["id"]
    assert compose.psql("SELECT count(*) FROM threads_meta") == previous_threads
    assert len(detail["messages"]) == draft["message_count"] and "audience" not in detail
    (file,) = detail["files"]
    assert file["available"] and file["sha256"] == draft["files"][0]["sha256"]
    request = {**old_offer, "answerer": {"kind": "agent", "id": instance["id"]}}
    if signed:
        request, created, first_turn = browser_continue(compose, colleague, publication, instance, nonce)
    else:
        created = colleague.request("POST", entry + "/continue", json=request).json()
    thread = created["thread_id"]
    assert thread != source and created["answerer"] == detail["answerer"]
    assert compose.psql("SELECT requester_id FROM agent_conversations WHERE thread_id=" + sql(thread) + " AND instance_id=" + sql(instance["id"])) == colleague.user["id"]
    assert compose.psql("SELECT count(*) FROM agent_protected_contexts WHERE thread_id=" + sql(thread)) == "0"
    state_url = "/api/langgraph/threads/{}/state"
    colleague.request("GET", state_url.format(thread))
    colleague.request("GET", state_url.format(source), expected=(403, 404))
    native_row = next(item for item in colleague.request("GET", "/api/shared").json()["files"] if item["publication_id"] == file["publication_id"])
    assert native_row["from_thread_id"] is None
    seed_url = "/api/threads/" + thread + "/shared-history"
    seed = colleague.request("GET", seed_url).json()
    assert seed["publication_id"] == publication and len(seed["message_ids"]) == len(detail["messages"])
    (copied,) = created["files"]
    assert copied["status"] == "copied" and copied["attachment"]["sha256"] == file["sha256"]
    saved = copied["attachment"]["virtual_path"].removeprefix("/mnt/user-data/")
    assert saved.startswith("uploads/") and session.request("GET", home + "/content", params={"path": saved}).content == ARTIFACT
    assert colleague.request("POST", entry + "/continue", json=request).json() == created
    attachment = copied["attachment"]
    assert attachment["source"] == {"kind": "shared_publication", "publication_id": file["publication_id"], "sha256": file["sha256"]}
    assert saved == "uploads/acceptance (2).txt"
    assert session.request("GET", home + "/content", params={"path": "uploads/acceptance.txt"}).content == sibling
    if not signed:
        chips = [attachment_chip(attachment)]
        first = colleague.run(thread, message="acceptance:continued-report:" + nonce, files=chips)
        assert first["status"] == "success"
        assert "I read the shared report in my files." in (Path(required("EVIDENCE")) / f"run-{first['run_id']}.sse").read_text()
        first_turn = {"run_id": first["run_id"], "file_path": chips[0]["path"], "via": "api"}
    result["first_turn"] = first_turn
    assert colleague.request("GET", seed_url).json() == seed
    result.update(source=source, publication=publication, destination=thread, message_count=len(seed["message_ids"]), member_file=file["publication_id"], copied_sha256=file["sha256"])

    # A later share has a different digest. Interrupt only its delivered Home file;
    # the complete, earlier snapshot remains available to test the older reader.
    assert member.run(source)["status"] == "success"
    second, second_body = agent_draft(session, source)
    assert second["snapshot"] != draft["snapshot"]
    shared = company(compose)
    inventory = json.loads(host(compose, "cat", "/srv/hartmesh/spaces/inventory.v2.json"))
    volume = next(v for v in inventory["volumes"] if v["slot_id"] == shared["slot"])
    assert volume["kind"] == "company" and volume["filesystem_uuid"] == shared["filesystem_uuid"]
    shared["control"] = Path(volume["mount_path"]) / "control"
    interrupted = cut_at_publication(session, compose, shared, second, second_body)

    older = Bundle()
    directory = Path(required("SNAPSHOT_FLOOR_DIR"))
    older.command = ["docker", "compose", "--project-directory", str(directory), "--env-file", required("HARTMESH_BUNDLE_ENV_FILE")]
    floor_image = next(ref for ref in (directory / "images.txt").read_text().splitlines() if "/hartmesh-backend@sha256:" in ref)
    start_gateway(older, session)
    try:
        result["floor"] = verify_image(older, "gateway", floor_image)
        eventually(lambda: phase(older, shared, interrupted["operation"]) == "complete|host-reconciled", timeout=150, interval=1)
        assert session.request("GET", "/api/shared/" + interrupted["path"]).content == ARTIFACT
        older_row = next(item for item in colleague.request("GET", "/api/shared").json()["files"] if item["publication_id"] == file["publication_id"])
        assert older_row["from_thread_id"] == source
        colleague.request("GET", state_url.format(thread))
        colleague.request("GET", state_url.format(source), expected=(403, 404))
        prior_detail = colleague.request("GET", entry).json()
        assert prior_detail["with"] == detail["with"] and prior_detail["answerer"] == {"kind": "assistant"}
        for key in ("messages", "files", "transcript"):
            assert prior_detail[key] == detail[key]
        assert publication in {row["publication_id"] for row in colleague.request("GET", SHARED).json()["conversations"]}
        assert colleague.request("GET", seed_url).json() == seed
        fallback = colleague.request("POST", entry + "/continue", json={**old_offer, "request_id": str(uuid.uuid4())}).json()
        assert fallback["answerer"] == {"kind": "assistant"} and fallback["thread_id"] not in (thread, source)
        assert compose.psql("SELECT count(*) FROM agent_conversations WHERE thread_id=" + sql(fallback["thread_id"])) == "0"
        session.request("DELETE", entry)
        assert session.request("GET", home + "/content", params={"path": saved}).content == ARTIFACT
        result.update(floor_fallback=fallback["thread_id"], interrupted_operation=interrupted["operation"], floor_resolution=phase(older, shared, interrupted["operation"]), copied_bytes_survived_removal=True)
    finally:
        stop_gateway(older)
        start_gateway(compose, session)
    assert verify_image(compose, "gateway", required("BACKEND_REF")) == result["backend"]
    result["complete"] = True
    (Path(required("EVIDENCE")) / "agent-sharing.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
