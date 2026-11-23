"""Use an installed, disposable Gateway to finish one conversation share across three restarts."""

import hashlib
import json
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("HARTMESH_VERIFY_SHARED_SNAPSHOTS") != "1" or os.environ.get("HARTMESH_REQUIRE_BUNDLE_INSTANCES") != "1":
    pytest.skip("requires the hosted snapshot publication rehearsal", allow_module_level=True)

from _compose_browser_session import verify_image
from _compose_shared_reader import POLICY
from test_compose_instance_acceptance import ARTIFACT, await_execution_ready, company, eventually, required, sql
from test_compose_instance_acceptance import compose as compose
from test_compose_instance_acceptance import session as session
from test_compose_shared_settlement import cut_domain_write, host, interrupted_request, phase, snapshot

from app.gateway.transcript import transcript_stem

pytestmark = pytest.mark.live
PREFIX = "/api/shared-conversations"


def interrupt_member(compose, session, shared, body, path, source, evidence, save):
    evidence.update(path=path, source=source)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with cut_domain_write(compose, path, removal=False) as waiting:
            request = pool.submit(interrupted_request, session, "POST", PREFIX, {"json": body})
            eventually(waiting, timeout=45, interval=0.25)
            row, facts = snapshot(compose, shared, "publish", "", source_path=source)
            operation = row["operation"]
            expected = facts["publication"]
            inode, device, size = map(int, host(compose, "stat", "-c", "%i %d %s", shared["data"] / path).split())
            digest = host(compose, "sha256sum", shared["data"] / path).split()[0]
            assert facts["publication_identity"] == {"inode": inode, "device": device} and facts["publication_order"] == "fact-before-name"
            assert row["actor"] == session.user["id"] and row["request"]["operation"] == "publish"
            publication_id = expected["publication_id"]
            assert str(uuid.UUID(publication_id)) == publication_id and expected == {
                "publication_id": publication_id,
                "from_path": source,
                "path": path,
                "size": size,
                "sha256": digest,
                "published_by": session.user["id"],
                "from_thread_id": body["thread_id"],
            }
            assert compose.psql("SELECT count(*) FROM shared_publications WHERE path=" + sql(path)) == "0"
            evidence.update(operation_id=operation, publication_id=publication_id, sha256=digest, size=size, publication_order=facts["publication_order"], identity=facts["publication_identity"])
            save()
            compose.run("kill", "-s", "KILL", "gateway")
        evidence["cutoff"] = request.result(timeout=30)
    started = time.monotonic()
    compose.run("up", "-d", "--wait", "--no-deps", "gateway", timeout=240)
    eventually(lambda: phase(compose, shared, operation) == "complete|host-reconciled", timeout=150, interval=1)
    evidence["restart_to_settlement_seconds"] = time.monotonic() - started
    stored = compose.psql("SELECT result->'facts' FROM storage_space_file_operations WHERE space_id=" + sql(shared["space"]) + " AND operation_id=" + sql(operation))
    assert json.loads(stored) == facts
    evidence["settled"] = "complete|host-reconciled"
    save()
    # The ordinary source sandbox's Redis owner may need to expire and be
    # re-observed after each abrupt stop before normal execution is available.
    policy = json.loads(compose.gateway("python", "-c", POLICY))
    assert policy["store"] in {"memory", "redis"}
    budget = (2 * policy["lease_seconds"] if policy["store"] == "redis" else 0) + 2 * policy["recheck_seconds"] + policy["startup_seconds"]
    started = time.monotonic()
    await_execution_ready(session, timeout=budget)
    evidence.update(readiness_budget_seconds=budget, readiness_seconds=time.monotonic() - started)
    assert hashlib.sha256(session.request("GET", "/api/shared/" + path).content).hexdigest() == digest
    save()


def continue_and_repeat(session, detail, file):
    publication_id = detail["publication_id"]
    body = {"request_id": str(uuid.uuid4()), "answerer": {"kind": "assistant"}}
    url = PREFIX + "/" + publication_id + "/continue"
    first = session.request("POST", url, json=body).json()
    thread = first["thread_id"]
    assert str(uuid.UUID(thread)) == thread and first["answerer"] == body["answerer"]
    (outcome,) = first["files"]
    assert outcome["status"] == "copied" and outcome["path"] == file["path"]
    attachment = outcome["attachment"]
    assert attachment["filename"] == "acceptance.txt" and attachment["size"] == len(ARTIFACT) and attachment["sha256"] == file["sha256"]
    source = detail["files"][0]["publication_id"]
    assert attachment["source"]["kind"] == "shared_publication" and attachment["source"]["publication_id"] == source
    artifact_url = "/api/threads/" + thread + "/artifacts/mnt/user-data/uploads/acceptance.txt"
    assert attachment["artifact_url"] == artifact_url
    assert session.request("GET", artifact_url).content == ARTIFACT
    history_url = "/api/threads/" + thread + "/shared-history"
    seed = session.request("GET", history_url).json()
    assert seed["publication_id"] == publication_id and len(seed["message_ids"]) == len(set(seed["message_ids"])) == len(detail["messages"])
    assert session.request("POST", url, json=body).json() == first
    assert session.request("GET", history_url).json() == seed
    listing = session.request("GET", "/api/threads/" + thread + "/uploads/list").json()
    assert [f["filename"] for f in listing["files"]] == ["acceptance.txt"]
    assert hashlib.sha256(session.request("GET", artifact_url).content).hexdigest() == file["sha256"]
    return {
        "thread_id": thread,
        "request_id": body["request_id"],
        "publication_id": publication_id,
        "seed_count": len(seed["message_ids"]),
        "file_count": len(listing["files"]),
        "artifact_sha256": file["sha256"],
        "artifact_size": attachment["size"],
    }


def verify_member_links(session, thread, detail):
    member = detail["files"][0]
    link = f"{PREFIX}/{detail['publication_id']}/members/{member['publication_id']}"
    plain = "/api/shared/" + member["shared_path"]
    for download, disposition in ((False, "inline"), (True, "attachment")):
        opened = session.request("GET", link, params={"download": download})
        assert opened.content == ARTIFACT and opened.headers["content-disposition"].startswith(disposition)
    session.request("DELETE", plain, params={"expected_publication_id": member["publication_id"]})
    for download in (False, True):
        session.request("GET", link, params={"download": download}, expected=410)
    replacement = session.request(
        "POST",
        "/api/shared/publish",
        json={"thread_id": thread, "path": "/mnt/user-data/outputs/acceptance.txt", "folder": member["shared_path"].rsplit("/", 1)[0]},
        expected=201,
    ).json()
    assert replacement["path"] == member["shared_path"] and replacement["publication_id"] != member["publication_id"]
    for download in (False, True):
        refused = session.request("GET", link, params={"download": download}, expected=410)
        assert "no longer available" in refused.text
    assert session.request("GET", plain).content == ARTIFACT
    session.request("DELETE", plain, params={"expected_publication_id": replacement["publication_id"]})
    return {"selected": member["publication_id"], "replacement": replacement["publication_id"], "sha256": member["sha256"], "stale_open_status": 410, "stale_download_status": 410}


def test_the_same_share_survives_interruption_of_an_output_transcript_and_manifest(compose, session):
    ref = required("BACKEND_REF")
    evidence = {"backend": verify_image(compose, "gateway", ref), "run_id": required("GITHUB_RUN_ID"), "attempt": required("GITHUB_RUN_ATTEMPT"), "head": required("GITHUB_SHA"), "operations": []}
    image_id = evidence["backend"]["image_id"]
    revision = compose.invoke(["docker", "image", "inspect", "--format", '{{index .Config.Labels "org.opencontainers.image.revision"}}', image_id]).stdout.strip()
    assert re.fullmatch("[0-9a-f]{40}", revision)
    assert revision == required("SNAPSHOT_BACKEND_REVISION")
    evidence.update(revision=revision, mode=required("SNAPSHOT_QUALIFICATION_MODE"))
    report = Path(required("EVIDENCE")) / "snapshot-publications.json"

    def save():
        report.write_text(json.dumps(evidence, indent=2), encoding="utf-8")

    try:
        shared = company(compose)
        inventory = json.loads(host(compose, "cat", "/srv/hartmesh/spaces/inventory.v2.json"))
        entry = next(e for e in inventory["volumes"] if e["slot_id"] == shared["slot"])
        assert entry["kind"] == "company" and entry["filesystem_uuid"] == shared["filesystem_uuid"]
        root = Path(entry["mount_path"])
        shared.update(data=root / "data/shared", control=root / "control")
        assert {"read", "publish", "continue"} <= set(session.request("GET", "/api/features").json()["shared_conversations"]["operations"])
        thread = session.thread()
        session.upload(thread)
        run = session.run(thread)
        evidence.update(thread_id=thread, source_run=run["run_id"])
        draft = session.request("GET", PREFIX + "/draft", params={"thread_id": thread}).json()
        assert draft["message_count"] >= 2 and not draft["already_shared"]
        (file,) = draft["files"]
        assert file["path"] == "files/acceptance.txt" and file["sha256"] == hashlib.sha256(ARTIFACT).hexdigest() and file["size"] == len(ARTIFACT)
        digest = draft["snapshot"]
        folder = "Conversations/" + transcript_stem(draft["title"])[:80] + " · " + digest[:8]
        body = {"thread_id": thread, "snapshot": digest}
        assert not session.request("GET", PREFIX).json()["conversations"]
        for leaf, source in [(file["path"], "/mnt/user-data/outputs/acceptance.txt"), ("transcript.md", "/conversation-snapshot/transcript"), ("conversation.json", "/conversation-snapshot/manifest")]:
            op = {}
            evidence["operations"].append(op)
            interrupt_member(compose, session, shared, body, folder + "/" + leaf, source, op, save)
            assert verify_image(compose, "gateway", ref) == evidence["backend"]
            conversations = session.request("GET", PREFIX).json()["conversations"]
            if leaf == "conversation.json":
                assert len(conversations) == 1 and conversations[0]["publication_id"] == op["publication_id"]
            else:
                assert conversations == []
        output, transcript, manifest = evidence["operations"]
        assert output["sha256"] == file["sha256"] and manifest["sha256"] == digest
        receipt = session.request("POST", PREFIX, json=body).json()
        assert receipt["already_shared"] and receipt["publication_id"] == manifest["publication_id"] and receipt["created_publication_ids"] == []
        detail = session.request("GET", PREFIX + "/" + receipt["publication_id"]).json()
        assert detail["transcript"]["available"] and detail["transcript"]["publication_id"] == transcript["publication_id"] and detail["transcript"]["sha256"] == transcript["sha256"]
        assert detail["files"][0]["available"] and detail["files"][0]["publication_id"] == output["publication_id"]
        assert detail["messages"][0]["type"] == "human" and detail["messages"][-1]["type"] == "ai"
        evidence.update(retried_without_duplicates=True, visible_messages=len(detail["messages"]), pending_after=len(compose.ai("status")["pending_operations"]))
        assert evidence["pending_after"] == 0
        evidence["continuation"] = continue_and_repeat(session, detail, file)
        assert evidence["continuation"]["thread_id"] != thread
        evidence["member_links"] = verify_member_links(session, thread, detail)
        save()
        for created in (evidence["continuation"]["thread_id"], thread):
            assert session.request("DELETE", "/api/threads/" + created).json()["success"] is True
    finally:
        save()
