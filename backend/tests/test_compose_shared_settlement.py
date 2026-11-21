"""Qualify the shipped Shared writer and reader on the disposable mounted bundle."""

import hashlib
import json
import os
import re
import select
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest

if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("HARTMESH_REQUIRE_BUNDLE_INSTANCES") != "1":
    pytest.skip("requires the hosted mounted bundle", allow_module_level=True)

from _compose_shared_reader import retire_reader
from test_compose_instance_acceptance import await_execution_ready, company, eventually, mounted, required, sql
from test_compose_instance_acceptance import compose as compose
from test_compose_instance_acceptance import session as session

pytestmark = pytest.mark.live


@contextmanager
def cut_domain_write(compose, path, *, removal):
    """Only the disposable fixture name reaches this bounded, observable SQL barrier."""
    name = "acceptance_shared_" + uuid.uuid4().hex
    key = int(uuid.uuid4().hex[:7], 16)
    waiting = f"SELECT pid FROM pg_locks WHERE locktype='advisory' AND classid=186621 AND objid={key} AND objsubid=2 AND granted"
    event = "UPDATE OF removed_at" if removal else "INSERT"
    compose.psql(
        f"CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
        f"PERFORM pg_advisory_xact_lock(186621,{key}); PERFORM pg_sleep(90); RAISE EXCEPTION 'fixture cutoff expired'; END $$; "
        f"CREATE TRIGGER {name} BEFORE {event} ON shared_publications FOR EACH ROW WHEN (NEW.path={sql(path)}) EXECUTE FUNCTION {name}()"
    )
    try:
        yield lambda: compose.psql(waiting)
    finally:
        # An interrupted HTTP client can leave PostgreSQL running the statement
        # until its next socket check. End only the backend holding this fixture's lock.
        compose.psql(f"SELECT pg_terminate_backend(pid) FROM ({waiting}) AS fixture")
        compose.psql(f"DROP TRIGGER {name} ON shared_publications; DROP FUNCTION {name}()")


@contextmanager
def pending_read_window(control):
    # Hold the same shared lock as a real reader across startup. Readers may
    # enter, and the exclusive maintenance pass can try again after release.
    lock = control / ".host-workflow.lock"
    with subprocess.Popen(["sudo", "-n", "flock", "--shared", "--timeout", "5", str(lock), "sh", "-c", "echo ready && cat >/dev/null"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) as holder:
        try:
            assert select.select([holder.stdout], [], [], 10)[0] and holder.stdout.readline().strip() == "ready"
            yield
        finally:
            holder.stdin.close()
            assert holder.wait(timeout=10) == 0


def host(compose, *args):
    return compose.invoke(["sudo", "-n", *map(str, args)]).stdout.strip()


def snapshot(compose, shared, action, filename):
    criteria = (
        "space_id="
        + sql(shared["space"])
        + " AND phase='pending' AND request->>'namespace'='hm.shared' AND "
        + ("request->'source'->>'path'=" + sql("/mnt/user-data/uploads/" + filename) if action == "publish" else "request->'parameters'->>'path'=" + sql(filename))
    )
    raw = compose.psql("SELECT json_build_object('operation',operation_id,'request',request,'actor',actor_id) FROM storage_space_file_operations WHERE " + criteria)
    assert raw, "the native writer did not leave an exact pending intent"
    row = json.loads(raw)
    assert re.fullmatch("[0-9a-f]{32}", row["operation"])
    receipt = json.loads(host(compose, "cat", shared["control"] / ("host-workflow-" + row["operation"] + ".json")))
    assert receipt["schema_version"] == 1 and receipt["operation_id"] == row["operation"] and receipt["request"] == row["request"]
    return row, receipt["facts"]


def phase(compose, shared, operation):
    return compose.psql("SELECT phase||'|'||COALESCE(result->>'resolution','') FROM storage_space_file_operations WHERE space_id=" + sql(shared["space"]) + " AND operation_id=" + sql(operation))


def attachment_counts(compose, shared):
    return json.loads(
        compose.psql(
            "SELECT coalesce(json_agg(row_to_json(r)), '[]'::json) FROM ("
            "SELECT a.phase, m.alias, m.writable, count(*) AS count FROM storage_space_mounts m "
            "JOIN storage_space_attachments a ON a.id=m.attachment_id WHERE a.phase<>'fenced' AND m.space_id=" + sql(shared["space"]) + " GROUP BY a.phase,m.alias,m.writable ORDER BY a.phase,m.alias,m.writable) r"
        )
    )


def read_from_chat(compose, shared, name, content):
    # Ordinary chat read-only roots are Docker binds. Generic Space mounts are
    # separate database rows and stay subject to their own recovery fences.
    value = compose.invoke(["docker", "exec", shared["reader"], "cat", "/mnt/user-data/shared/" + name])
    assert value.stdout.encode() == content
    return hashlib.sha256(value.stdout.encode()).hexdigest()


def interrupted_request(session, method, url, kwargs):
    try:
        response = session.request(method, url, expected=(502, 503, 504), **kwargs)
        return str(response.status_code)
    except httpx.TransportError:
        return "disconnected"


def interrupt(compose, session, shared, *, name, body, publication, sibling, digest):
    removing = publication is not None
    action = "remove" if removing else "publish"
    sibling_name, sibling_bytes = sibling
    path = shared["data"] / name
    method, url, kwargs = ("DELETE", "/api/shared/" + name, {"params": {"expected_publication_id": publication}}) if removing else ("POST", "/api/shared/publish", {"json": body})
    target = "/api/shared/" + name
    receipt = {"action": action, "attachments_at_cutoff": attachment_counts(compose, shared)}
    receipt["chat_read_before"] = read_from_chat(compose, shared, sibling_name, sibling_bytes)
    with ThreadPoolExecutor(max_workers=1) as client:
        with cut_domain_write(compose, name, removal=removing) as waiting:
            reply = client.submit(interrupted_request, session, method, url, kwargs)
            eventually(waiting, timeout=45, interval=0.25)
            row, facts = snapshot(compose, shared, action, name)
            assert row["actor"] == session.user["id"]
            operation = row["operation"]
            receipt["operation"] = operation
            if removing:
                assert "publication_order" not in facts
                assert facts["removal"]["path"] == name and facts["removal"]["publication_id"] == publication
                journal = facts["removal"]["journal"]
                assert re.fullmatch("remove-[a-f0-9]{32}", journal)
                staged = shared["control"] / "shared" / journal / "payload"
                host(compose, "test", "!", "-e", path)
                assert host(compose, "sha256sum", staged).split()[0] == digest
            else:
                device, inode, size = map(int, host(compose, "stat", "-c", "%d %i %s", path).split())
                assert facts["publication_order"] == "fact-before-name"
                assert facts["publication_identity"] == {"device": device, "inode": inode}
                publication_id = facts["publication"]["publication_id"]
                assert str(uuid.UUID(publication_id)) == publication_id
                assert facts["publication"] == {
                    "publication_id": publication_id,
                    "path": name,
                    "size": size,
                    "sha256": digest,
                    "published_by": session.user["id"],
                    "from_path": body["path"],
                    "from_thread_id": body["thread_id"],
                }
                assert host(compose, "sha256sum", path).split()[0] == digest
                assert compose.psql("SELECT count(*) FROM shared_publications WHERE path=" + sql(name)) == "0"
                receipt.update(native_identity=facts["publication_identity"], publication_id=publication_id, publication_order=facts["publication_order"])
            compose.run("kill", "-s", "KILL", "gateway")
        receipt["http_cutoff"] = reply.result(timeout=30)
    with pending_read_window(shared["control"]):
        restart_started = time.monotonic()
        compose.run("up", "-d", "--wait", "--no-deps", "gateway", timeout=240)
        assert phase(compose, shared, operation) == "pending|"
        status = compose.ai("status")
        assert any(item["operation_id"] == operation and item["space_id"] == shared["space"] for item in status["pending_operations"])
        receipt["chat_read_while_pending"] = read_from_chat(compose, shared, sibling_name, sibling_bytes)
        receipt["generic_attachments_while_pending"] = attachment_counts(compose, shared)
        if removing:
            names = {item["path"] for item in session.request("GET", "/api/shared").json()["files"]}
            assert sibling_name in names and name not in names
            assert session.request("GET", "/api/shared/" + sibling_name).content == sibling_bytes
            session.request("GET", target, expected=404)
            assert phase(compose, shared, operation) == "pending|" and host(compose, "sha256sum", staged).split()[0] == digest
            receipt["sibling_read_during_pending_removal"] = True
    eligible = time.monotonic()
    expected = "failed|host-settled" if removing else "complete|host-reconciled"
    eventually(lambda: phase(compose, shared, operation) == expected, timeout=150, interval=1)
    observed = time.monotonic()
    receipt["restart_to_terminal_seconds"] = observed - restart_started
    receipt["reader_release_to_terminal_seconds"] = observed - eligible
    assert 0 <= receipt["reader_release_to_terminal_seconds"] < 150
    if not removing:
        stored_facts = compose.psql("SELECT result->'facts' FROM storage_space_file_operations WHERE space_id=" + sql(shared["space"]) + " AND operation_id=" + sql(operation))
        assert json.loads(stored_facts) == facts
        receipt["settlement_retained_publication_facts"] = True
    receipt["settled"] = expected
    receipt["chat_read_after"] = read_from_chat(compose, shared, sibling_name, sibling_bytes)
    receipt["attachments_after_settlement"] = attachment_counts(compose, shared)
    assert hashlib.sha256(session.request("GET", target).content).hexdigest() == digest
    published = [item for item in session.request("GET", "/api/shared").json()["files"] if item["path"] == name]
    assert len(published) == 1 and published[0]["publication_id"] == (publication or receipt["publication_id"])
    return receipt


def test_interrupted_shared_write_preserves_proven_bytes_and_sibling(compose, session):
    image = required("BACKEND_REF")
    running = compose.run("ps", "-q", "gateway").stdout.strip()
    assert compose.invoke(["docker", "inspect", "--format", "{{.Image}}", running]).stdout.strip() == compose.invoke(["docker", "image", "inspect", "--format", "{{.Id}}", image]).stdout.strip()
    shared = company(compose)
    inventory = json.loads(host(compose, "cat", "/srv/hartmesh/spaces/inventory.v2.json"))
    entry = next(item for item in inventory["volumes"] if item["slot_id"] == shared["slot"])
    assert entry["kind"] == "company" and entry["filesystem_uuid"] == shared["filesystem_uuid"]
    root = Path(entry["mount_path"])
    shared.update(data=root / "data/shared", control=root / "control")
    suffix = uuid.uuid4().hex
    name, sibling = "report-" + suffix + ".txt", "notes-" + suffix + ".txt"
    content, sibling_bytes = ("Report " + suffix).encode(), ("Notes " + suffix).encode()
    digest = hashlib.sha256(content).hexdigest()
    thread = session.thread()
    for leaf, material in ((sibling, sibling_bytes), (name, content)):
        session.upload(thread, material, name=leaf)

    def source(leaf):
        return {"thread_id": thread, "path": "/mnt/user-data/uploads/" + leaf}

    session.request("POST", "/api/shared/publish", json=source(sibling), expected=201)
    reader = session.thread()
    session.upload(reader)
    session.run(reader)
    sandbox = compose.sandbox(reader)
    binding = mounted(sandbox, "/mnt/user-data/shared")
    assert binding["RW"] is False and binding["Source"] == str(shared["data"])
    assert sandbox["Config"]["Labels"]["hartmesh.storage.space." + shared["slot"]] == "1"
    shared["reader"] = sandbox["Id"]
    evidence = {"chat_readonly_container": shared["reader"], "chat_thread": reader, "backend": image, "database": "postgresql", "slot": shared["slot"], "sha256": digest, "operations": []}
    output = Path(required("EVIDENCE")) / "shared-settlement.json"
    publication = None
    try:
        for _ in range(2):
            result = interrupt(compose, session, shared, name=name, body=source(name), publication=publication, sibling=(sibling, sibling_bytes), digest=digest)
            evidence["operations"].append(result)
            publication = evidence["operations"][0]["publication_id"]
        next_write = time.monotonic()
        session.request("DELETE", "/api/shared/" + name, params={"expected_publication_id": publication})
        evidence["subsequent_remove_seconds"] = time.monotonic() - next_write
        session.request("GET", "/api/shared/" + name, expected=404)
        next_write = time.monotonic()
        fresh = session.request("POST", "/api/shared/publish", json=source(name), expected=201).json()
        evidence["subsequent_publish_seconds"] = time.monotonic() - next_write
        assert fresh["path"] == name and fresh["publication_id"] != publication and session.request("GET", "/api/shared/" + name).content == content
        evidence["subsequent_publication"] = fresh["publication_id"]
        evidence["pending_after"] = compose.ai("status")["file_operations_pending"]
        assert evidence["pending_after"] == 0
        retire_reader(compose, session, reader, shared["reader"], evidence, wait_ready=await_execution_ready)
    finally:
        output.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
