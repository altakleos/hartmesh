"""In-place downgrade qualification on the disposable, hosted bundle."""

import json
import os
import uuid
from pathlib import Path

import httpx
import pytest

if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("HARTMESH_VERIFY_HOME_ROLLBACK") != "1":
    pytest.skip("requires the hosted rollback rehearsal", allow_module_level=True)

from test_compose_instance_acceptance import MARKER, AllocationBarrier, Bundle, artifact, await_execution_ready, eventually, required, sql
from test_compose_instance_acceptance import compose as compose
from test_compose_instance_acceptance import session as session

pytestmark = pytest.mark.live


def stop_gateway(bundle):
    gateway = bundle.run("ps", "--no-trunc", "--quiet", "gateway").stdout.strip()
    assert len(gateway) == 64
    bundle.run("stop", "-t", "120", "gateway", timeout=150)
    (entry,) = json.loads(bundle.invoke(["docker", "inspect", gateway]).stdout)
    state = entry["State"]
    assert entry["Id"] == gateway and state["Status"] == "exited" and state["ExitCode"] in (0, 143) and not state["OOMKilled"], state
    log = bundle.invoke(["docker", "logs", "--since", state["StartedAt"], gateway])
    assert "Application shutdown complete." in log.stderr + log.stdout


def start_gateway(bundle, session):
    bundle.run("up", "-d", "--wait", "--no-deps", "gateway", timeout=300)
    # The shipped non-root master uses the rendered config and its /tmp PID.
    # After a replacement the previous worker can briefly retain a DNS answer.
    bundle.run("exec", "-T", "nginx", "nginx", "-c", "/tmp/nginx.conf", "-s", "reload")

    def through_proxy():
        try:
            response = session.request("GET", "/health/ready", expected=(200, 502, 503, 504), timeout=5)
        except httpx.TransportError:
            return False
        return response.status_code == 200 and response.json()["status"] == "ready"

    eventually(through_proxy, timeout=30, interval=1)
    gateway = bundle.run("ps", "--no-trunc", "--quiet", "gateway").stdout.strip()
    (entry,) = json.loads(bundle.invoke(["docker", "inspect", gateway]).stdout)
    return entry


def offline_check(bundle, *, expected):
    result = bundle.run(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "--entrypoint",
        "sh",
        "gateway",
        "/opt/hartmesh/gateway/as-user.sh",
        "/app/backend/.venv/bin/python",
        "-m",
        "app.gateway.agent_instances",
        "check-rollback",
        check=False,
    )
    body = json.loads(result.stdout.splitlines()[-1])
    assert body["command"] == "check-rollback" and result.returncode == body["returncode"] == expected, body
    return body


def add_pending(bundle, space, source, owner, batch, count):
    # Generate durable rows with the same shape as a pending cross-Space copy.
    # All paths and operation IDs belong exclusively to this rehearsal.
    bundle.psql(
        "INSERT INTO storage_space_file_operations(space_id,operation_id,actor_kind,actor_id,generation,phase,request,created_at) "
        f"SELECT d.id, {sql(batch)} || lpad(to_hex(n),8,'0'), 'human', {sql(owner)}, d.generation, 'pending', "
        "json_build_object('action','copy','source_id',s.id,'source_generation',s.generation,'source_path','rollback-source',"
        "'destination_id',d.id,'destination_path','rollback-target-' || n,'acknowledge_disclosure',true), now() "
        f"FROM storage_spaces d, storage_spaces s, generate_series(1,{count}) AS n WHERE s.id={sql(source)} AND d.id={sql(space)}"
    )


def give_marker(person, agent):
    person.request(
        "PUT",
        f"/api/agent-instances/{agent['id']}/home/content",
        params={"path": "uploads/input.txt", "operation_id": uuid.uuid4().hex},
        headers={"Content-Type": "application/octet-stream"},
        content=MARKER,
    )


def conversation(person, agent):
    return person.request("POST", f"/api/agent-instances/{agent['id']}/conversations", json={"creation_id": uuid.uuid4().hex}, expected=201).json()["thread_id"]


@pytest.mark.parametrize("version", ["61", "60"])
def test_reuse_checked_home_data_on_older_build(compose, session, version):
    older = Bundle()
    directory = Path(f"/opt/hartmesh-rollback-{version}")
    older.command = ["docker", "compose", "--project-directory", str(directory), "--env-file", required("HARTMESH_BUNDLE_ENV_FILE")]
    expected_image = next(ref for ref in (directory / "images.txt").read_text(encoding="utf-8").splitlines() if "/hartmesh-backend@sha256:" in ref)
    employees = json.loads(compose.psql("SELECT json_agg(row_to_json(a) ORDER BY a.home_id) FROM (SELECT id, home_id, custody FROM agent_instances WHERE status='active') a"))
    unaffected = next(agent for agent in employees if agent["custody"] == "company")
    others = [agent for agent in employees if agent != unaffected]
    quiet, affected = others[0], others[-1]
    assert quiet["home_id"] < affected["home_id"]
    user_space = compose.role(session.user["id"])["space"]
    shared = compose.psql("SELECT space_id FROM hm_storage_feature_links WHERE namespace='hm.company-space' AND key='company'")
    batch = uuid.uuid4().hex[:24]
    evidence = Path(required("EVIDENCE")) / f"rollback-{version}.json"
    initial_closures = [compose.ai("close-workspace", agent["id"])["verdict"] for agent in (quiet, unaffected)]
    assert set(initial_closures) <= {"closed", "already_closed"}
    if "closed" in initial_closures:
        stop_gateway(compose)
        start_gateway(compose, session)
        await_execution_ready(session)
    give_marker(session, affected)
    give_marker(session, unaffected)
    session.run(conversation(session, affected))
    stop_gateway(compose)
    add_pending(compose, quiet["home_id"], shared, session.user["id"], batch, 200)
    add_pending(compose, affected["home_id"], shared, session.user["id"], batch, 1)
    add_pending(compose, user_space, shared, session.user["id"], batch, 1)
    count = int(compose.psql("SELECT count(*) FROM storage_space_file_operations WHERE phase='pending'"))
    assert count >= 202
    blocked = offline_check(compose, expected=2)
    assert blocked["total"] == blocked["checked"] == count and blocked["pages"] >= 2
    assert any(row["space_id"] == affected["home_id"] and row["action"] == "copy" and row["attachments"] for row in blocked["blockers"])
    (evidence.parent / f"rollback-{version}-refusal.json").write_text(json.dumps(blocked, sort_keys=True), encoding="utf-8")

    start_gateway(compose, session)
    await_execution_ready(session)
    summary = compose.ai("status")
    assert summary["pending_operations_truncated"] and len(summary["pending_operations"]) == 200
    assert all(row["space_id"] != affected["home_id"] for row in summary["pending_operations"])
    page = compose.ai("pending")
    rows = list(page["operations"])
    while page["next_after"] is not None:
        page = compose.ai("pending", "--after", page["next_after"])
        rows.extend(page["operations"])
    assert len({(row["space_id"], row["operation_id"]) for row in rows}) == len(rows) == count
    assert any(row["space_id"] == user_space and row["instance_id"] is None for row in rows)
    recorded = next(row for row in rows if row["instance_id"] == affected["id"] and row["operation_id"] == batch + "00000001")
    closed = compose.ai("close-workspace", affected["id"])
    assert closed["verdict"] == "closed" and {a["attachment_id"] for a in closed["attachments"]} == {a["attachment_id"] for a in recorded["attachments"]}
    # A previously admitted non-web writer can still be running when the
    # Gateway exits. Its uncommitted operation must prevent a clean verdict.
    with AllocationBarrier(compose, lock_key="external-writer-" + batch) as writer:
        new_id = batch + "00000002"
        insert = (
            "BEGIN; INSERT INTO storage_space_file_operations(space_id,operation_id,actor_kind,actor_id,generation,phase,request,created_at) "
            f"SELECT space_id,{sql(new_id)},actor_kind,actor_id,generation,phase,request,now() FROM storage_space_file_operations "
            f"WHERE space_id={sql(user_space)} AND operation_id={sql(batch + '00000001')};\n"
        )
        writer.process.stdin.write(insert.encode("ascii"))
        writer.process.stdin.flush()
        eventually(lambda: compose.psql(f"SELECT state FROM pg_stat_activity WHERE pid={writer.identity['pid']}") == "idle in transaction", timeout=25, interval=1)
        stop_gateway(compose)
        refused = offline_check(compose, expected=1)
        assert "lock" in refused["error"].lower(), refused
        writer.process.stdin.write(b"COMMIT;\n")
        writer.process.stdin.flush()
    assert writer.drained
    count += 1
    # Operator closure fences SQL and removes the container. The current
    # Gateway owns journal retirement. Recover it while producers stay fenced,
    # then take the final offline inventory on the state the older build gets.
    current = start_gateway(compose, session)
    assert current["Config"]["Image"] == required("BACKEND_REF")
    current_readiness = await_execution_ready(session)
    stop_gateway(compose)
    cleared = offline_check(compose, expected=0)
    assert cleared["checked"] == cleared["total"] == count and cleared["pages"] >= 2 and cleared["blocked"] == 0

    entry = start_gateway(older, session)
    assert entry["Config"]["Image"] == expected_image and entry["State"]["Health"]["Status"] == "healthy"
    readiness = await_execution_ready(session)
    legacy_status = older.ai("status")
    assert legacy_status["file_operations_pending"] == count
    thread = conversation(session, unaffected)
    turn = session.run(thread)
    artifact(session, thread)
    evidence.write_text(
        json.dumps(
            {
                "version": version,
                "qualification": "older Gateway on the checked storage",
                "backend": expected_image,
                "image_id": entry["Image"],
                "gateway": entry["Id"],
                "first_check": blocked,
                "operator_closure": closed,
                "current_gateway": current["Config"]["Image"],
                "current_readiness_after_closure": current_readiness,
                "final_check": cleared,
                "readiness": readiness,
                "writer_refusal": refused,
                "older_pending_count": legacy_status["file_operations_pending"],
                "unaffected_employee": unaffected["id"],
                "thread_id": thread,
                "run_id": turn["run_id"],
                "result": turn["status"],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    older.ai("close-workspace", unaffected["id"])
    stop_gateway(older)
    # Remove only this case's synthetic records after the older build has drained.
    for space in (quiet["home_id"], affected["home_id"], user_space):
        compose.psql(f"DELETE FROM storage_space_file_operations WHERE space_id={sql(space)} AND left(operation_id,24)={sql(batch)}")
    start_gateway(compose, session)
    await_execution_ready(session)
