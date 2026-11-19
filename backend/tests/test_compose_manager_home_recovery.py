"""Observe manager acceptance while the last model response is in flight."""

import hashlib
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest

if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("HARTMESH_REQUIRE_BUNDLE_INSTANCES") != "1":
    pytest.skip("requires the hosted mounted bundle and scripted model", allow_module_level=True)

from test_compose_instance_acceptance import (
    activate,
    artifact,
    await_attempt,
    await_execution_ready,
    container_absent,
    delegate,
    eventually,
    new_conversation,
    read_workspace_attachment,
    required,
    sql,
    work_path,
    workspace,
)
from test_compose_instance_acceptance import compose as compose
from test_compose_instance_acceptance import session as session

pytestmark = pytest.mark.live


def pending_copy(compose, session, home):
    operation = uuid.uuid4().hex
    source = compose.role(session.user["id"])["space"]
    compose.psql(
        "INSERT INTO storage_space_file_operations(space_id,operation_id,actor_kind,actor_id,generation,phase,request,created_at) "
        f"SELECT h.id,{sql(operation)},'human',{sql(session.user['id'])},h.generation,'pending',"
        "json_build_object('action','copy','source_id',s.id,'source_generation',s.generation,'source_path','input.txt',"
        "'destination_id',h.id,'destination_path','uploads/recovery-example.txt','acknowledge_disclosure',true),now() - interval '6 minutes' "
        f"FROM storage_spaces h, storage_spaces s WHERE h.id={sql(home)} AND s.id={sql(source)}"
    )
    return operation


def stream_run(session, path, joined):
    with session.client.stream("GET", path + "/stream", timeout=120) as response:
        response.raise_for_status()
        joined.set()
        return response.read()


def streamed_answer(payload, marker):
    chunks = {}
    for block in payload.decode().replace("\r\n", "\n").split("\n\n"):
        fields = dict(line.split(": ", 1) for line in block.splitlines() if ": " in line)
        if fields.get("event") not in ("messages", "values", "updates"):
            continue
        data = json.loads(fields["data"])
        if fields["event"] == "messages":
            message = data[0]
            if message.get("type") == "AIMessageChunk" and isinstance(message.get("content"), str):
                key = message["id"]
                chunks[key] = chunks.get(key, "") + message["content"]
                if marker in chunks[key]:
                    return True
        else:
            snapshots = [data] if fields["event"] == "values" else data.values()
            for snapshot in snapshots:
                if isinstance(snapshot, dict) and any(m.get("type") == "ai" and marker in str(m.get("content")) for m in snapshot.get("messages", [])):
                    return True
    return False


@pytest.mark.parametrize("workload", ["idle", "chat", "assignment"])
def test_manager_accepts_files_with_a_retained_workspace(compose, session, workload):
    await_execution_ready(session)
    agent = json.loads(compose.psql(f"SELECT json_build_object('id',id,'home_id',home_id) FROM agent_instances WHERE status='active' AND custody='personal' AND owner_id={sql(session.user['id'])} AND name='Qualification AI assistant'"))
    home = agent["home_id"]
    generation = session.request("GET", f"/api/spaces/{home}").json()["generation"]
    context = new_conversation(session, {"instance": agent})
    session.run(context["thread_id"])
    original, _ = workspace(compose, context)
    context = new_conversation(session, context)
    thread = context["thread_id"]
    directory = Path(required("EVIDENCE"))
    token = uuid.uuid4().hex
    armed, waiting = directory / ("armed-" + token), directory / ("waiting-" + token)
    assert not armed.exists() and not waiting.exists()
    chat = work = stream = None
    facts = {
        "backend": required("BACKEND_REF"),
        "workload": workload,
        "original": original,
    }
    with ThreadPoolExecutor(max_workers=2) as clients:
        try:
            if workload != "idle":
                armed.touch()
                if workload == "chat":
                    chat = clients.submit(
                        session.run,
                        thread,
                        success=False,
                        message="acceptance:file:final:" + token,
                    )
                else:
                    work = delegate(session, context, "acceptance:work:final:" + token)
                    activate(session, context, work)
                eventually(waiting.is_file, timeout=45, interval=0.1)
                assert workspace(compose, context)[0] == original
                live = json.loads(
                    compose.psql(f"SELECT json_build_object('id',run_id,'user',user_id,'incarnation',run_incarnation) FROM runs WHERE thread_id={sql(thread)} AND status='running' AND metadata_json->>'agent_instance_id'={sql(agent['id'])}")
                )
                run_id = live["id"]
            if work:
                attempt = session.request("GET", work_path(context, work["id"])).json()["attempt"]
                assert attempt["run_id"] == run_id and compose.psql("SELECT attachment_id FROM agent_work_attempts WHERE id=" + sql(attempt["id"])) == original["attachment_id"]
            if chat or work:
                path = f"/api/langgraph/threads/{thread}/runs/{run_id}"
                joined = Event()
                stream = clients.submit(stream_run, session, path, joined)
                assert joined.wait(10)
            operation = pending_copy(compose, session, home)
            session.request(
                "POST",
                f"/api/spaces/{home}/recovery",
                json={
                    "generation": generation,
                    "operation_id": operation,
                    "acknowledge_uncertain_outcome": True,
                },
            )
            predicate = f"space_id={sql(home)} AND operation_id={sql(operation)} AND generation={generation}"
            outcome = compose.psql("SELECT phase,result->>'resolution',result->>'actor_kind',result->>'actor_id' FROM storage_space_file_operations WHERE " + predicate)
            assert outcome == "failed|owner-accepted-current-state|human|" + session.user["id"]
            closed = read_workspace_attachment(compose, original["attachment_id"], timeout=10)
            assert closed["phase"] == "fenced" and closed["fence_reason"] == "storage-recovery" and container_absent(compose, original["container_id"])
            facts.update(operation=operation, outcome=outcome, attachment=closed["phase"])
        finally:
            armed.unlink(missing_ok=True)
            waiting.unlink(missing_ok=True)
        if chat or work:
            delivered = stream.result(timeout=60)
            if chat:
                run = chat.result(timeout=60)
            else:

                def terminal_run():
                    row = session.request("GET", path).json()
                    return row if row["status"] in ("error", "interrupted", "success", "timeout") else None

                run = eventually(terminal_run, timeout=60, interval=1)
                finished = await_attempt(session, context, work["id"], {"failed", "cancelled", "succeeded"})
                assert finished["attempt"]["id"] == attempt["id"]
                facts["attempt"] = finished["attempt"]["status"]
                facts["attempt_id"] = attempt["id"]
                facts["assignment"] = finished["status"]
            marker = hashlib.sha256(token.encode("ascii")).hexdigest()
            assert run["run_id"] == run_id and run["status"] in ("error", "interrupted", "success") and delivered
            (directory / f"manager-recovery-{workload}.sse").write_bytes(delivered)

            def terminal_receipt():
                value = json.loads(
                    compose.psql(
                        "SELECT json_build_object('status',status,'reason',stop_reason,'worker',owner_worker_id,'incarnation',run_incarnation) FROM runs "
                        f"WHERE run_id={sql(run_id)} AND thread_id={sql(thread)} AND user_id={sql(live['user'])}"
                    )
                )
                return value if value["status"] == run["status"] else None

            stored = eventually(terminal_receipt, timeout=30, interval=1)
            assert stored["incarnation"] == live["incarnation"]
            facts["durable_run"] = stored
            messages = session.request("GET", f"/api/langgraph/threads/{thread}/state").json()["values"].get("messages", [])
            facts.update(
                run=run["run_id"],
                terminal=run["status"],
                stop_reason=run.get("stop_reason"),
                final_checkpoint=any(m.get("type") == "ai" and marker in str(m.get("content")) for m in messages),
                final_in_stream=streamed_answer(delivered, marker),
            )
    await_execution_ready(session)
    fresh = new_conversation(session, context)
    answer = session.run(fresh["thread_id"])
    assert streamed_answer(
        (directory / ("run-" + answer["run_id"] + ".sse")).read_bytes(),
        "Acceptance complete. Your synthetic artifact is ready.",
    )
    artifact(session, fresh["thread_id"])
    replacement, _ = workspace(compose, fresh)
    assert replacement["attachment_id"] != original["attachment_id"]
    facts.update(fresh_run=answer["run_id"], fresh_workspace=replacement)
    (directory / f"manager-recovery-{workload}.json").write_text(json.dumps(facts, indent=2), encoding="utf-8")
