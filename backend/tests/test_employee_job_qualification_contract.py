"""The pinned job fixture advances only on correlated real tool receipts."""

import importlib
import json
import subprocess
from pathlib import Path

import pytest
import yaml


def test_model_uses_offered_tools_and_real_item_and_notice_receipts(monkeypatch):
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root / "docker/acceptance"))
    module = importlib.import_module("job_provider")
    nonce = "b" * 32

    def reply(messages, names):
        body = {"messages": messages, "tools": [{"function": {"name": n}} for n in names]}
        return module.answer(body, lambda _: pytest.fail("unexpected default"))

    def turn(kind, stage):
        return [{"role": "user", "content": [{"type": "text", "text": json.dumps({"batch_id": "sample", "follow_up": f"acceptance:job:{kind}:{stage}:{nonce}"})}]}]

    def receive(messages, call, content):
        messages.extend([{"role": "assistant", **call}, {"role": "tool", "tool_call_id": call["tool_calls"][0]["id"], "content": content}])

    messages = turn("employee", "submit")
    create = reply(messages, ["batch_task"])
    spec = json.loads(create["tool_calls"][0]["function"]["arguments"])
    assert spec["subagent_type"] == "general-purpose" and nonce in spec["items"][0]["prompt"] and nonce in spec["continuation"]
    receive(messages, create, "Job sample accepted with 1 checks.")
    assert "submitted" in reply(messages, ["batch_task"])["content"]
    names = ["read_batch_result", "notices_read_notice"]
    with pytest.raises(ValueError):
        reply(turn("employee", "item"), names)
    barrier = []
    monkeypatch.setattr(module.provider, "final_observation", barrier.append)
    personal_item = turn("personal", "item")
    assert reply(personal_item, names) == {"content": "Checked sample " + nonce}
    assert barrier == [nonce + "-personal"]
    receive(personal_item, module.call("notices_read_notice", 0, {}, names), [{"type": "text", "text": "Notice from notices-a."}])
    with pytest.raises(ValueError):
        reply(personal_item, names)
    final = turn("employee", "final")
    read = reply(final, names)
    assert json.loads(read["tool_calls"][0]["function"]["arguments"]) == {"batch_id": "sample", "position": 0}
    window = json.dumps({"position": 0, "status": "ok", "offset": 0, "next_offset": None, "content": json.dumps({"position": 0, "status": "succeeded", "result": "Checked sample " + nonce})})
    personal_final = turn("personal", "final")
    receive(personal_final, reply(personal_final, names), window)
    assert reply(personal_final, names) == {"content": "Completed personal checks " + nonce}
    receive(personal_final, module.call("notices_read_notice", 1, {}, names), [{"type": "text", "text": "Notice from notices-a."}])
    with pytest.raises(ValueError):
        reply(personal_final, names)
    receive(final, read, window)
    notice = reply(final, names)
    receive(final, notice, [{"type": "text", "text": "Notice from notices-a."}])
    assert reply(final, names) == {"content": "Completed employee checks " + nonce}
    final[-1]["tool_call_id"] = "stale"
    with pytest.raises(ValueError):
        reply(final, names)
    off = turn("off", "final")
    receive(off, reply(off, names), window)
    with pytest.raises(ValueError):
        reply(off, names)
    assert "Completed" in reply(off, names[:1])["content"]
    replacement = turn("replacement", "final")
    tools = ["read_batch_result", "updated_read_notice"]
    receive(replacement, reply(replacement, tools), window)
    second = reply(replacement, tools)
    assert json.loads(second["tool_calls"][0]["function"]["arguments"])["position"] == 1
    remaining = json.dumps({"position": 1, "status": "ok", "offset": 0, "next_offset": None, "content": json.dumps({"position": 1, "status": "succeeded", "result": "Checked remaining " + nonce})})
    receive(replacement, second, remaining)
    with pytest.raises(ValueError):
        reply(replacement, [*tools, "notices_read_notice"])
    receive(replacement, reply(replacement, tools), [{"type": "text", "text": "Notice from notices-b."}])
    assert reply(replacement, tools) == {"content": "Completed replacement checks " + nonce}


def test_dedicated_mounted_job_shares_candidate_verification_and_preserves_bundle_default():
    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/compose-instances.yml").read_text())
    steps = {s.get("name"): s for s in workflow["jobs"]["bundle-instances"]["steps"]}
    signed = steps["Verify the signed workspace browser pair"]
    assert "inputs.verify_agent_jobs" in signed["if"] and "verify-candidate" in signed["run"] and "deploy/compose/images.txt" in signed["run"]
    install = steps["Install the connection qualification fixture"]
    assert "HARTMESH_BATCHES=on" in install["run"]
    assert install["env"]["SOURCE_JOB_REVISION"] == "${{ inputs.verify_agent_jobs && inputs.backend_image == '' && github.sha || '' }}"
    fixture = steps["Qualify employee jobs and final-turn recovery"]
    assert fixture["if"] == "inputs.verify_agent_jobs" and "employee-jobs.xml" in fixture["run"] and "employee-jobs.json" in fixture["run"]


@pytest.mark.parametrize(
    ("mode", "reference", "bundle", "valid"),
    [
        ("source", "current", "/opt/hartmesh", True),
        ("source", "other-pin", "/opt/hartmesh", False),
        ("source", "other-revision", "/opt/hartmesh", False),
        ("source", "other-origin", "/opt/hartmesh", False),
        ("source", "local-tag", "/opt/hartmesh", False),
        ("source", "current", "/opt/hartmesh-rollback-66", False),
        ("signed", "current", "/opt/hartmesh", False),
        ("signed", "release", "/opt/hartmesh", True),
        ("signed", "release", "/opt/hartmesh-rollback-66", True),
    ],
)
def test_tls_fixture_executes_only_the_admitted_backend(tmp_path, monkeypatch, mode, reference, bundle, valid):
    """Exercise the installed wrapper with a disposable pin and a Docker command recorder."""
    root = Path(__file__).resolve().parents[2]
    revision = "a" * 40
    image = "localhost:80/hartmesh-backend@sha256:" + "b" * 64
    if reference == "release":
        image = image.replace("localhost:80", "ghcr.io/altakleos")
    elif reference == "local-tag":
        image = "localhost:80/hartmesh-backend:ci"
    (tmp_path / "images.txt").write_text((image if reference != "other-pin" else "another-image") + "\n")
    (tmp_path / "connection-fixture-tls").mkdir()
    (tmp_path / "connection-fixture-tls/ca.pem").touch()
    recorder = tmp_path / "commands.bash"
    recorder.write_text(
        """grep() {
  if [[ "${!#}" == /opt/hartmesh/images.txt ]]; then command grep "${@:1:$#-1}" "$RUNNER_TEMP/images.txt"; else return 1; fi
}
docker() {
  if [[ "$1 $2" == 'image inspect' && "${!#}" == "$BACKEND_REF" ]]; then
    echo "$IMAGE_REVISION $IMAGE_SOURCE"
  elif [[ "$1" == run ]]; then
    printf '%s\n' "$@" > "$RUNNER_TEMP/run-command"
    return 79
  else return 1; fi
}
"""
    )
    for key, value in {
        "BASH_ENV": str(recorder),
        "RUNNER_TEMP": str(tmp_path),
        "GITHUB_ACTIONS": "true",
        "GITHUB_SHA": revision,
        "GITHUB_REPOSITORY": "altakleos/hartmesh",
        "SOURCE_JOB_REVISION": revision if mode == "source" else "",
        "IMAGE_REVISION": "d" * 40 if reference == "other-revision" else revision,
        "IMAGE_SOURCE": "https://github.com/example/other" if reference == "other-origin" else "https://github.com/altakleos/hartmesh",
        "BACKEND_REF": image,
    }.items():
        monkeypatch.setenv(key, value)
    result = subprocess.run(["bash", root / "docker/acceptance/connections/tls.sh", image, bundle], check=False)
    recorded = tmp_path / "run-command"
    if valid:
        assert result.returncode == 79 and image in recorded.read_text().splitlines()
    else:
        assert result.returncode != 0 and not recorded.exists()
