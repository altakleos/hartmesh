"""Operator process boundary; command services have independent SQL tests."""

import importlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

INSTANCE, SPACE, OPERATION = "a" * 32, "b" * 32, "c" * 32


@pytest.mark.parametrize(
    "arguments, expected",
    [
        (["list", "--all"], {"all": True}),
        (["status"], {}),
        (["verify"], {}),
        (["close-workspace", INSTANCE], {"instance_id": INSTANCE, "end_running_work": False, "wait_seconds": 120}),
        (["export", INSTANCE, "--end-running-work", "--wait-seconds", "0"], {"instance_id": INSTANCE, "end_running_work": True, "wait_seconds": 0}),
        (["supervise", INSTANCE, "--email", "lee@example.com"], {"instance_id": INSTANCE, "email": "lee@example.com"}),
        (["supervise", INSTANCE, "--issuer", "https://login.example.com", "--subject", "sub-lee"], {"issuer": "https://login.example.com", "subject": "sub-lee"}),
        (["delete", INSTANCE, "--wait-seconds", "1.5"], {"instance_id": INSTANCE, "wait_seconds": 1.5}),
        (["adopt-host", "--from", "old-daemon", "--acknowledge-old-host-gone"], {"old_host": "old-daemon", "acknowledge_old_host_gone": True}),
        (["settle", "--space", SPACE, "--operation", OPERATION, "--discard"], {"space_id": SPACE, "operation_id": OPERATION, "discard": True}),
    ],
)
def test_operator_cli_dispatches_only_the_named_verb_and_prints_one_sorted_document(monkeypatch, capsys, arguments, expected):
    module = importlib.import_module("app.gateway.agent_instances")
    calls = []

    async def run(command, **options):
        calls.append((command, options))
        return {"command": command, "note": "observed result", "returncode": 0}

    monkeypatch.setattr(module, "_run", run)
    assert module.main(arguments) == 0
    output = capsys.readouterr()
    document = json.loads(output.out)
    assert output.out == json.dumps(document, sort_keys=True) + "\n"
    assert output.err == "" and len(calls) == 1
    command, options = calls[0]
    assert command == arguments[0]
    assert all(options[key] == value for key, value in expected.items())


@pytest.mark.parametrize(
    "arguments, command, error",
    [
        ([], None, "required"),
        (["unknown"], None, "invalid choice"),
        (["status", "--json"], "status", "unrecognized arguments"),
        (["list", "--wait-seconds", "1"], "list", "unrecognized arguments"),
        (["delete"], "delete", "required"),
        (["export", "invalid"], "export", "32 hexadecimal"),
        (["close-workspace", INSTANCE, "--wait-seconds", "soon"], "close-workspace", "invalid float value"),
        (["close-workspace", INSTANCE, "--wait-seconds", "-1"], "close-workspace", "non-negative"),
        (["close-workspace", INSTANCE, "--wait-seconds", "nan"], "close-workspace", "finite"),
        (["close-workspace", INSTANCE, "--wait-seconds", "inf"], "close-workspace", "finite"),
        (["supervise", INSTANCE, "--issuer", "https://login.example.com"], "supervise", "--subject"),
        (["supervise", INSTANCE, "--email", "a@example.com", "--issuer", "https://login.example.com", "--subject", "sub"], "supervise", "--email"),
        (["settle", "--space", SPACE], "settle", "--operation"),
    ],
)
def test_bad_operator_arguments_refuse_without_opening_services(monkeypatch, capsys, arguments, command, error):
    module = importlib.import_module("app.gateway.agent_instances")

    async def unexpected(*_args, **_kwargs):
        pytest.fail("Malformed command lines must not open configuration, SQL or Docker")

    monkeypatch.setattr(module, "_run", unexpected)
    assert module.main(arguments) == 1
    output = capsys.readouterr()
    document = json.loads(output.out)
    assert document["command"] == command and document["returncode"] == 1
    assert document["note"] == "refused; nothing was changed"
    assert error in document["error"]
    assert output.out == json.dumps(document, sort_keys=True) + "\n"


@pytest.mark.parametrize("code", [0, 1, 2])
def test_operator_exit_status_matches_its_document(monkeypatch, capsys, code):
    module = importlib.import_module("app.gateway.agent_instances")

    async def run(_command, **_kwargs):
        return {"command": "status", "returncode": code, "note": "a result"}

    monkeypatch.setattr(module, "_run", run)
    assert module.main(["status"]) == code
    assert json.loads(capsys.readouterr().out)["returncode"] == code


def test_unexpected_operator_failure_keeps_tracebacks_off_stdout(monkeypatch, capsys):
    module = importlib.import_module("app.gateway.agent_instances")

    async def run(_command, **_kwargs):
        raise OSError("offline observation failed")

    monkeypatch.setattr(module, "_run", run)
    assert module.main(["status"]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out) == {"command": "status", "returncode": 1, "note": "failed; run the verb again", "error": "OSError: offline observation failed"}


def test_malformed_invocation_in_a_fresh_process_cannot_import_config_sql_or_gateway(tmp_path):
    guard = tmp_path / "sitecustomize.py"
    guard.write_text(
        "import sys\n"
        "class Guard:\n"
        "    def find_spec(self, fullname, *args):\n"
        "        if fullname in ('app.gateway.app', 'deerflow.config', 'sqlalchemy', 'app.gateway.agent_operator_services'):\n"
        "            raise RuntimeError('Unwanted startup import: ' + fullname)\n"
        "sys.meta_path.insert(0, Guard())\n",
        encoding="utf-8",
    )
    environment = {**os.environ, "PYTHONPATH": str(tmp_path) + os.pathsep + str(Path(__file__).resolve().parents[1])}
    result = subprocess.run([sys.executable, "-m", "app.gateway.agent_instances", "status", "--json"], env=environment, capture_output=True, text=True, timeout=10)
    assert result.returncode == 1 and result.stderr == ""
    document = json.loads(result.stdout)
    assert document["command"] == "status" and document["note"] == "refused; nothing was changed"
    assert document["error"] == "unrecognized arguments: --json"
    assert result.stdout == json.dumps(document, sort_keys=True) + "\n"
