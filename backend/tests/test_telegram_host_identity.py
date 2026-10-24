"""Docker identity reads use an offline executable and own bounded child cleanup."""

import json
import os
import subprocess
import sys

import pytest

from app.telegram_bots import deployment

pytestmark = pytest.mark.skipif(os.name != "posix", reason="The managed Docker endpoint is a local Unix socket")


def executable(tmp_path, monkeypatch, body):
    path = tmp_path / "docker"
    path.write_text("#!" + sys.executable + "\n" + body, encoding="utf-8")
    path.chmod(0o700)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ.get("PATH", ""))
    return path


def test_host_identity_forces_local_endpoint_and_clears_redirecting_environment(tmp_path, monkeypatch):
    record = tmp_path / "arguments.json"
    executable(
        tmp_path,
        monkeypatch,
        "import json,os,sys\n"
        "names=('HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','NO_PROXY')\n"
        "redirects={k:v for k,v in os.environ.items() if k.startswith('DOCKER_') or k.upper() in names}\n"
        f"with open({str(record)!r},'w',encoding='utf-8') as f: json.dump({{'args':sys.argv[1:],'redirects':redirects}},f)\n"
        "print('engine-offline')\n",
    )
    for name in ["DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH", "DOCKER_CONFIG", "DOCKER_API_VERSION", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"]:
        monkeypatch.setenv(name, "synthetic-remote-value")
    assert deployment.host_identity() == "engine-offline"
    observed = json.loads(record.read_text(encoding="utf-8"))
    assert observed["args"] == ["--host", "unix:///var/run/docker.sock", "info", "--format", "{{.ID}}"]
    assert observed["redirects"] == {}


@pytest.mark.parametrize("output", ["", "engine-a\nengine-b", "x" * 129, "engine with spaces", "engine\x1b[31m"])
def test_invalid_or_oversized_host_identity_is_unreadable(tmp_path, monkeypatch, output):
    executable(tmp_path, monkeypatch, f"import sys\nsys.stdout.write({output!r})\n")
    assert deployment.host_identity() is None


@pytest.mark.parametrize("ending", ["\n", "\r\n"])
def test_host_identity_accepts_exact_persisted_field_limit(tmp_path, monkeypatch, ending):
    expected = "x" * 128
    executable(tmp_path, monkeypatch, f"import sys\nsys.stdout.write({(expected + ending)!r})\n")
    assert deployment.host_identity() == expected


def test_failed_docker_command_cannot_supply_an_id(tmp_path, monkeypatch):
    executable(tmp_path, monkeypatch, "import sys\nprint('plausible-engine')\nsys.exit(1)\n")
    assert deployment.host_identity() is None


def test_missing_docker_command_returns_none_without_an_alternate_network_probe(monkeypatch):
    def absent(*args, **kwargs):
        raise FileNotFoundError("synthetic missing executable")

    monkeypatch.setattr(deployment.subprocess, "Popen", absent)
    assert deployment.host_identity() is None


def test_deployment_reader_imports_no_telegram_or_supervisor():
    script = """
import importlib.abc,sys
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname=='telegram' or fullname.startswith('telegram.') or fullname=='app.telegram_bots.supervisor':
   raise AssertionError('Forbidden runtime import: '+fullname)
sys.meta_path.insert(0,Block())
from app.telegram_bots.deployment import CopyGuard,GuardRefused,host_identity
assert 'telegram' not in sys.modules
assert 'app.telegram_bots.supervisor' not in sys.modules
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("behavior", ["wait", "oversized", "closed-output"])
def test_host_reader_kills_reaps_and_closes_a_timed_out_or_oversized_child(tmp_path, monkeypatch, behavior):
    body = "import os,sys,time\n"
    if behavior == "oversized":
        body += "sys.stdout.write('x'*1000000)\nsys.stdout.flush()\n"
    elif behavior == "closed-output":
        body += "print('plausible-engine',flush=True)\nos.close(1)\n"
    body += "time.sleep(20)\n"
    executable(tmp_path, monkeypatch, body)
    monkeypatch.setattr(deployment, "_HOST_READ_SECONDS", 0.2)
    original, processes = deployment.subprocess.Popen, []

    def capture(*args, **kwargs):
        process = original(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(deployment.subprocess, "Popen", capture)
    assert deployment.host_identity() is None
    assert len(processes) == 1
    assert processes[0].poll() is not None and processes[0].stdout.closed
