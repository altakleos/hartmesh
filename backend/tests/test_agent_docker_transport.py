"""Control requests name the admitted immutable container, never a recycled IP."""

import base64
import json
from types import SimpleNamespace

import httpx
import pytest

from deerflow.agent_instances.docker_transport import DockerControlTransport


def test_exact_container_request_preserves_binary_payload_and_response():
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        body = json.loads(kwargs["input"])
        assert body["url"] == "http://127.0.0.1:8080/v1/files"
        assert base64.b64decode(body["body"]) == b"\x00\xff"
        return SimpleNamespace(returncode=0, stdout=json.dumps({"status": 201, "headers": [["content-type", "application/octet-stream"]], "body": base64.b64encode(b"\x01\xfe").decode()}).encode(), stderr=b"")

    transport = DockerControlTransport("a" * 64, runner=run)
    with httpx.Client(transport=transport) as client:
        response = client.post("http://127.0.0.1:8080/v1/files", content=b"\x00\xff")
    assert response.status_code == 201 and response.content == b"\x01\xfe"
    assert calls[0][0][:5] == ["docker", "exec", "-i", "a" * 64, "python3"]
    assert "\x00" not in " ".join(calls[0][0])


@pytest.mark.parametrize("url", ["http://172.17.0.3:8080/v1/sandbox", "https://127.0.0.1:8080/v1/sandbox", "http://localhost:80/", "http://example.com/"])
def test_network_endpoint_or_redirect_is_never_a_control_fallback(url):
    def forbidden(*args, **kwargs):
        raise AssertionError("Rejected requests must not execute")

    with httpx.Client(transport=DockerControlTransport("a" * 64, runner=forbidden)) as client:
        with pytest.raises(httpx.TransportError):
            client.get(url)


def test_removed_container_has_no_name_or_ip_retry():
    calls = []

    def missing(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=1, stdout=b"", stderr=b"container is gone")

    with httpx.Client(transport=DockerControlTransport("a" * 64, runner=missing)) as client:
        with pytest.raises(httpx.TransportError):
            client.get("http://127.0.0.1:8080/v1/sandbox")
    assert len(calls) == 1 and calls[0][3] == "a" * 64


def test_instance_sdk_download_root_is_explicit_and_does_not_change_legacy_guard(monkeypatch):
    from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox

    bound = AioSandbox("instance", "http://127.0.0.1:8080", download_roots=("/mnt/spaces/home",))
    ordinary = AioSandbox("ordinary", "http://127.0.0.1:8080")
    try:
        for sandbox, path in ((bound, "/mnt/user-data/file"), (bound, "/mnt/spaces/another/file"), (bound, "/mnt/spaces/home/../../etc/passwd"), (ordinary, "/mnt/spaces/home/file")):
            with pytest.raises(PermissionError):
                sandbox.download_file(path)
    finally:
        bound.close()
        ordinary.close()
