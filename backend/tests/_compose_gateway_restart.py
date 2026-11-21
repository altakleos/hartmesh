"""Start and stop the disposable Gateway while checking the actual public route."""

import json

import httpx
from test_compose_instance_acceptance import eventually


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
