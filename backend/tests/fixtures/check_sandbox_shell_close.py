"""Real-image regression: closing a shell must release its pending exec worker."""

import json
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid


def check(base_url: str) -> None:
    def request(path: str, body=None, *, method="POST", timeout=2):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(base_url.rstrip("/") + path, data=data, method=method, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.load(response)

    session_id = "shell-close-smoke-" + uuid.uuid4().hex
    errors = []

    def execute():
        try:
            request("/v1/shell/exec", {"id": session_id, "command": "sleep 60", "hard_timeout": 60}, timeout=8)
        except urllib.error.HTTPError:
            # The closed session may answer 404/500 rather than a completed
            # command. The worker must still release its pending request.
            pass
        except Exception as error:
            errors.append(error)

    request("/v1/shell/sessions/create", {"id": session_id})
    worker = threading.Thread(target=execute, daemon=True)
    worker.start()
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            result = request("/v1/shell/view", {"id": session_id})
            if result.get("data", {}).get("command") == "sleep 60":
                break
            time.sleep(0.05)
        else:
            raise AssertionError("the image never started the shell command")
        started = time.monotonic()
        request("/v1/shell/sessions/" + session_id, method="DELETE")
        worker.join(timeout=5)
        assert not worker.is_alive(), "closing the shell left its exec worker waiting on the original hard timeout"
        assert not errors, errors
        print(f"Cancelled shell request drained in {time.monotonic() - started:.2f}s")
    finally:
        request("/v1/shell/sessions/" + session_id, method="DELETE")


if __name__ == "__main__":
    check(sys.argv[1])
