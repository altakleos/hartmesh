"""Task-owned native fixture creation, never a production attachment adapter."""

import re
import subprocess

from deerflow.spaces.docker import ATTACHMENT_LABEL, HOST_LABEL


def prepare_consumer_container(plan, image, program, containers, containment, *, runner=subprocess.run):
    previously_confirmed = containment["confirmed"]
    containment["confirmed"] = False
    arguments = [
        "docker",
        "create",
        "--pull=never",
        "--name",
        "hartmesh-consumer-" + plan.id,
        "--label",
        ATTACHMENT_LABEL + "=" + plan.id,
        "--label",
        HOST_LABEL + "=" + plan.host_id,
        "--read-only",
        "--user",
        "1000:1000",
        "--cap-drop=ALL",
        "--security-opt",
        "no-new-privileges",
        "--network",
        "none",
        "--memory",
        "128m",
        "--pids-limit",
        "64",
    ]
    for view in plan.views:
        arguments += ["--mount", "type=bind,src=" + view.source + ",dst=" + view.destination + ("" if view.writable else ",readonly")]
    result = runner([*arguments, image, "python", "-c", program], check=True, capture_output=True, text=True, timeout=30)
    identity = result.stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{64}", identity):
        raise RuntimeError("Native fixture creation did not capture an exact container ID")
    containers.append((identity, plan.id))
    containment["confirmed"] = previously_confirmed
    return identity
