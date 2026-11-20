"""Task-owned native fixture lifecycle, never a production attachment adapter."""

import errno
import os
import re
import sqlite3
import subprocess
import time
from contextlib import closing

from deerflow.spaces.docker import ATTACHMENT_LABEL, HOST_LABEL


def read_sqlite_page_snapshot(path):
    """Close host readback before backing teardown, including on query failure."""
    with closing(sqlite3.connect(path)) as connection:
        return connection.execute("PRAGMA integrity_check").fetchone(), connection.execute("SELECT body FROM pages").fetchall()


def remove_owned_loop(control, number, sysfs, *, expected, retiring_binding=None, ioctl=None, clock=time.monotonic, sleep=time.sleep):
    """Wait for the proved autoclear binding, then drain unbound openers."""
    if ioctl is None:
        import fcntl

        ioctl = fcntl.ioctl
    deadline = clock() + 5
    while True:
        current = sysfs.stat()
        if (current.st_dev, current.st_ino) != expected:
            raise RuntimeError("Owned loop device changed before removal")
        binding = sysfs / "loop"
        if binding.exists():
            if retiring_binding is None:
                raise RuntimeError("Owned loop device is still bound; no removal attempted")
            try:
                facts = {name: (binding / name).read_text(encoding="utf-8").strip() for name in ("backing_file", "offset", "sizelimit", "autoclear")}
            except FileNotFoundError:
                # sysfs removes attributes before the directory. Only a fresh
                # observation of an unbound owned device permits removal.
                pass
            else:
                if facts != retiring_binding or any(facts[name] != value for name, value in (("offset", "0"), ("sizelimit", "0"), ("autoclear", "1"))):
                    raise RuntimeError("Owned loop binding changed during retirement")
            if clock() >= deadline:
                raise TimeoutError("Owned autoclear loop binding did not retire")
            # umount can return before a temporary opener releases the old
            # autoclear binding. Only observe it; never detach a bound device.
            sleep(0.05)
            continue
        try:
            result = ioctl(control, 0x4C81, number)
        except OSError as exc:
            if exc.errno != errno.EBUSY or clock() >= deadline:
                raise
            # udev may still have an unbound device open after umount. Reprove
            # identity and absence of a binding before every removal attempt.
            sleep(0.05)
        else:
            if result != 0:
                raise RuntimeError("Owned loop removal returned an unknown result")
            return


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
    writer_runtime = os.environ.get("HARTMESH_NATIVE_WRITER_RUNTIME")
    if writer_runtime:
        index = arguments.index("--pull=never") + 1
        arguments[index:index] = ["--runtime", writer_runtime]
    for view in plan.views:
        arguments += ["--mount", "type=bind,src=" + view.source + ",dst=" + view.destination + ("" if view.writable else ",readonly")]
    result = runner([*arguments, image, "python", "-c", program], check=True, capture_output=True, text=True, timeout=30)
    identity = result.stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{64}", identity):
        raise RuntimeError("Native fixture creation did not capture an exact container ID")
    containers.append((identity, plan.id))
    containment["confirmed"] = previously_confirmed
    return identity
