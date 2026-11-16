"""Standard-library-only owner of the Compose storage preparation process group.

Other operator verbs delegate to the storage CLI. Only completed, contained
preparation can load the heavyweight inventory implementation into this parent.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

AT_START_LIMIT_SECONDS = 600


def _unescape_mount(value):
    return re.sub(r"\\(040|011|012|134)", lambda match: chr(int(match[1], 8)), value)


def memory_status(*, proc_root=Path("/proc")):
    """Read this process's cgroup v2 counters, including host cgroup namespaces."""
    try:
        own = next(line.split(":", 2)[2] for line in (proc_root / "self/cgroup").read_text(encoding="utf-8").splitlines() if line.startswith("0::"))
        group = Path(own)
        if not group.is_absolute() or ".." in group.parts:
            raise ValueError("invalid cgroup path")
        candidates = []
        for line in (proc_root / "self/mountinfo").read_text(encoding="utf-8").splitlines():
            before, separator, after = line.partition(" - ")
            if not separator or after.split()[0] != "cgroup2":
                continue
            fields = before.split()
            root, mount = Path(_unescape_mount(fields[3])), Path(_unescape_mount(fields[4]))
            if not root.is_absolute() or not mount.is_absolute() or ".." in root.parts or ".." in mount.parts:
                continue
            if group.is_relative_to(root):
                candidates.append((len(root.parts), mount / group.relative_to(root)))
        if not candidates:
            raise ValueError("own cgroup v2 mount unavailable")
        directory = max(candidates, key=lambda item: item[0])[1]
        peak = int((directory / "memory.peak").read_text(encoding="ascii").strip())
        raw_limit = (directory / "memory.max").read_text(encoding="ascii").strip()
        limit = None if raw_limit == "max" else int(raw_limit)
        if peak < 0 or limit is not None and limit <= 0:
            raise ValueError("invalid memory counter")
        budget = "unlimited" if limit is None else f"{limit / (1 << 20):.1f} MiB"
        return f"memory peak {peak / (1 << 20):.1f} MiB of {budget} (peak_bytes={peak} limit_bytes={raw_limit})"
    except Exception as exc:
        return f"memory unavailable ({type(exc).__name__})"


def report_memory():
    try:
        print(f"storage-init: {memory_status()}", file=sys.stderr, flush=True)
    except (OSError, ValueError):
        # Diagnostic IO cannot change the preparation verdict or containment.
        pass


def end_owned_group(child) -> bool:
    # The group, not completion of its Python leader, owns host-tool execution.
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        child.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        return False
    deadline = time.monotonic() + 5
    while True:
        live = False
        for proc in Path("/proc").iterdir():
            if not proc.name.isdigit():
                continue
            try:
                fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
                if int(fields[2]) == child.pid and fields[0] not in ("Z", "X"):
                    live = True
                    break
            except FileNotFoundError:
                continue
            except (OSError, ValueError, IndexError):
                return False
        if not live:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.02)


def _persisted_state(doc, data):
    try:
        from app.gateway.storage_init import KINDS, StorageInitializer, parse_plan

        tool = StorageInitializer(Path(data), parse_plan(os.environ))
        inventory = tool.load(optional=True)
        if inventory:
            doc["spaces"] = [dict(slot_id=s.slot_id, kind=s.kind, max_bytes=s.max_bytes, max_inodes=s.max_inodes, mounted=os.path.ismount(s.mount_path), mounted_now=None) for s in inventory.volumes.values()]
            doc["prepared"] = {kind: sum(s.kind == kind for s in inventory.volumes.values()) for kind in KINDS}
            doc["reserved_bytes"] = sum(s.max_bytes for s in inventory.volumes.values())
        if tool.plan:
            _, fields = tool.pool_plan(inventory)
            # Current pool facts are available; which slots this run made,
            # its initial fit and its fstab publication remain unknown.
            fields["fits"] = None
            doc.update(fields)
    except Exception as state_error:
        doc["problems"].append(f"persisted preparation state could not be read: {state_error}")


def at_start():
    child = None
    previous = signal.getsignal(signal.SIGTERM)

    def interrupted(signum, frame):
        raise InterruptedError("prepare supervisor was interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    try:
        child = subprocess.Popen([sys.executable, "-m", "app.gateway.storage_init", "prepare"], stdout=subprocess.PIPE, text=True, start_new_session=True)
        try:
            stdout, _ = child.communicate(timeout=AT_START_LIMIT_SECONDS)
            doc = json.loads(stdout)
            if not isinstance(doc, dict) or doc.get("command") != "prepare" or doc.get("returncode") not in (0, 1, 2):
                raise ValueError("prepare child returned an invalid document")
            if not end_owned_group(child):
                doc.update(returncode=2, verdict="incomplete", note="prepare ended; child containment could not be confirmed")
                doc.setdefault("problems", []).append(doc["note"])
            return doc
        except (subprocess.TimeoutExpired, InterruptedError, KeyboardInterrupt) as exc:
            confirmed = end_owned_group(child)
            cause = "prepare did not finish within 600 s" if isinstance(exc, subprocess.TimeoutExpired) else "prepare supervisor was interrupted"
            note = cause + ("; its children were ended" if confirmed else "; child containment could not be confirmed")
            data = os.environ.get("HARTMESH_DATA_DIR", "")
            doc = dict(
                command="prepare",
                returncode=2,
                note=note,
                verdict="incomplete",
                pool=str(Path(data) / "spaces"),
                problems=[note],
                plan=None,
                prepared=None,
                missing=None,
                prepared_now=None,
                restored=None,
                required_bytes=None,
                reserved_bytes=None,
                headroom_bytes=None,
                free_bytes=None,
                free_inodes=None,
                fits=None,
                data_disk=None,
                spaces=[],
                fstab=dict(entries=None, changed=None),
                seconds=AT_START_LIMIT_SECONDS,
            )
            if confirmed:
                _persisted_state(doc, data)
            return doc
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            if child is not None:
                end_owned_group(child)
        finally:
            signal.signal(signal.SIGTERM, previous)
            report_memory()


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if args != ["prepare", "--at-start"]:
        from app.gateway.storage_init import main as operator_main

        return operator_main(args)
    try:
        doc = at_start()
    except Exception as exc:
        doc = dict(command="prepare", returncode=1, error=f"{type(exc).__name__}: {exc}", note="failed; run the verb again")
    print(json.dumps(doc, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
