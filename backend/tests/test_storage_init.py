"""Offline operator behavior; syscall doubles do not qualify native enforcement."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.gateway import storage_init as si

PREFLIGHT = si.StorageInitializer.preflight
MOUNT_FACTS = si.StorageInitializer.mounted_image_facts


@pytest.mark.parametrize("argv", [[], ["unknown"], ["resize"], ["forget"]])
def test_malformed_command_line_prints_the_document_and_exits_one(argv, capsys):
    assert si.main(argv) == 1
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    doc = json.loads(lines[0])
    assert doc["returncode"] == 1 and doc["error"] and doc["note"] == "refused; nothing was changed"
    assert lines[0] == json.dumps(doc, sort_keys=True)


def test_absent_capacity_means_one_gib_and_absent_agent_count_means_zero():
    assert si.parse_plan({}) is None
    p = si.parse_plan({"HARTMESH_USERS": " 2 "})
    assert p.users == 2 and p.user_gib == p.company_gib == 1
    assert p.ai_assistants_per_user == p.ai_employees == 0


@pytest.mark.parametrize("key", list(si.KEYS))
@pytest.mark.parametrize("value", ["-1", "1.5", "on", "١", "10000"])
def test_invalid_key_values_are_refused_with_the_renderer_texts(key, value):
    with pytest.raises(si.Refused, match=key):
        si.parse_plan({"HARTMESH_USERS": "2", key: value})


def test_prepare_refuses_a_plan_above_max_spaces_with_the_renderer_text():
    with pytest.raises(si.Refused, match="the keys call for 1201 Spaces.*at most 256"):
        si.parse_plan({"HARTMESH_USERS": "100", "HARTMESH_AI_ASSISTANTS_PER_USER": "10", "HARTMESH_AI_EMPLOYEES": "100"})


def test_fstab_sync_rewrites_only_between_the_markers(tmp_path):
    original = "# untouched\n/dev/root / ext4 defaults 0 1\n# hartmesh-storage begin\nold line\n# hartmesh-storage end\n# tail\n"
    replacement, added, removed = si.fstab_text(original, ["new line"])
    assert replacement == original.replace("old line", "new line") and (added, removed) == (1, 1)
    assert si.fstab_text(replacement, ["new line"])[0] == replacement


@pytest.mark.parametrize("text", ["# hartmesh-storage begin\n", "# hartmesh-storage end\n", "# hartmesh-storage begin\n# hartmesh-storage end\n# hartmesh-storage begin\n# hartmesh-storage end\n"])
def test_fstab_sync_refuses_malformed_markers(text):
    with pytest.raises(si.Refused, match="fstab"):
        si.fstab_text(text, [])


@pytest.mark.parametrize("form", ["0::/system.slice/docker-{id}.scope", "1:devices:/docker/{id}"])
def test_quiesce_refuses_while_a_process_of_a_container_has_a_slot(tmp_path, form):
    own, other = "1" * 64, "2" * 64
    for pid, group in (("self", form.format(id=own)), ("42", form.format(id=other)), ("43", "0::/system.slice/systemd-logind.service")):
        proc = tmp_path / pid
        proc.mkdir()
        (proc / "cgroup").write_text(group, encoding="utf-8")
        (proc / "mountinfo").write_text("8 2 7:1 / /data rw - ext4 /dev/loop1 rw\n", encoding="utf-8")
    assert si.container_holders({"a" * 32: "/dev/loop1"}, proc_root=tmp_path) == [{"pid": 42, "container_id": other[:12], "slot_id": "a" * 32}]


def test_holder_scan_does_not_infer_absence_from_unreadable_process(tmp_path):
    (tmp_path / "self").mkdir()
    (tmp_path / "self/cgroup").write_text("0::/\n", encoding="utf-8")
    (tmp_path / "42").mkdir()
    with pytest.raises(si.Refused):
        si.container_holders({"a" * 32: "/dev/loop1"}, proc_root=tmp_path)


@pytest.fixture
def operator(tmp_path, monkeypatch):
    """Small allocated files and explicit mount facts, with no privileged action."""
    mounted, commands, trees = set(), [], {}
    monkeypatch.setattr(si.os, "geteuid", lambda: 0)
    monkeypatch.setattr(si.os, "chown", lambda *args, **kwargs: None)
    monkeypatch.setattr(si.os, "fchown", lambda *args, **kwargs: None)
    monkeypatch.setattr(si.os, "posix_fallocate", lambda fd, offset, length: si.os.ftruncate(fd, offset + length))
    monkeypatch.setattr(si.os.path, "ismount", lambda p: Path(p) in mounted)
    monkeypatch.setattr(si.StorageInitializer, "preflight", lambda self: None)
    monkeypatch.setattr(si.StorageInitializer, "mounted_image_facts", lambda self, spec, **kwargs: {"type": "ext4", "root": "/", "path": str(spec.mount_path), "options": {"rw", "nodev", "nosuid"}})
    monkeypatch.setattr(si, "container_holders", lambda *args, **kwargs: [])
    monkeypatch.setattr(si.PreparedVolumeCatalog, "verify", lambda self, slot: None)

    def host_run(self, *args, **kwargs):
        commands.append(args)
        if args[0] == "mount":
            mount = Path(args[-1])
            mounted.add(mount)
            for path in trees.pop(mount, []):
                path.rename(mount / path.name)
        if args[0] == "umount":
            mount = Path(args[-1])
            mounted.discard(mount)
            saved = tmp_path / (mount.name + ".saved")
            saved.mkdir(exist_ok=True)
            trees[mount] = []
            for path in mount.iterdir():
                destination = saved / path.name
                path.rename(destination)
                trees[mount].append(destination)
        if args[0] == "findmnt":
            return "ext4\n" if "FSTYPE" in args else "shared\n"
        if args[0] == "cat":
            p = Path(args[1])
            return p.read_text(encoding="utf-8") if p.exists() else ""
        if args[0] == "sh":
            Path(args[-1]).write_text(kwargs["input"], encoding="utf-8")
        return ""

    monkeypatch.setattr(si.Host, "run", host_run)
    original_statvfs = si.os.statvfs
    monkeypatch.setattr(si.os, "statvfs", lambda p: SimpleNamespace(f_blocks=50 << 30, f_frsize=1, f_bavail=48 << 30, f_favail=1_000_000) if Path(p) == tmp_path / "spaces" else original_statvfs(p))
    tool = si.StorageInitializer(tmp_path, si.parse_plan({"HARTMESH_USERS": "2", "HARTMESH_AI_ASSISTANTS_PER_USER": "1", "HARTMESH_AI_EMPLOYEES": "1"}), capacities=dict.fromkeys(si.KINDS, 32 << 20), fstab_path=tmp_path / "fstab")
    tool.setup()
    return tool, mounted, commands


def test_prepare_prepares_every_space_the_seven_keys_call_for_typed_by_kind(operator):
    tool, mounted, commands = operator
    doc = tool.prepare()
    assert doc["returncode"] == 0 and doc["verdict"] == "complete"
    assert doc["prepared_now"] == dict(company=1, user=2, assistant=2, employee=1)
    entries = json.loads(tool.manifest.read_text(encoding="utf-8"))["volumes"]
    assert [e["kind"] for e in entries] == ["company", "user", "user", "assistant", "assistant", "employee"]
    assert all(e["max_inodes"] == (32 << 20) // 8192 and "host_image_path" not in e for e in entries)
    assert len(mounted) == 6 and len([c for c in commands if c[0] == "mkfs.ext4"]) == 6


def test_prepare_is_idempotent_and_prepares_only_the_missing_slots(operator):
    tool, _, commands = operator
    tool.prepare()
    before = tool.manifest.read_bytes()
    assert tool.prepare()["verdict"] == "nothing-to-do"
    assert tool.manifest.read_bytes() == before
    tool.plan = tool.plan.model_copy(update={"ai_employees": 2})
    assert tool.prepare()["prepared_now"] == dict(company=0, user=0, assistant=0, employee=1)
    assert len([c for c in commands if c[0] == "mkfs.ext4"]) == 7


def test_prepare_writes_one_progress_line_per_space_to_stderr(operator, capsys):
    operator[0].prepare()
    captured = capsys.readouterr()
    assert not captured.out and len(captured.err.splitlines()) == 6
    assert all(line.startswith("storage-init: prepared ") and line.endswith(" s") for line in captured.err.splitlines())


def test_inventory_is_republished_after_each_slot_by_replace_and_fsync(operator, monkeypatch):
    tool = operator[0]
    original, counts = si.os.replace, []

    def replace(source, destination):
        if Path(destination) == tool.manifest:
            counts.append(len(json.loads(Path(source).read_text(encoding="utf-8"))["volumes"]))
        return original(source, destination)

    monkeypatch.setattr(si.os, "replace", replace)
    tool.prepare()
    assert counts == list(range(7))


def test_fit_is_computed_once_for_whole_missing_set_and_refuses_before_images(operator, monkeypatch):
    tool, _, commands = operator
    original = si.storage_fit

    def no_fit(*args, **kwargs):
        from dataclasses import replace

        return replace(original(*args, **kwargs), fits=False, free_bytes=1 << 30)

    monkeypatch.setattr(si, "storage_fit", no_fit)
    doc = tool.prepare()
    assert doc["verdict"] == "disk" and doc["returncode"] == 2
    assert tool.manifest.exists() and not list(tool.pool.glob("*.img"))
    assert not [c for c in commands if c[0] == "mkfs.ext4"]
    assert "Grow the data disk" in doc["note"]


def test_prepare_creates_only_the_directory_when_users_is_absent(tmp_path, monkeypatch):
    monkeypatch.setattr(si.os, "geteuid", lambda: 0)
    monkeypatch.setattr(si.os, "fchown", lambda *args: None)
    tool = si.StorageInitializer(tmp_path, None)
    with tool.locked():
        assert tool.prepare()["verdict"] == "nothing-to-do"
    assert sorted(p.name for p in tool.pool.iterdir()) == [".storage-init.lock"]


@pytest.mark.parametrize("change", ["disk", "image", "mount", "schema", "kind"])
def test_an_inventory_that_names_a_path_outside_pool_or_schema_one_is_refused(operator, change):
    tool = operator[0]
    tool.prepare()
    doc = json.loads(tool.manifest.read_text(encoding="utf-8"))
    if change == "disk":
        doc["data_disk"] = "/other"
    elif change in ("image", "mount"):
        doc["volumes"][0][change + "_path"] = "/other"
    elif change == "schema":
        doc["schema_version"] = 1
    else:
        del doc["volumes"][0]["kind"]
    tool.manifest.chmod(0o600)
    tool.manifest.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(si.Refused):
        tool.load()


def test_symlink_image_is_refused_before_host_actions(operator):
    tool, _, commands = operator
    tool.prepare()
    spec = next(iter(tool.load().volumes.values()))
    spec.image_path.unlink()
    spec.image_path.symlink_to("/etc/passwd")
    commands.clear()
    with pytest.raises(si.Refused):
        tool.load()
    assert not commands


def test_mount_all_mounts_missing_images_and_leaves_mounted_ones(operator):
    tool, mounted, commands = operator
    tool.prepare()
    spec = next(iter(tool.load().volumes.values()))
    mounted.remove(spec.mount_path)
    commands.clear()
    doc = tool.mount_all()
    assert doc["mounted"] == [spec.slot_id] and doc["already"] == 5 and not doc["failed"]


def test_forget_refuses_files_without_discard_and_keeps_other_kinds(operator):
    tool, _, _ = operator
    tool.prepare()
    spec = next(iter(tool.load().volumes.values()))
    (spec.mount_path / "data/input").write_bytes(b"keep")
    with pytest.raises(si.Refused, match="holds files"):
        tool.forget(spec.slot_id, discard=False)
    assert spec.image_path.exists()
    doc = tool.forget(spec.slot_id, discard=True)
    assert doc["had_data"] and doc["image_deleted"] and not spec.image_path.exists()
    assert tool.prepare()["prepared_now"]["company"] == 1


def test_quiesce_unmounts_unlisted_image_and_reports_busy(operator, monkeypatch):
    tool, mounted, commands = operator
    tool.prepare()
    slot = uuid4().hex
    (tool.pool / f"{slot}.img").write_bytes(b"fixture")
    orphan = tool.pool / f"{slot}.volume"
    orphan.mkdir()
    mounted.add(orphan)
    doc = tool.quiesce()
    assert slot in doc["unmounted"] and not mounted and doc["returncode"] == 0
    assert (tool.pool / f"{slot}.img").exists()


def test_status_without_inventory_computes_fit_and_does_not_prepare(operator):
    tool = operator[0]
    doc = tool.status()
    assert doc["returncode"] == 0 and not doc["inventory"] and doc["fits"]
    assert not tool.manifest.exists() and not list(tool.pool.glob("*.img"))


def test_forget_refuses_unrelated_mount_before_deleting_populated_image(operator, monkeypatch):
    tool, _, commands = operator
    tool.prepare()
    spec = next(iter(tool.load().volumes.values()))
    original = spec.image_path.read_bytes()

    def wrong_mount(self, value):
        raise si.Refused("slot is mounted from another image")

    monkeypatch.setattr(si.StorageInitializer, "mounted_image_facts", wrong_mount)
    commands.clear()
    with pytest.raises(si.Refused, match="another image"):
        tool.forget(spec.slot_id)
    assert spec.image_path.read_bytes() == original
    assert not [c for c in commands if c[0] == "umount"]


def test_forget_discard_can_reclaim_an_unmounted_unformatted_image(operator):
    tool, _, commands = operator
    slot = uuid4().hex
    image = tool.pool / f"{slot}.img"
    image.write_bytes(b"interrupted mkfs")
    assert tool.forget(slot, discard=True)["image_deleted"]
    assert not image.exists() and not [c for c in commands if c[0] == "mount"]


def test_prepare_refuses_retained_plus_missing_slots_above_limit(operator, monkeypatch):
    tool, _, commands = operator
    tool.prepare()
    before = tool.manifest.read_bytes()
    monkeypatch.setattr(si, "MAX_SPACES", 6)
    tool.plan = tool.plan.model_copy(update={"ai_assistants_per_user": 0, "ai_employees": 4})
    commands.clear()
    with pytest.raises(si.Refused, match="existing pool plus missing slots"):
        tool.prepare()
    assert tool.manifest.read_bytes() == before
    assert not [c for c in commands if c[0] == "mkfs.ext4"]


@pytest.mark.parametrize("verb", ["forget", "resize"])
def test_resize_and_forget_report_busy_when_loop_device_stays(operator, monkeypatch, verb):
    tool, _, commands = operator
    tool.prepare()
    spec = next(iter(tool.load().volumes.values()))
    monkeypatch.setattr(tool, "loop", lambda image: "/dev/loop1")
    monkeypatch.setattr(si, "read_ext4_identity", lambda path: spec.filesystem_uuid)
    commands.clear()
    doc = tool.forget(spec.slot_id, discard=True) if verb == "forget" else tool.resize("1", slot=spec.slot_id)
    assert doc["returncode"] == 2 and doc["busy"] == [spec.slot_id]
    assert spec.image_path.exists()
    assert not [c for c in commands if c[0] in ("resize2fs", "e2fsck")]


def test_resize_refuses_smaller_than_interrupted_image_and_keeps_it(operator, monkeypatch):
    tool = operator[0]
    tool.prepare()
    spec = next(iter(tool.load().volumes.values()))
    with spec.image_path.open("r+b") as stream:
        stream.truncate(2 << 30)
    with pytest.raises(si.Refused, match="image.*already 2 GiB"):
        tool.resize("1", slot=spec.slot_id)
    assert spec.image_path.stat().st_size == 2 << 30


@pytest.mark.parametrize("leader_exits", [False, True])
def test_at_start_ends_descendants_even_if_leader_exits_or_descendant_ignores_term(tmp_path, monkeypatch, leader_exits):
    pidfile = tmp_path / "descendant.pid"
    program = tmp_path / "owned-group.py"
    grandchild = f"import os,signal,time; from pathlib import Path; signal.signal(signal.SIGTERM,signal.SIG_IGN); Path({str(pidfile)!r}).write_text(str(os.getpid())); time.sleep(120)"
    program.write_text(
        f"import subprocess,sys,time,json\nfrom pathlib import Path\n"
        f"subprocess.Popen([sys.executable,'-c',{grandchild!r}],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)\n"
        f"while not Path({str(pidfile)!r}).exists(): time.sleep(.005)\n" + ("print(json.dumps(dict(command='prepare',returncode=0,note='done')),flush=True)\n" if leader_exits else "time.sleep(120)\n"),
        encoding="utf-8",
    )
    original = subprocess.Popen
    monkeypatch.setattr(si.subprocess, "Popen", lambda args, **kwargs: original([sys.executable, str(program)], **kwargs))
    monkeypatch.setattr(si, "AT_START_LIMIT_SECONDS", 0.5)
    doc = si.at_start()
    assert doc["returncode"] == (0 if leader_exits else 2)
    assert pidfile.exists()
    pid = int(pidfile.read_text(encoding="utf-8"))
    try:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            proc = Path(f"/proc/{pid}/stat")
            if not proc.exists() or proc.read_text(encoding="utf-8").rsplit(")", 1)[1].split()[0] == "Z":
                break
            time.sleep(0.01)
        else:
            pytest.fail("storage-init left a live mutating descendant")
    finally:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


@pytest.mark.parametrize("missing", si.HOST_TOOLS)
def test_missing_host_tool_is_refused_with_its_package(operator, monkeypatch, missing):
    tool = operator[0]

    def run(self, *args, **kwargs):
        if args[-1] == missing:
            raise si.Refused("not found")
        return "ext4"

    monkeypatch.setattr(si.Host, "run", run)
    with pytest.raises(si.Refused, match=missing):
        PREFLIGHT(tool)


def test_host_commands_run_through_nsenter_into_pid_one(monkeypatch):
    commands = []

    def run(args, **kwargs):
        commands.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(si.subprocess, "run", run)
    si.Host().run("mount", "-o", "loop,nodev,nosuid", "/pool/image.img", "/pool/image.volume")
    assert commands == [["nsenter", "-m", "-t", "1", "--", "mount", "-o", "loop,nodev,nosuid", "/pool/image.img", "/pool/image.volume"]]


def test_prepare_never_creates_data_or_control_when_mount_is_not_visible(operator, monkeypatch):
    tool = operator[0]
    monkeypatch.setattr(si.os.path, "ismount", lambda path: False)
    doc = tool.prepare()
    assert doc["returncode"] == 2 and doc["verdict"] == "incomplete"
    assert list(tool.pool.glob("*.img")) and not list(tool.pool.glob("*.volume/data"))
    assert not list(tool.pool.glob("*.volume/control"))


def test_resize_by_kind_preserves_other_kinds_and_updates_bound(operator, monkeypatch):
    tool, _, commands = operator
    tool.prepare()
    monkeypatch.setattr(si, "read_ext4_identity", lambda path: tool.load().volumes[path.stem].filesystem_uuid)
    before = tool.load()
    commands.clear()
    doc = tool.resize("1", kind="user", capacity_bytes=64 << 20)
    assert doc["returncode"] == 0 and len(doc["grown"]) == 2
    after = tool.load()
    for slot, spec in before.volumes.items():
        current = after.volumes[slot]
        assert current.filesystem_uuid == spec.filesystem_uuid and current.kind == spec.kind
        assert current.max_bytes == (64 << 20 if spec.kind == "user" else 32 << 20)
        assert current.max_inodes == current.max_bytes // 8192
    assert len([c for c in commands if c[0] == "resize2fs"]) == 2


def test_mount_identity_rejects_other_kernel_backing_file(operator, monkeypatch):
    from deerflow.spaces import backings

    tool = operator[0]
    tool.prepare()
    spec = next(iter(tool.load().volumes.values()))
    meta = spec.mount_path.stat()
    facts = dict(type="ext4", root="/", path=str(spec.mount_path), device=f"{os.major(meta.st_dev)}:{os.minor(meta.st_dev)}")
    monkeypatch.setattr(backings, "_mount_record", lambda fd: facts)
    original = Path.read_text
    monkeypatch.setattr(Path, "read_text", lambda path, *args, **kwargs: "/unrelated/image.img\n" if path.name == "backing_file" else original(path, *args, **kwargs))
    with pytest.raises(si.Refused, match="another image"):
        MOUNT_FACTS(tool, spec)


@pytest.fixture
def verified_operator(operator, monkeypatch):
    """Independent kernel/allocation facts with real mode changes on small files."""
    tool, mounted, commands = operator
    tool.prepare()
    inventory = tool.load()
    spec = next(iter(inventory.volumes.values()))
    owners = {tool.pool: (0, 1000), tool.manifest: (1000, 1000), spec.image_path: (0, 1000), spec.mount_path: (0, 0), spec.mount_path / "data": (1000, 1000), spec.mount_path / "control": (1000, 1000)}
    original = Path.lstat

    def metadata(path, *args, **kwargs):
        value = original(path, *args, **kwargs)
        if path in owners:
            fields = list(value)
            fields[4], fields[5] = owners[path]
            return os.stat_result(fields)
        return value

    monkeypatch.setattr(Path, "lstat", metadata)
    monkeypatch.setattr(si, "read_ext4_identity", lambda path: inventory.volumes[path.stem].filesystem_uuid)
    monkeypatch.setattr(si, "verify_image_extents", lambda *args: None)
    return tool, spec, mounted, commands


def test_verify_repairs_control_permissions_after_kernel_identity_proof(verified_operator, monkeypatch):
    tool, spec, _, _ = verified_operator
    (spec.mount_path / "control").chmod(0o770)

    def verify(catalog, slot):
        if (spec.mount_path / "control").stat().st_mode & 0o077:
            raise si.BackingUnavailable("control remains host-private")

    monkeypatch.setattr(si.PreparedVolumeCatalog, "verify", verify)
    assert tool.slot_state(spec)["state"] == "wrong-modes"
    assert tool.slot_state(spec, repair=True)["state"] == "repaired"
    assert (spec.mount_path / "control").stat().st_mode & 0o777 == 0o700


@pytest.mark.parametrize("state", ["read-only", "wrong-mount"])
def test_verify_never_repairs_or_checks_mounted_read_only_or_wrong_mount(verified_operator, monkeypatch, state):
    tool, spec, _, commands = verified_operator
    options = {"ro", "nodev", "nosuid"} if state == "read-only" else {"rw"}
    monkeypatch.setattr(si.StorageInitializer, "mounted_image_facts", lambda self, value, **kwargs: dict(type="ext4", root="/", path=str(value.mount_path), options=options))
    commands.clear()
    assert tool.slot_state(spec, repair=True)["state"] == state
    assert not [c for c in commands if c[0] in ("e2fsck", "mount", "sh")]


@pytest.mark.parametrize("state", ["image-missing", "wrong-size", "not-allocated", "uuid-mismatch", "not-mounted"])
def test_verify_reports_each_unusable_space_state(verified_operator, monkeypatch, state):
    tool, spec, mounted, _ = verified_operator
    if state == "image-missing":
        spec.image_path.unlink()
    elif state == "wrong-size":
        with spec.image_path.open("r+b") as stream:
            stream.truncate(123)
    elif state == "not-allocated":

        def holes(*args):
            raise si.BackingUnavailable("hole")

        monkeypatch.setattr(si, "verify_image_extents", holes)
    elif state == "uuid-mismatch":
        monkeypatch.setattr(si, "read_ext4_identity", lambda path: str(uuid4()))
    else:
        mounted.remove(spec.mount_path)
    row = tool.slot_state(spec)
    assert row["state"] == state and row["kind"] == spec.kind
    assert row["detail"] == ("hole" if state == "not-allocated" else None)


def test_second_run_refuses_on_lock_without_reading_inventory(operator, monkeypatch):
    tool = operator[0]
    monkeypatch.setattr(si, "LOCK_WAIT_SECONDS", 0.02)
    fd = os.open(tool.pool / ".storage-init.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        si.fcntl.flock(fd, si.fcntl.LOCK_EX)
        with pytest.raises(si.Refused, match="did not finish within 30 s"):
            with tool.locked():
                pytest.fail("second operator passed the pool lock")
    finally:
        os.close(fd)


def test_fstab_sync_reports_unwritable_fstab_and_preserves_inventory(operator, monkeypatch):
    tool = operator[0]
    tool.prepare()
    before = tool.manifest.read_bytes()
    tool.fstab_path.write_text("# operator text\n", encoding="utf-8")
    original = si.Host.run

    def readonly(self, *args, **kwargs):
        if args[0] == "sh":
            raise si.Refused("Read-only file system")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(si.Host, "run", readonly)
    with pytest.raises(si.Refused, match="/etc/fstab cannot be written: Read-only file system"):
        tool.fstab_sync()
    assert tool.manifest.read_bytes() == before and tool.fstab_path.read_text(encoding="utf-8") == "# operator text\n"


def test_prepare_at_start_exits_zero_and_reports_real_returncode(monkeypatch, capsys):
    monkeypatch.setattr(si, "at_start", lambda: si.refusal("prepare", "disk unavailable"))
    assert si.main(["prepare", "--at-start"]) == 0
    assert json.loads(capsys.readouterr().out)["returncode"] == 1


def test_at_start_incomplete_document_lists_persisted_slots_and_marks_unknown_facts(operator, monkeypatch):
    tool = operator[0]
    tool.prepare()
    monkeypatch.setenv("HARTMESH_DATA_DIR", str(tool.data_dir))
    for key in si.KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HARTMESH_USERS", "2")

    class Child:
        def communicate(self, **kwargs):
            raise subprocess.TimeoutExpired("owned fixture", 600)

    monkeypatch.setattr(si.subprocess, "Popen", lambda *args, **kwargs: Child())
    monkeypatch.setattr(si, "end_owned_group", lambda child: True)
    doc = si.at_start()
    assert doc["returncode"] == 2 and len(doc["spaces"]) == 6
    assert all(row["mounted"] and row["mounted_now"] is None for row in doc["spaces"])
    assert doc["prepared"]["user"] == 2 and doc["prepared_now"] is None
    assert doc["fstab"]["entries"] is None and doc["fits"] is None


@pytest.mark.parametrize("argv", [["prepare"], ["mount-all"], ["fstab-sync"], ["verify"], ["status"], ["quiesce"], ["forget", "a" * 32], ["resize", "a" * 32, "1"]])
def test_every_verb_prints_one_sorted_document(operator, monkeypatch, capsys, argv):
    tool = operator[0]
    monkeypatch.setenv("HARTMESH_DATA_DIR", str(tool.data_dir))
    for key in si.KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HARTMESH_USERS", "1")
    name = argv[0].replace("-", "_")
    monkeypatch.setattr(si.StorageInitializer, name, lambda self, *args, **kwargs: dict(command=argv[0], returncode=0, note="done"))
    assert si.main(argv) == 0
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    assert lines[0] == json.dumps(dict(command=argv[0], returncode=0, note="done"), sort_keys=True)
