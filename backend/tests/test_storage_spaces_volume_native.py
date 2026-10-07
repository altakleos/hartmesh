"""No-skip merge tier: actual disk mounts and unprivileged Docker writers.

Default contributor runs skip only this separately qualified Linux tier. Its
dedicated CI job requires the fixture and rejects every skipped test.
"""

from __future__ import annotations

import errno
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest

from deerflow.spaces.backings import BackingUnavailable, PreparedVolumeCatalog


@pytest.fixture(scope="module")
def prepared(tmp_path_factory):
    if os.environ.get("HARTMESH_REQUIRE_MANAGED_STORAGE") != "1":
        pytest.skip("Managed-disk qualification runs in its mandatory Linux CI job")
    if os.geteuid() != 0 or not shutil.which("docker"):
        pytest.fail("Mandatory managed-storage tier requires real mount authority and Docker")
    image = os.environ.get("HARTMESH_STORAGE_WRITER_IMAGE")
    if not image:
        pytest.fail("Mandatory native writer image was not provided")
    script = Path(__file__).resolve().parents[2] / "scripts" / "storage_spaces_volume.py"
    spec = importlib.util.spec_from_file_location("native_storage_operator", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    disk = tmp_path_factory.mktemp("resource volume")
    volumes = module.prepare_volumes(disk, count=1, size_bytes=64 << 20, inodes=256, reserve_bytes=16 << 20, reserve_inodes=64, data_uid=1000)
    catalog = PreparedVolumeCatalog.from_manifest(disk / "inventory.v1.json")
    containment = {"confirmed": True}
    try:
        yield catalog, volumes[0], image, containment
    finally:
        # Every native invocation proves its named container absent first.
        if containment["confirmed"]:
            subprocess.run([shutil.which("umount"), str(volumes[0].mount_path)], check=True, timeout=30)


@pytest.fixture
def backing(prepared):
    catalog, spec, image, containment = prepared
    if not containment["confirmed"]:
        pytest.fail("Earlier native writer containment is pending; preserve its disk")
    volume = catalog.verify(spec.slot_id)
    try:
        yield catalog, volume, image, containment
    finally:
        # This fixture owns only its new data/control tree, never other slots.
        if containment["confirmed"]:
            for parent in (volume.data_path, volume.control_path):
                for child in parent.iterdir():
                    if child.is_dir() and not child.is_symlink():
                        shutil.rmtree(child)
                    else:
                        child.unlink()


def _native(backing, program: str, *, readonly: bool = False):
    _catalog, volume, image, containment = backing
    name = "hartmesh-storage-native-" + uuid.uuid4().hex
    mount = f"type=bind,src={volume.data_path},dst=/data" + (",readonly" if readonly else "")
    command = [
        "docker",
        "run",
        "--rm",
        "--pull=never",
        "--name",
        name,
        "--label",
        "hartmesh.task=storage-spaces-ci",
        "--read-only",
        "--user",
        "1000:1000",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--network",
        "none",
        "--mount",
        mount,
        image,
        "python3",
        "-c",
        program,
    ]
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=60)
        return json.loads(result.stdout)
    finally:
        containment["confirmed"] = False
        subprocess.run(["docker", "rm", "-f", name], check=False, capture_output=True, timeout=30)
        probe = subprocess.run(["docker", "inspect", name], check=False, capture_output=True, text=True, timeout=15)
        if probe.returncode != 1 or "No such object" not in probe.stderr:
            pytest.fail("Native writer containment could not be confirmed; preserve the backing")
        containment["confirmed"] = True


def test_native_repository_binary_and_database_survive_a_new_container(backing):
    first = _native(
        backing,
        "import os,sqlite3,json; os.mkdir('/data/.git'); open('/data/.git/HEAD','wb').write(b'ref: refs/heads/main\\n'); "
        "open('/data/binary','wb').write(bytes(range(256))); db=sqlite3.connect('/data/wiki.sqlite'); "
        "db.execute('CREATE TABLE pages (title TEXT)'); db.execute('INSERT INTO pages VALUES (?)',('Home',)); "
        "db.commit(); db.close(); print(json.dumps({'created':True}))",
    )
    assert first == {"created": True}
    second = _native(backing, "import sqlite3,json; db=sqlite3.connect('/data/wiki.sqlite'); title=db.execute('SELECT title FROM pages').fetchone()[0]; print(json.dumps({'title':title,'binary':len(open('/data/binary','rb').read())}))")
    assert second == {"title": "Home", "binary": 256}
    with backing[1].filesystem() as fs:
        assert fs.read_bytes(".git/HEAD", max_bytes=100) == b"ref: refs/heads/main\n"


def test_host_created_files_and_folders_are_usable_by_the_native_view(backing):
    with backing[1].filesystem() as fs:
        fs.mkdir("notes")
        fs.write_atomic("notes/page", b"host-created", expected_sha256=None, create=True)
    result = _native(backing, "import json; before=open('/data/notes/page','rb').read(); open('/data/notes/page','wb').write(b'native-edited'); print(json.dumps({'before':before.decode()}))")
    assert result == {"before": "host-created"}
    with backing[1].filesystem() as fs:
        assert fs.read_bytes("notes/page", max_bytes=100) == b"native-edited"


def test_arbitrary_native_byte_growth_hits_the_kernel_bound_and_keeps_reserve(backing):
    catalog, volume, _image, _containment = backing
    result = _native(
        backing,
        "import os,errno,json; fd=os.open('/data/full',os.O_CREAT|os.O_WRONLY,0o600); chunk=b'x'*(1<<20); error=None\n"
        "try:\n while True: os.write(fd,chunk)\nexcept OSError as exc: error=exc.errno\nfinally: os.close(fd)\n"
        "print(json.dumps({'errno':error,'size':os.stat('/data/full').st_size}))",
    )
    assert result["errno"] == errno.ENOSPC
    assert 0 < result["size"] <= volume.spec.max_bytes
    catalog.verify_reserve()
    assert catalog.verify(volume.spec.slot_id, previous=volume).available_bytes < 1 << 20


def test_arbitrary_native_inode_growth_hits_the_kernel_bound(backing):
    result = _native(
        backing,
        "import os,json; count=0; error=None\ntry:\n while True:\n"
        "  fd=os.open('/data/f-'+str(count),os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600); os.close(fd); count+=1\n"
        "except OSError as exc: error=exc.errno\nprint(json.dumps({'errno':error,'files':count}))",
    )
    assert result["errno"] == errno.ENOSPC
    assert 0 < result["files"] <= backing[1].spec.max_inodes
    backing[0].verify_reserve()


def test_full_filesystem_edit_leaves_the_existing_file_intact(backing):
    volume = backing[1]
    with volume.filesystem() as fs:
        fs.write_atomic("page", b"before", expected_sha256=None, create=True)
    _native(
        backing,
        "import os,json; fd=os.open('/data/full',os.O_CREAT|os.O_WRONLY,0o600); error=None\ntry:\n while True: os.write(fd,b'x'*(1<<20))\nexcept OSError as exc: error=exc.errno\nfinally: os.close(fd)\nprint(json.dumps({'errno':error}))",
    )
    with volume.filesystem() as fs:
        with pytest.raises(OSError) as raised:
            fs.write_atomic("page", b"after" * 100000, expected_sha256=hashlib.sha256(b"before").hexdigest())
        assert raised.value.errno == errno.ENOSPC
        assert fs.read_bytes("page", max_bytes=100) == b"before"
    assert not list(volume.control_path.glob("stage-*"))


def test_private_backup_growth_counts_against_the_same_bound(backing):
    volume = backing[1]
    with pytest.raises(OSError) as raised:
        with (volume.control_path / "retained-backup").open("wb", buffering=0) as backup:
            while True:
                backup.write(b"x" * (1 << 20))
    assert raised.value.errno == errno.ENOSPC
    assert backing[0].verify(volume.spec.slot_id, previous=volume).available_bytes < 1 << 20
    backing[0].verify_reserve()


def test_readonly_view_and_private_mount_boundary(backing):
    result = _native(
        backing,
        "import os,json; error=None\ntry: open('/data/forbidden','wb').write(b'x')\nexcept OSError as exc: error=exc.errno\n"
        "print(json.dumps({'errno':error,'control':os.path.exists('/control'),'socket':os.path.exists('/var/run/docker.sock')}))",
        readonly=True,
    )
    assert result == {"errno": errno.EROFS, "control": False, "socket": False}


def test_reopening_inventory_preserves_identity_and_root_replacement_is_detected(backing):
    catalog, volume, _image, _containment = backing
    restarted = PreparedVolumeCatalog.from_manifest(catalog.data_disk / "inventory.v1.json")
    assert restarted.verify(volume.spec.slot_id, previous=volume).spec == volume.spec
    retired = volume.data_path.with_name("retired-data")
    volume.data_path.rename(retired)
    volume.data_path.mkdir()
    try:
        with pytest.raises(BackingUnavailable, match="incarnation"):
            catalog.verify(volume.spec.slot_id, previous=volume)
    finally:
        volume.data_path.rmdir()
        retired.rename(volume.data_path)


def test_mounted_uuid_mismatch_is_refused_before_file_capability(backing, monkeypatch):
    import deerflow.spaces.backings as module

    monkeypatch.setattr(module, "mounted_ext4_identity", lambda fd: str(uuid.uuid4()))
    with pytest.raises(BackingUnavailable, match="Mounted filesystem UUID"):
        backing[0].verify(backing[1].spec.slot_id)
