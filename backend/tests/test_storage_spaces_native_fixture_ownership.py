"""An ambiguous create must not permit deleting an owned fixture's backing."""

import ast
import errno
import sqlite3
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from _storage_spaces_native_test_support import prepare_consumer_container, read_sqlite_page_snapshot, remove_owned_loop


@pytest.mark.parametrize("missing_table", [False, True])
def test_native_readback_closes_the_actual_connection_before_backing_retirement(tmp_path, monkeypatch, missing_table):
    database = tmp_path / "pages.sqlite"
    seed = sqlite3.connect(database)
    try:
        if not missing_table:
            seed.execute("CREATE TABLE pages (body TEXT)")
            seed.execute("INSERT INTO pages VALUES ('committed')")
            seed.commit()
    finally:
        seed.close()
    original_connect = sqlite3.connect
    retained = []

    def connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        retained.append(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect)
    try:
        if missing_table:
            with pytest.raises(sqlite3.OperationalError, match="no such table"):
                read_sqlite_page_snapshot(database)
        else:
            assert read_sqlite_page_snapshot(database) == (("ok",), [("committed",)])
        assert len(retained) == 1
        # Holding the real connection prevents GC from hiding missing closure.
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            retained[0].execute("SELECT 1")
    finally:
        for connection in retained:
            connection.close()


def test_create_effect_before_lost_acknowledgement_preserves_fixture_uncertainty(tmp_path):
    plan = SimpleNamespace(id="a" * 32, host_id="fixture", views=())
    effect = tmp_path / "created-container"
    containers, containment = [], {"confirmed": True}

    def create_then_error(arguments, **_kwargs):
        effect.write_text("task-owned stopped container", encoding="utf-8")
        raise subprocess.TimeoutExpired(arguments, 30)

    with pytest.raises(subprocess.TimeoutExpired):
        prepare_consumer_container(plan, "fixture-image", "pass", containers, containment, runner=create_then_error)
    assert effect.exists() and not containers
    assert containment["confirmed"] is False


@pytest.mark.parametrize("previously_confirmed", [True, False])
def test_exact_capture_keeps_owned_identity_and_never_clears_earlier_uncertainty(previously_confirmed):
    plan = SimpleNamespace(id="a" * 32, host_id="fixture", views=())
    containers, containment = [], {"confirmed": previously_confirmed}

    def acknowledged(arguments, **_kwargs):
        return subprocess.CompletedProcess(arguments, 0, stdout="c" * 64 + "\n")

    assert prepare_consumer_container(plan, "fixture-image", "pass", containers, containment, runner=acknowledged) == "c" * 64
    assert containers == [("c" * 64, plan.id)]
    assert containment["confirmed"] is previously_confirmed


def test_resident_native_fixture_uses_real_provider_registry_and_dedicated_inodes():
    # Parse as data. Importing/collecting the mounted-volume suite locally is
    # forbidden, including merely to borrow one of its fixtures.
    tree = ast.parse(Path(__file__).with_name("test_storage_spaces_volume_native.py").read_text(encoding="utf-8"))
    functions = {node.name: node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    setup = ast.unparse(functions["resident_runtime"])
    assert any(isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "pytest_asyncio" and node.attr == "fixture" for node in functions["resident_runtime"].decorator_list)
    assert "run_file_io(AioSandboxProvider)" in setup and "ResidentRegistry(" in setup and "RunManager()" in setup
    assert "qualified_attachments(" in setup and "settle_contained(" in setup
    assert "begin_restore(" in setup and "finish_restore(" in setup and "retained_claims(" in setup
    assert "external_native_claims" in setup and "shutdown" in setup and "unregister" in setup
    assert "4096" in ast.unparse(functions["resident_prepared"])
    native = ast.unparse(functions["test_agent_two_chats_use_one_qualified_home_with_real_aio_protocol"])
    assert "resident_runtime" in native and "object.__new__" not in native and "native_prepare(" in native
    fixture_values = [
        ast.unparse(node.value)
        for node in ast.walk(functions["test_agent_two_chats_use_one_qualified_home_with_real_aio_protocol"])
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "runtime" for target in node.targets)
    ]
    assert fixture_values == ["resident_runtime"], "Presentation context must not replace the resident fixture"
    containment = ast.unparse(functions["test_host_containment_removes_the_real_container_and_the_next_run_attaches_afresh"])
    for fact in ("contain_host", "fenced_by_kind", "fenced_by_id", "fence_reason", "fenced_at", "subprocess.run"):
        assert fact in containment


@pytest.mark.parametrize("change", [None, "rebound", "replaced", "timeout", "denied"])
def test_owned_loop_removal_reconciles_only_same_unbound_device(tmp_path, change):
    sysfs = tmp_path / "loop"
    sysfs.mkdir()
    meta = sysfs.stat()
    expected = meta.st_dev, meta.st_ino
    now, calls = [0.0], []

    def ioctl(fd, operation, number):
        calls.append((fd, operation, number))
        assert (fd, operation, number) == (41, 0x4C81, 73)
        if len(calls) == 1 or change == "timeout":
            if change == "rebound":
                (sysfs / "loop").mkdir()
            elif change == "replaced":
                sysfs.rename(tmp_path / "original")
                sysfs.mkdir()
            raise OSError(errno.EACCES if change == "denied" else errno.EBUSY, "fixture opener")
        return 0

    def sleep(seconds):
        assert seconds == 0.05
        now[0] += 1

    def remove():
        remove_owned_loop(41, 73, sysfs, expected=expected, ioctl=ioctl, clock=lambda: now[0], sleep=sleep)

    if change in {"rebound", "replaced"}:
        with pytest.raises(RuntimeError, match="changed|bound"):
            remove()
        assert len(calls) == 1
    elif change in {"timeout", "denied"}:
        with pytest.raises(OSError) as caught:
            remove()
        assert caught.value.errno == (errno.EBUSY if change == "timeout" else errno.EACCES)
        assert len(calls) == (6 if change == "timeout" else 1)
    else:
        remove()
        assert len(calls) == 2
    assert sysfs.is_dir()


@pytest.mark.parametrize("change", [None, "foreign", "autoclear", "timeout"])
def test_owned_loop_waits_only_for_its_original_autoclear_binding(tmp_path, change):
    sysfs, binding = tmp_path, tmp_path / "loop"
    binding.mkdir()
    original = {"backing_file": "/own.img", "offset": "0", "sizelimit": "0", "autoclear": "1"}
    for name, value in original.items():
        (binding / name).write_text(value + "\n", encoding="utf-8")
    meta = sysfs.stat()
    now, calls = [0.0], []

    def ioctl(*args):
        assert not binding.exists(), "No removal request while the loop is bound"
        calls.append(args)
        return 0

    def sleep(seconds):
        assert seconds == 0.05
        now[0] += 1
        if change is None:
            for child in binding.iterdir():
                child.unlink()
            binding.rmdir()
        elif change in {"foreign", "autoclear"}:
            (binding / ("backing_file" if change == "foreign" else "autoclear")).write_text("/foreign.img\n" if change == "foreign" else "0\n", encoding="utf-8")

    def remove():
        remove_owned_loop(41, 73, sysfs, expected=(meta.st_dev, meta.st_ino), retiring_binding=original, ioctl=ioctl, clock=lambda: now[0], sleep=sleep)

    if change == "timeout":
        with pytest.raises(TimeoutError, match="retire"):
            remove()
    elif change in {"foreign", "autoclear"}:
        with pytest.raises(RuntimeError, match="binding"):
            remove()
    else:
        remove()
        assert calls == [(41, 0x4C81, 73)]
    if change is not None:
        assert not calls and binding.is_dir()


@pytest.mark.parametrize("unavailable", [errno.ENOENT, errno.ENODEV], ids=["attribute-removed", "backing-device-gone"])
@pytest.mark.parametrize("outcome", ["retired", "rebound", "incomplete", "replaced"])
def test_owned_loop_observes_partial_sysfs_retirement(tmp_path, monkeypatch, outcome, unavailable):
    sysfs = tmp_path / "device"
    binding = sysfs / "loop"
    binding.mkdir(parents=True)
    original = {"backing_file": "/own.img", "offset": "0", "sizelimit": "0", "autoclear": "1"}
    # An open sysfs attribute may lose its device before the directory retires.
    backing = binding / "backing_file"
    if unavailable == errno.ENODEV:
        backing.touch()
        read_text = Path.read_text

        def read_attribute(path, *args, **kwargs):
            if path == backing and path.stat().st_size == 0:
                raise OSError(errno.ENODEV, "device retired")
            return read_text(path, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", read_attribute)
    meta = sysfs.stat()
    now, calls = [0.0], []

    def sleep(seconds):
        assert seconds == 0.05 and not calls
        now[0] += 1
        if outcome == "retired":
            backing.unlink(missing_ok=True)
            binding.rmdir()
        elif outcome == "rebound":
            for name, value in {**original, "backing_file": "/other.img"}.items():
                (binding / name).write_text(value, encoding="utf-8")
        elif outcome == "replaced":
            sysfs.rename(tmp_path / "old-device")
            sysfs.mkdir()

    def ioctl(*args):
        assert outcome == "retired" and not binding.exists()
        calls.append(args)
        return 0

    def remove():
        remove_owned_loop(41, 73, sysfs, expected=(meta.st_dev, meta.st_ino), retiring_binding=original, ioctl=ioctl, clock=lambda: now[0], sleep=sleep)

    if outcome == "retired":
        remove()
        assert calls == [(41, 0x4C81, 73)]
    elif outcome == "incomplete":
        with pytest.raises(TimeoutError, match="retire"):
            remove()
        assert now[0] == 5 and not calls
    else:
        with pytest.raises(RuntimeError, match="changed"):
            remove()
        assert not calls


def test_loop_retirement_propagates_unrelated_attribute_errors(tmp_path, monkeypatch):
    binding = tmp_path / "loop"
    binding.mkdir()
    meta = tmp_path.stat()

    def io_error(path, **kwargs):
        raise OSError(errno.EIO, "sysfs read error")

    monkeypatch.setattr(Path, "read_text", io_error)
    with pytest.raises(OSError) as caught:
        remove_owned_loop(41, 73, tmp_path, expected=(meta.st_dev, meta.st_ino), retiring_binding={}, ioctl=pytest.fail, sleep=pytest.fail)
    assert caught.value.errno == errno.EIO
