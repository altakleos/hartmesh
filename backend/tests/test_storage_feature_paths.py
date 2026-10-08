import pytest

from deerflow.config.paths import Paths, get_paths, paths_scope
from deerflow.features.paths import FeaturePaths
from deerflow.files.manager import keep_file
from deerflow.files.shared import publish_file, shared_mutation_state
from deerflow.projects.documents import stage_document_bytes


@pytest.mark.asyncio
async def test_feature_bytes_and_private_staging_use_separate_qualified_roots(tmp_path):
    from types import SimpleNamespace

    volumes = {}
    for namespace in ("hm.my-files", "hm.shared", "hm.projects"):
        root = tmp_path / namespace
        data, control = root / "data", root / "control"
        data.mkdir(parents=True)
        control.mkdir(mode=0o700)
        volumes[namespace] = SimpleNamespace(data_path=data, control_path=control)
    paths = FeaturePaths(Paths(tmp_path / "legacy"), "alice", volumes)
    source = tmp_path / "source.txt"
    source.write_bytes(b"exact")

    async def chunks():
        yield b"shelf"

    with paths_scope(paths):
        assert get_paths() is paths
        kept = keep_file("alice", source, name="report.txt")
        published = publish_file(source, name="report.txt")
        staged = await stage_document_bytes(paths, user_id="alice", project_id="p" * 32, chunks=chunks(), max_bytes=100)
        state = shared_mutation_state()
        state.acquire()
        state.close()
    assert (volumes["hm.my-files"].data_path / kept.path).read_bytes() == b"exact"
    assert (volumes["hm.shared"].data_path / published.path).read_bytes() == b"exact"
    assert staged.staging_path.is_relative_to(volumes["hm.projects"].control_path)
    assert not list(volumes["hm.projects"].data_path.rglob(".staging"))
    assert not (volumes["hm.shared"].data_path / ".shared-state").exists()
    with pytest.raises(PermissionError):
        paths.user_files_dir("bob")
    assert not (tmp_path / "legacy").exists()


def test_native_project_files_are_not_feature_orphans_and_same_size_edits_fail_shelf_integrity(tmp_path):
    import hashlib
    import os
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    from deerflow.projects.documents import _content_intact
    from deerflow.projects.trash import SweepReport, _reconcile_storage

    data, control = tmp_path / "data", tmp_path / "control"
    data.mkdir()
    control.mkdir(mode=0o700)
    paths = FeaturePaths(Paths(tmp_path / "legacy"), "alice", {"hm.projects": SimpleNamespace(data_path=data, control_path=control)})
    project = "a" * 32
    namespace = paths.project_document_path("alice", f"{project}/documents/ordinary")
    original = namespace / "original" / "guide.txt"
    original.parent.mkdir(parents=True)
    original.write_bytes(b"first")
    row = {"stored_relpath": f"{project}/documents/ordinary", "name": "guide.txt", "size_bytes": 5, "sha256": hashlib.sha256(b"first").hexdigest()}
    assert _content_intact(paths, user_id="alice", row=row)
    original.write_bytes(b"other")
    assert not _content_intact(paths, user_id="alice", row=row)
    cutoff = datetime.now(UTC) - timedelta(days=1)
    os.utime(original, (cutoff.timestamp() - 1, cutoff.timestamp() - 1))
    staging = paths.project_staging_dir("alice", project)
    staging.mkdir(parents=True)
    orphan = staging / "own-staging"
    orphan.write_bytes(b"temporary")
    os.utime(orphan, (cutoff.timestamp() - 1, cutoff.timestamp() - 1))
    report = SweepReport()
    _reconcile_storage(paths, user_id="alice", rows=[], guard_cutoff=cutoff, report=report)
    assert original.read_bytes() == b"other"
    assert not orphan.exists()
    assert report.staging_removed == 1 and report.orphans_removed == 0
