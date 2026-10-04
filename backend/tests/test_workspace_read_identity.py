"""Workspace samples, digests and text must describe one owned opened file."""

import hashlib
import os

import pytest

from deerflow.files import store
from deerflow.workspace_changes import WorkspaceRoot, scan_workspace_roots, scanner


@pytest.mark.parametrize("capability", ["directory_descriptors", "directory_walk", "no_follow"])
def test_workspace_capability_failure_is_explicit(tmp_path, monkeypatch, capability):
    root = WorkspaceRoot(name="outputs", host_path=tmp_path, virtual_prefix="/mnt/user-data/outputs")
    if capability == "directory_descriptors":
        monkeypatch.setattr(store, "_DIR_FD", False)
    elif capability == "no_follow":
        monkeypatch.setattr(store, "_O_NOFOLLOW", 0)
    else:
        monkeypatch.delattr(scanner.os, "fwalk")
    with pytest.raises(store.SafeFileAccessUnavailable):
        scan_workspace_roots([root])


def test_workspace_sample_hash_and_text_share_the_opened_file(tmp_path, monkeypatch):
    output = tmp_path / "outputs"
    output.mkdir()
    target = output / "report.txt"
    original = b"original report\n" * 1000
    target.write_bytes(original)
    replacement = tmp_path / "replacement.part"
    replacement.write_bytes(b"replacement report\n" * 1000)
    real_read_sample = scanner._read_sample

    def replace_after_sample(source):
        sample = real_read_sample(source)
        os.replace(replacement, target)
        return sample

    monkeypatch.setattr(scanner, "_read_sample", replace_after_sample)
    root = WorkspaceRoot(name="outputs", host_path=output, virtual_prefix="/mnt/user-data/outputs")
    result = scan_workspace_roots([root], text_paths={"/mnt/user-data/outputs/report.txt"})
    snapshot = result.files["/mnt/user-data/outputs/report.txt"]
    assert snapshot.size == len(original)
    assert snapshot.sha256 == hashlib.sha256(original).hexdigest()
    assert snapshot.text == original.decode("utf-8")
