import hashlib

import pytest
from pydantic import ValidationError

from app.gateway.snapshot_manifest import SnapshotMember, freeze_manifest, member_record, parse_manifest


def digest(data):
    return hashlib.sha256(data).hexdigest()


def test_the_snapshot_is_reproducible_and_only_keeps_visible_words_and_distinct_files():
    messages = [{"type": "human", "id": "a", "content": "Hello"}, {"type": "tool", "content": "tool data"}, {"type": "ai", "content": "World"}]
    files = [SnapshotMember(path="files/notes/a.txt", size=4, sha256=digest(b"same")), SnapshotMember(path="files/notes/b.txt", size=4, sha256=digest(b"same")), SnapshotMember(path="files/else/c.txt", size=4, sha256=digest(b"same"))]
    one = freeze_manifest("Quarter / one", messages, files)
    assert one == freeze_manifest("Quarter / one", messages, reversed(files))
    manifest = parse_manifest(one.document)
    assert [(row.type, row.content) for row in manifest.messages] == [("human", "Hello"), ("ai", "World")]
    assert [f.path for f in manifest.files] == ["files/else/c.txt", "files/notes/a.txt"]
    assert manifest.files[1].also_named == ["b.txt"]
    assert manifest.transcript.sha256 == digest(one.transcript)
    assert one.folder.count("/") == 1 and one.folder.endswith(digest(one.document)[:8])


def test_newer_equal_bytes_and_unlisted_paths_are_not_members():
    member = SnapshotMember(path="files/notes.txt", size=4, sha256=digest(b"same"))
    manifest = {"path": "Conversations/Notes · 01234567/conversation.json", "published_at": "2026-01-02T00:00:00+00:00"}
    record = {"path": "Conversations/Notes · 01234567/files/notes.txt", "size": 4, "sha256": member.sha256, "published_at": "2026-01-01T00:00:00+00:00", "removed_at": None}
    assert member_record(manifest, member, record)
    assert not member_record(manifest, member, {**record, "published_at": "2026-01-03T00:00:00+00:00"})
    assert not member_record(manifest, member, {**record, "path": "Conversations/else/notes.txt"})
    assert not member_record(manifest, member, {**record, "sha256": "0" * 64})


def test_reader_accepts_the_version_one_agent_label_and_refuses_nonmember_paths():
    frozen = freeze_manifest("Notes", [], [])
    import json

    doc = json.loads(frozen.document)
    doc["with"] = {"kind": "employee", "id": "agent-a", "name": "Editor"}
    assert parse_manifest(json.dumps(doc).encode()).with_agent.name == "Editor"
    doc["files"] = [dict(path="outside/file.txt", sha256="a" * 64, size=10)]
    with pytest.raises((ValidationError, ValueError)):
        parse_manifest(json.dumps(doc).encode())
