"""Native file controllers can await fresh admission before publication."""

import hashlib
from unittest.mock import AsyncMock

import pytest

from deerflow.features.behaviors import MyFilesBehavior
from deerflow.features.publications import copy_and_record
from deerflow.files.store import StoredFile


@pytest.mark.asyncio
async def test_my_files_awaits_an_explicit_native_copy_instead_of_the_legacy_copy(monkeypatch):
    source = object()
    kept = object()
    copy = AsyncMock(return_value=kept)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("The native controller must not invoke the legacy copy")

    monkeypatch.setattr("deerflow.features.behaviors.manager.keep_file", forbidden)
    result = await MyFilesBehavior().execute("keep", user_id="alice", source=source, name="report.txt", folder="Reports", copy_async=copy)
    assert result is kept
    copy.assert_awaited_once_with(source=source, name="report.txt", folder="Reports", user_id="alice")


@pytest.mark.asyncio
async def test_publication_records_the_awaited_native_copy_and_keeps_provenance():
    source = object()
    published = StoredFile(path="Reports/report.txt", name="report.txt", size=4, modified=0, sha256=hashlib.sha256(b"data").hexdigest())
    native = AsyncMock(return_value=published)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("The native publication must not invoke a legacy filesystem copy")

    repo = AsyncMock()
    repo.record_publication.side_effect = lambda **facts: facts
    result, record = await copy_and_record(repo, source, name="report.txt", folder="Reports", user_id="alice", thread_id="chat-a", source_path="/mnt/user-data/outputs/report.txt", copy=forbidden, remove=forbidden, copy_async=native)
    assert result is published
    native.assert_awaited_once_with(source, name="report.txt", folder="Reports")
    assert record["published_by"] == "alice"
    assert record["from_thread_id"] == "chat-a"
    assert record["from_path"] == "/mnt/user-data/outputs/report.txt"
    assert record["sha256"] == published.sha256


@pytest.mark.asyncio
async def test_refused_native_copy_never_writes_a_publication_record():
    from deerflow.spaces.contract import SpaceDenied

    denied = AsyncMock(side_effect=SpaceDenied("Source authority changed before publication"))
    repo = AsyncMock()
    with pytest.raises(SpaceDenied):
        await copy_and_record(
            repo,
            object(),
            name="report.txt",
            folder="",
            user_id="alice",
            thread_id="chat-a",
            source_path="/mnt/user-data/outputs/report.txt",
            copy=lambda *_args, **_kwargs: None,
            remove=lambda *_args, **_kwargs: None,
            copy_async=denied,
        )
    repo.record_publication.assert_not_awaited()
