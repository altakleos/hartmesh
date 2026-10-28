"""Opted-in managed inputs survive native upload-directory failure."""

import errno

import pytest

from app.channels import manager
from app.channels.message_bus import InboundMessage
from deerflow.uploads import manager as uploads

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("number", [errno.ENOSPC, errno.EDQUOT, errno.EACCES])
async def test_registered_scope_gets_failure_without_downloading_or_aborting_text(monkeypatch, number):
    observed, reads = [], []
    original = OSError(number, "synthetic disk failure")

    def prepare(*args, **kwargs):
        raise original

    async def notify(file, error):
        observed.append((file, error))

    async def reader(*args):
        reads.append(args)
        return b"never downloaded"

    monkeypatch.setattr(uploads, "ensure_uploads_dir", prepare)
    monkeypatch.setitem(manager.INBOUND_FILE_READERS, "managed-prepare-test", reader)
    monkeypatch.setitem(manager.INBOUND_FILE_WRITE_FAILED, "managed-prepare-test", notify)
    monkeypatch.setattr(manager, "INBOUND_FILE_PREPARE_FILTERS", {"managed-prepare-test": lambda file: file.get("managed") is True}, raising=False)
    files = [{"filename": "a.csv", "managed": True}, {"filename": "b.csv", "managed": True}]
    message = InboundMessage(channel_name="managed-prepare-test", chat_id="chat", user_id="person", text="Keep these words", files=files)
    assert await manager._ingest_inbound_files("thread", message, user_id="owner") == []
    assert observed == [(file, original) for file in files]
    assert reads == [] and message.text == "Keep these words"


@pytest.mark.parametrize("files", [[{"filename": "legacy.csv"}], [{"filename": "managed.csv", "managed": True}, "invalid-member"]])
async def test_unmatched_legacy_preparation_failure_keeps_original_boundary(monkeypatch, files):
    def prepare(*args, **kwargs):
        raise OSError(errno.ENOSPC, "synthetic disk failure")

    async def notify(*args):
        pytest.fail("unmatched input must not invoke managed notification")

    monkeypatch.setattr(uploads, "ensure_uploads_dir", prepare)
    monkeypatch.setitem(manager.INBOUND_FILE_WRITE_FAILED, "managed-prepare-test", notify)
    monkeypatch.setattr(manager, "INBOUND_FILE_PREPARE_FILTERS", {"managed-prepare-test": lambda file: file.get("managed") is True}, raising=False)
    message = InboundMessage(channel_name="managed-prepare-test", chat_id="chat", user_id="person", text="Words", files=files)
    with pytest.raises(OSError) as error:
        await manager._ingest_inbound_files("thread", message)
    assert error.value.errno == errno.ENOSPC
