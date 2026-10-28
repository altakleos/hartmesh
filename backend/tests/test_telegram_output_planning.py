"""Pure metadata planning and off-loop image headers grant no file authority."""

import asyncio
import struct
import threading
import zlib
from copy import deepcopy

import pytest

from app.telegram_bots import files


def source(index, *, size=10_000_000, name=None, state="ready"):
    name = name or f"image-{index}.png"
    return {"path": "/mnt/user-data/outputs/" + name, "filename": name, "size": size, "mime_type": "application/octet-stream", "state": state}


def png_header(width, height):
    def chunk(kind, value):
        return struct.pack(">I", len(value)) + kind + value + struct.pack(">I", zlib.crc32(kind + value))

    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(b"\0")) + chunk(b"IEND", b"")


@pytest.mark.parametrize("answer,caption", [("x" * 1024, True), ("😀" * 512, True), ("😀" * 513, False)])
def test_single_file_caption_counts_visible_utf16_and_always_uses_document(answer, caption):
    plan = files.plan_output_files([source(0)], answer_visible=answer, capacity=200_000_000, max_bytes=50_000_000)
    assert plan.caption is caption
    assert plan.parts[0]["kind"] == ("document" if caption else "photo")
    assert plan.parts[0]["unavailable"] is None


@pytest.mark.parametrize("count,capacity,groups", [(2, 200_000_000, [2]), (10, 200_000_000, [10]), (11, 200_000_000, [10, 1]), (11, 50_000_000, [5, 5, 1])])
def test_candidate_albums_partition_before_reads_by_capacity_and_item_limit(count, capacity, groups):
    descriptors = [source(i) for i in range(count)]
    plan = files.plan_output_files(descriptors, answer_visible="Long " * 300, capacity=capacity, max_bytes=50_000_000)
    assert not plan.caption and [len(part["files"]) for part in plan.parts] == groups
    assert [item["path"] for part in plan.parts for item in part["files"]] == [item["path"] for item in descriptors]
    assert all(sum(item["size"] for item in part["files"]) <= capacity for part in plan.parts)


def test_unavailable_and_oversized_slots_preserve_order_and_prevent_caption():
    descriptors = [source(0, size=None, state="unavailable"), source(1, size=50_000_001), source(2, name="report.pdf", size=3)]
    plan = files.plan_output_files(descriptors, answer_visible="Short answer", capacity=50_000_000, max_bytes=50_000_000)
    assert not plan.caption
    assert [part["unavailable"] for part in plan.parts] == ["metadata", "oversize", None]
    assert all(part["kind"] == "document" for part in plan.parts)
    descriptors[2]["filename"] = "changed.pdf"
    assert plan.parts[-1]["files"][0]["filename"] == "report.pdf"


def test_documents_and_nonphoto_suffixes_are_album_order_barriers():
    descriptors = [source(0), source(1, name="vector.svg", size=3), source(2), source(3)]
    plan = files.plan_output_files(descriptors, answer_visible="Long " * 300, capacity=200_000_000, max_bytes=50_000_000)
    assert [part["kind"] for part in plan.parts] == ["photo", "document", "album"]
    original = deepcopy(plan.parts[-1])
    changed = files.classify_output_part(plan.parts[-1], [True, False])
    assert [part["kind"] for part in changed] == ["photo", "document"] and plan.parts[-1] == original
    assert [part["kind"] for part in files.classify_output_part(original, [False, False])] == ["document", "document"]


@pytest.mark.parametrize("mutation", [{"size": True}, {"size": None}, {"state": "unknown"}, {"path": "/mnt/user-data/uploads/private.txt"}, {"filename": "../private"}])
def test_malformed_metadata_cannot_gain_media_parts(mutation):
    with pytest.raises(ValueError):
        files.plan_output_files([{**source(0), **mutation}], answer_visible="Answer", capacity=200_000_000, max_bytes=50_000_000)


@pytest.mark.asyncio
@pytest.mark.parametrize("name,width,height,expected", [("image.PNG", 5000, 5000, True), ("image.png", 5001, 5000, False), ("image.png", 40, 2, True), ("image.png", 41, 2, False), ("image.svg", 2, 2, False)])
async def test_header_shape_uses_no_pixel_decode(name, width, height, expected, monkeypatch):
    from PIL import Image

    def no_pixels(*args, **kwargs):
        raise AssertionError("Pixel decode is outside this header classifier")

    monkeypatch.setattr(Image.Image, "load", no_pixels)
    assert await files.classify_output_photo(name, png_header(width, height)) is expected


@pytest.mark.asyncio
async def test_bad_header_is_a_document_and_cancellation_drains_native_header_worker(monkeypatch):
    from PIL import Image

    assert await files.classify_output_photo("image.png", b"not an image") is False
    entered, release, completed = threading.Event(), threading.Event(), []
    actual = Image.open

    def held(*args, **kwargs):
        entered.set()
        assert release.wait(15)
        try:
            return actual(*args, **kwargs)
        finally:
            completed.append(True)

    monkeypatch.setattr(Image, "open", held)
    task = asyncio.create_task(files.classify_output_photo("image.png", png_header(2, 2)))
    try:
        assert await asyncio.to_thread(entered.wait, 15)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not completed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert completed == [True]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_renamed_nonphoto_body_and_unbounded_header_read_are_documents(monkeypatch):
    from PIL import Image

    # A GIF signature and logical screen header: filename alone is insufficient.
    assert await files.classify_output_photo("renamed.png", b"GIF89a\x02\0\x02\0\0\0\0;") is False
    observed = []

    def excessive_header(source, **kwargs):
        try:
            source.read()
        except OSError:
            observed.append("refused")
            raise
        observed.append("copied")
        raise AssertionError("An oversized whole-body header read must be refused")

    monkeypatch.setattr(Image, "open", excessive_header)
    assert await files.classify_output_photo("large.png", b"x" * 100_000) is False
    assert observed == ["refused"]


@pytest.mark.parametrize("capacity,groups", [(200_000_000, [10]), (50_000_000, [5, 5]), (25_000_000, [2, 2, 2, 2, 2])])
def test_saved_album_repartitions_for_current_capacity_without_answer_or_reads(capacity, groups):
    original = {"kind": "album", "files": [source(index) for index in range(10)], "unavailable": None}
    unchanged = deepcopy(original)
    result = files.repartition_output_part(original, capacity=capacity)
    assert [len(part["files"]) for part in result] == groups
    assert [member for part in result for member in part["files"]] == original["files"]
    assert all(sum(member["size"] for member in part["files"]) <= capacity for part in result)
    assert original == unchanged
    if capacity == 200_000_000:
        assert result == (original,)


def test_saved_album_capacity_partition_keeps_single_photo_tail_and_refuses_oversized_member():
    original = {"kind": "album", "files": [source(index) for index in range(6)], "unavailable": None}
    result = files.repartition_output_part(original, capacity=50_000_000)
    assert [part["kind"] for part in result] == ["album", "photo"]
    assert files.repartition_output_part(result[-1], capacity=50_000_000) == (result[-1],)
    with pytest.raises(ValueError):
        files.repartition_output_part(original, capacity=9_999_999)
    with pytest.raises(ValueError):
        files.repartition_output_part({**original, "kind": "document"}, capacity=50_000_000)
