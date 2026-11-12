"""Only explicit ordered media rewrites and complete typed ACKs qualify results."""

from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from test_telegram_work_result_files import present, result_source

from app.telegram_bots.producer import Outbox


def delivery():
    from app.telegram_bots.work_result_ledger import initial_ledger

    view = present(result_source(["one.png", "two.png", "report.pdf"]))
    recipes = [{**part, "source_ref": {"key": "work.request.presentation", "values": {"request_id": "1" * 32, "part": index}}} for index, part in enumerate(view.parts)]
    parts = Outbox._parts(recipes, 0)
    return SimpleNamespace(parts=parts, payload={"presentation_version": 0, "work_result_ledger": initial_ledger(parts)}), recipes


def acknowledge(part, *ids):
    part.update(state="sent", message_ids=list(ids), sent_at=datetime.now(UTC).isoformat())
    if "files" in part:
        part.setdefault("result_file_hashes", {str(file["source_index"]): "a" * 64 for file in part["files"]})


def qualify(item, recipes, selected=None, *, acknowledged=False):
    from app.telegram_bots.work_result_ledger import qualify_result_parts

    return qualify_result_parts(item, recipes, selected=selected, acknowledged=acknowledged)


def test_result_card_requires_every_file_and_all_album_acknowledgements():
    item, recipes = delivery()
    card = item.parts[-1]
    card["state"] = "sending"
    with pytest.raises(ValueError):
        qualify(item, recipes, card)
    acknowledge(item.parts[0], 101, 102)
    acknowledge(item.parts[1], 103)
    assert qualify(item, recipes, card) == recipes[-1]
    acknowledge(card, 104)
    assert qualify(item, recipes, card, acknowledged=True) == recipes[-1]
    for ids in ([101], [101, 101], [101, True], [101, -2]):
        item.parts[0]["message_ids"] = ids
        with pytest.raises(ValueError):
            qualify(item, recipes, card, acknowledged=True)


def test_document_fallback_retains_order_hashes_and_the_acknowledged_prefix():
    from app.telegram_bots.work_result_ledger import replace_media

    item, recipes = delivery()
    album = item.parts[0]
    album.update(state="sending", result_file_hashes={"0": "a" * 64, "1": "b" * 64})
    replace_media(item, album["part_id"], (("document", [album["files"][0]]), ("document", [album["files"][1]])), reason="photo-rejected")
    assert [part["kind"] for part in item.parts] == ["album", "document", "document", "document", "text"]
    assert item.parts[0]["state"] == "retired"
    assert [part["result_file_hashes"] for part in item.parts[1:3]] == [{"0": "a" * 64}, {"1": "b" * 64}]
    acknowledge(item.parts[1], 201)
    item.parts[2]["state"] = "sending"
    assert qualify(item, recipes, item.parts[2])["files"][0]["source_index"] == 1
    before = deepcopy(item.parts[1])
    # A later retry cannot rewrite a known ACK or skip its still-pending sibling.
    with pytest.raises(ValueError):
        replace_media(item, before["part_id"], (("document", before["files"]),), reason="capacity")
    assert item.parts[1] == before
    item.parts[-1]["state"] = "sending"
    with pytest.raises(ValueError):
        qualify(item, recipes, item.parts[-1])


def test_visible_file_failure_is_a_required_part_before_the_card():
    from app.telegram_bots.work_result_ledger import replace_unavailable

    item, recipes = delivery()
    acknowledge(item.parts[0], 301, 302)
    item.parts[1]["state"] = "sending"
    replace_unavailable(item, item.parts[1]["part_id"], key="file.too_large_out")
    warning, card = item.parts[-2:]
    assert warning["result_file_warning"] == {"key": "file.too_large_out", "file": recipes[1]["files"][0]}
    card["state"] = "sending"
    with pytest.raises(ValueError):
        qualify(item, recipes, card)
    acknowledge(warning, 303)
    assert qualify(item, recipes, card) == recipes[-1]
    warning["result_file_warning"]["file"]["filename"] = "another-file.pdf"
    with pytest.raises(ValueError):
        qualify(item, recipes, card)


@pytest.mark.parametrize("corruption", ["order", "hash", "source", "hidden_child", "cycle", "unrecorded_retirement", "sound"])
def test_result_ledger_rejects_invented_or_changed_delivery_history(corruption):
    from app.telegram_bots.work_result_ledger import replace_media

    item, recipes = delivery()
    album = item.parts[0]
    album["state"] = "sending"
    replace_media(item, album["part_id"], tuple(("document", [file]) for file in album["files"]), reason="photo-rejected")
    if corruption == "order":
        item.parts[1:3] = reversed(item.parts[1:3])
    elif corruption == "hash":
        item.parts[1]["result_file_hashes"] = {"0": "not-a-digest"}
    elif corruption == "source":
        item.parts[1]["files"][0]["source"]["space_id"] = "9" * 32
    elif corruption == "hidden_child":
        item.parts.pop(1)
    elif corruption == "cycle":
        item.payload["work_result_ledger"]["events"][0]["new"] = [album["part_id"]]
    elif corruption == "unrecorded_retirement":
        item.parts[-2]["state"] = "retired"
    else:
        item.parts[1]["silent"] = False
    with pytest.raises(ValueError):
        qualify(item, recipes)
