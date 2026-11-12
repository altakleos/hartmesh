"""A refreshed result refers to actual earlier media ACKs, never copies them."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from test_telegram_work_result_files import result_source
from test_telegram_work_result_ledger import acknowledge, delivery


def history(message_id=101):
    from app.telegram_bots.work_result_history import archive_media
    from app.telegram_bots.work_result_identity import result_identity
    from app.telegram_bots.work_result_ledger import replace_media

    item, recipes = delivery()
    source = result_source(["one.png", "two.png", "report.pdf"])
    raw = deepcopy(source.work)
    for reference in raw["outcome"]["sources"]:
        reference.pop("current_contents")
    item.payload["work_result_identity"] = result_identity(SimpleNamespace(**raw))
    item.parts[0]["state"] = "sending"
    item.parts[0]["result_file_hashes"] = {"0": "a" * 64, "1": "b" * 64}
    replace_media(item, item.parts[0]["part_id"], [("document", [file]) for file in recipes[0]["files"]], reason="photo-rejected")
    acknowledge(item.parts[1], message_id)
    proof = archive_media(item, source, locale="en", options={"version": 1, "capacity": 300, "max_bytes": 100})
    return item, source, proof


def test_partial_album_history_keeps_real_receipt_and_unsent_byte_identity():
    from app.telegram_bots.work_result_history import read_media_history

    item, source, proof = history()
    before = deepcopy(item)
    carried, hashes = read_media_history(item, proof, source)
    assert carried == {0: {"part_id": item.parts[1]["part_id"], "source_index": 0}}
    assert hashes == {0: "a" * 64, 1: "b" * 64}
    assert item == before


def test_historical_ack_survives_current_read_loss_without_historical_access():
    from app.telegram_bots.work_result_history import read_media_history

    item, source, proof = history()
    source.work["outcome"]["sources"][0] = {"kind": "unavailable", "current_contents": "not_checked"}
    carried, _hashes = read_media_history(item, proof, source)
    assert carried[0]["part_id"] == item.parts[1]["part_id"]


@pytest.mark.parametrize("damage", ["missing", "ids", "hash", "source", "outcome", "assignment", "duplicate", "protection"])
def test_media_history_refuses_foreign_or_changed_receipts(damage):
    from app.telegram_bots.work_result_history import read_media_history

    item, source, proof = history()
    sent = item.parts[1]
    if damage == "missing":
        item.parts.remove(sent)
    elif damage == "ids":
        sent["message_ids"] = [True]
    elif damage == "hash":
        sent["result_file_hashes"]["0"] = "c" * 64
    elif damage == "source":
        source.work["outcome"]["sources"][0]["path"] = "elsewhere.png"
    elif damage == "outcome":
        source.work["outcome_id"] = "9" * 32
    elif damage == "assignment":
        source.work["assignment_revision"] += 1
    elif damage == "duplicate":
        item.parts.append(deepcopy(sent))
    else:
        sent["protect_content"] = True
    with pytest.raises(ValueError):
        read_media_history(item, proof, source)


def test_refresh_carries_actual_album_child_without_fabricating_sent_parts():
    from app.telegram_bots.work_result_history import read_media_history
    from app.telegram_bots.work_result_ledger import carry_media, qualify_result_parts

    previous, source, proof = history()
    carried, hashes = read_media_history(previous, proof, source)
    fresh, recipes = delivery()
    carry_media(fresh, carried, hashes)
    qualify_result_parts(fresh, recipes, carried=carried)
    assert all(part["state"] != "sent" and part["message_ids"] == [] for part in fresh.parts)
    remaining = [part for part in fresh.parts if "files" in part and part["state"] == "pending"]
    assert [[file["source_index"] for file in part["files"]] for part in remaining] == [[1], [2]]
    assert remaining[0]["result_file_hashes"] == {"1": "b" * 64}
    event = fresh.payload["work_result_ledger"]["events"][0]
    assert event["receipts"] == [carried[0]]
    event["receipts"][0]["part_id"] = "9" * 32
    with pytest.raises(ValueError):
        qualify_result_parts(fresh, recipes, carried=carried, known_hashes=hashes)


@pytest.mark.parametrize("damage", ["remove", "replace"])
def test_refreshed_pending_bytes_cannot_forget_historical_digest(damage):
    from app.telegram_bots.work_result_history import read_media_history
    from app.telegram_bots.work_result_ledger import carry_media, qualify_result_parts

    previous, source, proof = history()
    carried, hashes = read_media_history(previous, proof, source)
    fresh, recipes = delivery()
    carry_media(fresh, carried, hashes)
    for part in fresh.parts:
        if "1" in part.get("result_file_hashes", {}):
            if damage == "remove":
                part["result_file_hashes"].pop("1")
            else:
                part["result_file_hashes"]["1"] = "c" * 64
    with pytest.raises(ValueError):
        qualify_result_parts(fresh, recipes, carried=carried, known_hashes=hashes)


def test_known_ack_cannot_be_removed_from_current_carry_coverage():
    from app.telegram_bots.work_result_history import read_media_history
    from app.telegram_bots.work_result_ledger import qualify_result_parts

    previous, source, proof = history()
    carried, _hashes = read_media_history(previous, proof, source)
    fresh, recipes = delivery()
    with pytest.raises(ValueError):
        qualify_result_parts(fresh, recipes, carried=carried)


def test_historical_media_proof_cannot_be_dropped_while_actual_receipt_remains():
    from app.telegram_bots.work_result_history import qualified_history

    previous, source, _proof = history()
    previous.payload["presentation_version"] = 1
    with pytest.raises(ValueError):
        qualified_history(previous, source)


@pytest.mark.parametrize("field", ["message_ids", "presentation_version", "source_index"])
def test_actual_historical_receipt_rejects_boolean_integer_aliases(field):
    from app.telegram_bots.work_result_history import read_media_history

    item, source, proof = history(1)
    if field == "message_ids":
        item.parts[1][field] = [True]
    elif field == "presentation_version":
        item.parts[1][field] = False
    else:
        item.parts[1]["files"][0][field] = False
    with pytest.raises(ValueError):
        read_media_history(item, proof, source)
